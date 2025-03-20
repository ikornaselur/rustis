use nom::{
    branch::alt,
    bytes::complete::{tag, take},
    combinator::{map, value},
    IResult, Parser,
};

#[derive(Debug, PartialEq, Clone)]
#[allow(clippy::upper_case_acronyms)]
enum OpCode {
    EOF,
    SELECTDB,
    EXPIRETIME,
    EXPIRETIMEMS,
    RESIZEDB,
    AUX,
}

#[derive(Debug, PartialEq)]
enum EncodedString<'a> {
    String(&'a [u8]),
    U8(u8),
    U16(u16),
    U32(u32),
}

#[derive(Debug, PartialEq)]
enum EncodedLength {
    Length(usize),
    U8(u8),
    U16(u16),
    U32(u32),
}

/// Parse header
///
/// Parses the magic string "REDIS" and version, returning the version
fn nom_rdb_header(input: &[u8]) -> IResult<&[u8], u16> {
    let (input, _) = tag(&b"REDIS"[..]).parse(input)?;
    let (input, version) = map(take(4usize), |v: &[u8]| {
        String::from_utf8_lossy(v).parse::<u16>().unwrap()
    })
    .parse(input)?;

    Ok((input, version))
}

/// Parse metadata section
///
/// Parse the contents of a metadata section, this does *not* parse the actual OpCode, it is
/// expected to be matched elsewhere before parsing the actual section itself
fn nom_metadata_section(input: &[u8]) -> IResult<&[u8], (EncodedString, EncodedString)> {
    let (input, key) = nom_size_encoded_string(input)?;
    let (input, value) = nom_size_encoded_string(input)?;
    Ok((input, (key, value)))
}

/// Parse RDB Op Code
fn nom_rdb_op_code(input: &[u8]) -> IResult<&[u8], OpCode> {
    let (input, op_code) = alt((
        value(OpCode::AUX, tag(&[0xFA][..])),
        value(OpCode::RESIZEDB, tag(&[0xFB][..])),
        value(OpCode::EXPIRETIMEMS, tag(&[0xFC][..])),
        value(OpCode::EXPIRETIME, tag(&[0xFD][..])),
        value(OpCode::SELECTDB, tag(&[0xFE][..])),
        value(OpCode::EOF, tag(&[0xFF][..])),
    ))
    .parse(input)?;

    Ok((input, op_code))
}

/// Parse size encoding
///
/// If the first two bits are 0b00:
///     The size is the remaining 6 bits of the byte
///     Example: 0b00001010 -> 10
///
/// If the first two bits are 0b01:
///     The size is the next 14 bits, in big-endian
///     Example: 0b01000010 10111100 -> 700
///
/// If the first two bits are 0b11:
///     The size is in little endian, either 8, 16 or 32 bits
///     If the first byte is 0xC0 (0b11000000): 8-bits
///     If the first byte is 0xC1 (0b11000001): 16-bits
///     If the first byte is 0xC2 (0b11000010): 32-bits
///     If the first byte is 0xC3 (0b11000011): LZF-compressed (not supported)
///
/// If the first 8 bits are 0b10000000:
///     Size is 32 bit (next 4 bytes), big endian:
///     Example: 0b10000000 0 0 01000010 01101000 -> 17_000
///
/// If the first 8 bits are 0b10000001:
///     Size is 64 bit (next 8 bytes), big endian.
///     0b10000001 0 0 0 00000001 0 0 0 0 -> 4_294_967_296
///
/// NOTE: 8 bits of zeroes are just represented as a single 0 above for brevity
///
/// We return a tuple of the size encoded value and wether it is a stored integer (0b11)
fn nom_size_encoding(input: &[u8]) -> IResult<&[u8], EncodedLength> {
    let (input, first_byte) = take(1usize).parse(input)?;

    let encoding_type = first_byte[0] >> 6;
    let first_byte_data = first_byte[0] & 0b0011_1111;

    match (encoding_type, first_byte_data) {
        (0b00, _) => Ok((input, EncodedLength::Length(first_byte_data as usize))),
        (0b01, _) => {
            let (input, second_byte) = take(1usize).parse(input)?;
            Ok((
                input,
                EncodedLength::Length(
                    u16::from_be_bytes([first_byte_data, second_byte[0]]) as usize
                ),
            ))
        }
        (0b10, 0) => {
            let (input, bytes) = take(4usize).parse(input)?;
            Ok((
                input,
                EncodedLength::Length(u32::from_be_bytes(bytes.try_into().unwrap()) as usize),
            ))
        }
        (0b10, 1) => {
            let (input, bytes) = take(8usize).parse(input)?;
            Ok((
                input,
                EncodedLength::Length(u64::from_be_bytes(bytes.try_into().unwrap()) as usize),
            ))
        }
        (0b11, 0) => {
            let (input, bytes) = take(1usize).parse(input)?;
            Ok((
                input,
                EncodedLength::U8(u8::from_le_bytes(bytes.try_into().unwrap())),
            ))
        }
        (0b11, 1) => {
            let (input, bytes) = take(2usize).parse(input)?;
            Ok((
                input,
                EncodedLength::U16(u16::from_le_bytes(bytes.try_into().unwrap())),
            ))
        }
        (0b11, 2) => {
            let (input, bytes) = take(4usize).parse(input)?;
            Ok((
                input,
                EncodedLength::U32(u32::from_le_bytes(bytes.try_into().unwrap())),
            ))
        }
        (0b11, 3) => {
            // LZF-compressed
            unimplemented!()
        }
        _ => unreachable!(),
    }
}

/// Parse size-encoded string
///
/// Note: We work with values as &[u8], that includes strings
fn nom_size_encoded_string(input: &[u8]) -> IResult<&[u8], EncodedString> {
    let (input, encoded_length) = nom_size_encoding(input)?;

    match encoded_length {
        EncodedLength::Length(l) => {
            let (input, string) = take(l).parse(input)?;
            Ok((input, EncodedString::String(string)))
        }
        EncodedLength::U8(val) => Ok((input, EncodedString::U8(val))),
        EncodedLength::U16(val) => Ok((input, EncodedString::U16(val))),
        EncodedLength::U32(val) => Ok((input, EncodedString::U32(val))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nom_rdb_header() {
        assert_eq!(nom_rdb_header(b"REDIS0006"), Ok((&b""[..], 6)));
        assert_eq!(nom_rdb_header(b"REDIS0012"), Ok((&b""[..], 12)));
    }

    #[test]
    fn test_nom_op_code() {
        assert_eq!(nom_rdb_op_code(&[0xFA]), Ok((&b""[..], OpCode::AUX)));
        assert_eq!(nom_rdb_op_code(&[0xFB]), Ok((&b""[..], OpCode::RESIZEDB)));
        assert_eq!(
            nom_rdb_op_code(&[0xFC]),
            Ok((&b""[..], OpCode::EXPIRETIMEMS))
        );
        assert_eq!(nom_rdb_op_code(&[0xFD]), Ok((&b""[..], OpCode::EXPIRETIME)));
        assert_eq!(nom_rdb_op_code(&[0xFE]), Ok((&b""[..], OpCode::SELECTDB)));
        assert_eq!(nom_rdb_op_code(&[0xFF]), Ok((&b""[..], OpCode::EOF)));
    }

    #[test]
    fn test_nom_size_encoding_6_bits() {
        assert_eq!(
            nom_size_encoding(&[0b00001010]),
            Ok((&b""[..], EncodedLength::Length(10)))
        );
        assert_eq!(
            nom_size_encoding(&[0x0D]),
            Ok((&b""[..], EncodedLength::Length(13)))
        );
    }

    #[test]
    fn test_nom_size_encoding_14_bits() {
        assert_eq!(
            nom_size_encoding(&[0b01000010, 0b10111100]),
            Ok((&b""[..], EncodedLength::Length(700)))
        );
    }

    #[test]
    fn test_nom_size_encoding_32_bits() {
        assert_eq!(
            nom_size_encoding(&[0b10000000, 0b00000000, 0b00000000, 0b01000010, 0b01101000]),
            Ok((&b""[..], EncodedLength::Length(17_000)))
        );
    }

    #[test]
    fn test_nom_size_encoding_64_bits() {
        assert_eq!(
            nom_size_encoding(&[
                0b10000001, 0b00000000, 0b00000000, 0b00000000, 0b00000001, 0b00000000, 0b00000000,
                0b00000000, 0b00000000
            ]),
            Ok((&b""[..], EncodedLength::Length(4_294_967_296)))
        );
    }

    #[test]
    fn test_nom_size_encoding_string_8_bits() {
        assert_eq!(
            nom_size_encoding(&[0xC0, 0x7B]),
            Ok((&b""[..], EncodedLength::U8(123)))
        );
    }

    #[test]
    fn test_nom_size_encoding_string_16_bits() {
        assert_eq!(
            nom_size_encoding(&[0xC1, 0x39, 0x30]),
            Ok((&b""[..], EncodedLength::U16(12345)))
        );
    }

    #[test]
    fn test_nom_size_encoding_string_32_bits() {
        assert_eq!(
            nom_size_encoding(&[0xC2, 0x87, 0xD6, 0x12, 0x00]),
            Ok((&b""[..], EncodedLength::U32(1234567)))
        );
    }

    #[test]
    fn test_nom_string() {
        let string = &b"Hello, world!"[..];
        assert_eq!(
            nom_size_encoded_string(&[
                0x0D, b'H', b'e', b'l', b'l', b'o', b',', b' ', b'w', b'o', b'r', b'l', b'd', b'!'
            ]),
            Ok((&b""[..], EncodedString::String(string)))
        );
    }

    #[test]
    fn test_nom_metadata_section() {
        // redis-ver: 7.4.2
        let data = &[
            0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2E, 0x34,
            0x2E, 0x32,
        ];

        let (input, (key, value)) = nom_metadata_section(data).unwrap();
        assert_eq!(input, &b""[..]);
        assert_eq!(key, EncodedString::String(&b"redis-ver"[..]));
        assert_eq!(value, EncodedString::String(&b"7.4.2"[..]));

        // redis-bits: 64
        let data = &[
            0x0A, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x62, 0x69, 0x74, 0x73, 0xC0, 0x40,
        ];
        let (input, (key, value)) = nom_metadata_section(data).unwrap();
        assert_eq!(input, &b""[..]);
        assert_eq!(key, EncodedString::String(&b"redis-bits"[..]));
        assert_eq!(value, EncodedString::U8(64));
    }
}
