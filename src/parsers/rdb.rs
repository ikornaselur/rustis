use nom::{
    branch::alt,
    bytes::complete::{tag, take, take_until},
    character::complete::digit1,
    combinator::{map, map_res, value},
    multi::count,
    sequence::delimited,
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
///     Ignore the remaining 6 bits of the first byte,
///     size is 32 bit (next 4 bytes), big endian:
///     Example: 0b10000000 00000000 00000000 01000010 01101000 -> 17000
///
/// If the first 8 bits are 0b10000001:
///     Ignore the remaining 6 bits of the first byte,
///     size is 64 bit (next 8 bytes), big endian:
///
///
/// TODO: Do I need to support the output being 8u/u16/u32/u64 with an enum?
fn nom_size_encoding(input: &[u8]) -> IResult<&[u8], usize> {
    let (input, first_byte) = take(1usize).parse(input)?;

    match first_byte[0] >> 6 {
        0b00 => Ok((input, first_byte[0] as usize & 0b0011_1111)),
        0b01 => {
            let (input, second_byte) = take(1usize).parse(input)?;
            Ok((
                input,
                ((first_byte[0] as usize & 0b0011_1111) << 8) | second_byte[0] as usize,
            ))
        }
        0b10 if first_byte[0] == 0b10000000 => {
            let (input, bytes) = take(4usize).parse(input)?;
            Ok((
                input,
                u32::from_be_bytes(bytes.try_into().unwrap()) as usize,
            ))
        }
        0b10 if first_byte[0] == 0b10000001 => {
            let (input, bytes) = take(8usize).parse(input)?;
            Ok((
                input,
                u64::from_be_bytes(bytes.try_into().unwrap()) as usize,
            ))
        }
        0b11 if first_byte[0] == 0b11000000 => {
            let (input, bytes) = take(1usize).parse(input)?;
            Ok((input, u8::from_le_bytes(bytes.try_into().unwrap()) as usize))
        }
        0b11 if first_byte[0] == 0b11000001 => {
            let (input, bytes) = take(2usize).parse(input)?;
            Ok((
                input,
                u16::from_le_bytes(bytes.try_into().unwrap()) as usize,
            ))
        }
        0b11 if first_byte[0] == 0b11000010 => {
            let (input, bytes) = take(4usize).parse(input)?;
            Ok((
                input,
                u32::from_le_bytes(bytes.try_into().unwrap()) as usize,
            ))
        }
        0b11 if first_byte[0] == 0b11000011 => {
            // LZF-compressed
            unimplemented!()
        }
        _ => unreachable!(),
    }
}

/// Parse size-encoded string
///
/// Note: We work with values as &[u8], that includes strings
fn nom_size_encoded_string(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (input, size) = nom_size_encoding(input)?;
    let (input, string) = take(size).parse(input)?;
    Ok((input, string))
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
        assert_eq!(nom_size_encoding(&[0b00001010]), Ok((&b""[..], 10)));
        assert_eq!(nom_size_encoding(&[0x0D]), Ok((&b""[..], 13)));
    }

    #[test]
    fn test_nom_size_encoding_14_bits() {
        assert_eq!(
            nom_size_encoding(&[0b01000010, 0b10111100]),
            Ok((&b""[..], 700))
        );
    }

    #[test]
    fn test_nom_size_encoding_32_bits() {
        assert_eq!(
            nom_size_encoding(&[0b10000000, 0b00000000, 0b00000000, 0b01000010, 0b01101000]),
            Ok((&b""[..], 17000))
        );
    }

    #[test]
    fn test_nom_size_encoding_64_bits() {
        assert_eq!(
            nom_size_encoding(&[
                0b10000001, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000,
                0b01000010, 0b01101000
            ]),
            Ok((&b""[..], 17000))
        );
    }

    #[test]
    fn test_nom_size_encoding_string_8_bits() {
        assert_eq!(nom_size_encoding(&[0xC0, 0x7B]), Ok((&b""[..], 123)));
    }

    #[test]
    fn test_nom_size_encoding_string_16_bits() {
        assert_eq!(
            nom_size_encoding(&[0xC1, 0x39, 0x30]),
            Ok((&b""[..], 12345))
        );
    }

    #[test]
    fn test_nom_size_encoding_string_32_bits() {
        assert_eq!(
            nom_size_encoding(&[0xC2, 0x87, 0xD6, 0x12, 0x00]),
            Ok((&b""[..], 1234567))
        );
    }

    #[test]
    fn test_nom_string() {
        let string = &b"Hello, world!"[..];
        assert_eq!(
            nom_size_encoded_string(&[
                0x0D, b'H', b'e', b'l', b'l', b'o', b',', b' ', b'w', b'o', b'r', b'l', b'd', b'!'
            ]),
            Ok((&b""[..], string))
        );
    }
}
