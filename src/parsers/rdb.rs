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
}
