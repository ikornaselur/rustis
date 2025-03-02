use crate::error::{Result, RustisError};
use crate::resp::RESPData;
use nom::{
    branch::alt,
    bytes::complete::{tag, take, take_until},
    character::complete::digit1,
    combinator::{map, map_res},
    multi::count,
    sequence::delimited,
    IResult, Parser,
};

/// Parse a simple string
///
/// > Simple strings are encoded as a plus (+) character, followed by a string. The string
/// > mustn't contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
///
/// Example:
///
/// ```ignore
/// +OK\r\n
/// ```
fn nom_simple_string(input: &[u8]) -> IResult<&[u8], RESPData> {
    map(
        delimited(tag(&b"+"[..]), take_until(&b"\r\n"[..]), tag(&b"\r\n"[..])),
        |s: &[u8]| RESPData::SimpleString(s),
    )
    .parse(input)
}

/// Parse a simple error
///
/// > RESP has specific data types for errors. Simple errors, or simply just errors, are similar
/// > to simple strings, but their first character is the minus (-) character. The difference
/// > between simple strings and errors in RESP is that clients should treat errors as
/// > exceptions, whereas the string encoded in the error type is the error message itself.
///
/// Example :
///
/// ```ignore
/// -Error message\r\n
/// ```
fn nom_simple_error(input: &[u8]) -> IResult<&[u8], RESPData> {
    map(
        delimited(tag(&b"-"[..]), take_until(&b"\r\n"[..]), tag(&b"\r\n"[..])),
        |s: &[u8]| RESPData::SimpleError(s),
    )
    .parse(input)
}

/// Parse a bulk string
///
/// > A bulk string represents a single binary string. The string can be of any size, but by
/// > default, Redis limits it to 512 MB (see the proto-max-bulk-len configuration directive).
/// > RESP encodes bulk strings in the following way:
/// >
/// > ```ignore
/// > $<length>\r\n<data>\r\n
/// > ```
///
/// Example:
///
/// ```ignore
/// $5\r\nhello\r\n
/// ```
fn nom_bulk_string(input: &[u8]) -> IResult<&[u8], RESPData> {
    let (input, length) = map_res(
        delimited(tag(&b"$"[..]), digit1, tag(&b"\r\n"[..])),
        |digits: &[u8]| {
            std::str::from_utf8(digits)
                .map_err(|e| e.to_string())
                .and_then(|s| s.parse::<usize>().map_err(|e| e.to_string()))
        },
    )
    .parse(input)?;
    let (input, data) = take(length).parse(input)?;
    let (input, _) = tag(&b"\r\n"[..]).parse(input)?;

    Ok((input, RESPData::BulkString(data)))
}

/// Parse an array
///
/// > Clients send commands to the Redis server as RESP arrays. Similarly, some Redis commands that
/// > return collections of elements use arrays as their replies. An example is the LRANGE command
/// > that returns elements of a list.
/// >
/// > RESP Arrays' encoding uses the following format:
///
/// ```ignore
/// *<number-of-elements>\r\n<element-1>...<element-n>
/// ```
fn nom_array(input: &[u8]) -> IResult<&[u8], RESPData> {
    let (input, length) = map_res(
        delimited(tag(&b"*"[..]), digit1, tag(&b"\r\n"[..])),
        |digits: &[u8]| {
            std::str::from_utf8(digits)
                .map_err(|e| e.to_string())
                .and_then(|s| s.parse::<usize>().map_err(|e| e.to_string()))
        },
    )
    .parse(input)?;

    let (input, elements) = count(nom_data, length).parse(input)?;

    Ok((input, RESPData::Array(elements)))
}

fn nom_data(input: &[u8]) -> IResult<&[u8], RESPData> {
    let mut parser = alt((
        nom_simple_string,
        nom_simple_error,
        nom_bulk_string,
        // nom_integer,
        nom_array,
        // nom_null,
        // nom_boolean,
        // nom_double,
        // nom_big_number,
        // nom_bulk_error,
        // nom_verbatim_string,
        // nom_map,
        // nom_attribute,
        // nom_set,
        // nom_push,
    ));

    parser.parse(input)
}

/// Parse input into `RESPData`
pub(crate) fn parse_input(input: &[u8]) -> Result<Vec<RESPData>> {
    let mut data = vec![];

    let mut input = input;

    while !input.is_empty() {
        match nom_data(input) {
            Ok((remaining, d)) => {
                data.push(d);
                input = remaining;
            }
            Err(err) => return Err(err.into()),
        }
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nom_simple_string() {
        assert_eq!(
            nom_simple_string(b"+OK\r\n"),
            Ok((&b""[..], RESPData::SimpleString(b"OK")))
        );
    }

    #[test]
    fn test_nom_simple_error() {
        assert_eq!(
            nom_simple_error(b"-Error message\r\n"),
            Ok((&b""[..], RESPData::SimpleError(b"Error message")))
        );

        assert_eq!(
            nom_simple_error(b"-ERR unknown command 'foobar'\r\n"),
            Ok((
                &b""[..],
                RESPData::SimpleError(b"ERR unknown command 'foobar'")
            ))
        );
    }

    #[test]
    fn test_nom_bulk_string() {
        assert_eq!(
            nom_bulk_string(b"$5\r\nhello\r\n"),
            Ok((&b""[..], RESPData::BulkString(b"hello")))
        );

        assert_eq!(
            nom_bulk_string(b"$0\r\n\r\n"),
            Ok((&b""[..], RESPData::BulkString(b"")))
        );

        assert_eq!(
            nom_bulk_string(b"$4\r\na\r\nb\r\n"),
            Ok((&b""[..], RESPData::BulkString(b"a\r\nb")))
        );
    }

    #[test]
    fn test_nom_array_empty() {
        assert_eq!(
            nom_array(b"*0\r\n"),
            Ok((&b""[..], RESPData::Array(vec![])))
        );
    }

    #[test]
    fn test_nom_array_with_values() {
        assert_eq!(
            nom_array(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"),
            Ok((
                &b""[..],
                RESPData::Array(vec![
                    RESPData::BulkString(b"foo"),
                    RESPData::BulkString(b"bar")
                ])
            ))
        );
    }

    #[test]
    fn test_parse_input() {
        assert_eq!(
            parse_input(b"+OK\r\n").unwrap(),
            RESPData::SimpleString(b"OK")
        );
        assert_eq!(
            parse_input(b"-Error message\r\n").unwrap(),
            RESPData::SimpleError(b"Error message")
        );
        assert_eq!(
            parse_input(b"$5\r\nhello\r\n").unwrap(),
            RESPData::BulkString(b"hello")
        );
    }

    #[test]
    fn test_parse_input_invalid() {
        assert!(matches!(
            parse_input(b"invalid input").unwrap_err(),
            RustisError::InvalidInput(_)
        ));

        assert!(matches!(
            parse_input(b"+OK\r\nextra data").unwrap_err(),
            RustisError::InvalidInput(_)
        ));
    }
}
