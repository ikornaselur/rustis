use crate::error::{Result, RustisError};
use crate::resp::RESPData;
use nom::{
    branch::alt,
    bytes::complete::{tag, take, take_until},
    character::complete::crlf,
    combinator::map,
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
fn nom_simple_string(input: &str) -> IResult<&str, RESPData> {
    map(delimited(tag("+"), take_until("\r\n"), crlf), |s: &str| {
        RESPData::SimpleString(s)
    })
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
fn nom_simple_error(input: &str) -> IResult<&str, RESPData> {
    map(delimited(tag("-"), take_until("\r\n"), crlf), |s: &str| {
        RESPData::SimpleError(s)
    })
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
fn nom_bulk_string(input: &str) -> IResult<&str, RESPData> {
    let (input, length) = map(delimited(tag("$"), take_until("\r\n"), crlf), |s: &str| {
        s.parse::<usize>().unwrap()
    })
    .parse(input)?;
    let (input, data) = take(length).parse(input)?;
    let (input, _) = crlf.parse(input)?;

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
fn nom_array(input: &str) -> IResult<&str, RESPData> {
    let (input, array_length) = map(delimited(tag("*"), take_until("\r\n"), crlf), |s: &str| {
        s.parse::<usize>().unwrap()
    })
    .parse(input)?;

    let (input, elements) = count(nom_data, array_length).parse(input)?;

    Ok((input, RESPData::Array(elements)))
}

fn nom_data(input: &str) -> IResult<&str, RESPData> {
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

/// Parse input into RESPData
pub(crate) fn parse_input(input: &str) -> Result<RESPData> {
    let (input, data) = match nom_data(input) {
        Ok((input, data)) => (input, data),
        Err(e) => return Err(RustisError::InvalidInput(format!("{}", e))),
    };

    // XXX: Is there a reason there might be extra data?
    if !input.is_empty() {
        return Err(RustisError::InvalidInput("Extra data found".to_string()));
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nom_simple_string() {
        assert_eq!(
            nom_simple_string("+OK\r\n"),
            Ok(("", RESPData::SimpleString("OK")))
        );
    }

    #[test]
    fn test_nom_simple_error() {
        assert_eq!(
            nom_simple_error("-Error message\r\n"),
            Ok(("", RESPData::SimpleError("Error message")))
        );

        assert_eq!(
            nom_simple_error("-ERR unknown command 'foobar'\r\n"),
            Ok(("", RESPData::SimpleError("ERR unknown command 'foobar'")))
        );
    }

    #[test]
    fn test_nom_bulk_string() {
        assert_eq!(
            nom_bulk_string("$5\r\nhello\r\n"),
            Ok(("", RESPData::BulkString("hello")))
        );

        assert_eq!(
            nom_bulk_string("$0\r\n\r\n"),
            Ok(("", RESPData::BulkString("")))
        );

        assert_eq!(
            nom_bulk_string("$4\r\na\r\nb\r\n"),
            Ok(("", RESPData::BulkString("a\r\nb")))
        );
    }

    #[test]
    fn test_nom_array() {
        assert_eq!(
            nom_array("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"),
            Ok((
                "",
                RESPData::Array(vec![
                    RESPData::BulkString("foo"),
                    RESPData::BulkString("bar")
                ])
            ))
        );
        assert_eq!(nom_array("*0\r\n"), Ok(("", RESPData::Array(vec![]))));
    }

    #[test]
    fn test_parse_input() {
        assert_eq!(
            parse_input("+OK\r\n").unwrap(),
            RESPData::SimpleString("OK")
        );
        assert_eq!(
            parse_input("-Error message\r\n").unwrap(),
            RESPData::SimpleError("Error message")
        );
        assert_eq!(
            parse_input("$5\r\nhello\r\n").unwrap(),
            RESPData::BulkString("hello")
        );
    }

    #[test]
    fn test_parse_input_invalid() {
        assert!(matches!(
            parse_input("invalid input").unwrap_err(),
            RustisError::InvalidInput(_)
        ));

        assert!(matches!(
            parse_input("+OK\r\nextra data").unwrap_err(),
            RustisError::InvalidInput(_)
        ));
    }
}
