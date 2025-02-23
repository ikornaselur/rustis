#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RESPData<'a> {
    SimpleString(&'a [u8]),
    SimpleError(&'a [u8]),
    // Integer(i64),
    BulkString(&'a [u8]),
    Array(Vec<RESPData<'a>>),
    // Null,
    // Boolean(bool),
    // Double(f64),
    // BigNumber(BigInt),
    // BulkError(String),
    // VerbatimString(String),
    // Map(HashMap<String, RESPData>),
    // Attribute(String, Box<RESPData>),
    // Set(Vec<RESPDataType>),
    // Push(Vec<RESPDataType>),
}
