use crate::error::Result;
use crate::parsers::rdb;
use memmap2::Mmap;
use once_cell::sync::Lazy;
use std::fs::File;
use std::{collections::HashMap, sync::RwLock};

#[derive(Debug)]
pub(crate) struct DBValue {
    pub value: Vec<u8>,
    pub ttl: Option<u128>,
}

impl DBValue {
    pub fn new(value: Vec<u8>, ttl: Option<u128>) -> Self {
        Self { value, ttl }
    }
}

pub(crate) type Database = HashMap<Vec<u8>, DBValue>;
pub(crate) static DATABASES: Lazy<RwLock<Vec<Database>>> = Lazy::new(|| {
    let mut dbs = Vec::with_capacity(16);
    for _ in 0..16 {
        dbs.push(HashMap::new());
    }
    RwLock::new(dbs)
});

/// Load a RDB file from disk
///
/// The contents of the RDB file will completely replace the contents of the in-memory databases,
/// meaning that anything that is in the database at the time of calling this function will be
/// cleared out first
pub fn load_rdb(path: &str) -> Result<()> {
    // Clear out the existing databases
    log::debug!("Loading RDB file: {}", path);
    log::trace!("Clearing out databases");
    let mut dbs = DATABASES.write().unwrap();
    dbs.iter_mut().for_each(|db| {
        db.clear();
    });

    log::trace!("Reading RDB file with Mmap");
    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };

    // Start with the header
    log::trace!("Parsing RDB header");
    let input = &mmap[..];
    let (mut input, version) = rdb::nom_rdb_header(input)?;
    log::debug!("RDB version: {}", version);

    let mut db_num = 0;

    loop {
        match rdb::nom_opcode_or_value_type(input) {
            Ok((rest, rdb::ParsedOpCodeOrValueType::OpCode(op_code))) => {
                input = rest;
                log::trace!("Parsed OpCode: {:?}", op_code);
                match op_code {
                    rdb::OpCode::EOF => {
                        log::trace!("Parsed EOF OpCode, stopping");
                        // TODO: Handle CRC64
                        break;
                    }
                    rdb::OpCode::SELECTDB => {
                        let (rest, encoded_db) = rdb::nom_size_encoding(input)?;
                        input = rest;
                        match encoded_db {
                            rdb::EncodedLength::Length(next_db) => {
                                log::trace!("Parsed SELECTDB OpCode, switching to DB: {}", next_db);
                                db_num = next_db;
                            }
                            _ => {
                                log::error!("Invalid SELECTDB OpCode, expected Length");
                                break;
                            }
                        }
                    }
                    rdb::OpCode::RESIZEDB => {
                        // We'll just read the values, but we're not doing anything with them yet
                        let (rest, hash_table_size) = rdb::nom_size_encoding(input)?;
                        input = rest;
                        log::trace!(
                            "Parsed RESIZEDB OpCode, hash table size: {}",
                            hash_table_size.as_usize()
                        );

                        let (rest, expiry_hash_table_size) = rdb::nom_size_encoding(input)?;
                        input = rest;
                        log::trace!(
                            "Parsed RESIZEDB OpCode, expiry hash table size: {}",
                            expiry_hash_table_size.as_usize()
                        );
                    }
                    rdb::OpCode::EXPIRETIME => {
                        unimplemented!();
                    }
                    rdb::OpCode::EXPIRETIMEMS => {
                        unimplemented!();
                    }
                    rdb::OpCode::AUX => {
                        let (rest, (key, value)) = rdb::nom_metadata_section(input).unwrap();

                        let key = match key {
                            rdb::EncodedString::String(k) => k,
                            _ => unimplemented!(),
                        };

                        let value = match value {
                            rdb::EncodedString::String(v) => v,
                            rdb::EncodedString::U8(v) => &v.to_string().into_bytes(),
                            rdb::EncodedString::U16(v) => &v.to_string().into_bytes(),
                            rdb::EncodedString::U32(v) => &v.to_string().into_bytes(),
                        };

                        input = rest;
                        log::trace!(
                            "Parsed AUX OpCode, key: {:?}, value: {:?}",
                            String::from_utf8_lossy(key),
                            String::from_utf8_lossy(value)
                        );
                    }
                    _ => {
                        log::warn!("Unhandled OpCode: {:?}", op_code);
                        break;
                    }
                }
            }
            Ok((rest, rdb::ParsedOpCodeOrValueType::ValueType(rdb::ValueTypeEncoding::String))) => {
                log::trace!("Parsed ValueType: String");
                let (rest, key) = rdb::nom_size_encoded_string(rest)?;
                let (rest, value) = rdb::nom_size_encoded_string(rest)?;
                input = rest;

                let key = match key {
                    rdb::EncodedString::String(k) => k,
                    _ => unimplemented!(),
                };
                let value = match value {
                    rdb::EncodedString::String(v) => v,
                    _ => unimplemented!(),
                };

                log::trace!(
                    "Setting key: {:?}, value: {:?}",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(value)
                );

                // Set the value in the current selected db
                if let Some(db) = dbs.get_mut(db_num) {
                    // TODO: Handle TTL
                    db.insert(key.to_vec(), DBValue::new(value.to_vec(), None));
                }
                // TODO: Handle else??
            }
            Ok((rest, rdb::ParsedOpCodeOrValueType::ValueType(value_type))) => {
                log::trace!("Parsed ValueType: {:?}", value_type);
                input = rest;
                unimplemented!();
            }
            Err(e) => {
                log::error!("Error parsing RDB file: {:?}", e);
                break;
            }
        }
    }

    log::trace!("Finished parsing RDB file");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const RDB_FILE: &str = "tests/files/simple.rdb";

    #[test]
    fn test_load_rdb() {
        load_rdb(RDB_FILE).unwrap();
    }
}
