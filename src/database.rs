use once_cell::sync::Lazy;
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
