use once_cell::sync::Lazy;
use std::{collections::HashMap, sync::RwLock};

pub(crate) type Database = HashMap<Vec<u8>, Vec<u8>>;
pub(crate) static DATABASES: Lazy<RwLock<Vec<Database>>> = Lazy::new(|| {
    let mut dbs = Vec::with_capacity(16);
    for _ in 0..16 {
        dbs.push(HashMap::new());
    }
    RwLock::new(dbs)
});
