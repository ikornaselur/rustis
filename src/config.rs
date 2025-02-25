#[derive(Debug)]
pub struct Config {
    pub dir: String,
    pub dbfilename: String,
    pub host: String,
    pub port: u16,
    pub snapshot_interval: u64,
}

impl Config {
    pub fn dir(&self) -> &str {
        &self.dir
    }

    pub fn dbfilename(&self) -> &str {
        &self.dbfilename
    }

    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
