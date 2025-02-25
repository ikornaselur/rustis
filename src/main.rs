use clap::Parser;
use redis_starter_rust::{Config, Result, Server};
use std::{cell::RefCell, rc::Rc};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The path to the directory where the RDB file is stored
    #[arg(short, long, default_value = "/tmp/redis-data")]
    dir: String,

    // The name of the RDB file
    #[arg(long, default_value = "dump.rdb")]
    dbfilename: String,

    // Hostname to listen on
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    // Port to listen on
    #[arg(short, long, default_value = "6379")]
    port: u16,
}

fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let config = Rc::new(RefCell::new(Config {
        dir: args.dir,
        dbfilename: args.dbfilename,
        host: args.host,
        port: args.port,
    }));

    let mut server = Server::new(config)?;

    server.run_forever()?;

    Ok(())
}
