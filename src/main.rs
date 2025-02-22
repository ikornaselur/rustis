use redis_starter_rust::{Result, Server};

fn main() -> Result<()> {
    env_logger::init();
    let mut server = Server::new("127.0.0.1:6379")?;

    server.run_forever()?;

    Ok(())
}
