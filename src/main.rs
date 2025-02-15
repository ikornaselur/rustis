use std::io::prelude::*;
use std::net::TcpListener;
use thiserror::Error;

type Result<T> = std::result::Result<T, RustisError>;

#[derive(Error, Debug)]
enum RustisError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => loop {
                let mut buf = [0; 512];
                match stream.read(&mut buf) {
                    Ok(n) => {
                        if n == 0 {
                            break;
                        }
                        println!("Request: {}", String::from_utf8_lossy(&buf[..]));
                        let response = b"+PONG\r\n";
                        let _ = stream.write(&response[..])?;
                    }
                    Err(e) => {
                        println!("error: {}", e);
                        break;
                    }
                }
            },
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}
