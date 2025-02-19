use nix::poll::{poll, PollFd, PollFlags, PollTimeout};
use std::io;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::AsFd;
use thiserror::Error;

type Result<T> = std::result::Result<T, RustisError>;

#[derive(Error, Debug)]
enum RustisError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

fn handle_message(buf: &[u8]) {
    log::info!("Data: {:?}", String::from_utf8_lossy(buf));
    match buf {
        b"PING\r\n" => {
            log::info!("Received PING");
        }
        _ => {
            log::warn!("Unknown command");
        }
    }
}

fn main() -> Result<()> {
    env_logger::init();
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    listener.set_nonblocking(true)?;

    let mut connections: Vec<TcpStream> = vec![];

    loop {
        let mut poll_fds = Vec::with_capacity(1 + connections.len());
        poll_fds.push(PollFd::new(listener.as_fd(), PollFlags::POLLIN));

        for conn in &connections {
            poll_fds.push(PollFd::new(conn.as_fd(), PollFlags::POLLIN));
        }

        match poll(&mut poll_fds, PollTimeout::from(1000u16)) {
            Ok(n) if n > 0 => {
                log::debug!("Events: {}", n);

                // New connections
                if let Some(events) = poll_fds[0].revents() {
                    if events.contains(PollFlags::POLLIN) {
                        match listener.accept() {
                            Ok((stream, addr)) => {
                                log::info!("Accepted connection from: {}", addr);
                                stream.set_nonblocking(true)?;
                                connections.push(stream);
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                log::debug!("Accept would block");
                                continue;
                            }
                            Err(e) => {
                                log::error!("Accept error: {}", e);
                            }
                        }
                    }
                }

                // Existing connections
                connections.retain(|mut conn| {
                    let mut buf = [0; 1024];
                    match conn.read(&mut buf) {
                        Ok(0) => {
                            log::info!("Connection closed");
                            false // Remove this connection
                        }
                        Ok(n) => {
                            log::info!("Read {} bytes", n);
                            handle_message(&buf[..n]);
                            let response = b"+PONG\r\n";

                            match conn.write_all(response) {
                                Ok(_) => {
                                    log::info!("Sent PONG");
                                }
                                Err(e) => {
                                    log::error!("Error sending PONG: {}", e);
                                }
                            }

                            true
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            log::debug!("Read would block");
                            true
                        }
                        Err(e) => {
                            log::error!("Read error: {}", e);
                            false // Remove this connection
                        }
                    }
                });
            }
            Ok(_) => {
                log::trace!("Poll timeout");
                continue;
            }
            Err(e) => {
                log::error!("error: {}", e);
                break;
            }
        }
    }

    Ok(())
}
