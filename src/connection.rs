use crate::{error::RustisError, parse::parse_input, resp::RESPData, Result};
use nix::poll::PollFlags;
use std::{
    io::{ErrorKind, Read, Write},
    net::TcpStream,
    os::{fd::BorrowedFd, unix::io::AsFd},
};

pub(crate) struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub(crate) fn new(stream: TcpStream) -> Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Connection { stream })
    }

    pub(crate) fn as_fd(&self) -> BorrowedFd {
        self.stream.as_fd()
    }

    pub(crate) fn process_event(mut self, event: Option<&PollFlags>) -> Result<Self> {
        if let Some(revents) = event {
            if revents.contains(PollFlags::POLLIN) {
                let mut buf = [0; 1024];
                match self.stream.read(&mut buf) {
                    Ok(0) => {
                        log::info!("Connection closed");
                        return Err(RustisError::ClientDisconnected);
                    }
                    Ok(n) => {
                        log::info!("Read {} bytes", n);
                        if log::log_enabled!(log::Level::Debug) {
                            let data = String::from_utf8_lossy(&buf[..n]);
                            log::debug!("Data: {}", data);
                        }

                        match parse_input(&buf[..n]) {
                            Ok(RESPData::SimpleString(s)) => match s {
                                b"PING" => {
                                    log::debug!("Received PING");
                                    self.stream.write_all(b"+PONG\r\n")?;
                                }
                                _ => {
                                    let data = String::from_utf8_lossy(&buf[..n]);
                                    log::error!("Unknown command: {}", data);
                                    return Err(RustisError::UnknownCommand(data.to_string()));
                                }
                            },
                            Ok(RESPData::Array(array)) => match &array[..] {
                                [RESPData::BulkString(s)] if s.eq_ignore_ascii_case(b"ping") => {
                                    log::debug!("Received PING");
                                    self.stream.write_all(b"+PONG\r\n")?;
                                }
                                [RESPData::BulkString(s)] if s.eq_ignore_ascii_case(b"command") => {
                                    log::debug!("Received COMMAND");
                                    self.stream.write_all(b"+OK\r\n")?;
                                }
                                [RESPData::BulkString(s), RESPData::BulkString(msg)]
                                    if s.eq_ignore_ascii_case(b"echo") =>
                                {
                                    log::debug!("Received ECHO");
                                    self.stream.write_all(b"+")?;
                                    self.stream.write_all(msg)?;
                                    self.stream.write_all(b"\r\n")?;
                                }
                                _ => todo!(),
                            },
                            Ok(_) => todo!(),
                            Err(e) => {
                                log::error!("Error parsing input: {}", e);
                                let data = String::from_utf8_lossy(&buf[..n]);
                                return Err(RustisError::InvalidInput(data.to_string()));
                            }
                        };
                    }
                    Err(e) if e.kind() == ErrorKind::WouldBlock => {
                        log::debug!("Read would block");
                    }
                    Err(e) => {
                        log::error!("Read error: {}", e);
                        return Err(RustisError::ReadError);
                    }
                }
            }
        }
        Ok(self)
    }
}
