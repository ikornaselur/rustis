use crate::{database::DATABASES, error::RustisError, parse::parse_input, resp::RESPData, Result};
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
        let revents = match event {
            Some(e) => e,
            None => return Ok(self),
        };

        if !revents.contains(PollFlags::POLLIN) {
            return Ok(self);
        }

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
                    Ok(RESPData::SimpleString(s)) => self.process_simple_string(s)?,
                    Ok(RESPData::Array(array)) => self.process_array(&array[..])?,
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
        Ok(self)
    }

    fn process_simple_string(&mut self, string: &[u8]) -> Result<()> {
        match string {
            b"PING" => {
                log::debug!("Received PING");
                self.stream.write_all(b"+PONG\r\n")?;
            }
            buf => {
                let data = String::from_utf8_lossy(buf);
                log::error!("Unknown command: {}", data);
                return Err(RustisError::UnknownCommand(data.to_string()));
            }
        }

        Ok(())
    }

    fn process_array(&mut self, array: &[RESPData]) -> Result<()> {
        match array {
            [RESPData::BulkString(s)] if s.eq_ignore_ascii_case(b"ping") => self.handle_ping()?,
            [RESPData::BulkString(s)] if s.eq_ignore_ascii_case(b"command") => {
                self.handle_command()?
            }
            [RESPData::BulkString(s), args @ ..] if s.eq_ignore_ascii_case(b"echo") => {
                self.handle_echo(args)?
            }
            [RESPData::BulkString(s), args @ ..] if s.eq_ignore_ascii_case(b"set") => {
                self.handle_set(args)?
            }
            [RESPData::BulkString(s), args @ ..] if s.eq_ignore_ascii_case(b"get") => {
                self.handle_get(args)?
            }
            _ => todo!(),
        }

        Ok(())
    }

    fn handle_ping(&mut self) -> Result<()> {
        log::debug!("Received PING");
        self.stream.write_all(b"+PONG\r\n")?;
        Ok(())
    }

    fn handle_command(&mut self) -> Result<()> {
        log::debug!("Received COMMAND");
        self.stream.write_all(b"+OK\r\n")?;
        Ok(())
    }

    fn handle_echo(&mut self, args: &[RESPData]) -> Result<()> {
        log::debug!("Received ECHO");
        match args {
            [RESPData::BulkString(msg)] => {
                self.stream.write_all(b"+")?;
                self.stream.write_all(msg)?;
                self.stream.write_all(b"\r\n")?;
            }
            // Are multiple args supported?
            _ => todo!(),
        }

        Ok(())
    }

    fn handle_set(&mut self, args: &[RESPData]) -> Result<()> {
        log::debug!("Received SET");

        let (key, value) = match (&args[0], &args[1]) {
            (RESPData::BulkString(k), RESPData::BulkString(v)) => (k, v),
            _ => todo!(),
        };

        let mut dbs = DATABASES.write().unwrap();
        if let Some(db) = dbs.get_mut(0) {
            log::debug!("Setting key: {:?}, value: {:?}", key, value);
            db.insert(key.to_vec(), value.to_vec());
        }

        self.stream.write_all(b"+OK\r\n")?;

        Ok(())
    }

    fn handle_get(&mut self, args: &[RESPData]) -> Result<()> {
        log::debug!("Received GET");

        let key = match &args {
            [RESPData::BulkString(k)] => k,
            _ => todo!(),
        };

        let mut dbs = DATABASES.write().unwrap();
        if let Some(db) = dbs.get_mut(0) {
            if let Some(value) = db.get(&key.to_vec()) {
                log::debug!("Found value: {:?}", value);
                self.stream.write_all(b"$")?;
                self.stream.write_all(value.len().to_string().as_bytes())?;
                self.stream.write_all(b"\r\n")?;
                self.stream.write_all(value)?;
                self.stream.write_all(b"\r\n")?;
            } else {
                log::debug!("Key not found");
                self.stream.write_all(b"$-1\r\n")?;
            }
        }

        Ok(())
    }
}
