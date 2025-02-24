use crate::{
    client_error,
    database::{DBValue, DATABASES},
    error::RustisError,
    parse::parse_input,
    resp::RESPData,
    Result,
};
use nix::poll::PollFlags;
use std::{
    io::{ErrorKind, Read, Write},
    net::TcpStream,
    os::{fd::BorrowedFd, unix::io::AsFd},
    time::{SystemTime, UNIX_EPOCH},
};

const BUFFER_SIZE: usize = 32 * 1024;

pub(crate) struct Connection {
    stream: TcpStream,
}

fn now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
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
        let Some(revents) = event else {
            return Ok(self);
        };

        if !revents.contains(PollFlags::POLLIN) {
            return Ok(self);
        }

        // TODO: Is there a performance improvement to be done here? Reuse a buffer?
        let mut buf = [0; BUFFER_SIZE];
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

                match self.process_input(&buf[..n]) {
                    Ok(()) => {}
                    Err(RustisError::ClientError(msg)) => {
                        log::info!("Client error: {}", msg);
                        self.write_error(msg.as_bytes())?;
                        return Ok(self);
                    }
                    Err(e) => {
                        log::error!("Error processing input: {}", e);
                        return Err(e);
                    }
                }
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

    fn process_input(&mut self, buf: &[u8]) -> Result<()> {
        match parse_input(buf) {
            Ok(RESPData::SimpleString(s)) => self.process_simple_string(s)?,
            Ok(RESPData::Array(array)) => self.process_array(&array[..])?,
            Ok(_) => todo!(),
            Err(e) => {
                log::error!("Error parsing input: {}", e);
                let data = String::from_utf8_lossy(buf);
                return Err(RustisError::InvalidInput(data.to_string()));
            }
        };
        Ok(())
    }

    /// Helper function to write an error to the client
    ///
    /// The function will write "-ERR " followed by the error message and a CRLF
    fn write_error(&mut self, error: &[u8]) -> Result<()> {
        self.stream.write_all(b"-ERR ")?;
        self.stream.write_all(error)?;
        self.stream.write_all(b"\r\n")?;
        Ok(())
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
                self.handle_command()?;
            }
            [RESPData::BulkString(s), args @ ..] if s.eq_ignore_ascii_case(b"echo") => {
                self.handle_echo(args)?;
            }
            [RESPData::BulkString(s), args @ ..] if s.eq_ignore_ascii_case(b"set") => {
                self.handle_set(args)?;
            }
            [RESPData::BulkString(s), args @ ..] if s.eq_ignore_ascii_case(b"get") => {
                self.handle_get(args)?;
            }
            // ERR unknown command '<command>', with args beginning with: ???
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

        let Some((RESPData::BulkString(key), args)) = args.split_first() else {
            return client_error!("wrong number of arguments for 'set' command");
        };
        let Some((RESPData::BulkString(value), args)) = args.split_first() else {
            return client_error!("wrong number of arguments for 'set' command");
        };

        let mut ttl = None;

        let mut iter = args.iter();
        while let Some(arg) = iter.next() {
            match arg {
                RESPData::BulkString(s) if s.eq_ignore_ascii_case(b"nx") => {
                    log::debug!("NX option");
                    todo!();
                }
                RESPData::BulkString(s) if s.eq_ignore_ascii_case(b"xx") => {
                    log::debug!("XX option");
                    todo!();
                }
                RESPData::BulkString(s) if s.eq_ignore_ascii_case(b"ex") => {
                    log::trace!("EX option");
                    // We need to get the value for EX
                    if let Some(RESPData::BulkString(s)) = iter.next() {
                        let s = String::from_utf8_lossy(s);
                        let Ok(s) = s.parse::<u128>() else {
                            return client_error!("value is not an integer or out of range");
                        };
                        log::trace!("Seconds: {}", s);
                        ttl = Some(now() + s * 1000);
                    } else {
                        return client_error!("value is not an integer or out of range");
                    }
                }
                RESPData::BulkString(s) if s.eq_ignore_ascii_case(b"px") => {
                    log::trace!("PX option");
                    // We need to get the value for PX
                    if let Some(RESPData::BulkString(ms)) = iter.next() {
                        let ms = String::from_utf8_lossy(ms);
                        let Ok(ms) = ms.parse::<u128>() else {
                            return client_error!("value is not an integer or out of range");
                        };
                        log::trace!("Milliseconds: {}", ms);
                        ttl = Some(now() + ms);
                    } else {
                        return client_error!("value is not an integer or out of range");
                    }
                }
                RESPData::BulkString(s) if s.eq_ignore_ascii_case(b"exat") => {
                    log::trace!("EXAT option");
                    // We need to get the value for EXAT
                    if let Some(RESPData::BulkString(ts)) = iter.next() {
                        let ts = String::from_utf8_lossy(ts);
                        let Ok(ts) = ts.parse::<u128>() else {
                            return client_error!("value is not an integer or out of range");
                        };
                        log::trace!("Timestamp: {}", ts);
                        ttl = Some(ts * 1000);
                    } else {
                        self.write_error(b"value is not an integer or out of range")?;
                        return Ok(());
                    }
                }
                RESPData::BulkString(s) if s.eq_ignore_ascii_case(b"pxat") => {
                    log::trace!("PXAT option");
                    // We need to get the value for PXAT
                    if let Some(RESPData::BulkString(ts)) = iter.next() {
                        let ts = String::from_utf8_lossy(ts);
                        let Ok(ts) = ts.parse::<u128>() else {
                            return client_error!("value is not an integer or out of range");
                        };
                        log::trace!("Timestamp: {}", ts);
                        ttl = Some(ts);
                    } else {
                        return client_error!("value is not an integer or out of range");
                    }
                }
                RESPData::BulkString(s) if s.eq_ignore_ascii_case(b"keepttl") => {
                    log::debug!("KEEPTTL option");
                    todo!();
                }
                _ => {
                    return client_error!("syntax error");
                }
            }
        }

        let mut dbs = DATABASES.write().unwrap();
        if let Some(db) = dbs.get_mut(0) {
            if log::log_enabled!(log::Level::Debug) {
                let key = String::from_utf8_lossy(key);
                let value = String::from_utf8_lossy(value);
                log::debug!("Setting key: {:?}, value: {:?}", key, value);
            }
            db.insert(key.to_vec(), DBValue::new(value.to_vec(), ttl));
        }

        log::trace!("Responding with OK");
        self.stream.write_all(b"+OK\r\n")?;

        Ok(())
    }

    fn handle_get(&mut self, args: &[RESPData]) -> Result<()> {
        log::debug!("Received GET");

        let [RESPData::BulkString(key)] = &args else {
            todo!()
        };

        let mut dbs = DATABASES.write().unwrap();
        if let Some(db) = dbs.get_mut(0) {
            if let Some(DBValue { value, ttl }) = db.get(&key.to_vec()) {
                if let Some(ttl) = ttl {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
                    if now > *ttl {
                        log::debug!("Key has expired");
                        self.stream.write_all(b"$-1\r\n")?;
                        db.remove(&key.to_vec());
                        return Ok(());
                    }
                }
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
