use crate::{
    database::{DBValue, DATABASES},
    error::RustisError,
    parsers,
    resp::RESPData,
    Config, Result,
};
use nix::poll::PollFlags;
use std::{
    cell::RefCell,
    io::{ErrorKind, Read, Write},
    net::TcpStream,
    os::{
        fd::BorrowedFd,
        unix::io::{AsFd, AsRawFd},
    },
    rc::Rc,
    time::{SystemTime, UNIX_EPOCH},
};

const BUFFER_SIZE: usize = 32 * 1024;

const CRLF: &[u8] = b"\r\n";
const NULL: &[u8] = b"$-1\r\n";
const EMPTY_ARRAY: &[u8] = b"*0\r\n";
const OK: &[u8] = b"+OK\r\n";

pub(crate) struct Connection {
    stream: TcpStream,
    config: Rc<RefCell<Config>>,
}

fn now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn parse_u128_arg<'a, I>(iter: &mut I) -> Result<u128>
where
    I: Iterator<Item = &'a RESPData<'a>>,
{
    if let Some(RESPData::BulkString(raw)) = iter.next() {
        let s = String::from_utf8_lossy(raw);
        let Ok(s) = s.parse::<u128>() else {
            return client_error!("value is not an integer or out of range");
        };
        Ok(s)
    } else {
        client_error!("syntax error")
    }
}

impl Connection {
    pub(crate) fn new(stream: TcpStream, config: Rc<RefCell<Config>>) -> Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Connection { stream, config })
    }

    pub(crate) fn as_fd(&self) -> BorrowedFd {
        self.stream.as_fd()
    }

    pub(crate) fn as_raw_fd(&self) -> i32 {
        self.stream.as_raw_fd()
    }

    pub(crate) fn process_event(mut self, event: Option<&PollFlags>) -> Result<Self> {
        let Some(revents) = event else {
            return Ok(self);
        };

        if !revents.contains(PollFlags::POLLIN) {
            return Ok(self);
        }

        // TODO: Is there a performance improvement to be done here? Reuse a buffer?
        let mut buf = vec![0; BUFFER_SIZE].into_boxed_slice();
        match self.stream.read(&mut buf) {
            Ok(0) => {
                log::info!("Connection closed");
                return Err(RustisError::ClientDisconnected);
            }
            Ok(n) => {
                log::trace!("Read {} bytes", n);
                log::trace!("Data: {:?}", String::from_utf8_lossy(&buf[..n]));

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
        for data in parsers::resp_data::parse(buf)? {
            match data {
                RESPData::SimpleString(s) => self.process_simple_string(s)?,
                RESPData::Array(array) => self.process_array(&array[..])?,
                _ => todo!(),
            }
        }
        Ok(())
    }

    /// Helper function to write an error to the client
    ///
    /// The function will write "-ERR " followed by the error message and a CRLF
    fn write_error(&mut self, error: &[u8]) -> Result<()> {
        self.stream.write_all(b"-ERR ")?;
        self.stream.write_all(error)?;
        self.stream.write_all(CRLF)?;
        Ok(())
    }

    /// Helper function to write an Array to the client
    fn write_array(&mut self, array: Vec<&[u8]>) -> Result<()> {
        let element_count = array.len();

        let mut buf = Vec::new();
        buf.push(b'*');
        write!(buf, "{}\r\n", element_count)?;

        array.iter().for_each(|element| {
            buf.push(b'$');
            write!(buf, "{}\r\n", element.len()).unwrap();
            buf.extend_from_slice(element);
            buf.extend_from_slice(CRLF);
        });

        self.stream.write_all(&buf)?;

        Ok(())
    }

    /// Helper function to write a BulkString
    fn write_bulk_string(&mut self, data: &[u8]) -> Result<()> {
        let mut buf = Vec::new();
        buf.push(b'$');
        write!(buf, "{}\r\n", data.len()).unwrap();
        buf.extend_from_slice(data);
        buf.extend_from_slice(CRLF);

        self.stream.write_all(&buf)?;

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
        if let Some(RESPData::BulkString(s)) = array.first() {
            match s.to_ascii_uppercase().as_slice() {
                b"PING" => self.handle_ping()?,
                b"COMMAND" => self.handle_command()?,
                b"ECHO" => self.handle_echo(&array[1..])?,
                b"SET" => self.handle_set(&array[1..])?,
                b"GET" => self.handle_get(&array[1..])?,
                b"CONFIG" => self.handle_config(&array[1..])?,
                b"CLIENT" => self.handle_client(&array[1..])?,
                _ => todo!(),
            }
        } else {
            todo!()
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
        self.stream.write_all(OK)?;
        Ok(())
    }

    fn handle_echo(&mut self, args: &[RESPData]) -> Result<()> {
        log::debug!("Received ECHO");
        match args {
            [RESPData::BulkString(msg)] => {
                let mut buf = Vec::new();
                buf.push(b'+');
                buf.extend_from_slice(msg);
                buf.extend_from_slice(CRLF);

                self.stream.write_all(&buf)?;
            }
            // Are multiple args supported?
            _ => todo!(),
        }

        Ok(())
    }

    fn handle_config(&mut self, args: &[RESPData]) -> Result<()> {
        log::debug!("Received CONFIG");

        let Some((RESPData::BulkString(subcommand), args)) = args.split_first() else {
            return client_error!("wrong number of arguments for 'config' command");
        };

        match subcommand.to_ascii_uppercase().as_slice() {
            b"GET" => self.handle_config_get(args)?,
            b"SET" => self.handle_config_set(args)?,
            _ => todo!(),
        }

        Ok(())
    }

    fn handle_client(&mut self, _args: &[RESPData]) -> Result<()> {
        log::debug!("Received CLIENT");

        // TODO: Set this somewhere.. Now we just tell the client that we've set this
        self.stream.write_all(OK)?;

        Ok(())
    }

    fn handle_config_get(&mut self, args: &[RESPData]) -> Result<()> {
        log::debug!("Received CONFIG GET");
        let Some((RESPData::BulkString(key), _)) = args.split_first() else {
            return client_error!("wrong number of arguments for 'config|get' command");
        };

        // TODO: Config support getting multiple config values at the same time, for now we just
        // support one

        match key.to_ascii_uppercase().as_slice() {
            b"DBFILENAME" => {
                let dbfilename = {
                    let config = self.config.borrow();
                    config.dbfilename.clone()
                };

                self.write_array(vec![&b"dbfilename"[..], dbfilename.as_bytes()])?;
            }
            b"DIR" => {
                let dir = {
                    let config = self.config.borrow();
                    config.dir.clone()
                };

                self.write_array(vec![&b"dir"[..], &dir.as_bytes()])?;
            }
            _ => {
                self.stream.write_all(EMPTY_ARRAY)?;
            }
        }
        Ok(())
    }

    fn handle_config_set(&mut self, _args: &[RESPData]) -> Result<()> {
        todo!();
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
        let mut nx = false;
        let mut xx = false;
        let mut keep_ttl = false;

        let mut iter = args.iter();
        while let Some(RESPData::BulkString(arg)) = iter.next() {
            match arg.to_ascii_uppercase().as_slice() {
                b"NX" => {
                    log::debug!("NX option");
                    nx = true;
                }
                b"XX" => {
                    log::debug!("XX option");
                    xx = true;
                }
                b"EX" => {
                    log::trace!("EX option");
                    if ttl.is_some() {
                        return client_error!("syntax error");
                    }
                    ttl = Some(now() + parse_u128_arg(&mut iter)? * 1000);
                }
                b"PX" => {
                    log::trace!("PX option");
                    if ttl.is_some() {
                        return client_error!("syntax error");
                    }
                    ttl = Some(now() + parse_u128_arg(&mut iter)?);
                }
                b"EXAT" => {
                    log::trace!("EXAT option");
                    if ttl.is_some() {
                        return client_error!("syntax error");
                    }
                    ttl = Some(parse_u128_arg(&mut iter)? * 1000);
                }
                b"PXAT" => {
                    log::trace!("PXAT option");
                    if ttl.is_some() {
                        return client_error!("syntax error");
                    }
                    ttl = Some(parse_u128_arg(&mut iter)?);
                }
                b"KEEPTTL" => {
                    log::debug!("KEEPTTL option");
                    keep_ttl = true;
                }
                _ => {
                    return client_error!("syntax error");
                }
            }
        }

        // These are mutually exclusive
        if nx && xx {
            return client_error!("syntax error");
        }

        let mut dbs = DATABASES.write().unwrap();
        if let Some(db) = dbs.get_mut(0) {
            log::debug!(
                "SET {:?} = {:?}",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(value)
            );

            // If NX is set, then we only set the key if it does not already exist
            if nx && db.contains_key(&key.to_vec()) {
                log::trace!("Key already exists");
                self.stream.write_all(NULL)?;
                return Ok(());
            }

            // If XX is set, then we only set the key if it *does* already exist
            if xx && !db.contains_key(&key.to_vec()) {
                log::trace!("Key does not exist");
                self.stream.write_all(NULL)?;
                return Ok(());
            }

            // If keep_ttl is set, we need to check if it was previously set to reuse
            if keep_ttl {
                if let Some(DBValue { ttl: old_ttl, .. }) = db.get(&key.to_vec()) {
                    ttl = *old_ttl;
                }
            }

            db.insert(key.to_vec(), DBValue::new(value.to_vec(), ttl));
        }
        // TODO: Else?

        log::trace!("Responding with OK");
        self.stream.write_all(OK)?;

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
                        self.stream.write_all(NULL)?;
                        db.remove(&key.to_vec());
                        return Ok(());
                    }
                }
                log::debug!("Found value: {:?}", value);
                self.write_bulk_string(value)?;
            } else {
                log::debug!("Key not found");
                self.stream.write_all(NULL)?;
            }
        }

        Ok(())
    }
}
