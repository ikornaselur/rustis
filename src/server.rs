use nix::poll::{poll, PollFd, PollFlags, PollTimeout};
use std::io;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::AsFd;

use crate::parse::parse_input;
use crate::resp::RESPData;
use crate::Result;

const POLL_TIMEOUT: u16 = 1000;

pub struct Server {
    listener: TcpListener,
    connections: Vec<TcpStream>,
}

impl Server {
    pub fn new(addr: &str) -> Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Server {
            listener,
            connections: Vec::new(),
        })
    }

    pub fn run_once(&mut self) -> Result<()> {
        // We need to keep track of how many connections exist when we poll, so that we can only
        // drain those when we handle existing connections
        let polled_count = self.connections.len();

        let (listener_event, connection_events) = {
            let mut poll_fds = Vec::with_capacity(1 + polled_count);
            poll_fds.push(PollFd::new(self.listener.as_fd(), PollFlags::POLLIN));
            for conn in &self.connections {
                poll_fds.push(PollFd::new(conn.as_fd(), PollFlags::POLLIN));
            }
            match poll(&mut poll_fds, PollTimeout::from(POLL_TIMEOUT)) {
                Ok(n) => {
                    log::trace!("Events: {}", n);
                    let listener_event = poll_fds[0].revents();
                    let connection_events = poll_fds
                        .iter()
                        .skip(1)
                        .map(|pfd| pfd.revents())
                        .collect::<Vec<_>>();
                    (listener_event, connection_events)
                }
                Err(e) => {
                    log::error!("Poll error: {}", e);
                    panic!("Poll error: {}", e);
                }
            }
        };

        if let Some(revents) = listener_event {
            if revents.contains(PollFlags::POLLIN) {
                self.accept_new_connections()?;
            }
        }

        self.process_existing_connections(&connection_events, polled_count);
        Ok(())
    }

    pub fn run_forever(&mut self) -> Result<()> {
        loop {
            self.run_once()?;
        }
    }

    fn accept_new_connections(&mut self) -> Result<()> {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    log::info!("Accepted connection from: {}", addr);
                    stream.set_nonblocking(true)?;
                    self.connections.push(stream);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    log::error!("Accept error: {}", e);
                    break;
                }
            }
        }
        Ok(())
    }

    fn process_existing_connections(&mut self, events: &[Option<PollFlags>], polled_count: usize) {
        let mut polled_conns: Vec<TcpStream> = self.connections.drain(..polled_count).collect();

        polled_conns = polled_conns
            .into_iter()
            .zip(events.iter())
            .filter_map(|(mut conn, event)| {
                if let Some(revents) = event {
                    if revents.contains(PollFlags::POLLIN) {
                        let mut buf = [0; 1024];
                        match conn.read(&mut buf) {
                            Ok(0) => {
                                log::info!("Connection closed");
                                return None;
                            }
                            Ok(n) => {
                                log::info!("Read {} bytes", n);
                                let data = String::from_utf8_lossy(&buf[..n]);
                                log::debug!("Data: {}", data);
                                match parse_input(&data) {
                                    Ok(RESPData::SimpleString(s)) => match s {
                                        "PING" => {
                                            log::debug!("Received PING");
                                            conn.write_all(b"+PONG\r\n").unwrap();
                                        }
                                        _ => {
                                            log::error!("Unknown command: {}", s);
                                            return None;
                                        }
                                    },
                                    Ok(RESPData::Array(array)) => match &array[..] {
                                        [RESPData::BulkString(s)]
                                            if s.eq_ignore_ascii_case("ping") =>
                                        {
                                            log::debug!("Received PING");
                                            conn.write_all(b"+PONG\r\n").unwrap();
                                        }
                                        [RESPData::BulkString(s)]
                                            if s.eq_ignore_ascii_case("command") =>
                                        {
                                            log::debug!("Received COMMAND");
                                            conn.write_all(b"+OK\r\n").unwrap();
                                        }
                                        [RESPData::BulkString(s), RESPData::BulkString(msg)]
                                            if s.eq_ignore_ascii_case("echo") =>
                                        {
                                            log::debug!("Received ECHO");
                                            conn.write_all(b"+").unwrap();
                                            conn.write_all(msg.as_bytes()).unwrap();
                                            conn.write_all(b"\r\n").unwrap();
                                        }
                                        _ => todo!(),
                                    },
                                    Ok(_) => todo!(),
                                    Err(e) => {
                                        log::error!("Error parsing input: {}", e);
                                        return None;
                                    }
                                };
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                log::debug!("Read would block");
                            }
                            Err(e) => {
                                log::error!("Read error: {}", e);
                                return None;
                            }
                        }
                    }
                }
                Some(conn)
            })
            .collect();

        self.connections = polled_conns
            .into_iter()
            .chain(self.connections.drain(..))
            .collect();
    }
}
