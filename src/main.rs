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

const POLL_TIMEOUT: u16 = 1000; // Milliseconds

fn accept_new_connections(listener: &TcpListener, connections: &mut Vec<TcpStream>) -> Result<()> {
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                log::info!("Accepted connection from: {}", addr);
                stream.set_nonblocking(true)?;
                connections.push(stream);
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

fn process_existing_connections(connections: &mut Vec<TcpStream>, events: &[Option<PollFlags>]) {
    let new_connections: Vec<_> = connections
        .drain(..)
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
                            log::info!("Data: {:?}", String::from_utf8_lossy(&buf));
                            let response = b"+PONG\r\n";
                            if let Err(e) = conn.write_all(response) {
                                log::error!("Error sending PONG: {}", e);
                                return None;
                            } else {
                                log::info!("Sent PONG");
                            }
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
    *connections = new_connections;
}

fn main() -> Result<()> {
    env_logger::init();
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    listener.set_nonblocking(true)?;
    let mut connections: Vec<TcpStream> = Vec::new();

    loop {
        let (listener_event, connection_events) = {
            // We need to construct the poll_fds every time to work with the borrow checker.
            // The overhead of this should be fine, though by the time we get into some HARD
            // BENCHMARKING ... we'll revisit this, maybe? Probably not.
            let mut poll_fds = Vec::with_capacity(1 + connections.len());
            poll_fds.push(PollFd::new(listener.as_fd(), PollFlags::POLLIN));
            for conn in &connections {
                poll_fds.push(PollFd::new(conn.as_fd(), PollFlags::POLLIN));
            }

            match poll(&mut poll_fds, PollTimeout::from(POLL_TIMEOUT)) {
                Ok(n) if n > 0 => {
                    log::debug!("Events: {}", n);
                    let listener_event = poll_fds[0].revents();
                    // Since we have some events, let's map them into an event list that matches
                    // the listener + connections, so we can process those
                    let connection_events = poll_fds
                        .iter()
                        .skip(1)
                        .map(|pfd| pfd.revents())
                        .collect::<Vec<_>>();
                    (listener_event, connection_events)
                }
                Ok(_) => {
                    log::trace!("Poll timeout");
                    (None, vec![])
                }
                Err(e) => {
                    log::error!("Poll error: {}", e);
                    break;
                }
            }
        };

        if let Some(revents) = listener_event {
            if revents.contains(PollFlags::POLLIN) {
                accept_new_connections(&listener, &mut connections)?;
            }
        }

        process_existing_connections(&mut connections, &connection_events);
    }

    Ok(())
}
