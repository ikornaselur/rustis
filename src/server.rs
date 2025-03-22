use crate::{connection::Connection, database::load_rdb, Config, Result};
use nix::{
    poll::{poll, PollFd, PollFlags, PollTimeout},
    unistd::{close, fork, ForkResult},
};
use std::{
    cell::RefCell,
    io::ErrorKind,
    net::TcpListener,
    os::unix::io::{AsFd, AsRawFd},
    path::Path,
    process,
    rc::Rc,
    time::{Duration, Instant},
};

const POLL_TIMEOUT: u16 = 1000;

pub struct Server {
    listener: TcpListener,
    connections: Vec<Connection>,
    last_snapshot: Instant,
    snapshot_interval: Duration,
    config: Rc<RefCell<Config>>,
}

impl Server {
    pub fn new(config: Rc<RefCell<Config>>) -> Result<Self> {
        let (listener, snapshot_interval) = {
            let config = config.borrow();

            (
                TcpListener::bind(config.listen_addr())?,
                Duration::from_secs(config.snapshot_interval),
            )
        };

        listener.set_nonblocking(true)?;

        // Check if the dbfilename exists
        let db_path = {
            let config = config.borrow();
            format!("{}/{}", config.dir(), config.dbfilename())
        };
        if Path::new(&db_path).exists() {
            log::info!("Loading RDB file: {}", db_path);
            load_rdb(&db_path)?;
        } else {
            log::debug!("No RDB file found at: {}", db_path);
        }

        Ok(Server {
            listener,
            connections: Vec::new(),
            last_snapshot: Instant::now(),
            snapshot_interval,
            config,
        })
    }

    /// Run the event loop once
    ///
    /// This will:
    ///     * Poll for events on the listener, accepting new connections
    ///     * Poll for events on the existing connections, processing them
    ///     * Fork the process (at a configurable interval), save a snapshot and exit (the child)
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
                    return Err(e.into());
                }
            }
        };

        if let Some(revents) = listener_event {
            if revents.contains(PollFlags::POLLIN) {
                self.accept_new_connections()?;
            }
        }

        self.process_existing_connections(&connection_events, polled_count);

        if self.last_snapshot.elapsed() >= self.snapshot_interval {
            self.fork_and_save();
            self.last_snapshot = Instant::now();
        }

        Ok(())
    }

    pub fn run_forever(&mut self) -> Result<()> {
        log::debug!("Starting event loop");
        loop {
            self.run_once()?;
        }
    }

    fn fork_and_save(&self) {
        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                log::debug!("Saving snapshot");

                log::trace!("Closing all connections in forked snapshot saver");
                for conn in &self.connections {
                    let _ = close(conn.as_raw_fd());
                }
                let _ = close(self.listener.as_raw_fd());

                // TODO: Save snapshot

                process::exit(0);
            }
            Ok(ForkResult::Parent { child }) => {
                log::trace!("Forked child process with PID: {}", child);
            }
            Err(e) => {
                log::error!("Fork error: {}", e);
            }
        }
    }

    fn accept_new_connections(&mut self) -> Result<()> {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    log::info!("Accepted connection from: {}", addr);
                    self.connections
                        .push(Connection::new(stream, Rc::clone(&self.config))?);
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => {
                    log::error!("Accept error: {}", e);
                    break;
                }
            }
        }
        Ok(())
    }

    /// Process existing connections, removing any that have been closed
    ///
    /// We do this by:
    ///
    /// 1. Take the connections that were polled
    /// 2. Zip them with the events that were polled
    /// 3. Filter out any connections that have been closed
    /// 4. Put the remaining connections back into the connections list
    fn process_existing_connections(&mut self, events: &[Option<PollFlags>], polled_count: usize) {
        let mut polled_conns: Vec<Connection> = self.connections.drain(..polled_count).collect();

        polled_conns = polled_conns
            .into_iter()
            .zip(events.iter())
            .filter_map(|(conn, event)| conn.process_event(event.as_ref()).ok())
            .collect();

        self.connections = polled_conns
            .into_iter()
            .chain(self.connections.drain(..))
            .collect();
    }
}
