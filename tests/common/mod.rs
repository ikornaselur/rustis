use std::{
    io::{BufRead, BufReader},
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

fn get_binary_path() -> PathBuf {
    let output = Command::new("cargo")
        .arg("build")
        .arg("--release")
        .output()
        .expect("Failed to compile project");

    if !output.status.success() {
        panic!(
            "Compilation failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    PathBuf::from("target/release/redis-starter-rust")
}

pub struct TestServer {
    port: u16,
    child: Child,
}

impl TestServer {
    pub fn start() -> Self {
        let binary_path = get_binary_path();

        // Let's bind to a random free port and use that
        // XXX: A potential race condition? If it ever happens, I'll just deal with it then
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to a free port");
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let mut child = Command::new(binary_path)
            .args(["--port", &port.to_string()])
            .env("RUST_LOG", "trace")
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start Redis server");

        let stderr = child.stderr.take().expect("Failed to capture stderr");

        let ready_flag = Arc::new(Mutex::new(false));
        let ready_flag_clone = Arc::clone(&ready_flag);

        // Spawn a thread to monitor stderr (default for env_logger) for the "Starting event loop",
        // so we know the server is ready to accept connections..
        thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines().map_while(Result::ok) {
                println!("[SERVER LOG] {}", line);

                // Notify the main thread that the server is ready
                if line.contains("Starting event loop") {
                    let mut ready = ready_flag_clone.lock().unwrap();
                    *ready = true;
                }
            }
        });

        // Wait for the server to be ready
        let start_time = Instant::now();
        loop {
            let ready = ready_flag.lock().unwrap();
            if *ready {
                break;
            }
            if start_time.elapsed() > Duration::from_secs(5) {
                panic!("Test server failed to start within 5 seconds.");
            }
            drop(ready);
            thread::sleep(Duration::from_millis(100));
        }

        Self { child, port }
    }

    pub fn connection_string(&self) -> String {
        format!("redis://127.0.0.1:{}", self.port)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}
