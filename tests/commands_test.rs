mod common;

use common::TestServer;
use redis::Commands;

#[test]
fn test_ping() {
    let server = TestServer::start(None);
    let client = redis::Client::open(server.connection_string()).unwrap();
    let mut conn = client.get_connection().unwrap();

    let pong: String = conn.ping().unwrap();
    assert_eq!(pong, "PONG");
}

#[test]
fn test_set_and_get() {
    let server = TestServer::start(None);
    let client = redis::Client::open(server.connection_string()).unwrap();
    let mut conn = client.get_connection().unwrap();

    let _: () = conn.set("my_key", "my_value").unwrap();

    let result: String = conn.get("my_key").unwrap();

    assert_eq!(result, "my_value");
}

#[test]
fn test_echo() {
    let server = TestServer::start(None);
    let client = redis::Client::open(server.connection_string()).unwrap();
    let mut conn = client.get_connection().unwrap();

    let result: String = redis::cmd("ECHO")
        .arg("Hello, world!")
        .query(&mut conn)
        .unwrap();

    assert_eq!(result, "Hello, world!");
}
