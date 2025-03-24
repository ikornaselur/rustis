mod common;

use common::TestServer;
use redis::Commands;

#[test]
fn test_key_from_loaded_rdb() {
    let server = TestServer::start(Some(vec![
        "--dir",
        "./tests/files",
        "--dbfilename",
        "simple.rdb",
    ]));
    let client = redis::Client::open(server.connection_string()).unwrap();
    let mut conn = client.get_connection().unwrap();

    let result: String = conn.get("mykey").unwrap();
    assert_eq!(result, "myvalue");
}

#[test]
fn test_expiry_from_rdb() {
    let server = TestServer::start(Some(vec![
        "--dir",
        "./tests/files",
        "--dbfilename",
        "expiry.rdb",
    ]));
    let client = redis::Client::open(server.connection_string()).unwrap();
    let mut conn = client.get_connection().unwrap();

    // NOTE: I set long-expiry to a random value.. which turned out to be Nov 20 2286.
    // If this test is failing and it's after that date, then please regenerate the file with a
    // later expiry.
    let result: String = conn.get("long-expiry").unwrap();
    assert_eq!(result, "baz");

    let result: Option<String> = conn.get("short-expiry").unwrap();
    assert_eq!(result, None);
}
