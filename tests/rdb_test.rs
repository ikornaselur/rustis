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
