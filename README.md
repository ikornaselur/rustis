# Rustis

Following the ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis) on CodeCrafters.

This implementation is single-threaded, synchronous and uses an event loop.
Thought it might be a good way to learn more about polling and event loops.

## What's supported?

Barely anything!

So far.. 

* PING
* ECHO message
* SET key value [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds] [NX|XX] [KEEPTTL]
* GET key
* CONFIG GET key

## Usage

```
Usage: redis-starter-rust [OPTIONS]

Options:
  -d, --dir <DIR>                The path to the directory where the RDB file is stored [default: /tmp/redis-data]
      --dbfilename <DBFILENAME>  [default: dump.rdb]
  -H, --host <HOST>              [default: 127.0.0.1]
  -p, --port <PORT>              [default: 6379]
  -h, --help                     Print help
  -V, --version                  Print version
```
