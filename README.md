[![Build Status](https://travis-ci.org/iFaceless/tinkv.svg?branch=master)](https://travis-ci.org/iFaceless/tinkv)
[![tinkv](http://meritbadge.herokuapp.com/tinkv)](https://crates.io/crates/tinkv)
[![License: MIT OR Apache-2.0](https://img.shields.io/crates/l/tinkv.svg)](#license)

# ![TinKV Logo](https://i.loli.net/2020/06/08/3hYVFNurxGoLei7.jpg)

[TinKV](https://github.com/iFaceless/tinkv) is a simple and fast key-value storage engine written in Rust. Inspired by [basho/bitcask](https://github.com/basho/bitcask), written after attending the [Talent Plan courses](https://github.com/pingcap/talent-plan). 

**Notes**:
- *Do not use it in production.*
- *Operations like set/remove/compact are not thread-safe currently.*

Happy hacking~

![Overview.jpg](https://i.loli.net/2020/06/25/zXkiSZ5fvjGtQDg.jpg)

![Engine.jpg](https://i.loli.net/2020/06/25/cEw9gsrDVJCzyTO.jpg)

# Features

- Embeddable (use `tinkv` as a library);
- Builtin CLI (`tinkv`);
- Builtin Redis compatible server;
- Predictable read/write performance.

# Usage
## As a library

```shell
$ cargo add tinkv
```

Full example usage can be found in [examples/basic.rs](./examples/basic.rs).

```rust
use pretty_env_logger;
use tinkv::{self, Store};

fn main() -> tinkv::Result<()> {
    pretty_env_logger::init();
    let mut store = Store::open("/path/to/tinkv")?;
    store.set("hello".as_bytes(), "tinkv".as_bytes())?;

    let value = store.get("hello".as_bytes())?;
    assert_eq!(value, Some("tinkv".as_bytes().to_vec()));

    store.remove("hello".as_bytes())?;

    let value_not_found = store.get("hello".as_bytes())?;
    assert_eq!(value_not_found, None);

    Ok(())
}
```

### Open with custom options

```rust
use pretty_env_logger;
use tinkv::{self, Store};

fn main() -> tinkv::Result<()> {
    let mut store = tinkv::OpenOptions::new()
        .max_data_file_size(1024 * 1024)
        .max_key_size(128)
        .max_value_size(128)
        .sync(true)
        .open(".tinkv")?;
    store.set("hello".as_bytes(), "world".as_bytes())?;
    Ok(())
}
```

### APIs
Public APIs of tinkv store are very easy to use:
| API                      |                   Description                                 |
|--------------------------|---------------------------------------------------------------|
|`Store::open(path)`       | Open a new or existing datastore. The directory must be writeable and readable for tinkv store.|`
|`tinkv::OpenOptions()`    | Open a new or existing datastore with custom options. |
|`store.get(key)`          | Get value by key from datastore.|
|`store.set(key, value)`   | Store a key value pair into datastore.|
|`store.remove(key, value)`| Remove a key from datastore.|
|`store.compact()`         | Merge data files into a more compact form. drop stale segments to release disk space. Produce hint files after compaction for faster startup.|
|`store.keys()`            | Return all the keys in database.|
|`store.len()`             | Return total number of keys in database.|
|`store.for_each(f: Fn(key, value) -> Result<bool>)`             | Iterate all keys in database and call function `f` for each entry.|
|`store.stas()`            | Get current statistics of database.|
|`store.sync()`            | Force any writes to datastore.|
|`store.close()`           | Close datastore, sync all pending writes to disk.|

### Run examples

```shell
$ RUST_LOG=trace cargo run --example basic
```

`RUST_LOG` level can be one of [`trace`, `debug`, `info`, `error`].

<details>
    <summary>CLICK HERE | Example output.</summary>

```shell
$ RUST_LOG=info cargo run --example basic

 2020-06-18T10:20:03.497Z INFO  tinkv::store > open store path: .tinkv
 2020-06-18T10:20:04.853Z INFO  tinkv::store > build keydir done, got 100001 keys. current stats: Stats { size_of_stale_entries: 0, total_stale_entries: 0, total_active_entries: 100001, total_data_files: 1, size_of_all_data_files: 10578168 }
200000 keys written in 9.98773 secs, 20024.57 keys/s
initial: Stats { size_of_stale_entries: 21155900, total_stale_entries: 200000, total_active_entries: 100001, total_data_files: 2, size_of_all_data_files: 31733728 }
key_1 => "value_1_1592475604853568000_hello_world"
after set 1: Stats { size_of_stale_entries: 21155900, total_stale_entries: 200000, total_active_entries: 100002, total_data_files: 2, size_of_all_data_files: 31733774 }
after set 2: Stats { size_of_stale_entries: 21155946, total_stale_entries: 200001, total_active_entries: 100002, total_data_files: 2, size_of_all_data_files: 31733822 }
after set 3: Stats { size_of_stale_entries: 21155994, total_stale_entries: 200002, total_active_entries: 100002, total_data_files: 2, size_of_all_data_files: 31733870 }
after remove: Stats { size_of_stale_entries: 21156107, total_stale_entries: 200003, total_active_entries: 100001, total_data_files: 2, size_of_all_data_files: 31733935 }
 2020-06-18T10:20:14.841Z INFO  tinkv::store > compact 2 data files
after compaction: Stats { size_of_stale_entries: 0, total_stale_entries: 0, total_active_entries: 100001, total_data_files: 2, size_of_all_data_files: 10577828 }
key_1 => "value_1_1592475604853568000_hello_world"
```
</details>

## CLI

Install `tinkv` executable binaries.

```shell
$ cargo install tinkv
```

```shell
$ tinkv --help
...
USAGE:
    tinkv [FLAGS] <path> <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -q, --quiet      Pass many times for less log output
    -V, --version    Prints version information
    -v, --verbose    Pass many times for more log output

ARGS:
    <path>    Path to tinkv datastore

SUBCOMMANDS:
    compact    Compact data files in datastore and reclaim disk space
    del        Delete a key value pair from datastore
    get        Retrive value of a key, and display the value
    help       Prints this message or the help of the given subcommand(s)
    keys       List all keys in datastore
    scan       Perform a prefix scanning for keys
    set        Store a key value pair into datastore
    stats      Display statistics of the datastore
```

Example usages:
```shell
$ tinkv /tmp/db set hello world
$ tinkv /tmp/db get hello
world

# Change verbosity level (info).
$ tinkv /tmp/db -vvv compact
2020-06-20T10:32:45.582Z INFO  tinkv::store > open store path: tmp/db
2020-06-20T10:32:45.582Z INFO  tinkv::store > build keydir from data file /tmp/db/000000000001.tinkv.data
2020-06-20T10:32:45.583Z INFO  tinkv::store > build keydir from data file /tmp/db/000000000002.tinkv.data
2020-06-20T10:32:45.583Z INFO  tinkv::store > build keydir done, got 1 keys. current stats: Stats { size_of_stale_entries:0, total_stale_entries: 0, total_active_entries: 1,total_data_files: 2, size_of_all_data_files: 60 }
2020-06-20T10:32:45.583Z INFO  tinkv::store > there are 3 datafiles need to be compacted
```

## Client & Server

[`tinkv-server`](./bin/../src/bin/tinkv-server.rs) is a redis-compatible key/value store server. However, not all the redis commmands are supported. The available commands are:

- `get <key>`
- `set <key> <value>`
- `del <key>`
- `ping [<message>]`
- `exists <key>`
- `info`
- `command`
- `keys <pattern>`
- `dbsize`
- `compact`: extended command to trigger a compaction manually.

Key/value pairs are persisted in log files of directory `/urs/local/var/tinkv`. The default listening address of `tinkv-server` is `127.0.0.1:7379`, and you can connect to it with a redis client.

### Quick Start

Install `tinkv-server` is very simple:

```shell
$ cargo install tinkv
```

Start server with default config (set log level to `info` mode):

```shell
$ tinkv-server -vv
2020-06-24T13:46:49.341Z INFO  tinkv::store > open store path: /usr/local/var/tinkv
2020-06-24T13:46:49.343Z INFO  tinkv::store > build keydir from data file /usr/local/var/tinkv/000000000001.tinkv.data
2020-06-24T13:46:49.343Z INFO  tinkv::store > build keydir from data file /usr/local/var/tinkv/000000000002.tinkv.data
2020-06-24T13:46:49.343Z INFO  tinkv::store > build keydir done, got 0 keys. current stats: Stats { size_of_stale_entries: 0,total_stale_entries: 0, total_active_entries: 0, total_data_files: 2, size_of_all_data_files: 0 }
2020-06-24T13:46:49.343Z INFO  tinkv::server > TinKV server is listening at '127.0.0.1:9815'
```

Communicate with `tinkv-server` by using `reids-cli`:

```shell
$ redis-cli -p 7379
127.0.0.1:9815> ping
PONG
127.0.0.1:9815> ping "hello, tinkv"
"hello, tinkv"
127.0.0.1:9815> set user.name 0xE8551CCB
OK
127.0.0.1:9815> get user.name
"0xE8551CCB"
127.0.0.1:9815> del user.name
OK
127.0.0.1:9815> get user.name
(nil)
127.0.0.1:9815> compact
OK
127.0.0.1:9815> not_found
(error) ERR unsupported command 'NOT_FOUND'
127.0.0.1:9815>
```

# About Compaction

Compation process will be triggered if `size_of_stale_entries >= config::COMPACTION_THRESHOLD` after each call of `set/remove`. Compaction steps are very simple and easy to understand:
1. Freeze current active segment, and switch to another one.
2. Create a compaction segment file, then iterate all the entries in `keydir` (in-memory hash table), copy related data entries into compaction file and update `keydir`.
3. Remove all the stale segment files.

Hint files (for fast startup) of corresponding data files will be generated after each compaction.

You can call `store.compact()` method to trigger compaction process if nessesary.

```rust
use pretty_env_logger;
use tinkv::{self, Store};

fn main() -> tinkv::Result<()> {
    pretty_env_logger::init();
    let mut store = Store::open("/path/to/tinkv")?;
    store.compact()?;

    Ok(())
}
```

# Structure of Data Directory

```shell
.tinkv
├── 000000000001.tinkv.hint -- related index/hint file, for fast startup
├── 000000000001.tinkv.data -- immutable data file
└── 000000000002.tinkv.data -- active data file
```

# Refs
## Projects
I'm not familiar with erlang, but I found some implementations in other languages worth learning.

1. Go: [prologic/bitcask](https://github.com/prologic/bitcask)
2. Go: [prologic/bitraft](https://github.com/prologic/bitraft)
3. Python: [turicas/pybitcask](https://github.com/turicas/pybitcask)
4. Rust: [dragonquest/bitcask](https://github.com/dragonquest/bitcask)

Found another simple key-value database based on Bitcask model, please refer [xujiajun/nutsdb](https://github.com/xujiajun/nutsdb).

## Blogs

- [Implementing a Copyless Redis Protocol in Rust with Parsing Combinators](https://dpbriggs.ca/blog/Implementing-A-Copyless-Redis-Protocol-in-Rust-With-Parsing-Combinators)

## Issues

- [Expected type parameter, found struct](https://stackoverflow.com/questions/26049939/expected-type-parameter-found-struct)
- [Help understanding how trait bounds workd](https://users.rust-lang.org/t/help-understanding-how-trait-bounds-work/19253/3)
- [Idiomatic way to take ownership of all items in a Vec<String>?](https://users.rust-lang.org/t/idiomatic-way-to-take-ownership-of-all-items-in-a-vec-string/7811/12)
- [Idiomatic callbacks in Rust](https://stackoverflow.com/questions/41081240/idiomatic-callbacks-in-rust)
- [What are reasonable ways to store a callback in a struct?](https://users.rust-lang.org/t/what-are-reasonable-ways-to-store-a-callback-in-a-struct/5810)
- [Things Rust doesn’t let you do](https://medium.com/@GolDDranks/things-rust-doesnt-let-you-do-draft-f596a3c740a5)

# License

Licensed under the [MIT license](./LICENSE).
