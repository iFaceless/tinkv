# ![TinKV Logo](https://i.loli.net/2020/06/08/3hYVFNurxGoLei7.jpg)

[TinKV](https://github.com/iFaceless/tinkv) is a simple and fast key-value storage engine written in Rust. Inspired by [basho/bitcask](https://github.com/basho/bitcask), written after attending the [Talent Plan courses](https://github.com/pingcap/talent-plan). 

Notes:
- *Do not use it in production.*
- *Operations like set/remove/compact are not thread-safe.*

Happy hacking~

![tinkv-overview.png](https://i.loli.net/2020/06/17/DW5JTEF4MlCsOLZ.png)

# Usage
## As a library

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

Public APIs of tinkv store are very easy to use:
| API                      |                   Description                                 |
|--------------------------|---------------------------------------------------------------|
|`Store::open(path)`       | Open a new or existing datastore. The directory must be writeable and readable for tinkv store.|`
|`store.get(key)`          | Get value by key from datastore.|
|`store.set(key, value)`   | Store a key value pair into datastore.|
|`store.remove(key, value)`| Remove a key from datastore.|
|`store.compact()`         | Merge data files into a more compact form. drop stale segments to release disk space. Produce hint files after compaction for faster startup.|
|`store.keys()`            | Return all the keys in database.|
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

## Client & Server

**Redis-compatible protocol?**

**Note**: not all the redis commands are available, only a few of them are supported by tinkv.

- `get <key>`
- `set <key> <value>`
- `del <key>`

# Compaction

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
├── 000000000001.tinkv.data  -- immutable data file
└── 000000000002.tinkv.data  -- active data file
```

# Refs

I'm not familiar with erlang, but I found some implementations in other languages worth learning.

1. Go: [prologic/bitcask](https://github.com/prologic/bitcask)
2. Go: [prologic/bitraft](https://github.com/prologic/bitraft)
3. Python: [turicas/pybitcask](https://github.com/turicas/pybitcask)
4. Rust: [dragonquest/bitcask](https://github.com/dragonquest/bitcask)

Found another simple key-value database based on Bitcask model, please refer [xujiajun/nutsdb](https://github.com/xujiajun/nutsdb).