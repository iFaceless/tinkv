# ![TinKV Logo](https://i.loli.net/2020/06/08/3hYVFNurxGoLei7.jpg)

[TinKV](https://github.com/iFaceless/tinkv) is a simple and fast key-value storage engine written in Rust. Inspired by [basho/bitcask](https://github.com/basho/bitcask), written after attending the [Talent Plan courses](https://github.com/pingcap/talent-plan). 

Happy hacking~

![tinkv-overview.png](https://i.loli.net/2020/06/17/uKSHIBgdvjNCwER.png)

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

## Client & Server
### REST APIs

### Redis-compatible protocol

**Note**: not all the redis commands are available, only a few of them are supported by tinkv.

- `get <key>`
- `set <key> <value>`
- `del <key>`

# Compaction

Compation process will be triggered if `size_of_stale_entries >= 10MB` after each call of `set/remove`. Compaction policy is very simple and easy to understand:
1. Freeze current active segment, and switch to another one.
2. Create a compaction segment file, then iterate all the entries in `keydir` (in-memory hash table), copy related data entries into compaction file and update `keydir`.
3. Remove all the stale segment files.

You can call `Store::compact()` method to trigger compaction process if nessesary.

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

# Refs

I'm not familiar with erlang, but I found some implementations in other languages worth learning.

1. Go: [prologic/bitcask](https://github.com/prologic/bitcask)
2. Go: [prologic/bitraft](https://github.com/prologic/bitraft)
3. Python: [turicas/pybitcask](https://github.com/turicas/pybitcask)
4. Rust: [dragonquest/bitcask](https://github.com/dragonquest/bitcask)

Found another simple key-value database based on Bitcask model, please refer [xujiajun/nutsdb](https://github.com/xujiajun/nutsdb).