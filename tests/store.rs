use tempfile::TempDir;
use tinkv::{self, Result, Store};

#[test]
fn get_stored_value() -> Result<()> {
    let tmpdir = TempDir::new().expect("unable to create tmp dir");
    let mut store = Store::open(&tmpdir.path())?;

    store.set(b"version", b"1.0")?;
    store.set(b"name", b"tinkv")?;

    assert_eq!(store.get(b"version")?, Some(b"1.0".to_vec()));
    assert_eq!(store.get(b"name")?, Some(b"tinkv".to_vec()));
    assert_eq!(store.len(), 2);

    store.close()?;

    // open again, check persisted data.
    let mut store = Store::open(&tmpdir.path())?;
    assert_eq!(store.get(b"version")?, Some(b"1.0".to_vec()));
    assert_eq!(store.get(b"name")?, Some(b"tinkv".to_vec()));
    assert_eq!(store.len(), 2);

    Ok(())
}

#[test]
fn overwrite_value() -> Result<()> {
    let tmpdir = TempDir::new().expect("unable to create tmp dir");
    let mut store = Store::open(&tmpdir.path())?;

    store.set(b"version", b"1.0")?;
    assert_eq!(store.get(b"version")?, Some(b"1.0".to_vec()));

    store.set(b"version", b"2.0")?;
    assert_eq!(store.get(b"version")?, Some(b"2.0".to_vec()));

    store.close()?;

    // open again and check data
    let mut store = Store::open(&tmpdir.path())?;
    assert_eq!(store.get(b"version")?, Some(b"2.0".to_vec()));

    Ok(())
}

#[test]
fn get_non_existent_key() -> Result<()> {
    let tmpdir = TempDir::new().expect("unable to create tmp dir");
    let mut store = Store::open(&tmpdir.path())?;

    store.set(b"version", b"1.0")?;
    assert_eq!(store.get(b"version_foo")?, None);
    store.close()?;

    let mut store = Store::open(&tmpdir.path())?;
    assert_eq!(store.get(b"version_foo")?, None);

    Ok(())
}

#[test]
fn remove_key() -> Result<()> {
    let tmpdir = TempDir::new().expect("unable to create tmp dir");
    let mut store = Store::open(&tmpdir.path())?;

    store.set(b"version", b"1.0")?;
    assert!(store.remove(b"version").is_ok());
    assert_eq!(store.get(b"version")?, None);

    Ok(())
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    let tmpdir = TempDir::new().expect("unable to create tmp dir");
    let mut store = Store::open(&tmpdir.path())?;

    assert!(store.remove(b"version").is_err());

    Ok(())
}

#[test]
fn compaction() -> Result<()> {
    let tmpdir = TempDir::new().expect("unable to create tmp dir");
    let mut store = Store::open(&tmpdir.path())?;

    for it in 0..100 {
        for id in 0..1000 {
            let k = format!("key_{}", id);
            let v = format!("value_{}", it);
            store.set(k.as_bytes(), v.as_bytes())?;
        }

        let stats = store.stats();
        if stats.total_stale_entries <= 10000 {
            continue;
        }

        // trigger compaction
        store.compact()?;

        let stats = store.stats();
        assert_eq!(stats.size_of_stale_entries, 0);
        assert_eq!(stats.total_stale_entries, 0);
        assert_eq!(stats.total_active_entries, 1000);

        // close and reopen, chack persisted data
        store.close()?;

        store = Store::open(&tmpdir.path())?;

        let stats = store.stats();
        assert_eq!(stats.size_of_stale_entries, 0);
        assert_eq!(stats.total_stale_entries, 0);
        assert_eq!(stats.total_active_entries, 1000);

        for id in 0..1000 {
            let k = format!("key_{}", id);
            assert_eq!(
                store.get(k.as_bytes())?,
                Some(format!("value_{}", it).as_bytes().to_vec())
            );
        }
    }

    Ok(())
}
