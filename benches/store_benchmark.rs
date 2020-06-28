use criterion::{criterion_group, criterion_main, BatchSize, Criterion, ParameterizedBenchmark};
use rand::prelude::*;

use sled::{Db, Tree};
use std::iter;
use std::path::Path;
use tempfile::TempDir;
use tinkv::{self, Result, Store, TinkvError};

#[derive(Clone)]
pub struct SledStore(Db);

impl SledStore {
    fn open<P: AsRef<Path>>(path: P) -> Self {
        let tree = sled::open(path).expect("failed to open db");
        SledStore(tree)
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let tree: &Tree = &self.0;
        tree.insert(key, value.into_bytes())
            .map(|_| ())
            .map_err(|e| TinkvError::Custom(format!("{}", e)))?;
        tree.flush()
            .map_err(|e| TinkvError::Custom(format!("{}", e)))?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let tree: &Tree = &self.0;
        Ok(tree
            .get(key)
            .map_err(|e| TinkvError::Custom(format!("{}", e)))?
            .map(|i_vec| AsRef::<[u8]>::as_ref(&i_vec).to_vec())
            .map(String::from_utf8)
            .transpose()
            .map_err(|e| TinkvError::Custom(format!("{}", e)))?)
    }
}

fn set_benchmark(c: &mut Criterion) {
    let b = ParameterizedBenchmark::new(
        "tinkv-store",
        |b, _| {
            b.iter_batched(
                || {
                    let tmpdir = TempDir::new().unwrap();
                    (Store::open(&tmpdir.path()).unwrap(), tmpdir)
                },
                |(mut store, _tmpdir)| {
                    for i in 1..(1 << 12) {
                        store
                            .set(format!("key_{}", i).as_bytes(), b"value")
                            .unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
        iter::once(()),
    )
    .with_function("sled_store", |b, _| {
        b.iter_batched(
            || {
                let tmpdir = TempDir::new().unwrap();
                (SledStore::open(&tmpdir.path()), tmpdir)
            },
            |(mut db, _tmpdir)| {
                for i in 1..(1 << 12) {
                    db.set(format!("key_{}", i), "value".to_string()).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    c.bench("set_betchmark", b);
}

fn get_benchmark(c: &mut Criterion) {
    let b = ParameterizedBenchmark::new(
        "tinkv-store",
        |b, i| {
            let tempdir = TempDir::new().unwrap();
            let mut store = Store::open(&tempdir.path()).unwrap();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key_{}", key_i).as_bytes(), b"value")
                    .unwrap();
            }

            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(format!("key_{}", rng.gen_range(1, 1 << i)).as_bytes())
                    .unwrap();
            })
        },
        vec![8, 12, 16, 20],
    )
    .with_function("sled_store", |b, i| {
        let tmpdir = TempDir::new().unwrap();
        let mut db = SledStore::open(&tmpdir.path());
        for key_i in 1..(1 << i) {
            db.set(format!("key_{}", key_i), "value".to_owned())
                .unwrap();
        }

        let mut rng = SmallRng::from_seed([0; 16]);
        b.iter(|| {
            db.get(format!("key_{}", rng.gen_range(1, 1 << i))).unwrap();
        })
    });
    c.bench("get_benchmark", b);
}

criterion_group!(benches, set_benchmark, get_benchmark);
criterion_main!(benches);
