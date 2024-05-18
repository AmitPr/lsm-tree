use criterion::{criterion_group, criterion_main, Criterion};
use lsm_tree::{
    bloom::BloomFilter,
    segment::{
        block::header::Header as BlockHeader, meta::CompressionType, value_block::ValueBlock,
    },
    serde::Serializable,
    BlockCache, Config, MemTable, Value,
};
use nanoid::nanoid;
use std::{io::Write, sync::Arc};

fn iterate_level_manifest(c: &mut Criterion) {
    let mut group = c.benchmark_group("Iterate level manifest");

    for segment_count in [0, 1, 5, 10, 20, 50, 100, 250, 500, 1_000] {
        let folder = tempfile::tempdir().unwrap();
        let tree = Config::new(folder).block_size(1_024).open().unwrap();

        for x in 0..segment_count {
            tree.insert("a", "b", x as u64);
            tree.flush_active_memtable().unwrap();
        }

        group.bench_function(&format!("iterate {segment_count} segments"), |b| {
            let levels = tree.levels.read().unwrap();

            b.iter(|| {
                assert_eq!(levels.iter().count(), segment_count);
            });
        });
    }
}

fn find_segment(c: &mut Criterion) {
    let mut group = c.benchmark_group("Find segment in disjoint level");

    for segment_count in [1u64, 5, 10, 20, 50, 100, 250, 500, 1_000] {
        let folder = tempfile::tempdir().unwrap();
        let tree = Config::new(folder).block_size(1_024).open().unwrap();

        for x in 0..segment_count {
            tree.insert(x.to_be_bytes(), "", x as u64);
            tree.flush_active_memtable().unwrap();
        }

        let key = (segment_count / 2).to_be_bytes();

        group.bench_function(
            &format!("find segment in {segment_count} segments - binary search"),
            |b| {
                let levels = tree.levels.read().unwrap();

                b.iter(|| {
                    levels
                        .levels
                        .first()
                        .expect("should exist")
                        .get_segment_containing_key(&key)
                        .expect("should exist")
                });
            },
        );

        group.bench_function(
            &format!("find segment in {segment_count} segments - linear search"),
            |b| {
                let levels = tree.levels.read().unwrap();

                b.iter(|| {
                    levels
                        .levels
                        .first()
                        .expect("should exist")
                        .iter()
                        .find(|x| x.metadata.key_range.contains_key(&key))
                        .expect("should exist");
                });
            },
        );
    }
}

fn memtable_get_upper_bound(c: &mut Criterion) {
    let memtable = MemTable::default();

    for _ in 0..1_000_000 {
        memtable.insert(Value {
            key: format!("abc_{}", nanoid!()).as_bytes().into(),
            value: vec![].into(),
            seqno: 0,
            value_type: lsm_tree::ValueType::Value,
        });
    }

    c.bench_function("memtable get", |b| {
        b.iter(|| {
            memtable.get("abc", None);
        });
    });
}

fn tli_find_item(c: &mut Criterion) {
    use lsm_tree::segment::block_index::{
        block_handle::KeyedBlockHandle, top_level::TopLevelIndex,
    };

    let mut group = c.benchmark_group("TLI find item");

    for item_count in [10u64, 100, 1_000, 1_000_000] {
        let items = {
            let mut items = Vec::with_capacity(item_count as usize);

            for x in 0..item_count {
                items.push(KeyedBlockHandle {
                    end_key: x.to_be_bytes().into(),
                    offset: x,
                });
            }

            items
        };

        let index = TopLevelIndex::from_boxed_slice(items.into());

        group.bench_function(
            format!("TLI get_next_block_handle ({item_count} items)"),
            |b| {
                let key = (item_count / 10 * 6).to_be_bytes();
                let expected: Arc<[u8]> = (item_count / 10 * 6 + 1).to_be_bytes().into();

                let block = index.get_lowest_block_containing_key(&key).unwrap();

                b.iter(|| {
                    assert_eq!(
                        expected,
                        index.get_next_block_handle(block.offset).unwrap().end_key
                    );
                })
            },
        );

        group.bench_function(
            format!("TLI get_block_containing_item ({item_count} items)"),
            |b| {
                let key = (item_count / 10 * 6).to_be_bytes();

                b.iter(|| {
                    assert_eq!(
                        key,
                        &*index.get_lowest_block_containing_key(&key).unwrap().end_key
                    );
                })
            },
        );
    }
}

fn value_block_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("ValueBlock::size");

    for item_count in [10, 100, 1_000] {
        group.bench_function(format!("{item_count} items"), |b| {
            let items = (0..item_count)
                .map(|_| {
                    Value::new(
                        "a".repeat(16).as_bytes(),
                        "a".repeat(100).as_bytes(),
                        63,
                        lsm_tree::ValueType::Tombstone,
                    )
                })
                .collect();

            let block = ValueBlock {
                items,
                header: BlockHeader {
                    compression: CompressionType::Lz4,
                    crc: 0,
                    data_length: 0,
                    previous_block_offset: 0,
                },
            };

            b.iter(|| {
                block.size();
            })
        });
    }
}

fn value_block_size_find(c: &mut Criterion) {
    use lsm_tree::segment::block_index::{block_handle::KeyedBlockHandle, IndexBlock};

    let mut group = c.benchmark_group("Find item in IndexBlock");

    // NOTE: Anything above 1000 is unlikely
    for item_count in [10, 100, 500, 1_000, 5_000] {
        group.bench_function(format!("{item_count} items"), |b| {
            let items = (0u64..item_count)
                .map(|x| KeyedBlockHandle {
                    end_key: x.to_be_bytes().into(),
                    offset: 56,
                })
                .collect();

            let block = IndexBlock {
                items,
                header: BlockHeader {
                    compression: CompressionType::Lz4,
                    crc: 0,
                    data_length: 0,
                    previous_block_offset: 0,
                },
            };
            let key = &(item_count / 2).to_be_bytes();

            b.iter(|| block.get_lowest_data_block_handle_containing_item(key))
        });
    }
}

fn load_block_from_disk(c: &mut Criterion) {
    let mut group = c.benchmark_group("Load block from disk");

    for block_size in [1, 4, 8, 16, 32, 64] {
        group.bench_function(format!("{block_size} KiB"), |b| {
            let block_size = block_size * 1_024;

            let mut size = 0;

            let mut items = vec![];

            for x in 0u64.. {
                let value = Value::new(
                    x.to_be_bytes(),
                    x.to_string().repeat(100).as_bytes(),
                    63,
                    lsm_tree::ValueType::Tombstone,
                );

                size += value.size();

                items.push(value);

                if size >= block_size {
                    break;
                }
            }

            let mut block = ValueBlock {
                items: items.clone().into_boxed_slice(),
                header: BlockHeader {
                    compression: CompressionType::Lz4,
                    crc: 0,
                    data_length: 0,
                    previous_block_offset: 0,
                },
            };

            // Serialize block
            block.header.crc = ValueBlock::create_crc(&block.items).unwrap();
            let (header, data) = ValueBlock::to_bytes_compressed(&items, 0).unwrap();

            let mut file = tempfile::tempfile().unwrap();
            header.serialize(&mut file).unwrap();
            file.write_all(&data).unwrap();

            b.iter(|| {
                let loaded_block = ValueBlock::from_file_compressed(&mut file, 0).unwrap();

                assert_eq!(loaded_block.items.len(), block.items.len());
                assert_eq!(loaded_block.header.crc, block.header.crc);
            });
        });
    }
}

fn file_descriptor_table(c: &mut Criterion) {
    use std::fs::File;

    let file = tempfile::NamedTempFile::new().unwrap();

    let mut group = c.benchmark_group("Get file descriptor");

    group.bench_function("fopen", |b: &mut criterion::Bencher<'_>| {
        b.iter(|| {
            File::open(file.path()).unwrap();
        });
    });

    let id = (0, 523).into();
    let descriptor_table = lsm_tree::descriptor_table::FileDescriptorTable::new(1, 1);
    descriptor_table.insert(file.path(), id);

    group.bench_function("descriptor table", |b: &mut criterion::Bencher<'_>| {
        b.iter(|| {
            let guard = descriptor_table.access(&id).unwrap().unwrap();
            let _fd = guard.file.lock().unwrap();
        });
    });
}

fn bloom_filter_construction(c: &mut Criterion) {
    let mut filter = BloomFilter::with_fp_rate(1_000_000, 0.001);

    c.bench_function("bloom filter add key", |b| {
        b.iter(|| {
            let key = nanoid::nanoid!();
            filter.set_with_hash(BloomFilter::get_hash(key.as_bytes()));
        });
    });
}

fn bloom_filter_contains(c: &mut Criterion) {
    let mut filter = BloomFilter::with_fp_rate(10, 0.0001);

    for key in [
        b"item0", b"item1", b"item2", b"item3", b"item4", b"item5", b"item6", b"item7", b"item8",
        b"item9",
    ] {
        filter.set_with_hash(BloomFilter::get_hash(key));

        assert!(!filter.contains(nanoid::nanoid!().as_bytes()));
    }

    c.bench_function("bloom filter contains key, true positive", |b| {
        b.iter(|| filter.contains(b"item4"));
    });

    c.bench_function("bloom filter contains key, true negative", |b| {
        b.iter(|| filter.contains(b"sdfafdas"));
    });
}

// TODO: benchmark .prefix().next() and .next_back(), disjoint and non-disjoint

fn tree_get_pairs(c: &mut Criterion) {
    let mut group = c.benchmark_group("Get pairs");
    group.sample_size(10);

    for segment_count in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] {
        {
            let folder = tempfile::tempdir().unwrap();
            let tree = Config::new(folder)
                .block_size(1_024)
                .block_cache(Arc::new(BlockCache::with_capacity_bytes(0)))
                .open()
                .unwrap();

            let mut x = 0_u64;

            for _ in 0..segment_count {
                for _ in 0..10 {
                    let key = x.to_be_bytes();
                    x += 1;
                    tree.insert(key, key, 0);
                }
                tree.flush_active_memtable().unwrap();
            }

            group.bench_function(
                &format!("Tree::first_key_value (disjoint), {segment_count} segments"),
                |b| {
                    b.iter(|| {
                        assert!(tree.first_key_value().unwrap().is_some());
                    });
                },
            );

            group.bench_function(
                &format!("Tree::last_key_value (disjoint), {segment_count} segments"),
                |b| {
                    b.iter(|| {
                        assert!(tree.last_key_value().unwrap().is_some());
                    });
                },
            );
        }

        {
            let folder = tempfile::tempdir().unwrap();
            let tree = Config::new(folder)
                .block_size(1_024)
                .block_cache(Arc::new(BlockCache::with_capacity_bytes(0)))
                .open()
                .unwrap();

            let mut x = 0_u64;

            for _ in 0..segment_count {
                for _ in 0..10 {
                    let key = x.to_be_bytes();
                    x += 1;
                    tree.insert(key, key, 0);
                }
                tree.insert("a", vec![], 0);
                tree.insert(u64::MAX.to_be_bytes(), vec![], 0);
                tree.flush_active_memtable().unwrap();
            }

            group.bench_function(
                &format!("Tree::first_key_value (non-disjoint), {segment_count} segments"),
                |b| {
                    b.iter(|| {
                        assert!(tree.first_key_value().unwrap().is_some());
                    });
                },
            );

            group.bench_function(
                &format!("Tree::last_key_value (non-disjoint), {segment_count} segments"),
                |b| {
                    b.iter(|| {
                        assert!(tree.last_key_value().unwrap().is_some());
                    });
                },
            );
        }
    }
}

// TODO: benchmark point read disjoint vs non-disjoint level

criterion_group!(
    benches,
    tli_find_item,
    memtable_get_upper_bound,
    value_block_size_find,
    value_block_size,
    load_block_from_disk,
    file_descriptor_table,
    bloom_filter_construction,
    bloom_filter_contains,
    tree_get_pairs,
    iterate_level_manifest,
    find_segment,
);
criterion_main!(benches);
