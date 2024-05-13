use super::{CompactionStrategy, Input as CompactionPayload};
use crate::{
    compaction::Choice,
    file::{BLOCKS_FILE, SEGMENTS_FOLDER},
    levels::LevelManifest,
    merge::{BoxedIterator, MergeIterator},
    segment::{block_index::BlockIndex, id::GlobalSegmentId, multi_writer::MultiWriter, Segment},
    snapshot::Counter as SnapshotCounter,
    stop_signal::StopSignal,
    tree_inner::{SealedMemtables, TreeId},
    Config,
};
use std::{
    collections::HashSet,
    sync::{atomic::AtomicU64, Arc, RwLock, RwLockWriteGuard},
    time::Instant,
};

#[cfg(feature = "bloom")]
use crate::bloom::BloomFilter;

#[cfg(feature = "bloom")]
use crate::file::BLOOM_FILTER_FILE;

/// Compaction options
pub struct Options {
    pub tree_id: TreeId,

    pub segment_id_generator: Arc<AtomicU64>,

    /// Configuration of tree.
    pub config: Config,

    /// Levels manifest.
    pub levels: Arc<RwLock<LevelManifest>>,

    /// sealed memtables (required for temporarily locking).
    pub sealed_memtables: Arc<RwLock<SealedMemtables>>,

    /// Snapshot counter (required for checking if there are open snapshots).
    pub open_snapshots: SnapshotCounter,

    /// Compaction strategy.
    ///
    /// The one inside `config` is NOT used.
    pub strategy: Arc<dyn CompactionStrategy>,

    /// Stop signal
    pub stop_signal: StopSignal,
}

impl Options {
    pub fn from_tree(tree: &crate::Tree, strategy: Arc<dyn CompactionStrategy>) -> Self {
        Self {
            tree_id: tree.id,
            segment_id_generator: tree.segment_id_counter.clone(),
            config: tree.config.clone(),
            sealed_memtables: tree.sealed_memtables.clone(),
            levels: tree.levels.clone(),
            open_snapshots: tree.open_snapshots.clone(),
            stop_signal: tree.stop_signal.clone(),
            strategy,
        }
    }
}

/// Runs compaction task.
///
/// This will block until the compactor is fully finished.
pub fn do_compaction(opts: &Options) -> crate::Result<()> {
    log::trace!("compactor: acquiring levels manifest lock");
    let levels = opts.levels.write().expect("lock is poisoned");

    log::trace!("compactor: consulting compaction strategy");
    let choice = opts.strategy.choose(&levels, &opts.config);

    match choice {
        Choice::DoCompact(payload) => {
            merge_segments(levels, opts, &payload)?;
        }
        Choice::DeleteSegments(payload) => {
            drop_segments(
                levels,
                opts,
                &payload
                    .into_iter()
                    .map(|x| (opts.tree_id, x).into())
                    .collect::<Vec<_>>(),
            )?;
        }
        Choice::DoNothing => {
            log::trace!("Compactor chose to do nothing");
        }
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
fn merge_segments(
    mut levels: RwLockWriteGuard<'_, LevelManifest>,
    opts: &Options,
    payload: &CompactionPayload,
) -> crate::Result<()> {
    if opts.stop_signal.is_stopped() {
        log::debug!("compactor: stopping before compaction because of stop signal");
    }

    let segments_base_folder = opts.config.path.join(SEGMENTS_FOLDER);

    log::debug!(
        "compactor: Chosen {} segments to compact into a single new segment at level {}",
        payload.segment_ids.len(),
        payload.dest_level
    );

    let merge_iter = {
        let to_merge: Vec<Arc<Segment>> = {
            let segments = levels.get_all_segments();

            payload
                .segment_ids
                .iter()
                // NOTE: Throw away duplicate segment IDs
                .collect::<HashSet<_>>()
                .into_iter()
                .filter_map(|x| segments.get(x))
                .cloned()
                .collect()
        };

        // NOTE: When there are open snapshots
        // we don't want to GC old versions of items
        // otherwise snapshots will lose data
        //
        // Also, keep versions around for a bit (don't evict when compacting into L0 & L1)
        let no_snapshots_open = !opts.open_snapshots.has_open_snapshots();
        let is_deep_level = payload.dest_level >= 2;

        let mut segment_readers: Vec<BoxedIterator<'_>> = Vec::with_capacity(to_merge.len());

        for segment in to_merge {
            let iter = Box::new(
                segment
                    .iter()
                    .cache_policy(crate::segment::block::CachePolicy::Read),
            );
            segment_readers.push(iter);
        }

        MergeIterator::new(segment_readers).evict_old_versions(no_snapshots_open && is_deep_level)
    };

    let last_level = levels.last_level_index();

    levels.hide_segments(&payload.segment_ids);
    drop(levels);

    // NOTE: Only evict tombstones when reaching the last level,
    // That way we don't resurrect data beneath the tombstone
    let is_last_level = payload.dest_level == last_level;
    let should_evict_tombstones = is_last_level;

    let start = Instant::now();

    let mut segment_writer = MultiWriter::new(
        opts.segment_id_generator.clone(),
        payload.target_size,
        crate::segment::writer::Options {
            block_size: opts.config.inner.block_size,
            evict_tombstones: should_evict_tombstones,
            folder: opts.config.path.join(SEGMENTS_FOLDER),

            #[cfg(feature = "bloom")]
            bloom_fp_rate: if is_last_level { 0.1 } else { 0.01 }, // TODO: MONKEY
        },
    )?;

    for (idx, item) in merge_iter.enumerate() {
        segment_writer.write(item?)?;

        if idx % 100_000 == 0 && opts.stop_signal.is_stopped() {
            log::debug!("compactor: stopping amidst compaction because of stop signal");
            return Ok(());
        }
    }

    let created_segments = segment_writer.finish()?;

    log::debug!(
        "Compacted in {}ms ({} segments created)",
        start.elapsed().as_millis(),
        created_segments.len()
    );

    let created_segments = created_segments
        .into_iter()
        .map(|metadata| -> crate::Result<Segment> {
            let segment_id = metadata.id;

            let segment_folder = segments_base_folder.join(segment_id.to_string());
            metadata.write_to_file(&segment_folder)?;

            #[cfg(feature = "bloom")]
            let bloom_filter = BloomFilter::from_file(segment_folder.join(BLOOM_FILTER_FILE))?;

            Ok(Segment {
                tree_id: opts.tree_id,
                descriptor_table: opts.config.descriptor_table.clone(),
                metadata,
                block_cache: opts.config.block_cache.clone(),
                // TODO: if L0, L1, preload block index (non-partitioned)
                block_index: BlockIndex::from_file(
                    (opts.tree_id, segment_id).into(),
                    opts.config.descriptor_table.clone(),
                    segment_folder,
                    opts.config.block_cache.clone(),
                )?
                .into(),

                #[cfg(feature = "bloom")]
                bloom_filter,
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;

    log::trace!("compactor: acquiring levels manifest write lock");
    let mut levels = opts.levels.write().expect("lock is poisoned");

    for segment in created_segments {
        log::trace!("Persisting segment {}", segment.metadata.id);

        let segment_folder = segments_base_folder.join(segment.metadata.id.to_string());

        opts.config.descriptor_table.insert(
            segment_folder.join(BLOCKS_FILE),
            (opts.tree_id, segment.metadata.id).into(),
        );

        levels.insert_into_level(payload.dest_level, segment.into());
    }

    // IMPORTANT: Write lock memtable(s), otherwise segments may get deleted while a range read is happening
    log::trace!("compactor: acquiring sealed memtables write lock");
    let sealed_memtables_guard = opts.sealed_memtables.write().expect("lock is poisoned");

    for segment_id in &payload.segment_ids {
        log::trace!("Removing segment {segment_id}");
        levels.remove(*segment_id);
    }

    // NOTE: Segments are registered, we can unlock the memtable(s) safely
    drop(sealed_memtables_guard);

    // IMPORTANT: Write the segment with the removed segments first
    // Otherwise the folder is deleted, but the segment is still referenced!
    levels.write_to_disk()?;

    for segment_id in &payload.segment_ids {
        let segment_folder = segments_base_folder.join(segment_id.to_string());
        log::trace!("rm -rf segment folder at {segment_folder:?}");

        std::fs::remove_dir_all(segment_folder)?;
    }

    for segment_id in &payload.segment_ids {
        log::trace!("Closing file handles for segment data file");

        opts.config
            .descriptor_table
            .remove((opts.tree_id, *segment_id).into());
    }

    levels.show_segments(&payload.segment_ids);

    drop(levels);

    log::debug!("compactor: done");

    Ok(())
}

fn drop_segments(
    mut levels: RwLockWriteGuard<'_, LevelManifest>,
    opts: &Options,
    segment_ids: &[GlobalSegmentId],
) -> crate::Result<()> {
    log::debug!("compactor: Chosen {} segments to drop", segment_ids.len());

    // IMPORTANT: Write lock memtable, otherwise segments may get deleted while a range read is happening
    log::trace!("compaction: acquiring sealed memtables write lock");
    let memtable_lock = opts.sealed_memtables.write().expect("lock is poisoned");

    for key in segment_ids {
        let segment_id = key.segment_id();
        log::trace!("Removing segment {segment_id}");

        levels.remove(segment_id);
    }

    // IMPORTANT: Write the segment with the removed segments first
    // Otherwise the folder is deleted, but the segment is still referenced!
    levels.write_to_disk()?;

    drop(memtable_lock);
    drop(levels);

    for key in segment_ids {
        let segment_id = key.segment_id();
        log::trace!("rm -rf segment folder {segment_id}");

        std::fs::remove_dir_all(
            opts.config
                .path
                .join(SEGMENTS_FOLDER)
                .join(segment_id.to_string()),
        )?;
    }

    for key in segment_ids {
        log::trace!("Closing file handles for segment data file");
        opts.config.descriptor_table.remove(*key);
    }

    log::trace!("Dropped {} segments", segment_ids.len());

    Ok(())
}
