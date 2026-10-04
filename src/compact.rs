//! Offline compaction: rewriting the live blocks of mostly-dead objects so the
//! objects can be reclaimed, and so a read of one file range costs fewer
//! requests.
//!
//! ## What `prune` cannot do
//!
//! [`Cleaner::prune`](crate::Cleaner::prune) reclaims an object nothing points
//! into. An object that is 5% live is not that object: those few blocks hold the
//! file's only copy of their contents, so the other 95% cannot be given back
//! while they stay there. Rewriting them into a new checkpoint is what turns the
//! old object into one `prune` can take.
//!
//! The same rewrite is what makes a read cheaper. A request breaks where the
//! object changes, so blocks of one file range scattered across many objects
//! cost one request each; written out together they cost one. Both goals use the
//! same mechanism and differ only in what they select.
//!
//! ## How the rewrite happens
//!
//! Not by a format this crate writes. Nothing here builds a segment, assigns a
//! pointer or publishes an inode — that is `hyperfile`'s write path, and a second
//! implementation of it would drift from the first. What this does instead is ask
//! `hyperfile` to rewrite blocks it already knows how to rewrite:
//!
//! - [`Hyper::fs_read_ahead`] pulls a run of blocks into the cache with the read
//!   path's own coalescing, so a contiguous run costs a few large requests rather
//!   than one per block.
//! - [`Hyper::fs_block_mut`] marks a block dirty on acquisition — "acquiring a
//!   `BlockMut` and not modifying anything still produces a new version at the
//!   next flush". Nothing is copied and no content changes.
//! - `fs_flush` writes the dirty set as one checkpoint. The dirty set is a
//!   `BTreeMap` keyed by block index, so the blocks land in file order, packed,
//!   in one object — which is both halves of what compaction is for.
//!
//! [`Hyper::fs_block_state`] separates a hole and an explicit zero block from
//! real data, so neither is materialized: rewriting a hole would turn a sparse
//! file dense, and rewriting a zero block would turn a pointer that occupies no
//! storage into one that does.
//!
//! ## Why offline
//!
//! A compactor is a writer, and it writes *old* bytes — the content it read
//! earlier. `hyperfile`'s default conflict policy, `RetryLastWriterWins`,
//! refreshes from storage on a conflict and re-publishes the dirty set, which for
//! a compactor means republishing a stale copy of a block another writer has
//! since changed. So compaction runs with
//! [`FlushConflictPolicy::FailFast`](hyperfile::config::FlushConflictPolicy::FailFast)
//! and the caller must be the container's only writer. A concurrent publish is
//! then detected — the inode carries an `If-Match` on its ETag — and the pass
//! fails rather than losing the other writer's data.
//!
//! A log is the other half of that contract. This refuses to run when the
//! container's prefix holds keys that are neither the inode nor segment objects,
//! because a WAL is the usual reason for them and a compactor that does not read
//! the log would publish over entries recovery has not replayed yet.

use std::io::{Error, ErrorKind, Result};
use std::collections::{BTreeMap, BTreeSet};
use log::{debug, info, warn};
use hyperfile::{BlockIndex, Cno, SegmentId};
use hyperfile::config::{FlushConflictPolicy, HyperFileConfig, HyperFileConfigBuilder,
    HyperFileRuntimeConfig};
use hyperfile::file::flags::{FileFlags, HyperFileFlags};
use hyperfile::file::hyper::Hyper;
use hyperfile::staging::config::StagingConfig;
use crate::s3::{ContainerView, S3Cleaner};

/// How many blocks to ask `block_placement` about at a time. One entry per block
/// comes back, so this bounds the answer rather than the work: the sweep covers
/// the whole file either way.
const PLACEMENT_CHUNK_BLOCKS: usize = 64 * 1024;

/// How much of one stored object is still pointed into.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectUsage {
    /// The object, `<cno>` or `<cno>.<part>`.
    pub segid: SegmentId,
    /// Its size on storage.
    pub bytes: u64,
    /// Live data blocks in it.
    pub live_data_blocks: usize,
    /// Live metadata nodes in it.
    pub live_meta_blocks: usize,
    /// What those occupy, by the container's own block sizes.
    pub live_bytes: u64,
}

impl ObjectUsage {
    /// Nothing points into it: `prune` can take it, compaction has no work here.
    pub fn is_dead(&self) -> bool {
        self.live_data_blocks == 0 && self.live_meta_blocks == 0
    }

    /// Live bytes over stored bytes. Zero for a dead object, and the cost side
    /// of the trade: rewriting this much frees [`Self::bytes`].
    pub fn live_ratio(&self) -> f64 {
        if self.bytes == 0 {
            return 0.0;
        }
        self.live_bytes as f64 / self.bytes as f64
    }

    /// Whether a live metadata node is in it.
    ///
    /// Such an object cannot be freed by rewriting data blocks: a node is
    /// relocated when the flush rewrites it, which happens because something
    /// under it was dirtied, and which node covers which keys is not visible from
    /// outside the map. Compaction skips these rather than rewriting their data
    /// blocks for nothing.
    pub fn holds_metadata(&self) -> bool {
        self.live_meta_blocks > 0
    }
}

/// What a container's storage is made of, and what could be given back.
#[derive(Clone, Debug)]
pub struct Report {
    /// The checkpoint the walk was taken from.
    pub high_cno: Cno,
    pub data_block_size: usize,
    pub meta_block_size: usize,
    /// Objects whose checkpoint falls in the queried `[low, high]`, ascending.
    pub objects: Vec<ObjectUsage>,
    /// Objects outside that range, which no operation here will touch.
    pub out_of_range: usize,
    /// Keys under the container's prefix that are neither the inode nor a segment
    /// object. A WAL root nested under the container is the usual reason, and
    /// compaction refuses to run while any are present.
    pub foreign_keys: usize,
    pub foreign_sample: Option<String>,
}

impl Report {
    /// Bytes stored by the objects in range.
    pub fn bytes(&self) -> u64 {
        self.objects.iter().map(|o| o.bytes).sum()
    }

    /// Bytes still pointed into.
    pub fn bytes_live(&self) -> u64 {
        self.objects.iter().map(|o| o.live_bytes).sum()
    }

    /// Objects nothing points into, and what they hold. This is `prune`'s work,
    /// and it needs no rewrite.
    pub fn dead(&self) -> (usize, u64) {
        let dead = self.objects.iter().filter(|o| o.is_dead());
        (dead.clone().count(), dead.map(|o| o.bytes).sum())
    }

    /// What compaction at `max_live_ratio` would cost and free: `(objects,
    /// bytes_to_rewrite, bytes_freed)`.
    ///
    /// An estimate from the same numbers [`S3Cleaner::compact`] selects by, so a
    /// dry run and this agree. The bytes freed are the objects' own, which
    /// `prune` collects afterwards; the rewrite itself publishes a new checkpoint
    /// holding the live blocks.
    pub fn compactable(&self, max_live_ratio: f64) -> (usize, u64, u64) {
        let picked = self.objects.iter()
            .filter(|o| is_candidate(o, max_live_ratio));
        (
            picked.clone().count(),
            picked.clone().map(|o| o.live_bytes).sum(),
            picked.map(|o| o.bytes).sum(),
        )
    }
}

/// Which objects compaction may rewrite, and how much it may do at once.
#[derive(Clone, Debug)]
pub struct CompactOptions {
    /// Rewrite an object only if at most this much of it is still live. At `1.0`
    /// every live object is a candidate, which rewrites the whole container.
    pub max_live_ratio: f64,
    /// How many bytes of live data to gather before flushing. One batch becomes
    /// one checkpoint and one object, so this is also the largest contiguous run
    /// compaction can produce — and it bounds memory, since the batch is held
    /// dirty in the cache until the flush.
    pub batch_bytes: usize,
    /// Stop after rewriting this much. The budget is checked between objects, so
    /// one object's live data is never split across passes.
    pub max_bytes_rewritten: u64,
    /// Select and report, write nothing. No file is opened.
    pub dry_run: bool,
    /// Put `mtime` and `atime` back after the rewrite. A compaction changes no
    /// content, so it should not look like a modification. `ctime` moves
    /// regardless: `hyperfile` advances it on any attribute update, as POSIX
    /// requires, and it is not settable.
    pub preserve_times: bool,
    /// Run even though the prefix holds keys this crate does not recognize. Only
    /// set this having established that they are not a WAL.
    pub allow_foreign_keys: bool,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            max_live_ratio: 0.5,
            batch_bytes: 64 * 1024 * 1024,
            max_bytes_rewritten: u64::MAX,
            dry_run: false,
            preserve_times: true,
            allow_foreign_keys: false,
        }
    }
}

/// What a compaction pass did.
#[derive(Clone, Debug, Default)]
pub struct CompactOutcome {
    /// The objects whose live blocks were rewritten, ascending.
    pub selected: Vec<SegmentId>,
    /// Of those, the ones a walk taken afterwards still finds referenced. Empty
    /// on a pass that did what it set out to; anything here is an object that was
    /// not freed, and is reported rather than assumed away.
    pub still_referenced: Vec<SegmentId>,
    pub blocks_rewritten: usize,
    pub bytes_rewritten: u64,
    /// The selected objects' stored bytes — what `prune` can now reclaim.
    pub bytes_freed: u64,
    /// Checkpoints published: one per batch, and that is how far the container's
    /// latest checkpoint number advanced.
    ///
    /// Restoring the timestamps is not counted because it is not one. A flush
    /// with nothing dirty but the inode's attributes writes the inode object only
    /// and allocates no checkpoint number, so the restore costs a second inode
    /// write and moves `last_cno` not at all.
    pub checkpoints: usize,
    pub dry_run: bool,
}

impl S3Cleaner {
    /// What the container's storage is made of: per-object liveness for every
    /// object whose checkpoint falls in `[low, high]`.
    ///
    /// Read-only, and the same walk `collect` does plus one `ListObjectsV2` for
    /// sizes. `high == 0` means the latest published checkpoint.
    pub async fn report(&self, low: Cno, high: Cno) -> Result<Report> {
        let view = self.view(low, high).await?;
        Ok(build_report(&view))
    }

    /// Rewrite the live blocks of mostly-dead objects so `prune` can reclaim
    /// them.
    ///
    /// The caller must be the container's only writer for the duration; see the
    /// [module docs](self) for why that is not a nicety. Nothing is deleted here
    /// — a compaction makes objects reclaimable and `prune` reclaims them, which
    /// keeps the destructive step in one place.
    pub async fn compact(
        &self,
        low: Cno,
        high: Cno,
        opts: &CompactOptions,
    ) -> Result<CompactOutcome> {
        let view = self.view(low, high).await?;
        if view.listing.foreign > 0 && !opts.allow_foreign_keys {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "{} key(s) under the container's prefix are neither the inode nor \
                     segment objects (for example {:?}); a WAL is the usual reason, and \
                     compacting past a log this pass does not read would publish over \
                     entries recovery has not replayed. Set allow_foreign_keys once you \
                     have established otherwise",
                    view.listing.foreign,
                    view.listing.foreign_sample.as_deref().unwrap_or("?"),
                ),
            ));
        }

        let report = build_report(&view);
        let candidates = select_candidates(&report.objects, opts);
        let mut selected: BTreeSet<SegmentId> = BTreeSet::new();
        let mut bytes_planned = 0u64;
        let mut bytes_freed = 0u64;
        for usage in candidates {
            if bytes_planned.saturating_add(usage.live_bytes) > opts.max_bytes_rewritten {
                break;
            }
            bytes_planned += usage.live_bytes;
            bytes_freed += usage.bytes;
            selected.insert(usage.segid);
        }
        debug!("compact: {} object(s) selected of {} in range",
            selected.len(), report.objects.len());

        let mut outcome = CompactOutcome {
            selected: selected.iter().copied().collect(),
            bytes_freed,
            dry_run: opts.dry_run,
            ..Default::default()
        };
        if opts.dry_run {
            // The plan's own numbers, which is all a dry run can report: nothing
            // is opened, so nothing can be counted as done.
            outcome.blocks_rewritten =
                (bytes_planned / view.data_block_size.max(1) as u64) as usize;
            outcome.bytes_rewritten = bytes_planned;
            return Ok(outcome);
        }
        if selected.is_empty() {
            return Ok(outcome);
        }

        let batch_blocks = batch_blocks(opts.batch_bytes, view.data_block_size);
        let config = compaction_config(&self.root_uri(), batch_blocks, view.data_block_size);
        let mut file = Hyper::open(
            self.client().clone(), config, HyperFileFlags::from_flags(FileFlags::rdwr()),
        ).await?;
        let saved = if opts.preserve_times { Some(file.fs_getattr()?) } else { None };
        let blocks_in_file = {
            let size = file.fs_getattr()?.st_size as u64;
            size.div_ceil(view.data_block_size as u64)
        };

        // Which blocks are in the selected objects. `block_placement` answers
        // exactly that, per index, from the live map — so it needs no agreement
        // with a second reading of the container's layout. It is swept in chunks
        // because the answer is one entry per block and a large file has a lot of
        // them.
        //
        // One batch becomes one checkpoint. Indices arrive ascending, which is
        // also the order the flush writes them in, so the object they land in is
        // in file order.
        let mut batch: Vec<BlockIndex> = Vec::new();
        let mut result = Ok(());
        let mut start: BlockIndex = 0;
        'sweep: while start < blocks_in_file {
            let n = PLACEMENT_CHUNK_BLOCKS.min((blocks_in_file - start) as usize);
            let places = file.fs_block_placement(start, n).await?;
            let picked: Vec<BlockIndex> = places.iter().enumerate()
                .filter_map(|(i, place)| match place {
                    Some((segid, _)) if selected.contains(segid) => {
                        Some(start + i as BlockIndex)
                    },
                    _ => None,
                })
                .collect();
            for idx in picked {
                batch.push(idx);
                if batch.len() >= batch_blocks {
                    result = rewrite_batch(&mut file, &mut batch, view.data_block_size,
                        &mut outcome).await;
                    if result.is_err() {
                        break 'sweep;
                    }
                }
            }
            start += n as BlockIndex;
        }
        if result.is_ok() && !batch.is_empty() {
            result = rewrite_batch(&mut file, &mut batch, view.data_block_size,
                &mut outcome).await;
        }

        // Put the timestamps back even after a failure: the batches that landed
        // moved mtime, and the ones that did not are not a reason to leave it
        // moved. Only the inode object is rewritten: no segment, no new number.
        if let Some(stat) = saved
            && let Err(e) = file.fs_setattr(&stat).await
        {
            warn!("compact: could not restore timestamps: {}", e);
        }
        let _ = file.fs_release().await;
        result?;

        // Say what was actually freed rather than what should have been. A
        // selected object that is still referenced means a block was not
        // relocated, which is worth reporting where it can be seen.
        let after = self.view(low, 0).await?;
        let referenced = after.referenced_objects();
        outcome.still_referenced = outcome.selected.iter()
            .filter(|segid| referenced.contains(segid))
            .copied()
            .collect();
        if !outcome.still_referenced.is_empty() {
            warn!("compact: {} of {} selected object(s) are still referenced: {:?}",
                outcome.still_referenced.len(), outcome.selected.len(),
                outcome.still_referenced);
        }
        info!("compact: rewrote {} block(s) ({} bytes) in {} checkpoint(s), freeing {} bytes \
            across {} object(s)", outcome.blocks_rewritten, outcome.bytes_rewritten,
            outcome.checkpoints, outcome.bytes_freed, outcome.selected.len());
        Ok(outcome)
    }
}

/// Per-object liveness, from one view.
///
/// A location's size depends on what it holds, and a pointer does not say: a
/// metadata node is `meta_block_size` and a data block is `data_block_size`. The
/// metadata subset of the walk is what separates them.
pub(crate) fn build_report(view: &ContainerView) -> Report {
    let mut live_data: BTreeMap<SegmentId, usize> = BTreeMap::new();
    let mut live_meta: BTreeMap<SegmentId, usize> = BTreeMap::new();
    for blkptr in &view.live.all {
        let segid = view.object_of(blkptr);
        if view.live.meta.contains(blkptr) {
            *live_meta.entry(segid).or_default() += 1;
        } else {
            *live_data.entry(segid).or_default() += 1;
        }
    }

    let mut objects = Vec::new();
    let mut out_of_range = 0;
    for (segid, bytes) in &view.listing.objects {
        let cno = segid.as_cno();
        if cno < view.low || cno > view.high_cno {
            out_of_range += 1;
            continue;
        }
        let live_data_blocks = live_data.get(segid).copied().unwrap_or(0);
        let live_meta_blocks = live_meta.get(segid).copied().unwrap_or(0);
        objects.push(ObjectUsage {
            segid: *segid,
            bytes: *bytes,
            live_data_blocks,
            live_meta_blocks,
            live_bytes: (live_data_blocks * view.data_block_size
                + live_meta_blocks * view.meta_block_size) as u64,
        });
    }

    Report {
        high_cno: view.high_cno,
        data_block_size: view.data_block_size,
        meta_block_size: view.meta_block_size,
        objects,
        out_of_range,
        foreign_keys: view.listing.foreign,
        foreign_sample: view.listing.foreign_sample.clone(),
    }
}

/// Whether rewriting this object's live data would free it.
///
/// Three exclusions, each for its own reason: a dead object is `prune`'s work and
/// rewriting nothing frees nothing; an object holding a live metadata node cannot
/// be freed by rewriting data blocks, so its data would be rewritten for no
/// return; and one above the ratio costs more to rewrite than the policy accepts.
fn is_candidate(usage: &ObjectUsage, max_live_ratio: f64) -> bool {
    !usage.is_dead()
        && !usage.holds_metadata()
        && usage.live_ratio() <= max_live_ratio
}

/// Candidates, cheapest first.
///
/// Ordered by live ratio ascending, which is the same as bytes freed per byte
/// rewritten descending, with the larger object first among equals so a budget
/// spent early buys more.
fn select_candidates<'a>(
    objects: &'a [ObjectUsage],
    opts: &CompactOptions,
) -> Vec<&'a ObjectUsage> {
    let mut picked: Vec<&ObjectUsage> = objects.iter()
        .filter(|o| is_candidate(o, opts.max_live_ratio))
        .collect();
    picked.sort_by(|a, b| {
        a.live_ratio().total_cmp(&b.live_ratio())
            .then(b.bytes.cmp(&a.bytes))
            .then(a.segid.cmp(&b.segid))
    });
    picked
}

/// Consecutive indices, as `(first, count)`.
///
/// What `read_ahead` wants: it takes a byte range and plans it the way a read of
/// the same range would, so one call per run of adjacent blocks turns into a few
/// large requests. Asking per block instead would be one request per block, which
/// for scattered blocks it will be anyway.
fn into_runs(indices: &[BlockIndex]) -> Vec<(BlockIndex, usize)> {
    let mut runs: Vec<(BlockIndex, usize)> = Vec::new();
    for idx in indices {
        match runs.last_mut() {
            Some((start, count)) if *start + *count as BlockIndex == *idx => *count += 1,
            _ => runs.push((*idx, 1)),
        }
    }
    runs
}

/// Blocks per batch, at least one.
fn batch_blocks(batch_bytes: usize, data_block_size: usize) -> usize {
    (batch_bytes / data_block_size.max(1)).max(1)
}

/// The configuration a compacting handle needs, and why each field is not the
/// default.
fn compaction_config(
    root_uri: &str,
    batch_blocks: usize,
    data_block_size: usize,
) -> HyperFileConfig {
    let staging = StagingConfig::new_s3_uri(root_uri, None);
    // Room for a whole batch plus slack, so nothing publishes until the batch is
    // complete: a threshold crossing mid-batch would cut the output in two, and
    // under the parted default it would cut it into a partial — the opposite of
    // what this is for. `need_flush` tests the dirty set against both the byte
    // threshold and `segment_buffer_size`, so both have to be above it.
    let bytes = batch_blocks.saturating_mul(data_block_size).saturating_mul(2);
    let runtime = HyperFileRuntimeConfig {
        data_cache_dirty_max_bytes_threshold: bytes,
        data_cache_dirty_max_blocks_threshold: batch_blocks.saturating_mul(2),
        segment_buffer_size: bytes,
        // An hour rather than forever, which is only about arithmetic elsewhere;
        // a pass this long has other problems.
        data_cache_dirty_max_flush_interval: 3_600_000,
        // `read_ahead` installs into the read cache and `block_mut` takes what it
        // finds there. Sized below the batch, the batch would evict its own
        // blocks and each would be fetched again, one request per block.
        data_cache_blocks: batch_blocks.saturating_mul(2),
        // A partial is a second object for one checkpoint, which is a break in
        // the run this is assembling.
        parted_segment_enabled: false,
        // A compactor writes bytes it read earlier. Refreshing and re-publishing
        // on a conflict, which is what the default does, would republish a stale
        // copy over another writer's newer one.
        flush_conflict_policy: FlushConflictPolicy::FailFast,
        ..HyperFileRuntimeConfig::default()
    };
    HyperFileConfigBuilder::new()
        .with_staging_config(&staging)
        .with_runtime_config(&runtime)
        .build()
}

/// Rewrite one batch: warm it, dirty it, publish it. Clears `batch`.
async fn rewrite_batch(
    file: &mut Hyper<'static>,
    batch: &mut Vec<BlockIndex>,
    data_block_size: usize,
    outcome: &mut CompactOutcome,
) -> Result<()> {
    batch.sort_unstable();
    batch.dedup();
    for (start, count) in into_runs(batch) {
        let off = start as usize * data_block_size;
        let len = count * data_block_size;
        let installed = file.fs_read_ahead(off, len).await?;
        debug!("compact: read ahead {} block(s) at index {} installed {}",
            count, start, installed);
    }

    let mut touched = 0usize;
    for idx in batch.iter() {
        // A hole or an explicit zero block occupies no storage, so there is
        // nothing to relocate and writing one would cost storage the file does
        // not use today. `block_mut` declines a hole as well; this keeps the
        // decision where it can be read.
        if !file.fs_block_state(*idx).await?.is_mapped() {
            continue;
        }
        if file.fs_block_mut(*idx, false).await?.is_some() {
            touched += 1;
        }
    }
    let cno = file.fs_flush().await?;
    debug!("compact: published checkpoint {} with {} rewritten block(s)", cno, touched);

    // Counted as done rather than as planned: a block the map no longer points
    // at was not rewritten, and saying it was would make the outcome a plan.
    outcome.blocks_rewritten += touched;
    outcome.bytes_rewritten += (touched * data_block_size) as u64;
    outcome.checkpoints += 1;
    batch.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(segid: SegmentId, bytes: u64, data: usize, meta: usize) -> ObjectUsage {
        ObjectUsage {
            segid,
            bytes,
            live_data_blocks: data,
            live_meta_blocks: meta,
            live_bytes: (data * 4096 + meta * 4096) as u64,
        }
    }

    #[test]
    fn a_dead_object_is_pruned_not_compacted() {
        let o = usage(SegmentId::new(1), 1 << 20, 0, 0);
        assert!(o.is_dead());
        assert!(!is_candidate(&o, 1.0), "nothing to rewrite frees nothing");
    }

    /// An object a live metadata node sits in cannot be freed by rewriting data
    /// blocks: which keys that node covers is not visible from out here, so
    /// nothing this crate can do will relocate it.
    #[test]
    fn an_object_holding_metadata_is_not_a_candidate() {
        let o = usage(SegmentId::with_part(3, 0), 1 << 20, 1, 1);
        assert!(o.holds_metadata());
        assert!(!is_candidate(&o, 1.0));
    }

    #[test]
    fn the_ratio_is_a_ceiling() {
        let low = usage(SegmentId::new(1), 4096 * 100, 5, 0);   // 5% live
        let high = usage(SegmentId::new(2), 4096 * 100, 90, 0); // 90% live
        assert!(is_candidate(&low, 0.5));
        assert!(!is_candidate(&high, 0.5));
        assert!(is_candidate(&high, 0.9));
    }

    /// Cheapest first, and the bigger object first among equals: the budget is
    /// finite and the point is how much it buys.
    #[test]
    fn candidates_come_cheapest_first() {
        let objects = vec![
            usage(SegmentId::new(1), 4096 * 100, 40, 0), // 40%
            usage(SegmentId::new(2), 4096 * 100, 5, 0),  // 5%
            usage(SegmentId::new(3), 4096 * 200, 10, 0), // 5%, larger
        ];
        let opts = CompactOptions::default();
        let picked = select_candidates(&objects, &opts);
        let order: Vec<SegmentId> = picked.iter().map(|o| o.segid).collect();
        assert_eq!(order, vec![SegmentId::new(3), SegmentId::new(2), SegmentId::new(1)]);
    }

    #[test]
    fn runs_are_maximal() {
        assert_eq!(into_runs(&[]), vec![]);
        assert_eq!(into_runs(&[7]), vec![(7, 1)]);
        assert_eq!(into_runs(&[0, 1, 2, 5, 6, 9]), vec![(0, 3), (5, 2), (9, 1)]);
    }

    #[test]
    fn a_batch_holds_at_least_one_block() {
        assert_eq!(batch_blocks(0, 4096), 1);
        assert_eq!(batch_blocks(4095, 4096), 1);
        assert_eq!(batch_blocks(8 << 20, 4096), 2048);
    }

    /// `compactable` must answer from the same rule the pass selects by,
    /// otherwise a dry run and the pass disagree about what will happen.
    #[test]
    fn the_estimate_agrees_with_the_selection() {
        let report = Report {
            high_cno: 9,
            data_block_size: 4096,
            meta_block_size: 4096,
            objects: vec![
                usage(SegmentId::new(1), 4096 * 100, 5, 0),
                usage(SegmentId::new(2), 4096 * 100, 80, 0),
                usage(SegmentId::new(3), 4096 * 100, 0, 0),
                usage(SegmentId::new(4), 4096 * 100, 2, 3),
            ],
            out_of_range: 0,
            foreign_keys: 0,
            foreign_sample: None,
        };
        let (n, rewrite, freed) = report.compactable(0.5);
        assert_eq!(n, 1, "only the 5% object qualifies");
        assert_eq!(rewrite, 5 * 4096);
        assert_eq!(freed, 4096 * 100);
        assert_eq!(report.dead(), (1, 4096 * 100));
        let picked = select_candidates(&report.objects, &CompactOptions::default());
        assert_eq!(picked.len(), n);
    }
}
