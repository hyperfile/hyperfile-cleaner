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
//! - [`Hyper::fs_relocate`] marks a run of blocks to be rewritten at the next flush
//!   with their current bytes. It reads them with the read path's own coalescing,
//!   so a contiguous run costs a few large requests rather than one per block; it
//!   skips holes, explicit zero blocks and anything already dirty, so a sparse
//!   file stays sparse; and it touches neither `mtime`, `ctime` nor the read cache.
//! - One `fs_flush` at the end of the pass writes the dirty set as one checkpoint.
//!   The dirty set is keyed by block index, so the blocks land in file order and
//!   packed — which is both halves of what compaction is for.
//!
//! ## A compaction is not a change of content
//!
//! The pass has to publish a new checkpoint — blocks moved, so pointers, the map
//! and the inode changed — and so `last_cno` advances. A consumer that reads a new
//! `last_cno` as "the content changed" would take every compaction for a change.
//! `hyperfile` therefore marks a publish whose every dirty block came from
//! relocation with `InodeRaw::FLAG_COMPACTED`, and
//! [`Hyper::fs_content_cno`] stays where it was across it. That is the number a
//! consumer should compare.
//!
//! The mark is only set when the publish's number directly follows the previous
//! one, so it holds for exactly one checkpoint. Two passes back to back would
//! therefore move `content_cno` on the second, and [`S3Cleaner::compact`] refuses
//! to start on a checkpoint that is already marked unless
//! [`CompactOptions::allow_consecutive`] says otherwise.
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
use hyperfile::meta_format::BlockPtrFormat;
use hyperfile::staging::config::StagingConfig;
use crate::s3::{ContainerView, S3Cleaner};


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
    /// Whether the newest published checkpoint is itself a compaction
    /// (`InodeRaw::FLAG_COMPACTED`). `compact` will not run on top of one unless
    /// asked to, since a second pass in a row moves `content_cno`.
    pub latest_compacted: bool,
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
    /// The most live bytes one pass rewrites. A pass is one checkpoint, so this is
    /// also what it holds dirty in memory until the flush — `fs_relocate` does not
    /// write anything out on its own. Objects are taken whole and cheapest first
    /// until the next would pass this; what is left waits for the next pass.
    ///
    /// On a container that is not in the parted format a checkpoint is one object,
    /// so a pass is further capped at what one object can hold.
    pub max_pass_bytes: u64,
    /// How many bytes of blocks to hand `fs_relocate` at once. It holds what it
    /// reads in a buffer of its own before the dirty cache takes it, so this bounds
    /// that second copy.
    pub relocate_chunk_bytes: usize,
    /// Select and report, write nothing. No file is opened.
    pub dry_run: bool,
    /// Run even though the newest checkpoint is already a compaction. The flag that
    /// keeps `content_cno` still holds for one checkpoint only, so a second pass in
    /// a row is seen by consumers as a change of content. Set this when that is
    /// acceptable, for example to finish a container larger than one pass.
    pub allow_consecutive: bool,
    /// Run even though the prefix holds keys this crate does not recognize. Only
    /// set this having established that they are not a WAL.
    pub allow_foreign_keys: bool,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            max_live_ratio: 0.5,
            max_pass_bytes: 512 * 1024 * 1024,
            relocate_chunk_bytes: 64 * 1024 * 1024,
            dry_run: false,
            allow_consecutive: false,
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
    /// Blocks `fs_relocate` marked: what was done, not what was planned. A hole,
    /// an explicit zero block or a block already dirty is not counted.
    pub blocks_rewritten: usize,
    pub bytes_rewritten: u64,
    /// The selected objects' stored bytes — what `prune` can now reclaim.
    pub bytes_freed: u64,
    /// Checkpoints published: one for a pass that rewrote anything, none
    /// otherwise. That is how far `last_cno` advanced.
    pub checkpoints: usize,
    /// `fs_content_cno` before and after the pass. Equal when consumers will not
    /// see the pass as a change of content, which is the ordinary case. They
    /// differ when the publish could not be marked: its number did not follow the
    /// previous one — an earlier flush failed and gave its number up — or the
    /// newest checkpoint was already a compaction and the pass ran anyway.
    pub content_cno_before: Cno,
    pub content_cno_after: Cno,
    /// Whether the candidates outlasted the pass budget. Another pass would find
    /// more to do, at the cost of `content_cno` moving once (see
    /// [`CompactOptions::allow_consecutive`]).
    pub more_remaining: bool,
    pub dry_run: bool,
}

impl CompactOutcome {
    /// Whether a consumer comparing `content_cno` will see this pass as a change.
    pub fn content_moved(&self) -> bool {
        self.content_cno_before != self.content_cno_after
    }
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

        if view.latest_compacted && !opts.allow_consecutive && !opts.dry_run {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "checkpoint {} is already a compaction, and the flag that keeps \
                     content_cno still holds for one checkpoint only: a second pass in a \
                     row would be seen by consumers as a change of content. Write to the \
                     file first, or set allow_consecutive",
                    view.high_cno,
                ),
            ));
        }

        let report = build_report(&view);
        let pass_cap = pass_cap(opts.max_pass_bytes, &view.fmt);
        let (selected, bytes_planned, bytes_freed, more_remaining) =
            plan_pass(&report.objects, opts.max_live_ratio, pass_cap);
        debug!("compact: {} object(s) selected of {} in range, {} live bytes (cap {})",
            selected.len(), report.objects.len(), bytes_planned, pass_cap);

        let mut outcome = CompactOutcome {
            selected: selected.iter().copied().collect(),
            bytes_freed,
            more_remaining,
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

        let config = compaction_config(&self.root_uri(), bytes_planned, &view);
        let mut file = Hyper::open(
            self.client().clone(), config, HyperFileFlags::from_flags(FileFlags::rdwr()),
        ).await?;
        let result = self.relocate_and_publish(&mut file, &view, &selected, opts,
            &mut outcome).await;
        let _ = file.fs_release().await;
        result?;
        if outcome.checkpoints == 0 {
            return Ok(outcome);
        }

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
        if outcome.content_moved() {
            warn!("compact: content_cno moved {} -> {}; consumers will see this pass \
                as a change of content", outcome.content_cno_before,
                outcome.content_cno_after);
        }
        info!("compact: rewrote {} block(s) ({} bytes) in {} checkpoint(s), freeing {} bytes \
            across {} object(s)", outcome.blocks_rewritten, outcome.bytes_rewritten,
            outcome.checkpoints, outcome.bytes_freed, outcome.selected.len());
        Ok(outcome)
    }

    /// Mark every block of the selected objects for relocation, then publish once.
    async fn relocate_and_publish(
        &self,
        file: &mut Hyper<'static>,
        view: &ContainerView,
        selected: &BTreeSet<SegmentId>,
        opts: &CompactOptions,
        outcome: &mut CompactOutcome,
    ) -> Result<()> {
        outcome.content_cno_before = file.fs_content_cno()?;
        outcome.content_cno_after = outcome.content_cno_before;
        if selected.is_empty() {
            return Ok(());
        }
        let bs = view.data_block_size;
        let chunk_blocks = (opts.relocate_chunk_bytes / bs.max(1)).max(1);

        // Which blocks are in the selected objects. `block_placement` answers
        // exactly that, per index, from the live map — so it needs no agreement
        // with a second reading of the container's layout. It is asked only about
        // the windows the walk found mapped blocks in, never about the holes
        // between them. Runs go to `fs_relocate` in index order, which is also
        // the order the flush writes them in.
        let mut pending: Vec<BlockIndex> = Vec::new();
        for (start, n) in view.live.windows.iter().copied() {
            let places = file.fs_block_placement(start, n).await?;
            for (i, place) in places.iter().enumerate() {
                if let Some((segid, _)) = place
                    && selected.contains(segid)
                {
                    pending.push(start + i as BlockIndex);
                    if pending.len() >= chunk_blocks {
                        outcome.blocks_rewritten += relocate_runs(file, &pending).await?;
                        pending.clear();
                    }
                }
            }
        }
        if !pending.is_empty() {
            outcome.blocks_rewritten += relocate_runs(file, &pending).await?;
        }
        outcome.bytes_rewritten = (outcome.blocks_rewritten * bs) as u64;
        if outcome.blocks_rewritten == 0 {
            return Ok(());
        }

        let cno = file.fs_flush().await?;
        outcome.checkpoints = 1;
        outcome.content_cno_after = file.fs_content_cno()?;
        debug!("compact: published checkpoint {} with {} relocated block(s); content_cno \
            {} -> {}", cno, outcome.blocks_rewritten, outcome.content_cno_before,
            outcome.content_cno_after);
        Ok(())
    }
}

/// Hand `indices` to `fs_relocate` as runs of consecutive blocks.
async fn relocate_runs(file: &mut Hyper<'static>, indices: &[BlockIndex]) -> Result<usize> {
    let runs = into_runs(indices);
    let marked = file.fs_relocate(&runs).await?;
    debug!("compact: relocate {} block(s) in {} run(s) marked {}",
        indices.len(), runs.len(), marked);
    Ok(marked)
}

/// What one pass may rewrite: the caller's budget, and on a container that is not
/// parted, what one object can hold. Under any other format a checkpoint is one
/// object, and `hyperfile` refuses a flush larger than that. Room is left for the
/// summary and the index nodes the same object carries.
fn pass_cap(max_pass_bytes: u64, fmt: &BlockPtrFormat) -> u64 {
    if fmt.is_parted() {
        return max_pass_bytes;
    }
    let one_object = fmt.max_object_bytes() as u64;
    max_pass_bytes.min(one_object / 4 * 3)
}

/// Candidates, cheapest first, taken whole while they fit `cap`.
///
/// Returns `(selected, live_bytes, stored_bytes, more_remaining)`. An object is
/// never split across passes: one that does not fit ends the selection, so the
/// cheapest work is done first and nothing is half-moved.
fn plan_pass(
    objects: &[ObjectUsage],
    max_live_ratio: f64,
    cap: u64,
) -> (BTreeSet<SegmentId>, u64, u64, bool) {
    let mut selected = BTreeSet::new();
    let mut live = 0u64;
    let mut stored = 0u64;
    let candidates = select_candidates(objects, max_live_ratio);
    let total = candidates.len();
    for usage in candidates {
        if live.saturating_add(usage.live_bytes) > cap {
            break;
        }
        live += usage.live_bytes;
        stored += usage.bytes;
        selected.insert(usage.segid);
    }
    let more = selected.len() < total;
    (selected, live, stored, more)
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
        latest_compacted: view.latest_compacted,
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
fn select_candidates(
    objects: &[ObjectUsage],
    max_live_ratio: f64,
) -> Vec<&ObjectUsage> {
    let mut picked: Vec<&ObjectUsage> = objects.iter()
        .filter(|o| is_candidate(o, max_live_ratio))
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

/// The configuration a compacting handle needs, and why each field is not the
/// default.
fn compaction_config(root_uri: &str, pass_bytes: u64, view: &ContainerView) -> HyperFileConfig {
    let staging = StagingConfig::new_s3_uri(root_uri, None);
    // Nothing may publish before the pass is complete. `fs_relocate` does not
    // check the thresholds itself, but the flush interval is checked on the
    // flush path, and a pass that crossed a threshold would be cut in two —
    // which moves `last_cno` twice and leaves `content_cno` marked for only the
    // second. Both thresholds and the segment buffer go above the pass.
    let room = (pass_bytes as usize)
        .saturating_add(view.data_block_size)
        .saturating_mul(2)
        .max(16 * 1024 * 1024);
    let runtime = HyperFileRuntimeConfig {
        data_cache_dirty_max_bytes_threshold: room,
        data_cache_dirty_max_blocks_threshold: room / view.data_block_size.max(1),
        segment_buffer_size: room,
        data_cache_dirty_max_flush_interval: 3_600_000,
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
        let picked = select_candidates(&objects, 0.5);
        let order: Vec<SegmentId> = picked.iter().map(|o| o.segid).collect();
        assert_eq!(order, vec![SegmentId::new(3), SegmentId::new(2), SegmentId::new(1)]);
    }

    #[test]
    fn runs_are_maximal() {
        assert_eq!(into_runs(&[]), vec![]);
        assert_eq!(into_runs(&[7]), vec![(7, 1)]);
        assert_eq!(into_runs(&[0, 1, 2, 5, 6, 9]), vec![(0, 3), (5, 2), (9, 1)]);
    }

    /// A pass takes objects whole and cheapest first, and stops at the first one
    /// that does not fit rather than skipping to a smaller one: the cheapest work
    /// is done first, and the pass says there is more.
    #[test]
    fn a_pass_takes_whole_objects_until_the_budget() {
        let objects = vec![
            usage(SegmentId::new(1), 4096 * 100, 10, 0), // 10%, 40 KiB live
            usage(SegmentId::new(2), 4096 * 100, 20, 0), // 20%, 80 KiB live
            usage(SegmentId::new(3), 4096 * 100, 1, 0),  //  1%,  4 KiB live
        ];
        let (picked, live, stored, more) = plan_pass(&objects, 0.5, 4096 * 15);
        assert_eq!(picked.into_iter().collect::<Vec<_>>(),
            vec![SegmentId::new(1), SegmentId::new(3)]);
        assert_eq!(live, 4096 * 11);
        assert_eq!(stored, 4096 * 200);
        assert!(more, "object 2 did not fit and is still a candidate");

        let (picked, _, _, more) = plan_pass(&objects, 0.5, u64::MAX);
        assert_eq!(picked.len(), 3);
        assert!(!more);
    }

    /// A parted container splits a large checkpoint into several objects, so only
    /// the caller's budget applies. Any other format writes a checkpoint as one
    /// object and refuses a flush past it, so the pass has to fit one, with room
    /// for what else that object carries.
    #[test]
    fn an_unparted_pass_fits_one_object() {
        let big = u64::MAX;
        assert_eq!(pass_cap(big, &BlockPtrFormat::PartedSegment), big);
        let mg = BlockPtrFormat::MicroGroup.max_object_bytes() as u64;
        assert!(pass_cap(big, &BlockPtrFormat::MicroGroup) < mg);
        assert!(pass_cap(big, &BlockPtrFormat::MicroGroup) >= mg / 2);
        assert_eq!(pass_cap(4096, &BlockPtrFormat::MicroGroup), 4096,
            "a smaller budget is kept as given");
    }

    /// An object whose live data alone is over the budget is never taken, and
    /// the pass reports that it left work behind instead of doing nothing quietly.
    #[test]
    fn an_object_larger_than_the_budget_is_reported_not_split() {
        let objects = vec![usage(SegmentId::new(1), 4096 * 1000, 100, 0)];
        let (picked, live, _, more) = plan_pass(&objects, 0.5, 4096 * 10);
        assert!(picked.is_empty());
        assert_eq!(live, 0);
        assert!(more);
    }

    /// `compactable` must answer from the same rule the pass selects by,
    /// otherwise a dry run and the pass disagree about what will happen.
    #[test]
    fn the_estimate_agrees_with_the_selection() {
        let report = Report {
            high_cno: 9,
            latest_compacted: false,
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
        let picked = select_candidates(&report.objects, CompactOptions::default().max_live_ratio);
        assert_eq!(picked.len(), n);
    }
}
