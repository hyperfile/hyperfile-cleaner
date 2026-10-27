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
//! ## Beside the writer
//!
//! With [`CompactOptions::beside_writer`] the pass is not a second writer:
//! `Hyper::fs_begin_compaction` takes up the stored version, `fs_relocate`
//! copies the blocks as below, and `fs_publish_compaction` writes the copies'
//! map and *attaches* it to the stored inode -- its object's metadata, with one
//! conditional write that changes nothing else. The writer takes the
//! compaction in before its next publish of data and keeps every block it
//! changed, so `FailFast` on the writer's side still stops only a second writer.
//! Until then readers read through the objects as they were, and the emptied
//! objects become reclaimable only once the writer has published. What follows
//! is about the default.
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
use hyperfile::file::file::CompactionAttached;
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
    /// Whether the live metadata in it is rewritten by rewriting its live data.
    ///
    /// True for a data part — any part above 0 — of a `StreamSegment` container.
    /// That layout puts every leaf of the block map in the object holding the data
    /// under it, and the nodes above the leaves in part 0 only. A leaf in a data
    /// part is therefore live only while none of the blocks it was written with
    /// has changed, so those blocks are live and in the same object; relocating
    /// them dirties the leaf, and the flush writes it elsewhere with them.
    /// Under every other format the nodes sit in part 0 or the one object, apart
    /// from the data they describe, and this is false.
    pub meta_moves_with_data: bool,
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
    /// Such an object cannot in general be freed by rewriting data blocks: a node
    /// is relocated when the flush rewrites it, which happens because something
    /// under it was dirtied, and which node covers which keys is not visible from
    /// outside the map. Compaction skips these rather than rewriting their data
    /// blocks for nothing — unless [`Self::meta_moves_with_data`] says the node
    /// goes with the data.
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
    /// A compaction attached to the stored inode beside the writer and not yet
    /// taken in, as its object's metadata has it: the version it was built on,
    /// its parts, blocks moved and its map. `Unreadable` stops `prune`.
    pub attached: hyperfile::compaction::StoredCompaction,
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
    /// The most live bytes one pass rewrites. Objects are taken whole and
    /// cheapest first until the next would pass this; what is left waits for the
    /// next pass. Unlimited by default.
    ///
    /// On a parted container with partial segments enabled — the default for
    /// both — this is only a size: `fs_relocate` writes the pass out as partials
    /// once it crosses the dirty-data limits, so memory is bounded by those
    /// limits whatever the pass. Where no partial can be written, everything the
    /// pass relocates stays dirty until its one flush, so the pass is also capped
    /// at [`UNBOUNDED_PASS_BYTES`]; and a container that is not parted writes a
    /// checkpoint as one object, so its pass is capped at what one object holds.
    pub max_pass_bytes: u64,
    /// The runtime configuration of the handle that does the rewrite: the size of
    /// the partials a pass is written out as (`data_cache_dirty_min_bytes_to_part`
    /// and the dirty-data limits), `segment_buffer_size`, the read cache,
    /// multipart uploads. Give the one the container's writer uses, and what a
    /// compaction writes is shaped like what the writer writes.
    ///
    /// `None` uses [`compaction_runtime_default`]. Either way the cleaner
    /// overrides one field, `flush_conflict_policy`, to `FailFast`: a compactor
    /// writes bytes it read earlier, and retrying on a conflict would republish
    /// them over another writer's newer ones.
    pub runtime: Option<HyperFileRuntimeConfig>,
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
    /// Compact beside the container's writer: attach the pass to the stored inode
    /// rather than publish a checkpoint of its own (`Hyper::fs_begin_compaction`
    /// and `fs_publish_compaction` in `hyperfile`). It keeps the checkpoint
    /// number and the content checkpoint, and a writer of hyperfile 0.12 or later
    /// takes it in before its next publish of data rather than failing as on a
    /// second writer. Needs a container with parts (`PartedSegment`,
    /// `StreamSegment`). `allow_consecutive` does not apply: an attached
    /// compaction never moves `content_cno`.
    ///
    /// The pass fails with `ResourceBusy` while a compaction is attached and not
    /// yet taken in, or while the writer's publish is on its way, and with
    /// `TimedOut` past `max_age`; either way try again later.
    pub beside_writer: bool,
    /// Beside the writer, the longest a pass may take from its start to its
    /// attach; past it the pass is given up. Its copies are referenced by
    /// nothing until attached, and only a `prune` grace keeps them, so set this
    /// below the grace of any [`S3Cleaner::prune_with`] that may run meanwhile,
    /// less a margin. Ten minutes by default.
    pub max_age: std::time::Duration,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            max_live_ratio: 0.5,
            max_pass_bytes: u64::MAX,
            runtime: None,
            relocate_chunk_bytes: 64 * 1024 * 1024,
            dry_run: false,
            allow_consecutive: false,
            allow_foreign_keys: false,
            beside_writer: false,
            max_age: std::time::Duration::from_secs(600),
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
    /// otherwise. That is how far `last_cno` advanced. Always none beside the
    /// writer, whose pass keeps the number: see `attached`.
    pub checkpoints: usize,
    /// Beside the writer, what was attached: the version it was built on and the
    /// one it was attached to, the parts its objects are in, blocks moved, and
    /// how many conditional writes of the inode it took.
    pub attached: Option<CompactionAttached>,
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

        if view.latest_compacted && !opts.allow_consecutive && !opts.dry_run && !opts.beside_writer {
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
        let mut runtime = compaction_runtime(opts.runtime.as_ref());
        if opts.beside_writer {
            // Not the writer: a checkpoint the writer has on storage and has not
            // yet pointed the inode at is the writer's to publish. Taking it up
            // here would publish it under the writer and fail its own inode write.
            runtime.take_up_on_open = false;
        }
        let pass_cap = pass_cap(opts.max_pass_bytes, &view.fmt, &runtime);
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

        let config = compaction_config(&self.root_uri(), &runtime);
        let mut file = Hyper::open(
            self.client().clone(), config, HyperFileFlags::from_flags(FileFlags::rdwr()),
        ).await?;
        let result = self.relocate_and_publish(&mut file, &view, &selected, opts,
            &mut outcome).await;
        match result {
            // Published, or nothing to publish: close as usual.
            Ok(()) => { let _ = file.fs_release().await; },
            // A failed pass publishes nothing. `fs_release` would flush what was
            // marked, so a pass reported failed could still have published part
            // of itself. The handle is dropped instead; any partials it wrote sit
            // above the published checkpoint, where the next open steps past
            // them and a later `prune` collects them once a publish passes them.
            Err(e) => {
                drop(file);
                return Err(e);
            },
        }
        if outcome.checkpoints == 0 && outcome.attached.is_none_or(|p| p.moved == 0) {
            return Ok(outcome);
        }

        // Say what was actually freed rather than what should have been. A
        // selected object that is still referenced means a block was not
        // relocated, which is worth reporting where it can be seen. Beside the
        // writer, by the attached map while it is attached: the stored one still
        // points at every selected object until the writer takes it in.
        let after = self.view(low, 0).await?;
        let referenced = match (after.attached_root()?, outcome.attached) {
            (Some(root), Some(_)) => self.reachable_objects(&after, root).await?,
            _ => after.referenced_objects(),
        };
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
        if opts.beside_writer {
            // Takes up the version stored now: the placements below are its.
            file.fs_begin_compaction().await?;
            let res = self.relocate_selected(file, view, selected, opts, outcome).await;
            if res.is_err() || outcome.blocks_rewritten == 0 {
                let _ = file.fs_abort_compaction().await;
                return res;
            }
            let attached = file.fs_publish_compaction(opts.max_age).await?;
            outcome.content_cno_after = file.fs_content_cno()?;
            debug!("compact: attached a compaction of checkpoint {} to checkpoint {}: {} block(s) moved \
                into parts {}..{}, in {} attempt(s)", attached.base, attached.attached_to, attached.moved,
                attached.parts.0, attached.parts.1, attached.attempts);
            outcome.attached = Some(attached);
            return Ok(());
        }
        self.relocate_selected(file, view, selected, opts, outcome).await?;
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

    /// Mark every block of the selected objects for relocation.
    async fn relocate_selected(
        &self,
        file: &mut Hyper<'static>,
        view: &ContainerView,
        selected: &BTreeSet<SegmentId>,
        opts: &CompactOptions,
        outcome: &mut CompactOutcome,
    ) -> Result<()> {
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

/// The most a pass holds in memory when nothing can write it out early: a parted
/// container with partial segments turned off, or a container that is not parted.
/// There `fs_relocate` keeps every block dirty until the pass's one flush.
pub const UNBOUNDED_PASS_BYTES: u64 = 512 * 1024 * 1024;

/// What `hyperfile` takes the most bytes of one object to be, for this format and
/// configuration. A copy of `HyperTrait::max_object_bytes`, which is private, with
/// its 16 KiB floor.
fn max_object_bytes(fmt: &BlockPtrFormat, runtime: &HyperFileRuntimeConfig) -> u64 {
    runtime.segment_max_object_bytes.clamp(16 * 1024, fmt.max_object_bytes()) as u64
}

/// What one pass may rewrite.
///
/// Where `fs_relocate` can write partials, the caller's budget alone: memory is
/// bounded by the dirty-data limits. Where it cannot, also
/// [`UNBOUNDED_PASS_BYTES`]. And on a container that is not parted, a checkpoint
/// is one object and `hyperfile` refuses a flush larger, so the pass also fits
/// one object, with room for the summary and index nodes it carries.
fn pass_cap(max_pass_bytes: u64, fmt: &BlockPtrFormat, runtime: &HyperFileRuntimeConfig) -> u64 {
    if fmt.is_parted() {
        if runtime.parted_segment_enabled {
            return max_pass_bytes;
        }
        return max_pass_bytes.min(UNBOUNDED_PASS_BYTES);
    }
    let one_object = max_object_bytes(fmt, runtime);
    max_pass_bytes.min(UNBOUNDED_PASS_BYTES).min(one_object / 4 * 3)
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
            meta_moves_with_data: view.fmt.is_stream() && segid.is_partial(),
        });
    }

    Report {
        high_cno: view.high_cno,
        latest_compacted: view.latest_compacted,
        attached: view.compaction.clone(),
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
/// in general be freed by rewriting data blocks, so its data would be rewritten
/// for no return — except where the layout puts that node with the data
/// ([`ObjectUsage::meta_moves_with_data`]); and one above the ratio costs more to
/// rewrite than the policy accepts.
///
/// Getting the middle one wrong costs a rewrite, never data: compaction deletes
/// nothing, `prune` keeps whatever the walk afterwards finds live, and
/// [`CompactOutcome::still_referenced`] reports an object selected and not freed.
fn is_candidate(usage: &ObjectUsage, max_live_ratio: f64) -> bool {
    !usage.is_dead()
        && (!usage.holds_metadata() || usage.meta_moves_with_data)
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

/// The runtime configuration a compaction uses when the caller gives none.
///
/// Partials of 64 MiB: large enough that a compacted range reads in few requests,
/// small enough to bound what a pass holds. The flush interval is an hour so that
/// partials are cut by size rather than by how long the reads took.
pub fn compaction_runtime_default() -> HyperFileRuntimeConfig {
    const PART: usize = 64 * 1024 * 1024;
    HyperFileRuntimeConfig {
        data_cache_dirty_max_bytes_threshold: PART,
        data_cache_dirty_min_bytes_to_part: PART,
        segment_buffer_size: PART,
        data_cache_dirty_max_flush_interval: 3_600_000,
        ..HyperFileRuntimeConfig::default()
    }
}

/// The caller's runtime configuration, or the default, with the one field a
/// compaction cannot leave to the caller.
///
/// Nothing else needs forcing. A pass never publishes before its flush — since
/// `hyperfile 0.10.4`, `fs_relocate` answers the dirty-data limits with a partial
/// or not at all, never with a publish — so the limits can be whatever the
/// caller's writer uses.
fn compaction_runtime(given: Option<&HyperFileRuntimeConfig>) -> HyperFileRuntimeConfig {
    let mut runtime = given.cloned().unwrap_or_else(compaction_runtime_default);
    // A compactor writes bytes it read earlier. Refreshing and re-publishing on a
    // conflict, which `RetryLastWriterWins` does, would republish a stale copy
    // over another writer's newer one.
    runtime.flush_conflict_policy = FlushConflictPolicy::FailFast;
    runtime
}

fn compaction_config(root_uri: &str, runtime: &HyperFileRuntimeConfig) -> HyperFileConfig {
    let staging = StagingConfig::new_s3_uri(root_uri, None);
    HyperFileConfigBuilder::new()
        .with_staging_config(&staging)
        .with_runtime_config(runtime)
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
            meta_moves_with_data: false,
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

    /// A stream data part keeps its leaves with its data, so it is a candidate
    /// despite them; its part 0, which holds the nodes above the leaves, is not.
    #[test]
    fn a_stream_data_part_is_a_candidate_despite_its_leaves() {
        let part = ObjectUsage { meta_moves_with_data: true,
            ..usage(SegmentId::with_part(3, 2), 1 << 20, 20, 1) };
        assert!(part.holds_metadata());
        assert!(is_candidate(&part, 0.5));
        let part0 = usage(SegmentId::with_part(3, 0), 1 << 20, 20, 1);
        assert!(!is_candidate(&part0, 1.0));
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

    /// Where partials can be written, memory is bounded by the dirty-data limits
    /// and only the caller's budget applies. Where they cannot, the pass is held
    /// in memory whole and is capped; and a container that is not parted also has
    /// to fit its pass in one object, as `hyperfile` reckons one — the caller's
    /// `segment_max_object_bytes` included.
    #[test]
    fn the_pass_cap_follows_what_can_write_it_out() {
        let big = u64::MAX;
        let parts_on = compaction_runtime(None);
        assert!(parts_on.parted_segment_enabled);
        assert_eq!(pass_cap(big, &BlockPtrFormat::PartedSegment, &parts_on), big);

        let parts_off = HyperFileRuntimeConfig { parted_segment_enabled: false, ..parts_on.clone() };
        assert_eq!(pass_cap(big, &BlockPtrFormat::PartedSegment, &parts_off), UNBOUNDED_PASS_BYTES);

        let mg = BlockPtrFormat::MicroGroup.max_object_bytes() as u64;
        let cap = pass_cap(big, &BlockPtrFormat::MicroGroup, &parts_on);
        assert!(cap < mg && cap <= UNBOUNDED_PASS_BYTES);

        let small_objects = HyperFileRuntimeConfig {
            segment_max_object_bytes: 1024 * 1024, ..parts_on.clone()
        };
        assert_eq!(pass_cap(big, &BlockPtrFormat::MicroGroup, &small_objects), 1024 * 1024 / 4 * 3,
            "the caller's object size is what hyperfile refuses past");
        assert_eq!(pass_cap(4096, &BlockPtrFormat::MicroGroup, &parts_on), 4096,
            "a smaller budget is kept as given");
    }

    /// The caller's configuration is used as given, except for the conflict
    /// policy, which a compactor cannot leave at retry-and-overwrite.
    #[test]
    fn a_given_runtime_is_kept_but_for_the_conflict_policy() {
        use hyperfile::config::FlushConflictPolicy;
        let given = HyperFileRuntimeConfig {
            data_cache_dirty_min_bytes_to_part: 16 * 1024 * 1024,
            data_cache_dirty_max_bytes_threshold: 16 * 1024 * 1024,
            segment_mpu_chunk_size: 16 * 1024 * 1024,
            flush_conflict_policy: FlushConflictPolicy::RetryLastWriterWins,
            ..HyperFileRuntimeConfig::default()
        };
        let used = compaction_runtime(Some(&given));
        assert_eq!(used.flush_conflict_policy, FlushConflictPolicy::FailFast);
        assert_eq!(HyperFileRuntimeConfig {
            flush_conflict_policy: FlushConflictPolicy::RetryLastWriterWins, ..used
        }, given);
        assert_eq!(compaction_runtime(None).flush_conflict_policy, FlushConflictPolicy::FailFast);
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
            attached: Default::default(),
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
