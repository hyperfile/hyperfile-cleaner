# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Pre-1.0 caveat: while we are still on `0.x`, every minor version (`0.x.0`)
> may contain breaking API or on-disk changes. Read the **Breaking changes**
> section before upgrading.

## [0.10.1] - 2026-10-04

Error kinds follow `hyperfile 0.10.1`'s rule, so a caller decides whether to retry by
the kind alone, whichever crate the error came from.

### Changed

- **`prune` no longer fails with `Interrupted`.** A failed `DeleteObjects` reports the
  kind of its first failure: `ResourceBusy` for a throttled or failing store,
  `TimedOut` or `ConnectionReset` for the network, `PermissionDenied` for a denied
  key, `NotFound` for a missing bucket, `Other` for anything else. By convention
  `Interrupted` means "just retry", which a denied key does not deserve.
  `hyperfile 0.10.1` made the same change to its own batch delete.

  A key that failed inside a `DeleteObjects` that itself succeeded carries only S3's
  error code, so the codes that mean what a status means map to the same kind:
  `AccessDenied` is `PermissionDenied`, `SlowDown` and `InternalError` are
  `ResourceBusy`, and so on.

- **Listing the container's prefix reports the failure's kind**, where it was always
  `Other`. Every operation lists, so a network drop during `collect`, `prune`,
  `report` or `compact` now says `ConnectionReset` or `TimedOut`.

- **`hyperfile` requirement moves to `0.10.1`.** Errors from `hyperfile`'s own calls
  reach the caller through this crate unchanged, and only from 0.10.1 do they follow
  the same rule.

The table both crates answer by:

| kind | retry |
|---|---|
| `ConnectionReset`, `TimedOut`, `ResourceBusy` | yes |
| `AlreadyExists` | not as is |
| `NotFound`, `PermissionDenied`, `InvalidInput`, `InvalidData`, `Unsupported` | no |
| `StaleNetworkFileHandle` | no — reopen |
| `Other` | no |

The mapping is a copy of `hyperfile`'s `s3_error_kind`, which is `pub(crate)`; unit
tests pin both halves of it, the status table and the per-key codes.

### Verified

- 29 unit tests, five of them new: every status in `hyperfile`'s table, through a
  constructed SDK service error as well as directly; timeout and build failures; a
  refused condition recognised by its code in a 200; the per-key codes; and that no
  status or code maps to `Interrupted`.
- Against real S3, through the actual `DeleteObjects` path: a bucket that does not
  exist comes back `NotFound`; a bucket that exists in another region (a 301 the SDK
  does not follow) comes back `Other`, as `hyperfile` reports it.
- `clippy --all-targets` clean with `-D warnings`, by exit status.

## [0.10.0] - 2026-10-04

Follows `hyperfile 0.10.0`. A compaction is no longer seen as a change of content:
it publishes one checkpoint, `hyperfile` marks it `FLAG_COMPACTED`, and
`fs_content_cno()` stays where it was. The rewrite moves to `hyperfile`'s new
`fs_relocate`, so it touches neither `mtime` nor `ctime`.

### Breaking changes

- **`hyperfile` requirement moves from `0.9` to `0.10`.**
- **`CompactOptions` changed shape.** `batch_bytes` and `preserve_times` are gone;
  `max_bytes_rewritten` is now `max_pass_bytes`, defaulting to 512 MiB rather than
  unlimited; `relocate_chunk_bytes` and `allow_consecutive` are new. A literal built
  with `..Default::default()` only needs the renamed field.
- **A pass is one checkpoint, not one per batch.** `CompactOutcome::checkpoints` is
  1 for a pass that rewrote anything and 0 otherwise. A container with more to
  rewrite than `max_pass_bytes` takes several passes; `more_remaining` says so.
- **`compact` refuses to run on a checkpoint that is already a compaction**, with
  `InvalidInput`, unless `allow_consecutive` is set. See below for why.
- **`collect`, `prune`, `report` and `compact` refuse an inline file or a device
  node** with `InvalidInput`. Their `i_last_cno` holds inline data or `rdev`, not a
  checkpoint number. Nothing was deleted on the strength of it before — the summary
  that number named was not found — but the error pointed at a missing segment.

### Why one pass is one checkpoint

`FLAG_COMPACTED` is set only when a publish's number directly follows the previous
one, so it holds for exactly one checkpoint, and `content_cno` is `last_cno − 1`
while it is set. A pass that published once per batch, as 0.9 did, would be marked
on its first checkpoint and move `content_cno` on every one after.

So the pass relocates everything it selected and flushes once. `fs_relocate` does not
check the dirty thresholds itself, and the compacting handle sets them above the pass
so nothing publishes partway. What that costs is memory: the whole pass is held dirty
until the flush, which `max_pass_bytes` bounds. `relocate_chunk_bytes` bounds the
second copy `fs_relocate` holds while it reads.

For the same reason a second pass straight after a first moves `content_cno` once —
its predecessor is marked, so the mark cannot cover both. That is refused by default;
a write in between clears the mark and the next pass is allowed again.
`Report::latest_compacted` says which case a container is in.

### Changed

- **The rewrite is `fs_relocate`**, replacing `fs_read_ahead` + `fs_block_mut` and the
  `fs_setattr` that put the timestamps back. It reads with the read path's
  coalescing, skips holes, explicit zero blocks and blocks already dirty, and touches
  neither `mtime`, `ctime` nor the read cache. `blocks_rewritten` is the count it
  returns — what was marked, not what was planned.
- **`CompactOutcome` reports `content_cno_before` and `content_cno_after`**, and
  `content_moved()` compares them. Equal is the ordinary case. They differ when the
  publish could not be marked: `allow_consecutive` was used, or an earlier flush
  failed and gave its number up, so this publish's number did not follow the last
  one.
- **A pass on a container that is not parted is capped at three quarters of one
  object.** Under any other format a checkpoint is one object, and `hyperfile`
  refuses a flush larger than that. Parted containers split a large checkpoint into
  data parts, so only `max_pass_bytes` applies to them.
- **Objects are taken whole and cheapest first, and the selection stops at the first
  that does not fit** rather than skipping to a smaller one.

### Verified

Against real S3 on `hyperfile 0.10.0`, `examples/compact_smoke`, six shapes:

- **Scattered** (64 blocks, one flush each): `last_cno` 64 → 65 in one checkpoint and
  `content_cno` 64 → 64, both from `fs_content_cno_fast` and from an opened file;
  reading the file went from 64 requests to 2; `report` then says `latest_compacted`.
- **Mostly dead** (a 4,239,360-byte checkpoint 1.5% live): 65,536 bytes rewritten
  recovered 4,145,152. `mtime` **and** `ctime` both unchanged.
- **Foreign key**: refused, then allowed.
- **Sparse, relocated four blocks at a time**: 31 blocks over eight `fs_relocate`
  calls, one checkpoint; `Zero` stayed `Zero`, `Unmapped` stayed `Unmapped`.
- **Consecutive passes** (48 checkpoints, a 16-block budget): the first pass
  relocated 16 with `more_remaining` and `content_cno` 48 → 48; the second was
  refused; forced with `allow_consecutive` it moved `content_cno` 48 → 49, as
  documented; a write then moved it 49 → 51 and cleared the mark; the third pass
  needed no permission and kept it at 51.
- **Unparted** (`MicroGroup`, 32 checkpoints): 31 blocks in one checkpoint,
  `content_cno` 32 → 32, 32 objects down to 2 after `prune`. The first time
  compaction has run on a container that is not parted; 0.7.1 and 0.9.0 listed it as
  untested.

Every shape reads its blocks back correct. `parted_smoke`, `smoke` and `split_smoke`
re-run with answers unchanged. 24 unit tests and the doctest; `clippy --all-targets`
clean with `-D warnings`, checked by exit status.

Still argued from the code rather than tested: the case where an earlier failed flush
leaves this pass's number not following the last publish, which needs a flush to fail
on storage; and the `FailFast` path under a concurrent publish.

## [0.9.0] - 2026-10-04

Follows `hyperfile 0.9.1` and `btree-ondisk 0.20.1`. No source change was needed; what
this release adds is evidence that the object-granular reclaim and compaction hold on
the one new shape `hyperfile 0.9` introduces.

The minor skips `0.8` to line up with the `hyperfile` release this tracks, as `0.7`
did: this crate's public types are that crate's types, and a version that drifts from it
invites the pairing to be guessed.

### Breaking changes

- **`hyperfile` requirement moves from `0.7` to `0.9`, and `btree-ondisk` from `0.20.0`
  to `0.20.1`.** Consumers must move together. `btree-ondisk 0.20.1` is not optional:
  `hyperfile 0.9.1` requires it, because 0.20.0 installed index nodes read back from S3
  over nodes already changed in memory, after which the handle could never flush again.
  Compaction is a writer that flushes repeatedly, so it is on the path that bug sat on.

### What `hyperfile 0.9` changed that matters here

- **A checkpoint may be several objects even when no partial segment was written.**
  Under the parted format, a flush larger than `segment_max_object_bytes` writes
  data-only parts `<segid>.1…` and then part 0. This was the shape this crate's model
  rested on, but only ever exercised with partials. It now has its own check (below).
  The limit defaults to, and cannot exceed, 1 GiB, and is floored at 16 KiB, which is
  what makes the check possible without writing a gigabyte.
- **A writable open takes up a checkpoint that reached storage without its inode**, once
  it checks out, after waiting out 5 s on a fresh one (`0.8.0`). `compact` opens the
  container writable, so this applies to it. Not exercised here: it needs a publish that
  dies between the segment and the inode.
- `HyperFileRuntimeConfig` gained `segment_max_object_bytes` and
  `segment_mpu_max_concurrency`. `compaction_config` builds the struct with
  `..HyperFileRuntimeConfig::default()`, so it is unaffected.

### Verified

- `examples/split_smoke` (new), against real S3, with `segment_max_object_bytes` at
  64 KiB and a 2 MiB checkpoint written in a single flush, so that size alone is why it
  has several objects:
  - Checkpoint 1 is **35 objects**, `1.0` to `1.34`, none written as a partial segment.
  - The 8 parts that each hold one surviving block are referenced and are never offered
    for reclaim; the 27 that hold nothing are.
  - `compact` rewrites the 8 survivors in one checkpoint, after which `prune` leaves no
    object of checkpoint 1, taking storage from 4,468,736 to 2,265,088 bytes.
  - Survivors and overwritten blocks both read back correct.
- `parted_smoke`, `smoke` and `compact_smoke` re-run on `0.9.1` with answers identical to
  `0.7.x`: the six-object parted checkpoint reclaimed exactly, three default-format
  checkpoints all referenced, and the four compaction shapes (scattered, mostly dead,
  foreign key, sparse) unchanged.
- 21 unit tests and the doctest; `clippy --all-targets` clean with `-D warnings`, checked
  by exit status; `cargo publish --dry-run` builds from the packaged directory.

Still argued from the code rather than tested, as in `0.7.1`: the `FailFast` path, where
a concurrent writer publishes during a pass, and compaction of a container written in a
pre-`0.7` unparted format.

## [0.7.2] - 2026-10-04

Fixes a miscount in `compact`'s outcome. Nothing else changed.

### Fixed

- **`CompactOutcome::checkpoints` counted the timestamp restore as a checkpoint.**
  With `preserve_times` on, which is the default, the field read one higher than
  the container's latest checkpoint number actually moved.

  The restore goes through `fs_setattr`, and a flush with nothing dirty but the
  inode's attributes rewrites the inode object only: no segment, no new number. So
  the restore costs a second inode write and moves `last_cno` not at all. A
  checkpoint is one per batch, and that is now what the field says and counts; the
  comment beside the restore, which claimed the opposite, is corrected too.

  Measured against real S3 on a 32-checkpoint file, reported count against the real
  advance of the latest checkpoint number:

  | | advance | reported before | reported now |
  |---|---|---|---|
  | one batch, times restored | +1 | 2 | 1 |
  | one batch, times not restored | +1 | 1 | 1 |
  | 8-block batches over 31 blocks | +4 | 5 | 4 |
  | dry run | 0 | 0 | 0 |

  The `0.7.1` entry below gave the sparse case as 9 checkpoints; it is 8, the same
  overcount, and is corrected there.

### Notes

- **`compact` advances the latest checkpoint number**, by one per batch. It appends
  new checkpoints rather than tidying in place; the old objects become reclaimable
  and `prune` takes them. So after a compaction `high == 0` means the new latest
  checkpoint, which is what lets the following `prune(low, 0)` reclaim the old ones.
  A reader pinned to an older number with `open_cno` loses its objects to that
  `prune`, as it would to any other; compaction only makes objects go dead more
  often.

### Verified

- 21 unit tests and the doctest pass; `clippy --all-targets` clean with
  `-D warnings`, checked by exit status rather than by its last line of output.
- The table above, on real S3, before and after the change.
- `examples/compact_smoke` re-run end to end: scattered, mostly dead, foreign key,
  and sparse-in-batches all answer as before, with the sparse case now reporting 8
  checkpoints.

## [0.7.1] - 2026-09-23

Adds offline compaction: rewriting the live blocks of mostly-dead objects so
`prune` can reclaim them, and so a read of one file range costs fewer requests.
Stays inside `0.7` because nothing existing changed shape — and because the minor
tracks the `hyperfile` release this speaks to.

### Added

- **`S3Cleaner::report(low, high)`** — read-only per-object liveness. For each
  object in range: its stored bytes, how many live data blocks and live metadata
  nodes are in it, and what those occupy. `Report::dead()` is what `prune` would
  take; `Report::compactable(ratio)` estimates what a rewrite would cost and
  free, from the same rule `compact` selects by, so a dry run and the estimate
  agree.

  Counting live bytes needs both halves of `btree-ondisk`'s reachability API.
  `collect_ptrs` reports every live location and `collect_meta_ptrs` the metadata
  subset, and a pointer does not say which it is — while a metadata node is
  `meta_block_size` and a data block is `data_block_size`. The split is also what
  identifies the objects compaction must skip.

- **`S3Cleaner::compact(low, high, &opts)`** and `compact::CompactOptions` —
  rewrites the live blocks of objects at or below `max_live_ratio`, cheapest
  first, in batches of `batch_bytes`. One batch is one checkpoint and one object.
  Deletes nothing: a compaction makes objects reclaimable and `prune` reclaims
  them, which keeps the destructive step in one place.

  The rewrite is `hyperfile`'s own write path, reached through public API rather
  than reimplemented here — a second implementation of the segment format would
  drift from the first. `fs_read_ahead` warms a run of blocks with the read path's
  coalescing; `fs_block_mut` marks a block dirty on acquisition, which produces a
  new version at the next flush without changing a byte; the flush writes the
  dirty set sorted by block index and packed, which is both halves of what
  compaction is for. `fs_block_state` keeps holes and explicit zero blocks out of
  it, so a sparse file stays sparse.

  Which blocks are in a candidate object comes from `fs_block_placement`, which
  answers that per index from the live map. Two other sources were tried and are
  wrong: `SegmentReadWrite::build_block_map` returns the entries of whatever
  B+tree leaves an object happens to hold, not the blocks stored in it — on a
  container written one block per flush it reported checkpoint 34 as holding 34
  blocks when its object held one — and `SegmentSum::blocks`, which is the right
  data, has `pub(crate)` fields.

### Notes on the contract

- **Exclusive write access is required**, and not as a nicety. A compactor writes
  bytes it read earlier, so `hyperfile`'s default `RetryLastWriterWins` would, on
  a conflict, refresh and republish a stale copy over another writer's newer one.
  The compacting handle therefore runs with `FailFast`, where a concurrent publish
  fails the pass instead.
- **A possible log stops the pass.** Keys under the container's prefix that are
  neither the inode nor segment objects are refused (`InvalidInput`, naming one),
  because a WAL is the usual reason and publishing past a log this pass does not
  read would bury entries recovery has not replayed. `allow_foreign_keys`
  overrides it.
- The compacting handle sets `parted_segment_enabled = false` and thresholds above
  the batch, so nothing publishes mid-batch: a threshold crossing would cut the
  output into a partial, which is the fragmentation compaction exists to remove.
- `mtime` and `atime` are restored afterwards; `ctime` moves regardless, since
  `hyperfile` advances it on any attribute update and it is not settable.
  `preserve_times` turns the restore off.

### Verified

- 21 unit tests, including candidate selection (dead objects and
  metadata-holding objects excluded, the ratio as a ceiling, cheapest first),
  run-grouping for `read_ahead`, and that `compactable` answers from the same rule
  the pass selects by.
- Against real S3, `examples/compact_smoke`, four shapes:
  - **Scattered** (64 blocks, one flush each, so 64 objects): 63 candidates,
    rewritten in one batch; reading the whole file went from **64 requests to 2**;
    `prune` then took the 63 objects, 761,856 bytes down to 278,528. Sampled
    blocks read back byte-correct.
  - **Mostly dead** (a 4,239,360-byte checkpoint left 1.5% live by an overwrite):
    16 live blocks rewritten, 65,536 bytes, and `prune` recovered **4,145,152
    bytes** — 63× what the rewrite cost. Survivors and overwritten blocks both
    verified. `mtime` unchanged across the pass, `ctime` advanced.
  - **With a foreign key** (a WAL-shaped object under the prefix): `report` counts
    it, `compact` refuses with `InvalidInput`, and runs once
    `allow_foreign_keys` is set.
  - **Sparse, in four-block batches**: 31 blocks rewritten across 8 checkpoints,
    which is the batch loop firing mid-sweep rather than only at the tail. Block
    states are identical before and after — `Zero` stayed `Zero` and `Unmapped`
    stayed `Unmapped` — so an explicit zero block did not start costing storage
    and a hole did not make the file dense.

  The chunked `block_placement` sweep needs a file larger than 64 Ki blocks to
  loop, so it was exercised by setting the chunk to 8 blocks and repeating all
  four shapes: same numbers throughout.

  Not covered, and argued from the code rather than tested: the `FailFast` path,
  where a concurrent writer publishes during a pass. Reaching it needs a publish
  interleaved inside one `compact` call, which from outside the crate is a race
  rather than a test. Also untested: compaction of a container in a pre-`0.7`
  unparted format.
- `collect` / `prune` re-verified after the refactor that put them on the shared
  container view: the parted and default-format runs answer exactly as before.
- `clippy --all-targets` clean with `-D warnings`.

## [0.7.0] - 2026-09-06

Follows `hyperfile 0.7.0`, where a checkpoint may be written as several objects.
The unit this crate reports and deletes is now a stored object rather than a
checkpoint, and the delete set comes from what is on storage rather than from
arithmetic over the range.

The minor version skips `0.6` to line up with the `hyperfile` release this tracks:
a reader who knows which `hyperfile` a container was written by knows which cleaner
speaks to it, and this crate's public types are that crate's types. `0.6.0` was
tagged locally and never published; everything in it is here.

### Breaking changes

- **`hyperfile` requirement moves from `0.6.11` to `0.7`.** `SegmentId` is no
  longer an alias for `u64`: it is a struct naming a stored object — a checkpoint
  number and, under `BlockPtrFormat::PartedSegment`, which of that checkpoint's
  objects. Consumers must move together, since a `SegmentId` from one build is not
  the other's.

  The requirement names the minor, not a patch, so a `0.7.x` fix arrives without an
  edit here. `hyperfile 0.7.1` is the first one: it repairs the
  `blocking + wal` build, which is the feature pair a WAL-object reclaim in this
  crate would need.

- **`Cleaner::collect` and `Cleaner::prune` take `Cno`, not `SegmentId`.** The
  range has always been a range of checkpoint numbers, and `hyperfile 0.7.0` has
  a name for that. Callers passing integer literals are unaffected — `Cno` is
  `u64` — and `high == 0` still means "the latest published checkpoint", now read
  from `Inode::get_last_cno()`.

- **The returned lists name objects.** On an unparted container that is what they
  always were, and a `SegmentId` prints as the plain number. On a parted one they
  carry a part, so a caller that formats them will see `7.2`.

### Added

- **Parted containers are reclaimable at object granularity.** The referenced set
  is built with the container's own pointer format, read from the published
  inode's `i_meta_config`, so a pointer under `PartedSegment` resolves to the
  object that holds the block rather than to its checkpoint.

  This is not a refinement of checkpoint-granular reclaim, it is what makes any
  reclaim possible there. A checkpoint abandoned by a rollback or an `abort_txn`
  leaves its partials on storage, and the retry **reuses the same checkpoint
  number** — so the dead partials and the live checkpoint share a cno, and
  anything reasoning in whole checkpoints must keep them forever.

  `hyperfile 0.7.0` makes `PartedSegment` the **default**, so this is the ordinary
  path rather than one a caller opts into: every container that release creates
  writes `<cno>.0` objects, and the bare-name case is now only the containers that
  already exist.

  Two kinds of object are kept even when no pointer names them: the summary of
  `high`, which is the object the walk itself reads and would read again on the
  next run; and the summary part of a checkpoint that still has a live part,
  without which the checkpoint's live blocks would be there but unparseable. The
  second uses upstream's own predicate, `SegmentId::is_partial` — a partial is a
  part above zero, and an unparted checkpoint is never one.

### Changed

- **The delete set is what exists, not what the range computes.** `prune` used to
  build a key per checkpoint number in `[low, high]` and let S3 absorb the ones
  that were not there. Under a parted container that produces the wrong names —
  a streamed checkpoint is `<cno>.0`, `<cno>.1`, … with no count the caller knows
  — and deleting a name that does not exist succeeds, so the failure would have
  been a silent no-op rather than an error.

  The objects under the container's prefix are listed instead (one paginated
  `ListObjectsV2`), and names that are not segment objects — the inode, a nested
  WAL prefix — are skipped. This is also the only way an abandoned partial can be
  seen at all: nothing references it and no arithmetic names it.

- **The summary object of `high` is resolved before it is read.** It is part 0 on
  a parted container and the bare name otherwise. `Staging::open` would find it
  either way — given a bare number with no object of that name it tries the
  summary part too — but naming it outright saves a 404 on every container
  `hyperfile 0.7.0` creates.

- A checkpoint number past `u32::MAX` is refused with `InvalidInput`. That is what
  a block pointer can name, and `SegmentId::new_from_cno` asserts on more in a
  debug build.

### Verified

- 14 unit tests, covering the object-name parser round-tripping
  `Segment::segid_to_staging_file_id`, the keys that must not be read as segment
  objects, and each retain rule — including the abandoned-partial case, which
  cannot be forced from outside without fault injection.
- Against real S3, **parted container with real partials** (`examples/parted_smoke`,
  8 MiB written under a 1 MiB dirty threshold): checkpoint 1 comes back as six
  objects `1.0` through `1.5`, all referenced, nothing reclaimable, and a `prune`
  leaves the data intact. Overwriting the file reports exactly those six as
  reclaimable and nothing of checkpoint 2; after pruning them the data reads back
  intact and a second `prune` has nothing to do.
- Against real S3, **unparted container**: `examples/smoke` (3 MiB over 3 flushes)
  reports `[1, 2, 3]` referenced, none reclaimable. A direct-bmap file (2 blocks,
  2 checkpoints) reports both referenced with `prune` a no-op and the data intact;
  overwriting both blocks then makes exactly checkpoints 1 and 2 reclaimable, and
  the data survives pruning them.
- Both of those re-run after `PartedSegment` became the default, where the same
  containers come back as `1.0`, `2.0`, `3.0` and the answers are unchanged.
- `clippy --all-targets` clean with `-D warnings`, and the parted and default-format
  runs repeated against the published `hyperfile 0.7.0` and again on `0.7.1`, rather
  than a local path, with identical results.

## [0.5.0] - 2026-08-27

Moves the reachability walk onto `btree-ondisk 0.20`'s `collect_ptrs`, which
owns the direct/B+tree distinction that this crate used to branch on by hand.
The public API is unchanged.

### Breaking changes

- **`btree-ondisk` requirement moves from `^0.18` to `0.20.0`, `hyperfile` from
  `0.6.10` to `0.6.11`.** cargo treats 0.x minors as incompatible, so a binary
  that links this crate alongside another `btree-ondisk` consumer has to move
  both together or end up with two `btree-ondisk`s in its graph and no way to
  pass a `BMap` between them.

### Changed

- **`collect_segment_id` is one call instead of a shape branch.**
  `btree-ondisk 0.20` added `BMap::collect_ptrs`, which reports every location a
  map references — its metadata nodes and the block pointers in its leaves — for
  both bmap shapes. The B+tree arm (`nonleafnode_iter`) and the direct arm
  (`seek_key` + `lookup`, added in 0.4.0 to fix the data loss described below)
  are both gone, along with the per-slot `checked_add` walk.

  This is not just less code: `collect_ptrs` returns `Err` rather than a short
  list when a subtree cannot be enumerated, so an unreadable metadata node can
  no longer be mistaken for an absent reference. `nonleafnode_iter` had no way to
  report that — and a truncated reachable set is what deletes live data.

- **The bmap root handed to `BMap::read` is aligned by construction.** The walk
  used to copy `i_bmap` out of the segment header into a local; `BMapRawType` is
  `[u8; N]`, whose alignment is 1, so the copy sat wherever the compiler put it,
  while `BMap::read` wants 8-byte alignment. It worked on x86-64 and nothing
  enforced it. The whole `InodeRaw` is moved into the walk now and the field
  borrowed out of it: `InodeRaw` is `align(8)` and `i_bmap` sits at an 8-aligned
  offset within it. Same fix as `hyperfile 0.6.11` made at its own three call
  sites.

- The walk stays on a `spawn_blocking` thread even though `collect_ptrs` is a
  plain `async fn` and needs no `block_in_place`. This crate builds
  `btree-ondisk` with `rc`, so a `BMap` holds `Rc<DirectNode<..>>` and is
  `!Send`; holding one across an await would make `collect` and `prune` non-`Send`
  and break the `Cleaner` trait's `+ Send` bound. Verified by trying it —
  22 errors, all rooted at `Rc<DirectNode<'_, u64>>`.

### Verified

- 4 unit tests and the doctest pass; `clippy --all-targets -D warnings` clean.
- `cargo tree` resolves a single `btree-ondisk 0.20.0`, shared with
  `hyperfile 0.6.11`.
- Against real S3, **direct bmap** (2 blocks over 2 flushes, the shape that hit
  the 0.4.0 data loss): `collect` reports `referenced=[1, 2]`,
  `unreferenced=[]`; `prune` is a no-op and both blocks read back intact.
  Overwriting both blocks in one flush then reports `referenced=[3]`,
  `unreferenced=[1, 2]`; `prune` deletes exactly those two and the new contents
  read back intact.
- Against real S3, **B+tree bmap** via `examples/smoke` (3 MiB over 3 flushes,
  768 data blocks): `referenced=[1, 2, 3]`, `unreferenced=[]`.

## [0.4.0] - 2026-08-27

Raises the `hyperfile` floor past an on-disk layout change, and fixes a
data-loss defect in `prune`: for any file small enough to keep a direct bmap, it
deleted the segments holding that file's only live data. The public API is
unchanged.

### Breaking changes

- **`hyperfile` requirement moves from `^0.3` to `0.6.10`.** `hyperfile 0.4.0`
  reordered `InodeRaw`'s on-disk layout — `i_size`, `i_blocks`, `i_last_seq`,
  `i_last_cno` and `i_bmap` are now a contiguous tail so that tail can double as
  the payload of an inlined small file. The struct is still 160 bytes but the
  serialized offsets differ, so an inode written by a `0.3.x`-based tool is
  misread by `0.4.0` and later, and vice versa. This crate reads the inode out of
  a segment header to find the latest checkpoint, so it has to agree with
  whatever wrote the container.

  Keeping the `^0.3` requirement also forced any binary that links both this
  crate and a `0.6.x` consumer into either a build failure or two mutually
  incompatible `hyperfile`s in the dependency graph.

- Consumers must be rebuilt against `hyperfile 0.6.10`. There is no on-disk
  migration for a container written by a `0.3.x`-based tool.

### Fixed

- **`prune` deleted the live data of any file small enough to keep a direct
  bmap.** `collect_segment_id` derived the referenced set purely from
  `BMap::nonleafnode_iter`, which walks intermediate nodes. A direct bmap has
  none — the root, which lives inside the inode, holds the data block pointers
  itself — so the iterator correctly yielded nothing and the referenced set came
  back *empty*. Every segment was then reported unreferenced, including the ones
  holding the file's only live blocks.

  Confirmed against real S3 before the fix: a two-block file reported
  `0 / 2 segments` reachable, `prune` deleted both segments, and reads came back
  `NoSuchKey`. After the fix the same file reports `2 / 2` reachable and `prune`
  is a no-op, while a subsequent overwrite of both blocks correctly leaves the
  two superseded segments reclaimable and prunes exactly those.

  The direct case is now walked by key (`seek_key` / `lookup`), bounded by the
  root's slot count. `collect_segment_id` returns `Result` accordingly.

- **`collect` and `prune` rejected a file with a single checkpoint.** The segid
  range is documented as inclusive at both ends, and `collect(1, 0)` — the form
  this crate's own documentation gives as the typical call — resolves `high` to
  `get_last_seq()`, so it lands on `low == high` for a file flushed once. The
  guard was `low >= high_segid`, which turned that into
  `invalid segment range [1, 1]`. It is now `low > high_segid`, so only a
  genuinely empty range is an error; that still covers a file that has never
  been flushed (`get_last_seq() == 0`). The check is extracted as
  `validate_segid_range` with unit tests pinning the boundary.

- **`prune` no longer fails on the first segment of a small-flush container.**
  `hyperfile 0.4.6` fixed a defect where `Staging::open` always read
  `SEGMENT_HEADER_FETCH_SIZE` (512 KiB) into a buffer that `do_get_object` then
  rejected for being longer than the object, and computed
  `actual_ss_bytes - SEGMENT_HEADER_FETCH_SIZE` unconditionally, which
  underflowed for exactly those segments. `prune` calls `open` to read each
  segment summary, so it failed immediately.

  Segments smaller than 512 KiB arise whenever a caller flushes small batches
  often — a filesystem over a single hyperfile flushes once per filesystem
  commit, so its segments are tens of KiB. `hyperfile`'s own workloads use 100
  MiB to 1 GiB segment buffers, which is why nothing hit this earlier.

### Changed

- `S3BlockLoader::new` gained a fourth parameter, `Arc<ReadTiming>`, so a
  loader's object-request counters belong to the file that owns them. The bmap
  walk in `collect_unref_seg` has no staging handle to borrow them from and
  nowhere to report them — the cleaner's public API exposes no counters — so it
  constructs a throwaway `ReadTiming` rather than growing the API to thread one
  through. No change to this crate's own signatures.

### Verified

- 4 unit tests, including the `validate_segid_range` boundary cases.
- Against real S3, via a downstream CLI:
  - **Direct bmap** (2 blocks, 2 segments): `2 / 2` reachable, `prune --apply` a
    no-op, both blocks read back intact. Overwriting both blocks then leaves
    exactly the two superseded segments unreferenced, and `prune` deletes those
    two and nothing else, with the data still intact afterwards.
  - **B+tree bmap** (40 blocks over 40 flushes, root at level 2): `40 / 40`
    reachable, nothing reclaimable, all 40 blocks intact.
  - **Single checkpoint**: `collect` and `prune` now answer instead of returning
    `invalid segment range [1, 1]`.
- `examples/smoke` against real S3 (3 flushes; `referenced=[1,2,3]`,
  `unreferenced=[]`).
- Earlier, on `0.4.6`: `collect`/`prune` against a container written by a
  `0.4.6`-based filesystem — 203 segments walked, split correct, data intact
  after pruning.
- `clippy --all-targets` clean with `-D warnings`.

## [0.3.0] - 2026-05-27

### Added

- `S3Cleaner::with_client`, for callers that already have a configured
  `aws_sdk_s3::Client`.
- Crate-level and per-item rustdoc; README rewritten with a runnable example.
- Unit test `delete_empty_keys_is_noop` and `examples/smoke.rs` (3-flush write
  then `collect`), verified against a real bucket.

### Fixed

- **Per-key `DeleteObjects` failures are surfaced.** `DeleteObjects` is a
  partial-success API: an `Ok` response can still carry an `errors` list, so a
  prune could report success while leaving orphaned objects in S3. The response
  is now parsed and per-key failures are merged with SDK-level batch failures
  into one `Interrupted` error carrying the failed-object count and a sample key.
- `S3Cleaner::new` returns `InvalidInput` on a bad URI instead of panicking.
- `collect_unref_seg` converts a `JoinError` to `io::Error` instead of
  panicking.
- Empty `delete_keys` short-circuits.
- `MaybeUninit::zeroed().assume_init()` replaced with `InodeRaw::default()`.

### Changed

- `hyperfile` `0.2.0` → `0.3` (with `blocking`), `btree-ondisk` `0.16` → `0.18`,
  adapting to the new `BMap::read -> Result<Self>` signature.
- `tokio` features narrowed from `full` to `rt + rt-multi-thread + macros`.

## [0.2.0] - 2025-09-24

First packaged release: `collect` and `prune` split into separate APIs, crate
metadata (README, license, manifest), and dependency updates.
