# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Pre-1.0 caveat: while we are still on `0.x`, every minor version (`0.x.0`)
> may contain breaking API or on-disk changes. Read the **Breaking changes**
> section before upgrading.

## [0.6.0] - 2026-09-05

Follows `hyperfile 0.7.0`, where a checkpoint may be written as several objects.
The unit this crate reports and deletes is now a stored object rather than a
checkpoint, and the delete set comes from what is on storage rather than from
arithmetic over the range.

### Breaking changes

- **`hyperfile` requirement moves from `0.6.11` to `0.7.0`.** `SegmentId` is no
  longer an alias for `u64`: it is a struct naming a stored object — a checkpoint
  number and, under `BlockPtrFormat::PartedSegment`, which of that checkpoint's
  objects. Consumers must move together, since a `SegmentId` from one build is not
  the other's.

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
  runs repeated against the published `hyperfile 0.7.0` from crates.io rather than a
  local path, with identical results.

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
