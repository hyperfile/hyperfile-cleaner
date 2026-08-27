# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Pre-1.0 caveat: while we are still on `0.x`, every minor version (`0.x.0`)
> may contain breaking API or on-disk changes. Read the **Breaking changes**
> section before upgrading.

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
