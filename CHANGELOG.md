# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Pre-1.0 caveat: while we are still on `0.x`, every minor version (`0.x.0`)
> may contain breaking API or on-disk changes. Read the **Breaking changes**
> section before upgrading.

## [0.4.0] - 2026-08-27

A dependency release: the crate's own API is unchanged, but it can now only read
containers written by `hyperfile 0.4.0` or later, which is why this is a minor
bump rather than a patch.

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

- `examples/smoke` against real S3 (3 flushes; `referenced=[1,2,3]`,
  `unreferenced=[]`).
- `collect` and `prune` driven from a downstream CLI over a 5-checkpoint
  container: the referenced/unreferenced split matched, the unreferenced segment
  was actually deleted, and the data read back intact afterwards.
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
