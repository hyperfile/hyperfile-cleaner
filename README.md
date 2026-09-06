# Hyperfile Cleaner

Segments cleaner for [Hyperfile](https://crates.io/crates/hyperfile).

## What it does

Hyperfile is a copy-on-write log-structured file format on S3. Each
flush produces a new segment object. Over time, older segments lose
references as the metadata tree gets rewritten. This crate
enumerates and (optionally) deletes segment objects that are no
longer referenced by the latest published checkpoint.

Two operations on the [`Cleaner`] trait:

- **`collect(low, high)`** — dry-run inspection. Walks the metadata
  tree, returns `(referenced, unreferenced)` lists of segment
  objects.
- **`prune(low, high)`** — `collect` + delete the unreferenced list
  via S3 `DeleteObjects`.

`low` and `high` are checkpoint numbers (`Cno`), inclusive at both
ends; `high == 0` is a sentinel for "use the latest checkpoint".

What is reported and deleted is a `SegmentId`, which names a stored
object. Under `BlockPtrFormat::PartedSegment` one checkpoint may be
several objects — `7.0`, `7.1`, … — and they are reclaimed
separately, because a checkpoint abandoned partway leaves its
partials behind under the same number the retry then reuses. Under
every other format an object is a checkpoint and prints as the
plain number.

## Usage

```rust,no_run
use hyperfile_cleaner::{Cleaner, s3::S3Cleaner};

# async fn run() -> std::io::Result<()> {
let cleaner = S3Cleaner::new("s3://my-bucket/path/to/file").await?;

// Dry-run: see what would be deleted.
let (refed, unrefed) = cleaner.collect(1, 0).await?;
println!("referenced segments  : {:?}", refed);
println!("unreferenced segments: {:?}", unrefed);

// Actually delete.
cleaner.prune(1, 0).await?;
# Ok(())
# }
```

For tools that already have a configured `aws_sdk_s3::Client`, use
`S3Cleaner::with_client(&client, root_uri)` instead of `new`.

## Limitations

- The cleaner does not coordinate with in-flight writers. It
  snapshots the inode at the start of the call. New segments
  published after that point are outside the configured range and
  remain untouched, as are the partials of a checkpoint a writer has
  open — those carry the number above the published one.
- Compaction (rewriting partially-live segments) is not supported.

## Status

> :warning: Under development. Not recommended for production.

## License

Apache-2.0.

## Contributing

PRs welcome.
