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
object. Under `BlockPtrFormat::PartedSegment` — the default since
`hyperfile 0.7.0` — and `StreamSegment` (`hyperfile 0.11.0`), one
checkpoint may be several objects, `7.0`, `7.1`, … — and they are
reclaimed separately, because a checkpoint
abandoned partway leaves its partials behind under the same number
the retry then reuses. Under every other format an object is a
checkpoint and prints as the plain number.

## Compaction

`prune` reclaims an object nothing points into. An object that is 5%
live is not that object: rewriting those few blocks elsewhere is what
turns it into one `prune` can take. The same rewrite is what makes a
read cheaper, since a request breaks where the object changes.

Two more operations, on `S3Cleaner` itself:

- **`report(low, high)`** — read-only. Per-object liveness and what
  could be given back: `dead()` is `prune`'s work, `compactable(ratio)`
  estimates what a rewrite would cost and free.
- **`compact(low, high, &opts)`** — rewrites the live blocks of objects
  at or below `max_live_ratio`, as one checkpoint. Deletes nothing;
  `prune` does that afterwards.

```rust,no_run
use hyperfile_cleaner::{Cleaner, compact::CompactOptions, s3::S3Cleaner};

# async fn run() -> std::io::Result<()> {
let cleaner = S3Cleaner::new("s3://my-bucket/path/to/file").await?;

let report = cleaner.report(1, 0).await?;
let (objects, rewrite, freed) = report.compactable(0.5);
println!("{objects} objects: rewrite {rewrite} bytes to free {freed}");

let out = cleaner.compact(1, 0, &CompactOptions::default()).await?;
assert!(!out.content_moved());
cleaner.prune(1, 0).await?;
# Ok(())
# }
```

The rewrite is `hyperfile`'s, not this crate's: `fs_relocate` marks the
blocks to be rewritten with their current bytes, reading them with the
read path's own coalescing, and one flush writes them sorted by block
index and packed. Holes and explicit zero blocks are left alone, so a
sparse file stays sparse, and neither `mtime` nor `ctime` moves.

**A compaction is not a change of content.** It has to publish a new
checkpoint, so `last_cno` advances by one. `hyperfile` marks that
checkpoint `FLAG_COMPACTED`, and `fs_content_cno()` stays where it was:
that is the number to compare when asking whether a file changed, not
`last_cno`. The mark holds for one checkpoint, so two passes in a row
would move `content_cno` on the second; `compact` refuses to start on a
checkpoint that is already marked unless `allow_consecutive` is set.
`CompactOutcome::content_moved()` says whether consumers will see the
pass as a change — they will when an earlier flush failed and gave its
number up, which the mark does not span.

**Compaction requires exclusive write access.** It is a writer, and it
writes bytes it read earlier, so it runs with
`FlushConflictPolicy::FailFast` and a concurrent publish makes the pass
fail rather than overwrite the other writer's newer data. It also
refuses to run when the container's prefix holds keys that are neither
the inode nor segment objects, because a WAL is the usual reason and a
compactor that does not read the log would publish over entries
recovery has not replayed.

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

## Choosing the hyperfile build

`hyperfile` comes in two builds that cannot share a binary: `blocking`, its
direct-API build, and `reactor`, its default. They link `btree-ondisk` with `rc`
and `arc` nodes respectively. This crate follows whichever is selected:

```toml
# default: hyperfile's `blocking` build
hyperfile-cleaner = "0.10"

# alongside a crate that uses hyperfile with its default features
hyperfile-cleaner = { version = "0.10", default-features = false, features = ["reactor"] }
```

Selecting both, or neither, fails to build — in `btree-ondisk`, which is compiled
first, so its error is the one shown.

`collect`, `prune` and `report` return futures that are `Send` in both builds.
`compact`'s is not, in either: it drives a `Hyper`, which under `blocking` holds
`Rc` nodes and under `reactor` is not `Sync` while its `fs_flush` holds a `&Hyper`
across an await. Await it where it is created rather than spawning it.

## Limitations

- The cleaner does not coordinate with in-flight writers. It
  snapshots the inode at the start of the call. New segments
  published after that point are outside the configured range and
  remain untouched, as are the partials of a checkpoint a writer has
  open — those carry the number above the published one.
- Compaction is offline: it needs exclusive write access for the pass,
  and a container holding a WAL is refused unless the caller overrides
  it. Compacting a container a writer is actively publishing to needs a
  background task inside `hyperfile`, not a separate process.
- On a parted container a pass of any size is written out as partial
  segments as it goes, so it holds no more than the dirty-data limits
  in memory, and it is still one checkpoint. Where no partial can be
  written — a container that is not parted, or partials turned off — a
  pass holds everything it relocates until its one flush and is capped
  at `UNBOUNDED_PASS_BYTES` (512 MiB). A container with more to rewrite
  than one pass takes several, and each pass after the first in a row
  moves `content_cno` once.
- The handle that rewrites uses `CompactOptions::runtime` if given —
  pass the configuration the container's writer uses, and compaction
  writes objects of the same shape — and otherwise
  `compaction_runtime_default()`, 64 MiB partials. Only
  `flush_conflict_policy` is overridden, to `FailFast`.
- An inline file or a device node has no segments; `collect`, `prune`,
  `report` and `compact` refuse them.
- An object holding a live metadata node is not a compaction candidate,
  except a data part of a `StreamSegment` container. A node is relocated
  when the flush rewrites it, which needs something under it dirtied,
  and which keys a node covers is not visible from here; the stream
  layout is the exception because it keeps every leaf in the object
  with its data, so rewriting the data rewrites the leaf.
- A container whose inode records a format this build does not know is
  refused. Cleaners up to 0.10.5 instead read it as `MicroGroup`, and
  on a `StreamSegment` container that reports every object unreferenced:
  **do not run a cleaner older than 0.11 on a container written by
  hyperfile 0.11 in the stream format.**

## Status

> :warning: Under development. Not recommended for production.

## License

Apache-2.0.

## Contributing

PRs welcome.
