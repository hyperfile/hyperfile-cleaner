//! `hyperfile-cleaner` enumerates and removes unreferenced
//! segments from a [hyperfile](https://crates.io/crates/hyperfile).
//!
//! ## Model
//!
//! A hyperfile is stored on S3 as a single inode object plus a
//! sequence of segment objects. Each segment is keyed by a
//! monotonically increasing `SegmentId` and contains both the
//! data blocks written in that flush and the metadata B+tree
//! nodes that index them.
//!
//! Because hyperfile is copy-on-write, segments accumulate as
//! the file is modified. Once a segment is no longer referenced
//! by the latest published checkpoint's metadata tree, its
//! storage can be reclaimed.
//!
//! The [`Cleaner`] trait provides two operations:
//!
//! - [`Cleaner::collect`] walks the metadata tree of the latest
//!   (or a caller-specified) checkpoint and returns
//!   `(referenced, unreferenced)` segment lists for a
//!   `[low, high]` segid range. Useful for dry-run / inspection.
//! - [`Cleaner::prune`] performs the same walk and additionally
//!   deletes the unreferenced segments via S3 `DeleteObjects`.
//!
//! ## Concurrency caveat
//!
//! The cleaner does not coordinate with in-flight writers. It
//! takes a snapshot of the inode at the start of the call and
//! operates on that. New segments published after that point are
//! always outside the configured range (because `high` was fixed
//! at snapshot time), so the prune is safe against concurrent
//! appends. It is, however, the caller's responsibility to ensure
//! `low` is not also being actively reused.
//!
//! ## Example
//!
//! ```no_run
//! use hyperfile_cleaner::{Cleaner, s3::S3Cleaner};
//!
//! # async fn run() -> std::io::Result<()> {
//! let cleaner = S3Cleaner::new("s3://my-bucket/path/to/file").await?;
//!
//! // Inspect what would be deleted (dry-run): pass high=0 to
//! // mean "use the latest checkpoint".
//! let (refed, unrefed) = cleaner.collect(1, 0).await?;
//! println!("referenced  : {:?}", refed);
//! println!("unreferenced: {:?}", unrefed);
//!
//! // Actually delete the unreferenced range.
//! cleaner.prune(1, 0).await?;
//! # Ok(())
//! # }
//! ```

#[cfg(all(feature = "blocking", feature = "reactor"))]
compile_error!(
    "features `blocking` and `reactor` are mutually exclusive: they select \
     hyperfile builds that link btree-ondisk `rc` and `arc` respectively. \
     For `reactor`, set `default-features = false`."
);
#[cfg(not(any(feature = "blocking", feature = "reactor")))]
compile_error!(
    "select a hyperfile build: feature `blocking` (the default) or `reactor`"
);

use std::io::Result;
use hyperfile::{Cno, SegmentId};

pub mod compact;
pub mod s3;

/// Operations supported by a hyperfile cleaner backend.
///
/// Both methods take a `[low, high]` range of **checkpoint numbers**
/// ([`Cno`]). `high == 0` is a sentinel meaning "use the latest published
/// checkpoint (`Inode::get_last_cno`)". `low` is inclusive; a typical value
/// is `1` to cover the entire file history.
///
/// The range is inclusive at *both* ends, so `low == high` is valid and
/// answered normally — which is what `collect(1, 0)` resolves to on a file with
/// a single checkpoint. Only `low > high` is an error, and that also covers a
/// file that has never been flushed (`get_last_cno() == 0`), since there is no
/// metadata tree to walk.
///
/// What the methods *report and delete* is [`SegmentId`], which names a stored
/// object rather than a checkpoint: a checkpoint written under
/// `BlockPtrFormat::PartedSegment` is several objects — `<cno>.0`, `<cno>.1`, …
/// — and they are not reclaimable together. Under every other format an object
/// and a checkpoint are the same thing and a `SegmentId` prints as the plain
/// number.
pub trait Cleaner {
    /// Walk the metadata tree of the checkpoint identified by `high`
    /// (or the latest if `high == 0`) and return
    /// `(referenced, unreferenced)` lists of segment objects, the
    /// unreferenced one limited to checkpoints within `[low, high]`.
    ///
    /// Useful for dry-run inspection before invoking [`Cleaner::prune`].
    fn collect(
        &self,
        low: Cno,
        high: Cno,
    ) -> impl Future<Output = Result<(Vec<SegmentId>, Vec<SegmentId>)>> + Send;

    /// Walk the metadata tree as in [`Cleaner::collect`] and then
    /// delete the unreferenced segment objects from the underlying
    /// store.
    ///
    /// Errors if any individual delete batch fails; in that case
    /// previously-deleted batches stay deleted (S3 has no
    /// transactional batch). The returned error message mentions
    /// the failed object count and a sample failed key.
    fn prune(
        &self,
        low: Cno,
        high: Cno,
    ) -> impl Future<Output = Result<()>> + Send;
}
