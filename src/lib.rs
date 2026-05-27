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

use std::io::Result;
use hyperfile::SegmentId;

pub mod s3;

/// Operations supported by a hyperfile cleaner backend.
///
/// Both methods take a `[low, high]` segment-id range. `high == 0`
/// is a sentinel meaning "use the latest published checkpoint
/// (`Inode::get_last_seq`)". `low` is inclusive; a typical value is
/// `1` to cover the entire file history.
pub trait Cleaner {
    /// Walk the metadata tree of the checkpoint identified by `high`
    /// (or the latest if `high == 0`) and return
    /// `(referenced, unreferenced)` segment id lists within
    /// `[low, high]`.
    ///
    /// Useful for dry-run inspection before invoking [`Cleaner::prune`].
    fn collect(
        &self,
        low: SegmentId,
        high: SegmentId,
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
        low: SegmentId,
        high: SegmentId,
    ) -> impl Future<Output = Result<()>> + Send;
}
