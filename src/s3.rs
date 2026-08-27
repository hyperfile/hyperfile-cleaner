//! S3-backed cleaner implementation.
//!
//! `S3Cleaner` enumerates the segments referenced by the latest
//! published inode of a hyperfile and (optionally) deletes anything
//! in a configured `[low, high]` segid range that isn't referenced.
//!
//! See the crate-level [`Cleaner`] trait for the public API shape.

use std::io::{Result, Error, ErrorKind};
use std::collections::BTreeSet;
use std::sync::Arc;
use aws_sdk_s3::Client;
use log::{debug, error};
use btree_ondisk::bmap::BMap;
use btree_ondisk::BlockLoader;
use btree_ondisk::NullNodeCache;
use hyperfile::{BlockIndex, BlockPtr, SegmentId};
use hyperfile::file::ReadTiming;
use hyperfile::meta_loader::s3::S3BlockLoader;
use hyperfile::ondisk::InodeRaw;
use hyperfile::inode::{Inode, OnDiskState};
use hyperfile::segment::{Segment, SegmentSum, SegmentReadWrite};
use hyperfile::staging::{Staging, config::StagingConfig, s3::S3Staging};
use hyperfile::config::HyperFileRuntimeConfig;
use hyperfile::s3uri::S3Uri;
use hyperfile::meta_format::BlockPtrFormat;
use crate::Cleaner;

/// S3-backed cleaner for a single hyperfile rooted at a given
/// `s3://<bucket>/<prefix>` URI.
///
/// The cleaner does not retain or open the file's data. It only
/// reads the published inode and walks the metadata B+tree to
/// enumerate referenced segments, then deletes the unreferenced
/// ones via `DeleteObjects`.
///
/// Concurrency: the cleaner is not aware of in-flight writers. If
/// a writer publishes a new checkpoint while `collect` /
/// `prune` is running, the result reflects whatever inode was
/// loaded at the start of the call. New segments published after
/// that point are outside the configured `[low, high]` range and
/// are not affected.
pub struct S3Cleaner {
    client: Client,
    bucket: String,
    root_path: String,
    staging: S3Staging,
}

impl S3Cleaner {
    /// Construct a cleaner for `root_uri`, using the AWS SDK's
    /// default credential / region resolution
    /// (`aws_config::load_from_env`).
    ///
    /// Returns `InvalidInput` if `root_uri` is not a valid S3 URI.
    pub async fn new(root_uri: &str) -> Result<Self> {
        let cfg = aws_config::load_from_env().await;
        let client = Client::new(&cfg);
        Self::with_client(&client, root_uri).await
    }

    /// Construct a cleaner using a caller-supplied `Client`. Use
    /// this when you need to control region / endpoint / credential
    /// resolution explicitly (for example, in tests or in a tool
    /// that already has a configured client).
    ///
    /// Returns `InvalidInput` if `root_uri` is not a valid S3 URI.
    pub async fn with_client(client: &Client, root_uri: &str) -> Result<Self> {
        let trimmed = root_uri.strip_suffix('/').unwrap_or(root_uri);
        let s3uri = S3Uri::parse(trimmed).map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid S3 URI {:?}: {}", root_uri, e),
            )
        })?;
        let runtime_config = HyperFileRuntimeConfig::default();
        let staging_config = StagingConfig::new_s3_staging(s3uri.bucket, "", s3uri.key, None);
        let staging = S3Staging::from(client, staging_config, runtime_config).await?;
        Ok(Self {
            client: client.clone(),
            bucket: s3uri.bucket.to_owned(),
            root_path: s3uri.key.to_owned(),
            staging,
        })
    }

    async fn load_segment_sum(&self, segid: SegmentId) -> Result<SegmentSum> {
        self.staging.open(segid).await
    }

    async fn load_raw_inode(&self) -> Result<(InodeRaw, Option<OnDiskState>)> {
        let mut raw_inode = InodeRaw::default();
        let inode_state = self.staging.load_inode(raw_inode.as_mut_u8_slice()).await?;
        Ok((raw_inode, inode_state))
    }

    /// Collect every segment id that `bmap` still points into.
    ///
    /// `collect_ptrs` reports every location the map references — its metadata
    /// nodes and the block pointers stored in its leaves — for both bmap shapes,
    /// so this needs no shape branch of its own. It returns `Err` rather than a
    /// short list if any subtree could not be enumerated, which is what makes it
    /// safe to derive a delete set from: a truncated reachable set is
    /// indistinguishable from a complete one.
    ///
    /// This replaces a hand-written walk that branched on `get_stat().btree` and
    /// used `nonleafnode_iter` for the B+tree shape. That iterator yields
    /// nothing for a direct bmap, whose root lives inside the inode and holds
    /// the data block pointers itself, so the referenced set came back *empty*
    /// for every file small enough to keep one and `prune` deleted that file's
    /// only live data. `btree-ondisk 0.20` owns the shape distinction now.
    fn collect_segment_id<L>(
        bmap: BMap<'_, BlockIndex, BlockPtr, BlockPtr, L, NullNodeCache>,
    ) -> Result<BTreeSet<SegmentId>>
    where
        L: BlockLoader<BlockPtr> + Clone,
    {
        let mut segid_set: BTreeSet<SegmentId> = BTreeSet::new();
        // `collect_ptrs` is a plain `async fn`, but a bmap built with
        // btree-ondisk's `rc` feature is `!Send`, so awaiting it directly
        // would make `collect`/`prune` non-`Send` and break the `Cleaner`
        // trait. The walk therefore stays on the blocking thread this
        // function is called from, driven the same way btree-ondisk drives
        // itself under `tokio-runtime`.
        let handle = tokio::runtime::Handle::current();
        for blkptr in handle.block_on(bmap.collect_ptrs())? {
            if BlockPtrFormat::is_on_staging(&blkptr) {
                segid_set.insert(BlockPtrFormat::decode_segid(&blkptr));
            }
        }
        Ok(segid_set)
    }

    /// Delete `delete_keys` from `bucket` in batches of 1000 (the
    /// `DeleteObjects` per-call max).
    ///
    /// Failures are tracked at two levels and merged into the same
    /// returned error:
    ///
    /// 1. **SDK-level (transport / auth / etc.)**: the entire batch
    ///    is recorded as failed. The SDK error is logged at
    ///    `error!` level.
    /// 2. **Per-key (S3 partial success in `DeleteObjectsOutput`)**:
    ///    `DeleteObjects` is a partial-success API; even when the
    ///    batch returns `Ok`, individual keys may have failed (e.g.
    ///    `AccessDenied`, `NoSuchKey`). The response's `errors`
    ///    list is parsed and each failing key recorded.
    ///
    /// Returns `Err(Interrupted)` if any key (at either level)
    /// failed. The error message includes the failed object count
    /// and a sample failed key. Successfully-deleted keys stay
    /// deleted (S3 has no transactional batch delete).
    async fn do_delete_objects(
        client: &Client,
        bucket: &str,
        delete_keys: Vec<String>,
    ) -> Result<()> {
        use aws_sdk_s3::types::ObjectIdentifier;
        if delete_keys.is_empty() {
            return Ok(());
        }
        let mut failed_keys: Vec<String> = Vec::new();
        for keys in delete_keys.chunks(1000) {
            let obj_ids = keys.iter()
                .map(|k| {
                    ObjectIdentifier::builder()
                        .key(k)
                        .build()
                        .expect("ObjectIdentifier requires non-empty key")
                })
                .collect::<Vec<ObjectIdentifier>>();
            // We use quiet=true so the response only carries the
            // `errors` list (not the success list); this is what
            // we want for partial-failure tracking.
            let delete = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(obj_ids))
                .quiet(true)
                .build()
                .expect("Delete::builder requires non-empty object list");
            match client.delete_objects()
                .bucket(bucket)
                .delete(delete)
                .send()
                .await
            {
                Ok(output) => {
                    // Partial-success path: even though the batch
                    // returned Ok at the SDK level, individual keys
                    // may have failed. Pick those out of the
                    // response.
                    if let Some(errors) = output.errors {
                        for e in errors {
                            let key = e.key.unwrap_or_default();
                            let code = e.code.as_deref().unwrap_or("?");
                            let msg = e.message.as_deref().unwrap_or("");
                            error!("DeleteObjects per-key failure: key={} code={} message={}",
                                key, code, msg);
                            if !key.is_empty() {
                                failed_keys.push(key);
                            }
                        }
                    }
                },
                Err(sdk_err) => {
                    // SDK-level failure: treat the whole batch as
                    // failed (we don't know which keys made it).
                    error!("DeleteObjects batch failed (bucket={}, count={}): {}",
                        bucket, keys.len(), sdk_err);
                    failed_keys.extend(keys.iter().cloned());
                }
            }
        }
        if failed_keys.is_empty() {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Interrupted,
                format!(
                    "DeleteObjects failed for {} object(s); first failed key: {:?}",
                    failed_keys.len(),
                    failed_keys.first(),
                ),
            ))
        }
    }

    /// Inner shared implementation used by both `collect` and
    /// `prune`. Returns `(referenced, unreferenced)` segment id
    /// vectors where:
    ///  - `referenced` is the set of segids reachable via the
    ///    latest published inode's metadata tree, intersected with
    ///    `[low, high]`.
    ///  - `unreferenced` is `[low, high]` minus the referenced set.
    ///
    /// `high == 0` is a sentinel meaning "use the latest checkpoint
    /// (`inode.get_last_seq()`)".
    async fn collect_unref_seg(
        &self,
        low: SegmentId,
        high: SegmentId,
    ) -> Result<(Vec<SegmentId>, Vec<SegmentId>)> {
        let high_segid = if high == 0 {
            let (raw_inode, inode_state) = self.load_raw_inode().await?;
            let inode = Inode::from_raw(&raw_inode, inode_state);
            inode.get_last_seq()
        } else {
            high
        };

        debug!("collect unreferenced segment low: {}, high: {}", low, high_segid);
        validate_segid_range(low, high_segid)?;

        // load segment summary for the high checkpoint
        let ss = self.load_segment_sum(high_segid).await?;
        // The whole inode is moved into the walk, not just its `i_bmap` field:
        // `BMap::read` wants an 8-byte-aligned buffer, and `BMapRawType` is
        // `[u8; N]`, whose own alignment is 1, so a copy of the field alone
        // lands wherever the compiler put it. `InodeRaw` is `align(8)` and
        // `i_bmap` sits at an 8-aligned offset within it, so borrowing the
        // field out of an owned inode is aligned by construction.
        let raw_inode = ss.hdr.s_inode;
        let meta_block_size = 1 << ss.hdr.s_meta_blk_shift;

        // Walk the bmap on a blocking thread: a bmap built with
        // btree-ondisk's `rc` feature is `!Send`, so it cannot be held
        // across an await in a `Cleaner` method.
        // Counters are per-loader and the cleaner has nowhere to report
        // them, so give this walk its own throwaway ReadTiming rather
        // than threading one through the public API.
        let loader = S3BlockLoader::new(
            &self.client, &self.bucket, &self.root_path,
            Arc::new(ReadTiming::default()),
        );
        let join = tokio::task::spawn_blocking(move || -> Result<BTreeSet<SegmentId>> {
            let bmap = BMap::<BlockIndex, BlockPtr, BlockPtr, S3BlockLoader, NullNodeCache>
                ::read(&raw_inode.i_bmap, meta_block_size, loader, NullNodeCache)?;
            Self::collect_segment_id(bmap)
        });
        let set = match join.await {
            Ok(inner) => inner?,
            Err(join_err) => {
                return Err(Error::other(
                    format!("bmap walk task failed to join: {}", join_err),
                ));
            }
        };

        // Build the unreferenced list within [low, high].
        let mut non_ref = Vec::new();
        for n in low..=high_segid {
            if !set.contains(&n) {
                non_ref.push(n);
            }
        }
        let v_ref = set.into_iter().collect::<Vec<SegmentId>>();
        Ok((v_ref, non_ref))
    }
}

/// Reject a segid range that cannot be walked.
///
/// `[low, high]` is inclusive at both ends, so `low == high_segid` is a
/// well-formed query — "what does checkpoint N reference, and is anything in
/// `[N, N]` unreferenced" — and is answered normally. It is reached whenever a
/// caller asks about the entire history of a file with a single checkpoint:
/// `collect(1, 0)`, the form this crate's own documentation gives as the typical
/// call, resolves `high` to `get_last_seq() == 1` and so lands on `low == high`.
/// Rejecting it made `collect` and `prune` unusable on any freshly created file
/// until it had been flushed at least twice.
///
/// `low > high_segid` is genuinely empty and stays an error, which also covers
/// `high_segid == 0` (no checkpoint has ever been published, so there is no
/// metadata tree to walk).
fn validate_segid_range(low: SegmentId, high_segid: SegmentId) -> Result<()> {
    if low > high_segid {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("invalid segment range [{}, {}]", low, high_segid),
        ));
    }
    Ok(())
}

impl Cleaner for S3Cleaner {
    async fn collect(
        &self,
        low: SegmentId,
        high: SegmentId,
    ) -> Result<(Vec<SegmentId>, Vec<SegmentId>)> {
        self.collect_unref_seg(low, high).await
    }

    /// Prune unreferenced segments in `[low, high]`. Implemented
    /// as `collect` followed by `DeleteObjects` of the
    /// unreferenced list.
    async fn prune(&self, low: SegmentId, high: SegmentId) -> Result<()> {
        let (_, non_ref) = self.collect_unref_seg(low, high).await?;
        let delete_keys = non_ref.iter()
            .map(|segid| format!(
                "{}/{}",
                self.root_path,
                Segment::segid_to_staging_file_id(*segid),
            ))
            .collect::<Vec<String>>();
        debug!("unreferenced segments to delete: {:?}", delete_keys);
        Self::do_delete_objects(&self.client, &self.bucket, delete_keys).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builder calls in `do_delete_objects` panic only on empty
    /// inputs that we guard against. Verify the empty-input fast
    /// path.
    #[tokio::test]
    async fn delete_empty_keys_is_noop() {
        let cfg = aws_config::load_from_env().await;
        let client = Client::new(&cfg);
        // Empty list -> no SDK call -> Ok regardless of credentials.
        let r = S3Cleaner::do_delete_objects(&client, "any-bucket", Vec::new()).await;
        assert!(r.is_ok(), "empty key list should not call S3");
    }

    /// `[low, high]` is inclusive at both ends, so a single-checkpoint
    /// file -- where `collect(1, 0)` resolves to `low == high == 1` --
    /// must be accepted. It used to be rejected, which made the
    /// documented call form fail on any file flushed only once.
    #[test]
    fn segid_range_accepts_a_single_checkpoint() {
        assert!(validate_segid_range(1, 1).is_ok());
    }

    #[test]
    fn segid_range_accepts_a_normal_span() {
        assert!(validate_segid_range(1, 5).is_ok());
    }

    /// Only a genuinely empty range is an error. `high_segid == 0` is the
    /// "no checkpoint published yet" case and falls out of the same rule.
    #[test]
    fn segid_range_rejects_an_empty_span() {
        for (low, high) in [(5u64, 3u64), (2, 1), (1, 0)] {
            let e = validate_segid_range(low, high)
                .expect_err("low > high must be rejected");
            assert_eq!(e.kind(), ErrorKind::InvalidInput);
            assert!(
                e.to_string().contains(&format!("[{}, {}]", low, high)),
                "error should name the range, got {:?}", e.to_string(),
            );
        }
    }
}
