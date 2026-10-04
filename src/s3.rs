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
use hyperfile::{BlockIndex, BlockPtr, Cno, SegmentId};
use hyperfile::file::ReadTiming;
use hyperfile::meta_loader::s3::S3BlockLoader;
use hyperfile::ondisk::InodeRaw;
use hyperfile::inode::{Inode, OnDiskState};
use hyperfile::segment::{Segment, SegmentSum, SegmentReadWrite};
use hyperfile::staging::{Staging, config::StagingConfig, s3::S3Staging};
use hyperfile::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
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

    /// The URI this cleaner was built for, as a writer would need it.
    pub(crate) fn root_uri(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.root_path)
    }

    /// The client, for an operation that needs to open the file itself.
    pub(crate) fn client(&self) -> &Client {
        &self.client
    }

    async fn load_raw_inode(&self) -> Result<(InodeRaw, Option<OnDiskState>)> {
        let mut raw_inode = InodeRaw::default();
        let inode_state = self.staging.load_inode(raw_inode.as_mut_u8_slice()).await?;
        Ok((raw_inode, inode_state))
    }

    /// Every location the bmap still points into, split by what holds it.
    ///
    /// `collect_ptrs` reports every location the map references — its metadata
    /// nodes and the block pointers stored in its leaves — for both bmap shapes,
    /// so this needs no shape branch of its own. It returns `Err` rather than a
    /// short list if any subtree could not be enumerated, which is what makes it
    /// safe to derive a delete set from: a truncated reachable set is
    /// indistinguishable from a complete one.
    ///
    /// `collect_meta_ptrs` is the metadata-node subset of the same walk, and
    /// reads no leaf node to answer. Having both is what lets an object's live
    /// bytes be counted — a metadata node is `meta_block_size`, a data block is
    /// `data_block_size`, and a pointer does not say which it is — and it is what
    /// tells a compactor which objects it cannot free: a live metadata node is
    /// not relocatable by rewriting data blocks.
    fn walk_live_ptrs<L>(
        bmap: BMap<'_, BlockIndex, BlockPtr, BlockPtr, L, NullNodeCache>,
    ) -> Result<LivePtrs>
    where
        L: BlockLoader<BlockPtr> + Clone,
    {
        // `collect_ptrs` is a plain `async fn`, but a bmap built with
        // btree-ondisk's `rc` feature is `!Send`, so awaiting it directly
        // would make `collect`/`prune` non-`Send` and break the `Cleaner`
        // trait. The walk therefore stays on the blocking thread this
        // function is called from, driven the same way btree-ondisk drives
        // itself under `tokio-runtime`.
        let handle = tokio::runtime::Handle::current();
        let on_staging = |v: Vec<BlockPtr>| -> BTreeSet<BlockPtr> {
            v.into_iter().filter(BlockPtrFormat::is_on_staging).collect()
        };
        let all = on_staging(handle.block_on(bmap.collect_ptrs())?);
        let meta = on_staging(handle.block_on(bmap.collect_meta_ptrs())?);
        Ok(LivePtrs { all, meta })
    }

    /// Every segment object that exists under the container's prefix, with its
    /// size, and whatever else is there.
    ///
    /// A segment object is named by [`Segment::segid_to_staging_file_id`]: ten
    /// digits, and a `.<part>` suffix when the checkpoint was streamed. Anything
    /// else under the prefix — the inode object, a nested WAL prefix — is not one
    /// and is skipped, as is any key with a further `/` in it. What was skipped is
    /// reported rather than discarded: a key under this prefix that is neither the
    /// inode nor a segment object is most likely a WAL, and a compaction must not
    /// run against a container whose log it is not reading.
    ///
    /// The delete set is derived from what is *here*, not from arithmetic over
    /// `[low, high]`. Two reasons, both new in `hyperfile 0.7.0`: a checkpoint's
    /// object names are no longer derivable from its number, since a streamed one
    /// is `<cno>.0`, `<cno>.1`, … with no bound the caller knows; and a partial
    /// whose checkpoint was abandoned is referenced by nothing and named by
    /// nothing, so a walk that only visits computed names cannot see it at all.
    async fn list_segment_objects(&self) -> Result<ObjectListing> {
        let prefix = format!("{}/", self.root_path);
        let mut listing = ObjectListing::default();
        let mut pages = self.client.list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&prefix)
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            let page = page.map_err(|e| Error::new(
                s3_error_kind(&e),
                format!("listing segment objects under s3://{}/{} failed: {}",
                    self.bucket, prefix, e),
            ))?;
            for obj in page.contents() {
                let Some(key) = obj.key() else { continue };
                if key == self.staging.inode_file {
                    continue;
                }
                let Some(name) = key.strip_prefix(&prefix) else { continue };
                match parse_segment_object_name(name) {
                    Some(segid) => {
                        listing.objects.push((segid, obj.size().unwrap_or(0).max(0) as u64));
                    },
                    None => {
                        listing.foreign += 1;
                        if listing.foreign_sample.is_none() {
                            listing.foreign_sample = Some(key.to_string());
                        }
                    },
                }
            }
        }
        listing.objects.sort();
        Ok(listing)
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
    /// Returns an error if any key (at either level) failed, whose kind is
    /// that of the **first** failure — the same rule `hyperfile` applies to
    /// its own batch delete since 0.10.1, so a caller decides whether to
    /// retry `prune` by the kind alone: a throttled or unreachable store is
    /// worth another try, a denied key is not. The message includes the
    /// failed object count and a sample failed key. Successfully-deleted keys
    /// stay deleted (S3 has no transactional batch delete).
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
        let mut first_kind: Option<ErrorKind> = None;
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
                            first_kind.get_or_insert(delete_code_kind(e.code.as_deref()));
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
                    first_kind.get_or_insert(s3_error_kind(&sdk_err));
                    failed_keys.extend(keys.iter().cloned());
                }
            }
        }
        if failed_keys.is_empty() {
            Ok(())
        } else {
            Err(Error::new(
                first_kind.unwrap_or(ErrorKind::Other),
                format!(
                    "DeleteObjects failed for {} object(s); first failed key: {:?}",
                    failed_keys.len(),
                    failed_keys.first(),
                ),
            ))
        }
    }

    /// Everything `collect`, `report` and `compact` need from storage, gathered
    /// once: the checkpoint being walked, the container's pointer format and
    /// block sizes, every live location, and every object that exists.
    ///
    /// `high == 0` is a sentinel meaning "use the latest checkpoint
    /// (`inode.get_last_cno()`)".
    pub(crate) async fn view(&self, low: Cno, high: Cno) -> Result<ContainerView> {
        // Read unconditionally, not only for the `high == 0` sentinel: the
        // container's pointer format lives in the inode, and it decides both
        // which object holds a checkpoint's summary and how a pointer decodes.
        let (raw_inode, inode_state) = self.load_raw_inode().await?;
        refuse_unsegmented(&raw_inode)?;
        let fmt = container_ptr_format(&raw_inode);
        let latest_compacted = raw_inode.is_compacted();
        let high_cno = if high == 0 {
            let inode = Inode::from_raw(&raw_inode, inode_state);
            inode.get_last_cno()
        } else {
            high
        };

        debug!("view of container low: {}, high: {}, format: {:?}", low, high_cno, fmt);
        validate_segid_range(low, high_cno)?;

        // Which object holds this checkpoint's summary, metadata and inode:
        // part 0 when the container streams its checkpoints, the bare name
        // otherwise. `Staging::open` would find it either way — given a bare
        // number with no object of that name it tries the summary part too —
        // but naming it outright saves a 404 on every parted container, which
        // since `hyperfile 0.7.0` is every container it creates.
        let high_summary = summary_obj(high_cno, &fmt);
        let ss = self.load_segment_sum(high_summary).await?;
        // The whole inode is moved into the walk, not just its `i_bmap` field:
        // `BMap::read` wants an 8-byte-aligned buffer, and `BMapRawType` is
        // `[u8; N]`, whose own alignment is 1, so a copy of the field alone
        // lands wherever the compiler put it. `InodeRaw` is `align(8)` and
        // `i_bmap` sits at an 8-aligned offset within it, so borrowing the
        // field out of an owned inode is aligned by construction.
        let ss_inode = ss.hdr.s_inode;
        // Both sizes come from the segment header rather than from the inode's
        // `i_meta_config`: they have been there since the first release, while
        // that field was zero on containers old enough that it cannot be
        // decoded at all.
        let meta_block_size = 1usize << ss.hdr.s_meta_blk_shift;
        let data_block_size = 1usize << ss.hdr.s_data_blk_shift;

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
        let join = tokio::task::spawn_blocking(move || -> Result<LivePtrs> {
            let bmap = BMap::<BlockIndex, BlockPtr, BlockPtr, S3BlockLoader, NullNodeCache>
                ::read(&ss_inode.i_bmap, meta_block_size, loader, NullNodeCache)?;
            Self::walk_live_ptrs(bmap)
        });
        let live = match join.await {
            Ok(inner) => inner?,
            Err(join_err) => {
                return Err(Error::other(
                    format!("bmap walk task failed to join: {}", join_err),
                ));
            }
        };

        let listing = self.list_segment_objects().await?;
        Ok(ContainerView {
            low,
            high_cno,
            latest_compacted,
            fmt,
            high_summary,
            data_block_size,
            meta_block_size,
            live,
            listing,
        })
    }

    /// Inner shared implementation used by both `collect` and
    /// `prune`. Returns `(referenced, unreferenced)` lists of segment
    /// *objects* where:
    ///  - `referenced` is every object the latest published inode's metadata
    ///    tree still points into, whether or not it falls in `[low, high]`.
    ///  - `unreferenced` is every object that exists under the container's
    ///    prefix whose checkpoint number falls in `[low, high]` and which
    ///    nothing points into, less the two kinds this keeps regardless (see
    ///    [`partition_objects`]).
    async fn collect_unref_seg(
        &self,
        low: Cno,
        high: Cno,
    ) -> Result<(Vec<SegmentId>, Vec<SegmentId>)> {
        let view = self.view(low, high).await?;
        Ok(partition_objects(
            &view.object_ids(),
            &view.referenced_objects(),
            view.low,
            view.high_cno,
            view.high_summary,
        ))
    }
}

/// Every live location of a container, and the metadata-node subset of it.
pub(crate) struct LivePtrs {
    pub(crate) all: BTreeSet<BlockPtr>,
    pub(crate) meta: BTreeSet<BlockPtr>,
}

/// What a listing of the container's prefix found.
#[derive(Default)]
pub(crate) struct ObjectListing {
    /// Segment objects and their sizes, ascending.
    pub(crate) objects: Vec<(SegmentId, u64)>,
    /// How many keys under the prefix were neither the inode nor a segment
    /// object. A WAL root nested under the container is the usual reason.
    pub(crate) foreign: usize,
    pub(crate) foreign_sample: Option<String>,
}

/// One pass over a container's storage, shared by every operation.
pub(crate) struct ContainerView {
    pub(crate) low: Cno,
    pub(crate) high_cno: Cno,
    /// Whether the published inode marks its checkpoint as a compaction
    /// (`InodeRaw::FLAG_COMPACTED`): it moved blocks and changed no content.
    pub(crate) latest_compacted: bool,
    pub(crate) fmt: BlockPtrFormat,
    pub(crate) high_summary: SegmentId,
    pub(crate) data_block_size: usize,
    pub(crate) meta_block_size: usize,
    pub(crate) live: LivePtrs,
    pub(crate) listing: ObjectListing,
}

impl ContainerView {
    pub(crate) fn object_ids(&self) -> Vec<SegmentId> {
        self.listing.objects.iter().map(|(segid, _)| *segid).collect()
    }

    /// The objects at least one live location names.
    pub(crate) fn referenced_objects(&self) -> BTreeSet<SegmentId> {
        self.live.all.iter()
            .map(|p| BlockPtrFormat::decode(p, &self.fmt).0)
            .collect()
    }

    /// Which object a live location is in.
    pub(crate) fn object_of(&self, blkptr: &BlockPtr) -> SegmentId {
        BlockPtrFormat::decode(blkptr, &self.fmt).0
    }
}

/// The kind an S3 failure is reported with, by `hyperfile`'s rule (0.10.1):
///
/// | kind | means | retry |
/// |---|---|---|
/// | `ConnectionReset`, `TimedOut` | the network | yes |
/// | `ResourceBusy` | the store throttling or failing (429, 5xx) | yes |
/// | `AlreadyExists` | a conditional write refused | not as is |
/// | `NotFound`, `PermissionDenied`, `InvalidInput` | missing, refused, a bad request | no |
/// | `Other` | anything else | no |
///
/// A copy of `hyperfile::s3commons::s3_error_kind`, which is `pub(crate)`. The two
/// have to agree, because a caller of this crate sees errors from both and decides
/// by the kind alone; `status_kind` and `code_kind` carry the tables so the tests
/// below pin them.
fn s3_error_kind<E, R>(err: &aws_sdk_s3::error::SdkError<E, R>) -> ErrorKind
where
    E: aws_sdk_s3::error::ProvideErrorMetadata,
    R: HttpStatus,
{
    use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
    match err {
        SdkError::TimeoutError(_) => ErrorKind::TimedOut,
        SdkError::DispatchFailure(_) => ErrorKind::ConnectionReset,
        SdkError::ConstructionFailure(_) => ErrorKind::InvalidInput,
        // A response that arrived but could not be read — most often one cut off
        // part-way, which is the network again.
        SdkError::ResponseError(_) => ErrorKind::ConnectionReset,
        _ => {
            // A refused condition is reported by code as well as by status:
            // a directory bucket answers some with 200 and the error in the body.
            if matches!(err.code(), Some("PreconditionFailed" | "ConditionalRequestConflict")) {
                return ErrorKind::AlreadyExists;
            }
            status_kind(err.raw_response().map(|r| r.status_u16()))
        },
    }
}

/// The status half of [`s3_error_kind`].
fn status_kind(status: Option<u16>) -> ErrorKind {
    match status {
        Some(404) => ErrorKind::NotFound,
        Some(403) => ErrorKind::PermissionDenied,
        Some(408) => ErrorKind::TimedOut,
        Some(409 | 412) => ErrorKind::AlreadyExists,
        Some(429 | 500 | 502 | 503 | 504) => ErrorKind::ResourceBusy,
        Some(400 | 405 | 411 | 416 | 501) => ErrorKind::InvalidInput,
        _ => ErrorKind::Other,
    }
}

/// The kind of one key's failure inside a `DeleteObjects` that itself succeeded.
///
/// Such a failure has no status of its own, only S3's error code, so the codes
/// that correspond to the statuses above are mapped to the same kinds.
fn delete_code_kind(code: Option<&str>) -> ErrorKind {
    match code {
        Some("AccessDenied" | "AllAccessDisabled" | "AccountProblem"
            | "InvalidAccessKeyId" | "SignatureDoesNotMatch") => ErrorKind::PermissionDenied,
        Some("NoSuchKey" | "NoSuchBucket" | "NoSuchVersion") => ErrorKind::NotFound,
        Some("SlowDown" | "InternalError" | "ServiceUnavailable" | "Throttling"
            | "ThrottlingException" | "RequestLimitExceeded" | "TooManyRequests") => ErrorKind::ResourceBusy,
        Some("RequestTimeout") => ErrorKind::TimedOut,
        Some("PreconditionFailed" | "ConditionalRequestConflict" | "OperationAborted") => ErrorKind::AlreadyExists,
        Some("InvalidArgument" | "InvalidRequest" | "MalformedXML" | "KeyTooLongError") => ErrorKind::InvalidInput,
        _ => ErrorKind::Other,
    }
}

/// What [`s3_error_kind`] needs of a raw response: its status. A trait rather than
/// the SDK's response type so the mapping does not name it.
trait HttpStatus {
    fn status_u16(&self) -> u16;
}

impl HttpStatus for aws_sdk_s3::config::http::HttpResponse {
    fn status_u16(&self) -> u16 {
        self.status().as_u16()
    }
}

/// `S_IFMT` and the two device types, as the inode stores its mode. POSIX fixes
/// these values, so they are the same on every host.
const S_IFMT: u32 = 0o170000;
const S_IFCHR: u32 = 0o020000;
const S_IFBLK: u32 = 0o060000;

/// Refuse an inode whose checkpoint fields do not hold checkpoint numbers.
///
/// An inline file keeps its data in the inode's tail, over `i_last_seq`,
/// `i_last_cno` and the bmap root; a device node keeps its `rdev` in
/// `i_last_cno`. Read as a checkpoint number either is whatever the bytes
/// happen to be. Nothing would be deleted on the strength of it — the summary
/// it named would not be found — but the error would point at a missing segment
/// rather than at what is actually wrong.
fn refuse_unsegmented(raw: &InodeRaw) -> Result<()> {
    let fmt = raw.i_mode & S_IFMT;
    let what = if raw.is_inline() {
        Some("an inline file, whose data lives in the inode")
    } else if fmt == S_IFCHR || fmt == S_IFBLK {
        Some("a device node, which stores rdev where a checkpoint number would be")
    } else {
        None
    };
    match what {
        Some(what) => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("this container is {}: it has no segments to collect, prune or compact", what),
        )),
        None => Ok(()),
    }
}

/// The pointer format the container was written with, as its inode records it.
///
/// Falls back to the pre-parting format rather than failing. A container written
/// before `i_meta_config` was populated carries zero there, which decodes to
/// nothing usable — and a container that old cannot be parted, because parting
/// did not exist. The only thing this decides is how a pointer's low bits are
/// read and which name holds a summary, and for every format but
/// `PartedSegment` those are what they have always been.
fn container_ptr_format(raw_inode: &InodeRaw) -> BlockPtrFormat {
    match HyperFileMetaConfig::try_from_u32(raw_inode.i_meta_config) {
        Ok(cfg) => cfg.block_ptr_format,
        Err(e) => {
            debug!("inode meta config {:#x} is not one this build can read ({}); \
                reading pointers as {:?}", raw_inode.i_meta_config, e,
                BlockPtrFormat::MicroGroup);
            BlockPtrFormat::MicroGroup
        },
    }
}

/// The object holding a checkpoint's summary, metadata blocks and inode.
fn summary_obj(cno: Cno, fmt: &BlockPtrFormat) -> SegmentId {
    let segid = SegmentId::new_from_cno(cno);
    if fmt.is_parted() {
        segid.at_part(Segment::SUMMARY_PART)
    } else {
        segid
    }
}

/// A segment object name, or `None` for a key that is not one.
///
/// The names are what [`Segment::segid_to_staging_file_id`] produces: ten digits
/// zero-padded, optionally `.<part>`. `None` and `Some(0)` are different names
/// on purpose upstream, so they stay different objects here.
fn parse_segment_object_name(name: &str) -> Option<SegmentId> {
    // Nested prefixes — a WAL root under the container, say — are not segment
    // objects of this container.
    if name.contains('/') {
        return None;
    }
    let (seq_str, part_str) = match name.split_once('.') {
        Some((seq, part)) => (seq, Some(part)),
        None => (name, None),
    };
    if seq_str.len() != 10 || !seq_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Ten digits can name more than a `u32` holds; such an object cannot have
    // been written by this format, so it is not one of ours.
    let seq_id = seq_str.parse::<u32>().ok()?;
    match part_str {
        None => Some(SegmentId::new(seq_id)),
        Some(p) => {
            if p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            Some(SegmentId::with_part(seq_id, p.parse::<u16>().ok()?))
        },
    }
}

/// Split what exists into what is still pointed into and what can go.
///
/// `referenced` is object-granular, which is what makes a parted container
/// reclaimable at all. Two kinds of object are kept even when no pointer names
/// them:
///
/// - **The summary of `high`.** It is the object this cleaner just read to find
///   the checkpoint's bmap, and the one it would read on the next run. A
///   container whose newest checkpoint holds no live block — an empty file, or
///   one whose every block was superseded within the same checkpoint — would
///   otherwise have its own head deleted, and the next `collect` would fail with
///   `NoSuchKey`.
/// - **The summary part of a checkpoint that still has a live part.** Only
///   reachable under `PartedSegment`: the parts carry data, part 0 carries the
///   summary and metadata that describe them. Deleting it would leave a
///   checkpoint whose blocks are live but which nothing can parse, which is what
///   `open_cno` and a repair tool need. "Not a partial" is upstream's own
///   predicate for that object — a partial is a part above zero, and an unparted
///   checkpoint is never one.
///
/// Everything else in `[low, high]` that nothing points into is reclaimable,
/// including the partials of a checkpoint that was abandoned — those share their
/// checkpoint number with the retry that replaced them, so they are invisible to
/// anything reasoning in whole checkpoints.
fn partition_objects(
    existing: &[SegmentId],
    referenced: &BTreeSet<SegmentId>,
    low: Cno,
    high: Cno,
    high_summary: SegmentId,
) -> (Vec<SegmentId>, Vec<SegmentId>) {
    let live_cnos: BTreeSet<Cno> = referenced.iter().map(|s| s.as_cno()).collect();
    let mut unref = Vec::new();
    for obj in existing {
        let cno = obj.as_cno();
        if cno < low || cno > high {
            continue;
        }
        if referenced.contains(obj) {
            continue;
        }
        if *obj == high_summary {
            continue;
        }
        if !obj.is_partial() && live_cnos.contains(&cno) {
            continue;
        }
        unref.push(*obj);
    }
    let v_ref = referenced.iter().copied().collect::<Vec<SegmentId>>();
    (v_ref, unref)
}

/// Reject a checkpoint range that cannot be walked.
///
/// `[low, high]` is inclusive at both ends, so `low == high_cno` is a
/// well-formed query — "what does checkpoint N reference, and is anything in
/// `[N, N]` unreferenced" — and is answered normally. It is reached whenever a
/// caller asks about the entire history of a file with a single checkpoint:
/// `collect(1, 0)`, the form this crate's own documentation gives as the typical
/// call, resolves `high` to `get_last_cno() == 1` and so lands on `low == high`.
/// Rejecting it made `collect` and `prune` unusable on any freshly created file
/// until it had been flushed at least twice.
///
/// `low > high_cno` is genuinely empty and stays an error, which also covers
/// `high_cno == 0` (no checkpoint has ever been published, so there is no
/// metadata tree to walk).
///
/// A checkpoint number past `u32::MAX` is refused rather than truncated: that is
/// what a block pointer can name, so no object could carry the number, and
/// `SegmentId::new_from_cno` would assert on it in a debug build.
fn validate_segid_range(low: Cno, high_cno: Cno) -> Result<()> {
    if low > high_cno {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("invalid segment range [{}, {}]", low, high_cno),
        ));
    }
    if high_cno > u32::MAX as Cno {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("checkpoint {} is past what a block pointer can name ({})",
                high_cno, u32::MAX),
        ));
    }
    Ok(())
}

impl Cleaner for S3Cleaner {
    async fn collect(
        &self,
        low: Cno,
        high: Cno,
    ) -> Result<(Vec<SegmentId>, Vec<SegmentId>)> {
        self.collect_unref_seg(low, high).await
    }

    /// Prune unreferenced segment objects in `[low, high]`. Implemented
    /// as `collect` followed by `DeleteObjects` of the
    /// unreferenced list.
    async fn prune(&self, low: Cno, high: Cno) -> Result<()> {
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

    // --- error kinds, aligned with hyperfile 0.10.1 ---

    use aws_sdk_s3::config::http::HttpResponse;
    use aws_sdk_s3::error::{ErrorMetadata, SdkError};
    use aws_sdk_s3::operation::delete_objects::DeleteObjectsError;

    fn service_error(status: u16, code: Option<&str>) -> SdkError<DeleteObjectsError, HttpResponse> {
        let mut meta = ErrorMetadata::builder();
        if let Some(code) = code {
            meta = meta.code(code);
        }
        let resp = HttpResponse::new(
            status.try_into().expect("a valid status"),
            aws_sdk_s3::primitives::SdkBody::empty(),
        );
        SdkError::service_error(DeleteObjectsError::generic(meta.build()), resp)
    }

    /// The table both crates answer by. If hyperfile moves a status, this is the
    /// test that should fail first.
    #[test]
    fn statuses_map_as_hyperfile_maps_them() {
        for (status, kind) in [
            (404, ErrorKind::NotFound),
            (403, ErrorKind::PermissionDenied),
            (408, ErrorKind::TimedOut),
            (409, ErrorKind::AlreadyExists),
            (412, ErrorKind::AlreadyExists),
            (429, ErrorKind::ResourceBusy),
            (500, ErrorKind::ResourceBusy),
            (503, ErrorKind::ResourceBusy),
            (400, ErrorKind::InvalidInput),
            (501, ErrorKind::InvalidInput),
            (418, ErrorKind::Other),
        ] {
            assert_eq!(status_kind(Some(status)), kind, "status {}", status);
            assert_eq!(s3_error_kind(&service_error(status, None)), kind, "status {}", status);
        }
        assert_eq!(status_kind(None), ErrorKind::Other);
    }

    /// A request that never got an answer is the network, or a bad request.
    #[test]
    fn transport_failures_are_retryable_and_build_failures_are_not() {
        let timeout: SdkError<DeleteObjectsError, HttpResponse> = SdkError::timeout_error("slow");
        assert_eq!(s3_error_kind(&timeout), ErrorKind::TimedOut);
        let build: SdkError<DeleteObjectsError, HttpResponse> = SdkError::construction_failure("bad");
        assert_eq!(s3_error_kind(&build), ErrorKind::InvalidInput);
    }

    /// A refused condition is told apart by its code too: a directory bucket can
    /// carry one in a 200.
    #[test]
    fn a_refused_condition_is_recognised_by_its_code() {
        assert_eq!(s3_error_kind(&service_error(200, Some("PreconditionFailed"))),
            ErrorKind::AlreadyExists);
        assert_eq!(s3_error_kind(&service_error(200, Some("ConditionalRequestConflict"))),
            ErrorKind::AlreadyExists);
    }

    /// A key that failed inside a successful `DeleteObjects` has only a code. The
    /// codes that mean the same as a status map to the same kind, and the one
    /// distinction that matters for `prune` holds: busy is retryable, denied is not.
    #[test]
    fn per_key_delete_codes_map_to_the_same_kinds() {
        assert_eq!(delete_code_kind(Some("AccessDenied")), ErrorKind::PermissionDenied);
        assert_eq!(delete_code_kind(Some("SlowDown")), ErrorKind::ResourceBusy);
        assert_eq!(delete_code_kind(Some("InternalError")), ErrorKind::ResourceBusy);
        assert_eq!(delete_code_kind(Some("ServiceUnavailable")), ErrorKind::ResourceBusy);
        assert_eq!(delete_code_kind(Some("RequestTimeout")), ErrorKind::TimedOut);
        assert_eq!(delete_code_kind(Some("NoSuchBucket")), ErrorKind::NotFound);
        assert_eq!(delete_code_kind(Some("SomethingNew")), ErrorKind::Other);
        assert_eq!(delete_code_kind(None), ErrorKind::Other);
    }

    /// No failure is reported as `Interrupted` any more: by convention that means
    /// "just retry", which a denied key does not deserve.
    #[test]
    fn nothing_maps_to_interrupted() {
        for status in 100u16..600 {
            assert_ne!(status_kind(Some(status)), ErrorKind::Interrupted);
        }
        for code in ["AccessDenied", "SlowDown", "InternalError", "NoSuchKey", "?"] {
            assert_ne!(delete_code_kind(Some(code)), ErrorKind::Interrupted);
        }
    }

    /// An inline file's checkpoint fields hold its data, and a device node's hold
    /// its rdev. Neither is a checkpoint number, so the walk refuses up front
    /// rather than looking for a segment the bytes happen to name.
    #[test]
    fn an_inode_without_segments_is_refused() {
        let mut inline = InodeRaw { i_mode: 0o100644, ..Default::default() };
        inline.set_inline(b"hello");
        let e = refuse_unsegmented(&inline).expect_err("inline must be refused");
        assert_eq!(e.kind(), ErrorKind::InvalidInput);
        assert!(e.to_string().contains("inline"), "{}", e);

        for mode in [0o020644u32, 0o060644] {
            let dev = InodeRaw { i_mode: mode, ..Default::default() };
            let e = refuse_unsegmented(&dev).expect_err("a device node must be refused");
            assert!(e.to_string().contains("device"), "{}", e);
        }

        for mode in [0o100644u32, 0o040755, 0o120777] {
            let ok = InodeRaw { i_mode: mode, ..Default::default() };
            assert!(refuse_unsegmented(&ok).is_ok(), "mode {:o} has segments", mode);
        }
    }

    /// A checkpoint number no pointer could name is refused rather than
    /// truncated into one that names a different object.
    #[test]
    fn segid_range_rejects_a_checkpoint_past_a_pointer() {
        let e = validate_segid_range(1, u32::MAX as Cno + 1)
            .expect_err("a cno past u32::MAX must be rejected");
        assert_eq!(e.kind(), ErrorKind::InvalidInput);
        assert!(validate_segid_range(1, u32::MAX as Cno).is_ok());
    }

    // --- object names ---

    #[test]
    fn object_names_round_trip_through_the_parser() {
        for segid in [
            SegmentId::new(1),
            SegmentId::new(4294967295),
            SegmentId::with_part(7, 0),
            SegmentId::with_part(7, 16383),
        ] {
            let name = Segment::segid_to_staging_file_id(segid);
            assert_eq!(parse_segment_object_name(&name), Some(segid),
                "name {:?} did not parse back", name);
        }
    }

    /// An unsuffixed name and part 0 are different objects upstream, so they
    /// have to stay different here — sharing them would mean deleting the
    /// summary of one container shape while reporting the other.
    #[test]
    fn a_bare_name_is_not_part_zero() {
        assert_eq!(parse_segment_object_name("0000000007"), Some(SegmentId::new(7)));
        assert_eq!(parse_segment_object_name("0000000007.0"),
            Some(SegmentId::with_part(7, 0)));
    }

    #[test]
    fn keys_that_are_not_segment_objects_are_skipped() {
        for name in [
            "inode",              // the inode object
            "wal/0000000001",     // a nested prefix, not an object of ours
            "0000000001/0_0_4096",
            "000000001",          // nine digits
            "00000000012",        // eleven
            "9999999999",         // ten digits, past what a u32 can hold
            "0000000001.",        // empty part
            "0000000001.x",
            "0000000001.65536",   // past what a u16 can hold
            "0000000001.0.1",
            "",
        ] {
            assert_eq!(parse_segment_object_name(name), None,
                "{:?} should not be read as a segment object", name);
        }
    }

    // --- what gets reclaimed ---

    fn partition(
        existing: &[SegmentId],
        referenced: &[SegmentId],
        low: Cno,
        high: Cno,
        high_summary: SegmentId,
    ) -> Vec<SegmentId> {
        let set: BTreeSet<SegmentId> = referenced.iter().copied().collect();
        let (_, unref) = partition_objects(existing, &set, low, high, high_summary);
        unref
    }

    /// The unparted case, which is every container written before 0.7.0: an
    /// object is a checkpoint, and one nothing points into goes.
    #[test]
    fn an_unreferenced_checkpoint_is_reclaimable() {
        let existing = [SegmentId::new(1), SegmentId::new(2), SegmentId::new(3)];
        let unref = partition(&existing, &[SegmentId::new(3)], 1, 3, SegmentId::new(3));
        assert_eq!(unref, vec![SegmentId::new(1), SegmentId::new(2)]);
    }

    /// Objects outside `[low, high]` are not candidates, which is what makes a
    /// prune safe against a writer publishing underneath it.
    #[test]
    fn objects_outside_the_range_are_left_alone() {
        let existing = [
            SegmentId::new(1), SegmentId::new(2), SegmentId::new(3), SegmentId::new(4),
        ];
        let unref = partition(&existing, &[], 2, 3, SegmentId::new(9));
        assert_eq!(unref, vec![SegmentId::new(2), SegmentId::new(3)]);
    }

    /// A checkpoint whose newest state holds no live block would otherwise have
    /// its own head deleted, and the next `collect` would fail to read it.
    #[test]
    fn the_summary_of_high_is_kept_even_when_nothing_points_into_it() {
        let existing = [SegmentId::new(1), SegmentId::new(2)];
        let unref = partition(&existing, &[], 1, 2, SegmentId::new(2));
        assert_eq!(unref, vec![SegmentId::new(1)]);

        // Same for a streamed container, where the head is part 0.
        let existing = [SegmentId::with_part(2, 0), SegmentId::with_part(2, 1)];
        let unref = partition(&existing, &[], 1, 2, SegmentId::with_part(2, 0));
        assert_eq!(unref, vec![SegmentId::with_part(2, 1)]);
    }

    /// The partials of an abandoned checkpoint share their number with the
    /// retry that replaced them. Reclaiming them is the whole reason the
    /// referenced set is object-granular.
    #[test]
    fn an_abandoned_partial_is_reclaimable_while_its_checkpoint_lives() {
        let existing = [
            SegmentId::with_part(5, 0), // summary of the checkpoint that landed
            SegmentId::with_part(5, 1), // its data
            SegmentId::with_part(5, 2), // written, then abandoned by a rollback
        ];
        let referenced = [SegmentId::with_part(5, 0), SegmentId::with_part(5, 1)];
        let unref = partition(&existing, &referenced, 1, 9, SegmentId::with_part(9, 0));
        assert_eq!(unref, vec![SegmentId::with_part(5, 2)]);
    }

    /// A live checkpoint has to stay parseable: its data parts are named by
    /// pointers, its summary is what describes them.
    #[test]
    fn the_summary_part_of_a_live_checkpoint_is_kept() {
        let existing = [SegmentId::with_part(4, 0), SegmentId::with_part(4, 3)];
        let unref = partition(&existing, &[SegmentId::with_part(4, 3)], 1, 9,
            SegmentId::with_part(9, 0));
        assert!(unref.is_empty(), "part 0 describes the live part 3, got {:?}", unref);
    }

    /// With no live part left, the summary goes too — otherwise a parted
    /// container would never give any storage back.
    #[test]
    fn a_wholly_dead_parted_checkpoint_goes_including_its_summary() {
        let existing = [
            SegmentId::with_part(4, 0), SegmentId::with_part(4, 1),
            SegmentId::with_part(6, 0),
        ];
        let unref = partition(&existing, &[SegmentId::with_part(6, 0)], 1, 9,
            SegmentId::with_part(6, 0));
        assert_eq!(unref, vec![SegmentId::with_part(4, 0), SegmentId::with_part(4, 1)]);
    }
}
