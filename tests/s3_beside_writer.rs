//! The cleaner beside a container's writer, against real S3: compactions
//! attached by `CompactOptions::beside_writer` and taken in by the writer, and
//! what `prune` may take while a writer, a compactor and readers are about.
//!
//! ```bash
//! HYPERFILE_TEST_BUCKET=… HYPERFILE_TEST_REGION=… \
//!   cargo test --release --test s3_beside_writer -- --ignored --test-threads=1
//! ```
#![recursion_limit = "1024"]

mod common;
use common::*;

use hyperfile::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use hyperfile::file::flags::FileFlags;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::mode::FileMode;
use hyperfile::meta_format::BlockPtrFormat;
use hyperfile_cleaner::{Cleaner, s3::S3Cleaner};

const BS: usize = 4096;
const MIB: usize = 1 << 20;
const FORMATS: [BlockPtrFormat; 2] = [BlockPtrFormat::PartedSegment, BlockPtrFormat::StreamSegment];

fn configs(fmt: BlockPtrFormat) -> (HyperFileMetaConfig, HyperFileRuntimeConfig) {
    let mut meta = HyperFileMetaConfig::default();
    meta.block_ptr_format = fmt;
    let mut rt = HyperFileRuntimeConfig::default();
    rt.data_cache_dirty_max_bytes_threshold = 1 << 40;
    rt.data_cache_dirty_max_blocks_threshold = (1 << 40) / BS;
    rt.data_cache_dirty_max_flush_interval = u64::MAX / 2;
    rt.segment_buffer_size = 1 << 40;
    rt.segment_max_object_bytes = 4 * MIB;
    rt.stream_segment_bytes = 0;
    rt.data_cache_blocks = 0;
    (meta, rt)
}

fn data(off: usize, len: usize, g: u8) -> Vec<u8> {
    (off..off + len).map(|i| ((i / BS) as u8).wrapping_mul(13) ^ g ^ (i as u8)).collect()
}

/// 16 MiB in objects of 4 MiB, then its second half again: the first
/// checkpoint's second half is dead.
async fn written(client: &aws_sdk_s3::Client, uri: &str, fmt: BlockPtrFormat) -> std::io::Result<(Hyper<'static>, Vec<u8>)> {
    let (meta, rt) = configs(fmt);
    let mut w = Hyper::fs_create_opt(client, uri, FileFlags::rdwr(), FileMode::default_file(), &meta, &rt).await?;
    let mut want = data(0, 16 * MIB, 1);
    w.fs_write(0, &want).await?;
    w.fs_flush().await?;
    let second = data(8 * MIB, 8 * MIB, 2);
    w.fs_write(8 * MIB, &second).await?;
    w.fs_flush().await?;
    want[8 * MIB..].copy_from_slice(&second);
    Ok((w, want))
}

async fn read_cold(client: &aws_sdk_s3::Client, uri: &str, len: usize) -> std::io::Result<Vec<u8>> {
    let mut r = Hyper::fs_open(client, uri, FileFlags::rdonly()).await?;
    let mut v = vec![0u8; len];
    r.fs_read(0, &mut v).await?;
    let _ = r.fs_release().await;
    Ok(v)
}

const AGE: std::time::Duration = std::time::Duration::from_secs(600);

/// Attach a compaction of the whole file, through hyperfile itself.
async fn attach(client: &aws_sdk_s3::Client, uri: &str) -> std::io::Result<hyperfile::file::file::CompactionAttached> {
    let mut c = Hyper::fs_open(client, uri, FileFlags::rdwr()).await?;
    c.fs_begin_compaction().await?;
    c.fs_relocate(&[(0, 16 * MIB / BS)]).await?;
    let out = c.fs_publish_compaction(AGE).await?;
    let _ = c.fs_release().await;
    Ok(out)
}

/// The objects under the parts `[p0, p1)` of checkpoint `base`.
fn copies(names: &[String], a: &hyperfile::file::file::CompactionAttached) -> Vec<String> {
    (a.parts.0..a.parts.1).map(|p| format!("{:0>10}.{}", a.base, p)).filter(|n| names.contains(n)).collect()
}

/// `prune` while a compaction is attached keeps both what the stored inode reads
/// through and the copies; the writer takes it in, and the next `prune` takes
/// what it emptied and keeps the copies. The data reads back cold throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn prune_keeps_an_attached_compaction_and_frees_after_the_writer() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    for fmt in FORMATS {
        let tf = TestFile::new();
        let uri = tf.uri();
        let res: std::io::Result<_> = async {
            let (mut w, mut want) = written(&client, &uri, fmt).await?;
            let all = tf.objects(&client).await;
            let a = attach(&client, &uri).await?;
            let cleaner = S3Cleaner::with_client(&client, &uri).await?;
            let report = cleaner.report(1, 0).await?;
            let mine = copies(&tf.objects(&client).await, &a);
            cleaner.prune(1, 0).await?;
            let attached = tf.objects(&client).await;
            let got_attached = read_cold(&client, &uri, 16 * MIB).await?;
            let tail = data(15 * MIB, MIB, 3);
            w.fs_write(15 * MIB, &tail).await?;
            w.fs_flush().await?;
            want[15 * MIB..].copy_from_slice(&tail);
            let _ = w.fs_release().await;
            drop(w);
            // The copies the writer now reads through: those it must have found.
            let live: Vec<String> = cleaner.report(1, 0).await?.objects.iter()
                .filter(|o| o.live_data_blocks + o.live_meta_blocks > 0)
                .map(|o| match o.segid.part_id() {
                    Some(p) => format!("{:0>10}.{}", o.segid.as_cno(), p),
                    None => format!("{:0>10}", o.segid.as_cno()),
                }).collect();
            let live: Vec<String> = mine.iter().filter(|n| live.contains(n)).cloned().collect();
            cleaner.prune(1, 0).await?;
            let after = tf.objects(&client).await;
            let got = read_cold(&client, &uri, 16 * MIB).await?;
            Ok((a, report, mine, live, all, attached, after, want, got, got_attached))
        }.await;
        tf.cleanup(&client).await;
        let (a, report, mine, live, all, attached, after, want, got, got_attached) =
            res.unwrap_or_else(|e| panic!("{:?}: {:?}: {}", fmt, e.kind(), e));
        let hyperfile::compaction::StoredCompaction::Pending(p) = report.attached else {
            panic!("{:?}: the report has no attached compaction: {:?}", fmt, report.attached)
        };
        assert_eq!((p.base, p.parts, p.moved), (a.base, a.parts, a.moved), "{:?}", fmt);
        assert!(!mine.is_empty(), "{:?}: no copies", fmt);
        // What checkpoint 1 still held was dead before the compaction: the writer's
        // version reads none of it. Everything else stays while attached.
        let gone: Vec<&String> = all.iter().filter(|n| !attached.contains(n)).collect();
        assert!(gone.iter().all(|n| n.starts_with("0000000001.") || n.as_str() == "0000000001"),
            "{:?}: prune took what the stored inode reads through: {:?}", fmt, gone);
        assert!(mine.iter().all(|n| attached.contains(n)), "{:?}: prune took copies while attached: {:?} of {:?}", fmt, attached, mine);
        assert!(got_attached == want[..].iter().take(15 * MIB).chain(data(15 * MIB, MIB, 2).iter()).copied().collect::<Vec<u8>>(),
            "{:?}: read back wrong while attached", fmt);
        assert!(!live.is_empty() && live.iter().all(|n| after.contains(n)),
            "{:?}: prune took copies the writer reads through: {:?} of {:?}", fmt, after, live);
        assert!(after.iter().all(|n| n.starts_with(&format!("{:0>10}", a.base)) || n > &format!("{:0>10}", a.base)),
            "{:?}: objects before the compaction's base outlived it: {:?}", fmt, after);
        assert!(got == want, "{:?}: read back wrong after prune", fmt);
    }
}

/// `prune` with an older `high` keeps what the attached compaction reads through.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn prune_up_to_an_older_checkpoint_keeps_an_attached_compaction() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    for fmt in FORMATS {
        let tf = TestFile::new();
        let uri = tf.uri();
        let res: std::io::Result<_> = async {
            let (mut w, mut want) = written(&client, &uri, fmt).await?;
            let a = attach(&client, &uri).await?;
            let cleaner = S3Cleaner::with_client(&client, &uri).await?;
            cleaner.prune(1, a.base).await?;
            let mine = copies(&tf.objects(&client).await, &a);
            let tail = data(15 * MIB, MIB, 3);
            w.fs_write(15 * MIB, &tail).await?;
            w.fs_flush().await?;
            want[15 * MIB..].copy_from_slice(&tail);
            let _ = w.fs_release().await;
            drop(w);
            cleaner.prune(1, a.base).await?;
            let got = read_cold(&client, &uri, 16 * MIB).await?;
            Ok((want, got, mine))
        }.await;
        tf.cleanup(&client).await;
        let (want, got, mine) = res.unwrap_or_else(|e| panic!("{:?}: {:?}: {}", fmt, e.kind(), e));
        assert!(!mine.is_empty(), "{:?}: the first prune took the copies", fmt);
        assert!(got == want, "{:?}: read back wrong after prune", fmt);
    }
}

/// The inode says a compaction is attached and its object's metadata is gone, or
/// does not read: `prune` refuses and deletes nothing; the writer drops the
/// compaction at its next publish, and `prune` runs again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn prune_refuses_metadata_it_cannot_read() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let fmt = BlockPtrFormat::PartedSegment;
    for meta in [None, Some("v1;base=nonsense")] {
        let tf = TestFile::new();
        let uri = tf.uri();
        let res: std::io::Result<_> = async {
            let (mut w, mut want) = written(&client, &uri, fmt).await?;
            attach(&client, &uri).await?;
            // A copy of the inode object onto itself with its metadata replaced.
            let key = format!("{}/inode", tf.prefix);
            let mut copy = client.copy_object().bucket(&tf.bucket).key(&key)
                .copy_source(format!("{}/{}", tf.bucket, key))
                .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace);
            if let Some(m) = meta {
                copy = copy.metadata(hyperfile::compaction::METADATA_KEY, m);
            }
            copy.send().await.map_err(std::io::Error::other)?;
            let before = tf.objects(&client).await;
            let cleaner = S3Cleaner::with_client(&client, &uri).await?;
            let refused = cleaner.prune(1, 0).await.map_err(|e| e.kind());
            let still = tf.objects(&client).await;
            let tail = data(15 * MIB, MIB, 3);
            w.fs_write(15 * MIB, &tail).await?;
            w.fs_flush().await?;
            want[15 * MIB..].copy_from_slice(&tail);
            let _ = w.fs_release().await;
            drop(w);
            cleaner.prune(1, 0).await?;
            let got = read_cold(&client, &uri, 16 * MIB).await?;
            Ok((refused, before, still, want, got))
        }.await;
        tf.cleanup(&client).await;
        let (refused, before, still, want, got) = res.unwrap_or_else(|e| panic!("{:?}: {:?}: {}", meta, e.kind(), e));
        assert_eq!(refused, Err(std::io::ErrorKind::InvalidData), "{:?}", meta);
        assert_eq!(before, still, "{:?}: a refused prune deleted something", meta);
        assert!(got == want, "{:?}: read back wrong", meta);
    }
}

/// 16 MiB, then every other MiB again: each 4 MiB object of the first checkpoint
/// is half dead.
async fn half_dead(client: &aws_sdk_s3::Client, uri: &str, fmt: BlockPtrFormat) -> std::io::Result<(Hyper<'static>, Vec<u8>)> {
    let (meta, rt) = configs(fmt);
    let mut w = Hyper::fs_create_opt(client, uri, FileFlags::rdwr(), FileMode::default_file(), &meta, &rt).await?;
    let mut want = data(0, 16 * MIB, 1);
    w.fs_write(0, &want).await?;
    w.fs_flush().await?;
    for m in (1..16).step_by(2) {
        let d = data(m * MIB, MIB, 2);
        w.fs_write(m * MIB, &d).await?;
        want[m * MIB..(m + 1) * MIB].copy_from_slice(&d);
    }
    w.fs_flush().await?;
    Ok((w, want))
}

/// `compact` beside a writer that keeps its handle open and has writes not yet
/// published: the pass is attached, the number and the content checkpoint stay,
/// readers read through the objects as they were; the writer then takes it in,
/// and `prune` frees the objects the pass emptied. The data reads back cold
/// throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn compact_beside_a_writer_then_prune() {
    use hyperfile_cleaner::compact::CompactOptions;
    let _ = env_logger::try_init();
    let client = make_client().await;
    for fmt in FORMATS {
        let tf = TestFile::new();
        let uri = tf.uri();
        let res: std::io::Result<_> = async {
            let (mut w, mut want) = half_dead(&client, &uri, fmt).await?;
            let last_before = w.fs_last_cno();
            let content_before = w.fs_content_cno()?;
            // The writer's changes, not yet published.
            let d = data(2 * MIB, MIB, 3);
            w.fs_write(2 * MIB, &d).await?;
            want[2 * MIB..3 * MIB].copy_from_slice(&d);
            let cleaner = S3Cleaner::with_client(&client, &uri).await?;
            let opts = CompactOptions { beside_writer: true, max_live_ratio: 0.6, ..Default::default() };
            let out = cleaner.compact(1, 0, &opts).await?;
            let after_compact = Hyper::fs_open(&client, &uri, FileFlags::rdonly()).await?;
            let (last_c, content_c) = (after_compact.fs_last_cno(), after_compact.fs_content_cno()?);
            drop(after_compact);
            let got_c = read_cold(&client, &uri, 16 * MIB).await?;
            let attached = tf.objects(&client).await.len();
            cleaner.prune(1, 0).await?;
            let kept = tf.objects(&client).await.len();
            w.fs_flush().await?;
            let _ = w.fs_release().await;
            drop(w);
            let before = tf.objects(&client).await.len();
            cleaner.prune(1, 0).await?;
            let after = tf.objects(&client).await.len();
            let _ = (attached, kept);
            let got = read_cold(&client, &uri, 16 * MIB).await?;
            Ok((want, got, got_c, out, last_before, content_before, last_c, content_c, before, after))
        }.await;
        tf.cleanup(&client).await;
        let (want, got, got_c, out, last_before, content_before, last_c, content_c, before, after) =
            res.unwrap_or_else(|e| panic!("{:?}: {:?}: {}", fmt, e.kind(), e));
        let p = out.attached.unwrap_or_else(|| panic!("{:?}: nothing attached: {:?}", fmt, out));
        assert!(p.moved > 0 && out.checkpoints == 0, "{:?}: {:?}", fmt, out);
        assert!(out.still_referenced.is_empty(), "{:?}: {:?}", fmt, out.still_referenced);
        assert_eq!((last_c, content_c), (last_before, content_before), "{:?}: the compaction moved the numbers", fmt);
        let mut want_c = want.clone();
        want_c[2 * MIB..3 * MIB].copy_from_slice(&data(2 * MIB, MIB, 1));
        assert!(got_c == want_c, "{:?}: read back wrong after the compaction", fmt);
        assert!(got == want, "{:?}: read back wrong after the writer and prune", fmt);
        assert!(after < before, "{:?}: prune freed nothing", fmt);
    }
}

const GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// A handle beside the writer, with partials of 1 MiB past 2 MiB dirty: what it
/// relocates is on storage before it publishes.
async fn compactor(client: &aws_sdk_s3::Client, uri: &str) -> std::io::Result<Hyper<'static>> {
    use hyperfile::config::HyperFileConfigBuilder;
    use hyperfile::file::flags::HyperFileFlags;
    use hyperfile::staging::config::StagingConfig;
    let mut rt = HyperFileRuntimeConfig::default();
    rt.data_cache_dirty_max_bytes_threshold = 2 * MIB;
    rt.data_cache_dirty_max_blocks_threshold = 2 * MIB / BS;
    rt.data_cache_dirty_min_bytes_to_part = MIB;
    rt.data_cache_dirty_max_flush_interval = u64::MAX / 2;
    rt.data_cache_blocks = 0;
    let cfg = HyperFileConfigBuilder::new().with_staging_config(&StagingConfig::new_s3_uri(uri, None))
        .with_runtime_config(&rt).build();
    Hyper::open(client.clone(), cfg, HyperFileFlags::from_flags(FileFlags::rdwr())).await
}

/// A reader holds a version that stopped being the newest within the grace, but
/// was published before it: what it reads stays, until a prune without grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn prune_with_a_grace_keeps_what_a_reader_holds() {
    use hyperfile_cleaner::s3::PruneOptions;
    let _ = env_logger::try_init();
    let client = make_client().await;
    for fmt in FORMATS {
        let tf = TestFile::new();
        let uri = tf.uri();
        let res: std::io::Result<_> = async {
            let (mut w, want) = written(&client, &uri, fmt).await?;
            let mut r = Hyper::fs_open(&client, &uri, FileFlags::rdonly()).await?;
            // Old enough that only the version current when the window opened
            // names what this reader reads.
            tokio::time::sleep(GRACE + std::time::Duration::from_secs(3)).await;
            w.fs_write(0, &data(0, 16 * MIB, 9)).await?;
            w.fs_flush().await?;
            let _ = w.fs_release().await;
            drop(w);
            let cleaner = S3Cleaner::with_client(&client, &uri).await?;
            let taken = cleaner.prune_with(1, 0, &PruneOptions { grace: GRACE, ..Default::default() }).await?;
            let mut got = vec![0u8; 16 * MIB];
            let read = r.fs_read(0, &mut got).await.map(|_| got);
            let _ = r.fs_release().await;
            Ok((want, read, taken))
        }.await;
        tf.cleanup(&client).await;
        let (want, read, taken) = res.unwrap_or_else(|e| panic!("{:?}: {:?}: {}", fmt, e.kind(), e));
        let got = read.unwrap_or_else(|e| panic!("{:?}: the reader lost its objects: {}", fmt, e));
        assert!(got == want, "{:?}: the reader reads wrong", fmt);
        let _ = taken;
    }
}

/// A compaction in flight has written its copies out and not yet published:
/// referenced by nothing, they are kept for being young.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn prune_with_a_grace_keeps_a_compaction_in_flight() {
    use hyperfile_cleaner::s3::PruneOptions;
    let _ = env_logger::try_init();
    let client = make_client().await;
    for fmt in FORMATS {
        let tf = TestFile::new();
        let uri = tf.uri();
        let res: std::io::Result<_> = async {
            let (mut w, want) = written(&client, &uri, fmt).await?;
            let _ = w.fs_release().await;
            drop(w);
            let objects_before = tf.objects(&client).await.len();
            let mut c = compactor(&client, &uri).await?;
            c.fs_begin_compaction().await?;
            c.fs_relocate(&[(0, 16 * MIB / BS)]).await?;
            let in_flight = tf.objects(&client).await.len() - objects_before;
            let cleaner = S3Cleaner::with_client(&client, &uri).await?;
            cleaner.prune_with(1, 0, &PruneOptions { grace: GRACE, ..Default::default() }).await?;
            c.fs_publish_compaction(AGE).await?;
            let _ = c.fs_release().await;
            drop(c);
            // The writer takes in copies that are all still there.
            w_take_in(&client, &uri).await?;
            let got = read_cold(&client, &uri, 16 * MIB).await?;
            Ok((want, got, in_flight))
        }.await;
        tf.cleanup(&client).await;
        let (want, got, in_flight) = res.unwrap_or_else(|e| panic!("{:?}: {:?}: {}", fmt, e.kind(), e));
        assert!(in_flight > 0, "{:?}: the compactor wrote nothing before publishing", fmt);
        assert!(got == want, "{:?}: read back wrong", fmt);
    }
}

/// Publish of data by a fresh writer: rewrite the last block as it is.
async fn w_take_in(client: &aws_sdk_s3::Client, uri: &str) -> std::io::Result<()> {
    let mut w = Hyper::fs_open(client, uri, FileFlags::rdwr()).await?;
    let mut b = vec![0u8; BS];
    w.fs_read(16 * MIB - BS, &mut b).await?;
    w.fs_write(16 * MIB - BS, &b).await?;
    w.fs_flush().await?;
    let _ = w.fs_release().await;
    Ok(())
}

/// A reader opened while the compaction was attached reads through the objects
/// as they were; the writer takes the compaction in, and a `prune` with a grace
/// keeps those objects for the reader. Past the grace they go.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn prune_with_a_grace_keeps_what_a_reader_held_before_the_take_in() {
    use hyperfile_cleaner::s3::PruneOptions;
    let _ = env_logger::try_init();
    let client = make_client().await;
    for fmt in FORMATS {
        let tf = TestFile::new();
        let uri = tf.uri();
        let res: std::io::Result<_> = async {
            let (mut w, want) = written(&client, &uri, fmt).await?;
            let _ = w.fs_release().await;
            drop(w);
            attach(&client, &uri).await?;
            let mut r = Hyper::fs_open(&client, &uri, FileFlags::rdonly()).await?;
            w_take_in(&client, &uri).await?;
            let cleaner = S3Cleaner::with_client(&client, &uri).await?;
            let within = cleaner.prune_with(1, 0, &PruneOptions { grace: GRACE, ..Default::default() }).await?;
            let mut got = vec![0u8; 16 * MIB];
            let read = r.fs_read(0, &mut got).await.map(|_| got);
            let _ = r.fs_release().await;
            tokio::time::sleep(GRACE + std::time::Duration::from_secs(3)).await;
            let past = cleaner.prune_with(1, 0, &PruneOptions { grace: GRACE, ..Default::default() }).await?;
            let cold = read_cold(&client, &uri, 16 * MIB).await?;
            Ok((want, read, within, past, cold))
        }.await;
        tf.cleanup(&client).await;
        let (want, read, within, past, cold) = res.unwrap_or_else(|e| panic!("{:?}: {:?}: {}", fmt, e.kind(), e));
        let got = read.unwrap_or_else(|e| panic!("{:?}: the reader lost its objects: {}", fmt, e));
        assert!(got == want, "{:?}: the reader reads wrong", fmt);
        assert!(within.iter().all(|s| s.as_cno() == 1), "{:?}: within the grace, took {:?}", fmt, within);
        assert!(past.iter().any(|s| s.as_cno() == 2), "{:?}: past the grace, kept what the compaction emptied: {:?}", fmt, past);
        assert!(cold == want, "{:?}: read back wrong", fmt);
    }
}

/// Holds the writer's first inode write until told to go on.
#[derive(Clone)]
struct SlowInode(std::sync::Arc<tokio::sync::Notify>, std::sync::Arc<std::sync::atomic::AtomicBool>);

impl hyperfile::staging::StagingIntercept<hyperfile::staging::s3::S3Staging> for SlowInode {
    fn before_flush_inode(&self, _: &hyperfile::staging::s3::S3Staging, _: &[u8], _: hyperfile::inode::FlushInodeFlag)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + '_ + Send>> {
        let hold = self.1.swap(false, std::sync::atomic::Ordering::SeqCst);
        let go = self.0.clone();
        Box::pin(async move { if hold { go.notified().await; } Ok(()) })
    }
    fn after_flush_inode(&self, _: &hyperfile::staging::s3::S3Staging, _: &[u8], _: hyperfile::inode::FlushInodeFlag)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + '_ + Send>> {
        Box::pin(async { Ok(()) })
    }
    fn after_remove_inode(&self, _: &hyperfile::staging::s3::S3Staging)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + '_ + Send>> {
        Box::pin(async { Ok(()) })
    }
}

/// The writer's inode write is slow: its part 0 has landed, and is old enough
/// that a writable open would take it up. `compact` beside the writer leaves it
/// to the writer -- it does not touch the inode, and waits with `ResourceBusy` --
/// and the writer, under `FailFast`, publishes. Compacted afterwards, the data
/// reads back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn compact_beside_a_writer_whose_inode_write_is_slow() {
    use hyperfile_cleaner::compact::CompactOptions;
    let _ = env_logger::try_init();
    let client = make_client().await;
    let fmt = BlockPtrFormat::PartedSegment;
    let tf = TestFile::new();
    let uri = tf.uri();
    let res: std::io::Result<_> = async {
        let (meta, mut rt) = configs(fmt);
        rt.flush_conflict_policy = hyperfile::config::FlushConflictPolicy::FailFast;
        let mut w = Hyper::fs_create_opt(&client, &uri, FileFlags::rdwr(), FileMode::default_file(), &meta, &rt).await?;
        let mut want = data(0, 16 * MIB, 1);
        w.fs_write(0, &want).await?;
        w.fs_flush().await?;
        for m in (1..16).step_by(2) {
            let d = data(m * MIB, MIB, 2);
            w.fs_write(m * MIB, &d).await?;
            want[m * MIB..(m + 1) * MIB].copy_from_slice(&d);
        }
        w.fs_flush().await?;
        let cleaner = S3Cleaner::with_client(&client, &uri).await?;
        let head = cleaner.report(1, 0).await?.high_cno;
        let go = std::sync::Arc::new(tokio::sync::Notify::new());
        w.with_staging_interceptor(SlowInode(go.clone(), std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true))));
        let d = data(0, MIB, 3);
        w.fs_write(0, &d).await?;
        want[..MIB].copy_from_slice(&d);
        let commit = format!("{:0>10}.0", head + 1);
        let opts = CompactOptions { beside_writer: true, max_live_ratio: 0.6, ..Default::default() };
        let compactor = async {
            while !tf.objects(&client).await.contains(&commit) {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            // Past the few seconds a writable open gives a writer to publish.
            tokio::time::sleep(std::time::Duration::from_secs(7)).await;
            let compacted = cleaner.compact(1, 0, &opts).await.map(|_| ()).map_err(|e| e.kind());
            let stored = cleaner.report(1, 0).await.map(|r| r.high_cno);
            go.notify_one();
            (compacted, stored)
        };
        let (flushed, (compacted, stored)) = tokio::join!(w.fs_flush(), compactor);
        let flushed = flushed.map(|_| ()).map_err(|e| (e.kind(), e.to_string()));
        let _ = w.fs_release().await;
        drop(w);
        let after = cleaner.compact(1, 0, &opts).await?;
        let got = read_cold(&client, &uri, 16 * MIB).await?;
        Ok((head, compacted, stored, flushed, after, want, got))
    }.await;
    tf.cleanup(&client).await;
    let (head, compacted, stored, flushed, after, want, got) = res.unwrap_or_else(|e| panic!("{:?}: {}", e.kind(), e));
    assert_eq!(stored.expect("report"), head, "the compactor published the writer's checkpoint for it");
    assert_eq!(compacted, Err(std::io::ErrorKind::ResourceBusy));
    flushed.unwrap_or_else(|e| panic!("the writer's publish failed: {:?}", e));
    assert!(after.attached.is_some_and(|p| p.moved > 0), "{:?}", after);
    assert!(got == want, "read back wrong");
}
