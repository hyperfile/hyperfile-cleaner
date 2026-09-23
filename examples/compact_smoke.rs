//! Temporary verification for offline compaction, against real S3.
//!
//! Two shapes, one mechanism:
//!
//! 1. **Scattered**: 64 blocks written one flush apiece, so each lands in its own
//!    object and reading the file costs a request per block. Compaction should
//!    gather them into one object and the read plan should collapse.
//! 2. **Mostly dead**: one large checkpoint, then an overwrite that supersedes all
//!    but a few of its blocks. Compaction should rewrite those few, after which
//!    the big object is reclaimable by `prune`.
#![recursion_limit = "1024"]
use std::io::Result;
use aws_sdk_s3::Client;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::mode::FileMode;
use hyperfile_cleaner::compact::CompactOptions;
use hyperfile_cleaner::{Cleaner, s3::S3Cleaner};

const BS: usize = 4096;

async fn read_requests(client: &Client, uri: &str, len: usize) -> Result<usize> {
    // A cold handle: the plan follows the data cache, so a warm file looks free.
    let mut file = Hyper::fs_open(client, uri, FileFlags::rdonly()).await?;
    let plan = file.fs_read_plan(0, len as u64).await?;
    let gets = plan.iter().filter(|e| e.is_get()).count();
    let _ = file.fs_release().await;
    Ok(gets)
}

async fn verify_block(client: &Client, uri: &str, idx: usize, want: u8) -> Result<()> {
    let mut file = Hyper::fs_open(client, uri, FileFlags::rdonly()).await?;
    let mut buf = vec![0u8; BS];
    let n = file.fs_read(idx * BS, &mut buf).await?;
    assert_eq!(n, BS, "short read of block {}", idx);
    assert!(buf.iter().all(|b| *b == want),
        "block {} reads {:#x}, wanted {:#x}", idx, buf[0], want);
    let _ = file.fs_release().await;
    Ok(())
}

/// 64 blocks, one flush each.
async fn scattered(client: &Client, uri: &str) -> Result<()> {
    println!("=== scattered: one block per checkpoint");
    let _ = Hyper::fs_unlink(client, uri).await;
    let mut file = Hyper::fs_create(
        client, uri, FileFlags::rdwr(), FileMode::default_file()).await?;
    for idx in 0..64usize {
        let payload = vec![idx as u8; BS];
        let _ = file.fs_write(idx * BS, &payload).await?;
        let _ = file.fs_flush().await?;
    }
    let _ = file.fs_release().await?;

    let cleaner = S3Cleaner::with_client(client, uri).await?;
    let before = cleaner.report(1, 0).await?;
    let gets_before = read_requests(client, uri, 64 * BS).await?;
    println!("before: {} objects, {} bytes, {} live, {} requests to read the file",
        before.objects.len(), before.bytes(), before.bytes_live(), gets_before);
    let (cand, rewrite, freed) = before.compactable(0.5);
    println!("before: {} candidate object(s), {} bytes to rewrite, {} bytes freed",
        cand, rewrite, freed);
    assert!(cand > 1, "every checkpoint but the newest should be mostly dead");

    let dry = cleaner.compact(1, 0, &CompactOptions { dry_run: true, ..Default::default() }).await?;
    assert_eq!(dry.selected.len(), cand, "a dry run must agree with the estimate");
    assert_eq!(dry.bytes_freed, freed);

    let out = cleaner.compact(1, 0, &CompactOptions::default()).await?;
    println!("compacted: {} block(s), {} bytes, in {} checkpoint(s); freed {} bytes",
        out.blocks_rewritten, out.bytes_rewritten, out.checkpoints, out.bytes_freed);
    assert!(out.still_referenced.is_empty(),
        "selected objects should hold nothing live afterwards: {:?}", out.still_referenced);

    // Every block must still read as itself.
    for idx in [0usize, 1, 31, 62, 63] {
        verify_block(client, uri, idx, idx as u8).await?;
    }

    let gets_after = read_requests(client, uri, 64 * BS).await?;
    println!("after: {} requests to read the file", gets_after);
    assert!(gets_after < gets_before / 4,
        "reading should cost far fewer requests: {} -> {}", gets_before, gets_after);

    let (refed, unrefed) = cleaner.collect(1, 0).await?;
    println!("after: {} referenced object(s), {} reclaimable", refed.len(), unrefed.len());
    assert!(unrefed.len() >= out.selected.len());
    cleaner.prune(1, 0).await?;

    let after = cleaner.report(1, 0).await?;
    println!("pruned: {} objects, {} bytes, {} live",
        after.objects.len(), after.bytes(), after.bytes_live());
    assert!(after.bytes() < before.bytes(), "storage should have shrunk");
    assert_eq!(after.dead(), (0, 0), "prune should have taken every dead object");
    for idx in [0usize, 1, 31, 62, 63] {
        verify_block(client, uri, idx, idx as u8).await?;
    }
    let _ = Hyper::fs_unlink(client, uri).await;
    Ok(())
}

/// One big checkpoint, then an overwrite that leaves a few of its blocks live.
async fn mostly_dead(client: &Client, uri: &str) -> Result<()> {
    println!("=== mostly dead: a 1%-live object");
    const BLOCKS: usize = 1024;
    const KEEP: usize = 64; // every 64th block survives the overwrite
    let _ = Hyper::fs_unlink(client, uri).await;
    let mut file = Hyper::fs_create(
        client, uri, FileFlags::rdwr(), FileMode::default_file()).await?;
    // Checkpoint 1: the whole file in one flush.
    for idx in 0..BLOCKS {
        let payload = vec![0x11u8; BS];
        let _ = file.fs_write(idx * BS, &payload).await?;
    }
    let _ = file.fs_flush().await?;
    // Checkpoint 2: overwrite everything except every KEEPth block, so what is
    // left of checkpoint 1 is a few data blocks and no live metadata.
    for idx in 0..BLOCKS {
        if idx % KEEP == 0 {
            continue;
        }
        let payload = vec![0x22u8; BS];
        let _ = file.fs_write(idx * BS, &payload).await?;
    }
    let _ = file.fs_flush().await?;
    let _ = file.fs_release().await?;

    let cleaner = S3Cleaner::with_client(client, uri).await?;
    let before_stat = Hyper::fs_getattr_fast(client, uri).await?;
    let before = cleaner.report(1, 0).await?;
    for o in &before.objects {
        println!("  {} : {} bytes, {} live data, {} live meta, ratio {:.3}",
            o.segid, o.bytes, o.live_data_blocks, o.live_meta_blocks, o.live_ratio());
    }
    let survivors = BLOCKS / KEEP;
    let first = before.objects.iter().find(|o| o.segid.as_cno() == 1)
        .expect("checkpoint 1 should still be there");
    assert_eq!(first.live_data_blocks, survivors,
        "checkpoint 1 should hold exactly the blocks the overwrite skipped");
    assert!(!first.holds_metadata(),
        "the overwrite should have rewritten every node of checkpoint 1");
    let first_bytes = first.bytes;

    let out = cleaner.compact(1, 0, &CompactOptions::default()).await?;
    println!("compacted: {} block(s), {} bytes; freed {} bytes across {:?}",
        out.blocks_rewritten, out.bytes_rewritten, out.bytes_freed, out.selected);
    assert!(out.selected.iter().any(|s| s.as_cno() == 1), "checkpoint 1 was the point");
    assert_eq!(out.blocks_rewritten, survivors);
    assert!(out.still_referenced.is_empty(), "got {:?}", out.still_referenced);

    // A compaction changes no content, so mtime must not move. ctime does:
    // hyperfile advances it on any attribute update and it is not settable.
    let after_stat = Hyper::fs_getattr_fast(client, uri).await?;
    assert_eq!((after_stat.st_mtime, after_stat.st_mtime_nsec),
        (before_stat.st_mtime, before_stat.st_mtime_nsec),
        "mtime moved: {} -> {}", before_stat.st_mtime, after_stat.st_mtime);
    assert!(after_stat.st_ctime >= before_stat.st_ctime, "ctime went backwards");
    println!("mtime preserved ({}), ctime now {}", after_stat.st_mtime, after_stat.st_ctime);

    cleaner.prune(1, 0).await?;
    let after = cleaner.report(1, 0).await?;
    println!("pruned: {} objects, {} bytes (was {} in {} objects)",
        after.objects.len(), after.bytes(), before.bytes(), before.objects.len());
    assert!(after.objects.iter().all(|o| o.segid.as_cno() != 1),
        "checkpoint 1 should be gone");
    // What was given back is the freed object, less what holding its live blocks
    // in a new checkpoint costs.
    let recovered = before.bytes() - after.bytes();
    println!("recovered {} bytes for {} rewritten", recovered, out.bytes_rewritten);
    assert!(recovered * 10 >= first_bytes * 9,
        "freeing a {} byte object recovered only {} bytes", first_bytes, recovered);

    // Content is what it was: the survivors and the overwritten blocks both.
    verify_block(client, uri, 0, 0x11).await?;
    verify_block(client, uri, KEEP, 0x11).await?;
    verify_block(client, uri, 1, 0x22).await?;
    verify_block(client, uri, BLOCKS - 1, 0x22).await?;
    let _ = Hyper::fs_unlink(client, uri).await;
    Ok(())
}

/// A key under the container's prefix that is not the inode and not a segment
/// object — what a WAL root nested under the container looks like from here.
/// Compaction must refuse rather than publish over a log it is not reading.
async fn refuses_a_possible_log(client: &Client, uri: &str) -> Result<()> {
    println!("=== a foreign key stops a compaction");
    let _ = Hyper::fs_unlink(client, uri).await;
    let mut file = Hyper::fs_create(
        client, uri, FileFlags::rdwr(), FileMode::default_file()).await?;
    for idx in 0..8usize {
        let payload = vec![idx as u8; BS];
        let _ = file.fs_write(idx * BS, &payload).await?;
        let _ = file.fs_flush().await?;
    }
    let _ = file.fs_release().await?;

    let (bucket, key) = uri.trim_start_matches("s3://").split_once('/').unwrap();
    let wal_key = format!("{}/wal/0000000001/0_0_4096", key);
    client.put_object().bucket(bucket).key(&wal_key)
        .body(vec![0u8; 16].into()).send().await
        .map_err(|e| std::io::Error::other(format!("put {}: {}", wal_key, e)))?;

    let cleaner = S3Cleaner::with_client(client, uri).await?;
    let report = cleaner.report(1, 0).await?;
    assert_eq!(report.foreign_keys, 1, "the report should name what it did not recognize");
    println!("report: {} foreign key(s), sample {:?}",
        report.foreign_keys, report.foreign_sample);

    let e = cleaner.compact(1, 0, &CompactOptions::default()).await
        .expect_err("compaction must refuse while a possible log is present");
    assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
    println!("refused: {}", e);

    // With the caller taking responsibility, it runs.
    let out = cleaner.compact(1, 0, &CompactOptions {
        allow_foreign_keys: true, ..Default::default()
    }).await?;
    assert!(out.blocks_rewritten > 0, "should have work to do");
    println!("allowed: rewrote {} block(s)", out.blocks_rewritten);

    client.delete_object().bucket(bucket).key(&wal_key).send().await
        .map_err(|e| std::io::Error::other(format!("delete {}: {}", wal_key, e)))?;
    let _ = Hyper::fs_unlink(client, uri).await;
    Ok(())
}

/// A sparse file, compacted in batches of four blocks.
///
/// Two things nothing else here reaches: the batch loop firing mid-sweep rather
/// than only at the tail, and what compaction must not do to a hole. Rewriting a
/// hole would make a sparse file dense, and rewriting an explicit zero block —
/// which occupies no storage — would start costing storage for the same read.
async fn sparse_in_small_batches(client: &Client, uri: &str) -> Result<()> {
    println!("=== sparse file, four-block batches");
    let _ = Hyper::fs_unlink(client, uri).await;
    let mut file = Hyper::fs_create(
        client, uri, FileFlags::rdwr(), FileMode::default_file()).await?;
    // Data at 0..16 and at 1000..1016, nothing in between: one large hole.
    for idx in (0..16usize).chain(1000..1016) {
        let payload = vec![0x33u8; BS];
        let _ = file.fs_write(idx * BS, &payload).await?;
        let _ = file.fs_flush().await?;
    }
    // An explicit zero block inside the written region.
    let _ = file.fs_write_zero(8 * BS, BS).await?;
    let _ = file.fs_flush().await?;
    let _ = file.fs_release().await?;

    let states_before = block_states(client, uri, &[0, 8, 500, 1000]).await?;
    println!("states before: {:?}", states_before);

    let cleaner = S3Cleaner::with_client(client, uri).await?;
    let out = cleaner.compact(1, 0, &CompactOptions {
        batch_bytes: 4 * BS, ..Default::default()
    }).await?;
    println!("compacted: {} block(s) in {} checkpoint(s)",
        out.blocks_rewritten, out.checkpoints);
    assert!(out.checkpoints > 3,
        "four-block batches over {} blocks should publish several checkpoints, got {}",
        out.blocks_rewritten, out.checkpoints);
    assert!(out.still_referenced.is_empty(), "got {:?}", out.still_referenced);

    let states_after = block_states(client, uri, &[0, 8, 500, 1000]).await?;
    println!("states after : {:?}", states_after);
    assert_eq!(states_before, states_after,
        "compaction must not change what a block is, only where it is");

    cleaner.prune(1, 0).await?;
    verify_block(client, uri, 0, 0x33).await?;
    verify_block(client, uri, 8, 0x00).await?;    // the explicit zero block
    verify_block(client, uri, 500, 0x00).await?;  // the hole
    verify_block(client, uri, 1015, 0x33).await?;
    let _ = Hyper::fs_unlink(client, uri).await;
    Ok(())
}

async fn block_states(client: &Client, uri: &str, indices: &[u64]) -> Result<Vec<String>> {
    let mut file = Hyper::fs_open(client, uri, FileFlags::rdonly()).await?;
    let mut out = Vec::new();
    for idx in indices {
        out.push(format!("{}:{:?}", idx, file.fs_block_state(*idx).await?));
    }
    let _ = file.fs_release().await;
    Ok(out)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    env_logger::init();
    let uri = std::env::var("HYPERFILE_STAGING_ROOT_URI")
        .expect("HYPERFILE_STAGING_ROOT_URI must be set");
    let cfg = aws_config::load_from_env().await;
    let client = Client::new(&cfg);

    scattered(&client, &format!("{}/scattered", uri)).await?;
    mostly_dead(&client, &format!("{}/mostly-dead", uri)).await?;
    refuses_a_possible_log(&client, &format!("{}/with-log", uri)).await?;
    sparse_in_small_batches(&client, &format!("{}/sparse", uri)).await?;
    println!("OK");
    Ok(())
}
