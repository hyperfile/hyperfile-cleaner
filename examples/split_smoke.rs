//! A checkpoint larger than `segment_max_object_bytes` is written as several objects
//! (hyperfile 0.9) even though no partial segment was ever written. The cleaner must
//! report every one of them, keep each part that holds a live block, compact across
//! them, and let `prune` take the rest.
#![recursion_limit = "1024"]
use std::io::Result;
use aws_sdk_s3::Client;
use hyperfile::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use hyperfile::file::flags::FileFlags;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::mode::FileMode;
use hyperfile::meta_format::BlockPtrFormat;
use hyperfile_cleaner::compact::CompactOptions;
use hyperfile_cleaner::{Cleaner, s3::S3Cleaner};

const BS: usize = 4096;
const BLOCKS: usize = 512; // 2 MiB
const KEEP: usize = 64;

fn configs() -> (HyperFileMetaConfig, HyperFileRuntimeConfig) {
    let meta = HyperFileMetaConfig::new(56, 4096, 4096, BlockPtrFormat::PartedSegment);
    let rt = HyperFileRuntimeConfig {
        // Small objects, and thresholds far above what one flush holds, so the
        // only reason this checkpoint has several objects is its size.
        segment_max_object_bytes: 64 * 1024,
        data_cache_dirty_max_bytes_threshold: 1 << 30,
        data_cache_dirty_max_blocks_threshold: 1 << 20,
        segment_buffer_size: 1 << 30,
        ..HyperFileRuntimeConfig::default()
    };
    (meta, rt)
}

async fn verify(client: &Client, uri: &str, idx: usize, want: u8) -> Result<()> {
    let mut file = Hyper::fs_open(client, uri, FileFlags::rdonly()).await?;
    let mut buf = vec![0u8; BS];
    assert_eq!(file.fs_read(idx * BS, &mut buf).await?, BS, "short read {}", idx);
    assert!(buf.iter().all(|b| *b == want), "block {} is {:#x}, wanted {:#x}", idx, buf[0], want);
    let _ = file.fs_release().await;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    env_logger::init();
    let uri = std::env::var("HYPERFILE_STAGING_ROOT_URI").expect("uri");
    let cfg = aws_config::load_from_env().await;
    let client = Client::new(&cfg);
    let (meta, rt) = configs();

    let _ = Hyper::fs_unlink(&client, &uri).await;
    let mut file = Hyper::fs_create_opt(&client, &uri, FileFlags::rdwr(),
        FileMode::default_file(), &meta, &rt).await?;
    // Checkpoint 1: everything, in ONE flush.
    for idx in 0..BLOCKS {
        let _ = file.fs_write(idx * BS, &vec![0x11u8; BS]).await?;
    }
    let _ = file.fs_flush().await?;
    // Checkpoint 2: overwrite all but every KEEPth block.
    for idx in 0..BLOCKS {
        if idx % KEEP != 0 {
            let _ = file.fs_write(idx * BS, &vec![0x22u8; BS]).await?;
        }
    }
    let _ = file.fs_flush().await?;
    let _ = file.fs_release().await?;

    let cleaner = S3Cleaner::with_client(&client, &uri).await?;
    let before = cleaner.report(1, 0).await?;
    let cp1: Vec<_> = before.objects.iter().filter(|o| o.segid.as_cno() == 1).collect();
    let cp2: Vec<_> = before.objects.iter().filter(|o| o.segid.as_cno() == 2).collect();
    println!("checkpoint 1: {} objects, checkpoint 2: {} objects", cp1.len(), cp2.len());
    for o in before.objects.iter().filter(|o| o.live_data_blocks + o.live_meta_blocks > 0
        || o.segid.as_cno() == 1).take(12) {
        println!("  {:>6} {:>8} B  data {:>3}  meta {:>2}  ratio {:.3}  partial={}",
            o.segid.to_string(), o.bytes, o.live_data_blocks, o.live_meta_blocks,
            o.live_ratio(), o.segid.is_partial());
    }
    assert!(cp1.len() > 2, "checkpoint 1 should be split by size alone: {}", cp1.len());
    assert!(cp1.iter().any(|o| o.segid.part_id() == Some(0)), "part 0 missing");
    assert!(cp1.iter().filter(|o| o.segid.is_partial()).count() > 1,
        "data-only parts expected");
    let live_in_cp1: usize = cp1.iter().map(|o| o.live_data_blocks).sum();
    assert_eq!(live_in_cp1, BLOCKS / KEEP, "survivors across all parts");

    // collect/prune must agree with report, and never offer a part with live data.
    let (refed, unrefed) = cleaner.collect(1, 0).await?;
    let live_parts: Vec<_> = cp1.iter().filter(|o| o.live_data_blocks > 0).collect();
    println!("collect: {} referenced, {} reclaimable; cp1 has {} part(s) with live data",
        refed.len(), unrefed.len(), live_parts.len());
    assert!(!unrefed.iter().any(|s| live_parts.iter().any(|o| o.segid == *s)),
        "a part holding live data must never be reclaimable");

    // Compaction across the split checkpoint, then prune.
    let out = cleaner.compact(1, 0, &CompactOptions::default()).await?;
    println!("compacted: {} block(s) in {} checkpoint(s), selected {}",
        out.blocks_rewritten, out.checkpoints, out.selected.len());
    assert_eq!(out.blocks_rewritten, BLOCKS / KEEP);
    assert!(out.still_referenced.is_empty(), "got {:?}", out.still_referenced);
    cleaner.prune(1, 0).await?;
    let after = cleaner.report(1, 0).await?;
    assert_eq!(after.dead(), (0, 0));
    assert!(after.objects.iter().all(|o| o.segid.as_cno() != 1),
        "every object of checkpoint 1 should be gone: {:?}",
        after.objects.iter().filter(|o| o.segid.as_cno() == 1).count());
    println!("after prune: {} objects, {} bytes (was {} in {})",
        after.objects.len(), after.bytes(), before.bytes(), before.objects.len());

    verify(&client, &uri, 0, 0x11).await?;
    verify(&client, &uri, KEEP, 0x11).await?;
    verify(&client, &uri, 1, 0x22).await?;
    verify(&client, &uri, BLOCKS - 1, 0x22).await?;
    println!("OK");
    let _ = Hyper::fs_unlink(&client, &uri).await;
    Ok(())
}
