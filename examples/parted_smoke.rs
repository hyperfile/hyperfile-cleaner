#![recursion_limit = "1024"]
//! Temporary verification for the hyperfile 0.7.0 migration: a container whose
//! format is `PartedSegment`, written so that memory pressure produces real
//! partial segments, must have its objects reported and reclaimed at object
//! granularity.
use std::io::Result;
use aws_sdk_s3::Client;
use hyperfile::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use hyperfile::meta_format::BlockPtrFormat;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::mode::FileMode;
use hyperfile_cleaner::{Cleaner, s3::S3Cleaner};

const CHUNK: usize = 512 * 1024;
const CHUNKS: usize = 16; // 8 MiB

fn configs() -> (HyperFileMetaConfig, HyperFileRuntimeConfig) {
    let meta = HyperFileMetaConfig::new(56, 4096, 4096, BlockPtrFormat::PartedSegment);
    let rt = HyperFileRuntimeConfig {
        // Cross the dirty threshold every couple of chunks, and let a partial be
        // worth writing well below it, so partials actually happen.
        data_cache_dirty_max_bytes_threshold: 1024 * 1024,
        data_cache_dirty_max_blocks_threshold: 256,
        data_cache_dirty_min_bytes_to_part: 256 * 1024,
        // Keep the interval from being the thing that triggers them.
        data_cache_dirty_max_flush_interval: 60_000,
        ..HyperFileRuntimeConfig::default()
    };
    (meta, rt)
}

async fn write_whole_file(hyper: &mut Hyper<'static>, fill: u8) -> Result<()> {
    for i in 0..CHUNKS {
        let payload = vec![fill.wrapping_add(i as u8); CHUNK];
        let _ = hyper.fs_write(i * CHUNK, &payload).await?;
    }
    let _ = hyper.fs_flush().await?;
    Ok(())
}

async fn verify_contents(hyper: &mut Hyper<'static>, fill: u8) -> Result<()> {
    for i in 0..CHUNKS {
        let mut buf = vec![0u8; CHUNK];
        let n = hyper.fs_read(i * CHUNK, &mut buf).await?;
        assert_eq!(n, CHUNK, "short read of chunk {}", i);
        let want = fill.wrapping_add(i as u8);
        assert!(buf.iter().all(|b| *b == want),
            "chunk {} came back wrong (wanted {:#x})", i, want);
    }
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    env_logger::init();
    let uri = std::env::var("HYPERFILE_STAGING_ROOT_URI")
        .expect("HYPERFILE_STAGING_ROOT_URI must be set");
    let cfg = aws_config::load_from_env().await;
    let client = Client::new(&cfg);
    let (meta, rt) = configs();

    let _ = Hyper::fs_unlink(&client, &uri).await;
    let mut hyper = Hyper::fs_create_opt(
        &client, &uri, FileFlags::rdwr(), FileMode::default_file(), &meta, &rt,
    ).await?;
    write_whole_file(&mut hyper, 0x10).await?;
    let _ = hyper.fs_release().await?;

    let cleaner = S3Cleaner::with_client(&client, &uri).await?;
    let (refed, unrefed) = cleaner.collect(1, 0).await?;
    println!("checkpoint 1: referenced={:?}", refed);
    println!("checkpoint 1: unreferenced={:?}", unrefed);
    assert!(refed.iter().any(|s| s.part_id().is_some()),
        "a parted container must report parted objects, got {:?}", refed);
    assert!(refed.iter().any(|s| s.is_partial()),
        "the write should have produced partials, got {:?}", refed);
    assert!(refed.iter().filter(|s| s.as_cno() == 1).count() > 1,
        "expected several objects in checkpoint 1, got {:?}", refed);

    // Whatever is unreferenced here is metadata a later partial superseded. It
    // must be reclaimable without touching the data, so prune and read back.
    cleaner.prune(1, 0).await?;
    let mut hyper = Hyper::fs_open(&client, &uri, FileFlags::rdwr()).await?;
    verify_contents(&mut hyper, 0x10).await?;
    println!("checkpoint 1 data intact after prune");

    // Overwrite everything: checkpoint 1's objects are all dead now.
    write_whole_file(&mut hyper, 0xA0).await?;
    let _ = hyper.fs_release().await?;

    let (refed, unrefed) = cleaner.collect(1, 0).await?;
    println!("checkpoint 2: referenced={:?}", refed);
    println!("checkpoint 2: unreferenced={:?}", unrefed);
    assert!(!unrefed.is_empty(), "checkpoint 1's objects must be reclaimable");
    assert!(unrefed.iter().all(|s| s.as_cno() < 2),
        "nothing from the live checkpoint may be reclaimable, got {:?}", unrefed);
    assert!(refed.iter().all(|s| s.as_cno() >= 2),
        "checkpoint 1 should hold nothing live, got {:?}", refed);

    cleaner.prune(1, 0).await?;
    let mut hyper = Hyper::fs_open(&client, &uri, FileFlags::rdwr()).await?;
    verify_contents(&mut hyper, 0xA0).await?;
    let _ = hyper.fs_release().await?;

    let (refed, unrefed) = cleaner.collect(1, 0).await?;
    println!("after prune: referenced={:?} unreferenced={:?}", refed, unrefed);
    assert!(unrefed.is_empty(), "a second prune should have nothing to do");
    println!("OK");

    let _ = Hyper::fs_unlink(&client, &uri).await;
    Ok(())
}
