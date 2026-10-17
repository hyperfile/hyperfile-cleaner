//! A container in hyperfile 0.11's `StreamSegment` format, against real S3.
//!
//! Every object of such a container is laid out tail first and describes itself:
//! its data, the leaves of the block map over that data, a summary and a header.
//! So a data part holds live metadata as long as its leaves are live — which the
//! other formats' rule "an object holding a live metadata node is not a
//! candidate" would read as "never compact a data part".
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
const BLOCKS: usize = 2048; // 8 MiB
const KEEP: usize = 64;

fn configs() -> (HyperFileMetaConfig, HyperFileRuntimeConfig) {
    let meta = HyperFileMetaConfig::new(56, 4096, 4096, BlockPtrFormat::StreamSegment);
    // Objects of about 1 MiB, cut both by the partials the dirty-data limits write
    // and, in a reactor build, by sending ahead.
    let rt = HyperFileRuntimeConfig {
        data_cache_dirty_max_bytes_threshold: 2 << 20,
        data_cache_dirty_max_blocks_threshold: (2 << 20) / BS,
        data_cache_dirty_min_bytes_to_part: 1 << 20,
        segment_buffer_size: 2 << 20,
        segment_max_object_bytes: 1 << 20,
        stream_segment_bytes: 1 << 20,
        data_cache_dirty_max_flush_interval: 600_000,
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

/// Objects that hold a live leaf of the block map and are still mostly dead.
///
/// A leaf goes into the object holding its data. Overwriting half of every 1024
/// blocks leaves the leaves wholly inside the untouched half live, in objects
/// that are half dead. Those objects hold live metadata, and only the stream
/// layout guarantees that rewriting their data rewrites that metadata too.
async fn live_leaves(client: &Client, uri: &str) -> Result<()> {
    println!("=== objects holding a live leaf");
    const N: usize = 4096; // 16 MiB
    let (meta, mut rt) = configs();
    // Objects of about 4 MiB, so one holds several leaves.
    rt.segment_max_object_bytes = 4 << 20;
    rt.stream_segment_bytes = 4 << 20;
    rt.data_cache_dirty_max_bytes_threshold = 8 << 20;
    rt.data_cache_dirty_max_blocks_threshold = (8 << 20) / BS;
    rt.data_cache_dirty_min_bytes_to_part = 4 << 20;
    rt.segment_buffer_size = 8 << 20;
    let _ = Hyper::fs_unlink(client, uri).await;
    let mut file = Hyper::fs_create_opt(client, uri, FileFlags::rdwr(),
        FileMode::default_file(), &meta, &rt).await?;
    for idx in 0..N {
        let _ = file.fs_write(idx * BS, &vec![0x31u8; BS]).await?;
    }
    let _ = file.fs_flush().await?;
    for idx in 0..N {
        if idx % 1024 < 512 {
            let _ = file.fs_write(idx * BS, &vec![0x32u8; BS]).await?;
        }
    }
    let _ = file.fs_flush().await?;
    let _ = file.fs_release().await?;

    let cleaner = S3Cleaner::with_client(client, uri).await?;
    let before = cleaner.report(1, 0).await?;
    let cp1: Vec<_> = before.objects.iter().filter(|o| o.segid.as_cno() == 1).collect();
    for o in &cp1 {
        println!("  {:>5} {:>8} B  data {:>4}  meta {:>2}  ratio {:.3}",
            o.segid.to_string(), o.bytes, o.live_data_blocks, o.live_meta_blocks, o.live_ratio());
    }
    let with_live_meta = cp1.iter().filter(|o| o.segid.is_partial() && o.holds_metadata()).count();
    println!("data parts of checkpoint 1 holding a live leaf: {}", with_live_meta);
    assert!(with_live_meta > 0, "the scenario should leave live leaves in data parts");

    let out = cleaner.compact(1, 0, &CompactOptions::default()).await?;
    let picked_with_meta = cp1.iter().filter(|o| o.holds_metadata() && out.selected.contains(&o.segid)).count();
    println!("compact: {} block(s), selected {} ({} holding a live leaf), still referenced {:?}, content {} -> {}",
        out.blocks_rewritten, out.selected.len(), picked_with_meta, out.still_referenced,
        out.content_cno_before, out.content_cno_after);
    assert!(picked_with_meta > 0, "a data part holding a live leaf is a candidate");
    assert!(out.still_referenced.is_empty(),
        "rewriting a data part's live data must free its leaves too: {:?}", out.still_referenced);
    assert!(!out.content_moved());

    cleaner.prune(1, 0).await?;
    let after = cleaner.report(1, 0).await?;
    let picked_left: Vec<_> = after.objects.iter().filter(|o| out.selected.contains(&o.segid))
        .map(|o| o.segid.to_string()).collect();
    assert!(picked_left.is_empty(), "selected objects left after prune: {:?}", picked_left);
    println!("after prune: {} object(s), {} B (was {} in {})",
        after.objects.len(), after.bytes(), before.bytes(), before.objects.len());
    for idx in [0usize, 511, 512, 1023, 1024, 3583, 3584, N - 1] {
        verify(client, uri, idx, if idx % 1024 < 512 { 0x32 } else { 0x31 }).await?;
    }
    let _ = Hyper::fs_unlink(client, uri).await;
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
    for idx in 0..BLOCKS {
        let _ = file.fs_write(idx * BS, &vec![0x11u8; BS]).await?;
    }
    let _ = file.fs_flush().await?;
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
    println!("checkpoint 1: {} object(s); checkpoint 2: {} object(s)", cp1.len(),
        before.objects.iter().filter(|o| o.segid.as_cno() == 2).count());
    for o in cp1.iter().take(6) {
        println!("  {:>5} {:>8} B  data {:>3}  meta {:>2}  ratio {:.3}",
            o.segid.to_string(), o.bytes, o.live_data_blocks, o.live_meta_blocks, o.live_ratio());
    }
    let survivors: usize = cp1.iter().map(|o| o.live_data_blocks).sum();
    assert_eq!(survivors, BLOCKS / KEEP, "survivors across checkpoint 1's objects");
    let (cand, rewrite, freed) = before.compactable(0.5);
    println!("compactable at 0.5: {} object(s), rewrite {} B, free {} B", cand, rewrite, freed);

    // collect / prune exactness: nothing holding a survivor is offered.
    let (_, unrefed) = cleaner.collect(1, 0).await?;
    let live: Vec<_> = cp1.iter().filter(|o| o.live_data_blocks + o.live_meta_blocks > 0)
        .map(|o| o.segid).collect();
    assert!(!unrefed.iter().any(|s| live.contains(s)), "a live object offered for reclaim");
    cleaner.prune(1, 0).await?;
    verify(&client, &uri, 0, 0x11).await?;
    verify(&client, &uri, KEEP, 0x11).await?;
    verify(&client, &uri, 1, 0x22).await?;
    println!("collect/prune: exact, data intact");

    let out = cleaner.compact(1, 0, &CompactOptions::default()).await?;
    println!("compact: {} block(s), {} checkpoint(s), selected {}, still referenced {}, content {} -> {}",
        out.blocks_rewritten, out.checkpoints, out.selected.len(), out.still_referenced.len(),
        out.content_cno_before, out.content_cno_after);
    // Every selected object's live data, which includes every survivor of
    // checkpoint 1 that is in a candidate. Under sending ahead a later
    // checkpoint's object can be a candidate too: a block written again while its
    // object was open leaves a dead slot behind.
    let selected_live: usize = before.objects.iter()
        .filter(|o| out.selected.contains(&o.segid)).map(|o| o.live_data_blocks).sum();
    assert_eq!(out.blocks_rewritten, selected_live, "every live block of a selected object");
    assert!(out.still_referenced.is_empty(), "selected but not freed: {:?}", out.still_referenced);
    assert!(!out.content_moved());

    cleaner.prune(1, 0).await?;
    let after = cleaner.report(1, 0).await?;
    let left: Vec<_> = after.objects.iter().filter(|o| o.segid.as_cno() == 1)
        .map(|o| o.segid.to_string()).collect();
    println!("after prune: {} object(s), {} B (was {} in {}); left of checkpoint 1: {:?}",
        after.objects.len(), after.bytes(), before.bytes(), before.objects.len(), left);
    assert!(left.is_empty(), "checkpoint 1 should be gone");
    for idx in [0usize, KEEP, 1, BLOCKS - 1] {
        verify(&client, &uri, idx, if idx % KEEP == 0 { 0x11 } else { 0x22 }).await?;
    }
    let _ = Hyper::fs_unlink(&client, &uri).await;
    live_leaves(&client, &format!("{}-leaves", uri)).await?;
    println!("OK");
    Ok(())
}
