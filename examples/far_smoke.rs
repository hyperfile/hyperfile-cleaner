//! A file with blocks far past its other data, as a consumer lays out its own
//! state at `1 << 56`. Compaction has to finish — asking about every index from 0
//! to EOF would be 2^44 of them — and has to move those blocks like any other.
#![recursion_limit = "1024"]
use std::io::Result;
use std::time::{Duration, Instant};
use aws_sdk_s3::Client;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::mode::FileMode;
use hyperfile_cleaner::compact::CompactOptions;
use hyperfile_cleaner::s3::S3Cleaner;

const BS: usize = 4096;
const FAR: usize = 1 << 56;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let uri = std::env::var("HYPERFILE_STAGING_ROOT_URI").expect("uri");
    let cfg = aws_config::load_from_env().await;
    let client = Client::new(&cfg);
    let _ = Hyper::fs_unlink(&client, &uri).await;
    let mut file = Hyper::fs_create(&client, &uri, FileFlags::rdwr(), FileMode::default_file()).await?;
    // Low data, one flush per block, and the far blocks, each in its own checkpoint.
    for idx in 0..8usize {
        let _ = file.fs_write(idx * BS, &vec![idx as u8; BS]).await?;
        let _ = file.fs_flush().await?;
    }
    for k in 0..3usize {
        let _ = file.fs_write(FAR + k * BS, &vec![0xF0 + k as u8; BS]).await?;
        let _ = file.fs_flush().await?;
    }
    let _ = file.fs_release().await?;
    println!("built: size {}", Hyper::fs_getattr_fast(&client, &uri).await?.st_size);

    let cleaner = S3Cleaner::with_client(&client, &uri).await?;
    let t = Instant::now();
    let r = tokio::time::timeout(Duration::from_secs(120),
        cleaner.compact(1, 0, &CompactOptions::default())).await;
    match r {
        Err(_) => panic!("compact did not finish in 120 s"),
        Ok(Err(e)) => return Err(e),
        Ok(Ok(out)) => {
            println!("compact: {} block(s) in {:?}, content {} -> {}",
                out.blocks_rewritten, t.elapsed(), out.content_cno_before, out.content_cno_after);
            // Seven low blocks and two far ones; the newest checkpoint is not a
            // candidate, so the last far block stays where it is.
            assert_eq!(out.blocks_rewritten, 10);
            assert!(!out.content_moved());
        },
    }
    let mut file = Hyper::fs_open(&client, &uri, FileFlags::rdonly()).await?;
    for k in 0..3usize {
        let mut buf = vec![0u8; BS];
        let _ = file.fs_read(FAR + k * BS, &mut buf).await?;
        assert!(buf.iter().all(|b| *b == 0xF0 + k as u8), "far block {} wrong", k);
    }
    let places = file.fs_block_placement((FAR / BS) as u64, 3).await?;
    let objs: Vec<String> = places.iter().map(|p| p.map(|(s, _)| s.to_string()).unwrap_or_default()).collect();
    println!("far blocks now in: {:?}", objs);
    assert_eq!(objs[0], objs[1], "the two far blocks that were candidates moved together");
    assert_ne!(objs[0], objs[2], "the newest checkpoint's block was not a candidate");
    let _ = file.fs_release().await;
    let _ = Hyper::fs_unlink(&client, &uri).await;
    println!("OK");
    Ok(())
}
