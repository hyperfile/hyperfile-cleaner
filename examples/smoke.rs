//! Integration smoke: build a 3-flush hyperfile, then call
//! `collect` to verify the cleaner reports a non-empty referenced
//! list. Used to catch regressions during development.
//!
//! Run with:
//!   AWS_REGION=<region> \
//!     HYPERFILE_STAGING_ROOT_URI=s3://my-bucket/test-prefix \
//!     cargo run --release --example smoke
use std::io::Result;
use aws_sdk_s3::Client;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::mode::FileMode;
use hyperfile_cleaner::{Cleaner, s3::S3Cleaner};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    env_logger::init();
    let uri = std::env::var("HYPERFILE_STAGING_ROOT_URI")
        .expect("HYPERFILE_STAGING_ROOT_URI must be set");
    let cfg = aws_config::load_from_env().await;
    let client = Client::new(&cfg);

    let _ = Hyper::fs_unlink(&client, &uri).await;

    let mut hyper = Hyper::fs_create(
        &client, &uri, FileFlags::rdwr(), FileMode::default_file()
    ).await?;
    // ≥ 512 KiB each flush so S3Staging::open's header read fits.
    for round in 0..3 {
        let payload = vec![0xA0u8 + round as u8; 1024 * 1024];
        let off = round as usize * 1024 * 1024;
        let _ = hyper.fs_write(off, &payload).await?;
        let _ = hyper.fs_flush().await?;
        println!("flush {} done (1 MiB)", round + 1);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let _ = hyper.fs_release().await?;

    // Test new() (env-based) and with_client()
    let cleaner = S3Cleaner::with_client(&client, &uri).await?;
    let (refed, unrefed) = cleaner.collect(1, 0).await?;
    println!("referenced  : {:?}", refed);
    println!("unreferenced: {:?}", unrefed);
    assert!(!refed.is_empty(), "expected at least one referenced segment");
    println!("OK");
    let _ = Hyper::fs_unlink(&client, &uri).await;
    Ok(())
}
