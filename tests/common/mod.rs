//! Shared by the S3 suites: a client for the test bucket and a container prefix
//! of its own, removed afterwards.
#![allow(dead_code)]

use aws_sdk_s3::Client;

pub fn test_bucket() -> String {
    std::env::var("HYPERFILE_TEST_BUCKET").expect(
        "HYPERFILE_TEST_BUCKET is not set: the S3 suites need a bucket you own, and \
         HYPERFILE_TEST_REGION for its region")
}

pub async fn make_client() -> Client {
    let region = std::env::var("HYPERFILE_TEST_REGION").expect("HYPERFILE_TEST_REGION is not set");
    let config = aws_config::from_env().region(aws_config::Region::new(region)).load().await;
    Client::new(&config)
}

/// A container prefix nothing else uses.
pub struct TestFile {
    pub bucket: String,
    pub prefix: String,
}

impl TestFile {
    pub fn new() -> Self {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        Self { bucket: test_bucket(), prefix: format!("hyperfile-cleaner-test/{}-{}", nanos, std::process::id()) }
    }

    pub fn uri(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }

    /// Every object name under the container, without the prefix.
    pub async fn objects(&self, client: &Client) -> Vec<String> {
        let mut names = Vec::new();
        let mut pages = client.list_objects_v2().bucket(&self.bucket).prefix(format!("{}/", self.prefix))
            .into_paginator().send();
        while let Some(page) = pages.next().await {
            for o in page.expect("list").contents() {
                if let Some(k) = o.key() {
                    names.push(k.rsplit('/').next().unwrap().to_string());
                }
            }
        }
        names.sort();
        names
    }

    pub async fn cleanup(&self, client: &Client) {
        for name in self.objects(client).await {
            let _ = client.delete_object().bucket(&self.bucket).key(format!("{}/{}", self.prefix, name)).send().await;
        }
    }
}
