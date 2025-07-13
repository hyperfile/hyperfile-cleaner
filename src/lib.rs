use std::io::Result;
use hyperfile::SegmentId;

pub mod s3;

pub trait Cleaner {
    fn prune(&self, low: SegmentId, high: SegmentId) -> impl Future<Output = Result<()>> + Send;
}
