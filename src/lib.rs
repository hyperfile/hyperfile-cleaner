use std::io::Result;
use hyperfile::SegmentId;

pub mod s3;

pub trait Cleaner {
    fn collect(&self, low: SegmentId, high: SegmentId) -> impl Future<Output = Result<(Vec<SegmentId>, Vec<SegmentId>)>> + Send;
    fn prune(&self, low: SegmentId, high: SegmentId) -> impl Future<Output = Result<()>> + Send;
}
