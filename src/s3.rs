use std::io::{Result, Error, ErrorKind};
use std::collections::BTreeSet;
use aws_sdk_s3::Client;
use log::{debug, error};
use btree_ondisk::bmap::BMap;
use btree_ondisk::BlockLoader;
use hyperfile::{BlockIndex, BlockPtr, SegmentId};
use hyperfile::meta_loader::s3::S3BlockLoader;
use hyperfile::ondisk::InodeRaw;
use hyperfile::inode::{Inode, OnDiskState};
use hyperfile::segment::{Segment, SegmentSum, SegmentReadWrite};
use hyperfile::staging::{Staging, config::StagingConfig, s3::S3Staging};
use hyperfile::config::HyperFileRuntimeConfig;
use hyperfile::s3uri::S3Uri;
use hyperfile::meta_format::BlockPtrFormat;
use crate::Cleaner;

pub struct S3Cleaner {
    client: Client,
    bucket: String,
    root_path: String,
    staging: S3Staging,
}

impl S3Cleaner {
    pub async fn new(root_uri: &str) -> Result<Self> {
        let root_uri = if let Some(remove_tail_slash) = root_uri.strip_suffix('/') {
            remove_tail_slash
        } else {
            root_uri
        };
        let s3uri = S3Uri::parse(root_uri).expect("failed to parse root path for s3");
        let config = aws_config::load_from_env().await;
        let client = aws_sdk_s3::Client::new(&config);
        let runtime_config = HyperFileRuntimeConfig::default();
        let staging_config = StagingConfig::new_s3_staging(s3uri.bucket, "", s3uri.key, None);
        let staging = S3Staging::from(&client, staging_config, runtime_config).await?;
        Ok(Self {
            client: client,
            bucket: s3uri.bucket.to_owned(),
            root_path: s3uri.key.to_owned(),
            staging: staging,
        })
    }

    async fn load_segment_sum(&self, segid: SegmentId) -> Result<SegmentSum> {
        self.staging.open(segid).await
    }

    async fn load_raw_inode(&self) -> Result<(InodeRaw, Option<OnDiskState>)> {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let inode_state = self.staging.load_inode(&mut raw_inode.as_mut_u8_slice()).await?;
        Ok((raw_inode, inode_state))
    }

	fn collect_segment_id<'a, L>(bmap: BMap<'a, BlockIndex, BlockPtr, BlockPtr, L>) -> BTreeSet<u64>
    	where L: BlockLoader<BlockPtr> + Clone,
	{
    	// collect segment id in u64
    	let mut segid_set: BTreeSet<u64> = BTreeSet::new();

    	let iter = bmap.nonleafnode_iter();
    	for blkptr in iter {
    	    if BlockPtrFormat::is_on_staging(&blkptr) {
    	        let segid = BlockPtrFormat::decode_segid(&blkptr);
    	        segid_set.insert(segid);
    	    }
    	}
		segid_set
	}

    async fn do_delete_objects(client: &Client, bucket: &str, delete_keys: Vec<String>) -> Result<()> {
        use aws_sdk_s3::types::ObjectIdentifier;
        let mut err = false;
        for keys in delete_keys.chunks(1000).into_iter() {
            let obj_ids = keys.into_iter().map(|k| {
                    // FIXME: move the value, avoid ref
                    ObjectIdentifier::builder()
                        .key(k.to_string())
                        .build()
                        .unwrap()
                }).collect::<Vec<ObjectIdentifier>>();
            let delete = aws_sdk_s3::types::Delete::builder()
                    .set_objects(Some(obj_ids))
                    .quiet(true)
                    .build()
                    .unwrap();
            match client.delete_objects()
                .bucket(bucket)
                .delete(delete)
                .send()
                .await
            {
                Ok(_) => {},
                Err(sdk_err) => {
                    err = true;
                    // TODO: check delete objects result with signle delete error
                    error!("delete objects error: {}", sdk_err);
                }
            }
        }
        if err {
            return Err(Error::new(ErrorKind::Interrupted, "at least one of delete objects op failed"));
        }
        Ok(())
    }

    // collect referenced and unreferenced segment between low and high
    // return:
    //   (referenced seg list, unreferenced seg list)
    async fn collect_unref_seg(&self, low: SegmentId, high: SegmentId) -> Result<(Vec<SegmentId>, Vec<SegmentId>)> {
        let high_segid = if high == 0 {
            // load last inode from disk
            let (raw_inode, inode_state) = self.load_raw_inode().await?;
            let inode = Inode::from_raw(&raw_inode, inode_state);
            inode.get_last_seq()
        } else {
            high
        };

        debug!("collect unreferenced segment low: {}, high: {}", low, high_segid);
        if low >= high_segid {
            let err_msg = format!("invalid segment range [{}, {}]", low, high_segid);
            return Err(Error::new(ErrorKind::InvalidInput, err_msg));
        }

        // load segment sum
        let ss = self.load_segment_sum(high_segid).await?;
        let i_bmap = ss.hdr.s_inode.i_bmap;
        let meta_block_size = 1 << ss.hdr.s_meta_blk_shift;

        // build bmap
        let loader = S3BlockLoader::new(&self.client, &self.bucket, &self.root_path);
        let set = tokio::task::spawn_blocking(move || {
            let bmap = BMap::<BlockIndex, BlockPtr, BlockPtr, S3BlockLoader>::read(&i_bmap, meta_block_size, loader);
            Self::collect_segment_id(bmap)
        }).await.expect("spawn blocking task failed");

        let high = high_segid;
        // build non-referenced list
        let mut non_ref = Vec::new();
        for n in low..=high {
            if set.get(&n).is_none() {
                non_ref.push(n);
            }
        }
        let v_ref = set.into_iter().map(|segid| segid).collect::<Vec<SegmentId>>();
        Ok((v_ref, non_ref))
    }
}

impl Cleaner for S3Cleaner {
    async fn collect(&self, low: SegmentId, high: SegmentId) -> Result<(Vec<SegmentId>, Vec<SegmentId>)> {
        self.collect_unref_seg(low, high).await
    }

    // prune unreferenced segments between low and high
    async fn prune(&self, low: SegmentId, high: SegmentId) -> Result<()> {
        let (_, non_ref) = self.collect_unref_seg(low, high).await?;
        let delete_keys = non_ref.iter().map(|segid| {
                                format!("{}/{}", self.root_path, Segment::segid_to_staging_file_id(*segid))
                            }).collect::<Vec<String>>();
        debug!("unrefereneced segments to delete {:?}", delete_keys);
        Self::do_delete_objects(&self.client, &self.bucket, delete_keys).await?;
        Ok(())
    }
}
