mod localcache {
    use std::fs::{self, File};
    use std::io::prelude::Write;

    use deltalake::{
        storage::{file::FileStorageBackend, localcache::LocalCacheStorageBackend},
        DeltaTableBuilder,
    };

    #[tokio::test]
    async fn test_localcache_with_fs() {
        let test_table = tempdir::TempDir::new("test_table").unwrap();

        let local_uri = test_table.path().to_str().unwrap();
        let remote_uri = "./tests/data/simple_table";

        let local = FileStorageBackend::new(local_uri);
        let remote = FileStorageBackend::new(remote_uri);

        let cache_storage =
            LocalCacheStorageBackend::new(local, local_uri, Box::new(remote), remote_uri);

        let table = DeltaTableBuilder::from_uri(local_uri)
            .unwrap()
            .with_storage_backend(Box::new(cache_storage))
            .load()
            .await
            .unwrap();

        assert_eq!(
            test_table
                .path()
                .join("_delta_log/00000000000000000000.json")
                .as_path()
                .exists(),
            true
        );
        assert_eq!(
            test_table
                .path()
                .join("_delta_log/00000000000000000004.json")
                .as_path()
                .exists(),
            true
        );
    }

    #[tokio::test]
    ///The local version of _last_checkpoint should not be used since it might be stale
    async fn test_localcache_last_checkpoint() {
        let test_table = tempdir::TempDir::new("test_table").unwrap();

        let local_uri = test_table.path().to_str().unwrap();
        let remote_uri = "./tests/data/checkpoints";

        let local = FileStorageBackend::new(local_uri);
        let remote = FileStorageBackend::new(remote_uri);

        let cache_storage =
            LocalCacheStorageBackend::new(local, local_uri, Box::new(remote), remote_uri);

        fs::create_dir(test_table.path().join("_delta_log").as_path()).unwrap();
        let mut stale_checkpoint = File::create(
            test_table
                .path()
                .join("_delta_log/_last_checkpoint")
                .as_path(),
        )
        .unwrap();

        stale_checkpoint
            .write(b"{\"version\":5,\"size\":13}")
            .unwrap();

        let table = DeltaTableBuilder::from_uri(local_uri)
            .unwrap()
            .with_storage_backend(Box::new(cache_storage))
            .load()
            .await
            .unwrap();

        assert_eq!(
            test_table
                .path()
                .join("_delta_log/00000000000000000000.json")
                .as_path()
                .exists(),
            false
        );
        assert_eq!(
            test_table
                .path()
                .join("_delta_log/00000000000000000005.checkpoint.parquet")
                .as_path()
                .exists(),
            false
        );
        assert_eq!(
            test_table
                .path()
                .join("_delta_log/00000000000000000010.checkpoint.parquet")
                .as_path()
                .exists(),
            true
        );
        assert_eq!(
            test_table
                .path()
                .join("_delta_log/00000000000000000012.json")
                .as_path()
                .exists(),
            true
        );
    }
}
