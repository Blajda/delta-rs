//! Implement a cache for storage backends using the local filesystem
use std::pin::Pin;

use super::{file::FileStorageBackend, ObjectMeta};
use crate::{StorageBackend, StorageError};
use futures::Stream;

#[derive(Debug)]
/// A cache for storage backends using the local filesystem
pub struct LocalCacheStorageBackend {
    local: FileStorageBackend,
    local_uri: String,
    remote: Box<dyn StorageBackend>,
    remote_uri: String,
}

impl LocalCacheStorageBackend {
    ///Create a new StorageBackend from the remote backend with local as cache
    pub fn new(
        local: FileStorageBackend,
        local_uri: &str,
        remote: Box<dyn StorageBackend>,
        remote_uri: &str,
    ) -> Self {
        LocalCacheStorageBackend {
            local,
            local_uri: local_uri.to_string(),
            remote,
            remote_uri: remote_uri.to_string(),
        }
    }
}

impl LocalCacheStorageBackend {

    ///Cache the object at path if it doesn't exist locally
    pub async fn cache(&self, path: &str) -> Result<(), StorageError> {
        //TODO: If the file already exists then we don't need to read it into memory
        self.get_obj(path).await?;
        Ok(())
    }
}


#[async_trait::async_trait]
impl StorageBackend for LocalCacheStorageBackend {
    /// Fetch object metadata without reading the actual content
    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        return self.remote.head_obj(path).await;
    }

    /// Fetch object content
    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        let rel_path = path.to_owned().replace(&self.remote_uri, "");
        let local_path = self.local_uri.to_string() + &rel_path;

        if rel_path == "/_delta_log/_last_checkpoint" {
            return self.remote.get_obj(path).await;
        }

        match self.local.get_obj(&local_path).await {
            Ok(data) => Ok(data),
            Err(StorageError::NotFound) => {
                let remote = self.remote.get_obj(path).await?;
                self.local.put_obj(&local_path, &remote).await?;
                Ok(remote)
            }
            Err(e) => Err(e),
        }
    }

    /// Create new object with `obj_bytes` as content.
    ///
    /// Implementation note:
    ///
    /// To support safe concurrent read, if `path` already exists, `put_obj` needs to update object
    /// content in backing store atomically, i.e. reader of the object should never read a partial
    /// write.
    async fn put_obj(&self, path: &str, obj_bytes: &[u8]) -> Result<(), StorageError> {
        self.remote.put_obj(path, obj_bytes).await
    }

    /// Moves object from `src` to `dst`.
    ///
    /// Implementation note:
    ///
    /// For a multi-writer safe backend, `rename_obj_noreplace` needs to implement rename if not exists semantic.
    /// In other words, if the destination path already exists, rename should return a
    /// [StorageError::AlreadyExists] error.
    async fn rename_obj_noreplace(&self, src: &str, dst: &str) -> Result<(), StorageError> {
        self.remote.rename_obj_noreplace(src, dst).await
    }

    /// Deletes object by `path`.
    async fn delete_obj(&self, path: &str) -> Result<(), StorageError> {
        self.remote.delete_obj(path).await
    }

    /// Return a list of objects by `path` prefix in an async stream.
    async fn list_objs<'a>(
        &'a self,
        path: &'a str,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<ObjectMeta, StorageError>> + Send + 'a>>,
        StorageError,
    > {
        self.remote.list_objs(path).await
    }
}
