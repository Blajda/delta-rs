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

    /// Obtain the absolute remote path 
    pub fn remote_path(&self, path: &str) -> String {
        let rel_path = path.to_owned().replace(&self.local_uri, "");
        let remote_path = self.remote_uri.to_string() + &rel_path;
        return remote_path;
    }

    /// Obtain the local, remote and realative paths
    pub fn get_paths(&self, path: &str) -> (String, String, String) {
        let rel_path = path.to_owned().replace(&self.local_uri, "");
        let remote_path = self.remote_uri.to_string() + &rel_path;
        let local_path = path;

        return (local_path.to_string(), remote_path, rel_path)
    }
}


#[async_trait::async_trait]
impl StorageBackend for LocalCacheStorageBackend {
    /// Fetch object metadata without reading the actual content
    async fn head_obj(&self, path: &str) -> Result<ObjectMeta, StorageError> {
        let (_local_path, remote_path, _rel_path) = self.get_paths(path);
        return self.remote.head_obj(&remote_path).await;
    }

    /// Fetch object content
    async fn get_obj(&self, path: &str) -> Result<Vec<u8>, StorageError> {
        let (local_path, remote_path, rel_path) = self.get_paths(path);

        if rel_path == "/_delta_log/_last_checkpoint" {
            return self.remote.get_obj(&remote_path).await;
        }

        match self.local.get_obj(&local_path).await {
            Ok(data) => Ok(data),
            Err(StorageError::NotFound) => {
                let data = self.remote.get_obj(&remote_path).await?;
                self.local.put_obj(&local_path, &data).await?;
                Ok(data)
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
        let (_local_path, remote_path, _rel_path) = self.get_paths(path);
        self.remote.put_obj(&remote_path, obj_bytes).await
    }

    /// Moves object from `src` to `dst`.
    ///
    /// Implementation note:
    ///
    /// For a multi-writer safe backend, `rename_obj_noreplace` needs to implement rename if not exists semantic.
    /// In other words, if the destination path already exists, rename should return a
    /// [StorageError::AlreadyExists] error.
    async fn rename_obj_noreplace(&self, src: &str, dst: &str) -> Result<(), StorageError> {
        self.remote.rename_obj_noreplace(&self.remote_path(src), &self.remote_path(dst)).await
    }

    /// Deletes object by `path`.
    async fn delete_obj(&self, path: &str) -> Result<(), StorageError> {
        self.remote.delete_obj(&self.remote_path(path)).await
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
