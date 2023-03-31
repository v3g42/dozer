use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use dozer_cache::{
    cache::{RoCache, RwCacheManager},
    errors::CacheError,
};
use dozer_types::{serde_json, types::Schema};
use futures_util::StreamExt;

mod log_reader;
pub const SCHEMA_FILE_NAME: &str = "schemas.json";

pub fn build_cache(
    cache_manager: &dyn RwCacheManager,
    endpoint_name: &str,
    schema: Schema,
    pipeline_path: PathBuf,
) -> Result<Box<dyn RoCache>, CacheError> {
    let mut log_reader = log_reader::LogReader::new(pipeline_path, endpoint_name)?;

    let secondary_indexes = todo!("Generate default index definitions");
    let rw_cache = cache_manager.create_cache(
        schema,
        secondary_indexes,
        todo!("conflict resolution from config"),
    )?;
    let ro_cache = cache_manager
        .open_ro_cache(rw_cache.name())?
        .expect("Cache just created");
    tokio::spawn(async move {
        while let Some(executor_operation) = log_reader.next().await {
            todo!("Insert operation into cache");
        }
    });
    Ok(ro_cache)
}

pub fn load_schemas(path: &str) -> Result<HashMap<String, Schema>, CacheError> {
    let path = Path::new(&path).join(SCHEMA_FILE_NAME);

    let schema_str = std::fs::read_to_string(path.clone())
        .map_err(|e| CacheError::SchemasNotInitializedPath(path.clone()))?;

    serde_json::from_str::<HashMap<String, Schema>>(&schema_str)
        .map_err(|e| CacheError::DeserializeSchemas(path))
}
