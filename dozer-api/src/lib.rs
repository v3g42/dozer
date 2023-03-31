use std::{ops::Deref, path::PathBuf, sync::Arc};

use arc_swap::ArcSwap;
use dozer_cache::{cache::RwCacheManager, CacheReader};
use dozer_types::{log::info, models::api_endpoint::ApiEndpoint, types::Schema};
mod api_helper;

#[derive(Debug)]
pub struct RoCacheEndpoint {
    cache_reader: ArcSwap<CacheReader>,
    endpoint: ApiEndpoint,
    schema: Schema,
    pipeline_path: PathBuf,
}

impl RoCacheEndpoint {
    pub fn new(
        cache_manager: &dyn RwCacheManager,
        schema: Schema,
        endpoint: ApiEndpoint,
        pipeline_path: PathBuf,
    ) -> Result<Self, ApiError> {
        let cache_reader = open_cache_reader(cache_manager, &endpoint.name, schema, pipeline_path)?;
        Ok(Self {
            cache_reader: ArcSwap::from_pointee(cache_reader),
            schema,
            endpoint,
            pipeline_path,
        })
    }

    pub fn cache_reader(&self) -> impl Deref<Target = Arc<CacheReader>> + '_ {
        self.cache_reader.load()
    }

    pub fn endpoint(&self) -> &ApiEndpoint {
        &self.endpoint
    }

    pub fn redirect_cache(&self, cache_manager: &dyn RwCacheManager) -> Result<(), ApiError> {
        let cache_reader = open_cache_reader(
            cache_manager,
            &self.endpoint.name,
            self.schema,
            self.pipeline_path,
        )?;
        self.cache_reader.store(Arc::new(cache_reader));
        Ok(())
    }
}

fn open_cache_reader(
    cache_manager: &dyn RwCacheManager,
    name: &str,
    schema: Schema,
    pipeline_path: PathBuf,
) -> Result<CacheReader, ApiError> {
    let cache =
        build_cache(cache_manager, name, schema, pipeline_path).map_err(ApiError::OpenCache)?;
    info!("[api] Serving {} using cache {}", name, cache.name());
    Ok(CacheReader::new(cache))
}

// Exports
pub mod auth;
mod cache_builder;
pub mod errors;
pub mod generator;
pub mod grpc;
pub mod rest;
// Re-exports
pub use actix_web;
pub use async_trait;
pub use cache_builder::load_schemas;
use errors::ApiError;
pub use openapiv3;
pub use tokio;
pub use tonic;

use crate::cache_builder::build_cache;

#[cfg(test)]
mod test_utils;
