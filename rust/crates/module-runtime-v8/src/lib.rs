//! V8/TypeScript module-engine seam.
//!
//! This crate intentionally does not depend on a V8 embedding crate yet. It
//! fixes the host-facing lifecycle and resource-budget contract first, so the
//! eventual deno_core or rusty_v8 integration cannot leak into server code.

use std::time::Duration;

use gonvex_module_runtime::{
    BoxFuture, Invocation, InvocationResult, ModuleArtifact, ModuleEngine, ModuleError,
    ModuleHost, ModuleLanguage, ModuleManifest,
};

#[derive(Clone, Debug)]
pub struct V8Config {
    pub max_heap_bytes: usize,
    pub execution_timeout: Duration,
    pub max_result_bytes: usize,
    pub max_db_calls: usize,
    pub recycle_after_calls: usize,
    pub isolate_pool_size: usize,
}

impl Default for V8Config {
    fn default() -> Self {
        Self {
            max_heap_bytes: 64 * 1024 * 1024,
            execution_timeout: Duration::from_secs(10),
            max_result_bytes: 8 * 1024 * 1024,
            max_db_calls: 100,
            recycle_after_calls: 10_000,
            isolate_pool_size: 1,
        }
    }
}

#[derive(Clone)]
pub struct V8ModuleEngine {
    manifest: ModuleManifest,
    config: V8Config,
    _javascript: Vec<u8>,
}

impl V8ModuleEngine {
    pub fn from_artifact(artifact: ModuleArtifact, config: V8Config) -> Result<Self, ModuleError> {
        if !matches!(artifact.manifest.language, ModuleLanguage::TypeScript) {
            return Err(ModuleError::InvalidArtifact(
                "V8 adapter requires a TypeScript artifact".to_owned(),
            ));
        }
        if artifact.payload.is_empty() {
            return Err(ModuleError::InvalidArtifact("empty JavaScript artifact".to_owned()));
        }
        Ok(Self {
            manifest: artifact.manifest,
            config,
            _javascript: artifact.payload,
        })
    }

    pub fn config(&self) -> &V8Config {
        &self.config
    }
}

impl ModuleEngine for V8ModuleEngine {
    fn manifest(&self) -> &ModuleManifest {
        &self.manifest
    }

    fn invoke<'a>(
        &'a self,
        _host: &'a dyn ModuleHost,
        _invocation: Invocation,
    ) -> BoxFuture<'a, Result<InvocationResult, ModuleError>> {
        Box::pin(async {
            Err(ModuleError::Unsupported(
                "V8 execution adapter is not linked yet; Go remains the active dev engine".to_owned(),
            ))
        })
    }
}
