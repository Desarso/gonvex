//! Wasm Component Model module-engine seam.
//!
//! Wasmtime is deliberately feature-gated and not pulled into the initial
//! workspace. This keeps the ABI and generation registry cheap to compile
//! while leaving a clear adapter boundary for Rust/C++/C# components.

use gonvex_module_runtime::{
    BoxFuture, Invocation, InvocationResult, ModuleArtifact, ModuleEngine, ModuleError,
    ModuleHost, ModuleLanguage, ModuleManifest,
};

#[derive(Clone, Debug)]
pub struct WasmConfig {
    pub max_fuel: u64,
    pub max_memory_bytes: usize,
    pub max_result_bytes: usize,
    pub max_db_calls: usize,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            max_fuel: 10_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
            max_result_bytes: 8 * 1024 * 1024,
            max_db_calls: 100,
        }
    }
}

#[derive(Clone)]
pub struct WasmModuleEngine {
    manifest: ModuleManifest,
    config: WasmConfig,
    _component: Vec<u8>,
}

impl WasmModuleEngine {
    pub fn from_artifact(artifact: ModuleArtifact, config: WasmConfig) -> Result<Self, ModuleError> {
        if !matches!(artifact.manifest.language, ModuleLanguage::Rust | ModuleLanguage::Wasm) {
            return Err(ModuleError::InvalidArtifact(
                "Wasmtime adapter requires a Rust/Wasm component artifact".to_owned(),
            ));
        }
        if artifact.payload.is_empty() {
            return Err(ModuleError::InvalidArtifact("empty Wasm component".to_owned()));
        }
        Ok(Self {
            manifest: artifact.manifest,
            config,
            _component: artifact.payload,
        })
    }

    pub fn config(&self) -> &WasmConfig {
        &self.config
    }
}

impl ModuleEngine for WasmModuleEngine {
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
                "Wasmtime execution adapter is not linked yet".to_owned(),
            ))
        })
    }
}
