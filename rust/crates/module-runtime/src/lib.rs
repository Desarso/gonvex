//! Language-neutral Gonvex module ABI.
//!
//! The ABI deliberately transports JSON-shaped metadata and byte payloads.
//! The host never depends on TypeScript, Go, or a particular database driver.
//! V8 and Wasm adapters can therefore implement this crate without changing
//! the HTTP, Postgres, change-feed, or generation lifecycle code.

use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type ModuleGeneration = u64;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ModuleLanguage {
    TypeScript,
    Rust,
    Wasm,
    Other(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FunctionKind {
    Query,
    Reducer,
    Action,
    Http,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FunctionContract {
    pub path: String,
    pub kind: FunctionKind,
    #[serde(default)]
    pub internal: bool,
    #[serde(default)]
    pub delivery: Option<String>,
    #[serde(default)]
    pub args_schema: Option<serde_json::Value>,
    #[serde(default)]
    pub result_schema: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModuleManifest {
    pub module_id: String,
    pub generation: ModuleGeneration,
    pub language: ModuleLanguage,
    pub artifact_hash: String,
    #[serde(default)]
    pub functions: Vec<FunctionContract>,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModuleArtifact {
    pub manifest: ModuleManifest,
    /// Bundled JavaScript, Wasm component bytes, or another engine payload.
    /// The host treats this as opaque and verifies `artifact_hash` before load.
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct IdentityContext {
    pub account_id: Option<String>,
    pub member_id: Option<String>,
    pub email: Option<String>,
    #[serde(default)]
    pub permissions: serde_json::Value,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvocationContext {
    pub project_id: String,
    pub tenant_id: String,
    pub operation_id: Option<String>,
    pub identity: IdentityContext,
    pub generation: ModuleGeneration,
    #[serde(default)]
    pub capabilities: Capabilities,
    #[serde(skip)]
    pub deadline: Option<SystemTime>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Capabilities {
    pub db_read: bool,
    pub db_write: bool,
    pub run_reducer: bool,
    pub network: bool,
    pub storage: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Invocation {
    pub function: String,
    pub kind: FunctionKind,
    pub args: Vec<u8>,
    pub context: InvocationContext,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvocationResult {
    pub value: Vec<u8>,
    #[serde(default)]
    pub committed_revision: Option<u64>,
    #[serde(default)]
    pub origin_command_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum HostCall {
    DbQuery { statement: String, parameters: Vec<u8> },
    DbWrite { statement: String, parameters: Vec<u8> },
    RunReducer { function: String, args: Vec<u8> },
    Fetch { request: Vec<u8> },
    Storage { operation: String, payload: Vec<u8> },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HostResponse {
    pub value: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum HostError {
    #[error("capability {0} is not available to this invocation")]
    CapabilityDenied(&'static str),
    #[error("host call failed: {0}")]
    Failed(String),
}

impl HostCall {
    pub fn check_capability(&self, capabilities: &Capabilities) -> Result<(), HostError> {
        let (allowed, name) = match self {
            Self::DbQuery { .. } => (capabilities.db_read, "db_read"),
            Self::DbWrite { .. } => (capabilities.db_write, "db_write"),
            Self::RunReducer { .. } => (capabilities.run_reducer, "run_reducer"),
            Self::Fetch { .. } => (capabilities.network, "network"),
            Self::Storage { .. } => (capabilities.storage, "storage"),
        };
        if allowed {
            Ok(())
        } else {
            Err(HostError::CapabilityDenied(name))
        }
    }
}

/// Host operations are intentionally opaque to the module engine. The host
/// implementation owns the Postgres transaction, credentials, authorization,
/// and external service clients.
pub trait ModuleHost: Send + Sync {
    fn call<'a>(
        &'a self,
        context: &'a InvocationContext,
        call: HostCall,
    ) -> BoxFuture<'a, Result<HostResponse, HostError>>;
}

#[derive(Debug, Error)]
pub enum ModuleError {
    #[error("module function {0} is not registered")]
    FunctionNotFound(String),
    #[error("module function {0} has the wrong kind")]
    WrongFunctionKind(String),
    #[error("module artifact is invalid: {0}")]
    InvalidArtifact(String),
    #[error("module execution exceeded its budget: {0}")]
    BudgetExceeded(String),
    #[error("module engine is not available: {0}")]
    Unsupported(String),
    #[error("module execution failed: {0}")]
    Execution(String),
}

/// The only runtime contract the Rust host needs from an application module.
/// Implementations may be V8, Wasmtime, or another engine in the future.
pub trait ModuleEngine: Send + Sync {
    fn manifest(&self) -> &ModuleManifest;

    fn invoke<'a>(
        &'a self,
        host: &'a dyn ModuleHost,
        invocation: Invocation,
    ) -> BoxFuture<'a, Result<InvocationResult, ModuleError>>;
}

pub fn remaining_budget(context: &InvocationContext) -> Option<Duration> {
    context.deadline.and_then(|deadline| deadline.duration_since(SystemTime::now()).ok())
}
