//! The wire contract between the Rust host and the isolate bootstrap.
//!
//! Everything crossing into JavaScript is JSON text: the adapter never hands a
//! live host handle, a database connection, or a tenant identifier that the
//! host has not already decided to disclose. Everything coming back is parsed
//! into an explicit enum, so a module cannot widen its own surface by returning
//! an unexpected shape.

use gonvex_module_runtime::{
    Capabilities, FunctionKind, HostCall, IdentityContext, ModuleError,
};
use serde::{Deserialize, Serialize};

/// The capabilities a function kind may ever reach, before the host's own
/// grant is applied. This is the structural half of Query/Reducer/Action
/// separation: a query has no path to a write no matter what the host granted,
/// and an action has no direct database access at all — it goes through a
/// reducer, which is where the host owns the transaction.
fn structural_capabilities(kind: &FunctionKind) -> Capabilities {
    match kind {
        FunctionKind::Query => Capabilities {
            db_read: true,
            ..Capabilities::default()
        },
        FunctionKind::Reducer => Capabilities {
            db_read: true,
            db_write: true,
            ..Capabilities::default()
        },
        // Actions and HTTP handlers run outside the transaction: they may reach
        // the network and storage, and mutate only by calling a reducer.
        FunctionKind::Action | FunctionKind::Http => Capabilities {
            run_reducer: true,
            network: true,
            storage: true,
            ..Capabilities::default()
        },
    }
}

/// Intersects the kind's structural reach with the grant on the invocation.
pub(crate) fn effective_capabilities(kind: &FunctionKind, granted: &Capabilities) -> Capabilities {
    let structural = structural_capabilities(kind);
    Capabilities {
        db_read: structural.db_read && granted.db_read,
        db_write: structural.db_write && granted.db_write,
        run_reducer: structural.run_reducer && granted.run_reducer,
        network: structural.network && granted.network,
        storage: structural.storage && granted.storage,
    }
}

pub(crate) fn kind_name(kind: &FunctionKind) -> &'static str {
    match kind {
        FunctionKind::Query => "query",
        FunctionKind::Reducer => "reducer",
        FunctionKind::Action => "action",
        FunctionKind::Http => "http",
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CapabilityFlags {
    db_read: bool,
    db_write: bool,
    run_reducer: bool,
    network: bool,
    storage: bool,
}

impl From<&Capabilities> for CapabilityFlags {
    fn from(capabilities: &Capabilities) -> Self {
        Self {
            db_read: capabilities.db_read,
            db_write: capabilities.db_write,
            run_reducer: capabilities.run_reducer,
            network: capabilities.network,
            storage: capabilities.storage,
        }
    }
}

/// Identity is the only part of the invocation context the module sees. The
/// project and tenant identifiers stay in the host, which scopes every host
/// call itself, so module code cannot be the place tenancy is enforced.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct IdentityView<'a> {
    account_id: Option<&'a str>,
    member_id: Option<&'a str>,
    email: Option<&'a str>,
    permissions: &'a serde_json::Value,
}

impl<'a> From<&'a IdentityContext> for IdentityView<'a> {
    fn from(identity: &'a IdentityContext) -> Self {
        Self {
            account_id: identity.account_id.as_deref(),
            member_id: identity.member_id.as_deref(),
            email: identity.email.as_deref(),
            permissions: &identity.permissions,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DispatchRequest<'a> {
    pub(crate) function: &'a str,
    pub(crate) kind: &'static str,
    pub(crate) capabilities: CapabilityFlags,
    pub(crate) identity: IdentityView<'a>,
    pub(crate) max_result_bytes: usize,
}

/// The host operations a module may ask for, named one by one so an unknown
/// `kind` is a parse error rather than a call the host has to interpret. Each
/// variant lowers into exactly one `HostCall`.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub(crate) enum HostCallRequest {
    DbQuery {
        statement: String,
        #[serde(default)]
        parameters: serde_json::Value,
    },
    DbWrite {
        statement: String,
        #[serde(default)]
        parameters: serde_json::Value,
    },
    RunReducer {
        function: String,
        #[serde(default)]
        args: serde_json::Value,
    },
    Fetch {
        #[serde(default)]
        request: serde_json::Value,
    },
    Storage {
        operation: String,
        #[serde(default)]
        payload: serde_json::Value,
    },
}

impl HostCallRequest {
    pub(crate) fn into_host_call(self) -> Result<HostCall, String> {
        let encode = |value: serde_json::Value| {
            serde_json::to_vec(&value).map_err(|err| format!("host call payload is not encodable: {err}"))
        };
        Ok(match self {
            Self::DbQuery {
                statement,
                parameters,
            } => HostCall::DbQuery {
                statement,
                parameters: encode(parameters)?,
            },
            Self::DbWrite {
                statement,
                parameters,
            } => HostCall::DbWrite {
                statement,
                parameters: encode(parameters)?,
            },
            Self::RunReducer { function, args } => HostCall::RunReducer {
                function,
                args: encode(args)?,
            },
            Self::Fetch { request } => HostCall::Fetch {
                request: encode(request)?,
            },
            Self::Storage { operation, payload } => HostCall::Storage {
                operation,
                payload: encode(payload)?,
            },
        })
    }
}

/// What the op hands back to the bootstrap. Denial and failure are separate so
/// module code can tell "you were never allowed to do this" from "the host
/// tried and it broke".
#[derive(Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub(crate) enum HostCallOutcome {
    Ok { value: String },
    Denied { message: String },
    Failed { message: String },
}

impl HostCallOutcome {
    /// Encoding here rather than through serde_v8 keeps the op's return type a
    /// plain string, so the module boundary stays one shape in both directions.
    pub(crate) fn encode(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            r#"{"status":"failed","message":"host call outcome could not be encoded"}"#.to_owned()
        })
    }

    pub(crate) fn ok(value: String) -> Self {
        Self::Ok { value }
    }

    pub(crate) fn denied(message: String) -> Self {
        Self::Denied { message }
    }

    pub(crate) fn failed(message: String) -> Self {
        Self::Failed { message }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum DispatchErrorKind {
    /// The manifest and the bundle disagree: the export is not callable.
    Dispatch,
    /// The module's own handler threw.
    Handler,
    /// The handler returned something JSON cannot represent.
    Result,
    /// The handler returned more than the configured result budget.
    ResultSize,
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
enum DispatchOutcome {
    Ok {
        value: String,
    },
    Error {
        kind: DispatchErrorKind,
        message: String,
        #[serde(default)]
        stack: Option<String>,
    },
}

/// Turns the dispatcher's envelope into the host's error vocabulary. The
/// envelope is JSON text rather than a thrown value so a handler failure and an
/// engine failure never look alike.
pub(crate) fn decode_result(envelope: &str) -> Result<Vec<u8>, ModuleError> {
    let outcome: DispatchOutcome = serde_json::from_str(envelope).map_err(|err| {
        ModuleError::Execution(format!("module dispatcher returned an unreadable envelope: {err}"))
    })?;
    match outcome {
        DispatchOutcome::Ok { value } => Ok(value.into_bytes()),
        DispatchOutcome::Error {
            kind,
            message,
            stack,
        } => Err(match kind {
            DispatchErrorKind::Dispatch => ModuleError::InvalidArtifact(message),
            DispatchErrorKind::Handler => ModuleError::Execution(stack.unwrap_or(message)),
            DispatchErrorKind::Result => ModuleError::Execution(message),
            DispatchErrorKind::ResultSize => ModuleError::BudgetExceeded(message),
        }),
    }
}
