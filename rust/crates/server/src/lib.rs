//! Rust host-side module generation lifecycle.
//!
//! This crate is intentionally independent of HTTP, Postgres, and an engine
//! implementation. It can be embedded beside the existing Go runtime first;
//! the Go loader remains the default until a future selector installs a V8 or
//! Wasm engine here.

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use gonvex_module_runtime::{
    BoxFuture, Invocation, InvocationResult, ModuleEngine, ModuleError, ModuleHost,
};
use thiserror::Error;

struct GenerationEntry {
    engine: Arc<dyn ModuleEngine>,
    in_flight: Mutex<usize>,
    drained: Condvar,
    retiring: Mutex<bool>,
}

impl GenerationEntry {
    fn new(engine: Arc<dyn ModuleEngine>) -> Self {
        Self {
            engine,
            in_flight: Mutex::new(0),
            drained: Condvar::new(),
            retiring: Mutex::new(false),
        }
    }

    fn acquire(self: &Arc<Self>) -> Option<GenerationLease> {
        let mut retiring = self.retiring.lock().expect("generation retirement lock");
        if *retiring {
            return None;
        }
        let mut in_flight = self.in_flight.lock().expect("generation call lock");
        *in_flight += 1;
        drop(in_flight);
        drop(retiring);
        Some(GenerationLease {
            entry: Arc::clone(self),
        })
    }

    fn retire(&self) {
        *self.retiring.lock().expect("generation retirement lock") = true;
        if *self.in_flight.lock().expect("generation call lock") == 0 {
            self.drained.notify_all();
        }
    }

    fn wait_for_drain(&self, timeout: Option<Duration>) -> bool {
        let mut in_flight = self.in_flight.lock().expect("generation call lock");
        if let Some(timeout) = timeout {
            let deadline = Instant::now() + timeout;
            while *in_flight != 0 {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return false;
                }
                let (next, result) = self
                    .drained
                    .wait_timeout(in_flight, remaining)
                    .expect("generation drain wait");
                in_flight = next;
                if result.timed_out() && *in_flight != 0 {
                    return false;
                }
            }
            true
        } else {
            while *in_flight != 0 {
                in_flight = self.drained.wait(in_flight).expect("generation drain wait");
            }
            true
        }
    }
}

pub struct GenerationLease {
    entry: Arc<GenerationEntry>,
}

impl GenerationLease {
    pub fn engine(&self) -> &Arc<dyn ModuleEngine> {
        &self.entry.engine
    }

    pub fn generation(&self) -> u64 {
        self.entry.engine.manifest().generation
    }
}

impl Drop for GenerationLease {
    fn drop(&mut self) {
        let mut in_flight = self.entry.in_flight.lock().expect("generation call lock");
        *in_flight = in_flight.saturating_sub(1);
        if *in_flight == 0 {
            self.entry.drained.notify_all();
        }
    }
}

pub struct RetiredGeneration {
    generation: u64,
    entry: Arc<GenerationEntry>,
}

impl RetiredGeneration {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn wait_for_drain(&self, timeout: Option<Duration>) -> bool {
        self.entry.wait_for_drain(timeout)
    }
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("module generation {0} is not newer than the active generation")]
    NonMonotonicGeneration(u64),
    #[error("no active module generation")]
    Empty,
    #[error("module invocation failed: {0}")]
    Invocation(#[from] ModuleError),
}

pub struct GenerationRegistry {
    active: RwLock<Option<Arc<GenerationEntry>>>,
    generations: Mutex<BTreeMap<u64, Arc<GenerationEntry>>>,
}

impl Default for GenerationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl GenerationRegistry {
    pub fn new() -> Self {
        Self {
            active: RwLock::new(None),
            generations: Mutex::new(BTreeMap::new()),
        }
    }

    /// Atomically makes `engine` the target for new calls. Existing leases keep
    /// the old engine alive until their calls complete.
    pub fn activate(
        &self,
        engine: Arc<dyn ModuleEngine>,
    ) -> Result<Option<RetiredGeneration>, RegistryError> {
        let generation = engine.manifest().generation;
        let mut active = self.active.write().expect("active generation lock");
        if let Some(current) = active.as_ref() {
            let current_generation = current.engine.manifest().generation;
            if generation <= current_generation {
                return Err(RegistryError::NonMonotonicGeneration(generation));
            }
            current.retire();
        }
        let next = Arc::new(GenerationEntry::new(engine));
        let previous = std::mem::replace(&mut *active, Some(Arc::clone(&next)));
        self.generations
            .lock()
            .expect("generation registry lock")
            .insert(generation, next);
        Ok(previous.map(|entry| RetiredGeneration {
            generation: entry.engine.manifest().generation,
            entry,
        }))
    }

    pub fn acquire(&self) -> Result<GenerationLease, RegistryError> {
        let active = self.active.read().expect("active generation lock");
        let entry = active.as_ref().ok_or(RegistryError::Empty)?;
        entry.acquire().ok_or(RegistryError::Empty)
    }

    pub fn active_generation(&self) -> Option<u64> {
        self.active
            .read()
            .expect("active generation lock")
            .as_ref()
            .map(|entry| entry.engine.manifest().generation)
    }

    /// Drops retired engine references once their in-flight calls have drained.
    /// A timeout leaves the generation retained, making a forced shutdown an
    /// explicit host policy instead of silently dropping an invocation.
    pub fn reap(&self, timeout: Option<Duration>) -> Vec<u64> {
        let mut generations = self.generations.lock().expect("generation registry lock");
        let active_generation = self.active_generation();
        let retired: Vec<(u64, Arc<GenerationEntry>)> = generations
            .iter()
            .filter_map(|(generation, entry)| {
                if Some(*generation) == active_generation {
                    None
                } else {
                    Some((*generation, Arc::clone(entry)))
                }
            })
            .collect();
        let mut removed = Vec::new();
        for (generation, entry) in retired {
            if entry.wait_for_drain(timeout) {
                generations.remove(&generation);
                removed.push(generation);
            }
        }
        removed
    }
}

pub struct ModuleHostRuntime {
    pub registry: Arc<GenerationRegistry>,
}

impl ModuleHostRuntime {
    pub fn new(registry: Arc<GenerationRegistry>) -> Self {
        Self { registry }
    }

    pub fn invoke<'a>(
        &'a self,
        host: &'a dyn ModuleHost,
        invocation: Invocation,
    ) -> BoxFuture<'a, Result<InvocationResult, RegistryError>> {
        Box::pin(async move {
            let lease = self.registry.acquire()?;
            let result = lease.engine().invoke(host, invocation).await;
            result.map_err(RegistryError::Invocation)
        })
    }
}
