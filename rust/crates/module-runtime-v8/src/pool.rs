//! Isolate pooling and recycling.
//!
//! `isolate_pool_size` bounds live isolates and therefore concurrent module
//! calls: a permit is taken before an isolate is leased, so load sheds by
//! queueing rather than by creating unbounded V8 heaps.

use std::sync::{Arc, Mutex};

use gonvex_module_runtime::ModuleError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::isolate::{IsolateWorker, ModuleSource, WorkerCall};
use crate::V8Config;

struct PooledIsolate {
    worker: IsolateWorker,
    calls: usize,
}

pub(crate) struct IsolatePool {
    source: Arc<ModuleSource>,
    config: V8Config,
    idle: Mutex<Vec<PooledIsolate>>,
    permits: Arc<Semaphore>,
}

impl IsolatePool {
    pub(crate) fn new(source: Arc<ModuleSource>, config: V8Config) -> Self {
        let size = config.isolate_pool_size.max(1);
        Self {
            source,
            config,
            idle: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(size)),
        }
    }

    pub(crate) async fn acquire(&self) -> Result<IsolateLease<'_>, ModuleError> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| ModuleError::Unsupported("module isolate pool is closed".to_owned()))?;
        // The guard is dropped before the await below: an isolate is started
        // outside the lock so a slow bundle load never blocks other calls.
        let pooled = self.idle.lock().expect("isolate pool lock").pop();
        let pooled = match pooled {
            Some(pooled) => pooled,
            None => self.start().await?,
        };
        Ok(IsolateLease {
            pool: self,
            pooled: Some(pooled),
            _permit: permit,
        })
    }

    async fn start(&self) -> Result<PooledIsolate, ModuleError> {
        let (worker, ready) = IsolateWorker::spawn(Arc::clone(&self.source), self.config.clone())?;
        match ready.await {
            Ok(Ok(())) => Ok(PooledIsolate { worker, calls: 0 }),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(ModuleError::Execution(
                "module isolate stopped before it finished loading".to_owned(),
            )),
        }
    }

    /// Dropping a `PooledIsolate` closes its call channel, which ends the
    /// worker loop and tears the isolate down. `recycle_after_calls` of 0 means
    /// an isolate is never reused.
    fn release(&self, pooled: PooledIsolate, healthy: bool) {
        if !healthy || pooled.calls >= self.config.recycle_after_calls {
            return;
        }
        let mut idle = self.idle.lock().expect("isolate pool lock");
        if idle.len() < self.config.isolate_pool_size.max(1) {
            idle.push(pooled);
        }
    }
}

pub(crate) struct IsolateLease<'a> {
    pool: &'a IsolatePool,
    pooled: Option<PooledIsolate>,
    _permit: OwnedSemaphorePermit,
}

impl IsolateLease<'_> {
    pub(crate) fn dispatch(&self, call: WorkerCall) -> Result<(), ModuleError> {
        self.pooled
            .as_ref()
            .expect("leased isolate is present until the lease ends")
            .worker
            .send(call)
    }

    /// Returns the isolate to the pool after it served one call.
    pub(crate) fn finish(mut self, healthy: bool) {
        if let Some(mut pooled) = self.pooled.take() {
            pooled.calls += 1;
            self.pool.release(pooled, healthy);
        }
    }

    /// Returns an isolate that ran nothing, so prewarming does not spend a call
    /// from the isolate's recycle budget.
    pub(crate) fn release_unused(mut self) {
        if let Some(pooled) = self.pooled.take() {
            self.pool.release(pooled, true);
        }
    }
}

impl Drop for IsolateLease<'_> {
    fn drop(&mut self) {
        // A lease that ends without `finish` means the call was cancelled or the
        // isolate died mid-call. Retire it: it may still be running JavaScript
        // for the abandoned invocation.
        if let Some(pooled) = self.pooled.take() {
            self.pool.release(pooled, false);
        }
    }
}
