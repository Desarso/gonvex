use std::process::ExitCode;
use std::sync::atomic::{AtomicI32, Ordering};
use std::thread;
use std::time::Duration;

use gonvex_runtime::{Worker, WorkerConfig};

static SHUTDOWN_SIGNAL: AtomicI32 = AtomicI32::new(0);

extern "C" fn record_shutdown_signal(signal: libc::c_int) {
    SHUTDOWN_SIGNAL.store(signal, Ordering::Relaxed);
}

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let Some(program) = args.next() else {
        eprintln!("usage: gonvex-runtime <go-worker> [args...]");
        return ExitCode::FAILURE;
    };

    // SAFETY: the handler only stores an integer in a lock-free atomic.
    unsafe {
        libc::signal(
            libc::SIGTERM,
            record_shutdown_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGINT,
            record_shutdown_signal as *const () as libc::sighandler_t,
        );
    }

    let config = WorkerConfig::new(program).args(args);
    let mut worker = match Worker::start(config) {
        Ok(worker) => worker,
        Err(error) => {
            eprintln!("gonvex-runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    eprintln!("gonvex-runtime: worker ready at {}", worker.address());

    loop {
        if SHUTDOWN_SIGNAL.load(Ordering::Relaxed) != 0 {
            return match worker.begin_graceful_shutdown() {
                Ok(status) if status.success() => ExitCode::SUCCESS,
                Ok(status) => {
                    eprintln!("gonvex-runtime: worker exited during shutdown with {status}");
                    ExitCode::FAILURE
                }
                Err(error) => {
                    eprintln!("gonvex-runtime: worker shutdown failed: {error}");
                    ExitCode::FAILURE
                }
            };
        }

        match worker.try_wait() {
            Ok(Some(status)) if status.success() => return ExitCode::SUCCESS,
            Ok(Some(status)) => {
                eprintln!("gonvex-runtime: worker exited with {status}");
                return ExitCode::FAILURE;
            }
            Ok(None) => thread::sleep(Duration::from_millis(50)),
            Err(error) => {
                eprintln!("gonvex-runtime: failed to inspect worker: {error}");
                return ExitCode::FAILURE;
            }
        }
    }
}
