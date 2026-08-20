//! Process lifecycle for the Go worker used while the Gonvex runtime moves to
//! Rust.
//!
//! This crate does not accept public traffic. It gives the child an inherited
//! loopback listener, waits for its existing `/healthz` endpoint, and owns its
//! shutdown deadlines.

#[cfg(not(unix))]
compile_error!("gonvex-runtime worker supervision currently requires Unix");

use std::ffi::{OsStr, OsString};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::thread;
use std::time::{Duration, Instant};

use thiserror::Error;

const WORKER_LISTENER_FD: libc::c_int = 3;
const HEALTH_PATH: &str = "/healthz";

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub env: Vec<(OsString, OsString)>,
    pub readiness_timeout: Duration,
    pub readiness_poll_interval: Duration,
    pub terminate_timeout: Duration,
    pub interrupt_timeout: Duration,
}

impl WorkerConfig {
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
            readiness_timeout: Duration::from_secs(30),
            readiness_poll_interval: Duration::from_millis(50),
            terminate_timeout: Duration::from_secs(15),
            interrupt_timeout: Duration::from_secs(5),
        }
    }

    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("failed to bind the worker listener: {0}")]
    Bind(#[source] io::Error),
    #[error("failed to read the worker listener address: {0}")]
    ListenerAddress(#[source] io::Error),
    #[error("failed to spawn worker {program}: {source}")]
    Spawn {
        program: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("worker exited before it became ready: {0}")]
    ExitedBeforeReady(ExitStatus),
    #[error("worker did not become ready within {0:?}")]
    ReadinessTimeout(Duration),
    #[error("failed to inspect worker state: {0}")]
    Wait(#[source] io::Error),
    #[error("failed to send signal {signal} to worker: {source}")]
    Signal {
        signal: libc::c_int,
        #[source]
        source: io::Error,
    },
    #[error("failed to kill worker after its shutdown deadlines: {0}")]
    Kill(#[source] io::Error),
}

pub struct Worker {
    child: Child,
    address: SocketAddr,
    terminate_timeout: Duration,
    interrupt_timeout: Duration,
}

impl Worker {
    pub fn start(config: WorkerConfig) -> Result<Self, WorkerError> {
        let listener =
            TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).map_err(WorkerError::Bind)?;
        let address = listener
            .local_addr()
            .map_err(WorkerError::ListenerAddress)?;
        let listener_fd = listener.as_raw_fd();

        let mut command = Command::new(&config.program);
        command.args(&config.args);
        command.envs(config.env.iter().map(|(key, value)| (key, value)));
        command.env("GONVEX_ADDR", address.to_string());
        command.env("GONVEX_RUNTIME_WORKER", "1");
        command.env("GONVEX_RELOAD_SUPERVISOR", "false");
        command.env("GONVEX_WORKER_LISTENER_FD", WORKER_LISTENER_FD.to_string());

        // SAFETY: only async-signal-safe libc calls run between fork and exec.
        // `listener` remains alive until spawn completes, so listener_fd is
        // valid in the child. Clearing FD_CLOEXEC also covers the case where
        // the listener was already allocated as fd 3 and dup2 is a no-op.
        unsafe {
            command.pre_exec(move || {
                if libc::dup2(listener_fd, WORKER_LISTENER_FD) == -1 {
                    return Err(io::Error::last_os_error());
                }
                let flags = libc::fcntl(WORKER_LISTENER_FD, libc::F_GETFD);
                if flags == -1 {
                    return Err(io::Error::last_os_error());
                }
                if libc::fcntl(WORKER_LISTENER_FD, libc::F_SETFD, flags & !libc::FD_CLOEXEC) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let child = command.spawn().map_err(|source| WorkerError::Spawn {
            program: config.program.clone(),
            source,
        })?;
        drop(listener);

        let mut worker = Self {
            child,
            address,
            terminate_timeout: config.terminate_timeout,
            interrupt_timeout: config.interrupt_timeout,
        };
        if let Err(error) =
            worker.wait_until_ready(config.readiness_timeout, config.readiness_poll_interval)
        {
            worker.force_stop();
            return Err(error);
        }
        Ok(worker)
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    /// Starts bounded graceful shutdown. SIGTERM gets the first deadline,
    /// SIGINT gets the second, and a worker still running after both is killed.
    pub fn begin_graceful_shutdown(&mut self) -> Result<ExitStatus, WorkerError> {
        if let Some(status) = self.child.try_wait().map_err(WorkerError::Wait)? {
            return Ok(status);
        }

        self.send_signal(libc::SIGTERM)?;
        if let Some(status) = self.wait_until_exit(self.terminate_timeout)? {
            return Ok(status);
        }

        self.send_signal(libc::SIGINT)?;
        if let Some(status) = self.wait_until_exit(self.interrupt_timeout)? {
            return Ok(status);
        }

        self.child.kill().map_err(WorkerError::Kill)?;
        self.child.wait().map_err(WorkerError::Wait)
    }

    fn wait_until_ready(
        &mut self,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<(), WorkerError> {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().map_err(WorkerError::Wait)? {
                return Err(WorkerError::ExitedBeforeReady(status));
            }

            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(WorkerError::ReadinessTimeout(timeout));
            }
            if health_is_ready(self.address, remaining.min(Duration::from_millis(250))) {
                return Ok(());
            }
            thread::sleep(poll_interval.min(remaining));
        }
    }

    fn wait_until_exit(&mut self, timeout: Duration) -> Result<Option<ExitStatus>, WorkerError> {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().map_err(WorkerError::Wait)? {
                return Ok(Some(status));
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Ok(None);
            }
            thread::sleep(Duration::from_millis(10).min(remaining));
        }
    }

    fn send_signal(&self, signal: libc::c_int) -> Result<(), WorkerError> {
        // SAFETY: the child PID comes from std::process::Child and the signal
        // value is one of the Unix constants used above.
        let result = unsafe { libc::kill(self.child.id() as libc::pid_t, signal) };
        if result == -1 {
            return Err(WorkerError::Signal {
                signal,
                source: io::Error::last_os_error(),
            });
        }
        Ok(())
    }

    fn force_stop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.force_stop();
    }
}

fn health_is_ready(address: SocketAddr, timeout: Duration) -> bool {
    let Ok(mut stream) = TcpStream::connect_timeout(&address, timeout) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let request =
        format!("GET {HEALTH_PATH} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut response = [0_u8; 256];
    let Ok(read) = stream.read(&mut response) else {
        return false;
    };
    let first_line = response[..read]
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    first_line.starts_with(b"HTTP/1.1 200 ") || first_line.starts_with(b"HTTP/1.0 200 ")
}

pub fn command_config(program: impl AsRef<OsStr>) -> WorkerConfig {
    WorkerConfig::new(Path::new(program.as_ref()).to_path_buf())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::os::fd::FromRawFd;
    use std::process::ExitStatus;
    use std::time::Duration;

    use super::{Worker, WorkerConfig, WorkerError};

    const HELPER_ENV: &str = "GONVEX_RUNTIME_TEST_HELPER";

    fn helper_config(mode: &str) -> WorkerConfig {
        WorkerConfig::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--ignored",
                "--exact",
                "tests::worker_helper",
                "--nocapture",
            ])
            .env(HELPER_ENV, mode)
    }

    #[test]
    fn starts_worker_with_inherited_listener_and_required_environment() {
        let mut config = helper_config("ready");
        config.readiness_timeout = Duration::from_secs(2);
        config.terminate_timeout = Duration::from_secs(1);
        config.interrupt_timeout = Duration::from_millis(100);

        let mut worker = Worker::start(config).expect("worker becomes ready");
        assert!(worker.address().ip().is_loopback());
        assert_ne!(worker.address().port(), 0);
        assert!(worker.try_wait().expect("worker state").is_none());

        let status = worker.begin_graceful_shutdown().expect("worker shuts down");
        assert_terminated_by(status, libc::SIGTERM);
    }

    #[test]
    fn reports_a_worker_that_exits_before_readiness() {
        let mut config = helper_config("exit");
        config.readiness_timeout = Duration::from_secs(2);

        let error = match Worker::start(config) {
            Ok(_) => panic!("worker unexpectedly became ready"),
            Err(error) => error,
        };
        assert!(matches!(error, WorkerError::ExitedBeforeReady(_)));
    }

    #[test]
    fn times_out_when_health_never_returns_200() {
        let mut config = helper_config("unready");
        config.readiness_timeout = Duration::from_millis(150);
        config.readiness_poll_interval = Duration::from_millis(10);

        let error = match Worker::start(config) {
            Ok(_) => panic!("worker unexpectedly became ready"),
            Err(error) => error,
        };
        assert!(matches!(error, WorkerError::ReadinessTimeout(_)));
    }

    #[cfg(unix)]
    fn assert_terminated_by(status: ExitStatus, signal: libc::c_int) {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(signal));
    }

    #[test]
    #[ignore]
    fn worker_helper() {
        let Ok(mode) = std::env::var(HELPER_ENV) else {
            return;
        };
        if mode == "exit" {
            return;
        }

        assert_eq!(std::env::var("GONVEX_RUNTIME_WORKER").as_deref(), Ok("1"));
        assert_eq!(
            std::env::var("GONVEX_RELOAD_SUPERVISOR").as_deref(),
            Ok("false")
        );
        assert_eq!(
            std::env::var("GONVEX_WORKER_LISTENER_FD").as_deref(),
            Ok("3")
        );

        // SAFETY: the supervisor promises that fd 3 is the inherited listener,
        // and this helper takes sole ownership of it in the child process.
        let listener = unsafe { TcpListener::from_raw_fd(3) };
        assert_eq!(
            listener.local_addr().expect("listener address").to_string(),
            std::env::var("GONVEX_ADDR").expect("GONVEX_ADDR")
        );

        loop {
            let (mut stream, _) = listener.accept().expect("health connection");
            let mut request = [0_u8; 512];
            let read = stream.read(&mut request).expect("health request");
            assert!(request[..read].starts_with(b"GET /healthz HTTP/1.1\r\n"));
            let status = if mode == "ready" {
                "200 OK"
            } else {
                "503 Service Unavailable"
            };
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
        }
    }
}
