#![allow(dead_code)]

use std;
use std::io;
use std::error::Error;
use std::os::unix::io::RawFd;
use std::time::{Duration, Instant};

use serde_json as json;
use futures::{Async, Future};
use byteorder::{ByteOrder, BigEndian};
use bytes::{BytesMut, BufMut};
use tokio_core::reactor::{self, Timeout};
use tokio_io::codec::{Encoder, Decoder};
use nix::sys::signal::{kill, Signal};
use nix::unistd::{close, pipe, fork, ForkResult, Pid};

use ctx::prelude::*;

use config::ServiceConfig;
use io::PipeFile;
use worker::{WorkerMessage, WorkerCommand};
use event::Reason;
use exec::exec_worker;
use service::{self, FeService};

const HEARTBEAT: u64 = 2;
const WORKER_TIMEOUT: i8 = 98;
pub const WORKER_INIT_FAILED: i8 = 99;
pub const WORKER_BOOT_FAILED: i8 = 100;

pub struct Process {
    idx: usize,
    pid: Pid,
    state: ProcessState,
    hb: Instant,
    addr: Address<FeService>,
    timeout: Duration,
    startup_timeout: u64,
    shutdown_timeout: u64,
    sink: Sink<ProcessSink>,
}

#[derive(Debug)]
enum ProcessState {
    Starting,
    Failed,
    Running,
    Stopping,
}

#[derive(PartialEq, Debug)]
pub enum ProcessMessage {
    Message(WorkerMessage),
    StartupTimeout,
    StopTimeout,
    Heartbeat,
    Kill,
}

#[derive(Debug, Clone)]
pub enum ProcessError {
    /// Heartbeat failed
    Heartbeat,
    /// Worker startup process failed, possibly application initialization failed
    FailedToStart(Option<String>),
    /// Timeout during startup
    StartupTimeout,
    /// Timeout during graceful stop
    StopTimeout,
    /// Worker configuratin error
    ConfigError(String),
    /// Worker init failed
    InitFailed,
    /// Worker boot failed
    BootFailed,
    /// Worker received signal
    Signal(usize),
    /// Worker exited with code
    ExitCode(i8),
}

impl ProcessError {
    pub fn from(code: i8) -> ProcessError {
        match code {
            WORKER_TIMEOUT => ProcessError::StartupTimeout,
            WORKER_INIT_FAILED => ProcessError::InitFailed,
            WORKER_BOOT_FAILED => ProcessError::BootFailed,
            code => ProcessError::ExitCode(code),
        }
    }
}

impl<'a> std::convert::From<&'a ProcessError> for Reason
{
    fn from(ob: &'a ProcessError) -> Self {
        match ob {
            &ProcessError::Heartbeat => Reason::HeartbeatFailed,
            &ProcessError::FailedToStart(ref err) =>
                Reason::FailedToStart(
                    if let &Some(ref e) = err { Some(format!("{}", e))} else {None}),
            &ProcessError::StartupTimeout => Reason::StartupTimeout,
            &ProcessError::StopTimeout => Reason::StopTimeout,
            &ProcessError::ConfigError(ref err) => Reason::WorkerError(err.clone()),
            &ProcessError::InitFailed => Reason::InitFailed,
            &ProcessError::BootFailed => Reason::BootFailed,
            &ProcessError::Signal(sig) => Reason::Signal(sig),
            &ProcessError::ExitCode(code) => Reason::ExitCode(code),
        }
    }
}


impl Process {

    pub fn start(idx: usize, handle: &reactor::Handle,
                 cfg: &ServiceConfig, addr: Address<FeService>) -> (Pid, Option<Address<Process>>)
    {
        // fork process and esteblish communication
        let (pid, pipe) = match Process::fork(handle, cfg) {
            Ok(res) => res,
            Err(err) => {
                let pid = Pid::from_raw(-1);
                service::ProcessFailed(
                    idx, pid,
                    ProcessError::FailedToStart(Some(format!("{}", err))))
                    .send_to(&addr);

                return (pid, None)
            }
        };

        let timeout = Duration::new(cfg.timeout as u64, 0);
        let startup_timeout = cfg.startup_timeout as u64;
        let shutdown_timeout = cfg.shutdown_timeout as u64;

        // start Process service
        let (r, w) = pipe.ctx_framed(TransportCodec, TransportCodec);
        let addr = Builder::with_service_init(
            r, handle,
            move |ctx| Process {
                idx: idx,
                pid: pid,
                state: ProcessState::Starting,
                hb: Instant::now(),
                addr: addr,
                timeout: timeout,
                startup_timeout: startup_timeout,
                shutdown_timeout: shutdown_timeout,
                sink: ctx.add_sink(ProcessSink, w)
            })
            .add_future(
                Timeout::new(Duration::new(cfg.startup_timeout as u64, 0), &handle).unwrap()
                    .map(|_| ProcessMessage::StartupTimeout)
            )
            .run();

        (pid, Some(addr))
    }

    fn fork(handle: &reactor::Handle, cfg: &ServiceConfig) -> Result<(Pid, PipeFile), io::Error>
    {
        let (p_read, p_write, ch_read, ch_write) = Process::create_pipes()?;

        // fork
        let pid = match fork() {
            Ok(ForkResult::Parent{ child }) => child,
            Ok(ForkResult::Child) => {
                let _ = close(p_write);
                let _ = close(ch_read);
                exec_worker(cfg, p_read, ch_write);
                unreachable!();
            },
            Err(err) => {
                error!("Fork failed: {}", err.description());
                return Err(io::Error::new(io::ErrorKind::Other, err.description()))
            }
        };

        // initialize worker communication channel
        let _ = close(p_read);
        let _ = close(ch_write);
        let pipe = PipeFile::new(ch_read, p_write, handle);

        Ok((pid, pipe))
    }

    fn create_pipes() -> Result<(RawFd, RawFd, RawFd, RawFd), io::Error> {
        // open communication pipes
        let (p_read, p_write) = match pipe() {
            Ok((r, w)) => (r, w),
            Err(err) => {
                error!("Can not create pipe: {}", err);
                return Err(io::Error::new(
                    io::ErrorKind::Other, format!("Can not create pipe: {}", err)))
            }
        };
        let (ch_read, ch_write) = match pipe() {
            Ok((r, w)) => (r, w),
            Err(err) => {
                error!("Can not create pipe: {}", err);
                return Err(io::Error::new(
                    io::ErrorKind::Other, format!("Can not create pipe: {}", err)))
            }
        };
        Ok((p_read, p_write, ch_read, ch_write))
    }

    fn kill(&self, ctx: &mut Context<Self>) {
        let fut = Box::new(
            Timeout::new(Duration::new(1, 0), ctx.handle())
                .unwrap()
                .map(|_| ProcessMessage::Kill));
        ctx.add_future(fut);
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = kill(self.pid, Signal::SIGKILL);
    }
}

struct ProcessSink;

impl SinkService for ProcessSink {

    type Service = Process;
    type SinkMessage = Result<WorkerCommand, io::Error>;
}

impl Service for Process {

    type Context = Context<Self>;
    type Message = Result<ProcessMessage, io::Error>;
    type Result = Result<(), ()>;

    fn finished(&mut self, ctx: &mut Self::Context) -> Result<Async<()>, ()>
    {
        self.kill(ctx);
        Ok(Async::NotReady)
    }

    fn call(&mut self, ctx: &mut Self::Context, msg: Self::Message)
            -> Result<Async<()>, ()>
    {
        match msg {
            Ok(ProcessMessage::Message(msg)) => match msg {
                WorkerMessage::forked => {
                    debug!("Worker forked (pid:{})", self.pid);
                    self.sink.send_buffered(WorkerCommand::prepare);
                }
                WorkerMessage::loaded => {
                    match self.state {
                        ProcessState::Starting => {
                            debug!("Worker loaded (pid:{})", self.pid);
                            service::ProcessLoaded(self.idx, self.pid).send_to(&self.addr);

                            // start heartbeat timer
                            self.state = ProcessState::Running;
                            self.hb = Instant::now();
                            let fut = Box::new(
                                Timeout::new(
                                    Duration::new(HEARTBEAT, 0), ctx.handle())
                                    .unwrap()
                                    .map(|_| ProcessMessage::Heartbeat));
                            ctx.add_future(fut);
                        },
                        _ => {
                            warn!("Received `loaded` message from worker (pid:{})", self.pid);
                        }
                    }
                }
                WorkerMessage::hb => {
                    self.hb = Instant::now();
                }
                WorkerMessage::reload => {
                    // worker requests reload
                    info!("Worker requests reload (pid:{})", self.pid);
                    service::ProcessMessage(self.idx, self.pid, WorkerMessage::reload)
                        .send_to(&self.addr);
                }
                WorkerMessage::restart => {
                    // worker requests reload
                    info!("Worker requests restart (pid:{})", self.pid);
                    service::ProcessMessage(self.idx, self.pid, WorkerMessage::restart)
                        .send_to(&self.addr);
                }
                WorkerMessage::cfgerror(msg) => {
                    error!("Worker config error: {} (pid:{})", msg, self.pid);
                    service::ProcessFailed(self.idx, self.pid, ProcessError::ConfigError(msg))
                        .send_to(&self.addr);
                }
            }
            Ok(ProcessMessage::StartupTimeout) => {
                match self.state {
                    ProcessState::Starting => {
                        error!("Worker startup timeout after {} secs", self.startup_timeout);
                        service::ProcessFailed(self.idx, self.pid, ProcessError::StartupTimeout)
                            .send_to(&self.addr);

                        self.state = ProcessState::Failed;
                        let _ = kill(self.pid, Signal::SIGKILL);
                        return Ok(Async::Ready(()))
                    },
                    _ => ()
                }
            }
            Ok(ProcessMessage::StopTimeout) => {
                match self.state {
                    ProcessState::Stopping => {
                        info!("Worker shutdown timeout aftre {} secs", self.shutdown_timeout);
                        service::ProcessFailed(self.idx, self.pid, ProcessError::StopTimeout)
                            .send_to(&self.addr);

                        self.state = ProcessState::Failed;
                        let _ = kill(self.pid, Signal::SIGKILL);
                        return Ok(Async::Ready(()))
                    },
                    _ => ()
                }
            }
            Ok(ProcessMessage::Heartbeat) => {
                // makes sense only in running state
                if let ProcessState::Running = self.state {
                    if Instant::now().duration_since(self.hb) > self.timeout {
                        // heartbeat timed out
                        error!("Worker heartbeat failed (pid:{}) after {:?} secs",
                               self.pid, self.timeout);
                        service::ProcessFailed(self.idx, self.pid, ProcessError::Heartbeat)
                            .send_to(&self.addr);
                    } else {
                        // send heartbeat to worker process and reset hearbeat timer
                        self.sink.send_buffered(WorkerCommand::hb);
                        let fut = Box::new(
                                Timeout::new(Duration::new(HEARTBEAT, 0), ctx.handle())
                                    .unwrap()
                                    .map(|_| ProcessMessage::Heartbeat));
                        ctx.add_future(fut);
                    }
                }
            }
            Ok(ProcessMessage::Kill) => {
                let _ = kill(self.pid, Signal::SIGKILL);
                return Ok(Async::Ready(()))
            }
            Err(_) => self.kill(ctx),
        }
        Ok(Async::NotReady)
    }
}

pub struct SendCommand(pub WorkerCommand);

impl Message for SendCommand {

    type Item = ();
    type Error = ();
    type Service = Process;

    fn handle(&self, srv: &mut Process, _: &mut Context<Process>) -> MessageFuture<Self>
    {
        srv.sink.send_buffered(self.0.clone());
        Box::new(fut::ok(()))
    }
}

pub struct StartProcess;

impl Message for StartProcess {

    type Item = ();
    type Error = ();
    type Service = Process;

    fn handle(&self, srv: &mut Process, _: &mut Context<Process>) -> MessageFuture<Self>
    {
        srv.sink.send_buffered(WorkerCommand::start);
        Box::new(fut::ok(()))
    }
}

pub struct PauseProcess;

impl Message for PauseProcess {

    type Item = ();
    type Error = ();
    type Service = Process;

    fn handle(&self, srv: &mut Process, _: &mut Context<Process>) -> MessageFuture<Self>
    {
        srv.sink.send_buffered(WorkerCommand::pause);
        Box::new(fut::ok(()))
    }
}

pub struct ResumeProcess;

impl Message for ResumeProcess {

    type Item = ();
    type Error = ();
    type Service = Process;

    fn handle(&self, srv: &mut Process, _: &mut Context<Process>) -> MessageFuture<Self>
    {
        srv.sink.send_buffered(WorkerCommand::resume);
        Box::new(fut::ok(()))
    }
}

pub struct StopProcess;

impl Message for StopProcess {

    type Item = ();
    type Error = ();
    type Service = Process;

    fn handle(&self, srv: &mut Process, ctx: &mut Context<Process>) -> MessageFuture<Self>
    {
        info!("Stopping worker: (pid:{})", srv.pid);
        match srv.state {
            ProcessState::Running => {
                srv.sink.send_buffered(WorkerCommand::stop);

                srv.state = ProcessState::Stopping;
                if let Ok(timeout) = Timeout::new(
                    Duration::new(srv.shutdown_timeout, 0), ctx.handle())
                {
                    ctx.add_future(timeout.map(|_| ProcessMessage::StopTimeout));
                    let _ = kill(srv.pid, Signal::SIGTERM);
                } else {
                    // can not create timeout
                    let _ = kill(srv.pid, Signal::SIGQUIT);
                    // return Ok(Async::Ready(()))
                }
            },
            _ => {
                let _ = kill(srv.pid, Signal::SIGQUIT);
                // return Ok(Async::Ready(()))
            }
        }
        Box::new(fut::ok(()))
    }
}

pub struct QuitProcess;

impl Message for QuitProcess {

    type Item = ();
    type Error = ();
    type Service = Process;

    fn handle(&self, srv: &mut Process, ctx: &mut Context<Process>) -> MessageFuture<Self>
    {
        let _ = kill(srv.pid, Signal::SIGQUIT);
        srv.kill(ctx);
        Box::new(fut::ok(()))
    }
}


struct TransportCodec;

impl Decoder for TransportCodec {
    type Item = ProcessMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let size = {
            if src.len() < 2 {
                return Ok(None)
            }
            BigEndian::read_u16(src.as_ref()) as usize
        };

        if src.len() >= size + 2 {
            src.split_to(2);
            let buf = src.split_to(size);
            Ok(Some(ProcessMessage::Message(json::from_slice::<WorkerMessage>(&buf)?)))
        } else {
            Ok(None)
        }
    }
}

impl Encoder for TransportCodec {
    type Item = WorkerCommand;
    type Error = io::Error;

    fn encode(&mut self, msg: WorkerCommand, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg = json::to_string(&msg).unwrap();
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 2);
        dst.put_u16::<BigEndian>(msg_ref.len() as u16);
        dst.put(msg_ref);

        Ok(())
    }
}
