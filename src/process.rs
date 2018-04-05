#![allow(dead_code)]

use std;
use std::io;
use std::error::Error;
use std::os::unix::io::RawFd;
use std::time::{Duration, Instant};

use serde_json as json;
use byteorder::{ByteOrder, BigEndian};
use bytes::{BytesMut, BufMut};
use tokio_io::AsyncRead;
use tokio_io::io::WriteHalf;
use tokio_io::codec::{FramedRead, Encoder, Decoder};
use nix::sys::signal::{kill, Signal};
use nix::unistd::{close, pipe, fork, ForkResult, Pid};

use actix::prelude::*;

use config::ServiceConfig;
use io::PipeFile;
use worker::{WorkerMessage, WorkerCommand};
use event::Reason;
use exec::exec_worker;
use service::{self, FeService};

const HEARTBEAT: u64 = 2;
const WORKER_TIMEOUT: i32 = 98;
pub const WORKER_INIT_FAILED: i32 = 99;
pub const WORKER_BOOT_FAILED: i32 = 100;

pub struct Process {
    idx: usize,
    pid: Pid,
    state: ProcessState,
    hb: Instant,
    addr: Addr<Unsync, FeService>,
    timeout: Duration,
    startup_timeout: u64,
    shutdown_timeout: u64,
    framed: actix::io::FramedWrite<WriteHalf<PipeFile>, TransportCodec>,
}

impl Actor for Process {
    type Context = Context<Self>;

    fn stopping(&mut self, ctx: &mut Context<Self>) -> Running {
        self.kill(ctx, false);
        Running::Stop
    }
}

impl StreamHandler<ProcessMessage, io::Error> for Process {

    fn finished(&mut self, ctx: &mut Context<Self>) {
        self.kill(ctx, false);
        ctx.stop();
    }

    fn handle(&mut self, msg: ProcessMessage, ctx: &mut Self::Context) {
        ctx.notify(msg);
    }
}

#[derive(Debug)]
enum ProcessState {
    Starting,
    Failed,
    Running,
    Stopping,
}

#[derive(PartialEq, Debug, Message)]
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
        match code as i32 {
            WORKER_TIMEOUT => ProcessError::StartupTimeout,
            WORKER_INIT_FAILED => ProcessError::InitFailed,
            WORKER_BOOT_FAILED => ProcessError::BootFailed,
            code => ProcessError::ExitCode(code as i8),
        }
    }
}

impl<'a> std::convert::From<&'a ProcessError> for Reason
{
    fn from(ob: &'a ProcessError) -> Self {
        match *ob {
            ProcessError::Heartbeat => Reason::HeartbeatFailed,
            ProcessError::FailedToStart(ref err) =>
                Reason::FailedToStart(
                    if let &Some(ref e) = err { Some(format!("{}", e))} else {None}),
            ProcessError::StartupTimeout => Reason::StartupTimeout,
            ProcessError::StopTimeout => Reason::StopTimeout,
            ProcessError::ConfigError(ref err) => Reason::WorkerError(err.clone()),
            ProcessError::InitFailed => Reason::InitFailed,
            ProcessError::BootFailed => Reason::BootFailed,
            ProcessError::Signal(sig) => Reason::Signal(sig),
            ProcessError::ExitCode(code) => Reason::ExitCode(code),
        }
    }
}


impl Process {

    pub fn start(idx: usize, cfg: &ServiceConfig, addr: Addr<Unsync, FeService>)
                 -> (Pid, Option<Addr<Unsync, Process>>)
    {
        // fork process and esteblish communication
        let (pid, pipe) = match Process::fork(idx, cfg) {
            Ok(res) => res,
            Err(err) => {
                let pid = Pid::from_raw(-1);
                addr.do_send(
                    service::ProcessFailed(
                        idx, pid,
                        ProcessError::FailedToStart(Some(format!("{}", err)))));

                return (pid, None)
            }
        };

        let timeout = Duration::new(u64::from(cfg.timeout), 0);
        let startup_timeout = u64::from(cfg.startup_timeout);
        let shutdown_timeout = u64::from(cfg.shutdown_timeout);

        // start Process service
        let addr = Process::create(move |ctx| {
            let (r, w) = pipe.split();
            ctx.add_stream(FramedRead::new(r, TransportCodec));
            ctx.notify_later(ProcessMessage::StartupTimeout,
                             Duration::new(startup_timeout as u64, 0));
            Process {
                idx, pid, addr, timeout, startup_timeout, shutdown_timeout,
                state: ProcessState::Starting,
                hb: Instant::now(),
                framed: actix::io::FramedWrite::new(w, TransportCodec, ctx)
            }});
        (pid, Some(addr))
    }

    fn fork(idx: usize, cfg: &ServiceConfig) -> Result<(Pid, PipeFile), io::Error>
    {
        let (p_read, p_write, ch_read, ch_write) = Process::create_pipes()?;

        // fork
        let pid = match fork() {
            Ok(ForkResult::Parent{ child }) => child,
            Ok(ForkResult::Child) => {
                let _ = close(p_write);
                let _ = close(ch_read);
                exec_worker(idx, cfg, p_read, ch_write);
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
        let pipe = PipeFile::new(ch_read, p_write, Arbiter::handle());

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

    fn kill(&self, ctx: &mut Context<Self>, graceful: bool) {
        if graceful {
            ctx.notify_later(ProcessMessage::Kill, Duration::new(1, 0));
        } else {
            let _ = kill(self.pid, Signal::SIGKILL);
            ctx.terminate();
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = kill(self.pid, Signal::SIGKILL);
    }
}

impl actix::io::WriteHandler<io::Error> for Process {}

impl Handler<ProcessMessage> for Process {
    type Result = ();

    fn handle(&mut self, msg: ProcessMessage, ctx: &mut Context<Self>) {
        match msg {
            ProcessMessage::Message(msg) => match msg {
                WorkerMessage::forked => {
                    debug!("Worker forked (pid:{})", self.pid);
                    self.framed.write(WorkerCommand::prepare);
                }
                WorkerMessage::loaded => {
                    match self.state {
                        ProcessState::Starting => {
                            debug!("Worker loaded (pid:{})", self.pid);
                            self.addr.do_send(
                                service::ProcessLoaded(self.idx, self.pid));

                            // start heartbeat timer
                            self.state = ProcessState::Running;
                            self.hb = Instant::now();
                            ctx.notify_later(
                                ProcessMessage::Heartbeat, Duration::new(HEARTBEAT, 0));
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
                    self.addr.do_send(
                        service::ProcessMessage(
                            self.idx, self.pid, WorkerMessage::reload));
                }
                WorkerMessage::restart => {
                    // worker requests reload
                    info!("Worker requests restart (pid:{})", self.pid);
                    self.addr.do_send(
                        service::ProcessMessage(
                            self.idx, self.pid, WorkerMessage::restart));
                }
                WorkerMessage::cfgerror(msg) => {
                    error!("Worker config error: {} (pid:{})", msg, self.pid);
                    self.addr.do_send(
                        service::ProcessFailed(
                            self.idx, self.pid, ProcessError::ConfigError(msg)));
                }
            }
            ProcessMessage::StartupTimeout => {
                if let ProcessState::Starting = self.state {
                    error!("Worker startup timeout after {} secs", self.startup_timeout);
                    self.addr.do_send(
                        service::ProcessFailed(
                            self.idx, self.pid, ProcessError::StartupTimeout));

                    self.state = ProcessState::Failed;
                    let _ = kill(self.pid, Signal::SIGKILL);
                    ctx.stop();
                    return
                }
            }
            ProcessMessage::StopTimeout => {
                if let ProcessState::Stopping = self.state {
                    info!("Worker shutdown timeout aftre {} secs", self.shutdown_timeout);
                    self.addr.do_send(
                        service::ProcessFailed(
                            self.idx, self.pid, ProcessError::StopTimeout));

                    self.state = ProcessState::Failed;
                    let _ = kill(self.pid, Signal::SIGKILL);
                    ctx.stop();
                    return
                }
            }
            ProcessMessage::Heartbeat => {
                // makes sense only in running state
                if let ProcessState::Running = self.state {
                    if Instant::now().duration_since(self.hb) > self.timeout {
                        // heartbeat timed out
                        error!("Worker heartbeat failed (pid:{}) after {:?} secs",
                               self.pid, self.timeout);
                        self.addr.do_send(
                            service::ProcessFailed(
                                self.idx, self.pid, ProcessError::Heartbeat));
                    } else {
                        // send heartbeat to worker process and reset hearbeat timer
                        self.framed.write(WorkerCommand::hb);
                        ctx.notify_later(
                            ProcessMessage::Heartbeat, Duration::new(HEARTBEAT, 0));
                    }
                }
            }
            ProcessMessage::Kill => {
                let _ = kill(self.pid, Signal::SIGKILL);
                ctx.stop();
                return
            }
        }
    }
}

#[derive(Message)]
pub struct SendCommand(pub WorkerCommand);

impl Handler<SendCommand> for Process {
    type Result = ();

    fn handle(&mut self, msg: SendCommand, _: &mut Context<Process>) {
        self.framed.write(msg.0);
    }
}

#[derive(Message)]
pub struct StartProcess;

impl Handler<StartProcess> for Process {
    type Result = ();

    fn handle(&mut self, _: StartProcess, _: &mut Context<Process>) {
        self.framed.write(WorkerCommand::start);
    }
}

#[derive(Message)]
pub struct PauseProcess;

impl Handler<PauseProcess> for Process {
    type Result = ();

    fn handle(&mut self, _: PauseProcess, _: &mut Context<Process>) {
        self.framed.write(WorkerCommand::pause);
    }
}

#[derive(Message)]
pub struct ResumeProcess;

impl Handler<ResumeProcess> for Process {
    type Result = ();

    fn handle(&mut self, _: ResumeProcess, _: &mut Context<Process>) {
        self.framed.write(WorkerCommand::resume);
    }
}

#[derive(Message)]
pub struct StopProcess;

impl Handler<StopProcess> for Process {
    type Result = ();

    fn handle(&mut self, _: StopProcess, ctx: &mut Context<Process>)
    {
        info!("Stopping worker: (pid:{})", self.pid);
        match self.state {
            ProcessState::Running => {
                self.state = ProcessState::Stopping;

                self.framed.write(WorkerCommand::stop);
                ctx.notify_later(
                    ProcessMessage::StopTimeout,
                    Duration::new(self.shutdown_timeout, 0));
                let _ = kill(self.pid, Signal::SIGTERM);
            },
            _ => {
                let _ = kill(self.pid, Signal::SIGQUIT);
                ctx.terminate();
            }
        }
    }
}

#[derive(Message)]
pub struct QuitProcess(pub bool);

impl Handler<QuitProcess> for Process {
    type Result = ();

    fn handle(&mut self, msg: QuitProcess, ctx: &mut Context<Process>) {
        if msg.0 {
            let _ = kill(self.pid, Signal::SIGQUIT);
            self.kill(ctx, true);
        } else {
            self.kill(ctx, false);
            let _ = kill(self.pid, Signal::SIGKILL);
            ctx.terminate();
        }
    }
}

pub struct TransportCodec;

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
