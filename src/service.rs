#![allow(dead_code)]

use std;
use futures::{unsync, Async, Stream};
use tokio_core::reactor;
use nix::unistd::Pid;

use ctx::prelude::*;

use event::{Event, Reason};
use config::ServiceConfig;
use worker::{Worker, WorkerMessage};
use process::ProcessError;


#[derive(PartialEq, Debug)]
/// Service interface
pub enum ServiceCommand {
    // /// Gracefully reload workers
    // Reload,
    // /// Reconfigure active workers
    // Configure(usize, String),
    // /// Gracefully stopping workers
    // Stop,
    /// Quit all workers
    Quit,
}

/// Service state
#[derive(Debug)]
enum ServiceState {
    Running,
    Failed,
    Stopped,
    Starting(Task<StartStatus>),
    Reloading(Task<ReloadStatus>),
    Stopping(Task<()>),
}

impl ServiceState {
    fn description(&self) -> &'static str {
        match *self {
            ServiceState::Running => "running",
            ServiceState::Failed => "failed",
            ServiceState::Stopped => "stopped",
            ServiceState::Starting(_) => "starting",
            ServiceState::Reloading(_) => "reloading",
            ServiceState::Stopping(_) => "stopping",
        }
    }

    fn error(&self) -> ServiceOperationError {
        match *self {
            ServiceState::Running => ServiceOperationError::Running,
            ServiceState::Failed => ServiceOperationError::Failed,
            ServiceState::Stopped => ServiceOperationError::Stopped,
            ServiceState::Starting(_) => ServiceOperationError::Starting,
            ServiceState::Reloading(_) => ServiceOperationError::Reloading,
            ServiceState::Stopping(_) => ServiceOperationError::Stopping,
        }
    }
}

#[derive(Debug)]
/// Service errors
pub enum ServiceOperationError {
    Starting,
    Reloading,
    Stopping,
    Running,
    Stopped,
    Failed,
}

#[derive(Debug)]
pub enum ServiceMessage {
    /// external command
    Command(ServiceCommand),
}

#[derive(Clone, Debug)]
pub enum StartStatus {
    Success,
    Failed,
    Stopping,
}

#[derive(Clone, Debug)]
pub enum ReloadStatus {
    Success,
    Failed,
    Stopping,
}

pub struct FeService {
    name: String,
    state: ServiceState,
    paused: bool,
    workers: Vec<Worker>,
    tx: unsync::mpsc::UnboundedSender<ServiceCommand>,
}

impl FeService {

    pub fn start(handle: &reactor::Handle,
                 num: u16,
                 cfg: ServiceConfig) -> Address<FeService>
    {
        let (tx, rx) = unsync::mpsc::unbounded();

        Builder::with_service_init(
            rx.map(|cmd| ServiceMessage::Command(cmd)), handle,
            move|ctx| {
                let h = ctx.handle();

                // create workers
                let mut workers = Vec::new();
                for idx in 0..num as usize {
                    workers.push(Worker::new(idx, h, cfg.clone(), ctx.address()));
                }

                FeService {
                    name: cfg.name.clone(),
                    state: ServiceState::Starting(Task::new()),
                    paused: false,
                    workers: workers,
                    tx: tx}
            }).run()
    }

    fn check_loading_workers(&mut self, restart_stopped: bool) -> (bool, bool) {
        let mut in_process = false;
        let mut failed = false;

        for worker in self.workers.iter_mut() {
            if worker.is_failed() {
                failed = true;
            }
            else if worker.is_stopped() {
                if restart_stopped {
                    // strange
                    worker.reload(true, Reason::None);
                    in_process = true;
                }
            }
            else if !worker.is_running() {
                in_process = true;
            }
        }
        (failed, in_process)
    }

    // update internal state
    fn update(&mut self) {
        let state = std::mem::replace(&mut self.state, ServiceState::Failed);

        match state {
            ServiceState::Starting(task) => {
                let (failed, in_process) = self.check_loading_workers(true);

                // if we have failed workers, stop all and change service state to failed
                if failed {
                    if in_process {
                        for worker in self.workers.iter_mut() {
                            if !(worker.is_stopped() || worker.is_failed()) {
                                worker.stop(Reason::SomeWorkersFailed)
                            }
                        }
                        self.state = ServiceState::Starting(task);
                    } else {
                        task.set_result(StartStatus::Failed);
                        self.state = ServiceState::Failed;
                    }
                } else {
                    if !in_process {
                        task.set_result(StartStatus::Success);
                        self.state = ServiceState::Running;
                    } else {
                        self.state = ServiceState::Starting(task);
                    }
                }
            },
            ServiceState::Reloading(task) => {
                let (failed, in_process) = self.check_loading_workers(true);

                // if we have failed workers, stop all and change service state to failed
                if failed {
                    if in_process {
                        for worker in self.workers.iter_mut() {
                            if !(worker.is_stopped() || worker.is_failed()) {
                                worker.stop(Reason::SomeWorkersFailed)
                            }
                        }
                        self.state = ServiceState::Reloading(task);
                    } else {
                        task.set_result(ReloadStatus::Failed);
                        self.state = ServiceState::Failed;
                    }
                } else {
                    if !in_process {
                        task.set_result(ReloadStatus::Success);
                        self.state = ServiceState::Running;
                    } else {
                        self.state = ServiceState::Reloading(task);
                    }
                }
            },
            ServiceState::Stopping(task) => {
                let (_, in_process) = self.check_loading_workers(false);

                if !in_process {
                    task.set_result(());
                    self.state = ServiceState::Stopped;
                } else {
                    self.state = ServiceState::Stopping(task);
                }
            },
            state => self.state = state,
        }
    }

    fn message(&mut self, pid: Pid, message: WorkerMessage) {
        for worker in self.workers.iter_mut() {
            worker.message(pid, &message)
        }
    }

}


impl Service for FeService {

    type Context = Context<Self>;
    type Message = Result<ServiceMessage, ()>;
    type Result = Result<(), ()>;

    fn start(&mut self, _: &mut Self::Context) {
        // start workers
        for worker in self.workers.iter_mut() {
            worker.start(Reason::Initial);
        }
    }

    fn finished(&mut self, _: &mut Self::Context) -> Result<Async<()>, ()> {
        // command center probably dead
        Ok(Async::Ready(()))
    }

    fn call(&mut self, _: &mut Self::Context,
            cmd: Result<ServiceMessage, ()>) -> Result<Async<()>, ()>
    {
        match cmd {
            // Ok(ServiceMessage::Command(ServiceCommand::Reload)) => {
            //    let _ = ctx.reload(true);
            //}
            // CtxResult::Ok(ServiceCommand::Configure(_num, _exec)) => {
            // }
            // Ok(ServiceMessage::Command(ServiceCommand::Stop)) => {
            //    let _ = ctx.stop(true);
            // }
            Ok(ServiceMessage::Command(ServiceCommand::Quit)) => {
                // self.stop(ctx, false);
            }
            Err(_) =>
                return Ok(Async::Ready(())), // command center probably dead
        }

        Ok(Async::NotReady)
    }
}

pub struct ProcessMessage(pub usize, pub Pid, pub WorkerMessage);

impl Message for ProcessMessage {

    type Item = ();
    type Error = ();
    type Service = FeService;

    fn handle(&self, srv: &mut FeService, _: &mut Context<FeService>) -> MessageFuture<Self>
    {
        srv.workers[self.0].message(self.1, &self.2);
        srv.update();
        Box::new(fut::ok(()))
    }
}

pub struct ProcessFailed(pub usize, pub Pid, pub ProcessError);

impl Message for ProcessFailed {

    type Item = ();
    type Error = ();
    type Service = FeService;

    fn handle(&self, srv: &mut FeService, _: &mut Context<FeService>) -> MessageFuture<Self>
    {
        srv.workers[self.0].exited(self.1, &self.2);
        srv.update();
        Box::new(fut::ok(()))
    }
}

pub struct ProcessLoaded(pub usize, pub Pid);

impl Message for ProcessLoaded {

    type Item = ();
    type Error = ();
    type Service = FeService;

    fn handle(&self, srv: &mut FeService, _: &mut Context<FeService>) -> MessageFuture<Self>
    {
        srv.workers[self.0].loaded(self.1);
        srv.update();
        Box::new(fut::ok(()))
    }
}


pub struct ProcessExited(pub Pid, pub ProcessError);

impl Message for ProcessExited {

    type Item = ();
    type Error = ();
    type Service = FeService;

    fn handle(&self, srv: &mut FeService, _: &mut Context<FeService>) -> MessageFuture<Self>
    {
        for worker in srv.workers.iter_mut() {
            worker.exited(self.0, &self.1);
        }
        srv.update();
        Box::new(fut::ok(()))
    }
}

/// Service status command
pub struct ServicePids;

impl Message for ServicePids {

    type Item = Vec<String>;
    type Error = ();
    type Service = FeService;

    fn handle(&self, srv: &mut FeService, _: &mut Context<FeService>) -> MessageFuture<Self>
    {
        let mut pids = Vec::new();
        for worker in srv.workers.iter() {
            if let Some(pid) = worker.pid() {
                pids.push(format!("{}", pid));
            }
        }
        Box::new(fut::ok(pids))
    }
}

/// Service status command
pub struct ServiceStatus;

impl Message for ServiceStatus {

    type Item = (String, Vec<(String, Vec<Event>)>);
    type Error = ();
    type Service = FeService;

    fn handle(&self, srv: &mut FeService, _: &mut Context<FeService>) -> MessageFuture<Self>
    {
        let mut events: Vec<(String, Vec<Event>)> = Vec::new();
        for worker in srv.workers.iter() {
            events.push(
                (format!("worker({})", worker.idx + 1), Vec::from(&worker.events)));
        }

        let status = match srv.state {
            ServiceState::Running => if srv.paused { "paused" } else { "running" }
            _ => srv.state.description()
        };
        Box::new(fut::ok((status.to_owned(), events)))
    }
}

/// Start service command
pub struct StartService;

impl Message for StartService {

    type Item = StartStatus;
    type Error = ServiceOperationError;
    type Service = FeService;

    fn handle(&self, srv: &mut FeService, _: &mut Context<FeService>) -> MessageFuture<Self>
    {
        match srv.state {
            ServiceState::Starting(ref mut task) => {
                Box::new(
                    task.wait().ctxfuture().then(|res, _, _| match res {
                        Ok(res) => fut::result(Ok(res)),
                        Err(_) => fut::result(Err(ServiceOperationError::Failed)),
                    }))
            }
            ServiceState::Failed | ServiceState::Stopped => {
                debug!("Starting service: {:?}", srv.name);
                let mut task = Task::new();
                let rx = task.wait();
                srv.paused = false;
                srv.state = ServiceState::Starting(task);
                for worker in srv.workers.iter_mut() {
                    worker.start(Reason::ConsoleRequest);
                }
                Box::new(
                    rx.ctxfuture().then(|res, _, _| match res {
                        Ok(res) => fut::result(Ok(res)),
                        Err(_) => fut::result(Err(ServiceOperationError::Failed)),
                }))
            }
            _ => Box::new(fut::result(Err(srv.state.error())))
        }
    }
}

/// Pause service command
pub struct PauseService;

impl Message for PauseService {

    type Item = ();
    type Error = ServiceOperationError;
    type Service = FeService;

    fn handle(&self, srv: &mut FeService, _: &mut Context<FeService>) -> MessageFuture<Self>
    {
        let res = match srv.state {
            ServiceState::Running => {
                debug!("Pause service: {:?}", srv.name);
                for worker in srv.workers.iter_mut() {
                    worker.pause(Reason::ConsoleRequest);
                }
                srv.paused = true;
                Ok(())
            }
            _ => Err(srv.state.error())
        };
        Box::new(fut::result(res))
    }
}

/// Resume service command
pub struct ResumeService;

impl Message for ResumeService {

    type Item = ();
    type Error = ServiceOperationError;
    type Service = FeService;

    fn handle(&self, srv: &mut FeService, _: &mut Context<FeService>) -> MessageFuture<Self>
    {
        let res = match srv.state {
            ServiceState::Running => {
                debug!("Resume service: {:?}", srv.name);
                for worker in srv.workers.iter_mut() {
                    worker.resume(Reason::ConsoleRequest);
                }
                srv.paused = false;
                Ok(())
            }
            _ => Err(srv.state.error())
        };
        Box::new(fut::result(res))
    }
}

/// Reload service
pub struct ReloadService(pub bool);

impl Message for ReloadService {

    type Item = ReloadStatus;
    type Error = ServiceOperationError;
    type Service = FeService;

    fn handle(&self, srv: &mut FeService, _: &mut Context<FeService>) -> MessageFuture<Self>
    {
        match srv.state {
            ServiceState::Reloading(ref mut task) => {
                Box::new(
                    task.wait().ctxfuture().then(|res, _, _| match res {
                        Ok(res) => fut::result(Ok(res)),
                        Err(_) => fut::result(Err(ServiceOperationError::Failed)),
                    }))
            }
            ServiceState::Running | ServiceState::Failed | ServiceState::Stopped => {
                debug!("Reloading service: {:?}", srv.name);
                let mut task = Task::new();
                let rx = task.wait();
                srv.paused = false;
                srv.state = ServiceState::Reloading(task);
                for worker in srv.workers.iter_mut() {
                    worker.reload(self.0, Reason::ConsoleRequest);
                }
                Box::new(
                    rx.ctxfuture().then(|res, _, _| match res {
                        Ok(res) => fut::result(Ok(res)),
                        Err(_) => fut::result(Err(ServiceOperationError::Failed)),
                    }))
            }
            _ => Box::new(fut::result(Err(srv.state.error())))
        }
    }
}

/// Stop service command
pub struct StopService(pub bool, pub Reason);

impl Message for StopService {

    type Item = ();
    type Error = ();
    type Service = FeService;

    fn handle(&self, srv: &mut FeService, _: &mut Context<FeService>) -> MessageFuture<Self>
    {
        let state = std::mem::replace(&mut srv.state, ServiceState::Stopped);

        match state {
            ServiceState::Failed | ServiceState::Stopped => {
                srv.state = state;
                return Box::new(fut::err(()))
            },
            ServiceState::Stopping(mut task) => {
                let rx = task.wait();
                srv.state = ServiceState::Stopping(task);
                return Box::new(
                    rx.ctxfuture().then(|res, _, _| match res {
                        Ok(_) => fut::ok(()),
                        Err(_) => fut::err(()),
                    }));
            },
            ServiceState::Starting(task) => {
                task.set_result(StartStatus::Stopping);
            }
            ServiceState::Reloading(task) => {
                task.set_result(ReloadStatus::Stopping);
            }
            ServiceState::Running => ()
        }

        // stop workers
        let mut task = Task::new();
        let rx = task.wait();
        srv.paused = false;
        srv.state = ServiceState::Stopping(task);
        for worker in srv.workers.iter_mut() {
            if self.0 {
                worker.stop(self.1.clone());
            } else {
                worker.quit(self.1.clone());
            }
        }
        srv.update();

        Box::new(
            rx.ctxfuture().then(|res, _, _| match res {
                Ok(_) => fut::ok(()),
                Err(_) => fut::err(()),
            }))
    }
}


#[derive(Debug)]
struct Task<T> where T: Clone + std::fmt::Debug {
    waiters: Vec<unsync::oneshot::Sender<T>>,
}

impl<T> Task<T> where T: Clone + std::fmt::Debug {

    fn new() -> Task<T> {
        Task { waiters: Vec::new() }
    }

    fn wait(&mut self) -> unsync::oneshot::Receiver<T> {
        let (tx, rx) = unsync::oneshot::channel();
        self.waiters.push(tx);
        rx
    }

    fn set_result(self, result: T) {
        for waiter in self.waiters {
            let _ = waiter.send(result.clone());
        }
    }
}
