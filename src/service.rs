#![allow(dead_code)]

use std;
use nix::unistd::Pid;

use actix::prelude::*;

use event::{Event, Reason};
use config::ServiceConfig;
use worker::{Worker, WorkerMessage};
use process::ProcessError;

/// Service state
enum ServiceState {
    Running,
    Failed,
    Stopped,
    Starting(actix::Condition<StartStatus>),
    Reloading(actix::Condition<ReloadStatus>),
    Stopping(actix::Condition<()>),
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
}

impl FeService {

    pub fn start(num: u16, cfg: ServiceConfig) -> Address<FeService>
    {
        FeService::create(move |ctx| {
            // create4 workers
            let mut workers = Vec::new();
            for idx in 0..num as usize {
                workers.push(Worker::new(idx, cfg.clone(), ctx.address()));
            }

            FeService {
                name: cfg.name.clone(),
                state: ServiceState::Starting(actix::Condition::default()),
                paused: false,
                workers: workers}
        })
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
                        task.set(StartStatus::Failed);
                        self.state = ServiceState::Failed;
                    }
                } else {
                    if !in_process {
                        task.set(StartStatus::Success);
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
                        task.set(ReloadStatus::Failed);
                        self.state = ServiceState::Failed;
                    }
                } else {
                    if !in_process {
                        task.set(ReloadStatus::Success);
                        self.state = ServiceState::Running;
                    } else {
                        self.state = ServiceState::Reloading(task);
                    }
                }
            },
            ServiceState::Stopping(task) => {
                let (_, in_process) = self.check_loading_workers(false);

                if !in_process {
                    task.set(());
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


impl Actor for FeService {

    type Context = Context<Self>;

    fn started(&mut self, _: &mut Context<Self>) {
        // start workers
        for worker in self.workers.iter_mut() {
            worker.start(Reason::Initial);
        }
    }
}

pub struct ProcessMessage(pub usize, pub Pid, pub WorkerMessage);

impl ResponseType<ProcessMessage> for FeService {
    type Item = ();
    type Error = ();
}

impl Handler<ProcessMessage> for FeService {

    fn handle(&mut self, msg: ProcessMessage, _: &mut Context<Self>)
              -> Response<Self, ProcessMessage>
    {
        self.workers[msg.0].message(msg.1, &msg.2);
        self.update();
        Self::empty()
    }
}

pub struct ProcessFailed(pub usize, pub Pid, pub ProcessError);

impl ResponseType<ProcessFailed> for FeService {
    type Item = ();
    type Error = ();
}

impl Handler<ProcessFailed> for FeService {

    fn handle(&mut self, msg: ProcessFailed, _: &mut Context<Self>)
              -> Response<Self, ProcessFailed>
    {
        self.workers[msg.0].exited(msg.1, &msg.2);
        self.update();
        Self::empty()
    }
}

pub struct ProcessLoaded(pub usize, pub Pid);

impl ResponseType<ProcessLoaded> for FeService {
    type Item = ();
    type Error = ();
}

impl Handler<ProcessLoaded> for FeService {

    fn handle(&mut self, msg: ProcessLoaded, _: &mut Context<Self>)
              -> Response<Self, ProcessLoaded>
    {
        self.workers[msg.0].loaded(msg.1);
        self.update();
        Self::empty()
    }
}

pub struct ProcessExited(pub Pid, pub ProcessError);

impl ResponseType<ProcessExited> for FeService {
    type Item = ();
    type Error = ();
}

impl Handler<ProcessExited> for FeService {

    fn handle(&mut self, msg: ProcessExited, _: &mut Context<Self>)
              -> Response<Self, ProcessExited>
    {
        for worker in self.workers.iter_mut() {
            worker.exited(msg.0, &msg.1);
        }
        self.update();
        Self::empty()
    }
}

/// Service status command
pub struct Pids;

impl ResponseType<Pids> for FeService {
    type Item = Vec<String>;
    type Error = ();
}

impl Handler<Pids> for FeService {

    fn handle(&mut self, _: Pids, _: &mut Context<Self>) -> Response<Self, Pids>
    {
        let mut pids = Vec::new();
        for worker in self.workers.iter() {
            if let Some(pid) = worker.pid() {
                pids.push(format!("{}", pid));
            }
        }
        Self::reply(pids)
    }
}

/// Service status command
pub struct Status;

impl ResponseType<Status> for FeService {
    type Item = (String, Vec<(String, Vec<Event>)>);
    type Error = ();
}

impl Handler<Status> for FeService {

    fn handle(&mut self, _: Status, _: &mut Context<Self>) -> Response<Self, Status>
    {
        let mut events: Vec<(String, Vec<Event>)> = Vec::new();
        for worker in self.workers.iter() {
            events.push(
                (format!("worker({})", worker.idx + 1), Vec::from(&worker.events)));
        }

        let status = match self.state {
            ServiceState::Running => if self.paused { "paused" } else { "running" }
            _ => self.state.description()
        };
        Self::reply((status.to_owned(), events))
    }
}

/// Start service command
pub struct Start;

impl ResponseType<Start> for FeService {
    type Item = StartStatus;
    type Error = ServiceOperationError;
}

impl Handler<Start> for FeService {

    fn handle(&mut self, _: Start, _: &mut Context<Self>) -> Response<Self, Start>
    {
        match self.state {
            ServiceState::Starting(ref mut task) => {
                task.wait().actfuture().then(|res, _, _| match res {
                    Ok(res) => fut::result(Ok(res)),
                    Err(_) => fut::result(Err(ServiceOperationError::Failed)),
                }).into()
            }
            ServiceState::Failed | ServiceState::Stopped => {
                debug!("Starting service: {:?}", self.name);
                let mut task = actix::Condition::default();
                let rx = task.wait();
                self.paused = false;
                self.state = ServiceState::Starting(task);
                for worker in self.workers.iter_mut() {
                    worker.start(Reason::ConsoleRequest);
                }
                rx.actfuture().then(|res, _, _| match res {
                    Ok(res) => fut::result(Ok(res)),
                    Err(_) => fut::result(Err(ServiceOperationError::Failed)),
                }).into()
            }
            _ => Self::reply_error(self.state.error())
        }
    }
}

/// Pause service command
pub struct Pause;

impl ResponseType<Pause> for FeService {
    type Item = ();
    type Error = ServiceOperationError;
}

impl Handler<Pause> for FeService {

    fn handle(&mut self, _: Pause, _: &mut Context<Self>) -> Response<Self, Pause>
    {
        match self.state {
            ServiceState::Running => {
                debug!("Pause service: {:?}", self.name);
                for worker in self.workers.iter_mut() {
                    worker.pause(Reason::ConsoleRequest);
                }
                self.paused = true;
                Self::empty()
            }
            _ => Self::reply_error(self.state.error())
        }
    }
}

/// Resume service command
pub struct Resume;

impl ResponseType<Resume> for FeService {
    type Item = ();
    type Error = ServiceOperationError;
}

impl Handler<Resume> for FeService {

    fn handle(&mut self, _: Resume, _: &mut Context<Self>) -> Response<Self, Resume>
    {
        match self.state {
            ServiceState::Running => {
                debug!("Resume service: {:?}", self.name);
                for worker in self.workers.iter_mut() {
                    worker.resume(Reason::ConsoleRequest);
                }
                self.paused = false;
                Self::empty()
            }
            _ => Self::reply_error(self.state.error())
        }
    }
}

/// Reload service
pub struct Reload(pub bool);

impl ResponseType<Reload> for FeService {
    type Item = ReloadStatus;
    type Error = ServiceOperationError;
}

impl Handler<Reload> for FeService {

    fn handle(&mut self, msg: Reload, _: &mut Context<Self>) -> Response<Self, Reload>
    {
        match self.state {
            ServiceState::Reloading(ref mut task) => {
                task.wait().actfuture().then(|res, _, _| match res {
                    Ok(res) => fut::result(Ok(res)),
                    Err(_) => fut::result(Err(ServiceOperationError::Failed)),
                }).into()
            }
            ServiceState::Running | ServiceState::Failed | ServiceState::Stopped => {
                debug!("Reloading service: {:?}", self.name);
                let mut task = actix::Condition::default();
                let rx = task.wait();
                self.paused = false;
                self.state = ServiceState::Reloading(task);
                for worker in self.workers.iter_mut() {
                    worker.reload(msg.0, Reason::ConsoleRequest);
                }
                rx.actfuture().then(|res, _, _| match res {
                    Ok(res) => fut::result(Ok(res)),
                    Err(_) => fut::result(Err(ServiceOperationError::Failed)),
                }).into()
            }
            _ => Self::reply_error(self.state.error())
        }
    }
}

/// Stop service command
pub struct Stop(pub bool, pub Reason);

impl ResponseType<Stop> for FeService {
    type Item = ();
    type Error = ();
}

impl Handler<Stop> for FeService {

    fn handle(&mut self, msg: Stop, _: &mut Context<Self>) -> Response<Self, Stop>
    {
        let state = std::mem::replace(&mut self.state, ServiceState::Stopped);

        match state {
            ServiceState::Failed | ServiceState::Stopped => {
                self.state = state;
                return Self::reply_error(())
            },
            ServiceState::Stopping(mut task) => {
                let rx = task.wait();
                self.state = ServiceState::Stopping(task);
                return
                    rx.actfuture().then(|res, _, _| match res {
                        Ok(_) => fut::ok(()),
                        Err(_) => fut::err(()),
                    }).into();
            },
            ServiceState::Starting(task) => {
                task.set(StartStatus::Stopping);
            }
            ServiceState::Reloading(task) => {
                task.set(ReloadStatus::Stopping);
            }
            ServiceState::Running => ()
        }

        // stop workers
        let mut task = actix::Condition::default();
        let rx = task.wait();
        self.paused = false;
        self.state = ServiceState::Stopping(task);
        for worker in self.workers.iter_mut() {
            if msg.0 {
                worker.stop(msg.1.clone());
            } else {
                worker.quit(msg.1.clone());
            }
        }
        self.update();

        rx.actfuture().then(|res, _, _| match res {
            Ok(_) => fut::ok(()),
            Err(_) => fut::err(()),
        }).into()
    }
}
