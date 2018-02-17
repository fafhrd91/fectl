#![allow(dead_code)]

use std;
use std::time::Duration;
use nix::unistd::Pid;

use actix::prelude::*;
use actix::Response;
use futures::Future;

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

    pub fn start(num: u16, cfg: ServiceConfig) -> Addr<Unsync, FeService>
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

#[derive(Message)]
pub struct ProcessMessage(pub usize, pub Pid, pub WorkerMessage);

impl Handler<ProcessMessage> for FeService {
    type Result = ();

    fn handle(&mut self, msg: ProcessMessage, _: &mut Context<Self>) {
        self.workers[msg.0].message(msg.1, &msg.2);
        self.update();
    }
}

#[derive(Message)]
pub struct ProcessFailed(pub usize, pub Pid, pub ProcessError);

impl Handler<ProcessFailed> for FeService {
    type Result = ();

    fn handle(&mut self, msg: ProcessFailed, ctx: &mut Context<Self>) {
        // TODO: delay failure processing, needs better approach
        ctx.run_later(Duration::new(5, 0), move |act, _| {
            act.workers[msg.0].exited(msg.1, &msg.2);
            act.update();
        });
    }
}

#[derive(Message)]
pub struct ProcessLoaded(pub usize, pub Pid);

impl Handler<ProcessLoaded> for FeService {
    type Result = ();

    fn handle(&mut self, msg: ProcessLoaded, _: &mut Context<Self>) {
        self.workers[msg.0].loaded(msg.1);
        self.update();
    }
}

#[derive(Message)]
pub struct ProcessExited(pub Pid, pub ProcessError);

impl Handler<ProcessExited> for FeService {
    type Result = ();

    fn handle(&mut self, msg: ProcessExited, _: &mut Context<Self>) {
        for worker in self.workers.iter_mut() {
            worker.exited(msg.0, &msg.1);
        }
        self.update();
    }
}

/// Service status command
pub struct Pids;

impl Message for Pids {
    type Result = Vec<String>;
}

impl Handler<Pids> for FeService {
    type Result = MessageResult<Pids>;

    fn handle(&mut self, _: Pids, _: &mut Context<Self>) -> Self::Result {
        let mut pids = Vec::new();
        for worker in self.workers.iter() {
            if let Some(pid) = worker.pid() {
                pids.push(format!("{}", pid));
            }
        }
        MessageResult(pids)
    }
}

/// Service status command
pub struct Status;

impl Message for Status {
    type Result = Result<(String, Vec<(String, Vec<Event>)>), ()>;
}

impl Handler<Status> for FeService {
    type Result = Result<(String, Vec<(String, Vec<Event>)>), ()>;

    fn handle(&mut self, _: Status, _: &mut Context<Self>) -> Self::Result {
        let mut events: Vec<(String, Vec<Event>)> = Vec::new();
        for worker in self.workers.iter() {
            events.push(
                (format!("worker({})", worker.idx + 1), Vec::from(&worker.events)));
        }

        let status = match self.state {
            ServiceState::Running => if self.paused { "paused" } else { "running" }
            _ => self.state.description()
        };
        Ok((status.to_owned(), events))
    }
}

/// Start service command
pub struct Start;

impl Message for Start {
    type Result = Result<StartStatus, ServiceOperationError>;
}

impl Handler<Start> for FeService {
    type Result = Response<StartStatus, ServiceOperationError>;

    fn handle(&mut self, _: Start, _: &mut Context<Self>) -> Self::Result
    {
        match self.state {
            ServiceState::Starting(ref mut task) => {
                Response::async(task.wait().map_err(|_| ServiceOperationError::Failed))
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
                Response::async(rx.map_err(|_| ServiceOperationError::Failed))
            }
            _ => Response::reply(Err(self.state.error()))
        }
    }
}

/// Pause service command
pub struct Pause;

impl Message for Pause {
    type Result = Result<(), ServiceOperationError>;
}

impl Handler<Pause> for FeService {
    type Result = Result<(), ServiceOperationError>;

    fn handle(&mut self, _: Pause, _: &mut Context<Self>) -> Self::Result
    {
        match self.state {
            ServiceState::Running => {
                debug!("Pause service: {:?}", self.name);
                for worker in self.workers.iter_mut() {
                    worker.pause(Reason::ConsoleRequest);
                }
                self.paused = true;
                Ok(())
            }
            _ => Err(self.state.error())
        }
    }
}

/// Resume service command
pub struct Resume;

impl Message for Resume {
    type Result = Result<(), ServiceOperationError>;
}

impl Handler<Resume> for FeService {
    type Result = Result<(), ServiceOperationError>;

    fn handle(&mut self, _: Resume, _: &mut Context<Self>) -> Self::Result {
        match self.state {
            ServiceState::Running => {
                debug!("Resume service: {:?}", self.name);
                for worker in self.workers.iter_mut() {
                    worker.resume(Reason::ConsoleRequest);
                }
                self.paused = false;
                Ok(())
            }
            _ => Err(self.state.error())
        }
    }
}

/// Reload service
pub struct Reload(pub bool);

impl Message for Reload {
    type Result = Result<ReloadStatus, ServiceOperationError>;
}

impl Handler<Reload> for FeService {
    type Result = Response<ReloadStatus, ServiceOperationError>;

    fn handle(&mut self, msg: Reload, _: &mut Context<Self>) -> Self::Result {
        match self.state {
            ServiceState::Reloading(ref mut task) => {
                Response::async(task.wait().map_err(|_| ServiceOperationError::Failed))
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
                Response::async(rx.map_err(|_| ServiceOperationError::Failed))
            }
            _ => Response::reply(Err(self.state.error()))
        }
    }
}

/// Stop service command
pub struct Stop(pub bool, pub Reason);

impl Message for Stop {
    type Result = Result<(), ()>;
}

impl Handler<Stop> for FeService {
    type Result = Response<(), ()>;

    fn handle(&mut self, msg: Stop, _: &mut Context<Self>) -> Self::Result {
        let state = std::mem::replace(&mut self.state, ServiceState::Stopped);

        match state {
            ServiceState::Failed | ServiceState::Stopped => {
                self.state = state;
                return Response::reply(Err(()))
            },
            ServiceState::Stopping(mut task) => {
                let rx = task.wait();
                self.state = ServiceState::Stopping(task);
                return Response::async(rx.map(|_| ()).map_err(|_| ()));
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

        Response::async(rx.map(|_| ()).map_err(|_| ()))
    }
}
