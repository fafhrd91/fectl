use std::collections::HashMap;
use std::rc::Rc;

use nix::sys::wait::{waitpid, WaitStatus, WNOHANG};
use nix::unistd::getpid;

use actix::actors::signal;
use actix::prelude::*;
use actix::Response;
use futures::Future;

use config::Config;
use event::{Reason, ServiceStatus};
use process::ProcessError;
use service::{self, FeService, ReloadStatus, ServiceOperationError, StartStatus};

#[derive(Debug)]
/// Command center errors
pub enum CommandError {
    /// command center is not in Running state
    NotReady,
    /// service is not known
    UnknownService,
    /// service is stopped
    ServiceStopped,
    /// underlying service error
    Service(ServiceOperationError),
}

#[derive(PartialEq, Debug)]
enum State {
    Starting,
    Running,
    Stopping,
}

pub struct CommandCenter {
    cfg: Rc<Config>,
    state: State,
    services: HashMap<String, Addr<FeService>>,
    stop_waiter: Option<actix::Condition<bool>>,
    stopping: usize,
}

impl CommandCenter {
    pub fn start(cfg: Rc<Config>) -> Addr<CommandCenter> {
        CommandCenter {
            cfg,
            state: State::Starting,
            services: HashMap::new(),
            stop_waiter: None,
            stopping: 0,
        }.start()
    }

    fn exit(&mut self) {
        if let Some(waiter) = self.stop_waiter.take() {
            waiter.set(true);
        }

        System::current().stop();
    }

    fn stop(&mut self, ctx: &mut Context<Self>, graceful: bool) {
        if self.state != State::Stopping {
            info!("Stopping service");

            self.state = State::Stopping;
            for service in self.services.values() {
                self.stopping += 1;
                service
                    .send(service::Stop(graceful, Reason::Exit))
                    .into_actor(self)
                    .then(|res, srv, _| {
                        srv.stopping -= 1;
                        let exit = srv.stopping == 0;
                        if exit {
                            srv.exit();
                        }
                        match res {
                            Ok(_) => actix::fut::ok(()),
                            Err(_) => actix::fut::err(()),
                        }
                    }).spawn(ctx);
            }
        }
    }
}

pub struct ServicePids(pub String);

impl Message for ServicePids {
    type Result = Result<Vec<String>, CommandError>;
}

impl Handler<ServicePids> for CommandCenter {
    type Result = Response<Vec<String>, CommandError>;

    fn handle(
        &mut self, msg: ServicePids, _: &mut Context<CommandCenter>,
    ) -> Self::Result {
        match self.state {
            State::Running => match self.services.get(&msg.0) {
                Some(service) => Response::async(
                    service
                        .send(service::Pids)
                        .map_err(|_| CommandError::UnknownService),
                ),
                None => Response::reply(Err(CommandError::UnknownService)),
            },
            _ => Response::reply(Err(CommandError::NotReady)),
        }
    }
}

#[derive(Message)]
#[rtype(result = "Result<bool, ()>")]
pub struct Stop;

impl Handler<Stop> for CommandCenter {
    type Result = Response<bool, ()>;

    fn handle(&mut self, _: Stop, ctx: &mut Context<Self>) -> Self::Result {
        self.stop(ctx, true);

        if self.stop_waiter.is_none() {
            self.stop_waiter = Some(actix::Condition::default());
        }

        if let Some(ref mut waiter) = self.stop_waiter {
            Response::async(waiter.wait().map_err(|_| ()))
        } else {
            unreachable!();
        }
    }
}

/// Start Service by `name`
pub struct StartService(pub String);

impl Message for StartService {
    type Result = Result<StartStatus, CommandError>;
}

impl Handler<StartService> for CommandCenter {
    type Result = Response<StartStatus, CommandError>;

    fn handle(
        &mut self, msg: StartService, _: &mut Context<CommandCenter>,
    ) -> Self::Result {
        match self.state {
            State::Running => {
                info!("Starting service {:?}", msg.0);
                match self.services.get(&msg.0) {
                    Some(service) => Response::async(service.send(service::Start).then(
                        |res| match res {
                            Ok(Ok(status)) => Ok(status),
                            Ok(Err(err)) => Err(CommandError::Service(err)),
                            Err(_) => Err(CommandError::NotReady),
                        },
                    )),
                    None => Response::reply(Err(CommandError::UnknownService)),
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Response::reply(Err(CommandError::NotReady))
            }
        }
    }
}

/// Stop Service by `name`
pub struct StopService(pub String, pub bool);

impl Message for StopService {
    type Result = Result<(), CommandError>;
}

impl Handler<StopService> for CommandCenter {
    type Result = Response<(), CommandError>;

    fn handle(
        &mut self, msg: StopService, _: &mut Context<CommandCenter>,
    ) -> Self::Result {
        match self.state {
            State::Running => {
                info!("Stopping service {:?}", msg.0);
                match self.services.get(&msg.0) {
                    Some(service) => Response::async(
                        service
                            .send(service::Stop(msg.1, Reason::ConsoleRequest))
                            .then(|res| match res {
                                Ok(Ok(_)) => Ok(()),
                                _ => Err(CommandError::ServiceStopped),
                            }),
                    ),
                    None => Response::reply(Err(CommandError::UnknownService)),
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Response::reply(Err(CommandError::NotReady))
            }
        }
    }
}

/// Service status message
pub struct StatusService(pub String);

impl Message for StatusService {
    type Result = Result<ServiceStatus, CommandError>;
}

impl Handler<StatusService> for CommandCenter {
    type Result = Response<ServiceStatus, CommandError>;

    fn handle(
        &mut self, msg: StatusService, _: &mut Context<CommandCenter>,
    ) -> Self::Result {
        match self.state {
            State::Running => match self.services.get(&msg.0) {
                Some(service) => Response::async(service.send(service::Status).then(
                    |res| match res {
                        Ok(Ok(status)) => Ok(status),
                        _ => Err(CommandError::UnknownService),
                    },
                )),
                None => Response::reply(Err(CommandError::UnknownService)),
            },
            _ => Response::reply(Err(CommandError::NotReady)),
        }
    }
}

/// Pause service message
pub struct PauseService(pub String);

impl Message for PauseService {
    type Result = Result<(), CommandError>;
}

impl Handler<PauseService> for CommandCenter {
    type Result = Response<(), CommandError>;

    fn handle(
        &mut self, msg: PauseService, _: &mut Context<CommandCenter>,
    ) -> Self::Result {
        match self.state {
            State::Running => {
                info!("Pause service {:?}", msg.0);
                match self.services.get(&msg.0) {
                    Some(service) => Response::async(service.send(service::Pause).then(
                        |res| match res {
                            Ok(Ok(_)) => Ok(()),
                            Ok(Err(err)) => Err(CommandError::Service(err)),
                            Err(_) => Err(CommandError::UnknownService),
                        },
                    )),
                    None => Response::reply(Err(CommandError::UnknownService)),
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Response::reply(Err(CommandError::NotReady))
            }
        }
    }
}

/// Resume service message
pub struct ResumeService(pub String);

impl Message for ResumeService {
    type Result = Result<(), CommandError>;
}

impl Handler<ResumeService> for CommandCenter {
    type Result = Response<(), CommandError>;

    fn handle(
        &mut self, msg: ResumeService, _: &mut Context<CommandCenter>,
    ) -> Self::Result {
        match self.state {
            State::Running => {
                info!("Resume service {:?}", msg.0);
                match self.services.get(&msg.0) {
                    Some(service) => {
                        Response::async(service.send(service::Resume).then(|res| {
                            match res {
                                Ok(Ok(_)) => Ok(()),
                                Ok(Err(err)) => Err(CommandError::Service(err)),
                                Err(_) => Err(CommandError::UnknownService),
                            }
                        }))
                    }
                    None => Response::reply(Err(CommandError::UnknownService)),
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Response::reply(Err(CommandError::NotReady))
            }
        }
    }
}

/// Reload service
pub struct ReloadService(pub String, pub bool);

impl Message for ReloadService {
    type Result = Result<ReloadStatus, CommandError>;
}

impl Handler<ReloadService> for CommandCenter {
    type Result = Response<ReloadStatus, CommandError>;

    fn handle(&mut self, msg: ReloadService, _: &mut Context<Self>) -> Self::Result {
        match self.state {
            State::Running => {
                info!("Reloading service {:?}", msg.0);
                let graceful = msg.1;
                match self.services.get(&msg.0) {
                    Some(service) => {
                        Response::async(service.send(service::Reload(graceful)).then(
                            |res| match res {
                                Ok(Ok(status)) => Ok(status),
                                Ok(Err(err)) => Err(CommandError::Service(err)),
                                Err(_) => Err(CommandError::UnknownService),
                            },
                        ))
                    }
                    None => Response::reply(Err(CommandError::UnknownService)),
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Response::reply(Err(CommandError::NotReady))
            }
        }
    }
}

/// reload all services
pub struct ReloadAll;

impl Message for ReloadAll {
    type Result = ();
}

impl Handler<ReloadAll> for CommandCenter {
    type Result = ();

    fn handle(&mut self, _: ReloadAll, _: &mut Context<Self>) -> Self::Result {
        match self.state {
            State::Running => {
                info!("reloading all services");
                for srv in self.services.values() {
                    srv.do_send(service::Reload(true));
                }
            }
            _ => warn!("Can not reload in system in `{:?}` state", self.state),
        }
    }
}

/// Handle ProcessEvent (SIGHUP, SIGINT, etc)
impl Handler<signal::Signal> for CommandCenter {
    type Result = ();

    fn handle(&mut self, msg: signal::Signal, ctx: &mut Context<Self>) {
        match msg.0 {
            signal::SignalType::Int => {
                info!("SIGINT received, exiting");
                self.stop(ctx, false);
            }
            signal::SignalType::Hup => {
                info!("SIGHUP received, reloading");
                // self.handle(ReloadAll, ctx);
            }
            signal::SignalType::Term => {
                info!("SIGTERM received, stopping");
                self.stop(ctx, true);
            }
            signal::SignalType::Quit => {
                info!("SIGQUIT received, exiting");
                self.stop(ctx, false);
            }
            signal::SignalType::Child => {
                info!("SIGCHLD received");
                debug!("Reap workers");
                loop {
                    match waitpid(None, Some(WNOHANG)) {
                        Ok(WaitStatus::Exited(pid, code)) => {
                            info!("Worker {} exit code: {}", pid, code);
                            let err = ProcessError::from(code);
                            for srv in self.services.values_mut() {
                                srv.do_send(service::ProcessExited(pid, err.clone()));
                            }
                            continue;
                        }
                        Ok(WaitStatus::Signaled(pid, sig, _)) => {
                            info!("Worker {} exit by signal {:?}", pid, sig);
                            let err = ProcessError::Signal(sig as usize);
                            for srv in self.services.values_mut() {
                                srv.do_send(service::ProcessExited(pid, err.clone()));
                            }
                            continue;
                        }
                        Ok(_) => (),
                        Err(_) => (),
                    }
                    break;
                }
            }
        }
    }
}

impl Actor for CommandCenter {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        info!("Starting ctl service: {}", getpid());

        // listen for process signals
        let addr = ctx.address();
        System::current()
            .registry()
            .get::<signal::ProcessSignals>()
            .do_send(signal::Subscribe(addr.recipient()));

        // start services
        for cfg in &self.cfg.services {
            let service = FeService::start(cfg.num, cfg.clone());
            self.services.insert(cfg.name.clone(), service);
        }
        self.state = State::Running;
    }

    fn stopping(&mut self, _: &mut Context<Self>) -> Running {
        self.exit();
        Running::Stop
    }
}
