use std::rc::Rc;
use std::collections::HashMap;

use nix::unistd::getpid;
use nix::sys::wait::{waitpid, WaitStatus, WNOHANG};

use actix::prelude::*;
use actix::actors::signal;

use config::Config;
use event::{Reason, ServiceStatus};
use process::ProcessError;
use service::{self, FeService, StartStatus, ReloadStatus, ServiceOperationError};

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
    system: SyncAddress<System>,
    services: HashMap<String, Address<FeService>>,
    stop_waiter: Option<actix::Condition<bool>>,
    stopping: usize,
}

impl CommandCenter {

    pub fn start(cfg: Rc<Config>) -> Address<CommandCenter> {
        CommandCenter {
            cfg: cfg,
            state: State::Starting,
            system: Arbiter::system(),
            services: HashMap::new(),
            stop_waiter: None,
            stopping: 0,
        }.start()
    }

    fn exit(&mut self, success: bool) {
        if let Some(waiter) = self.stop_waiter.take() {
            waiter.set(true);
        }

        if success {
            self.system.send(msgs::SystemExit(0));
        } else {
            self.system.send(msgs::SystemExit(0));
        }
    }

    fn stop(&mut self, ctx: &mut Context<Self>, graceful: bool)
    {
        if self.state != State::Stopping {
            info!("Stopping service");

            self.state = State::Stopping;
            for service in self.services.values() {
                self.stopping += 1;
                service.call(service::Stop(graceful, Reason::Exit))
                    .then(|res, srv: &mut CommandCenter, _: &mut Context<Self>| {
                        srv.stopping -= 1;
                        let exit = srv.stopping == 0;
                        if exit {
                            srv.exit(true);
                        }
                        match res {
                            Ok(_) => fut::ok(()),
                            Err(_) => fut::err(()),
                        }
                    }).spawn(ctx);
            };
        }
    }
}


pub struct ServicePids(pub String);

impl ResponseType<ServicePids> for CommandCenter {
    type Item = Vec<String>;
    type Error = CommandError;
}

impl Handler<ServicePids> for CommandCenter {

    fn handle(&mut self, msg: ServicePids,
              _: &mut Context<CommandCenter>) -> Response<Self, ServicePids>
    {
        match self.state {
            State::Running => {
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.call(service::Pids).then(|res, _, _| match res {
                            Ok(Ok(status)) => fut::ok(status),
                            _ => fut::err(CommandError::UnknownService)
                        }).into(),
                    None => Response::Error(CommandError::UnknownService)
                }
            }
            _ => Response::Error(CommandError::NotReady)
        }
    }
}

pub struct Stop;

impl ResponseType<Stop> for CommandCenter {
    type Item = bool;
    type Error = ();
}

impl Handler<Stop> for CommandCenter {

    fn handle(&mut self, _: Stop, ctx: &mut Context<Self>) -> Response<Self, Stop>
    {
        self.stop(ctx, true);

        if self.stop_waiter.is_none() {
            self.stop_waiter = Some(actix::Condition::default());
        }

        if let Some(ref mut waiter) = self.stop_waiter {
            return
                waiter.wait().actfuture().then(|res, _, _| match res {
                    Ok(res) => fut::result(Ok(res)),
                    Err(_) => fut::result(Err(())),
                }).into()
        } else {
            unreachable!();
        }
    }
}


/// Start Service by `name`
pub struct StartService(pub String);

impl ResponseType<StartService> for CommandCenter {
    type Item = StartStatus;
    type Error = CommandError;
}

impl Handler<StartService> for CommandCenter {

    fn handle(&mut self, msg: StartService,
              _: &mut Context<CommandCenter>) -> Response<Self, StartService>
    {
        match self.state {
            State::Running => {
                info!("Starting service {:?}", msg.0);
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.call(service::Start)
                            .then(|res, _, _| match res {
                                Ok(Ok(status)) => fut::ok(status),
                                Ok(Err(err)) => fut::err(CommandError::Service(err)),
                                Err(_) => fut::err(CommandError::NotReady)
                            }).into(),
                    None => Response::Error(CommandError::UnknownService)
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Response::Error(CommandError::NotReady)
            }
        }
    }
}

/// Stop Service by `name`
pub struct StopService(pub String, pub bool);

impl ResponseType<StopService> for CommandCenter {
    type Item = ();
    type Error = CommandError;
}

impl Handler<StopService> for CommandCenter {

    fn handle(&mut self, msg: StopService,
              _: &mut Context<CommandCenter>) -> Response<Self, StopService>
    {
        match self.state {
            State::Running => {
                info!("Stopping service {:?}", msg.0);
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.call(service::Stop(msg.1, Reason::ConsoleRequest))
                            .then(|res, _, _| match res {
                                Ok(Ok(_)) => fut::ok(()),
                                _ => fut::err(CommandError::ServiceStopped),
                            }).into(),
                    None => Response::Error(CommandError::UnknownService)
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Response::Error(CommandError::NotReady)
            }
        }
    }
}

/// Service status message
pub struct StatusService(pub String);

impl ResponseType<StatusService> for CommandCenter {
    type Item = ServiceStatus;
    type Error = CommandError;
}

impl Handler<StatusService> for CommandCenter {

    fn handle(&mut self, msg: StatusService,
              _: &mut Context<CommandCenter>) -> Response<Self, StatusService>
    {
        match self.state {
            State::Running => {
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.call(service::Status)
                            .then(|res, _, _| match res {
                                Ok(Ok(status)) => fut::ok(status),
                                _ => fut::err(CommandError::UnknownService)
                            }).into(),
                    None => Response::Error(CommandError::UnknownService),
                }
            }
            _ => Response::Error(CommandError::NotReady)
        }
    }
}


/// Pause service message
pub struct PauseService(pub String);

impl ResponseType<PauseService> for CommandCenter {
    type Item = ();
    type Error = CommandError;
}

impl Handler<PauseService> for CommandCenter {

    fn handle(&mut self, msg: PauseService,
              _: &mut Context<CommandCenter>) -> Response<Self, PauseService>
    {
        match self.state {
            State::Running => {
                info!("Pause service {:?}", msg.0);
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.call(service::Pause)
                            .then(|res, _, _| match res {
                                Ok(Ok(_)) => fut::ok(()),
                                Ok(Err(err)) => fut::err(CommandError::Service(err)),
                                Err(_) => fut::err(CommandError::UnknownService)
                            }).into(),
                    None => Response::Error(CommandError::UnknownService)
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Response::Error(CommandError::NotReady)
            }
        }
    }
}

/// Resume service message
pub struct ResumeService(pub String);

impl ResponseType<ResumeService> for CommandCenter {
    type Item = ();
    type Error = CommandError;
}

impl Handler<ResumeService> for CommandCenter {

    fn handle(&mut self, msg: ResumeService,
              _: &mut Context<CommandCenter>) -> Response<Self, ResumeService>
    {
        match self.state {
            State::Running => {
                info!("Resume service {:?}", msg.0);
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.call(service::Resume)
                        .then(|res, _, _| match res {
                            Ok(Ok(_)) => fut::ok(()),
                            Ok(Err(err)) => fut::err(CommandError::Service(err)),
                            Err(_) => fut::err(CommandError::UnknownService)
                        }).into(),
                    None => Response::Error(CommandError::UnknownService)
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Response::Error(CommandError::NotReady)
            }
        }
    }
}

/// Reload service
pub struct ReloadService(pub String, pub bool);

impl ResponseType<ReloadService> for CommandCenter {
    type Item = ReloadStatus;
    type Error = CommandError;
}

impl Handler<ReloadService> for CommandCenter {

    fn handle(&mut self, msg: ReloadService, _: &mut Context<Self>)
              -> Response<Self, ReloadService>
    {
        match self.state {
            State::Running => {
                info!("Reloading service {:?}", msg.0);
                let graceful = msg.1;
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.call(service::Reload(graceful))
                        .then(|res, _, _| match res {
                            Ok(Ok(status)) => fut::ok(status),
                            Ok(Err(err)) => fut::err(CommandError::Service(err)),
                            Err(_) => fut::err(CommandError::UnknownService)
                        }).into(),
                    None => Response::Error(CommandError::UnknownService)
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Response::Error(CommandError::NotReady)
            }
        }
    }
}

/// reload all services
pub struct ReloadAll;

impl ResponseType<ReloadAll> for CommandCenter {
    type Item = ();
    type Error = CommandError;
}

impl Handler<ReloadAll> for CommandCenter {

    fn handle(&mut self, _: ReloadAll, _: &mut Context<Self>) -> Response<Self, ReloadAll>
    {
        match self.state {
            State::Running => {
                info!("reloading all services");
                for srv in self.services.values() {
                    srv.send(service::Reload(true));
                }
            }
            _ => warn!("Can not reload in system in `{:?}` state", self.state)
        };
        Response::Empty()
    }
}

impl ResponseType<signal::Signal> for CommandCenter {
    type Item = ();
    type Error = ();
}

/// Handle ProcessEvent (SIGHUP, SIGINT, etc)
impl Handler<signal::Signal> for CommandCenter {

    fn handle(&mut self, msg: signal::Signal, ctx: &mut Context<Self>)
              -> Response<Self, signal::Signal>
    {
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
                                srv.send(
                                    service::ProcessExited(pid.clone(), err.clone())
                                );
                            }
                            continue
                        }
                        Ok(WaitStatus::Signaled(pid, sig, _)) => {
                            info!("Worker {} exit by signal {:?}", pid, sig);
                            let err = ProcessError::Signal(sig as usize);
                            for srv in self.services.values_mut() {
                                srv.send(
                                    service::ProcessExited(pid.clone(), err.clone())
                                );
                            }
                            continue
                        },
                        Ok(_) => (),
                        Err(_) => (),
                    }
                    break
                }
            }
        };
        Response::Empty()
    }
}


impl Actor for CommandCenter {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>)
    {
        info!("Starting ctl service: {}", getpid());

        // listen for process signals
        Arbiter::system_registry().get::<signal::ProcessSignals>()
            .send(signal::Subscribe(ctx.sync_subscriber()));

        // start services
        for cfg in self.cfg.services.iter() {
            let service = FeService::start(cfg.num, cfg.clone());
            self.services.insert(cfg.name.clone(), service);
        }
        self.state = State::Running;
    }

    fn stopping(&mut self, _: &mut Context<Self>)
    {
        self.exit(true);
    }
}
