use std::rc::Rc;
use std::collections::HashMap;

use nix::unistd::getpid;
use nix::sys::wait::{waitpid, WaitStatus, WNOHANG};

use ctx::prelude::*;

use signals::{ProcessEvent, ProcessEventType};
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
    system: Address<ctx::System>,
    services: HashMap<String, Address<FeService>>,
    stop_waiter: Option<ctx::Waiter<bool>>,
    stopping: usize,
}

impl CommandCenter {

    pub fn start(cfg: Rc<Config>) -> Address<CommandCenter> {
        CommandCenter {
            cfg: cfg,
            state: State::Starting,
            system: ctx::get_system(),
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
            self.system.tell(ctx::SystemExit(0));
        } else {
            self.system.tell(ctx::SystemExit(0));
        }
    }

    fn stop(&mut self, ctx: &mut Context<Self>, graceful: bool)
    {
        if self.state != State::Stopping {
            info!("Stopping service");

            self.state = State::Stopping;
            for service in self.services.values() {
                self.stopping += 1;
                service.send(service::Stop(graceful, Reason::Exit))
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

impl Message for ServicePids {
    type Item = Vec<String>;
    type Error = CommandError;
}

impl MessageHandler<ServicePids> for CommandCenter {

    fn handle(&mut self, msg: ServicePids,
              _: &mut Context<CommandCenter>) -> MessageFuture<ServicePids, Self>
    {
        match self.state {
            State::Running => {
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.send(service::Pids).then(|res, _, _| match res {
                            Ok(Ok(status)) => fut::ok(status),
                            _ => fut::err(CommandError::UnknownService)
                        }).into(),
                    None => CommandError::UnknownService.to_error()
                }
            }
            _ => CommandError::NotReady.to_error()
        }
    }
}

pub struct Stop;

impl Message for Stop {
    type Item = bool;
    type Error = ();
}

impl MessageHandler<Stop> for CommandCenter {

    fn handle(&mut self, _: Stop, ctx: &mut Context<Self>) -> MessageFuture<Stop, Self>
    {
        self.stop(ctx, true);

        if self.stop_waiter.is_none() {
            self.stop_waiter = Some(ctx::Waiter::new());
        }

        if let Some(ref mut waiter) = self.stop_waiter {
            return
                waiter.wait().ctxfuture().then(|res, _, _| match res {
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

impl Message for StartService {
    type Item = StartStatus;
    type Error = CommandError;
}

impl MessageHandler<StartService> for CommandCenter {

    fn handle(&mut self, msg: StartService,
              _: &mut Context<CommandCenter>) -> MessageFuture<StartService, Self>
    {
        match self.state {
            State::Running => {
                info!("Starting service {:?}", msg.0);
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.send(service::Start)
                            .then(|res, _, _| match res {
                                Ok(Ok(status)) => fut::ok(status),
                                Ok(Err(err)) => fut::err(CommandError::Service(err)),
                                Err(_) => fut::err(CommandError::NotReady)
                            }).into(),
                    None => CommandError::UnknownService.to_error()
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                CommandError::NotReady.to_error()
            }
        }
    }
}

/// Stop Service by `name`
pub struct StopService(pub String, pub bool);

impl Message for StopService {
    type Item = ();
    type Error = CommandError;
}

impl MessageHandler<StopService> for CommandCenter {

    fn handle(&mut self, msg: StopService,
              _: &mut Context<CommandCenter>) -> MessageFuture<StopService, Self>
    {
        match self.state {
            State::Running => {
                info!("Stopping service {:?}", msg.0);
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.send(service::Stop(msg.1, Reason::ConsoleRequest))
                            .then(|res, _, _| match res {
                                Ok(Ok(_)) => fut::ok(()),
                                _ => fut::err(CommandError::ServiceStopped),
                            }).into(),
                    None => CommandError::UnknownService.to_error()
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                CommandError::NotReady.to_error()
            }
        }
    }
}

/// Service status message
pub struct StatusService(pub String);

impl Message for StatusService {
    type Item = ServiceStatus;
    type Error = CommandError;
}

impl MessageHandler<StatusService> for CommandCenter {

    fn handle(&mut self, msg: StatusService,
              _: &mut Context<CommandCenter>) -> MessageFuture<StatusService, Self>
    {
        match self.state {
            State::Running => {
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.send(service::Status)
                            .then(|res, _, _| match res {
                                Ok(Ok(status)) => fut::ok(status),
                                _ => fut::err(CommandError::UnknownService)
                            }).into(),
                    None => CommandError::UnknownService.to_error(),
                }
            }
            _ => CommandError::NotReady.to_error()
        }
    }
}


/// Pause service message
pub struct PauseService(pub String);

impl Message for PauseService {
    type Item = ();
    type Error = CommandError;
}

impl MessageHandler<PauseService> for CommandCenter {

    fn handle(&mut self, msg: PauseService,
              _: &mut Context<CommandCenter>) -> MessageFuture<PauseService, Self>
    {
        match self.state {
            State::Running => {
                info!("Pause service {:?}", msg.0);
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.send(service::Pause)
                            .then(|res, _, _| match res {
                                Ok(Ok(_)) => fut::ok(()),
                                Ok(Err(err)) => fut::err(CommandError::Service(err)),
                                Err(_) => fut::err(CommandError::UnknownService)
                            }).into(),
                    None => CommandError::UnknownService.to_error()
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                CommandError::NotReady.to_error()
            }
        }
    }
}

/// Resume service message
pub struct ResumeService(pub String);

impl Message for ResumeService {
    type Item = ();
    type Error = CommandError;
}

impl MessageHandler<ResumeService> for CommandCenter {

    fn handle(&mut self, msg: ResumeService,
              _: &mut Context<CommandCenter>) -> MessageFuture<ResumeService, Self>
    {
        match self.state {
            State::Running => {
                info!("Resume service {:?}", msg.0);
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.send(service::Resume)
                        .then(|res, _, _| match res {
                            Ok(Ok(_)) => fut::ok(()),
                            Ok(Err(err)) => fut::err(CommandError::Service(err)),
                            Err(_) => fut::err(CommandError::UnknownService)
                        }).into(),
                    None => CommandError::UnknownService.to_error()
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                CommandError::NotReady.to_error()
            }
        }
    }
}

/// Reload service
pub struct ReloadService(pub String, pub bool);

impl Message for ReloadService {
    type Item = ReloadStatus;
    type Error = CommandError;
}

impl MessageHandler<ReloadService> for CommandCenter {

    fn handle(&mut self, msg: ReloadService, _: &mut Context<Self>)
              -> MessageFuture<ReloadService, Self>
    {
        match self.state {
            State::Running => {
                info!("Reloading service {:?}", msg.0);
                let graceful = msg.1;
                match self.services.get(&msg.0) {
                    Some(service) =>
                        service.send(service::Reload(graceful))
                        .then(|res, _, _| match res {
                            Ok(Ok(status)) => fut::ok(status),
                            Ok(Err(err)) => fut::err(CommandError::Service(err)),
                            Err(_) => fut::err(CommandError::UnknownService)
                        }).into(),
                    None => CommandError::UnknownService.to_error()
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                CommandError::NotReady.to_error()
            }
        }
    }
}

/// reload all services
pub struct ReloadAll;

impl Message for ReloadAll {
    type Item = ();
    type Error = CommandError;
}

impl MessageHandler<ReloadAll> for CommandCenter {

    fn handle(&mut self, _: ReloadAll, _: &mut Context<Self>) -> MessageFuture<ReloadAll, Self>
    {
        match self.state {
            State::Running => {
                info!("reloading all services");
                for srv in self.services.values() {
                    srv.tell(
                        service::Reload(true)
                    );
                }
            }
            _ => warn!("Can not reload in system in `{:?}` state", self.state)
        };
        ().to_result()
    }
}

/// Handle ProcessEvent (SIGHUP, SIGINT, etc)
impl MessageHandler<ProcessEvent> for CommandCenter {

    fn handle(&mut self, msg: ProcessEvent, ctx: &mut Context<Self>)
              -> MessageFuture<ProcessEvent, Self>
    {
        match msg.0 {
            ProcessEventType::Int => {
                info!("SIGINT received, exiting");
                self.stop(ctx, false);
            }
            ProcessEventType::Hup => {
                info!("SIGHUP received, reloading");
                // self.handle(ReloadAll, ctx);
            }
            ProcessEventType::Term => {
                info!("SIGTERM received, stopping");
                self.stop(ctx, true);
            }
            ProcessEventType::Quit => {
                info!("SIGQUIT received, exiting");
                self.stop(ctx, false);
            }
            ProcessEventType::Child => {
                info!("SIGCHLD received");
                debug!("Reap workers");
                loop {
                    match waitpid(None, Some(WNOHANG)) {
                        Ok(WaitStatus::Exited(pid, code)) => {
                            info!("Worker {} exit code: {}", pid, code);
                            let err = ProcessError::from(code);
                            for srv in self.services.values_mut() {
                                srv.tell(
                                    service::ProcessExited(pid.clone(), err.clone())
                                );
                            }
                            continue
                        }
                        Ok(WaitStatus::Signaled(pid, sig, _)) => {
                            info!("Worker {} exit by signal {:?}", pid, sig);
                            let err = ProcessError::Signal(sig as usize);
                            for srv in self.services.values_mut() {
                                srv.tell(
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
        ().to_result()
    }
}


impl Service for CommandCenter {

    type Message = DefaultMessage;

    fn start(&mut self, _: &mut Context<Self>)
    {
        info!("Starting ctl service: {}", getpid());

        // start services
        for cfg in self.cfg.services.iter() {
            let service = FeService::start(cfg.num, cfg.clone());
            self.services.insert(cfg.name.clone(), service);
        }
        self.state = State::Running;
    }

    fn finished(&mut self, _: &mut Context<Self>) -> ServiceResult
    {
        self.exit(true);

        ServiceResult::Done
    }
}
