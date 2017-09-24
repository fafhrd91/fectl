use std::rc::Rc;
use std::collections::HashMap;

use libc;
use futures::unsync::oneshot;
use futures::{Future, Stream};
use tokio_core::reactor;
use tokio_signal;
use tokio_signal::unix::Signal;
use nix::unistd::getpid;
use nix::sys::wait::{waitpid, WaitStatus, WNOHANG};

use ctx::prelude::*;

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

#[derive(Debug)]
pub enum Command {
    Stop,
    Quit,
    Reload,
    ReapWorkers,
}

pub struct CommandCenter {
    cfg: Rc<Config>,
    state: State,
    stop: Option<oneshot::Sender<bool>>,
    services: HashMap<String, Address<FeService>>,
    stop_waiters: Vec<oneshot::Sender<bool>>,
    stopping: usize,
}

impl CommandCenter {

    pub fn start(cfg: Rc<Config>,
                 handle: &reactor::Handle,
                 stop: oneshot::Sender<bool>) -> Address<CommandCenter> {
        let cmd = CommandCenter {
            cfg: cfg,
            state: State::Starting,
            stop: Some(stop),
            services: HashMap::new(),
            stop_waiters: Vec::new(),
            stopping: 0,
        };

        // start command center
        Builder::build_default(cmd, &handle).run()
    }

    fn exit(&mut self, success: bool) {
        while let Some(waiter) = self.stop_waiters.pop() {
            let _ = waiter.send(true);
        }

        if let Some(stop) = self.stop.take() {
            let _ = stop.send(success);
        }
    }

    fn init_signals(&self, ctx: &mut Context<Self>) {
        let handle = ctx.handle().clone();

        // SIGINT
        tokio_signal::ctrl_c(&handle).map_err(|_| ())
            .ctxfuture()
            .map(|sig, _: &mut _, ctx: &mut Context<Self>|
                 ctx.add_stream(
                     sig.map_err(|_| ()).map(|_| {
                         info!("SIGINT received, exiting");
                         Command::Quit})))
            .spawn(ctx);

        // SIGHUP
        Signal::new(libc::SIGHUP, &handle).map_err(|_| ())
            .ctxfuture()
            .map(|sig, _: &mut _, ctx: &mut Context<Self>|
                 ctx.add_stream(
                     sig.map_err(|_| ()).map(|_| {
                         info!("SIGHUP received, reloading");
                         Command::Reload})))
            .spawn(ctx);

        // SIGTERM
        Signal::new(libc::SIGTERM, &handle).map_err(|_| ())
            .ctxfuture()
            .map(|sig, _: &mut _, ctx: &mut Context<Self>|
                 ctx.add_stream(
                     sig.map_err(|_| ()).map(|_| {
                         info!("SIGTERM received, stopping");
                         Command::Stop})))
            .spawn(ctx);

        // SIGQUIT
        Signal::new(libc::SIGQUIT, &handle).map_err(|_| ())
            .ctxfuture()
            .map(|sig, _: &mut _, ctx: &mut Context<Self>|
                 ctx.add_stream(
                     sig.map_err(|_| ()).map(|_| {
                         info!("SIGQUIT received, exiting");
                         Command::Quit})))
            .spawn(ctx);

        // SIGCHLD
        Signal::new(libc::SIGCHLD, &handle).map_err(|_| ())
            .ctxfuture()
            .map(|sig, _: &mut _, ctx: &mut Context<Self>|
                 ctx.add_stream(
                     sig.map_err(|_| ()).map(|_| {
                         info!("SIGCHLD received");
                         Command::ReapWorkers})))
            .spawn(ctx);
    }
    
    fn stop(&mut self, ctx: &mut Context<Self>, graceful: bool)
    {
        if self.state != State::Stopping {
            info!("Stopping service");

            self.state = State::Stopping;
            for service in self.services.values() {
                self.stopping += 1;
                service::Stop(graceful, Reason::Exit).send(service)
                    .ctxfuture()
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
                    Some(service) => Box::new(
                        service::Pids.send(service).ctxfuture()
                            .then(|res, _, _| match res {
                                Ok(Ok(status)) => fut::ok(status),
                                _ => fut::err(CommandError::UnknownService)
                            })),
                    None => Box::new(fut::err(CommandError::UnknownService)),
                }
            }
            _ => {
                Box::new(fut::err(CommandError::NotReady))
            }
        }
    }
}

pub struct Stop;

impl Message for Stop {
    type Item = oneshot::Receiver<bool>;
    type Error = ();
}

impl MessageHandler<Stop> for CommandCenter {

    fn handle(&mut self, _: Stop, ctx: &mut Context<Self>) -> MessageFuture<Stop, Self>
    {
        let (tx, rx) = oneshot::channel();
        self.stop_waiters.push(tx);
        self.stop(ctx, true);
        Box::new(fut::ok(rx))
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
                    Some(service) => Box::new(
                        service::Start.send(service).ctxfuture()
                            .then(|res, _, _| match res {
                                Ok(Ok(status)) => fut::ok(status),
                                Ok(Err(err)) => fut::err(CommandError::Service(err)),
                                Err(_) => fut::err(CommandError::NotReady)
                            })),
                    None => Box::new(fut::err(CommandError::UnknownService))
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Box::new(fut::err(CommandError::NotReady))
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
                    Some(service) => Box::new(
                        service::Stop(msg.1, Reason::ConsoleRequest).send(service)
                            .ctxfuture()
                            .then(|res, _, _| match res {
                                Ok(Ok(_)) => fut::ok(()),
                                _ => fut::err(CommandError::ServiceStopped),
                            })),
                        None => Box::new(fut::err(CommandError::UnknownService)),
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Box::new(fut::err(CommandError::NotReady))
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
                    Some(service) => Box::new(
                        service::Status.send(service).ctxfuture()
                            .then(|res, _, _| match res {
                                Ok(Ok(status)) => fut::ok(status),
                                _ => fut::err(CommandError::UnknownService)
                            })),
                    None => Box::new(fut::err(CommandError::UnknownService)),
                }
            }
            _ => {
                Box::new(fut::err(CommandError::NotReady))
            }
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
                    Some(service) => Box::new(
                        service::Pause.send(service).ctxfuture()
                            .then(|res, _, _| match res {
                                Ok(Ok(_)) => fut::ok(()),
                                Ok(Err(err)) => fut::err(CommandError::Service(err)),
                                Err(_) => fut::err(CommandError::UnknownService)
                            })),
                    None => Box::new(fut::err(CommandError::UnknownService))
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Box::new(fut::err(CommandError::NotReady))
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
                    Some(service) => Box::new(
                        service::Resume.send(service).ctxfuture()
                            .then(|res, _, _| match res {
                                Ok(Ok(_)) => fut::ok(()),
                                Ok(Err(err)) => fut::err(CommandError::Service(err)),
                                Err(_) => fut::err(CommandError::UnknownService)
                            })),
                        None => Box::new(fut::err(CommandError::UnknownService))
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Box::new(fut::err(CommandError::NotReady))
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
                    Some(service) => Box::new(
                        service::Reload(graceful).send(service).ctxfuture()
                            .then(|res, _, _| match res {
                                Ok(Ok(status)) => fut::ok(status),
                                Ok(Err(err)) => fut::err(CommandError::Service(err)),
                                Err(_) => fut::err(CommandError::UnknownService)
                            })),
                    None => Box::new(fut::err(CommandError::UnknownService))
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", self.state);
                Box::new(fut::err(CommandError::NotReady))
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
                    service::Reload(true).tell(srv);
                }
            }
            _ => warn!("Can not reload in system in `{:?}` state", self.state)
        };
        Box::new(fut::ok(()))
    }
}


impl Service for CommandCenter {

    type Context = Context<Self>;
    type Message = Result<Command, ()>;

    fn start(&mut self, ctx: &mut Self::Context)
    {
        info!("Starting ctl service: {}", getpid());
        self.init_signals(ctx);

        // start services
        for cfg in self.cfg.services.iter() {
            let service = FeService::start(ctx.handle(), cfg.num, cfg.clone());
            self.services.insert(cfg.name.clone(), service);
        }
        self.state = State::Running;
    }

    fn finished(&mut self, _: &mut Self::Context) -> ServiceResult
    {
        self.exit(true);

        ServiceResult::Done
    }

    fn call(&mut self, ctx: &mut Self::Context, cmd: Self::Message) -> ServiceResult
    {
        match cmd {
            Ok(Command::Stop) => {
                self.stop(ctx, true);
            }
            Ok(Command::Quit) => {
                self.stop(ctx, false);
            }
            Ok(Command::Reload) => {
                self.handle(ReloadAll, ctx);
            }
            Ok(Command::ReapWorkers) => {
                debug!("Reap workers");
                loop {
                    match waitpid(None, Some(WNOHANG)) {
                        Ok(WaitStatus::Exited(pid, code)) => {
                            info!("Worker {} exit code: {}", pid, code);
                            let err = ProcessError::from(code);
                            for srv in self.services.values_mut() {
                                service::ProcessExited(pid.clone(), err.clone()).tell(srv);
                            }
                            continue
                        }
                        Ok(WaitStatus::Signaled(pid, sig, _)) => {
                            info!("Worker {} exit by signal {:?}", pid, sig);
                            let err = ProcessError::Signal(sig as usize);
                            for srv in self.services.values_mut() {
                                service::ProcessExited(pid.clone(), err.clone()).tell(srv);
                            }
                            continue
                        },
                        Ok(_) => (),
                        Err(_) => (),
                    }
                    break
                }
            }
            Err(_) => {
                self.exit(false);
                return ServiceResult::Done
            }
        }

        ServiceResult::NotReady
    }
}
