use std::rc::Rc;
use std::collections::HashMap;

use libc;
use futures::unsync::oneshot;
use futures::{unsync, Async, Future, Stream};
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
    stop: Option<unsync::oneshot::Sender<bool>>,
    tx: unsync::mpsc::UnboundedSender<Command>,
    services: HashMap<String, Address<FeService>>,
    stop_waiters: Vec<unsync::oneshot::Sender<bool>>,
    stopping: usize,
}

impl CommandCenter {

    pub fn start(cfg: Rc<Config>, handle: &reactor::Handle, stop: unsync::oneshot::Sender<bool>)
                 -> Address<CommandCenter> {
        let (cmd_tx, cmd_rx) = unsync::mpsc::unbounded();

        let cmd = CommandCenter {
            cfg: cfg,
            state: State::Starting,
            stop: Some(stop),
            tx: cmd_tx,
            services: HashMap::new(),
            stop_waiters: Vec::new(),
            stopping: 0,
        };

        // start command center
        Builder::build(cmd, cmd_rx, &handle).run()
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

        // SIGHUP
        ctx.add_fut_stream(
            Box::new(
                Signal::new(libc::SIGHUP, &handle)
                    .map(|sig| Box::new(sig.map(|_| {
                        info!("SIGHUP received, reloading");
                        Command::Reload}).map_err(|_| ()))
                         as Box<ServiceStream<CommandCenter>>)
                    .map_err(|_| ()))
        );

        // SIGTERM
        ctx.add_fut_stream(
            Box::new(
                Signal::new(libc::SIGTERM, &handle)
                    .map(|sig| Box::new(sig.map(|_| {
                        info!("SIGTERM received, stopping");
                        Command::Stop}).map_err(|_| ()))
                         as Box<ServiceStream<CommandCenter>>)
                    .map_err(|_| ()))
        );

        // SIGINT
        ctx.add_fut_stream(
            Box::new(
                tokio_signal::ctrl_c(&handle)
                    .map(|sig| Box::new(sig.map(|_| {
                        info!("SIGINT received, exiting");
                        Command::Quit}).map_err(|_| ()))
                         as Box<ServiceStream<CommandCenter>>)
                    .map_err(|_| ()))
        );

        // SIGQUIT
        ctx.add_fut_stream(
            Box::new(
                Signal::new(libc::SIGQUIT, &handle)
                    .map(|sig| Box::new(sig.map(|_| {
                        info!("SIGQUIT received, exiting");
                        Command::Quit}).map_err(|_| ()))
                         as Box<ServiceStream<CommandCenter>>)
                    .map_err(|_| ()))
        );

        // SIGCHLD
        ctx.add_fut_stream(
            Box::new(
                Signal::new(libc::SIGCHLD, &handle)
                    .map(|sig| Box::new(sig.map(|_| {
                        debug!("SIGCHLD received");
                        Command::ReapWorkers}).map_err(|_| ()))
                         as Box<ServiceStream<CommandCenter>>)
                    .map_err(|_| ()))
        );
    }
    
    fn stop(&mut self, ctx: &mut Context<Self>, graceful: bool)
    {
        if self.state != State::Stopping {
            info!("Stopping service");

            self.state = State::Stopping;
            for service in self.services.values() {
                self.stopping += 1;
                service::StopService(graceful, Reason::Exit).send_to(service)
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
    type Service = CommandCenter;

    fn handle(&self, srv: &mut CommandCenter,
              _: &mut Context<CommandCenter>) -> MessageFuture<Self>
    {
        match srv.state {
            State::Running => {
                match srv.services.get(&self.0) {
                    Some(service) => Box::new(
                        service::ServicePids.send_to(service).ctxfuture()
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
    type Service = CommandCenter;

    fn handle(&self, srv: &mut CommandCenter,
              _: &mut Context<CommandCenter>) -> MessageFuture<Self>
    {
        let (tx, rx) = oneshot::channel();
        srv.stop_waiters.push(tx);
        let _ = srv.tx.unbounded_send(Command::Stop);
        Box::new(fut::ok(rx))
    }
}


/// Start Service by `name`
pub struct StartService(pub String);

impl Message for StartService {

    type Item = StartStatus;
    type Error = CommandError;
    type Service = CommandCenter;

    fn handle(&self, srv: &mut CommandCenter,
              _: &mut Context<CommandCenter>) -> MessageFuture<Self>
    {
        match srv.state {
            State::Running => {
                info!("Starting service {:?}", self.0);
                match srv.services.get(&self.0) {
                    Some(service) => Box::new(
                        service::StartService.send_to(service).ctxfuture()
                            .then(|res, _, _| match res {
                                Ok(Ok(status)) => fut::ok(status),
                                Ok(Err(err)) => fut::err(CommandError::Service(err)),
                                Err(_) => fut::err(CommandError::NotReady)
                            })),
                    None => Box::new(fut::err(CommandError::UnknownService))
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", srv.state);
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
    type Service = CommandCenter;

    fn handle(&self, srv: &mut CommandCenter,
              _: &mut Context<CommandCenter>) -> MessageFuture<Self>
    {
        match srv.state {
            State::Running => {
                info!("Stopping service {:?}", self.0);
                match srv.services.get(&self.0) {
                    Some(service) => Box::new(
                        service::StopService(self.1, Reason::ConsoleRequest).send_to(service)
                            .ctxfuture()
                            .then(|res, _, _| match res {
                                Ok(Ok(_)) => fut::ok(()),
                                _ => fut::err(CommandError::ServiceStopped),
                            })),
                        None => Box::new(fut::err(CommandError::UnknownService)),
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", srv.state);
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
    type Service = CommandCenter;

    fn handle(&self, srv: &mut CommandCenter,
              _: &mut Context<CommandCenter>) -> MessageFuture<Self>
    {
        match srv.state {
            State::Running => {
                match srv.services.get(&self.0) {
                    Some(service) => Box::new(
                        service::ServiceStatus.send_to(service).ctxfuture()
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
    type Service = CommandCenter;

    fn handle(&self, srv: &mut CommandCenter,
              _: &mut Context<CommandCenter>) -> MessageFuture<Self>
    {
        match srv.state {
            State::Running => {
                info!("Pause service {:?}", self.0);
                match srv.services.get(&self.0) {
                    Some(service) => Box::new(
                        service::PauseService.send_to(service).ctxfuture()
                            .then(|res, _, _| match res {
                                Ok(Ok(_)) => fut::ok(()),
                                Ok(Err(err)) => fut::err(CommandError::Service(err)),
                                Err(_) => fut::err(CommandError::UnknownService)
                            })),
                    None => Box::new(fut::err(CommandError::UnknownService))
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", srv.state);
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
    type Service = CommandCenter;

    fn handle(&self, srv: &mut CommandCenter,
              _: &mut Context<CommandCenter>) -> MessageFuture<Self>
    {
        match srv.state {
            State::Running => {
                info!("Resume service {:?}", self.0);
                match srv.services.get(&self.0) {
                    Some(service) => Box::new(
                        service::ResumeService.send_to(service).ctxfuture()
                            .then(|res, _, _| match res {
                                Ok(Ok(_)) => fut::ok(()),
                                Ok(Err(err)) => fut::err(CommandError::Service(err)),
                                Err(_) => fut::err(CommandError::UnknownService)
                            })),
                        None => Box::new(fut::err(CommandError::UnknownService))
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", srv.state);
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
    type Service = CommandCenter;

    fn handle(&self, srv: &mut CommandCenter,
              _: &mut Context<CommandCenter>) -> MessageFuture<Self>
    {
        match srv.state {
            State::Running => {
                info!("Reloading service {:?}", self.0);
                match srv.services.get(&self.0) {
                    Some(service) => Box::new(
                        service::ReloadService(self.1).send_to(service).ctxfuture()
                            .then(|res, _, _| match res {
                                Ok(Ok(status)) => fut::ok(status),
                                Ok(Err(err)) => fut::err(CommandError::Service(err)),
                                Err(_) => fut::err(CommandError::UnknownService)
                            })),
                    None => Box::new(fut::err(CommandError::UnknownService))
                }
            }
            _ => {
                warn!("Can not reload in system in `{:?}` state", srv.state);
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
    type Service = CommandCenter;

    fn handle(&self, srv: &mut CommandCenter,
              _: &mut Context<CommandCenter>) -> MessageFuture<Self>
    {
        match srv.state {
            State::Running => {
                info!("reloading all services");
                for srv in srv.services.values() {
                    let _ = service::ReloadService(true).send_to(srv);
                }
            }
            _ => warn!("Can not reload in system in `{:?}` state", srv.state)
        };
        Box::new(fut::ok(()))
    }
}


impl Service for CommandCenter {

    type Context = Context<Self>;
    type Message = Result<Command, ()>;
    type Result = Result<(), ()>;

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

    fn finished(&mut self, _: &mut Self::Context) -> Result<Async<()>, ()>
    {
        self.exit(true);
        Ok(Async::Ready(()))
    }

    fn call(&mut self, ctx: &mut Self::Context, cmd: Self::Message) -> Result<Async<()>, ()>
    {
        match cmd {
            Ok(Command::Stop) => {
                self.stop(ctx, true);
            }
            Ok(Command::Quit) => {
                self.stop(ctx, false);
            }
            Ok(Command::Reload) => {
                ReloadAll.handle(self, ctx);
            }
            Ok(Command::ReapWorkers) => {
                debug!("Reap workers");
                loop {
                    match waitpid(None, Some(WNOHANG)) {
                        Ok(WaitStatus::Exited(pid, code)) => {
                            info!("Worker {} exit code: {}", pid, code);
                            let err = ProcessError::from(code);
                            for srv in self.services.values_mut() {
                                let _ = service::ProcessExited(
                                    pid.clone(), err.clone()).send_to(srv);
                            }
                            continue
                        }
                        Ok(WaitStatus::Signaled(pid, sig, _)) => {
                            info!("Worker {} exit by signal {:?}", pid, sig);
                            let err = ProcessError::Signal(sig as usize);
                            for srv in self.services.values_mut() {
                                let _ = service::ProcessExited(
                                    pid.clone(), err.clone()).send_to(srv);
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
                return Ok(Async::Ready(()))
            }
        }

        Ok(Async::NotReady)
    }
}
