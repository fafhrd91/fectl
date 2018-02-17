use std;
use std::io;
use std::rc::Rc;
use std::ffi::OsStr;
use std::time::Duration;
use std::thread;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixListener as StdUnixListener;

use nix;
use libc;
use serde_json as json;
use byteorder::{BigEndian , ByteOrder};
use bytes::{BytesMut, BufMut};
use futures::Stream;
use tokio_core::reactor::Timeout;
use tokio_uds::{UnixStream, UnixListener};
use tokio_io::AsyncRead;
use tokio_io::io::WriteHalf;
use tokio_io::codec::{FramedRead, Encoder, Decoder};

use actix::prelude::*;

use client;
use logging;
use config::Config;
use version::PKG_INFO;
use cmd::{self, CommandCenter, CommandError};
use service::{StartStatus, ReloadStatus, ServiceOperationError};
use master_types::{MasterRequest, MasterResponse};

pub struct Master {
    cfg: Rc<Config>,
    cmd: Addr<Unsync, CommandCenter>,
}

impl Actor for Master {
    type Context = Context<Self>;
}

#[derive(Message)]
struct NetStream(UnixStream, std::os::unix::net::SocketAddr);

impl StreamHandler<NetStream, io::Error> for Master {

    fn handle(&mut self, msg: NetStream, _: &mut Context<Self>) {
        let cmd = self.cmd.clone();

        MasterClient::create(|ctx| {
            let (r, w) = msg.0.split();
            ctx.add_stream(FramedRead::new(r, MasterTransportCodec));

            MasterClient{
                cmd: cmd,
                framed: actix::io::FramedWrite::new(w, MasterTransportCodec, ctx)}
        })
    }
}

impl Drop for Master {
    fn drop(&mut self) {
        self.cfg.master.remove_files();
    }
}

struct MasterClient {
    cmd: Addr<Unsync, CommandCenter>,
    framed: actix::io::FramedWrite<WriteHalf<UnixStream>, MasterTransportCodec>,
}

impl Actor for MasterClient {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
    }
}

impl actix::io::WriteHandler<io::Error> for MasterClient {}

impl StreamHandler<MasterRequest, io::Error> for MasterClient {

    fn handle(&mut self, msg: MasterRequest, ctx: &mut Self::Context) {
        ctx.notify(msg);
    }
}

impl MasterClient {

    fn hb(&self, ctx: &mut Context<Self>) {
        let fut = Timeout::new(Duration::new(1, 0), Arbiter::handle())
            .unwrap()
            .actfuture()
            .then(|_, act: &mut MasterClient, ctx: &mut Context<Self>| {
                act.framed.write(MasterResponse::Pong);
                act.hb(ctx);
                actix::fut::ok(())
            });
        ctx.spawn(fut);
    }

    fn handle_error(&mut self, err: CommandError, _: &mut Context<Self>) {
        let _ = match err {
            CommandError::NotReady =>
                self.framed.write(MasterResponse::ErrorNotReady),
            CommandError::UnknownService =>
                self.framed.write(MasterResponse::ErrorUnknownService),
            CommandError::ServiceStopped =>
                self.framed.write(MasterResponse::ErrorServiceStopped),
            CommandError::Service(err) => match err {
                ServiceOperationError::Starting =>
                    self.framed.write(MasterResponse::ErrorServiceStarting),
                ServiceOperationError::Reloading =>
                    self.framed.write(MasterResponse::ErrorServiceReloading),
                ServiceOperationError::Stopping =>
                    self.framed.write(MasterResponse::ErrorServiceStopping),
                ServiceOperationError::Running =>
                    self.framed.write(MasterResponse::ErrorServiceRunning),
                ServiceOperationError::Stopped =>
                    self.framed.write(MasterResponse::ErrorServiceStopped),
                ServiceOperationError::Failed =>
                    self.framed.write(MasterResponse::ErrorServiceFailed),
            }
        };
    }

    fn stop(&mut self, name: String, ctx: &mut Context<Self>) {
        info!("Client command: Stop service '{}'", name);

        self.cmd.send(cmd::StopService(name, true))
            .into_actor(self)
            .then(|res, srv, ctx| {
                match res {
                    Err(_) => (),
                    Ok(Err(err)) => match err {
                        CommandError::ServiceStopped =>
                            srv.framed.write(MasterResponse::ServiceStarted),
                        _ => srv.handle_error(err, ctx),
                    }
                    Ok(Ok(_)) =>
                        srv.framed.write(MasterResponse::ServiceStopped),
                };
                actix::fut::ok(())
            }).spawn(ctx);
    }

    fn reload(&mut self, name: String, ctx: &mut Context<Self>, graceful: bool)
    {
        info!("Client command: Reload service '{}'", name);

        self.cmd.send(cmd::ReloadService(name, graceful))
            .into_actor(self)
            .then(|res, srv, ctx| {
                match res {
                    Err(_) => (),
                    Ok(Err(err)) => srv.handle_error(err, ctx),
                    Ok(Ok(res)) => {
                        let _ = match res {
                            ReloadStatus::Success =>
                                srv.framed.write(MasterResponse::ServiceStarted),
                            ReloadStatus::Failed =>
                                srv.framed.write(MasterResponse::ServiceFailed),
                            ReloadStatus::Stopping =>
                                srv.framed.write(MasterResponse::ErrorServiceStopping),
                        };
                    }
                }
                actix::fut::ok(())
            }).spawn(ctx);
    }

    fn start_service(&mut self, name: String, ctx: &mut Context<Self>) {
        info!("Client command: Start service '{}'", name);

        self.cmd.send(cmd::StartService(name))
            .into_actor(self)
            .then(|res, srv, ctx| {
                match res {
                    Err(_) => (),
                    Ok(Err(err)) => srv.handle_error(err, ctx),
                    Ok(Ok(res)) => {
                        let _ = match res {
                            StartStatus::Success =>
                                srv.framed.write(MasterResponse::ServiceStarted),
                            StartStatus::Failed =>
                                srv.framed.write(MasterResponse::ServiceFailed),
                            StartStatus::Stopping =>
                                srv.framed.write(MasterResponse::ErrorServiceStopping),
                        };
                    }
                }
                actix::fut::ok(())
            }).spawn(ctx);
    }
}

impl Message for MasterRequest {
    type Result = ();
}

impl Handler<MasterRequest> for MasterClient {
    type Result = ();

    fn handle(&mut self, msg: MasterRequest, ctx: &mut Context<Self>) {
        match msg {
            MasterRequest::Ping => {
                self.framed.write(MasterResponse::Pong);
            },
            MasterRequest::Start(name) =>
                self.start_service(name, ctx),
            MasterRequest::Reload(name) =>
                self.reload(name, ctx, true),
            MasterRequest::Restart(name) =>
                self.reload(name, ctx, false),
            MasterRequest::Stop(name) =>
                self.stop(name, ctx),
            MasterRequest::Pause(name) => {
                info!("Client command: Pause service '{}'", name);
                self.cmd.send(cmd::PauseService(name))
                    .into_actor(self)
                    .then(|res, srv, ctx| {
                        match res {
                            Err(_) => (),
                            Ok(Err(err)) => srv.handle_error(err, ctx),
                            Ok(Ok(_)) => {
                                srv.framed.write(MasterResponse::Done);
                            },
                        };
                        actix::fut::ok(())
                    }).spawn(ctx);
            }
            MasterRequest::Resume(name) => {
                info!("Client command: Resume service '{}'", name);
                self.cmd.send(cmd::ResumeService(name))
                    .into_actor(self)
                    .then(|res, srv, ctx| {
                        match res {
                            Err(_) => (),
                            Ok(Err(err)) => srv.handle_error(err, ctx),
                            Ok(Ok(_)) => {
                                srv.framed.write(MasterResponse::Done);
                            },
                        };
                        actix::fut::ok(())
                    }).spawn(ctx);
            }
            MasterRequest::Status(name) => {
                debug!("Client command: Service status '{}'", name);
                self.cmd.send(cmd::StatusService(name))
                    .into_actor(self)
                    .then(|res, srv, ctx| {
                        match res {
                            Err(_) => (),
                            Ok(Err(err)) => srv.handle_error(err, ctx),
                            Ok(Ok(status)) => {
                                srv.framed.write(MasterResponse::ServiceStatus(status));
                            },
                        };
                        actix::fut::ok(())
                    }).spawn(ctx);
            }
            MasterRequest::SPid(name) => {
                debug!("Client command: Service status '{}'", name);
                self.cmd.send(cmd::ServicePids(name))
                    .into_actor(self)
                    .then(|res, srv, ctx| {
                        match res {
                            Err(_) => (),
                            Ok(Err(err)) => srv.handle_error(err, ctx),
                            Ok(Ok(pids)) => {
                                srv.framed.write(MasterResponse::ServiceWorkerPids(pids));
                            },
                        };
                        actix::fut::ok(())
                    }).spawn(ctx);
            }
            MasterRequest::Pid => {
                self.framed.write(MasterResponse::Pid(
                    format!("{}", nix::unistd::getpid())));
            },
            MasterRequest::Version => {
                self.framed.write(MasterResponse::Version(
                    format!("{} {}", PKG_INFO.name, PKG_INFO.version)));
            },
            MasterRequest::Quit => {
                self.cmd.send(cmd::Stop)
                    .into_actor(self)
                    .then(|_, act, _| {
                        act.framed.write(MasterResponse::Done);
                        actix::fut::ok(())
                    }).spawn(ctx);
            }
        };
    }
}


/// Codec for Master transport
struct MasterTransportCodec;

impl Decoder for MasterTransportCodec
{
    type Item = MasterRequest;
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
            Ok(Some(json::from_slice::<MasterRequest>(&buf)?))
        } else {
            Ok(None)
        }
    }
}

impl Encoder for MasterTransportCodec
{
    type Item = MasterResponse;
    type Error = io::Error;

    fn encode(&mut self, msg: MasterResponse, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg = json::to_string(&msg).unwrap();
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 2);
        dst.put_u16::<BigEndian>(msg_ref.len() as u16);
        dst.put(msg_ref);

        Ok(())
    }
}

const HOST: &str = "127.0.0.1:57897";

/// Start master process
pub fn start(cfg: Config) -> bool {
    // init logging
    logging::init_logging(&cfg.logging);

    info!("Starting fectl process");

    // change working dir
    if let Err(err) = nix::unistd::chdir::<OsStr>(cfg.master.directory.as_ref()) {
        error!("Can not change directory {:?} err: {}", cfg.master.directory, err);
        return false
    }

    // check if other app is running
    for idx in 0..10 {
        match std::net::TcpListener::bind(HOST) {
            Ok(listener) => {
                std::mem::forget(listener);
                break
            }
            Err(_) => {
                if idx == 8 {
                    error!("Can not start: Another process is running.");
                    return false
                }
                info!("Trying to bind address, sleep for 5 seconds");
                thread::sleep(Duration::new(5, 0));
            }
        }
    }

    // create commands listener and also check if service process is running
    let lst = match StdUnixListener::bind(&cfg.master.sock) {
        Ok(lst) => lst,
        Err(err) => match err.kind() {
            io::ErrorKind::PermissionDenied => {
                error!("Can not create socket file {:?} err: Permission denied.",
                       cfg.master.sock);
                return false
            },
            io::ErrorKind::AddrInUse => {
                match client::is_alive(&cfg.master) {
                    client::AliveStatus::Alive => {
                        error!("Can not start: Another process is running.");
                        return false
                    },
                    client::AliveStatus::NotResponding => {
                        error!("Master process is not responding.");
                        if let Some(pid) = cfg.master.load_pid() {
                            error!("Master process: (pid:{})", pid);
                        } else {
                            error!("Can not load pid of the master process.");
                        }
                        return false
                    },
                    client::AliveStatus::NotAlive => {
                        // remove socket and try again
                        let _ = std::fs::remove_file(&cfg.master.sock);
                        match StdUnixListener::bind(&cfg.master.sock) {
                            Ok(lst) => lst,
                            Err(err) => {
                                error!("Can not create listener socket: {}", err);
                                return false
                            }
                        }
                    }
                }
            }
            _ => {
                error!("Can not create listener socket: {}", err);
                return false
            }
        }
    };

    // try to save pid
    if let Err(err) = cfg.master.save_pid() {
        error!("Can not write pid file {:?} err: {}", cfg.master.pid, err);
        return false
    }

    // set uid
    if let Some(uid) = cfg.master.uid {
        if let Err(err) = nix::unistd::setuid(uid) {
            error!("Can not set process uid, err: {}", err);
            return false
        }
    }

    // set gid
    if let Some(gid) = cfg.master.gid {
        if let Err(err) = nix::unistd::setgid(gid) {
            error!("Can not set process gid, err: {}", err);
            return false
        }
    }

    let daemon = cfg.master.daemon;
    if daemon {
        if let Err(err) = nix::unistd::daemon(true, false) {
            error!("Can not daemonize process: {}", err);
            return false
        }

        // close stdin
        let _ = nix::unistd::close(libc::STDIN_FILENO);

        // redirect stdout and stderr
        if let Some(ref stdout) = cfg.master.stdout {
            match std::fs::OpenOptions::new().append(true).create(true).open(stdout)
            {
                Ok(f) => {
                    let _ = nix::unistd::dup2(f.as_raw_fd(), libc::STDOUT_FILENO);
                }
                Err(err) =>
                    error!("Can open stdout file {}: {}", stdout, err),
            }
        }
        if let Some(ref stderr) = cfg.master.stderr {
            match std::fs::OpenOptions::new().append(true).create(true).open(stderr)
            {
                Ok(f) => {
                    let _ = nix::unistd::dup2(f.as_raw_fd(), libc::STDERR_FILENO);

                },
                Err(err) => error!("Can open stderr file {}: {}", stderr, err)
            }
        }

        // continue start process
        nix::sys::stat::umask(nix::sys::stat::Mode::from_bits(0o22).unwrap());
    }

    let cfg = Rc::new(cfg);

    // create uds stream
    let lst = match UnixListener::from_listener(lst, Arbiter::handle()) {
        Ok(lst) => lst,
        Err(err) => {
            error!("Can not create unix socket listener {:?}", err);
            return false
        }
    };

    // command center
    let cmd = CommandCenter::start(cfg.clone());

    // start uds master server
    let _: () = Master::create(|ctx| {
        ctx.add_stream(lst.incoming().map(|(s, a)| NetStream(s, a)));
        Master{cfg: cfg, cmd: cmd}}
    );

    if !daemon {
        println!("");
    }
    true
}
