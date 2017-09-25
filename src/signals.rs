#![allow(dead_code)]

use libc;
use futures::{Future, Stream};
use tokio_signal;
use tokio_signal::unix::Signal;

use ctx::prelude::*;

/// Different types of process events
#[derive(PartialEq, Clone, Copy, Debug)]
pub enum ProcessEventType {
    Hup,
    Int,
    Term,
    Quit,
    Child,
}

pub struct ProcessEvent(pub ProcessEventType);

impl Message for ProcessEvent {
    type Item = ();
    type Error = ();
}

pub struct ProcessEvents {
    subscribers: Vec<Box<Subscriber<ProcessEvent>>>,
}

impl ProcessEvents {
    pub fn start() -> Address<ProcessEvents> {
        ProcessEvents{subscribers: Vec::new()}.start()
    }
}

impl Actor for ProcessEvents {

    type Message = Result<ProcessEventType, ()>;

    fn start(&mut self, ctx: &mut Context<Self>) {
        let handle = Arbiter::handle();

        // SIGINT
        tokio_signal::ctrl_c(handle).map_err(|_| ())
            .actfuture()
            .map(|sig, _: &mut ProcessEvents, ctx: &mut Context<Self>|
                 ctx.add_stream(
                     sig.map(|_| ProcessEventType::Int).map_err(|_| ())))
            .spawn(ctx);

        // SIGHUP
        Signal::new(libc::SIGHUP, handle).map_err(|_| ())
            .actfuture()
            .map(|sig, _: &mut ProcessEvents, ctx: &mut Context<Self>|
                 ctx.add_stream(
                     sig.map(|_| ProcessEventType::Hup).map_err(|_| ())))
            .spawn(ctx);

        // SIGTERM
        Signal::new(libc::SIGTERM, handle).map_err(|_| ())
            .actfuture()
            .map(|sig, _: &mut Self, ctx: &mut Context<Self>|
                 ctx.add_stream(
                     sig.map(|_| ProcessEventType::Term).map_err(|_| ())))
            .spawn(ctx);

        // SIGQUIT
        Signal::new(libc::SIGQUIT, handle).map_err(|_| ())
            .actfuture()
            .map(|sig, _: &mut ProcessEvents, ctx: &mut Context<Self>|
                 ctx.add_stream(
                     sig.map(|_| ProcessEventType::Quit).map_err(|_| ())))
            .spawn(ctx);

        // SIGCHLD
        Signal::new(libc::SIGCHLD, handle).map_err(|_| ())
            .actfuture()
            .map(|sig, _: &mut ProcessEvents, ctx: &mut Context<Self>|
                 ctx.add_stream(
                     sig.map(|_| ProcessEventType::Child).map_err(|_| ())))
            .spawn(ctx);
    }

    fn call(&mut self, msg: Self::Message, _: &mut Context<Self>) -> ActorStatus
    {
        match msg {
            Ok(ev) => {
                for subscr in self.subscribers.iter() {
                    subscr.send(ProcessEvent(ev))
                }
                ActorStatus::NotReady
            }
            Err(_) => ActorStatus::Done
        }
    }
}


pub struct Subscribe(pub Box<Subscriber<ProcessEvent>>);

impl Message for Subscribe {
    type Item = ();
    type Error = ();
}

impl MessageHandler<Subscribe> for ProcessEvents {

    fn handle(&mut self, msg: Subscribe,
              _: &mut Context<ProcessEvents>) -> MessageFuture<Self, Subscribe>
    {
        self.subscribers.push(msg.0);
        ().to_result()
    }
}
