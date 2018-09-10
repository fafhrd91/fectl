extern crate env_logger;
extern crate time;
#[macro_use]
extern crate log;

extern crate structopt;
#[macro_use]
extern crate structopt_derive;

extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

extern crate byteorder;
extern crate bytes;
extern crate futures;
extern crate libc;
extern crate mio;
extern crate net2;
extern crate nix;
extern crate tokio;
extern crate toml;

#[macro_use]
extern crate actix;

mod addrinfo;
mod client;
mod cmd;
mod config;
mod config_helpers;
mod event;
mod exec;
mod io;
mod logging;
mod master;
mod master_types;
mod process;
mod service;
mod socket;
mod utils;
mod worker;

mod version {
    include!(concat!(env!("OUT_DIR"), "/version.rs"));
}

fn main() {
    let sys = actix::System::new("fectl");
    let loaded = match config::load_config() {
        Some(cfg) => master::start(cfg),
        None => false,
    };
    let code = if loaded { sys.run() } else { 1 };
    std::process::exit(code);
}
