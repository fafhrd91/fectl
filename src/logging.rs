use std::io::Write;
use std::str::FromStr;

use time;
use log::LevelFilter;
use env_logger::Builder;

use version::PKG_INFO;
use config::LoggingConfig;

pub fn init_logging(cfg: &LoggingConfig) {
    let level = cfg.level.as_ref().and_then(
        |s| match LevelFilter::from_str(&s) {
            Ok(lvl) => Some(lvl),
            Err(_) => {
                println!("Can not parse log level value, using `info` level");
                Some(LevelFilter::Info)
            }
        }).unwrap_or(LevelFilter::Info);

    Builder::new()
        .format(|buf, record| {
            let t = time::now();
            write!(buf, "{},{:03} - {} - {}",
                   time::strftime("%Y-%m-%d %H:%M:%S", &t).unwrap(),
                   t.tm_nsec / 1000_000,
                   record.level(),
                   record.args()
            )})
        .filter(Some(PKG_INFO.name), level)
        .init();
}
