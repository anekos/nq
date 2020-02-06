
#[macro_use] extern crate serde_derive;
extern crate docopt;
extern crate encoding;
extern crate serde_json;
extern crate mktemp;
extern crate quick_csv;
extern crate regex;
extern crate rusqlite;

use std::env;
use std::fs;
use std::os::unix::process::CommandExt;
use std::process::{exit, Command};

use rusqlite::Connection;

mod app_options;
mod cache;
mod db;
mod errors;
mod loader;
mod sql;
mod types;
mod ui;

use cache::{Cache, Source};
use errors::{AppResult, AppResultU};
use types::*;



fn main() {
    if let Err(err) = app() {
        eprintln!("{}", err);
        exit(1);
    }
}


fn app() -> AppResultU {
    let options = app_options::parse();

    if options.flag_version {
        println!("{}", env!("CARGO_PKG_VERSION").to_string());
        exit(0);
    }

    let input = parse_input(&options.arg_csv);
    let source = make_sqlite(&input, &options.flag_c)?;
    let format = options.format();

    let mut conn = Connection::open(source.as_ref())?;
    let tx = conn.transaction()?;

    let cache = Cache::new(&source, tx);
    let cache_state = cache.state(&input, &format)?;
    let config = loader::Config { no_header: options.flag_n, guess_lines: options.flag_g };

    if let Some(path) = source.as_ref().to_str() {
        eprintln!("cache: {}", path);
    }

    if options.flag_R || !cache_state.is_fresh() {
        match cache.refresh(&format, &input, &config,  &options.flag_e) {
            Ok(_) => (),
            err => {
                if cache_state == cache::State::Nothing {
                    source.remove_file()?;
                }
                return err;
            }
        }
    }

    exec_sqlite(&source, &options.flag_q, &options.arg_sqlite_options);

    Ok(())
}

fn parse_input(filepath: &str) -> Input {
    match filepath {
        "-" => Input::Stdin,
        _ => Input::File(filepath)
    }
}

fn make_sqlite(input: &Input, cache_filepath: &Option<String>) -> AppResult<Source> {
    match *input {
        Input::Stdin => Ok(Source::Temp(mktemp::Temp::new_file()?)),
        Input::File(ref input_path) => {
            match *cache_filepath {
                Some(ref path) => Ok(Source::File(path.clone())),
                None => {
                    let meta = fs::File::open(input_path)?.metadata()?;
                    if meta.is_file() {
                        let mut path = (*input_path).to_string();
                        path.push_str(".nq-cache.sqlite");
                        Ok(Source::File(path))
                    } else {
                        Ok(Source::Temp(mktemp::Temp::new_file()?))
                    }
                }
            }
        }
    }
}

fn exec_sqlite(source: &Source, query: &Option<String>, options: &[String]) {
    let cmd = env::var("NQ_SQLITE").unwrap_or_else(|_| "sqlite3".to_owned());
    let mut cmd = Command::new(cmd);
    cmd.arg(source.as_ref());
    cmd.args(options);
    if let Some(ref query) = *query {
        cmd.arg(query);
    }
    cmd.exec();
}
