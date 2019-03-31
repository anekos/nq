
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

mod app_options;
mod cache;
mod errors;
mod format;
mod sql;
mod types;
mod ui;

use cache::Cache;
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
    let cache = make_sqlite(&input, &options.flag_c)?;
    let cache_state = cache::State::get(&input, &cache)?;

    if let Some(path) = cache.as_ref().to_str() {
        eprintln!("cache: {}", path);
    }

    if !options.flag_R && !cache_state.is_fresh() {
        match cache.refresh(options.format(), &input, options.flag_n, options.flag_g, &options.flag_e) {
            Ok(_) => (),
            err => {
                if cache_state == cache::State::Nothing {
                    cache.remove_file()?;
                }
                return err;
            }
        }
    }

    exec_sqlite(&cache, &options.flag_q, &options.arg_sqlite_options);

    Ok(())
}

fn parse_input(filepath: &str) -> Input {
    match filepath {
        "-" => Input::Stdin,
        _ => Input::File(filepath)
    }
}

fn make_sqlite(input: &Input, cache_filepath: &Option<String>) -> AppResult<Cache> {
    match *input {
        Input::Stdin => Ok(Cache::Temp(mktemp::Temp::new_file()?)),
        Input::File(ref input_path) => {
            match *cache_filepath {
                Some(ref path) => Ok(Cache::File(path.clone())),
                None => {
                    let meta = fs::File::open(input_path)?.metadata()?;
                    if meta.is_file() {
                        let mut path = input_path.to_string();
                        path.push_str(".nq-cache.sqlite");
                        Ok(Cache::File(path))
                    } else {
                        Ok(Cache::Temp(mktemp::Temp::new_file()?))
                    }
                }
            }
        }
    }
}

fn exec_sqlite(cache: &Cache, query: &Option<String>, options: &[String]) {
    let cmd = env::var("NQ_SQLITE").unwrap_or_else(|_| "sqlite3".to_owned());
    let mut cmd = Command::new(cmd);
    cmd.arg(cache.as_ref());
    cmd.args(options);
    if let Some(ref query) = *query {
        cmd.arg(query);
    }
    cmd.exec();
}
