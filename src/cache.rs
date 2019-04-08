
use std::fs::{File, metadata, remove_file};
use std::io::{self, Read};
use std::path::Path;

use encoding::DecoderTrap;
use encoding::label::encoding_from_whatwg_label;
use regex::Regex;
use rusqlite::Connection;

use crate::errors::{AppResult, AppResultU};
use crate::loader::{Loader, self};
use crate::types::*;



#[derive(Clone, Copy, Debug, PartialEq)]
pub enum State {
    Fresh,
    Nothing,
    Stale,
}

pub enum Cache {
    File(String),
    Temp(mktemp::Temp),
}


impl State {
    pub fn get(input: &Input, cache: &Cache) -> AppResult<State> {
        match *input {
            Input::Stdin => Ok(State::Nothing),
            Input::File(input_filepath) => {
                match *cache {
                    Cache::File(ref cache_filepath) => {
                        if !Path::new(cache_filepath).exists() {
                            return Ok(State::Nothing)
                        }
                        let input = metadata(input_filepath)?.modified()?;
                        let cache = metadata(cache_filepath)?.modified()?;
                        Ok(if input < cache {
                            State::Fresh
                        } else {
                            State::Stale
                        })
                    },
                    _ => Ok(State::Nothing),
                }
            }
        }
    }

    pub fn is_fresh(self) -> bool {
        self == State::Fresh
    }
}


impl Cache {
    pub fn refresh(&self, format: Format, input: &Input, no_headers: bool, guess_lines: Option<usize>, encoding: &Option<String>) -> AppResultU {
        let source = read_file(input, encoding)?;

        let mut conn = Connection::open(self)?;
        let tx = conn.transaction()?;

        let config = loader::Config { no_headers, guess_lines };

        let load = |loader: &Loader| {
            loader.load(&tx, &source, &config)
        };

        match format {
            Format::Csv(delimiter) =>
                load(&loader::Csv { delimiter })?,
            Format::Json =>
                load(&loader::Json())?,
            Format::Ltsv =>
                load(&loader::Ltsv())?,
            Format::Simple =>
                load(&loader::Simple { delimiter: Regex::new(r"[ \t]+")? })?,
        }

        tx.commit()?;
        Ok(())
    }

    pub fn remove_file(&self) -> AppResultU {
        use Cache::*;

        match *self {
            File(ref path) => remove_file(path)?,
            Temp(_) => (),
        }

        Ok(())
    }
}

impl AsRef<Path> for Cache {
    fn as_ref(&self) -> &Path {
        match *self {
            Cache::File(ref path) => Path::new(path),
            Cache::Temp(ref path) => path.as_ref(),
        }
    }
}


fn read_file(input: &Input, encoding: &Option<String>) -> AppResult<String> {
    let mut buffer = String::new();

    if let Some(ref encoding) = *encoding {
        let encoding = encoding_from_whatwg_label(encoding).ok_or("Invalid encoding name")?;
        let mut bin: Vec<u8> = vec![];
        match *input {
            Input::File(ref input_filepath) => {
                let mut file = File::open(input_filepath)?;
                file.read_to_end(&mut bin)?;
            },
            Input::Stdin => {
                io::stdin().read_to_end(&mut bin)?;
            }
        }
        buffer = match encoding.decode(&bin, DecoderTrap::Replace) {
            Ok(s) => s,
            Err(s) => s.to_string(),
        };
    } else {
        match *input {
            Input::File(input_filepath) => {
                let mut file = File::open(input_filepath)?;
                file.read_to_string(&mut buffer)?;
            },
            Input::Stdin => {
                io::stdin().read_to_string(&mut buffer)?;
            }
        }
    }

    Ok(buffer)
}
