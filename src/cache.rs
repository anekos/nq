
use std::fs::{File, metadata, remove_file};
use std::io::{self, Read};
use std::path::Path;

use encoding::DecoderTrap;
use encoding::label::encoding_from_whatwg_label;
use regex::Regex;
use rusqlite::Transaction;

use crate::errors::{AppResult, AppResultU};
use crate::loader::{Config, Loader, self};
use crate::types::*;



#[derive(Clone, Copy, Debug, PartialEq)]
pub enum State {
    Fresh,
    Nothing,
    Stale,
}

pub struct Cache<'a> {
    source: &'a Source,
    tx: Transaction<'a>,
}

pub enum Source {
    File(String),
    Temp(mktemp::Temp),
}


impl<'a> Cache<'a> {
    pub fn new(source: &'a Source, tx: Transaction<'a>) -> Self {
        Self { source, tx }
    }

    pub fn refresh(self, format: Format, input: &Input, config: &Config, encoding: &Option<String>) -> AppResultU {
        let source = read_file(input, encoding)?;

        let load = |loader: &Loader| {
            loader.load(&self.tx, &source, config)
        };

        match format {
            Format::Csv(delimiter) =>
                load(&loader::Csv { delimiter })?,
            Format::Json =>
                load(&loader::Json())?,
            Format::Ltsv =>
                load(&loader::Ltsv())?,
            Format::Regex(ref format) =>
                load(&loader::Regex { format: Regex::new(format)? })?,
            Format::Simple =>
                load(&loader::Simple { delimiter: Regex::new(r"[ \t]+")? })?,
        }

        self.tx.commit()?;
        Ok(())
    }

    pub fn state(&self, input: &Input) -> AppResult<State> {
        match *input {
            Input::Stdin => Ok(State::Nothing),
            Input::File(input_filepath) => {
                match self.source {
                    Source::File(ref cache_filepath) => {
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
}


impl State {
    pub fn is_fresh(self) -> bool {
        self == State::Fresh
    }
}


impl Source {
    pub fn remove_file(&self) -> AppResultU {
        use Source::*;

        match *self {
            File(ref path) => remove_file(path)?,
            Temp(_) => (),
        }

        Ok(())
    }
}

impl AsRef<Path> for Source {
    fn as_ref(&self) -> &Path {
        match *self {
            Source::File(ref path) => Path::new(path),
            Source::Temp(ref path) => path.as_ref(),
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
