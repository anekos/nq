
use std::convert::Into;
use std::fs::{File, metadata, remove_file};
use std::io::{BufReader, Cursor, Read, self};
use std::path::Path;

use encoding::DecoderTrap;
use encoding::label::encoding_from_whatwg_label;
use regex::Regex;
use rusqlite::{NO_PARAMS, Transaction};

use crate::errors::{AppError, AppResult, AppResultU};
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
    pub fn format(&self) -> AppResult<String> {
        let meta: u32 = self.tx.query_row("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'meta'", NO_PARAMS, |row| row.get(0))?;
        match meta {
            0 => {
                self.tx.execute("CREATE TABLE meta (name TEXT PRIMARY KEY, value TEXT);", NO_PARAMS)?;
                Ok("".to_owned())
            },
            1 => {
                let result = self.tx.query_row("SELECT value FROM meta WHERE name = 'format'", NO_PARAMS, |row| row.get(0));
                match result {
                    Ok(format) => Ok(format),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok("".to_owned()),
                    Err(err) => Err(AppError::Sql(err)),
                }
            },
            _ => panic!("BUG"),
        }
    }

    pub fn new(source: &'a Source, tx: Transaction<'a>) -> Self {
        Self { source, tx }
    }

    pub fn refresh(self, format: &Format, input: &Input, config: &Config, encoding: &Option<String>) -> AppResultU {
        // let source = read_file(input, encoding)?;

        if let Input::File(path) = input {
            let file = File::open(path)?;
            let mut file = std::io::BufReader::new(file);
            loader::Json().load(&self.tx, &mut file, config)?;
        } else {
            panic!("Ooops");
        }

        match format {
            Format::Csv(delimiter) =>
                read_file(input, encoding, &self.tx, config, &loader::Csv { delimiter: *delimiter })?,
            Format::Json =>
                read_file(input, encoding, &self.tx, config, &loader::Json())?,
            Format::Ltsv =>
                read_file(input, encoding, &self.tx, config, &loader::Ltsv())?,
            Format::Regex(ref format) =>
                read_file(input, encoding, &self.tx, config, &loader::Regex { format: Regex::new(format)? })?,
            Format::Simple =>
                read_file(input, encoding, &self.tx, config, &loader::Simple { delimiter: Regex::new(r"[ \t]+")? })?,
        };


        if let Source::File(_) = self.source {
            let updated = self.tx.execute("UPDATE meta SET value = ? WHERE name = 'format';", &[&format.to_string()])?;
            match updated {
                0 => {
                    self.tx.execute("INSERT INTO meta VALUES('format', ?);", &[&format.to_string()])?;
                },
                1 => (),
                n => panic!("UPDATE has returned: {}", n),
            }
        }

        self.tx.commit()?;
        Ok(())
    }

    pub fn state(&self, input: &Input, format: &Format) -> AppResult<State> {
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
                        Ok(if input < cache && format.to_string() == self.format()? {
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


fn read_file<L: Loader>(input: &Input, encoding: &Option<String>, tx: &Transaction, config: &Config, loader: &L) -> AppResultU {

    let mut buffer = String::new();

    if let Some(ref encoding) = *encoding {
        let encoding = encoding_from_whatwg_label(encoding).ok_or("Invalid encoding name")?;
        match *input {
            Input::File(ref input_filepath) => {
                let mut file = File::open(input_filepath)?;
                file.read_to_string(&mut buffer)?;
            },
            Input::Stdin => {
                let mut bin: Vec<u8> = vec![];
                io::stdin().read_to_end(&mut bin)?;
                buffer = encoding.decode(&bin, DecoderTrap::Replace).unwrap_or_else(Into::into);
            },
        }
    } else {
        match *input {
            Input::File(input_filepath) => {
                let file = File::open(input_filepath)?;
                let mut file = BufReader::new(file);
                loader.load(tx, &mut file, config)?;
                return Ok(());
            },
            Input::Stdin => {
                io::stdin().read_to_string(&mut buffer)?;
            }
        }
    }

    let mut buffer = Cursor::new(buffer);
    loader.load(tx, &mut buffer, config)?;

    Ok(())
}
