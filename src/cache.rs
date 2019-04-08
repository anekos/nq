
use std::fs::{File, metadata, remove_file};
use std::io::{self, Read};
use std::path::Path;

use encoding::DecoderTrap;
use encoding::label::encoding_from_whatwg_label;
use rusqlite::{Transaction, Connection};

use crate::format;
use crate::types::*;
use crate::errors::{AppResult, AppResultU};



const ALPHAS: &[&str] = &[
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
    "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "A", "B", "C", "D", "E", "F",
    "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
    "W", "X", "Y", "Z"
];


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

        match format {
            Format::Ltsv => {
                let header = format::ltsv::header(&source)?;
                let types = Type::new(header.len());
                create_table(&tx, &types, header.as_slice())?;
                format::ltsv::insert_rows(&tx, &source)?;
            }
            Format::Json => {
                let header = format::json::header(&source)?;
                let header: Vec<&str> = header.iter().map(|it| it.as_ref()).collect();
                let types = Type::new(header.len());
                create_table(&tx, &types, header.as_slice())?;
                format::json::insert_rows(&tx, &source)?;
            }
            Format::Simple => {
                let reader = format::simple::Reader::new()?;
                let header = reader.header(&source)?;
                let types = Type::new(header.len());
                create_table(&tx, &types, header.as_slice())?;
                reader.insert_rows(&tx, header.len(), &source)?;
            }
            Format::Csv(delimiter) => {
                let mut content = format::csv::open(&source, delimiter)?;
                let header = content.nth(0).ok_or("Header not found")??;
                let header = if no_headers {
                    let columns = header.len();
                    alpha_header(columns)
                } else {
                    let _ = content.next();
                    header.columns()?.collect::<Vec<&str>>()
                };
                let mut types: Vec<Type> = vec![];
                types.resize(header.len(), Type::Int);
                if let Some(lines) = guess_lines {
                    let mut content = format::csv::open(&source, delimiter)?;
                    content.next().ok_or("No header")??;
                    format::csv::guess_types(&mut types, lines, content)?
                }

                create_table(&tx, &types, header.as_slice())?;
                format::csv::insert_rows(&tx, header.len(), content, &types)?;
            }
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


fn alpha_header(n: usize) -> Vec<&'static str> {
    let mut result = vec![];
    for alpha in ALPHAS.iter().take(n) {
        result.push(*alpha);
    }
    result
}

fn create_table(tx: &Transaction, types: &[Type], header: &[&str]) -> AppResultU {
    let mut create = "CREATE TABLE n (".to_owned();
    let mut first = true;
    for (i, name) in header.iter().enumerate() {
        let name = name.replace("'", "''");
        if first {
            first = false;
        } else {
            create.push(',');
        }
        let t = match types[i] {
            Type::Int => "integer",
            Type::Real => "real",
            Type::Text => "text",
        };
        create.push_str(&format!("'{}' {}", name, t));
    }
    create.push(')');

    tx.execute("DROP TABLE IF EXISTS n", &[]).unwrap();
    tx.execute(&create, &[])?;

    Ok(())
}
