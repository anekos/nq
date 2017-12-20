
#[macro_use] extern crate serde_derive;
extern crate docopt;
extern crate encoding;
extern crate mktemp;
extern crate quick_csv;
extern crate regex;
extern crate rusqlite;

use std::env;
use std::error::Error;
use std::fs::{File, metadata};
use std::io::{self, Read};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{exit, Command};

use encoding::DecoderTrap;
use encoding::label::encoding_from_whatwg_label;
use rusqlite::{Connection, Transaction};

mod types;
mod ui;
mod ltsv;
mod csv;
mod app_options;

use types::*;



fn main() {
    if let Err(err) = nq() {
        println!("Error: {}", err);
        exit(1);
    }
}


fn nq() -> Result<(), Box<Error>> {
    let options = app_options::parse();

    let input = parse_input(&options.arg_csv);
    let cache = make_sqlite(&input, &options.flag_c)?;
    let cache_is_fresh = !options.flag_R && is_fresh(&input, &cache)?;

    if !cache_is_fresh {
        let csv_text = read_file(&input, &options.flag_e)?;

        let mut conn = Connection::open(&cache)?;
        let tx = conn.transaction()?;

        if options.flag_l {
            let header = ltsv::header(&csv_text)?;
            let mut types: Vec<Type> = vec![];
            types.resize(header.len(), Type::Text);
            create_table(&tx, &types, header.as_slice())?;
            ltsv::insert_rows(&tx, &csv_text)?;
        } else {
            let mut content = csv::open(&csv_text, &options.flag_d)?;
            let header = content.next().ok_or("Header not found")??;
            let header = header.columns()?.collect::<Vec<&str>>();
            let mut types: Vec<Type> = vec![];
            types.resize(header.len(), Type::Int);
            if let Some(lines) = options.flag_g {
                let mut content = csv::open(&csv_text, &options.flag_d)?;
                content.next().ok_or("No header")??;
                csv::guess_types(&mut types, lines, content)?
            }

            create_table(&tx, &types, header.as_slice())?;
            csv::insert_rows(&tx, header.len(), content)?;
        }

        tx.commit()?;
    }

    exec_sqlite(&cache, &options.flag_q, &options.arg_sqlite_options);

    Ok(())
}

fn parse_input<'a>(filepath: &'a str) -> Input<'a> {
    match filepath {
        "-" => Input::Stdin,
        _ => Input::File(filepath)
    }
}

fn make_sqlite(input: &Input, cache_filepath: &Option<String>) -> Result <Cache, Box<Error>> {
    match *input {
        Input::Stdin => Ok(Cache::Temp(mktemp::Temp::new_file()?)),
        Input::File(ref input_path) => {
            match *cache_filepath {
                Some(ref path) => Ok(Cache::File(path.clone())),
                None => {
                    let mut path = input_path.to_string();
                    path.push_str(".nq-cache.sqlite");
                    Ok(Cache::File(path))
                }
            }
        }
    }
}

fn read_file(input: &Input, encoding: &Option<String>) -> Result<String, Box<Error>> {
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


fn is_fresh(input: &Input, cache: &Cache) -> Result<bool, Box<Error>> {
    match *input {
        Input::Stdin => Ok(false),
        Input::File(input_filepath) => {
            match *cache {
                Cache::File(ref cache_filepath) => {
                    if !Path::new(cache_filepath).exists() {
                        return Ok(false)
                    }
                    let input = metadata(input_filepath)?.modified()?;
                    let cache = metadata(cache_filepath)?.modified()?;
                    Ok(input < cache)
                },
                _ => panic!("Not implemented"),
            }
        }
    }
}


fn create_table(tx: &Transaction, types: &[Type], header: &[&str]) -> Result<(), Box<Error>> {
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
