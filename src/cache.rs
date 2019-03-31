use std::error::Error;
use std::fs::File;
use std::fs::metadata;
use std::io::{self, Read};
use std::path::Path;

use encoding::DecoderTrap;
use encoding::label::encoding_from_whatwg_label;
use rusqlite::{Transaction, Connection};

use crate::format;
use crate::types::*;



const ALPHAS: &[&str] = &[
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
    "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "A", "B", "C", "D", "E", "F",
    "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
    "W", "X", "Y", "Z"
];


pub fn is_fresh(input: &Input, cache: &Cache) -> Result<bool, Box<Error>> {
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
                _ => Ok(false),
            }
        }
    }
}

pub fn refresh(cache: &Cache, format: Format, input: &Input, no_headers: bool, guess_lines: Option<usize>, encoding: &Option<String>) -> Result<(), Box<Error>> {
    let csv_text = read_file(input, encoding)?;

    let mut conn = Connection::open(cache)?;
    let tx = conn.transaction()?;

    match format {
        Format::Ltsv => {
            let header = format::ltsv::header(&csv_text)?;
            let types = Type::new(header.len());
            create_table(&tx, &types, header.as_slice())?;
            format::ltsv::insert_rows(&tx, &csv_text)?;
        }
        Format::Json => {
            let header = format::json::header(&csv_text)?;
            let header: Vec<&str> = header.iter().map(|it| it.as_ref()).collect();
            let types = Type::new(header.len());
            create_table(&tx, &types, header.as_slice())?;
            format::json::insert_rows(&tx, &csv_text)?;
        }
        Format::Simple => {
            let reader = format::simple::Reader::new()?;
            let header = reader.header(&csv_text)?;
            let types = Type::new(header.len());
            create_table(&tx, &types, header.as_slice())?;
            reader.insert_rows(&tx, header.len(), &csv_text)?;
        }
        Format::Csv(delimiter) => {
            let mut content = format::csv::open(&csv_text, delimiter)?;
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
                let mut content = format::csv::open(&csv_text, delimiter)?;
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

fn alpha_header(n: usize) -> Vec<&'static str> {
    let mut result = vec![];
    for alpha in ALPHAS.iter().take(n) {
        result.push(*alpha);
    }
    result
}


pub fn create_table(tx: &Transaction, types: &[Type], header: &[&str]) -> Result<(), Box<Error>> {
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
