
#[macro_use] extern crate serde_derive;
extern crate docopt;
extern crate encoding;
extern crate quick_csv;
extern crate regex;
extern crate rusqlite;


use std::collections::HashSet;
use std::error::Error;
use std::fs::{File, metadata};
use std::io::Read;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{exit, Command};

use docopt::Docopt;
use encoding::DecoderTrap;
use encoding::label::encoding_from_whatwg_label;
use quick_csv::Csv;
use regex::Regex;
use rusqlite::types::ToSql;
use rusqlite::{Connection, Transaction};


#[derive(Eq, PartialEq, Clone, Debug)]
enum Type {
    Int = 2,
    Real = 1,
    Text = 0
}

const USAGE: &'static str = "
not q

Usage:
  nq [options] <csv> [-- <sqlite-options>...]
  nq (-h | --help)
  nq --version

Options:
  -c CACHE      Cache *.sqlite
  -d DELIMITER  Format: Delimter for csv
  -e ENCODING   CSV character encoding: https://encoding.spec.whatwg.org/#concept-encoding-get
  -g LINES      The number of rows for guess column types (defualt: 42)
  -l            Format: LTSV
  -q SQL        SQL
  -R            Force refresh cache
  -h --help     Show this screen.
  --version     Show version.
";


#[derive(Debug, Deserialize)]#[allow(non_snake_case)]
struct AppOptions {
    arg_csv: String,
    flag_c: Option<String>,
    flag_d: Option<char>,
    flag_e: Option<String>,
    flag_g: Option<usize>,
    flag_l: bool,
    flag_q: Option<String>,
    flag_R: bool,
    arg_sqlite_options: Vec<String>,
}


fn main() {
    if let Err(err) = nq() {
        println!("Error: {}", err);
        exit(1);
    }
}


fn nq() -> Result<(), Box<Error>> {
    let options: AppOptions = Docopt::new(USAGE).and_then(|d| d.deserialize()).unwrap_or_else(|e| e.exit());

    let cache_filepath = make_sqlite(&options.arg_csv, &options.flag_c)?;
    let cache_is_fresh = !options.flag_R && is_fresh(&options.arg_csv, &cache_filepath)?;

    if !cache_is_fresh {
        let csv_text = read_file(&options.arg_csv, &options.flag_e)?;

        let mut conn = Connection::open(&cache_filepath)?;
        let tx = conn.transaction()?;

        if options.flag_l {
            let header = ltsv_header(&csv_text)?;
            let mut types: Vec<Type> = vec![];
            types.resize(header.len(), Type::Text);
            create_table(&tx, &types, header.as_slice())?;
            insert_ltsv_rows(&tx, &csv_text)?;
        } else {
            let mut csv = open_csv(&csv_text, &options.flag_d)?;
            let header = csv.next().ok_or("Header not found")??;
            let header = header.columns()?.collect::<Vec<&str>>();
            let mut types: Vec<Type> = vec![];
            types.resize(header.len(), Type::Int);
            if let Some(lines) = options.flag_g {
                let mut csv = open_csv(&csv_text, &options.flag_d)?;
                csv.next().ok_or("No header")??;
                guess_types(&mut types, lines, csv)?
            }

            create_table(&tx, &types, header.as_slice())?;
            insert_csv_rows(&tx, header.len(), csv)?;
        }

        tx.commit()?;
    }

    exec_sqlite(&cache_filepath, &options.flag_q, &options.arg_sqlite_options);

    Ok(())
}


fn make_sqlite(sqlite_filepath: &str, cache_filepath: &Option<String>) -> Result <String, Box<Error>> {
    match *cache_filepath {
        Some(ref path) => Ok(path.clone()),
        None => {
            let mut path = sqlite_filepath.to_owned();
            path.push_str(".nq-cache.sqlite");
            Ok(path)
        }
    }
}

fn read_file(csv_filepath: &str, encoding: &Option<String>) -> Result<String, Box<Error>> {
    let mut buffer = String::new();
    let mut file = File::open(csv_filepath)?;

    if let Some(ref encoding) = *encoding {
        let encoding = encoding_from_whatwg_label(encoding).ok_or("Invalid encoding name")?;
        let mut bin: Vec<u8> = vec![];
        file.read_to_end(&mut bin)?;
        buffer = match encoding.decode(&bin, DecoderTrap::Replace) {
            Ok(s) => s,
            Err(s) => s.to_string(),
        };
    } else {
        file.read_to_string(&mut buffer)?;
    }

    Ok(buffer)
}

fn open_csv<'a>(csv_text: &'a str, delimiter: &Option<char>) -> Result<Csv<&'a [u8]>, Box<Error>> {
    let mut csv = quick_csv::Csv::from_string(csv_text);
    if let Some(delimiter) = *delimiter {
        csv = csv.delimiter(delimiter as u8);
    }
    Ok(csv)
}

fn is_fresh(csv_filepath: &str, sqlite_filepath: &str) -> Result<bool, Box<Error>> {
    if !Path::new(sqlite_filepath).exists() {
        return Ok(false)
    }
    let csv = metadata(csv_filepath)?.modified()?;
    let sqlite = metadata(sqlite_filepath)?.modified()?;
    Ok(csv < sqlite)
}

fn guess_types(types: &mut Vec<Type>, lines: usize, rows: Csv<&[u8]>) -> Result<(), Box<Error>> {
    if 0 == lines {
        return Ok(());
    }

    let int = Regex::new("^[-+]?\\d{1,18}$")?;
    let real = Regex::new("^[-+]?\\d+\\.\\d+$")?;

    for row in rows.into_iter().take(lines) {
        let row = row?;
        let columns: Vec<&str> = row.columns()?.collect();
        for (lv, column) in columns.iter().enumerate() {
            let types = &mut types[lv];
            if *types == Type::Int && !int.is_match(column) {
                *types = Type::Real;
            }
            if *types == Type::Real && !real.is_match(column) {
                *types = Type::Text;
            }
        }
    }

    Ok(())
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

fn insert_csv_rows(tx: &Transaction, headers: usize, rows: Csv<&[u8]>) -> Result<(), Box<Error>> {
    let insert = {
        let mut insert = "INSERT INTO n VALUES(".to_owned();
        let mut first = true;
        for _ in 0..headers {
            if first {
                insert.push_str("?");
                first = false;
            } else {
                insert.push_str(",?");
            }
        }
        insert.push_str(")");
        insert
    };

    let mut stmt = tx.prepare(&insert)?;
    let mut n = 0;
    for row in rows {
        n += 1;
        progress(n, false);
        let row = row?;
        let row: Vec<&str> = row.columns()?.collect();
        let row: Vec<&ToSql> = row.iter().map(|it| it as &ToSql).collect();
        stmt.execute(row.as_slice())?;
    }
    progress(n, true);

    Ok(())
}

fn ltsv_header(content: &str) -> Result<Vec<&str>, Box<Error>> {
    let mut names = HashSet::<&str>::new();

    for row in content.lines() {
        for column in row.split('\t') {
            if let Some(idx) = column.find(':') {
                if idx == 0 {
                    continue;
                }
                let (name, _) = column.split_at(idx);
                if !names.contains(name) {
                    names.insert(name);
                }
            }
        }
    }

    Ok(names.into_iter().collect())
}

fn insert_ltsv_rows(tx: &Transaction, content: &str) -> Result<(), Box<Error>> {
    let mut n = 0;

    for row in content.lines() {
        n += 1;
        progress(n, false);

        let mut names = String::new();
        let mut values = String::new();
        let mut args = Vec::<&str>::new();

        for (index, column) in row.split('\t').enumerate() {
            if let Some(idx) = column.find(':') {
                if idx == 0 && column.len() <= 1{
                    continue;
                }
                let (name, value) = column.split_at(idx);
                let value = &value[1..];

                if 0 < index {
                    names.push(',');
                    values.push(',');
                }

                let name = name.replace("'", "''");
                names.push_str(&format!("'{}'", name));
                values.push('?');

                args.push(value);
            }
        }

        let q = format!("INSERT INTO n ({}) VALUES ({})", names, values);
        let args: Vec<&ToSql> = args.iter().map(|it| it as &ToSql).collect();
        tx.execute(&q, &args)?;
    }

    progress(n, true);

    Ok(())
}

fn exec_sqlite(sqlite_filepath: &str, query: &Option<String>, options: &[String]) {
    let mut cmd = Command::new("sqlite3");
    cmd.arg(sqlite_filepath);
    cmd.args(options);
    if let Some(ref query) = *query {
        cmd.arg(query);
    }
    cmd.exec();
}

fn progress(n: usize, last: bool) {
    let just = n % 100 == 0;
    if last ^ just {
        eprintln!("{} rows", n);
    }
}
