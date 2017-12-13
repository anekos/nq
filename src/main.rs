
#[macro_use] extern crate serde_derive;
extern crate docopt;
extern crate encoding;
extern crate quick_csv;
extern crate rusqlite;


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
use rusqlite::types::ToSql;
use rusqlite::{Connection, Transaction};



const USAGE: &'static str = "
not q

Usage:
  nq [options] <csv> [-- <sqlite-options>...]
  nq (-h | --help)
  nq --version

Options:
  -e ENCODING   CSV character encoding: https://encoding.spec.whatwg.org/#concept-encoding-get
  -q SQL        SQL
  -c CACHE      Cache *.sqlite
  -h --help     Show this screen.
  --version     Show version.
";


#[derive(Debug, Deserialize)]
struct AppOptions {
    arg_csv: String,
    flag_c: Option<String>,
    flag_q: Option<String>,
    flag_e: Option<String>,
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
    let cache_is_fresh = is_fresh(&options.arg_csv, &cache_filepath)?;

    if !cache_is_fresh {
        let mut buffer = String::new();
        let mut csv = open_csv(&options.arg_csv, &mut buffer, &options.flag_e)?;

        let header = csv.next().ok_or("Header not found")??;
        let header = header.columns()?.collect::<Vec<&str>>();

        let mut conn = Connection::open(&cache_filepath)?;
        let tx = conn.transaction()?;

        create_table(&tx, header.as_slice())?;
        insert_rows(&tx, header.len(), csv)?;

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

fn open_csv<'a>(csv_filepath: &str, buffer: &'a mut String, encoding: &Option<String>) -> Result<Csv<&'a [u8]>, Box<Error>> {
    let mut file = File::open(csv_filepath)?;

    if let Some(ref encoding) = *encoding {
        let encoding = encoding_from_whatwg_label(encoding).ok_or("Invalid encoding name")?;
        let mut bin: Vec<u8> = vec![];
        file.read_to_end(&mut bin)?;
        *buffer = match encoding.decode(&bin, DecoderTrap::Replace) {
            Ok(s) => s,
            Err(s) => s.to_string(),
        };
    } else {
        file.read_to_string(buffer)?;
    }
    Ok(quick_csv::Csv::from_string(buffer))
}

fn is_fresh(csv_filepath: &str, sqlite_filepath: &str) -> Result<bool, Box<Error>> {
    if !Path::new(sqlite_filepath).exists() {
        return Ok(false)
    }
    let csv = metadata(csv_filepath)?.modified()?;
    let sqlite = metadata(sqlite_filepath)?.modified()?;
    Ok(csv < sqlite)
}

fn create_table(tx: &Transaction, header: &[&str]) -> Result<(), Box<Error>> {
    let mut create = "CREATE TABLE rows (".to_owned();
    let mut first = true;
    for name in header {
        let name = name.replace("'", "''");
        if first {
            first = false;
        } else {
            create.push(',');
        }
        create.push_str(&format!("'{}' text", name));
    }
    create.push(')');

    tx.execute("DROP TABLE IF EXISTS rows", &[]).unwrap();
    tx.execute(&create, &[])?;

    Ok(())
}

fn insert_rows(tx: &Transaction, headers: usize, rows: Csv<&[u8]>) -> Result<(), Box<Error>> {
    let insert = {
        let mut insert = "INSERT INTO rows VALUES(".to_owned();
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
        if n % 100 == 0 {
            eprintln!("{} rows", n);
        }
        if let Ok(row) = row {
            let row: Vec<&str> = row.columns()?.collect();
            let row: Vec<&ToSql> = row.iter().map(|it| it as &ToSql).collect();
            stmt.execute(row.as_slice())?;
        }
    }
    if n % 100 != 0 {
        eprintln!("{} rows", n);
    }

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
