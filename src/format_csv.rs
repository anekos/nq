
extern crate quick_csv;

use std::error::Error;

use quick_csv::Csv;
use regex::Regex;
use rusqlite:: Transaction;
use rusqlite::types::ToSql;

use ui;
use types::*;



pub fn open<'a>(csv_text: &'a str, delimiter: Option<u8>) -> Result<Csv<&'a [u8]>, Box<Error>> {
    let mut csv = quick_csv::Csv::from_string(csv_text);
    if let Some(delimiter) = delimiter {
        csv = csv.delimiter(delimiter);
    }
    Ok(csv)
}

pub fn guess_types(types: &mut Vec<Type>, lines: usize, rows: Csv<&[u8]>) -> Result<(), Box<Error>> {
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

pub fn insert_rows(tx: &Transaction, headers: usize, rows: Csv<&[u8]>) -> Result<(), Box<Error>> {
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

    let mut p = ui::Progress::new();
    let mut stmt = tx.prepare(&insert)?;
    for row in rows {
        p.progress();
        let row = row?;
        let row: Vec<&str> = row.columns()?.collect();
        let row: Vec<&ToSql> = row.iter().map(|it| it as &ToSql).collect();
        stmt.execute(row.as_slice())?;
    }
    p.complete();

    Ok(())
}

