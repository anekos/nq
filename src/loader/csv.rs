
use quick_csv::Csv;
use regex::Regex;
use rusqlite:: Transaction;
use rusqlite::types::ToSql;

use crate::db::TxExt;
use crate::errors::{AppResult, AppResultU};
use crate::ui;
use crate::types::*;



pub struct Loader {
    pub delimiter: Option<u8>,
}


impl super::Loader for Loader {
    fn load(&self, tx: &Transaction, source: &str, config: &super::Config) -> AppResultU {
        let mut content = open(&source, self.delimiter)?;
        let header = content.nth(0).ok_or("Header not found")??;
        let header = if config.no_headers {
            let columns = header.len();
            super::alpha_header(columns)
        } else {
            let _ = content.next();
            header.columns()?.collect::<Vec<&str>>()
        };
        let mut types: Vec<Type> = vec![];
        types.resize(header.len(), Type::Int);
        if let Some(lines) = config.guess_lines {
            let mut content = open(&source, self.delimiter)?;
            content.next().ok_or("No header")??;
            guess_types(&mut types, lines, content)?
        }

        tx.create_table(&types, header.as_slice())?;
        insert_rows(&tx, header.len(), content, &types)?;
        Ok(())
    }
}


fn guess_types(types: &mut Vec<Type>, lines: usize, rows: Csv<&[u8]>) -> AppResultU {
    if 0 == lines {
        return Ok(());
    }

    let int = Regex::new("^[-+]?\\d{1,18}$")?;
    let real = Regex::new("^[-+]?\\d+\\.\\d+$")?;

    for row in rows.take(lines) {
        let row = row?;
        let columns: Vec<&str> = row.columns()?.collect();
        for (lv, column) in columns.iter().enumerate() {
            let cleaned = column.replace(',', "");
            let types = &mut types[lv];
            if *types == Type::Int && !int.is_match(&cleaned) {
                *types = Type::Real;
            }
            if *types == Type::Real && !real.is_match(&cleaned) {
                *types = Type::Text;
            }
        }
    }

    Ok(())
}

fn insert_rows(tx: &Transaction, headers: usize, rows: Csv<&[u8]>, types: &[Type]) -> AppResultU {
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
        let row: Vec<String> = row.columns()?.enumerate().map(|(index, it)| {
            use Type::*;

            if let Some(t) = types.get(index) {
                match t {
                    Real | Int => it.replace(',', ""),
                    _ => it.to_owned()
                }
            } else {
                it.to_owned()
            }
        }).collect();
        let row: Vec<&ToSql> = row.iter().map(|it| it as &ToSql).collect();
        stmt.execute(row.as_slice())?;
    }
    p.complete();

    Ok(())
}

fn open<'a>(csv_text: &'a str, delimiter: Option<u8>) -> AppResult<Csv<&'a [u8]>> {
    let mut csv = quick_csv::Csv::from_string(csv_text);
    if let Some(delimiter) = delimiter {
        csv = csv.delimiter(delimiter);
    }
    Ok(csv)
}
