
use std::io::{BufRead, Seek};

extern crate quick_csv;

use regex::Regex;
use rusqlite:: Transaction;
use rusqlite::types::ToSql;

use crate::db::TxExt;
use crate::errors::{AppError, AppResult, AppResultU};
use crate::types::Type;
use crate::ui;



pub struct Loader {
    pub delimiter: Regex,
}


impl super::Loader for Loader {
    fn load<T: BufRead + Seek>(&self, tx: &Transaction, source: &mut T, config: &super::Config) -> AppResultU {
        let header = self.header(source, config.no_header)?;
        let types = Type::new(header.len());
        tx.create_table(&types, header.as_slice())?;
        self.insert_rows(tx, header.len(), source)?;
        Ok(())
    }
}

impl Loader {
    fn header<T: BufRead + Seek>(&self, rows: &mut T, no_header: bool) -> AppResult<Vec<String>> {
        let line = rows.lines().next().ok_or(AppError::Fixed("No lines"))?;
        let line = line?;
        let columns = self.split(&line, None);
        let result = if no_header {
            super::alpha_header(columns.len())
        } else {
            columns
        };
        let result = result.into_iter().map(|it| it.to_owned()).collect();
        Ok(result)
    }

    fn insert_rows<T: BufRead + Seek>(&self, tx: &Transaction, headers: usize, rows: &mut T) -> AppResultU {
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
        for row in rows.lines().skip(1) {
            let row = row?;
            p.progress();
            let row: Vec<&str> = self.split(&row, Some(headers));
            let row: Vec<&ToSql> = row.iter().map(|it| it as &ToSql).collect();
            stmt.execute(row.as_slice())?;
        }
        p.complete();

        Ok(())
    }

    fn split<'a>(&self, s: &'a str, n: Option<usize>) -> Vec<&'a str> {
        if let Some(n) = n {
            self.delimiter.splitn(s, n).collect()
        } else {
            self.delimiter.split(s).collect()
        }
    }

}
