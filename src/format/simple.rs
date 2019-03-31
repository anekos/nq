
extern crate quick_csv;

use std::error::Error;

use regex::Regex;
use rusqlite:: Transaction;
use rusqlite::types::ToSql;

use crate::ui;



pub struct Reader {
    pattern: Regex,
}


impl Reader {
    pub fn new() -> Result<Reader, Box<Error>> {
        Ok(Reader { pattern: Regex::new(r"[ \t]+")? })
    }

    pub fn header<'a>(&self, rows: &'a str) -> Result<Vec<&'a str>, &'static str> {
        let line = rows.lines().next().ok_or("No lines")?;
        Ok(self.split(line, None))
    }

    pub fn insert_rows(&self, tx: &Transaction, headers: usize, rows: &str) -> Result<(), Box<Error>> {
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
            p.progress();
            let row: Vec<&str> = self.split(row, Some(headers));
            let row: Vec<&ToSql> = row.iter().map(|it| it as &ToSql).collect();
            stmt.execute(row.as_slice())?;
        }
        p.complete();

        Ok(())
    }

    fn split<'a>(&self, s: &'a str, n: Option<usize>) -> Vec<&'a str> {
        if let Some(n) = n {
            self.pattern.splitn(s, n).collect()
        } else {
            self.pattern.split(s).collect()
        }
    }
}
