
use std::io::{BufRead, Seek};

use regex::Regex;
use rusqlite:: Transaction;
use rusqlite::types::ToSql;

use crate::db::TxExt;
use crate::errors::{AppError, AppResultU};
use crate::types::Type;
use crate::ui;



pub struct Loader {
    pub format: Regex,
}


impl super::Loader for Loader {
    fn load<T: BufRead + Seek>(&self, tx: &Transaction, source: &mut T, _: &super::Config) -> AppResultU {
        self.insert_rows(tx, source)?;
        Ok(())
    }
}

impl Loader {
    fn insert_rows<T: BufRead + Seek>(&self, tx: &Transaction, content: &mut T) -> AppResultU {
        let mut p = ui::Progress::new();

        let mut header = None;
        let mut insert = None;

        for row in content.lines() {
            let row = row?;

            p.progress();

            if let Some(matches) = self.format.captures(&row) {
                if header.is_none() {
                    let h = super::alpha_header(matches.len() - 1);
                    let types = Type::new(h.len());
                    tx.create_table(&types, h.as_slice())?;
                    insert = Some(super::insert_values(h.len()));
                    header = Some(h);
                }

                let mut values = Vec::<&str>::new();
                for i in 1 ..= header.as_ref().expect("BUG").len() {
                    values.push(matches.get(i).ok_or(AppError::FewColumns)?.as_str());
                }
                let args: Vec<&ToSql> = values.iter().map(|it| it as &ToSql).collect();
                tx.execute(insert.as_ref().expect("BUG"), &args)?;
            } else {
                eprintln!("Skip: {}", row);
            }
        }

        p.complete();

        Ok(())
    }

}
