
use std::collections::HashSet;

use rusqlite::types::ToSql;
use rusqlite:: Transaction;

use crate::db::TxExt;
use crate::errors::{AppResult, AppResultU};
use crate::types::Type;
use crate::ui;



pub struct Loader();


impl super::Loader for Loader {
    fn load(&self, tx: &Transaction, source: &str, _: &super::Config) -> AppResultU {
        let header = header(&source)?;
        let types = Type::new(header.len());
        tx.create_table(&types, header.as_slice())?;
        insert_rows(&tx, &source)?;
        Ok(())
    }
}


fn header(content: &str) -> AppResult<Vec<&str>> {
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

fn insert_rows(tx: &Transaction, content: &str) -> AppResultU {
    let mut p = ui::Progress::new();

    for row in content.lines() {
        p.progress();

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
        let args: Vec<&dyn ToSql> = args.iter().map(|it| it as &dyn ToSql).collect();
        tx.execute(&q, &*args)?;
    }

    p.complete();

    Ok(())
}
