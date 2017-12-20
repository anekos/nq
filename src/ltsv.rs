
use std::collections::HashSet;
use std::error::Error;

use rusqlite::types::ToSql;
use rusqlite:: Transaction;

use ui::progress;



pub fn header(content: &str) -> Result<Vec<&str>, Box<Error>> {
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

pub fn insert_rows(tx: &Transaction, content: &str) -> Result<(), Box<Error>> {
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
