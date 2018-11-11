
use std::collections::HashSet;
use std::error::Error;

use rusqlite:: Transaction;
use rusqlite::types::ToSql;
use serde_json::{Deserializer, Value, Map};

use sql;
use ui;


const NAME_DELIMITER: char =  '_';
const LINES_FOR_HEADER: usize = 100;


type ObjMap = Map<String, Value>;


pub fn header(content: &str) -> Result<Vec<String>, Box<Error>> {
    let mut names = HashSet::<String>::new();

    let stream = Deserializer::from_str(content).into_iter::<Value>();
    let mut p = ui::Progress::new();

    for it in stream {
        if LINES_FOR_HEADER < p.n {
            break;
        }
        p.progress();
        for name in column_names(&it?)? {
            names.insert(name);
        }
    }

    p.complete();

    Ok(names.into_iter().collect())
}

fn column_names(value: &Value) -> Result<Vec<String>, Box<Error>> {
    fn load_object(prefix: &str, result: &mut Vec<String>, object: &ObjMap) -> Result<(), Box<Error>> {
        for (n, v) in object.iter() {
            let mut new_prefix = prefix.to_string();
            if !prefix.is_empty() {
                new_prefix.push(NAME_DELIMITER);
            }
            new_prefix.push_str(n);

            match *v {
                Value::Object(ref obj) => {
                    load_object(&new_prefix, result, obj)?;
                }
                _ => result.push(new_prefix),
            }
        }

        Ok(())
    }

    let mut result = Vec::<String>::new();
    if let Value::Object(ref obj) = *value {
        load_object("", &mut result, obj)?;
    }
    Ok(result)
}

pub fn insert_rows(tx: &Transaction, content: &str) -> Result<(), Box<Error>> {
    let stream = Deserializer::from_str(content).into_iter::<Value>();

    let mut p = ui::Progress::new();
    for it in stream {
        p.progress();
        if let Value::Object(ref obj) = it? {
            insert_row(tx, obj)?;
        }
    }
    p.complete();

    Ok(())
}

pub fn insert_row(tx: &Transaction, obj: &ObjMap) -> Result<(), Box<Error>> {
    fn load_object(prefix: &str, names: &mut String, values: &mut String, args: &mut Vec<String>, object: &ObjMap) -> Result<(), Box<Error>> {
        for (n, v) in object.iter() {
            let mut new_prefix = prefix.to_string();
            if !prefix.is_empty() {
                new_prefix.push(NAME_DELIMITER);
            }
            new_prefix.push_str(n);

            let arg = match *v {
                Value::Object(ref obj) => {
                    load_object(&new_prefix, names, values, args, obj)?;
                    continue;
                }
                Value::String(ref v) => sql::quote_string(v),
                Value::Number(ref v) => format!("{}", v),
                Value::Bool(ref v) => if *v { "1" } else { "0" } .to_string(),
                _ => continue,
            };

            if !names.is_empty() {
                names.push(',');
                values.push(',');
            }

            names.push_str(&sql::quote_string(&new_prefix));
            values.push('?');
            args.push(arg);
        }

        Ok(())
    }

    let mut names = String::new();
    let mut values = String::new();
    let mut args = Vec::<String>::new();

    load_object("", &mut names, &mut values, &mut args, obj)?;

    let q = format!("INSERT INTO n ({}) VALUES ({})", names, values);
    let args: Vec<&ToSql> = args.iter().map(|it| it as &ToSql).collect();
    tx.execute(&q, &args)?;

    Ok(())
}
