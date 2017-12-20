
use std::collections::HashSet;
use std::error::Error;
use std::io::Cursor;
use std::str::from_utf8;

use json_reader::LevelReader;
use rusqlite:: Transaction;
use rusqlite::types::ToSql;

use json;
use sql;
use ui;


const NAME_DELIMITER: char =  '_';
const LINES_FOR_HEADER: usize = 100;


pub fn header(content: &str) -> Result<Vec<String>, Box<Error>> {
    let mut names = HashSet::<String>::new();

    let mut buf = Cursor::new(content);
    let mut r = LevelReader::new(&mut buf, 0);
    let mut p = ui::Progress::new();

    while let Some(it) = r.next() {
        if LINES_FOR_HEADER < p.n {
            break;
        }

        p.progress();
        let it = it?;
        let it = from_utf8(&it)?;
        for name in column_names(it)? {
            names.insert(name);
        }
    }

    p.complete();

    Ok(names.into_iter().collect())
}

fn column_names(buf: &str) -> Result<Vec<String>, Box<Error>> {
    use json::JsonValue;

    fn load_object(prefix: &str, result: &mut Vec<String>, object: &json::object::Object) -> Result<(), Box<Error>> {
        for (n, v) in object.iter() {
            let mut new_prefix = prefix.to_string();
            if 0 < prefix.len() {
                new_prefix.push(NAME_DELIMITER);
            }
            new_prefix.push_str(n);

            match *v {
                JsonValue::Object(ref obj) => {
                    load_object(&new_prefix, result, obj)?;
                }
                _ => result.push(new_prefix),
            }
        }

        Ok(())
    }

    let mut result = Vec::<String>::new();
    let jv = json::parse(buf)?;
    match jv {
        JsonValue::Object(ref obj) => load_object("", &mut result, obj)?,
        _ => (),
    }
    Ok(result)
}

pub fn insert_rows(tx: &Transaction, content: &str) -> Result<(), Box<Error>> {
    let mut buf = Cursor::new(content);
    let mut r = LevelReader::new(&mut buf, 0);

    let mut p = ui::Progress::new();
    while let Some(it) = r.next() {
        p.progress();
        let it = it?;
        let it = from_utf8(&it)?;
        insert_row(tx, &it)?;
    }
    p.complete();

    Ok(())
}

pub fn insert_row(tx: &Transaction, buf: &str) -> Result<(), Box<Error>> {
    use json::JsonValue;

    fn load_object(prefix: &str, names: &mut String, values: &mut String, args: &mut Vec<String>, object: &json::object::Object) -> Result<(), Box<Error>> {
        for (n, v) in object.iter() {
            let mut new_prefix = prefix.to_string();
            if 0 < prefix.len() {
                new_prefix.push(NAME_DELIMITER);
            }
            new_prefix.push_str(n);

            let arg = match *v {
                JsonValue::Object(ref obj) => {
                    load_object(&new_prefix, names, values, args, obj)?;
                    continue;
                }
                JsonValue::String(ref v) => sql::quote_string(v),
                JsonValue::Short(ref v) => sql::quote_string(v),
                JsonValue::Number(ref v) => format!("{}", v),
                JsonValue::Boolean(ref v) => if *v { "1" } else { "0" } .to_string(),
                _ => continue,
            };

            if 0 < names.len() {
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

    let jv = json::parse(buf)?;
    match jv {
        JsonValue::Object(ref obj) => load_object("", &mut names, &mut values, &mut args, obj)?,
        _ => (),
    }

    let q = format!("INSERT INTO n ({}) VALUES ({})", names, values);
    let args: Vec<&ToSql> = args.iter().map(|it| it as &ToSql).collect();
    tx.execute(&q, &args)?;

    Ok(())
}
