
extern crate encoding;
extern crate quick_csv;
extern crate rusqlite;

use std::env::args;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::process::exit;

use rusqlite::Connection;
use rusqlite::types::ToSql;


fn main() {
    if let Err(err) = nq() {
        println!("Error: {}", err);
        exit(1);
    }
}


fn nq() -> Result<(), Box<Error>> {
    let csv_file = args().nth(1).ok_or("No CSV path")?;
    let sqlite_file = args().nth(2);

    let mut buffer = String::new();

    let mut csv = {
        let mut file = File::open(csv_file)?;
        file.read_to_string(&mut buffer)?;
        quick_csv::Csv::from_string(&buffer)
    };

    let header = csv.next().ok_or("Header not found")??;
    let header: Vec<&str> = header.columns()?.collect();

    let mut conn =
        if let Some(sqlite_file) = sqlite_file {
            Connection::open(sqlite_file)?
        } else {
            Connection::open_in_memory()?
        };

    let tx = conn.transaction()?;

    {
        tx.execute("DROP TABLE IF EXISTS rows", &[]).unwrap();
        let mut create = "CREATE TABLE rows (".to_owned();
        let mut first = true;
        for name in &header {
            let name = name.replace("'", "''");
            if first {
                first = false;
            } else {
                create.push(',');
            }
            create.push_str(&format!("'{}' text", name));
        }
        create.push(')');
        tx.execute(&create, &[]).unwrap();
    }

    let insert = {
        let mut insert = "INSERT INTO rows VALUES(".to_owned();
        let mut first = true;
        for _ in header {
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

    {
        let mut stmt = tx.prepare(&insert)?;
        let mut n = 0;
        for row in csv.into_iter() {
            n += 1;
            if n % 100 == 0 {
                println!("{} rows", n);
            }
            if let Ok(row) = row {
                let row: Vec<&str> = row.columns()?.collect();
                let row: Vec<&ToSql> = row.iter().map(|it| it as &ToSql).collect();
                stmt.execute(row.as_slice())?;
            }
        }
        if n % 100 != 0 {
            println!("{} rows", n);
        }
    }

    tx.commit()?;

    Ok(())
}
