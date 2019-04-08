
use rusqlite::Transaction;

use crate::errors::AppResultU;
use crate::types::Type;



pub trait TxExt {
    fn create_table(&self, types: &[Type], header: &[&str]) -> AppResultU;
}

impl<'a> TxExt for Transaction<'a> {
    fn create_table(&self, types: &[Type], header: &[&str]) -> AppResultU {
        let mut create = "CREATE TABLE n (".to_owned();
        let mut first = true;
        for (i, name) in header.iter().enumerate() {
            let name = name.replace("'", "''");
            if first {
                first = false;
            } else {
                create.push(',');
            }
            let t = match types[i] {
                Type::Int => "integer",
                Type::Real => "real",
                Type::Text => "text",
            };
            create.push_str(&format!("'{}' {}", name, t));
        }
        create.push(')');

        self.execute("DROP TABLE IF EXISTS n", &[]).unwrap();
        self.execute(&create, &[])?;

        Ok(())
    }
}
