
use std::convert::AsRef;

use rusqlite::{NO_PARAMS, Transaction};

use crate::errors::AppResultU;
use crate::types::Type;



pub trait TxExt {
    fn create_table<T: AsRef<str>>(&self, types: &[Type], header: &[T]) -> AppResultU;
}

impl<'a> TxExt for Transaction<'a> {
    fn create_table<T: AsRef<str>>(&self, types: &[Type], header: &[T]) -> AppResultU {
        let mut create = "CREATE TABLE n (".to_owned();
        let mut first = true;
        for (i, name) in header.iter().enumerate() {
            let name = name.as_ref().replace("'", "''");
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

        self.execute("DROP TABLE IF EXISTS n", NO_PARAMS).unwrap();
        self.execute(&create, NO_PARAMS)?;

        Ok(())
    }
}
