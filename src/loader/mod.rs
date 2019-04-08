
use rusqlite::Transaction;

use crate::errors::AppResultU;

mod csv;
mod json;
mod ltsv;
mod simple;

pub use csv::{Loader as Csv};
pub use json::{Loader as Json};
pub use ltsv::{Loader as Ltsv};
pub use simple::{Loader as Simple};



pub struct Config {
    pub guess_lines: Option<usize>,
    pub no_headers: bool,
}

pub trait Loader {
    fn load(&self, tx: &Transaction, source: &str, config: &Config) -> AppResultU;
}
