
use rusqlite::Transaction;

use crate::errors::AppResultU;

mod csv;
mod json;
mod ltsv;
mod simple;
mod regex;

pub use csv::{Loader as Csv};
pub use json::{Loader as Json};
pub use ltsv::{Loader as Ltsv};
pub use self::regex::{Loader as Regex};
pub use simple::{Loader as Simple};



pub struct Config {
    pub guess_lines: Option<usize>,
    pub no_header: bool,
}

pub trait Loader {
    fn load(&self, tx: &Transaction, source: &str, config: &Config) -> AppResultU;
}


pub fn alpha_header(n: usize) -> Vec<&'static str> {
    const ALPHAS: &[&str] = &[
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p",
        "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "A", "B", "C", "D", "E", "F",
        "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
        "W", "X", "Y", "Z"
    ];

    let mut result = vec![];
    for alpha in ALPHAS.iter().take(n) {
        result.push(*alpha);
    }
    result
}

pub fn qs(n: usize) -> String {
    let mut result = "".to_owned();
    for i in 0 .. n {
        if i == 0 {
            result.push('?');
        } else {
            result.push_str(",?");
        }
    }
    result
}

pub fn insert_values(n: usize) -> String {
    format!("INSERT INTO n VALUES({})", qs(n))
}
