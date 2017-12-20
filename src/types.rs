
extern crate mktemp;

use std::path::Path;



#[derive(Eq, PartialEq, Clone, Debug)]
pub enum Type {
    Int = 2,
    Real = 1,
    Text = 0
}


pub enum Format {
    Csv(Option<u8>),
    Json,
    Ltsv,
}


pub enum Input<'a> {
    File(&'a str),
    Stdin,
}

pub enum Cache {
    File(String),
    Temp(mktemp::Temp),
}

impl AsRef<Path> for Cache {
    fn as_ref(&self) -> &Path {
        match *self {
            Cache::File(ref path) => Path::new(path),
            Cache::Temp(ref path) => path.as_ref(),
        }
    }
}

impl Type {
    pub fn new(size: usize) -> Vec<Type> {
        let mut types: Vec<Type> = vec![];
        types.resize(size, Type::Text);
        types
    }
}
