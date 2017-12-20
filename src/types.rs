
extern crate mktemp;

use std::path::Path;



#[derive(Eq, PartialEq, Clone, Debug)]
pub enum Type {
    Int = 2,
    Real = 1,
    Text = 0
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
