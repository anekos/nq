
extern crate mktemp;



#[derive(Eq, PartialEq, Clone, Debug)]
pub enum Type {
    Int = 2,
    Real = 1,
    Text = 0
}

#[derive(Clone, Debug)]
pub enum Format {
    Csv(Option<u8>),
    Json,
    Ltsv,
    Regex(String),
    Simple,
}

pub enum Input<'a> {
    File(&'a str),
    Stdin,
}


impl Format {
    pub fn to_sql_literal(&self) -> String {
        format!("{:?}", self)
    }
}

impl Type {
    pub fn new(size: usize) -> Vec<Type> {
        let mut types: Vec<Type> = vec![];
        types.resize(size, Type::Text);
        types
    }
}
