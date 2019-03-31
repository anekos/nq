
extern crate mktemp;



#[derive(Eq, PartialEq, Clone, Debug)]
pub enum Type {
    Int = 2,
    Real = 1,
    Text = 0
}

#[derive(Clone, Copy)]
pub enum Format {
    Csv(Option<u8>),
    Json,
    Ltsv,
    Simple,
}

pub enum Input<'a> {
    File(&'a str),
    Stdin,
}



impl Type {
    pub fn new(size: usize) -> Vec<Type> {
        let mut types: Vec<Type> = vec![];
        types.resize(size, Type::Text);
        types
    }
}
