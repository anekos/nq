


pub fn quote_string(s: &str) -> String {
    let s = s.replace("'", "''");
    format!("'{}'", s)
}
