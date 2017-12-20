
use docopt::Docopt;

use types::*;



const USAGE: &'static str = "
not q

Usage:
  nq [options] <csv> [-- <sqlite-options>...]
  nq (-h | --help)
  nq --version

Options:
  -c CACHE      Cache *.sqlite
  -d DELIMITER  Format: Delimter for CSV
  -e ENCODING   CSV character encoding: https://encoding.spec.whatwg.org/#concept-encoding-get
  -g LINES      The number of rows for guess column types (defualt: 42)
  -j            Format: JSON
  -l            Format: LTSV
  -q SQL        SQL
  -R            Force refresh cache
  -h --help     Show this screen.
  --version     Show version.
";

#[derive(Debug, Deserialize)]#[allow(non_snake_case)]
pub struct AppOptions {
    pub arg_csv: String,
    pub flag_c: Option<String>,
    pub flag_d: Option<char>,
    pub flag_e: Option<String>,
    pub flag_g: Option<usize>,
    pub flag_j: bool,
    pub flag_l: bool,
    pub flag_q: Option<String>,
    pub flag_R: bool,
    pub arg_sqlite_options: Vec<String>,
}

pub fn parse() -> AppOptions {
    Docopt::new(USAGE).and_then(|d| d.deserialize()).unwrap_or_else(|e| e.exit())
}

impl AppOptions {
    pub fn format(&self) -> Format {
        if self.flag_l {
            Format::Ltsv
        } else if self.flag_j {
            Format::Json
        } else {
            Format::Csv(self.flag_d.map(|it| it as u8))
        }
    }
}
