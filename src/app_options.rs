
use docopt::Docopt;

use crate::types::*;



const USAGE: &str = "
not q

Usage:
  nq [options] <csv> [-- <sqlite-options>...]
  nq (-h | --help)
  nq --version

Options:
  -c CACHE      Cache *.sqlite
  -d DELIMITER  Format: Delimter for CSV
  -e ENCODING   CSV character encoding: https://encoding.spec.whatwg.org/#concept-encoding-get
  -g LINES      Guess column types
  -j            Format: JSON
  -l            Format: LTSV
  -r FORMAT     Format: Regular expression
  -n            No header line
  -q SQL        SQL
  -s            Format: Simple (white spaces split text)
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
    pub flag_r: Option<String>,
    pub flag_j: bool,
    pub flag_l: bool,
    pub flag_n: bool,
    pub flag_q: Option<String>,
    pub flag_s: bool,
    pub flag_version: bool,
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
        } else if self.flag_s {
            Format::Simple
        } else if let Some(ref format) = self.flag_r {
            Format::Regex(format.to_owned())
        } else {
            Format::Csv(self.flag_d.map(|it| it as u8))
        }
    }
}
