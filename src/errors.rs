
use failure::Fail;



pub type AppResult<T> = Result<T, AppError>;
pub type AppResultU = Result<(), AppError>;



#[derive(Fail, Debug)]
pub enum AppError {
    #[fail(display = "IO Error: {}", 0)]
    Io(std::io::Error),
    #[fail(display = "CSV Error: {}", 0)]
    Csv(quick_csv::error::Error),
    #[fail(display = "Few columns")]
    FewColumns,
    #[fail(display = "Error: {}", 0)]
    Fixed(&'static str),
    #[fail(display = "Json Error: {}", 0)]
    Json(serde_json::Error),
    #[fail(display = "Regex Error: {}", 0)]
    Regex(regex::Error),
    #[fail(display = "SQL Error: {}", 0)]
    Sql(rusqlite::Error),
}


macro_rules! define_error {
    ($source:ty, $kind:ident) => {
        impl From<$source> for AppError {
            fn from(error: $source) -> AppError {
                AppError::$kind(error)
            }
        }
    }
}

define_error!(quick_csv::error::Error, Csv);
define_error!(regex::Error, Regex);
define_error!(rusqlite::Error, Sql);
define_error!(serde_json::Error, Json);
define_error!(std::io::Error, Io);


impl From<&'static str> for AppError {
    fn from(error: &'static str) -> Self {
        AppError::Fixed(error)
    }
}
