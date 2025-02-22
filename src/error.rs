use nom::error::Error as NomError;
use nom::Err as NomErr;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, RustisError>;

#[derive(Error, Debug)]
pub enum RustisError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Parsing error: {0}")]
    NomError(String),
}

impl<I: std::fmt::Debug> From<NomErr<NomError<I>>> for RustisError {
    fn from(err: NomErr<NomError<I>>) -> Self {
        match err {
            NomErr::Incomplete(_) => RustisError::NomError("Incomplete input".to_string()),
            NomErr::Error(e) => RustisError::NomError(format!("{:?}", e)),
            NomErr::Failure(e) => RustisError::NomError(format!("{:?}", e)),
        }
    }
}
