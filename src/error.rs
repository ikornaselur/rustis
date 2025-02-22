use thiserror::Error;

pub type Result<T> = std::result::Result<T, RustisError>;

#[derive(Error, Debug)]
pub enum RustisError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}
