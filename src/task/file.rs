use std::{
    io::{Error, ErrorKind},
    path::Path,
};

use tokio::fs;

pub async fn read_file(path: &str) -> Result<Vec<u8>, Error> {
    if path.trim().is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, "file path is empty"));
    }
    fs::read(path).await
}

pub async fn write_file(path: &str, data: &[u8]) -> Result<(), Error> {
    if path.trim().is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, "file path is empty"));
    }
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }
    fs::write(path, data).await
}
