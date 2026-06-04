use std::{
    io::{Error, ErrorKind},
    process::Command,
};

pub async fn execute(command: &str) -> Result<Vec<u8>, Error> {
    if command.trim().is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "shell command is empty",
        ));
    }

    #[cfg(target_os = "windows")]
    let output = Command::new("cmd").args(["/C", command]).output()?;

    #[cfg(not(target_os = "windows"))]
    let output = Command::new("sh").args(["-c", command]).output()?;

    let mut data = output.stdout;
    if data.is_empty() {
        data = output.stderr;
    }
    if data.is_empty() {
        data = b"Success.".to_vec();
    }
    Ok(data)
}
