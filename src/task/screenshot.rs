use screenshots::Screen;
use std::io::{Cursor, Error, ErrorKind};

pub async fn capture_png() -> Result<Vec<u8>, Error> {
    let screen = Screen::from_point(0, 0).map_err(|e| Error::new(ErrorKind::Other, e))?;
    let img = screen
        .capture()
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    let mut bytes = Vec::new();
    let _ = img.write_to(
        &mut Cursor::new(&mut bytes),
        screenshots::image::ImageOutputFormat::Png,
    );
    Ok(bytes)
}
