//! this provides a temporary workaround until Utf8Error is
//! stabilized.  This way we can kinda use the same API but
//! just from a different module.

pub struct Utf8Error;


pub fn from_utf8(v: &[u8]) -> Result<&str, Utf8Error> {
    match ::std::str::from_utf8(v) {
        Ok(rv) => { return Ok(rv); }
        Err(_) => { return Err(Utf8Error); }
    }
}
