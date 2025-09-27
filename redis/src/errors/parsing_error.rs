use std::{ffi::NulError, fmt, str::Utf8Error, string::FromUtf8Error};

use arcstr::ArcStr;

/// Describes a type conversion or parsing failure.
#[derive(Clone, Debug, PartialEq)]
pub struct ParsingError {
    pub(crate) description: ArcStr,
}

impl std::fmt::Display for ParsingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Incompatible type - ")?;
        self.description.fmt(f)
    }
}

impl std::error::Error for ParsingError {}

impl From<NulError> for ParsingError {
    fn from(err: NulError) -> ParsingError {
        format!("Value contains interior nul terminator: {err}",).into()
    }
}

impl From<Utf8Error> for ParsingError {
    fn from(_: Utf8Error) -> ParsingError {
        arcstr::literal!("Invalid UTF-8").into()
    }
}

#[cfg(feature = "uuid")]
impl From<uuid::Error> for ParsingError {
    fn from(err: uuid::Error) -> ParsingError {
        format!("Value is not a valid UUID: {err}").into()
    }
}

impl From<FromUtf8Error> for ParsingError {
    fn from(err: FromUtf8Error) -> ParsingError {
        format!("Cannot convert from UTF-8: {err}").into()
    }
}

impl From<String> for ParsingError {
    fn from(err: String) -> ParsingError {
        ParsingError {
            description: err.into(),
        }
    }
}

impl<'a> From<&'a str> for ParsingError {
    fn from(err: &'a str) -> ParsingError {
        ParsingError {
            description: err.into(),
        }
    }
}

impl From<ArcStr> for ParsingError {
    fn from(err: ArcStr) -> ParsingError {
        ParsingError { description: err }
    }
}
