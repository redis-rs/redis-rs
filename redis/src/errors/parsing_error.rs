use std::{borrow::Cow, ffi::NulError, fmt, str::Utf8Error, string::FromUtf8Error};

/// Describes a type conversion or parsing failure.
#[derive(Clone, Debug, PartialEq)]
pub struct ParsingError {
    pub(crate) description: Cow<'static, str>,
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
        ParsingError {
            description: format!("Value contains interior nul terminator: {err}",).into(),
        }
    }
}

impl From<Utf8Error> for ParsingError {
    fn from(_: Utf8Error) -> ParsingError {
        ParsingError {
            description: "Invalid UTF-8".into(),
        }
    }
}

#[cfg(feature = "uuid")]
impl From<uuid::Error> for ParsingError {
    fn from(err: uuid::Error) -> ParsingError {
        ParsingError {
            description: format!("Value is not a valid UUID: {err}").into(),
        }
    }
}

impl From<FromUtf8Error> for ParsingError {
    fn from(_: FromUtf8Error) -> ParsingError {
        ParsingError {
            description: "Cannot convert from UTF-8".into(),
        }
    }
}
