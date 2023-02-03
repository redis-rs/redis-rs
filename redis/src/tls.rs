//! Defines types to use with the tls.

use std::{error, fmt, result};

use native_tls::{Certificate as NativeCertificate, Error as NativeError};

/// A typedef of the result-type returned by many methods.
pub type Result<T> = result::Result<T, Error>;

/// An error returned from the TLS implementation.
pub struct Error(NativeError);

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        error::Error::source(&self.0)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, fmt)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.0, fmt)
    }
}

impl From<NativeError> for Error {
    fn from(err: NativeError) -> Error {
        Error(err)
    }
}

/// An X509 certificate.
#[derive(Clone)]
pub struct Certificate(pub(crate) NativeCertificate);

impl Certificate {
    /// Parses a DER-formatted X509 certificate.
    pub fn from_der(der: &[u8]) -> Result<Certificate> {
        let cert = NativeCertificate::from_der(der)?;
        Ok(Certificate(cert))
    }

    /// Parses a PEM-formatted X509 certificate.
    pub fn from_pem(pem: &[u8]) -> Result<Certificate> {
        let cert = NativeCertificate::from_pem(pem)?;
        Ok(Certificate(cert))
    }

    /// Returns the DER-encoded representation of this certificate.
    pub fn to_der(&self) -> Result<Vec<u8>> {
        let der = self.0.to_der()?;
        Ok(der)
    }
}

impl std::cmp::Eq for Certificate {
    fn assert_receiver_is_total_eq(&self) {}
}

impl std::cmp::PartialEq for Certificate {
    fn eq(&self, other: &Self) -> bool {
        match self.0.to_der() {
            Ok(self_der) => match other.0.to_der() {
                Ok(other_der) => self_der == other_der,
                Err(_) => false,
            },
            Err(_) => false,
        }
    }
}

impl std::fmt::Debug for Certificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Certificate")
            .field(&self.0.to_der())
            .finish()
    }
}
