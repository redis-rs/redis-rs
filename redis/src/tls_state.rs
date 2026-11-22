//! Type-level markers for how a builder secures its connections.
//!
//! `SentinelClientBuilder` takes these as type parameters, one for the connection to the Redis
//! nodes and one for the connection to the sentinels, so that the TLS options that exclude each
//! other cannot be combined: the compiler rejects it, instead of a runtime check or a setter that
//! silently wins.
//!
//! The markers are uninhabited and only exist as type parameters.

mod private {
    pub trait Sealed {}
}

/// No TLS option has been chosen yet.
///
/// Any of the options can still be picked: [`BuiltIn`] through the TLS mode and certificates
/// setters, or `Custom` through the rustls configuration setter.
#[non_exhaustive]
pub enum Unset {}

/// redis-rs builds the connector from the TLS mode, the certificates and the flags that were set.
///
/// The TLS mode and certificates setters can be called again in this state, but a rustls
/// configuration can no longer be chosen.
#[non_exhaustive]
pub enum BuiltIn {}

/// The caller supplied a complete [`rustls::ClientConfig`], which is used as it is.
///
/// The configuration decides what is trusted, so everything that would skip or replace its
/// verification is unavailable in this state: the TLS mode and certificates setters. Choosing a
/// configuration again is unavailable as well.
#[cfg(feature = "tls-rustls")]
#[non_exhaustive]
pub enum Custom {}

/// The states in which redis-rs owns the verification: [`Unset`] and [`BuiltIn`].
///
/// This trait is sealed, it cannot be implemented outside this crate.
pub trait BuiltInTls: private::Sealed {}

impl private::Sealed for Unset {}
impl private::Sealed for BuiltIn {}
impl BuiltInTls for Unset {}
impl BuiltInTls for BuiltIn {}
