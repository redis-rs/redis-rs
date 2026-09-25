mod basic;
mod collection;
mod commands;
mod numbers;
mod strings;
mod utils;
mod value;

use crate::errors::ParsingError;
#[cfg(feature = "ahash")]
pub(crate) use ahash::AHashMap as HashMap;
#[cfg(not(feature = "ahash"))]
pub(crate) use std::collections::HashMap;
use std::default::Default;
use std::fmt;
use std::io;

use crate::errors::RedisError;

pub(crate) use commands::SyncPushSender;
pub use commands::{
    ExistenceCheck, ExpireOption, Expiry, FieldExistenceCheck, IncrexResult, InfoDict,
    IntegerReplyOrNoOp, PushInfo, ReplicaInfo, Role, SetExpiry, ValueComparison, ValueType,
};
pub use utils::{
    calculate_value_digest, from_redis_value, from_redis_value_ref, is_valid_16_bytes_hex_digest,
};
use utils::{get_inner_value, get_owned_inner_value, is_nested_pairs, to_single_arg, vec_to_array};
pub use value::{PushKind, Value, VerbatimFormat};

/// Helper enum that is used in some situations to describe
/// the behavior of arguments in a numeric context.
#[derive(PartialEq, Eq, Clone, Debug, Copy)]
#[non_exhaustive]
pub enum NumericBehavior {
    /// This argument is not numeric.
    NonNumeric,
    /// This argument is an integer.
    NumberIsInteger,
    /// This argument is a floating point value.
    NumberIsFloat,
}

/// Library generic result type.
pub type RedisResult<T> = Result<T, RedisError>;

impl<T: FromRedisValue> FromRedisValue for RedisResult<T> {
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        match v {
            Value::ServerError(err) => Ok(Err(err.clone().into())),
            _ => from_redis_value_ref(v).map(|result| Ok(result)),
        }
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        match v {
            Value::ServerError(err) => Ok(Err(err.into())),
            _ => from_redis_value(v).map(|result| Ok(result)),
        }
    }
}

/// Library generic future type.
#[cfg(feature = "aio")]
pub type RedisFuture<'a, T> = futures_util::future::BoxFuture<'a, RedisResult<T>>;

/// Abstraction trait for redis command abstractions.
pub trait RedisWrite {
    /// Accepts a serialized redis command.
    fn write_arg(&mut self, arg: &[u8]);

    /// Accepts a serialized redis command.
    fn write_arg_fmt(&mut self, arg: impl fmt::Display) {
        self.write_arg(arg.to_string().as_bytes());
    }

    /// Appends an empty argument to the command, and returns a
    /// [`std::io::Write`] instance that can write to it.
    ///
    /// Writing multiple arguments into this buffer is unsupported. The resulting
    /// data will be interpreted as one argument by Redis.
    ///
    /// Writing no data is supported and is similar to having an empty bytestring
    /// as an argument.
    fn writer_for_next_arg(&mut self) -> impl io::Write + '_;

    /// Reserve space for `additional` arguments in the command
    ///
    /// `additional` is a list of the byte sizes of the arguments.
    ///
    /// # Examples
    /// Sending some Protobufs with `prost` to Redis.
    /// ```rust,ignore
    /// use prost::Message;
    ///
    /// let to_send: Vec<SomeType> = todo!();
    /// let mut cmd = Cmd::new();
    ///
    /// // Calculate and reserve the space for the args
    /// cmd.reserve_space_for_args(to_send.iter().map(Message::encoded_len));
    ///
    /// // Write the args to the buffer
    /// for arg in to_send {
    ///     // Encode the type directly into the Cmd buffer
    ///     // Supplying the required capacity again is not needed for Cmd,
    ///     // but can be useful for other implementers like Vec<Vec<u8>>.
    ///     arg.encode(cmd.bufmut_for_next_arg(arg.encoded_len()));
    /// }
    ///
    /// ```
    ///
    /// # Implementation note
    /// The default implementation provided by this trait is a no-op. It's therefore strongly
    /// recommended to implement this function. Depending on the internal buffer it might only
    /// be possible to use the numbers of arguments (`additional.len()`) or the total expected
    /// capacity (`additional.iter().sum()`). Implementors should assume that the caller will
    /// be wrong and might over or under specify the amount of arguments and space required.
    fn reserve_space_for_args(&mut self, additional: impl IntoIterator<Item = usize>) {
        // _additional would show up in the documentation, so we assign it
        // to make it used.
        let _do_nothing = additional;
    }

    #[cfg(feature = "bytes")]
    /// Appends an empty argument to the command, and returns a
    /// [`bytes::BufMut`] instance that can write to it.
    ///
    /// `capacity` should be equal or greater to the amount of bytes
    /// expected, as some implementations might not be able to resize
    /// the returned buffer.
    ///
    /// Writing multiple arguments into this buffer is unsupported. The resulting
    /// data will be interpreted as one argument by Redis.
    ///
    /// Writing no data is supported and is similar to having an empty bytestring
    /// as an argument.
    fn bufmut_for_next_arg(&mut self, capacity: usize) -> impl bytes::BufMut + '_ {
        // This default implementation is not the most efficient, but does
        // allow for implementers to skip this function. This means that
        // upstream libraries that implement this trait don't suddenly
        // stop working because someone enabled one of the async features.

        /// Has a temporary buffer that is written to [`writer_for_next_arg`]
        /// on drop.
        struct Wrapper<'a> {
            /// The buffer, implements [`bytes::BufMut`] allowing passthrough
            buf: Vec<u8>,
            /// The writer to the command, used on drop
            writer: Box<dyn io::Write + 'a>,
        }
        unsafe impl bytes::BufMut for Wrapper<'_> {
            fn remaining_mut(&self) -> usize {
                self.buf.remaining_mut()
            }

            unsafe fn advance_mut(&mut self, cnt: usize) {
                unsafe {
                    self.buf.advance_mut(cnt);
                }
            }

            fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
                self.buf.chunk_mut()
            }

            // Vec specializes these methods, so we do too
            fn put<T: bytes::buf::Buf>(&mut self, src: T)
            where
                Self: Sized,
            {
                self.buf.put(src);
            }

            fn put_slice(&mut self, src: &[u8]) {
                self.buf.put_slice(src);
            }

            fn put_bytes(&mut self, val: u8, cnt: usize) {
                self.buf.put_bytes(val, cnt);
            }
        }
        impl Drop for Wrapper<'_> {
            fn drop(&mut self) {
                self.writer.write_all(&self.buf).unwrap();
            }
        }

        Wrapper {
            buf: Vec::with_capacity(capacity),
            writer: Box::new(self.writer_for_next_arg()),
        }
    }
}

/// This trait marks that a value is serialized only into a single Redis value.
///
/// This should be implemented only for types that are serialized into exactly one value,
/// otherwise the compiler can't ensure the correctness of some commands.
pub trait ToSingleRedisArg: ToRedisArgs {
    /// Returns an estimate of the number of bytes this single argument
    /// will serialize to.
    fn arg_size(&self) -> usize {
        self.args_size()
    }
}

/// Used to convert a value into one or multiple redis argument
/// strings.  Most values will produce exactly one item but in
/// some cases it might make sense to produce more than one.
pub trait ToRedisArgs: Sized {
    /// This converts the value into a vector of bytes.  Each item
    /// is a single argument.  Most items generate a vector of a
    /// single item.
    ///
    /// The exception to this rule currently are vectors of items.
    fn to_redis_args(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        self.write_redis_args(&mut out);
        out
    }

    /// This writes the value into a vector of bytes.  Each item
    /// is a single argument.  Most items generate a single item.
    ///
    /// The exception to this rule currently are vectors of items.
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite;

    /// Returns an information about the contained value with regards
    /// to it's numeric behavior in a redis context.  This is used in
    /// some high level concepts to switch between different implementations
    /// of redis functions (for instance `INCR` vs `INCRBYFLOAT`).
    #[inline]
    fn describe_numeric_behavior(&self) -> NumericBehavior {
        NumericBehavior::NonNumeric
    }

    /// Returns the number of arguments this value will generate.
    ///
    /// This is used in some high level functions to intelligently switch
    /// between `GET` and `MGET` variants. Also, for some commands like HEXPIREDAT
    /// which require a specific number of arguments, this method can be used to
    /// know the number of arguments.
    #[inline]
    fn num_of_args(&self) -> usize {
        1
    }

    /// Returns an estimate of the number of bytes that will be written to the
    /// command for this value.
    #[inline]
    fn args_size(&self) -> usize {
        0
    }

    /// Returns both the number of Redis arguments and the estimated number of bytes this value
    /// will serialize to.
    #[inline]
    fn num_of_args_and_size(&self) -> (usize, usize) {
        (self.num_of_args(), self.args_size())
    }

    /// This only exists internally as a workaround for the lack of
    /// specialization.
    #[doc(hidden)]
    fn write_args_from_slice<W>(items: &[Self], out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        Self::make_arg_iter_ref(items.iter(), out);
    }

    /// This only exists internally as a workaround for the lack of
    /// specialization.
    #[doc(hidden)]
    fn make_arg_iter_ref<'a, I, W>(items: I, out: &mut W)
    where
        W: ?Sized + RedisWrite,
        I: Iterator<Item = &'a Self>,
        Self: 'a,
    {
        for item in items {
            item.write_redis_args(out);
        }
    }

    // this is used in absence of specialization to provide a default implementation for slices that can be overridden specifically for byte slices (&[u8])
    #[doc(hidden)]
    #[inline]
    fn num_of_args_and_size_for_slice(items: &[Self]) -> (usize, usize) {
        items
            .iter()
            .map(|item| item.num_of_args_and_size())
            .fold((0, 0), |(args, size), (item_args, item_size)| {
                (args + item_args, size + item_size)
            })
    }

    // this is used in absence of specialization to provide a default implementation for arrays that can be overridden specifically for byte arrays (&[u8; N])
    #[doc(hidden)]
    #[inline]
    fn num_of_args_and_size_for_array<const N: usize>(items: &[Self; N]) -> (usize, usize) {
        Self::num_of_args_and_size_for_slice(items.as_slice())
    }

    // this is used in absence of specialization to provide a default implementation for slices that can be overridden specifically for byte slices (&[u8])
    #[doc(hidden)]
    #[inline]
    fn is_single_vec_arg(items: &[Self]) -> bool {
        items.len() == 1 && items[0].num_of_args() <= 1
    }
}

/// This trait is used to convert a redis value into a more appropriate
/// type.
///
/// While a redis `Value` can represent any response that comes
/// back from the redis server, usually you want to map this into something
/// that works better in rust.  For instance you might want to convert the
/// return value into a `String` or an integer.
///
/// This trait is well supported throughout the library and you can
/// implement it for your own types if you want.
///
/// In addition to what you can see from the docs, this is also implemented
/// for tuples up to size 12 and for `Vec<u8>`.
pub trait FromRedisValue: Sized {
    /// Given a redis `Value` this attempts to convert it into the given
    /// destination type.  If that fails because it's not compatible an
    /// appropriate error is generated.
    fn from_redis_value_ref(v: &Value) -> Result<Self, ParsingError> {
        // By default, fall back to `from_redis_value_ref`.
        // This function only needs to be implemented if it can benefit
        // from taking `v` by value.
        Self::from_redis_value(v.clone())
    }

    /// Given a redis `Value` this attempts to convert it into the given
    /// destination type.  If that fails because it's not compatible an
    /// appropriate error is generated.
    fn from_redis_value(v: Value) -> Result<Self, ParsingError>;

    /// Similar to `from_redis_value_ref` but constructs a vector of objects
    /// from another vector of values.  This primarily exists internally
    /// to customize the behavior for vectors of tuples.
    fn from_redis_value_refs(items: &[Value]) -> Result<Vec<Self>, ParsingError> {
        items
            .iter()
            .map(FromRedisValue::from_redis_value_ref)
            .collect()
    }

    /// The same as `from_redis_value_refs`, but takes a `Vec<Value>` instead
    /// of a `&[Value]`.
    fn from_redis_values(items: Vec<Value>) -> Result<Vec<Self>, ParsingError> {
        items
            .into_iter()
            .map(FromRedisValue::from_redis_value)
            .collect()
    }

    /// The same as `from_redis_values`, but returns a result for each
    /// conversion to make handling them case-by-case possible.
    fn from_each_redis_values(items: Vec<Value>) -> Vec<Result<Self, ParsingError>> {
        items
            .into_iter()
            .map(FromRedisValue::from_redis_value)
            .collect()
    }

    /// Convert bytes to a single element vector.
    fn from_byte_slice(_vec: &[u8]) -> Option<Vec<Self>> {
        Self::from_redis_value(Value::BulkString(_vec.into()))
            .map(|rv| vec![rv])
            .ok()
    }

    /// Convert bytes to a single element vector.
    fn from_byte_vec(_vec: Vec<u8>) -> Result<Vec<Self>, ParsingError> {
        Self::from_redis_value(Value::BulkString(_vec)).map(|rv| vec![rv])
    }
}

/// Enum representing the communication protocol with the server.
///
/// This enum represents the types of data that the server can send to the client,
/// and the capabilities that the client can use.
#[derive(Clone, Eq, PartialEq, Default, Debug, Copy)]
#[non_exhaustive]
pub enum ProtocolVersion {
    /// <https://github.com/redis/redis-specifications/blob/master/protocol/RESP2.md>
    #[default]
    RESP2,
    /// <https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md>
    RESP3,
}

impl ProtocolVersion {
    /// Returns true if the protocol can support RESP3 features.
    pub fn supports_resp3(&self) -> bool {
        !matches!(self, Self::RESP2)
    }
}
