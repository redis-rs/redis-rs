use crate::cmd::{Cmd, cmd};
use crate::connection::ConnectionLike;
use crate::pipeline::Pipeline;
use crate::types::{
    ExistenceCheck, FromRedisValue, RedisResult, RedisWrite, ToRedisArgs, ToSingleRedisArg,
};

#[cfg(feature = "cluster")]
use crate::commands::ClusterPipeline;

use serde::ser::Serialize;

macro_rules! implement_json_commands {
    (
        $lifetime: lifetime
        $(
            $(#[$attr:meta])+
            fn $name:ident<$($tyargs:ident : $ty:ident),*>(
                $($argname:ident: $argty:ty),*) $body:block
        )*
    ) => (

        /// Implements RedisJSON commands for connection like objects.  This
        /// allows you to send commands straight to a connection or client.  It
        /// is also implemented for redis results of clients which makes for
        /// very convenient access in some basic cases.
        ///
        /// This allows you to use nicer syntax for some common operations.
        /// For instance this code:
        ///
        /// ```rust,no_run
        /// use redis::JsonCommands;
        /// use serde_json::json;
        /// # fn do_something() -> redis::RedisResult<()> {
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_connection()?;
        /// redis::cmd("JSON.SET").arg("my_key").arg("$").arg(&json!({"item": 42i32}).to_string()).exec(&mut con).unwrap();
        /// assert_eq!(redis::cmd("JSON.GET").arg("my_key").arg("$").query(&mut con), Ok(String::from(r#"[{"item":42}]"#)));
        /// # Ok(()) }
        /// ```
        ///
        /// Will become this:
        ///
        /// ```rust,no_run
        /// use redis::JsonCommands;
        /// use serde_json::json;
        /// # fn do_something() -> redis::RedisResult<()> {
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_connection()?;
        /// let _: () = con.json_set("my_key", "$", &json!({"item": 42i32}).to_string())?;
        /// assert_eq!(con.json_get("my_key", "$"), Ok(String::from(r#"[{"item":42}]"#)));
        /// assert_eq!(con.json_get("my_key", "$.item"), Ok(String::from(r#"[42]"#)));
        /// # Ok(()) }
        /// ```
        ///
        /// With RedisJSON commands, you have to note that all results will be wrapped
        /// in square brackets (or empty brackets if not found). If you want to deserialize it
        /// with e.g. `serde_json` you have to use `Vec<T>` for your output type instead of `T`.
        pub trait JsonCommands : ConnectionLike + Sized {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty, )* RV: FromRedisValue>(
                    &mut self $(, $argname: $argty)*) -> RedisResult<RV>
                    { Cmd::$name($($argname),*)?.query(self) }
            )*
        }

        impl Cmd {
            $(
                $(#[$attr])*
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                pub fn $name<$lifetime, $($tyargs: $ty),*>($($argname: $argty),*) -> RedisResult<Self> {
                    Ok($body)
                }
            )*
        }

        /// Implements RedisJSON commands over asynchronous connections. This
        /// allows you to send commands straight to a connection or client.
        ///
        /// This allows you to use nicer syntax for some common operations.
        /// For instance this code:
        ///
        /// ```rust,no_run
        /// use redis::JsonAsyncCommands;
        /// use serde_json::json;
        /// # async fn do_something() -> redis::RedisResult<()> {
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_multiplexed_async_connection().await?;
        /// redis::cmd("JSON.SET").arg("my_key").arg("$").arg(&json!({"item": 42i32}).to_string()).exec_async(&mut con).await?;
        /// assert_eq!(redis::cmd("JSON.GET").arg("my_key").arg("$").query_async(&mut con).await, Ok(String::from(r#"[{"item":42}]"#)));
        /// # Ok(()) }
        /// ```
        ///
        /// Will become this:
        ///
        /// ```rust,no_run
        /// use redis::JsonAsyncCommands;
        /// use serde_json::json;
        /// # async fn do_something() -> redis::RedisResult<()> {
        /// use redis::Commands;
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_multiplexed_async_connection().await?;
        /// let _: () = con.json_set("my_key", "$", &json!({"item": 42i32}).to_string()).await?;
        /// assert_eq!(con.json_get("my_key", "$").await, Ok(String::from(r#"[{"item":42}]"#)));
        /// assert_eq!(con.json_get("my_key", "$.item").await, Ok(String::from(r#"[42]"#)));
        /// # Ok(()) }
        /// ```
        ///
        /// With RedisJSON commands, you have to note that all results will be wrapped
        /// in square brackets (or empty brackets if not found). If you want to deserialize it
        /// with e.g. `serde_json` you have to use `Vec<T>` for your output type instead of `T`.
        ///
        #[cfg(feature = "aio")]
        pub trait JsonAsyncCommands : crate::aio::ConnectionLike + Send + Sized {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty + Send + Sync + $lifetime,)* RV>(
                    & $lifetime mut self
                    $(, $argname: $argty)*
                ) -> $crate::types::RedisFuture<'a, RV>
                where
                    RV: FromRedisValue,
                {
                    Box::pin(async move {
                        $body.query_async(self).await
                    })
                }
            )*
        }

        /// Implements RedisJSON commands for pipelines.  Unlike the regular
        /// commands trait, this returns the pipeline rather than a result
        /// directly.  Other than that it works the same however.
        impl Pipeline {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                pub fn $name<$lifetime, $($tyargs: $ty),*>(
                    &mut self $(, $argname: $argty)*
                ) -> RedisResult<&mut Self> {
                    self.add_command($body);
                    Ok(self)
                }
            )*
        }

        /// Implements RedisJSON commands for cluster pipelines.  Unlike the regular
        /// commands trait, this returns the cluster pipeline rather than a result
        /// directly.  Other than that it works the same however.
        #[cfg(feature = "cluster")]
        impl ClusterPipeline {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                pub fn $name<$lifetime, $($tyargs: $ty),*>(
                    &mut self $(, $argname: $argty)*
                ) -> RedisResult<&mut Self> {
                    self.add_command($body);
                    Ok(self)
                }
            )*
        }
    )
}

implement_json_commands! {
    'a

    /// Append the JSON `value` to the array at `path` after the last element in it.
    fn json_arr_append<K: ToSingleRedisArg, P: ToSingleRedisArg, V: Serialize>(key: K, path: P, value: &'a V) {
        cmd("JSON.ARRAPPEND").arg(key).arg(path).arg(serde_json::to_string(value)?).take()
    }

    /// Index array at `path`, returns first occurrence of `value`
    fn json_arr_index<K: ToSingleRedisArg, P: ToSingleRedisArg, V: Serialize>(key: K, path: P, value: &'a V) {
        cmd("JSON.ARRINDEX").arg(key).arg(path).arg(serde_json::to_string(value)?).take()
    }

    /// Same as `json_arr_index` except takes a `start` and a `stop` value, setting these to `0` will mean
    /// they make no effect on the query
    ///
    /// The default values for `start` and `stop` are `0`, so pass those in if you want them to take no effect
    fn json_arr_index_ss<K: ToSingleRedisArg, P: ToSingleRedisArg, V: Serialize>(key: K, path: P, value: &'a V, start: &'a isize, stop: &'a isize) {
        cmd("JSON.ARRINDEX").arg(key).arg(path).arg(serde_json::to_string(value)?).arg(start).arg(stop).take()
    }

    /// Inserts the JSON `value` in the array at `path` before the `index` (shifts to the right).
    ///
    /// `index` must be within the array's range.
    fn json_arr_insert<K: ToSingleRedisArg, P: ToSingleRedisArg, V: Serialize>(key: K, path: P, index: i64, value: &'a V) {
        cmd("JSON.ARRINSERT").arg(key).arg(path).arg(index).arg(serde_json::to_string(value)?).take()
    }

    /// Reports the length of the JSON Array at `path` in `key`.
    fn json_arr_len<K: ToSingleRedisArg, P: ToSingleRedisArg>(key: K, path: P) {
        cmd("JSON.ARRLEN").arg(key).arg(path).take()
    }

    /// Removes and returns an element from the `index` in the array.
    ///
    /// `index` defaults to `-1` (the end of the array).
    fn json_arr_pop<K: ToSingleRedisArg, P: ToSingleRedisArg>(key: K, path: P, index: i64) {
        cmd("JSON.ARRPOP").arg(key).arg(path).arg(index).take()
    }

    /// Trims an array so that it contains only the specified inclusive range of elements.
    ///
    /// This command is extremely forgiving and using it with out-of-range indexes will not produce an error.
    /// There are a few differences between how RedisJSON v2.0 and legacy versions handle out-of-range indexes.
    fn json_arr_trim<K: ToSingleRedisArg, P: ToSingleRedisArg>(key: K, path: P, start: i64, stop: i64) {
        cmd("JSON.ARRTRIM").arg(key).arg(path).arg(start).arg(stop).take()
    }

    /// Clears container values (Arrays/Objects), and sets numeric values to 0.
    fn json_clear<K: ToSingleRedisArg, P: ToSingleRedisArg>(key: K, path: P) {
        cmd("JSON.CLEAR").arg(key).arg(path).take()
    }

    /// Deletes a value at `path`.
    fn json_del<K: ToSingleRedisArg, P: ToSingleRedisArg>(key: K, path: P) {
        cmd("JSON.DEL").arg(key).arg(path).take()
    }

    /// Gets JSON Value at `path`.
    ///
    /// With RedisJSON commands, you have to note that all results will be wrapped
    /// in square brackets (or empty brackets if not found). If you want to deserialize it
    /// with e.g. `serde_json` you have to use `Vec<T>` for your output type instead of `T`.
    fn json_get<K: ToSingleRedisArg, P: ToRedisArgs>(key: K, path: P) {
        cmd("JSON.GET").arg(key).arg(path).take()
    }

    /// Gets JSON Values at `path`.
    ///
    /// With RedisJSON commands, you have to note that all results will be wrapped
    /// in square brackets (or empty brackets if not found). If you want to deserialize it
    /// with e.g. `serde_json` you have to use `Vec<T>` for your output type instead of `T`.
    fn json_mget<K: ToRedisArgs, P: ToSingleRedisArg>(key: K, path: P) {
        cmd("JSON.MGET").arg(key).arg(path).take()
    }

    /// Increments the number value stored at `path` by `number`.
    fn json_num_incr_by<K: ToSingleRedisArg, P: ToSingleRedisArg>(key: K, path: P, value: i64) {
        cmd("JSON.NUMINCRBY").arg(key).arg(path).arg(value).take()
    }

    /// Returns the keys in the object that's referenced by `path`.
    fn json_obj_keys<K: ToSingleRedisArg, P: ToSingleRedisArg>(key: K, path: P) {
        cmd("JSON.OBJKEYS").arg(key).arg(path).take()
    }

    /// Reports the number of keys in the JSON Object at `path` in `key`.
    fn json_obj_len<K: ToSingleRedisArg, P: ToSingleRedisArg>(key: K, path: P) {
        cmd("JSON.OBJLEN").arg(key).arg(path).take()
    }

    /// Sets the JSON Value at `path` in `key`.
    fn json_set<K: ToSingleRedisArg, P: ToSingleRedisArg, V: Serialize>(key: K, path: P, value: &'a V) {
        cmd("JSON.SET").arg(key).arg(path).arg(serde_json::to_string(value)?).take()
    }

    /// Sets the JSON Value at `path` in `key` with options.
    ///
    /// The value (JSON document or FPHA float array) is carried inside `options`
    /// alongside the optional `NX`/`XX` existence check. See [`JsonSetOptions`].
    fn json_set_options<K: ToSingleRedisArg, P: ToSingleRedisArg>(key: K, path: P, options: &'a JsonSetOptions) {
        cmd("JSON.SET").arg(key).arg(path).arg(options).take()
    }

        /// Sets the value at the path per key, for every given tuple.
    fn json_mset<K: ToSingleRedisArg, P: ToSingleRedisArg, V: Serialize>(key_path_values: &'a [(K,P,V)]) {
        let mut cmd = cmd("JSON.MSET");

        for (key, path, value) in key_path_values {
            cmd.arg(key)
               .arg(path)
               .arg(serde_json::to_string(value)?);
        }

        cmd
    }

    /// Appends the `json-string` values to the string at `path`.
    fn json_str_append<K: ToSingleRedisArg, P: ToSingleRedisArg, V: ToSingleRedisArg>(key: K, path: P, value: V) {
        cmd("JSON.STRAPPEND").arg(key).arg(path).arg(value).take()
    }

    /// Reports the length of the JSON String at `path` in `key`.
    fn json_str_len<K: ToSingleRedisArg, P: ToSingleRedisArg>(key: K, path: P) {
        cmd("JSON.STRLEN").arg(key).arg(path).take()
    }

    /// Toggle a `boolean` value stored at `path`.
    fn json_toggle<K: ToSingleRedisArg, P: ToSingleRedisArg>(key: K, path: P) {
        cmd("JSON.TOGGLE").arg(key).arg(path).take()
    }

    /// Reports the type of JSON value at `path`.
    fn json_type<K: ToSingleRedisArg, P: ToSingleRedisArg>(key: K, path: P) {
        cmd("JSON.TYPE").arg(key).arg(path).take()
    }
}

impl<T> JsonCommands for T where T: ConnectionLike {}

#[cfg(feature = "aio")]
impl<T> JsonAsyncCommands for T where T: crate::aio::ConnectionLike + Send + Sized {}

/// Storage-precision tag for the `FPHA` form of `JSON.SET`.
///
/// Pairs with an arbitrary `serde::Serialize` payload via
/// [`JsonSetOptions::fpha_serialize`] to set values whose shape (matrices,
/// objects with multiple FP-array fields, scalars) cannot be expressed as a
/// flat [`FphaInput`] slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FphaType {
    /// Server stores lanes as Google brain-float 16 (`bfloat16`).
    Bf16,
    /// Server stores lanes as IEEE-754 binary16.
    Fp16,
    /// Server stores lanes as IEEE-754 binary32.
    Fp32,
    /// Server stores lanes as IEEE-754 binary64.
    Fp64,
}

impl ToRedisArgs for FphaType {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            FphaType::Bf16 => out.write_arg(b"BF16"),
            FphaType::Fp16 => out.write_arg(b"FP16"),
            FphaType::Fp32 => out.write_arg(b"FP32"),
            FphaType::Fp64 => out.write_arg(b"FP64"),
        }
    }
}

/// Float-array payload for the `FPHA` form of `JSON.SET`.
///
/// Each variant holds a borrowed flat slice of lanes; the variant determines
/// both the serialized element type and the `FPHA <TYPE>` storage hint sent
/// to the server. Used together with [`JsonSetOptions::fpha`] when the value
/// is a flat 1-D vector.
///
/// For multi-dimensional payloads (matrices, objects with multiple FP-array
/// fields, scalars), use [`JsonSetOptions::fpha_serialize`] with an explicit
/// [`FphaType`] instead.
#[derive(Clone, Copy, Debug)]
pub enum FphaInput<'a> {
    /// Server stores lanes as Google brain-float 16 (`bfloat16`).
    Bf16(&'a [f32]),
    /// Server stores lanes as IEEE-754 binary16.
    Fp16(&'a [f32]),
    /// Server stores lanes as IEEE-754 binary32.
    Fp32(&'a [f32]),
    /// Server stores lanes as IEEE-754 binary64.
    Fp64(&'a [f64]),
}

impl FphaInput<'_> {
    fn fpha_type(&self) -> FphaType {
        match self {
            FphaInput::Bf16(_) => FphaType::Bf16,
            FphaInput::Fp16(_) => FphaType::Fp16,
            FphaInput::Fp32(_) => FphaType::Fp32,
            FphaInput::Fp64(_) => FphaType::Fp64,
        }
    }

    fn serialize_json(&self) -> RedisResult<String> {
        match *self {
            FphaInput::Bf16(s) | FphaInput::Fp16(s) | FphaInput::Fp32(s) => {
                serde_json::to_string(s)
            }
            FphaInput::Fp64(s) => serde_json::to_string(s),
        }
        .map_err(Into::into)
    }
}

/// Options for the [`JSON.SET`](https://redis.io/commands/json.set) command.
///
/// Carries the value to write (either a JSON document or an [`FphaInput`]
/// float-array payload) together with an optional `NX`/`XX` existence check.
///
/// # Example
/// ```rust,no_run
/// use redis::{ExistenceCheck, JsonCommands, JsonSetOptions};
/// use serde_json::json;
/// # fn do_something() -> redis::RedisResult<()> {
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let mut con = client.get_connection()?;
/// let opts = JsonSetOptions::json(&json!({"item": 42}))?
///     .conditional_set(ExistenceCheck::NX);
/// let _: () = con.json_set_options("my_key", "$", &opts)?;
/// # Ok(()) }
/// ```
pub struct JsonSetOptions {
    value: String,
    fpha_tag: Option<FphaType>,
    conditional_set: Option<ExistenceCheck>,
}

impl JsonSetOptions {
    /// Build options that set `value` as a JSON document.
    ///
    /// The value is serialized eagerly via `serde_json`; serialization errors
    /// surface here rather than at command-dispatch time.
    pub fn json<V: Serialize + ?Sized>(value: &V) -> RedisResult<Self> {
        Ok(Self {
            value: serde_json::to_string(value)?,
            fpha_tag: None,
            conditional_set: None,
        })
    }

    /// Build options that set the value from a flat float-array payload,
    /// emitting the trailing `FPHA <TYPE>` tag on the wire.
    ///
    /// The lanes are serialized as a JSON array of numbers; `FPHA <TYPE>` is a
    /// storage hint that tells the server which precision to pack the array as
    /// internally. The variant of [`FphaInput`] both selects the storage type
    /// and constrains the data element type, ensuring they match.
    ///
    /// For payloads that aren't a flat 1-D vector (matrices, objects with
    /// multiple FP-array fields, scalars), use [`Self::fpha_serialize`].
    pub fn fpha(input: FphaInput<'_>) -> RedisResult<Self> {
        Ok(Self {
            value: input.serialize_json()?,
            fpha_tag: Some(input.fpha_type()),
            conditional_set: None,
        })
    }

    /// Build options that serialize an arbitrary value with an `FPHA <TYPE>`
    /// storage hint.
    ///
    /// Unlike [`Self::fpha`], this accepts any [`Serialize`] payload, allowing
    /// nested arrays (matrices), objects with multiple FP-array fields, or
    /// scalars. The caller picks the [`FphaType`] explicitly; no compile-time
    /// pairing of the data element type with the storage hint is enforced.
    pub fn fpha_serialize<V: Serialize + ?Sized>(value: &V, ty: FphaType) -> RedisResult<Self> {
        Ok(Self {
            value: serde_json::to_string(value)?,
            fpha_tag: Some(ty),
            conditional_set: None,
        })
    }

    /// Apply an `NX` or `XX` existence check to the command.
    pub fn conditional_set(mut self, existence_check: ExistenceCheck) -> Self {
        self.conditional_set = Some(existence_check);
        self
    }
}

impl ToRedisArgs for JsonSetOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.value.as_bytes());
        if let Some(ref conditional_set) = self.conditional_set {
            conditional_set.write_redis_args(out);
        }
        if let Some(ref ty) = self.fpha_tag {
            out.write_arg(b"FPHA");
            ty.write_redis_args(out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::{Arg, Cmd, cmd};

    fn simple_args(c: &Cmd) -> Vec<Vec<u8>> {
        c.args_iter()
            .map(|a| match a {
                Arg::Simple(b) => b.to_vec(),
                Arg::Cursor => b"<CURSOR>".to_vec(),
            })
            .collect()
    }

    fn build(opts: &JsonSetOptions) -> Vec<Vec<u8>> {
        let mut c = cmd("JSON.SET");
        c.arg("k").arg("$").arg(opts);
        simple_args(&c)
    }

    #[test]
    fn json_value_writes_serialized_document_only() {
        let opts = JsonSetOptions::json(&serde_json::json!({"a": 1})).unwrap();
        assert_eq!(
            build(&opts),
            vec![
                b"JSON.SET".to_vec(),
                b"k".to_vec(),
                b"$".to_vec(),
                br#"{"a":1}"#.to_vec(),
            ],
        );
    }

    #[test]
    fn json_value_with_nx_appends_existence_check() {
        let opts = JsonSetOptions::json(&serde_json::json!(1))
            .unwrap()
            .conditional_set(ExistenceCheck::NX);
        let args = build(&opts);
        assert_eq!(args.last().unwrap(), b"NX");
        assert_eq!(args.len(), 5);
    }

    #[test]
    fn fpha_bf16_writes_json_array_and_appends_type_tag() {
        let lanes = [1.0_f32, 2.5];
        let opts = JsonSetOptions::fpha(FphaInput::Bf16(&lanes)).unwrap();
        let args = build(&opts);
        // [JSON.SET, k, $, <json>, FPHA, BF16]
        assert_eq!(args.len(), 6);
        assert_eq!(args[3], b"[1.0,2.5]");
        assert_eq!(args[4], b"FPHA");
        assert_eq!(args[5], b"BF16");
    }

    #[test]
    fn fpha_fp32_with_xx_orders_value_then_xx_then_fpha_tag() {
        let lanes = [1.0_f32, -0.5, 1234.5];
        let opts = JsonSetOptions::fpha(FphaInput::Fp32(&lanes))
            .unwrap()
            .conditional_set(ExistenceCheck::XX);
        let args = build(&opts);
        // [JSON.SET, k, $, <json>, XX, FPHA, FP32]
        assert_eq!(args.len(), 7);
        assert_eq!(args[3], b"[1.0,-0.5,1234.5]");
        assert_eq!(args[4], b"XX");
        assert_eq!(args[5], b"FPHA");
        assert_eq!(args[6], b"FP32");
    }

    #[test]
    fn fpha_fp64_emits_fp64_tag() {
        let lanes = [1.0_f64, 2.0, 3.0, 4.0];
        let opts = JsonSetOptions::fpha(FphaInput::Fp64(&lanes)).unwrap();
        let args = build(&opts);
        assert_eq!(args[3], b"[1.0,2.0,3.0,4.0]");
        assert_eq!(args[5], b"FP64");
    }

    #[test]
    fn fpha_fp16_emits_fp16_tag() {
        let lanes = [1.0_f32; 5];
        let opts = JsonSetOptions::fpha(FphaInput::Fp16(&lanes)).unwrap();
        let args = build(&opts);
        assert_eq!(args[3], b"[1.0,1.0,1.0,1.0,1.0]");
        assert_eq!(args[5], b"FP16");
    }

    #[test]
    fn fpha_empty_payload_still_emits_tag() {
        let opts = JsonSetOptions::fpha(FphaInput::Fp32(&[])).unwrap();
        let args = build(&opts);
        assert_eq!(args.len(), 6);
        assert_eq!(args[3], b"[]");
        assert_eq!(args[5], b"FP32");
    }

    #[test]
    fn fpha_type_matches_variant() {
        assert_eq!(FphaInput::Bf16(&[]).fpha_type(), FphaType::Bf16);
        assert_eq!(FphaInput::Fp16(&[]).fpha_type(), FphaType::Fp16);
        assert_eq!(FphaInput::Fp32(&[]).fpha_type(), FphaType::Fp32);
        assert_eq!(FphaInput::Fp64(&[]).fpha_type(), FphaType::Fp64);
    }

    #[test]
    fn fpha_serialize_with_matrix_emits_nested_json_and_tag() {
        let matrix: &[&[f32]] = &[&[1.0, 2.5], &[3.0, 4.0]];
        let opts = JsonSetOptions::fpha_serialize(matrix, FphaType::Bf16).unwrap();
        let args = build(&opts);
        assert_eq!(args.len(), 6);
        assert_eq!(args[3], b"[[1.0,2.5],[3.0,4.0]]");
        assert_eq!(args[4], b"FPHA");
        assert_eq!(args[5], b"BF16");
    }

    #[test]
    fn fpha_serialize_with_object_emits_serialized_value_and_tag() {
        let value = serde_json::json!({"weights": [1.0, 2.0], "bias": [0.5]});
        let opts = JsonSetOptions::fpha_serialize(&value, FphaType::Fp16).unwrap();
        let args = build(&opts);
        assert_eq!(args.len(), 6);
        assert_eq!(args[3], br#"{"bias":[0.5],"weights":[1.0,2.0]}"#);
        assert_eq!(args[5], b"FP16");
    }

    #[test]
    fn fpha_serialize_pairs_with_existence_check() {
        let opts = JsonSetOptions::fpha_serialize(&[1.0_f32, 2.0], FphaType::Fp32)
            .unwrap()
            .conditional_set(ExistenceCheck::NX);
        let args = build(&opts);
        // [JSON.SET, k, $, <json>, NX, FPHA, FP32]
        assert_eq!(args.len(), 7);
        assert_eq!(args[3], b"[1.0,2.0]");
        assert_eq!(args[4], b"NX");
        assert_eq!(args[5], b"FPHA");
        assert_eq!(args[6], b"FP32");
    }
}
