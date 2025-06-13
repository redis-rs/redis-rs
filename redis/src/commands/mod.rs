#![allow(unused_parens)]

use crate::cmd::{cmd, Cmd, Iter};
use crate::connection::{Connection, ConnectionLike, Msg};
use crate::pipeline::Pipeline;
use crate::types::{
    ExistenceCheck, ExpireOption, Expiry, FieldExistenceCheck, FromRedisValue, IntegerReplyOrNoOp,
    NumericBehavior, RedisResult, RedisWrite, SetExpiry, ToRedisArgs,
};
use std::collections::HashSet;

#[macro_use]
mod macros;

#[cfg(feature = "json")]
#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
mod json;

#[cfg(feature = "json")]
pub use json::JsonCommands;

#[cfg(all(feature = "json", feature = "aio"))]
pub use json::JsonAsyncCommands;

#[cfg(feature = "cluster")]
use crate::cluster_pipeline::ClusterPipeline;

#[cfg(feature = "geospatial")]
use crate::geo;

#[cfg(feature = "streams")]
use crate::streams;

#[cfg(feature = "acl")]
use crate::acl;
use crate::{RedisConnectionInfo, Value};

#[cfg(any(feature = "cluster", feature = "cache-aio"))]
pub(crate) fn is_readonly_cmd(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"BITCOUNT"
            | b"BITFIELD_RO"
            | b"BITPOS"
            | b"DBSIZE"
            | b"DUMP"
            | b"EVALSHA_RO"
            | b"EVAL_RO"
            | b"EXISTS"
            | b"EXPIRETIME"
            | b"FCALL_RO"
            | b"GEODIST"
            | b"GEOHASH"
            | b"GEOPOS"
            | b"GEORADIUSBYMEMBER_RO"
            | b"GEORADIUS_RO"
            | b"GEOSEARCH"
            | b"GET"
            | b"GETBIT"
            | b"GETRANGE"
            | b"HEXISTS"
            | b"HEXPIRETIME"
            | b"HGET"
            | b"HGETALL"
            | b"HKEYS"
            | b"HLEN"
            | b"HMGET"
            | b"HRANDFIELD"
            | b"HPTTL"
            | b"HPEXPIRETIME"
            | b"HSCAN"
            | b"HSTRLEN"
            | b"HTTL"
            | b"HVALS"
            | b"KEYS"
            | b"LCS"
            | b"LINDEX"
            | b"LLEN"
            | b"LOLWUT"
            | b"LPOS"
            | b"LRANGE"
            | b"MEMORY USAGE"
            | b"MGET"
            | b"OBJECT ENCODING"
            | b"OBJECT FREQ"
            | b"OBJECT IDLETIME"
            | b"OBJECT REFCOUNT"
            | b"PEXPIRETIME"
            | b"PFCOUNT"
            | b"PTTL"
            | b"RANDOMKEY"
            | b"SCAN"
            | b"SCARD"
            | b"SDIFF"
            | b"SINTER"
            | b"SINTERCARD"
            | b"SISMEMBER"
            | b"SMEMBERS"
            | b"SMISMEMBER"
            | b"SORT_RO"
            | b"SRANDMEMBER"
            | b"SSCAN"
            | b"STRLEN"
            | b"SUBSTR"
            | b"SUNION"
            | b"TOUCH"
            | b"TTL"
            | b"TYPE"
            | b"XINFO CONSUMERS"
            | b"XINFO GROUPS"
            | b"XINFO STREAM"
            | b"XLEN"
            | b"XPENDING"
            | b"XRANGE"
            | b"XREAD"
            | b"XREVRANGE"
            | b"ZCARD"
            | b"ZCOUNT"
            | b"ZDIFF"
            | b"ZINTER"
            | b"ZINTERCARD"
            | b"ZLEXCOUNT"
            | b"ZMSCORE"
            | b"ZRANDMEMBER"
            | b"ZRANGE"
            | b"ZRANGEBYLEX"
            | b"ZRANGEBYSCORE"
            | b"ZRANK"
            | b"ZREVRANGE"
            | b"ZREVRANGEBYLEX"
            | b"ZREVRANGEBYSCORE"
            | b"ZREVRANK"
            | b"ZSCAN"
            | b"ZSCORE"
            | b"ZUNION"
            | b"JSON.GET"
            | b"JSON.MGET"
    )
}

// Note - Brackets are needed around return types for purposes of macro branching.
implement_commands! {
    'a
    // most common operations

    /// Get the value of a key.  If key is a vec this becomes an `MGET` (if using `TypedCommands`, you should specifically use `mget` to get the correct return type.
    /// [Redis Docs](https://redis.io/commands/get/)
    fn get<K: ToRedisArgs>(key: K) -> (Option<String>) {
        cmd(if key.num_of_args() <= 1 { "GET" } else { "MGET" }).arg(key)
    }

    /// Get values of keys
    /// [Redis Docs](https://redis.io/commands/MGET)
    fn mget<K: ToRedisArgs>(key: K) -> (Vec<Option<String>>) {
        cmd("MGET").arg(key)
    }

    /// Gets all keys matching pattern
    /// [Redis Docs](https://redis.io/commands/KEYS)
    fn keys<K: ToRedisArgs>(key: K) -> (Vec<String>) {
        cmd("KEYS").arg(key)
    }

    /// Set the string value of a key.
    /// [Redis Docs](https://redis.io/commands/SET)
    fn set<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) -> (()) {
        cmd("SET").arg(key).arg(value)
    }

    /// Set the string value of a key with options.
    /// [Redis Docs](https://redis.io/commands/SET)
    fn set_options<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V, options: SetOptions) -> (Option<String>) {
        cmd("SET").arg(key).arg(value).arg(options)
    }

    /// Sets multiple keys to their values.
    #[allow(deprecated)]
    #[deprecated(since = "0.22.4", note = "Renamed to mset() to reflect Redis name")]
    /// [Redis Docs](https://redis.io/commands/MSET)
    fn set_multiple<K: ToRedisArgs, V: ToRedisArgs>(items: &'a [(K, V)]) -> (()) {
        cmd("MSET").arg(items)
    }

    /// Sets multiple keys to their values.
    /// [Redis Docs](https://redis.io/commands/MSET)
    fn mset<K: ToRedisArgs, V: ToRedisArgs>(items: &'a [(K, V)]) -> (()) {
        cmd("MSET").arg(items)
    }

    /// Set the value and expiration of a key.
    /// [Redis Docs](https://redis.io/commands/SETEX)
    fn set_ex<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V, seconds: u64) -> (()) {
        cmd("SETEX").arg(key).arg(seconds).arg(value)
    }

    /// Set the value and expiration in milliseconds of a key.
    /// [Redis Docs](https://redis.io/commands/PSETEX)
    fn pset_ex<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V, milliseconds: u64) -> (()) {
        cmd("PSETEX").arg(key).arg(milliseconds).arg(value)
    }

    /// Set the value of a key, only if the key does not exist
    /// [Redis Docs](https://redis.io/commands/SETNX)
    fn set_nx<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) -> (bool) {
        cmd("SETNX").arg(key).arg(value)
    }

    /// Sets multiple keys to their values failing if at least one already exists.
    /// [Redis Docs](https://redis.io/commands/MSETNX)
    fn mset_nx<K: ToRedisArgs, V: ToRedisArgs>(items: &'a [(K, V)]) -> (bool) {
        cmd("MSETNX").arg(items)
    }

    /// Set the string value of a key and return its old value.
    /// [Redis Docs](https://redis.io/commands/GETSET)
    fn getset<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) -> (Option<String>) {
        cmd("GETSET").arg(key).arg(value)
    }

    /// Get a range of bytes/substring from the value of a key. Negative values provide an offset from the end of the value.
    /// Redis returns an empty string if the key doesn't exist, not Nil
    /// [Redis Docs](https://redis.io/commands/GETRANGE)
    fn getrange<K: ToRedisArgs>(key: K, from: isize, to: isize) -> (String) {
        cmd("GETRANGE").arg(key).arg(from).arg(to)
    }

    /// Overwrite the part of the value stored in key at the specified offset.
    /// [Redis Docs](https://redis.io/commands/SETRANGE)
    fn setrange<K: ToRedisArgs, V: ToRedisArgs>(key: K, offset: isize, value: V) -> (usize) {
        cmd("SETRANGE").arg(key).arg(offset).arg(value)
    }

    /// Delete one or more keys.
    /// Returns the number of keys deleted.
    /// [Redis Docs](https://redis.io/commands/DEL)
    fn del<K: ToRedisArgs>(key: K) -> (usize) {
        cmd("DEL").arg(key)
    }

    /// Determine if a key exists.
    /// [Redis Docs](https://redis.io/commands/EXISTS)
    fn exists<K: ToRedisArgs>(key: K) -> (bool) {
        cmd("EXISTS").arg(key)
    }

    /// Determine the type of key.
    /// [Redis Docs](https://redis.io/commands/TYPE)
    fn key_type<K: ToRedisArgs>(key: K) -> (crate::types::ValueType) {
        cmd("TYPE").arg(key)
    }

    /// Set a key's time to live in seconds.
    /// Returns whether expiration was set.
    /// [Redis Docs](https://redis.io/commands/EXPIRE)
    fn expire<K: ToRedisArgs>(key: K, seconds: i64) -> (bool) {
        cmd("EXPIRE").arg(key).arg(seconds)
    }

    /// Set the expiration for a key as a UNIX timestamp.
    /// Returns whether expiration was set.
    /// [Redis Docs](https://redis.io/commands/EXPIREAT)
    fn expire_at<K: ToRedisArgs>(key: K, ts: i64) -> (bool) {
        cmd("EXPIREAT").arg(key).arg(ts)
    }

    /// Set a key's time to live in milliseconds.
    /// Returns whether expiration was set.
    /// [Redis Docs](https://redis.io/commands/PEXPIRE)
    fn pexpire<K: ToRedisArgs>(key: K, ms: i64) -> (bool) {
        cmd("PEXPIRE").arg(key).arg(ms)
    }

    /// Set the expiration for a key as a UNIX timestamp in milliseconds.
    /// Returns whether expiration was set.
    /// [Redis Docs](https://redis.io/commands/PEXPIREAT)
    fn pexpire_at<K: ToRedisArgs>(key: K, ts: i64) -> (bool) {
        cmd("PEXPIREAT").arg(key).arg(ts)
    }

    /// Get the absolute Unix expiration timestamp in seconds.
    /// Returns `ExistsButNotRelevant` if key exists but has no expiration time.
    /// [Redis Docs](https://redis.io/commands/EXPIRETIME)
    fn expire_time<K: ToRedisArgs>(key: K) -> (IntegerReplyOrNoOp) {
        cmd("EXPIRETIME").arg(key)
    }

    /// Get the absolute Unix expiration timestamp in milliseconds.
    /// Returns `ExistsButNotRelevant` if key exists but has no expiration time.
    /// [Redis Docs](https://redis.io/commands/PEXPIRETIME)
    fn pexpire_time<K: ToRedisArgs>(key: K) -> (IntegerReplyOrNoOp) {
        cmd("PEXPIRETIME").arg(key)
    }

    /// Remove the expiration from a key.
    /// Returns whether a timeout was removed.
    /// [Redis Docs](https://redis.io/commands/PERSIST)
    fn persist<K: ToRedisArgs>(key: K) -> (bool) {
        cmd("PERSIST").arg(key)
    }

    /// Get the time to live for a key in seconds.
    /// Returns `ExistsButNotRelevant` if key exists but has no expiration time.
    /// [Redis Docs](https://redis.io/commands/TTL)
    fn ttl<K: ToRedisArgs>(key: K) -> (IntegerReplyOrNoOp) {
        cmd("TTL").arg(key)
    }

    /// Get the time to live for a key in milliseconds.
    /// Returns `ExistsButNotRelevant` if key exists but has no expiration time.
    /// [Redis Docs](https://redis.io/commands/PTTL)
    fn pttl<K: ToRedisArgs>(key: K) -> (IntegerReplyOrNoOp) {
        cmd("PTTL").arg(key)
    }

    /// Get the value of a key and set expiration
    /// [Redis Docs](https://redis.io/commands/GETEX)
    fn get_ex<K: ToRedisArgs>(key: K, expire_at: Expiry) -> (Option<String>) {
        cmd("GETEX").arg(key).arg(expire_at)
    }

    /// Get the value of a key and delete it
    /// [Redis Docs](https://redis.io/commands/GETDEL)
    fn get_del<K: ToRedisArgs>(key: K) -> (Option<String>) {
        cmd("GETDEL").arg(key)
    }

    /// Copy the value from one key to another, returning whether the copy was successful.
    /// [Redis Docs](https://redis.io/commands/COPY)
    fn copy<KSrc: ToRedisArgs, KDst: ToRedisArgs, Db: ToString>(
        source: KSrc,
        destination: KDst,
        options: CopyOptions<Db>
    ) -> (bool) {
        cmd("COPY").arg(source).arg(destination).arg(options)
    }

    /// Rename a key.
    /// Errors if key does not exist.
    /// [Redis Docs](https://redis.io/commands/RENAME)
    fn rename<K: ToRedisArgs, N: ToRedisArgs>(key: K, new_key: N) -> (()) {
        cmd("RENAME").arg(key).arg(new_key)
    }

    /// Rename a key, only if the new key does not exist.
    /// Errors if key does not exist.
    /// Returns whether the key was renamed, or false if the new key already exists.
    /// [Redis Docs](https://redis.io/commands/RENAMENX)
    fn rename_nx<K: ToRedisArgs, N: ToRedisArgs>(key: K, new_key: N) -> (bool) {
        cmd("RENAMENX").arg(key).arg(new_key)
    }

    /// Unlink one or more keys. This is a non-blocking version of `DEL`.
    /// Returns number of keys unlinked.
    /// [Redis Docs](https://redis.io/commands/UNLINK)
    fn unlink<K: ToRedisArgs>(key: K) -> (usize) {
        cmd("UNLINK").arg(key)
    }

    // common string operations

    /// Append a value to a key.
    /// Returns length of string after operation.
    /// [Redis Docs](https://redis.io/commands/APPEND)
    fn append<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) -> (usize) {
        cmd("APPEND").arg(key).arg(value)
    }

    /// Increment the numeric value of a key by the given amount.  This
    /// issues a `INCRBY` or `INCRBYFLOAT` depending on the type.
    /// If the key does not exist, it is set to 0 before performing the operation.
    fn incr<K: ToRedisArgs, V: ToRedisArgs>(key: K, delta: V) -> (isize) {
        cmd(if delta.describe_numeric_behavior() == NumericBehavior::NumberIsFloat {
            "INCRBYFLOAT"
        } else {
            "INCRBY"
        }).arg(key).arg(delta)
    }

    /// Decrement the numeric value of a key by the given amount.
    /// If the key does not exist, it is set to 0 before performing the operation.
    /// [Redis Docs](https://redis.io/commands/DECRBY)
    fn decr<K: ToRedisArgs, V: ToRedisArgs>(key: K, delta: V) -> (isize) {
        cmd("DECRBY").arg(key).arg(delta)
    }

    /// Sets or clears the bit at offset in the string value stored at key.
    /// Returns the original bit value stored at offset.
    /// [Redis Docs](https://redis.io/commands/SETBIT)
    fn setbit<K: ToRedisArgs>(key: K, offset: usize, value: bool) -> (bool) {
        cmd("SETBIT").arg(key).arg(offset).arg(i32::from(value))
    }

    /// Returns the bit value at offset in the string value stored at key.
    /// [Redis Docs](https://redis.io/commands/GETBIT)
    fn getbit<K: ToRedisArgs>(key: K, offset: usize) -> (bool) {
        cmd("GETBIT").arg(key).arg(offset)
    }

    /// Count set bits in a string.
    /// Returns 0 if key does not exist.
    /// [Redis Docs](https://redis.io/commands/BITCOUNT)
    fn bitcount<K: ToRedisArgs>(key: K) -> (usize) {
        cmd("BITCOUNT").arg(key)
    }

    /// Count set bits in a string in a range.
    /// Returns 0 if key does not exist.
    /// [Redis Docs](https://redis.io/commands/BITCOUNT)
    fn bitcount_range<K: ToRedisArgs>(key: K, start: usize, end: usize) -> (usize) {
        cmd("BITCOUNT").arg(key).arg(start).arg(end)
    }

    /// Perform a bitwise AND between multiple keys (containing string values)
    /// and store the result in the destination key.
    /// Returns size of destination string after operation.
    /// [Redis Docs](https://redis.io/commands/BITOP").arg("AND)
    fn bit_and<D: ToRedisArgs, S: ToRedisArgs>(dstkey: D, srckeys: S) -> (usize) {
        cmd("BITOP").arg("AND").arg(dstkey).arg(srckeys)
    }

    /// Perform a bitwise OR between multiple keys (containing string values)
    /// and store the result in the destination key.
    /// Returns size of destination string after operation.
    /// [Redis Docs](https://redis.io/commands/BITOP").arg("OR)
    fn bit_or<D: ToRedisArgs, S: ToRedisArgs>(dstkey: D, srckeys: S) -> (usize) {
        cmd("BITOP").arg("OR").arg(dstkey).arg(srckeys)
    }

    /// Perform a bitwise XOR between multiple keys (containing string values)
    /// and store the result in the destination key.
    /// Returns size of destination string after operation.
    /// [Redis Docs](https://redis.io/commands/BITOP").arg("XOR)
    fn bit_xor<D: ToRedisArgs, S: ToRedisArgs>(dstkey: D, srckeys: S) -> (usize) {
        cmd("BITOP").arg("XOR").arg(dstkey).arg(srckeys)
    }

    /// Perform a bitwise NOT of the key (containing string values)
    /// and store the result in the destination key.
    /// Returns size of destination string after operation.
    /// [Redis Docs](https://redis.io/commands/BITOP").arg("NOT)
    fn bit_not<D: ToRedisArgs, S: ToRedisArgs>(dstkey: D, srckey: S) -> (usize) {
        cmd("BITOP").arg("NOT").arg(dstkey).arg(srckey)
    }

    /// Get the length of the value stored in a key.
    /// 0 if key does not exist.
    /// [Redis Docs](https://redis.io/commands/STRLEN)
    fn strlen<K: ToRedisArgs>(key: K) -> (usize) {
        cmd("STRLEN").arg(key)
    }

    // hash operations

    /// Gets a single (or multiple) fields from a hash.
    fn hget<K: ToRedisArgs, F: ToRedisArgs>(key: K, field: F) -> (Option<String>) {
        cmd(if field.num_of_args() <= 1 { "HGET" } else { "HMGET" }).arg(key).arg(field)
    }

    /// Get the value of one or more fields of a given hash key, and optionally set their expiration
    /// [Redis Docs](https://redis.io/commands/HGETEX").arg(key).arg(expire_at).arg("FIELDS)
    fn hget_ex<K: ToRedisArgs, F: ToRedisArgs>(key: K, fields: F, expire_at: Expiry) -> (Vec<String>) {
        cmd("HGETEX").arg(key).arg(expire_at).arg("FIELDS").arg(fields.num_of_args()).arg(fields)
    }

    /// Deletes a single (or multiple) fields from a hash.
    /// Returns number of fields deleted.
    /// [Redis Docs](https://redis.io/commands/HDEL)
    fn hdel<K: ToRedisArgs, F: ToRedisArgs>(key: K, field: F) -> (usize) {
        cmd("HDEL").arg(key).arg(field)
    }

    /// Get and delete the value of one or more fields of a given hash key
    /// [Redis Docs](https://redis.io/commands/HGETDEL").arg(key).arg("FIELDS)
    fn hget_del<K: ToRedisArgs, F: ToRedisArgs>(key: K, fields: F) -> (Vec<Option<String>>) {
        cmd("HGETDEL").arg(key).arg("FIELDS").arg(fields.num_of_args()).arg(fields)
    }

    /// Sets a single field in a hash.
    /// Returns number of fields added.
    /// [Redis Docs](https://redis.io/commands/HSET)
    fn hset<K: ToRedisArgs, F: ToRedisArgs, V: ToRedisArgs>(key: K, field: F, value: V) -> (usize) {
        cmd("HSET").arg(key).arg(field).arg(value)
    }

    /// Set the value of one or more fields of a given hash key, and optionally set their expiration
    /// [Redis Docs](https://redis.io/commands/HSETEX").arg(key).arg(hash_field_expiration_options).arg("FIELDS)
    fn hset_ex<K: ToRedisArgs, F: ToRedisArgs, V: ToRedisArgs>(key: K, hash_field_expiration_options: &'a HashFieldExpirationOptions, fields_values: &'a [(F, V)]) -> (bool) {
        cmd("HSETEX").arg(key).arg(hash_field_expiration_options).arg("FIELDS").arg(fields_values.len()).arg(fields_values)
    }

    /// Sets a single field in a hash if it does not exist.
    /// Returns whether the field was added.
    /// [Redis Docs](https://redis.io/commands/HSETNX)
    fn hset_nx<K: ToRedisArgs, F: ToRedisArgs, V: ToRedisArgs>(key: K, field: F, value: V) -> (bool) {
        cmd("HSETNX").arg(key).arg(field).arg(value)
    }

    /// Sets multiple fields in a hash.
    /// [Redis Docs](https://redis.io/commands/HMSET)
    fn hset_multiple<K: ToRedisArgs, F: ToRedisArgs, V: ToRedisArgs>(key: K, items: &'a [(F, V)]) -> (()) {
        cmd("HMSET").arg(key).arg(items)
    }

    /// Increments a value.
    /// Returns the new value of the field after incrementation.
    fn hincr<K: ToRedisArgs, F: ToRedisArgs, D: ToRedisArgs>(key: K, field: F, delta: D) -> (f64) {
        cmd(if delta.describe_numeric_behavior() == NumericBehavior::NumberIsFloat {
            "HINCRBYFLOAT"
        } else {
            "HINCRBY"
        }).arg(key).arg(field).arg(delta)
    }

    /// Checks if a field in a hash exists.
    /// [Redis Docs](https://redis.io/commands/HEXISTS)
    fn hexists<K: ToRedisArgs, F: ToRedisArgs>(key: K, field: F) -> (bool) {
        cmd("HEXISTS").arg(key).arg(field)
    }

    /// Get one or more fields' TTL in seconds.
    /// [Redis Docs](https://redis.io/commands/HTTL").arg(key).arg("FIELDS)
    fn httl<K: ToRedisArgs, F: ToRedisArgs>(key: K, fields: F) -> (Vec<IntegerReplyOrNoOp>) {
        cmd("HTTL").arg(key).arg("FIELDS").arg(fields.num_of_args()).arg(fields)
    }

    /// Get one or more fields' TTL in milliseconds.
    /// [Redis Docs](https://redis.io/commands/HPTTL").arg(key).arg("FIELDS)
    fn hpttl<K: ToRedisArgs, F: ToRedisArgs>(key: K, fields: F) -> (Vec<IntegerReplyOrNoOp>) {
        cmd("HPTTL").arg(key).arg("FIELDS").arg(fields.num_of_args()).arg(fields)
    }

    /// Set one or more fields' time to live in seconds.
    /// Returns an array where each element corresponds to the field at the same index in the fields argument.
    /// Each element of the array is either:
    /// 0 if the specified condition has not been met.
    /// 1 if the expiration time was updated.
    /// 2 if called with 0 seconds.
    /// Errors if provided key exists but is not a hash.
    /// [Redis Docs](https://redis.io/commands/HEXPIRE").arg(key).arg(seconds).arg(opt).arg("FIELDS)
    fn hexpire<K: ToRedisArgs, F: ToRedisArgs>(key: K, seconds: i64, opt: ExpireOption, fields: F) -> (Vec<IntegerReplyOrNoOp>) {
       cmd("HEXPIRE").arg(key).arg(seconds).arg(opt).arg("FIELDS").arg(fields.num_of_args()).arg(fields)
    }


    /// Set the expiration for one or more fields as a UNIX timestamp in milliseconds.
    /// Returns an array where each element corresponds to the field at the same index in the fields argument.
    /// Each element of the array is either:
    /// 0 if the specified condition has not been met.
    /// 1 if the expiration time was updated.
    /// 2 if called with a time in the past.
    /// Errors if provided key exists but is not a hash.
    /// [Redis Docs](https://redis.io/commands/HEXPIREAT").arg(key).arg(ts).arg(opt).arg("FIELDS)
    fn hexpire_at<K: ToRedisArgs, F: ToRedisArgs>(key: K, ts: i64, opt: ExpireOption, fields: F) -> (Vec<IntegerReplyOrNoOp>) {
        cmd("HEXPIREAT").arg(key).arg(ts).arg(opt).arg("FIELDS").arg(fields.num_of_args()).arg(fields)
    }

    /// Returns the absolute Unix expiration timestamp in seconds.
    /// [Redis Docs](https://redis.io/commands/HEXPIRETIME").arg(key).arg("FIELDS)
    fn hexpire_time<K: ToRedisArgs, F: ToRedisArgs>(key: K, fields: F) -> (Vec<IntegerReplyOrNoOp>) {
        cmd("HEXPIRETIME").arg(key).arg("FIELDS").arg(fields.num_of_args()).arg(fields)
    }

    /// Remove the expiration from a key.
    /// Returns 1 if the expiration was removed.
    /// [Redis Docs](https://redis.io/commands/HPERSIST").arg(key).arg("FIELDS)
    fn hpersist<K: ToRedisArgs, F :ToRedisArgs>(key: K, fields: F) -> (Vec<IntegerReplyOrNoOp>) {
        cmd("HPERSIST").arg(key).arg("FIELDS").arg(fields.num_of_args()).arg(fields)
    }

    /// Set one or more fields' time to live in milliseconds.
    /// Returns an array where each element corresponds to the field at the same index in the fields argument.
    /// Each element of the array is either:
    /// 0 if the specified condition has not been met.
    /// 1 if the expiration time was updated.
    /// 2 if called with 0 seconds.
    /// Errors if provided key exists but is not a hash.
    /// [Redis Docs](https://redis.io/commands/HPEXPIRE").arg(key).arg(milliseconds).arg(opt).arg("FIELDS)
    fn hpexpire<K: ToRedisArgs, F: ToRedisArgs>(key: K, milliseconds: i64, opt: ExpireOption, fields: F) -> (Vec<IntegerReplyOrNoOp>) {
        cmd("HPEXPIRE").arg(key).arg(milliseconds).arg(opt).arg("FIELDS").arg(fields.num_of_args()).arg(fields)
    }

    /// Set the expiration for one or more fields as a UNIX timestamp in milliseconds.
    /// Returns an array where each element corresponds to the field at the same index in the fields argument.
    /// Each element of the array is either:
    /// 0 if the specified condition has not been met.
    /// 1 if the expiration time was updated.
    /// 2 if called with a time in the past.
    /// Errors if provided key exists but is not a hash.
    /// [Redis Docs](https://redis.io/commands/HPEXPIREAT").arg(key).arg(ts).arg(opt).arg("FIELDS)
    fn hpexpire_at<K: ToRedisArgs, F: ToRedisArgs>(key: K, ts: i64,  opt: ExpireOption, fields: F) -> (Vec<IntegerReplyOrNoOp>) {
        cmd("HPEXPIREAT").arg(key).arg(ts).arg(opt).arg("FIELDS").arg(fields.num_of_args()).arg(fields)
    }

    /// Returns the absolute Unix expiration timestamp in seconds.
    /// [Redis Docs](https://redis.io/commands/HPEXPIRETIME").arg(key).arg("FIELDS)
    fn hpexpire_time<K: ToRedisArgs, F: ToRedisArgs>(key: K, fields: F) -> (Vec<IntegerReplyOrNoOp>) {
        cmd("HPEXPIRETIME").arg(key).arg("FIELDS").arg(fields.num_of_args()).arg(fields)
    }

    /// Gets all the keys in a hash.
    /// [Redis Docs](https://redis.io/commands/HKEYS)
    fn hkeys<K: ToRedisArgs>(key: K) -> (Vec<String>) {
        cmd("HKEYS").arg(key)
    }

    /// Gets all the values in a hash.
    /// [Redis Docs](https://redis.io/commands/HVALS)
    fn hvals<K: ToRedisArgs>(key: K) -> (Vec<String>) {
        cmd("HVALS").arg(key)
    }

    /// Gets all the fields and values in a hash.
    /// [Redis Docs](https://redis.io/commands/HGETALL)
    fn hgetall<K: ToRedisArgs>(key: K) -> (std::collections::HashMap<String, String>) {
        cmd("HGETALL").arg(key)
    }

    /// Gets the length of a hash.
    /// Returns 0 if key does not exist.
    /// [Redis Docs](https://redis.io/commands/HLEN)
    fn hlen<K: ToRedisArgs>(key: K) -> (usize) {
        cmd("HLEN").arg(key)
    }

    // list operations

    /// Pop an element from a list, push it to another list
    /// and return it; or block until one is available
    /// [Redis Docs](https://redis.io/commands/BLMOVE)
    fn blmove<S: ToRedisArgs, D: ToRedisArgs>(srckey: S, dstkey: D, src_dir: Direction, dst_dir: Direction, timeout: f64) -> (Option<String>) {
        cmd("BLMOVE").arg(srckey).arg(dstkey).arg(src_dir).arg(dst_dir).arg(timeout)
    }

    /// Pops `count` elements from the first non-empty list key from the list of
    /// provided key names; or blocks until one is available.
    /// [Redis Docs](https://redis.io/commands/BLMPOP").arg(timeout).arg(numkeys).arg(key).arg(dir).arg("COUNT)
    fn blmpop<K: ToRedisArgs>(timeout: f64, numkeys: usize, key: K, dir: Direction, count: usize) -> (Option<[String; 2]>) {
        cmd("BLMPOP").arg(timeout).arg(numkeys).arg(key).arg(dir).arg("COUNT").arg(count)
    }

    /// Remove and get the first element in a list, or block until one is available.
    /// [Redis Docs](https://redis.io/commands/BLPOP)
    fn blpop<K: ToRedisArgs>(key: K, timeout: f64) -> (Option<[String; 2]>) {
        cmd("BLPOP").arg(key).arg(timeout)
    }

    /// Remove and get the last element in a list, or block until one is available.
    /// [Redis Docs](https://redis.io/commands/BRPOP)
    fn brpop<K: ToRedisArgs>(key: K, timeout: f64) -> (Option<[String; 2]>) {
        cmd("BRPOP").arg(key).arg(timeout)
    }

    /// Pop a value from a list, push it to another list and return it;
    /// or block until one is available.
    /// [Redis Docs](https://redis.io/commands/BRPOPLPUSH)
    fn brpoplpush<S: ToRedisArgs, D: ToRedisArgs>(srckey: S, dstkey: D, timeout: f64) -> (Option<String>) {
        cmd("BRPOPLPUSH").arg(srckey).arg(dstkey).arg(timeout)
    }

    /// Get an element from a list by its index.
    /// [Redis Docs](https://redis.io/commands/LINDEX)
    fn lindex<K: ToRedisArgs>(key: K, index: isize) -> (Option<String>) {
        cmd("LINDEX").arg(key).arg(index)
    }

    /// Insert an element before another element in a list.
    /// [Redis Docs](https://redis.io/commands/LINSERT)
    fn linsert_before<K: ToRedisArgs, P: ToRedisArgs, V: ToRedisArgs>(
            key: K, pivot: P, value: V) -> (isize) {
        cmd("LINSERT").arg(key).arg("BEFORE").arg(pivot).arg(value)
    }

    /// Insert an element after another element in a list.
    /// [Redis Docs](https://redis.io/commands/LINSERT)
    fn linsert_after<K: ToRedisArgs, P: ToRedisArgs, V: ToRedisArgs>(
            key: K, pivot: P, value: V) -> (isize) {
        cmd("LINSERT").arg(key).arg("AFTER").arg(pivot).arg(value)
    }

    /// Returns the length of the list stored at key.
    /// [Redis Docs](https://redis.io/commands/LLEN)
    fn llen<K: ToRedisArgs>(key: K) -> (usize) {
        cmd("LLEN").arg(key)
    }

    /// Pop an element a list, push it to another list and return it
    /// [Redis Docs](https://redis.io/commands/LMOVE)
    fn lmove<S: ToRedisArgs, D: ToRedisArgs>(srckey: S, dstkey: D, src_dir: Direction, dst_dir: Direction) -> (String) {
        cmd("LMOVE").arg(srckey).arg(dstkey).arg(src_dir).arg(dst_dir)
    }

    /// Pops `count` elements from the first non-empty list key from the list of
    /// provided key names.
    /// [Redis Docs](https://redis.io/commands/LMPOP").arg(numkeys).arg(key).arg(dir).arg("COUNT)
    fn lmpop<K: ToRedisArgs>( numkeys: usize, key: K, dir: Direction, count: usize) -> (Option<(String, Vec<String>)>) {
        cmd("LMPOP").arg(numkeys).arg(key).arg(dir).arg("COUNT").arg(count)
    }

    /// Removes and returns the up to `count` first elements of the list stored at key.
    ///
    /// If `count` is not specified, then defaults to first element.
    /// [Redis Docs](https://redis.io/commands/LPOP)
    fn lpop<K: ToRedisArgs>(key: K, count: Option<core::num::NonZeroUsize>) -> Generic {
        cmd("LPOP").arg(key).arg(count)
    }

    /// Returns the index of the first matching value of the list stored at key.
    /// [Redis Docs](https://redis.io/commands/LPOS)
    fn lpos<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V, options: LposOptions) -> Generic {
        cmd("LPOS").arg(key).arg(value).arg(options)
    }

    /// Insert all the specified values at the head of the list stored at key.
    /// [Redis Docs](https://redis.io/commands/LPUSH)
    fn lpush<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) -> (usize) {
        cmd("LPUSH").arg(key).arg(value)
    }

    /// Inserts a value at the head of the list stored at key, only if key
    /// already exists and holds a list.
    /// [Redis Docs](https://redis.io/commands/LPUSHX)
    fn lpush_exists<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) -> (usize) {
        cmd("LPUSHX").arg(key).arg(value)
    }

    /// Returns the specified elements of the list stored at key.
    /// [Redis Docs](https://redis.io/commands/LRANGE)
    fn lrange<K: ToRedisArgs>(key: K, start: isize, stop: isize) -> (Vec<String>) {
        cmd("LRANGE").arg(key).arg(start).arg(stop)
    }

    /// Removes the first count occurrences of elements equal to value
    /// from the list stored at key.
    /// [Redis Docs](https://redis.io/commands/LREM)
    fn lrem<K: ToRedisArgs, V: ToRedisArgs>(key: K, count: isize, value: V) -> (usize) {
        cmd("LREM").arg(key).arg(count).arg(value)
    }

    /// Trim an existing list so that it will contain only the specified
    /// range of elements specified.
    /// [Redis Docs](https://redis.io/commands/LTRIM)
    fn ltrim<K: ToRedisArgs>(key: K, start: isize, stop: isize) -> (()) {
        cmd("LTRIM").arg(key).arg(start).arg(stop)
    }

    /// Sets the list element at index to value
    /// [Redis Docs](https://redis.io/commands/LSET)
    fn lset<K: ToRedisArgs, V: ToRedisArgs>(key: K, index: isize, value: V) -> (()) {
        cmd("LSET").arg(key).arg(index).arg(value)
    }

    /// Sends a ping to the server
    /// [Redis Docs](https://redis.io/commands/PING)
    fn ping<>() -> (String) {
         &mut cmd("PING")
    }

    /// Sends a ping with a message to the server
    /// [Redis Docs](https://redis.io/commands/PING)
    fn ping_message<K: ToRedisArgs>(message: K) -> (String) {
         cmd("PING").arg(message)
    }

    /// Removes and returns the up to `count` last elements of the list stored at key
    ///
    /// If `count` is not specified, then defaults to last element.
    /// [Redis Docs](https://redis.io/commands/RPOP)
    fn rpop<K: ToRedisArgs>(key: K, count: Option<core::num::NonZeroUsize>) -> Generic {
        cmd("RPOP").arg(key).arg(count)
    }

    /// Pop a value from a list, push it to another list and return it.
    /// [Redis Docs](https://redis.io/commands/RPOPLPUSH)
    fn rpoplpush<K: ToRedisArgs, D: ToRedisArgs>(key: K, dstkey: D) -> (Option<String>) {
        cmd("RPOPLPUSH").arg(key).arg(dstkey)
    }

    /// Insert all the specified values at the tail of the list stored at key.
    /// [Redis Docs](https://redis.io/commands/RPUSH)
    fn rpush<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) -> (usize) {
        cmd("RPUSH").arg(key).arg(value)
    }

    /// Inserts value at the tail of the list stored at key, only if key
    /// already exists and holds a list.
    /// [Redis Docs](https://redis.io/commands/RPUSHX)
    fn rpush_exists<K: ToRedisArgs, V: ToRedisArgs>(key: K, value: V) -> (usize) {
        cmd("RPUSHX").arg(key).arg(value)
    }

    // set commands

    /// Add one or more members to a set.
    /// [Redis Docs](https://redis.io/commands/SADD)
    fn sadd<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) -> (usize) {
        cmd("SADD").arg(key).arg(member)
    }

    /// Get the number of members in a set.
    /// [Redis Docs](https://redis.io/commands/SCARD)
    fn scard<K: ToRedisArgs>(key: K) -> (usize) {
        cmd("SCARD").arg(key)
    }

    /// Subtract multiple sets.
    /// [Redis Docs](https://redis.io/commands/SDIFF)
    fn sdiff<K: ToRedisArgs>(keys: K) -> (HashSet<String>) {
        cmd("SDIFF").arg(keys)
    }

    /// Subtract multiple sets and store the resulting set in a key.
    /// [Redis Docs](https://redis.io/commands/SDIFFSTORE)
    fn sdiffstore<D: ToRedisArgs, K: ToRedisArgs>(dstkey: D, keys: K) -> (usize) {
        cmd("SDIFFSTORE").arg(dstkey).arg(keys)
    }

    /// Intersect multiple sets.
    /// [Redis Docs](https://redis.io/commands/SINTER)
    fn sinter<K: ToRedisArgs>(keys: K) -> (HashSet<String>) {
        cmd("SINTER").arg(keys)
    }

    /// Intersect multiple sets and store the resulting set in a key.
    /// [Redis Docs](https://redis.io/commands/SINTERSTORE)
    fn sinterstore<D: ToRedisArgs, K: ToRedisArgs>(dstkey: D, keys: K) -> (usize) {
        cmd("SINTERSTORE").arg(dstkey).arg(keys)
    }

    /// Determine if a given value is a member of a set.
    /// [Redis Docs](https://redis.io/commands/SISMEMBER)
    fn sismember<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) -> (bool) {
        cmd("SISMEMBER").arg(key).arg(member)
    }

    /// Determine if given values are members of a set.
    /// [Redis Docs](https://redis.io/commands/SMISMEMBER)
    fn smismember<K: ToRedisArgs, M: ToRedisArgs>(key: K, members: M) -> (Vec<bool>) {
        cmd("SMISMEMBER").arg(key).arg(members)
    }

    /// Get all the members in a set.
    /// [Redis Docs](https://redis.io/commands/SMEMBERS)
    fn smembers<K: ToRedisArgs>(key: K) -> (HashSet<String>) {
        cmd("SMEMBERS").arg(key)
    }

    /// Move a member from one set to another.
    /// [Redis Docs](https://redis.io/commands/SMOVE)
    fn smove<S: ToRedisArgs, D: ToRedisArgs, M: ToRedisArgs>(srckey: S, dstkey: D, member: M) -> (bool) {
        cmd("SMOVE").arg(srckey).arg(dstkey).arg(member)
    }

    /// Remove and return a random member from a set.
    /// [Redis Docs](https://redis.io/commands/SPOP)
    fn spop<K: ToRedisArgs>(key: K) -> Generic {
        cmd("SPOP").arg(key)
    }

    /// Get one random member from a set.
    /// [Redis Docs](https://redis.io/commands/SRANDMEMBER)
    fn srandmember<K: ToRedisArgs>(key: K) -> (Option<String>) {
        cmd("SRANDMEMBER").arg(key)
    }

    /// Get multiple random members from a set.
    /// [Redis Docs](https://redis.io/commands/SRANDMEMBER)
    fn srandmember_multiple<K: ToRedisArgs>(key: K, count: usize) -> (Vec<String>) {
        cmd("SRANDMEMBER").arg(key).arg(count)
    }

    /// Remove one or more members from a set.
    /// [Redis Docs](https://redis.io/commands/SREM)
    fn srem<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) -> (usize) {
        cmd("SREM").arg(key).arg(member)
    }

    /// Add multiple sets.
    /// [Redis Docs](https://redis.io/commands/SUNION)
    fn sunion<K: ToRedisArgs>(keys: K) -> (HashSet<String>) {
        cmd("SUNION").arg(keys)
    }

    /// Add multiple sets and store the resulting set in a key.
    /// [Redis Docs](https://redis.io/commands/SUNIONSTORE)
    fn sunionstore<D: ToRedisArgs, K: ToRedisArgs>(dstkey: D, keys: K) -> (usize) {
        cmd("SUNIONSTORE").arg(dstkey).arg(keys)
    }

    // sorted set commands

    /// Add one member to a sorted set, or update its score if it already exists.
    /// [Redis Docs](https://redis.io/commands/ZADD)
    fn zadd<K: ToRedisArgs, S: ToRedisArgs, M: ToRedisArgs>(key: K, member: M, score: S) -> usize{
        cmd("ZADD").arg(key).arg(score).arg(member)
    }

    /// Add multiple members to a sorted set, or update its score if it already exists.
    /// [Redis Docs](https://redis.io/commands/ZADD)
    fn zadd_multiple<K: ToRedisArgs, S: ToRedisArgs, M: ToRedisArgs>(key: K, items: &'a [(S, M)]) -> (usize) {
        cmd("ZADD").arg(key).arg(items)
    }

     /// Add one member to a sorted set, or update its score if it already exists.
     /// [Redis Docs](https://redis.io/commands/ZADD)
    fn zadd_options<K: ToRedisArgs, S: ToRedisArgs, M: ToRedisArgs>(key: K, member: M, score: S, options:&'a SortedSetAddOptions) -> usize{
        cmd("ZADD").arg(key).arg(options).arg(score).arg(member)
    }

    /// Add multiple members to a sorted set, or update its score if it already exists.
    /// [Redis Docs](https://redis.io/commands/ZADD)
    fn zadd_multiple_options<K: ToRedisArgs, S: ToRedisArgs, M: ToRedisArgs>(key: K, items: &'a [(S, M)], options:&'a SortedSetAddOptions) -> (usize) {
        cmd("ZADD").arg(key).arg(options).arg(items)
    }

    /// Get the number of members in a sorted set.
    /// [Redis Docs](https://redis.io/commands/ZCARD)
    fn zcard<K: ToRedisArgs>(key: K) -> (usize) {
        cmd("ZCARD").arg(key)
    }

    /// Count the members in a sorted set with scores within the given values.
    /// [Redis Docs](https://redis.io/commands/ZCOUNT)
    fn zcount<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) -> (usize) {
        cmd("ZCOUNT").arg(key).arg(min).arg(max)
    }

    /// Increments the member in a sorted set at key by delta.
    /// If the member does not exist, it is added with delta as its score.
    /// [Redis Docs](https://redis.io/commands/ZINCRBY)
    fn zincr<K: ToRedisArgs, M: ToRedisArgs, D: ToRedisArgs>(key: K, member: M, delta: D) -> (f64) {
        cmd("ZINCRBY").arg(key).arg(delta).arg(member)
    }

    /// Intersect multiple sorted sets and store the resulting sorted set in
    /// a new key using SUM as aggregation function.
    /// [Redis Docs](https://redis.io/commands/ZINTERSTORE)
    fn zinterstore<D: ToRedisArgs, K: ToRedisArgs>(dstkey: D, keys: K) -> (usize) {
        cmd("ZINTERSTORE").arg(dstkey).arg(keys.num_of_args()).arg(keys)
    }

    /// Intersect multiple sorted sets and store the resulting sorted set in
    /// a new key using MIN as aggregation function.
    /// [Redis Docs](https://redis.io/commands/ZINTERSTORE)
    fn zinterstore_min<D: ToRedisArgs, K: ToRedisArgs>(dstkey: D, keys: K) -> (usize) {
        cmd("ZINTERSTORE").arg(dstkey).arg(keys.num_of_args()).arg(keys).arg("AGGREGATE").arg("MIN")
    }

    /// Intersect multiple sorted sets and store the resulting sorted set in
    /// a new key using MAX as aggregation function.
    /// [Redis Docs](https://redis.io/commands/ZINTERSTORE)
    fn zinterstore_max<D: ToRedisArgs, K: ToRedisArgs>(dstkey: D, keys: K) -> (usize) {
        cmd("ZINTERSTORE").arg(dstkey).arg(keys.num_of_args()).arg(keys).arg("AGGREGATE").arg("MAX")
    }

    /// [`Commands::zinterstore`], but with the ability to specify a
    /// multiplication factor for each sorted set by pairing one with each key
    /// in a tuple.
    /// [Redis Docs](https://redis.io/commands/ZINTERSTORE)
    fn zinterstore_weights<D: ToRedisArgs, K: ToRedisArgs, W: ToRedisArgs>(dstkey: D, keys: &'a [(K, W)]) -> (usize) {
        let (keys, weights): (Vec<&K>, Vec<&W>) = keys.iter().map(|(key, weight):&(K, W)| -> ((&K, &W)) {(key, weight)}).unzip();
        cmd("ZINTERSTORE").arg(dstkey).arg(keys.num_of_args()).arg(keys).arg("WEIGHTS").arg(weights)
    }

    /// [`Commands::zinterstore_min`], but with the ability to specify a
    /// multiplication factor for each sorted set by pairing one with each key
    /// in a tuple.
    /// [Redis Docs](https://redis.io/commands/ZINTERSTORE)
    fn zinterstore_min_weights<D: ToRedisArgs, K: ToRedisArgs, W: ToRedisArgs>(dstkey: D, keys: &'a [(K, W)]) -> (usize) {
        let (keys, weights): (Vec<&K>, Vec<&W>) = keys.iter().map(|(key, weight):&(K, W)| -> ((&K, &W)) {(key, weight)}).unzip();
        cmd("ZINTERSTORE").arg(dstkey).arg(keys.num_of_args()).arg(keys).arg("AGGREGATE").arg("MIN").arg("WEIGHTS").arg(weights)
    }

    /// [`Commands::zinterstore_max`], but with the ability to specify a
    /// multiplication factor for each sorted set by pairing one with each key
    /// in a tuple.
    /// [Redis Docs](https://redis.io/commands/ZINTERSTORE)
    fn zinterstore_max_weights<D: ToRedisArgs, K: ToRedisArgs, W: ToRedisArgs>(dstkey: D, keys: &'a [(K, W)]) -> (usize) {
        let (keys, weights): (Vec<&K>, Vec<&W>) = keys.iter().map(|(key, weight):&(K, W)| -> ((&K, &W)) {(key, weight)}).unzip();
        cmd("ZINTERSTORE").arg(dstkey).arg(keys.num_of_args()).arg(keys).arg("AGGREGATE").arg("MAX").arg("WEIGHTS").arg(weights)
    }

    /// Count the number of members in a sorted set between a given lexicographical range.
    /// [Redis Docs](https://redis.io/commands/ZLEXCOUNT)
    fn zlexcount<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) -> (usize) {
        cmd("ZLEXCOUNT").arg(key).arg(min).arg(max)
    }

    /// Removes and returns the member with the highest score in a sorted set.
    /// Blocks until a member is available otherwise.
    /// [Redis Docs](https://redis.io/commands/BZPOPMAX)
    fn bzpopmax<K: ToRedisArgs>(key: K, timeout: f64) -> (Option<(String, String, f64)>) {
        cmd("BZPOPMAX").arg(key).arg(timeout)
    }

    /// Removes and returns up to count members with the highest scores in a sorted set
    /// [Redis Docs](https://redis.io/commands/ZPOPMAX)
    fn zpopmax<K: ToRedisArgs>(key: K, count: isize) -> (Vec<String>) {
        cmd("ZPOPMAX").arg(key).arg(count)
    }

    /// Removes and returns the member with the lowest score in a sorted set.
    /// Blocks until a member is available otherwise.
    /// [Redis Docs](https://redis.io/commands/BZPOPMIN)
    fn bzpopmin<K: ToRedisArgs>(key: K, timeout: f64) -> (Option<(String, String, f64)>) {
        cmd("BZPOPMIN").arg(key).arg(timeout)
    }

    /// Removes and returns up to count members with the lowest scores in a sorted set
    /// [Redis Docs](https://redis.io/commands/ZPOPMIN)
    fn zpopmin<K: ToRedisArgs>(key: K, count: isize) -> (Vec<String>) {
        cmd("ZPOPMIN").arg(key).arg(count)
    }

    /// Removes and returns up to count members with the highest scores,
    /// from the first non-empty sorted set in the provided list of key names.
    /// Blocks until a member is available otherwise.
    /// [Redis Docs](https://redis.io/commands/BZMPOP)
    fn bzmpop_max<K: ToRedisArgs>(timeout: f64, keys: K, count: isize) -> (Option<(String, Vec<(String, f64)>)>) {
        cmd("BZMPOP").arg(timeout).arg(keys.num_of_args()).arg(keys).arg("MAX").arg("COUNT").arg(count)
    }

    /// Removes and returns up to count members with the highest scores,
    /// from the first non-empty sorted set in the provided list of key names.
    /// [Redis Docs](https://redis.io/commands/ZMPOP)
    fn zmpop_max<K: ToRedisArgs>(keys: K, count: isize) -> (Option<(String, Vec<(String, f64)>)>) {
        cmd("ZMPOP").arg(keys.num_of_args()).arg(keys).arg("MAX").arg("COUNT").arg(count)
    }

    /// Removes and returns up to count members with the lowest scores,
    /// from the first non-empty sorted set in the provided list of key names.
    /// Blocks until a member is available otherwise.
    /// [Redis Docs](https://redis.io/commands/BZMPOP)
    fn bzmpop_min<K: ToRedisArgs>(timeout: f64, keys: K, count: isize) -> (Option<(String, Vec<(String, f64)>)>) {
        cmd("BZMPOP").arg(timeout).arg(keys.num_of_args()).arg(keys).arg("MIN").arg("COUNT").arg(count)
    }

    /// Removes and returns up to count members with the lowest scores,
    /// from the first non-empty sorted set in the provided list of key names.
    /// [Redis Docs](https://redis.io/commands/ZMPOP)
    fn zmpop_min<K: ToRedisArgs>(keys: K, count: isize) -> (Option<(String, Vec<(String, f64)>)>) {
        cmd("ZMPOP").arg(keys.num_of_args()).arg(keys).arg("MIN").arg("COUNT").arg(count)
    }

    /// Return up to count random members in a sorted set (or 1 if `count == None`)
    /// [Redis Docs](https://redis.io/commands/ZRANDMEMBER)
    fn zrandmember<K: ToRedisArgs>(key: K, count: Option<isize>) -> Generic {
        cmd("ZRANDMEMBER").arg(key).arg(count)
    }

    /// Return up to count random members in a sorted set with scores
    /// [Redis Docs](https://redis.io/commands/ZRANDMEMBER)
    fn zrandmember_withscores<K: ToRedisArgs>(key: K, count: isize) -> Generic {
        cmd("ZRANDMEMBER").arg(key).arg(count).arg("WITHSCORES")
    }

    /// Return a range of members in a sorted set, by index
    /// [Redis Docs](https://redis.io/commands/ZRANGE)
    fn zrange<K: ToRedisArgs>(key: K, start: isize, stop: isize) -> (Vec<String>) {
        cmd("ZRANGE").arg(key).arg(start).arg(stop)
    }

    /// Return a range of members in a sorted set, by index with scores.
    /// [Redis Docs](https://redis.io/commands/ZRANGE)
    fn zrange_withscores<K: ToRedisArgs>(key: K, start: isize, stop: isize) -> (Vec<(String, f64)>) {
        cmd("ZRANGE").arg(key).arg(start).arg(stop).arg("WITHSCORES")
    }

    /// Return a range of members in a sorted set, by lexicographical range.
    /// [Redis Docs](https://redis.io/commands/ZRANGEBYLEX)
    fn zrangebylex<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) -> (Vec<String>) {
        cmd("ZRANGEBYLEX").arg(key).arg(min).arg(max)
    }

    /// Return a range of members in a sorted set, by lexicographical
    /// range with offset and limit.
    /// [Redis Docs](https://redis.io/commands/ZRANGEBYLEX)
    fn zrangebylex_limit<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(
            key: K, min: M, max: MM, offset: isize, count: isize) -> (Vec<String>) {
        cmd("ZRANGEBYLEX").arg(key).arg(min).arg(max).arg("LIMIT").arg(offset).arg(count)
    }

    /// Return a range of members in a sorted set, by lexicographical range.
    /// [Redis Docs](https://redis.io/commands/ZREVRANGEBYLEX)
    fn zrevrangebylex<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>(key: K, max: MM, min: M) -> (Vec<String>) {
        cmd("ZREVRANGEBYLEX").arg(key).arg(max).arg(min)
    }

    /// Return a range of members in a sorted set, by lexicographical
    /// range with offset and limit.
    /// [Redis Docs](https://redis.io/commands/ZREVRANGEBYLEX)
    fn zrevrangebylex_limit<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>(
            key: K, max: MM, min: M, offset: isize, count: isize) -> (Vec<String>) {
        cmd("ZREVRANGEBYLEX").arg(key).arg(max).arg(min).arg("LIMIT").arg(offset).arg(count)
    }

    /// Return a range of members in a sorted set, by score.
    /// [Redis Docs](https://redis.io/commands/ZRANGEBYSCORE)
    fn zrangebyscore<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) -> (Vec<String>) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max)
    }

    /// Return a range of members in a sorted set, by score with scores.
    /// [Redis Docs](https://redis.io/commands/ZRANGEBYSCORE)
    fn zrangebyscore_withscores<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) -> (Vec<(String, usize)>) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max).arg("WITHSCORES")
    }

    /// Return a range of members in a sorted set, by score with limit.
    /// [Redis Docs](https://redis.io/commands/ZRANGEBYSCORE)
    fn zrangebyscore_limit<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>
            (key: K, min: M, max: MM, offset: isize, count: isize) -> (Vec<String>) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max).arg("LIMIT").arg(offset).arg(count)
    }

    /// Return a range of members in a sorted set, by score with limit with scores.
    /// [Redis Docs](https://redis.io/commands/ZRANGEBYSCORE)
    fn zrangebyscore_limit_withscores<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>
            (key: K, min: M, max: MM, offset: isize, count: isize) -> (Vec<(String, usize)>) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max).arg("WITHSCORES")
            .arg("LIMIT").arg(offset).arg(count)
    }

    /// Determine the index of a member in a sorted set.
    /// [Redis Docs](https://redis.io/commands/ZRANK)
    fn zrank<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) -> (Option<usize>) {
        cmd("ZRANK").arg(key).arg(member)
    }

    /// Remove one or more members from a sorted set.
    /// [Redis Docs](https://redis.io/commands/ZREM)
    fn zrem<K: ToRedisArgs, M: ToRedisArgs>(key: K, members: M) -> (usize) {
        cmd("ZREM").arg(key).arg(members)
    }

    /// Remove all members in a sorted set between the given lexicographical range.
    /// [Redis Docs](https://redis.io/commands/ZREMRANGEBYLEX)
    fn zrembylex<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) -> (usize) {
        cmd("ZREMRANGEBYLEX").arg(key).arg(min).arg(max)
    }

    /// Remove all members in a sorted set within the given indexes.
    /// [Redis Docs](https://redis.io/commands/ZREMRANGEBYRANK)
    fn zremrangebyrank<K: ToRedisArgs>(key: K, start: isize, stop: isize) -> (usize) {
        cmd("ZREMRANGEBYRANK").arg(key).arg(start).arg(stop)
    }

    /// Remove all members in a sorted set within the given scores.
    /// [Redis Docs](https://redis.io/commands/ZREMRANGEBYSCORE)
    fn zrembyscore<K: ToRedisArgs, M: ToRedisArgs, MM: ToRedisArgs>(key: K, min: M, max: MM) -> (usize) {
        cmd("ZREMRANGEBYSCORE").arg(key).arg(min).arg(max)
    }

    /// Return a range of members in a sorted set, by index,
    /// ordered from high to low.
    /// [Redis Docs](https://redis.io/commands/ZREVRANGE)
    fn zrevrange<K: ToRedisArgs>(key: K, start: isize, stop: isize) -> (Vec<String>) {
        cmd("ZREVRANGE").arg(key).arg(start).arg(stop)
    }

    /// Return a range of members in a sorted set, by index, with scores
    /// ordered from high to low.
    /// [Redis Docs](https://redis.io/commands/ZREVRANGE)
    fn zrevrange_withscores<K: ToRedisArgs>(key: K, start: isize, stop: isize) -> (Vec<String>) {
        cmd("ZREVRANGE").arg(key).arg(start).arg(stop).arg("WITHSCORES")
    }

    /// Return a range of members in a sorted set, by score.
    /// [Redis Docs](https://redis.io/commands/ZREVRANGEBYSCORE)
    fn zrevrangebyscore<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>(key: K, max: MM, min: M) -> (Vec<String>) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min)
    }

    /// Return a range of members in a sorted set, by score with scores.
    /// [Redis Docs](https://redis.io/commands/ZREVRANGEBYSCORE)
    fn zrevrangebyscore_withscores<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>(key: K, max: MM, min: M) -> (Vec<String>) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min).arg("WITHSCORES")
    }

    /// Return a range of members in a sorted set, by score with limit.
    /// [Redis Docs](https://redis.io/commands/ZREVRANGEBYSCORE)
    fn zrevrangebyscore_limit<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>
            (key: K, max: MM, min: M, offset: isize, count: isize) -> (Vec<String>) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min).arg("LIMIT").arg(offset).arg(count)
    }

    /// Return a range of members in a sorted set, by score with limit with scores.
    /// [Redis Docs](https://redis.io/commands/ZREVRANGEBYSCORE)
    fn zrevrangebyscore_limit_withscores<K: ToRedisArgs, MM: ToRedisArgs, M: ToRedisArgs>
            (key: K, max: MM, min: M, offset: isize, count: isize) -> (Vec<String>) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min).arg("WITHSCORES")
            .arg("LIMIT").arg(offset).arg(count)
    }

    /// Determine the index of a member in a sorted set, with scores ordered from high to low.
    /// [Redis Docs](https://redis.io/commands/ZREVRANK)
    fn zrevrank<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) -> (Option<usize>) {
        cmd("ZREVRANK").arg(key).arg(member)
    }

    /// Get the score associated with the given member in a sorted set.
    /// [Redis Docs](https://redis.io/commands/ZSCORE)
    fn zscore<K: ToRedisArgs, M: ToRedisArgs>(key: K, member: M) -> (Option<f64>) {
        cmd("ZSCORE").arg(key).arg(member)
    }

    /// Get the scores associated with multiple members in a sorted set.
    /// [Redis Docs](https://redis.io/commands/ZMSCORE)
    fn zscore_multiple<K: ToRedisArgs, M: ToRedisArgs>(key: K, members: &'a [M]) -> (Option<Vec<f64>>) {
        cmd("ZMSCORE").arg(key).arg(members)
    }

    /// Unions multiple sorted sets and store the resulting sorted set in
    /// a new key using SUM as aggregation function.
    /// [Redis Docs](https://redis.io/commands/ZUNIONSTORE)
    fn zunionstore<D: ToRedisArgs, K: ToRedisArgs>(dstkey: D, keys: K) -> (usize) {
        cmd("ZUNIONSTORE").arg(dstkey).arg(keys.num_of_args()).arg(keys)
    }

    /// Unions multiple sorted sets and store the resulting sorted set in
    /// a new key using MIN as aggregation function.
    /// [Redis Docs](https://redis.io/commands/ZUNIONSTORE)
    fn zunionstore_min<D: ToRedisArgs, K: ToRedisArgs>(dstkey: D, keys: K) -> (usize) {
        cmd("ZUNIONSTORE").arg(dstkey).arg(keys.num_of_args()).arg(keys).arg("AGGREGATE").arg("MIN")
    }

    /// Unions multiple sorted sets and store the resulting sorted set in
    /// a new key using MAX as aggregation function.
    /// [Redis Docs](https://redis.io/commands/ZUNIONSTORE)
    fn zunionstore_max<D: ToRedisArgs, K: ToRedisArgs>(dstkey: D, keys: K) -> (usize) {
        cmd("ZUNIONSTORE").arg(dstkey).arg(keys.num_of_args()).arg(keys).arg("AGGREGATE").arg("MAX")
    }

    /// [`Commands::zunionstore`], but with the ability to specify a
    /// multiplication factor for each sorted set by pairing one with each key
    /// in a tuple.
    /// [Redis Docs](https://redis.io/commands/ZUNIONSTORE)
    fn zunionstore_weights<D: ToRedisArgs, K: ToRedisArgs, W: ToRedisArgs>(dstkey: D, keys: &'a [(K, W)]) -> (usize) {
        let (keys, weights): (Vec<&K>, Vec<&W>) = keys.iter().map(|(key, weight):&(K, W)| -> ((&K, &W)) {(key, weight)}).unzip();
        cmd("ZUNIONSTORE").arg(dstkey).arg(keys.num_of_args()).arg(keys).arg("WEIGHTS").arg(weights)
    }

    /// [`Commands::zunionstore_min`], but with the ability to specify a
    /// multiplication factor for each sorted set by pairing one with each key
    /// in a tuple.
    /// [Redis Docs](https://redis.io/commands/ZUNIONSTORE)
    fn zunionstore_min_weights<D: ToRedisArgs, K: ToRedisArgs, W: ToRedisArgs>(dstkey: D, keys: &'a [(K, W)]) -> (usize) {
        let (keys, weights): (Vec<&K>, Vec<&W>) = keys.iter().map(|(key, weight):&(K, W)| -> ((&K, &W)) {(key, weight)}).unzip();
        cmd("ZUNIONSTORE").arg(dstkey).arg(keys.num_of_args()).arg(keys).arg("AGGREGATE").arg("MIN").arg("WEIGHTS").arg(weights)
    }

    /// [`Commands::zunionstore_max`], but with the ability to specify a
    /// multiplication factor for each sorted set by pairing one with each key
    /// in a tuple.
    /// [Redis Docs](https://redis.io/commands/ZUNIONSTORE)
    fn zunionstore_max_weights<D: ToRedisArgs, K: ToRedisArgs, W: ToRedisArgs>(dstkey: D, keys: &'a [(K, W)]) -> (usize) {
        let (keys, weights): (Vec<&K>, Vec<&W>) = keys.iter().map(|(key, weight):&(K, W)| -> ((&K, &W)) {(key, weight)}).unzip();
        cmd("ZUNIONSTORE").arg(dstkey).arg(keys.num_of_args()).arg(keys).arg("AGGREGATE").arg("MAX").arg("WEIGHTS").arg(weights)
    }

    // hyperloglog commands

    /// Adds the specified elements to the specified HyperLogLog.
    /// [Redis Docs](https://redis.io/commands/PFADD)
    fn pfadd<K: ToRedisArgs, E: ToRedisArgs>(key: K, element: E) -> (bool) {
        cmd("PFADD").arg(key).arg(element)
    }

    /// Return the approximated cardinality of the set(s) observed by the
    /// HyperLogLog at key(s).
    /// [Redis Docs](https://redis.io/commands/PFCOUNT)
    fn pfcount<K: ToRedisArgs>(key: K) -> (usize) {
        cmd("PFCOUNT").arg(key)
    }

    /// Merge N different HyperLogLogs into a single one.
    /// [Redis Docs](https://redis.io/commands/PFMERGE)
    fn pfmerge<D: ToRedisArgs, S: ToRedisArgs>(dstkey: D, srckeys: S) -> (()) {
        cmd("PFMERGE").arg(dstkey).arg(srckeys)
    }

    /// Posts a message to the given channel.
    /// [Redis Docs](https://redis.io/commands/PUBLISH)
    fn publish<K: ToRedisArgs, E: ToRedisArgs>(channel: K, message: E) -> (usize) {
        cmd("PUBLISH").arg(channel).arg(message)
    }

    /// Posts a message to the given sharded channel.
    /// [Redis Docs](https://redis.io/commands/SPUBLISH)
    fn spublish<K: ToRedisArgs, E: ToRedisArgs>(channel: K, message: E) -> (usize) {
        cmd("SPUBLISH").arg(channel).arg(message)
    }

    // Object commands

    /// Returns the encoding of a key.
    /// [Redis Docs](https://redis.io/commands/OBJECT)
    fn object_encoding<K: ToRedisArgs>(key: K) -> (Option<String>) {
        cmd("OBJECT").arg("ENCODING").arg(key)
    }

    /// Returns the time in seconds since the last access of a key.
    /// [Redis Docs](https://redis.io/commands/OBJECT)
    fn object_idletime<K: ToRedisArgs>(key: K) -> (Option<usize>) {
        cmd("OBJECT").arg("IDLETIME").arg(key)
    }

    /// Returns the logarithmic access frequency counter of a key.
    /// [Redis Docs](https://redis.io/commands/OBJECT)
    fn object_freq<K: ToRedisArgs>(key: K) -> (Option<usize>) {
        cmd("OBJECT").arg("FREQ").arg(key)
    }

    /// Returns the reference count of a key.
    /// [Redis Docs](https://redis.io/commands/OBJECT)
    fn object_refcount<K: ToRedisArgs>(key: K) -> (Option<usize>) {
        cmd("OBJECT").arg("REFCOUNT").arg(key)
    }

    /// Returns the name of the current connection as set by CLIENT SETNAME.
    /// [Redis Docs](https://redis.io/commands/CLIENT)
    fn client_getname<>() -> (Option<String>) {
        cmd("CLIENT").arg("GETNAME")
    }

    /// Returns the ID of the current connection.
    /// [Redis Docs](https://redis.io/commands/CLIENT)
    fn client_id<>() -> (isize) {
        cmd("CLIENT").arg("ID")
    }

    /// Command assigns a name to the current connection.
    /// [Redis Docs](https://redis.io/commands/CLIENT)
    fn client_setname<K: ToRedisArgs>(connection_name: K) -> (()) {
        cmd("CLIENT").arg("SETNAME").arg(connection_name)
    }

    // ACL commands

    /// When Redis is configured to use an ACL file (with the aclfile
    /// configuration option), this command will reload the ACLs from the file,
    /// replacing all the current ACL rules with the ones defined in the file.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    /// [Redis Docs](https://redis.io/commands/ACL)
    fn acl_load<>() -> () {
        cmd("ACL").arg("LOAD")
    }

    /// When Redis is configured to use an ACL file (with the aclfile
    /// configuration option), this command will save the currently defined
    /// ACLs from the server memory to the ACL file.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    /// [Redis Docs](https://redis.io/commands/ACL)
    fn acl_save<>() -> () {
        cmd("ACL").arg("SAVE")
    }

    /// Shows the currently active ACL rules in the Redis server.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    /// [Redis Docs](https://redis.io/commands/ACL)
    fn acl_list<>() -> (Vec<String>) {
        cmd("ACL").arg("LIST")
    }

    /// Shows a list of all the usernames of the currently configured users in
    /// the Redis ACL system.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    /// [Redis Docs](https://redis.io/commands/ACL)
    fn acl_users<>() -> (Vec<String>) {
        cmd("ACL").arg("USERS")
    }

    /// Returns all the rules defined for an existing ACL user.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    /// [Redis Docs](https://redis.io/commands/ACL)
    fn acl_getuser<K: ToRedisArgs>(username: K) -> (Option<std::collections::HashMap<String, Value>>) {
        cmd("ACL").arg("GETUSER").arg(username)
    }

    /// Creates an ACL user without any privilege.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    /// [Redis Docs](https://redis.io/commands/ACL)
    fn acl_setuser<K: ToRedisArgs>(username: K) -> () {
        cmd("ACL").arg("SETUSER").arg(username)
    }

    /// Creates an ACL user with the specified rules or modify the rules of
    /// an existing user.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    /// [Redis Docs](https://redis.io/commands/ACL)
    fn acl_setuser_rules<K: ToRedisArgs>(username: K, rules: &'a [acl::Rule]) -> () {
        cmd("ACL").arg("SETUSER").arg(username).arg(rules)
    }

    /// Delete all the specified ACL users and terminate all the connections
    /// that are authenticated with such users.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    /// [Redis Docs](https://redis.io/commands/ACL)
    fn acl_deluser<K: ToRedisArgs>(usernames: &'a [K]) -> (usize) {
        cmd("ACL").arg("DELUSER").arg(usernames)
    }

    /// Simulate the execution of a given command by a given user.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    /// [Redis Docs](https://redis.io/commands/ACL)
    fn acl_dryrun<K: ToRedisArgs, C: ToRedisArgs, A: ToRedisArgs>(username: K, command: C, args: A) -> (String) {
        cmd("ACL").arg("DRYRUN").arg(username).arg(command).arg(args)
    }

    /// Shows the available ACL categories.
    /// [Redis Docs](https://redis.io/commands/ACL)
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_cat<>() -> (Vec<String>) {
        cmd("ACL").arg("CAT")
    }

    /// Shows all the Redis commands in the specified category.
    /// [Redis Docs](https://redis.io/commands/ACL)
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_cat_categoryname<K: ToRedisArgs>(categoryname: K) -> (Vec<String>) {
        cmd("ACL").arg("CAT").arg(categoryname)
    }

    /// Generates a 256-bits password starting from /dev/urandom if available.
    /// [Redis Docs](https://redis.io/commands/ACL)
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_genpass<>() -> (String) {
        cmd("ACL").arg("GENPASS")
    }

    /// Generates a 1-to-1024-bits password starting from /dev/urandom if available.
    /// [Redis Docs](https://redis.io/commands/ACL)
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_genpass_bits<>(bits: isize) -> (String) {
        cmd("ACL").arg("GENPASS").arg(bits)
    }

    /// Returns the username the current connection is authenticated with.
    /// [Redis Docs](https://redis.io/commands/ACL)
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_whoami<>() -> (String) {
        cmd("ACL").arg("WHOAMI")
    }

    /// Shows a list of recent ACL security events
    /// [Redis Docs](https://redis.io/commands/ACL)
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_log<>(count: isize) -> (Vec<String>) {
        cmd("ACL").arg("LOG").arg(count)

    }

    /// Clears the ACL log.
    /// [Redis Docs](https://redis.io/commands/ACL)
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_log_reset<>() -> () {
        cmd("ACL").arg("LOG").arg("RESET")
    }

    /// Returns a helpful text describing the different subcommands.
    /// [Redis Docs](https://redis.io/commands/ACL)
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_help<>() -> (String) {
        cmd("ACL").arg("HELP")
    }

    //
    // geospatial commands
    //

    /// Adds the specified geospatial items to the specified key.
    ///
    /// Every member has to be written as a tuple of `(longitude, latitude,
    /// member_name)`. It can be a single tuple, or a vector of tuples.
    ///
    /// `longitude, latitude` can be set using [`redis::geo::Coord`][1].
    ///
    /// [1]: ./geo/struct.Coord.html
    ///
    /// Returns the number of elements added to the sorted set, not including
    /// elements already existing for which the score was updated.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use redis::{Commands, Connection, RedisResult};
    /// use redis::geo::Coord;
    ///
    /// fn add_point(con: &mut Connection) -> (RedisResult<isize>) {
    ///     con.geo_add("my_gis", (Coord::lon_lat(13.361389, 38.115556), "Palermo"))
    /// }
    ///
    /// fn add_point_with_tuples(con: &mut Connection) -> (RedisResult<isize>) {
    ///     con.geo_add("my_gis", ("13.361389", "38.115556", "Palermo"))
    /// }
    ///
    /// fn add_many_points(con: &mut Connection) -> (RedisResult<isize>) {
    ///     con.geo_add("my_gis", &[
    ///         ("13.361389", "38.115556", "Palermo"),
    ///         ("15.087269", "37.502669", "Catania")
    ///     ])
    /// }
    /// ```
    /// [Redis Docs](https://redis.io/commands/GEOADD)
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geo_add<K: ToRedisArgs, M: ToRedisArgs>(key: K, members: M) -> (usize) {
        cmd("GEOADD").arg(key).arg(members)
    }

    /// Return the distance between two members in the geospatial index
    /// represented by the sorted set.
    ///
    /// If one or both the members are missing, the command returns NULL, so
    /// it may be convenient to parse its response as either `Option<f64>` or
    /// `Option<String>`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use redis::{Commands, RedisResult};
    /// use redis::geo::Unit;
    ///
    /// fn get_dists(con: &mut redis::Connection) {
    ///     let x: RedisResult<f64> = con.geo_dist(
    ///         "my_gis",
    ///         "Palermo",
    ///         "Catania",
    ///         Unit::Kilometers
    ///     );
    ///     // x is Ok(166.2742)
    ///
    ///     let x: RedisResult<Option<f64>> = con.geo_dist(
    ///         "my_gis",
    ///         "Palermo",
    ///         "Atlantis",
    ///         Unit::Meters
    ///     );
    ///     // x is Ok(None)
    /// }
    /// ```
    /// [Redis Docs](https://redis.io/commands/GEODIST)
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geo_dist<K: ToRedisArgs, M1: ToRedisArgs, M2: ToRedisArgs>(
        key: K,
        member1: M1,
        member2: M2,
        unit: geo::Unit
    ) -> (Option<String>) {
        cmd("GEODIST")
            .arg(key)
            .arg(member1)
            .arg(member2)
            .arg(unit)
    }

    /// Return valid [Geohash][1] strings representing the position of one or
    /// more members of the geospatial index represented by the sorted set at
    /// key.
    ///
    /// [1]: https://en.wikipedia.org/wiki/Geohash
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use redis::{Commands, RedisResult};
    ///
    /// fn get_hash(con: &mut redis::Connection) {
    ///     let x: RedisResult<Vec<String>> = con.geo_hash("my_gis", "Palermo");
    ///     // x is vec!["sqc8b49rny0"]
    ///
    ///     let x: RedisResult<Vec<String>> = con.geo_hash("my_gis", &["Palermo", "Catania"]);
    ///     // x is vec!["sqc8b49rny0", "sqdtr74hyu0"]
    /// }
    /// ```
    /// [Redis Docs](https://redis.io/commands/GEOHASH)
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geo_hash<K: ToRedisArgs, M: ToRedisArgs>(key: K, members: M) -> (Vec<String>) {
        cmd("GEOHASH").arg(key).arg(members)
    }

    /// Return the positions of all the specified members of the geospatial
    /// index represented by the sorted set at key.
    ///
    /// Every position is a pair of `(longitude, latitude)`. [`redis::geo::Coord`][1]
    /// can be used to convert these value in a struct.
    ///
    /// [1]: ./geo/struct.Coord.html
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use redis::{Commands, RedisResult};
    /// use redis::geo::Coord;
    ///
    /// fn get_position(con: &mut redis::Connection) {
    ///     let x: RedisResult<Vec<Vec<f64>>> = con.geo_pos("my_gis", &["Palermo", "Catania"]);
    ///     // x is [ [ 13.361389, 38.115556 ], [ 15.087269, 37.502669 ] ];
    ///
    ///     let x: Vec<Coord<f64>> = con.geo_pos("my_gis", "Palermo").unwrap();
    ///     // x[0].longitude is 13.361389
    ///     // x[0].latitude is 38.115556
    /// }
    /// ```
    /// [Redis Docs](https://redis.io/commands/GEOPOS)
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geo_pos<K: ToRedisArgs, M: ToRedisArgs>(key: K, members: M) -> (Vec<Option<(f64, f64)>>) {
        cmd("GEOPOS").arg(key).arg(members)
    }

    /// Return the members of a sorted set populated with geospatial information
    /// using [`geo_add`](#method.geo_add), which are within the borders of the area
    /// specified with the center location and the maximum distance from the center
    /// (the radius).
    ///
    /// Every item in the result can be read with [`redis::geo::RadiusSearchResult`][1],
    /// which support the multiple formats returned by `GEORADIUS`.
    ///
    /// [1]: ./geo/struct.RadiusSearchResult.html
    ///
    /// ```rust,no_run
    /// use redis::{Commands, RedisResult};
    /// use redis::geo::{RadiusOptions, RadiusSearchResult, RadiusOrder, Unit};
    ///
    /// fn radius(con: &mut redis::Connection) -> (Vec<RadiusSearchResult>) {
    ///     let opts = RadiusOptions::default().with_dist().order(RadiusOrder::Asc);
    ///     con.geo_radius("my_gis", 15.90, 37.21, 51.39, Unit::Kilometers, opts).unwrap()
    /// }
    /// ```
    /// [Redis Docs](https://redis.io/commands/GEORADIUS)
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geo_radius<K: ToRedisArgs>(
        key: K,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: geo::Unit,
        options: geo::RadiusOptions
    ) -> Generic {
        cmd("GEORADIUS")
            .arg(key)
            .arg(longitude)
            .arg(latitude)
            .arg(radius)
            .arg(unit)
            .arg(options)
    }

    /// Retrieve members selected by distance with the center of `member`. The
    /// member itself is always contained in the results.
    /// [Redis Docs](https://redis.io/commands/GEORADIUSBYMEMBER)
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geo_radius_by_member<K: ToRedisArgs, M: ToRedisArgs>(
        key: K,
        member: M,
        radius: f64,
        unit: geo::Unit,
        options: geo::RadiusOptions
    ) -> Generic {
        cmd("GEORADIUSBYMEMBER")
            .arg(key)
            .arg(member)
            .arg(radius)
            .arg(unit)
            .arg(options)
    }

    //
    // streams commands
    //

    /// Ack pending stream messages checked out by a consumer.
    ///
    /// ```text
    /// XACK <key> <group> <id> <id> ... <id>
    /// ```
    /// [Redis Docs](https://redis.io/commands/XACK)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xack<K: ToRedisArgs, G: ToRedisArgs, I: ToRedisArgs>(
        key: K,
        group: G,
        ids: &'a [I]) -> (usize) {
        cmd("XACK")
            .arg(key)
            .arg(group)
            .arg(ids)
    }


    /// Add a stream message by `key`. Use `*` as the `id` for the current timestamp.
    ///
    /// ```text
    /// XADD key <ID or *> [field value] [field value] ...
    /// ```
    /// [Redis Docs](https://redis.io/commands/XADD)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xadd<K: ToRedisArgs, ID: ToRedisArgs, F: ToRedisArgs, V: ToRedisArgs>(
        key: K,
        id: ID,
        items: &'a [(F, V)]
    ) -> (Option<String>) {
        cmd("XADD").arg(key).arg(id).arg(items)
    }


    /// BTreeMap variant for adding a stream message by `key`.
    /// Use `*` as the `id` for the current timestamp.
    ///
    /// ```text
    /// XADD key <ID or *> [rust BTreeMap] ...
    /// ```
    /// [Redis Docs](https://redis.io/commands/XADD)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xadd_map<K: ToRedisArgs, ID: ToRedisArgs, BTM: ToRedisArgs>(
        key: K,
        id: ID,
        map: BTM
    ) -> (Option<String>) {
        cmd("XADD").arg(key).arg(id).arg(map)
    }


    /// Add a stream message with options.
    ///
    /// Items can be any list type, e.g.
    /// ```rust
    /// // static items
    /// let items = &[("key", "val"), ("key2", "val2")];
    /// # use std::collections::BTreeMap;
    /// // A map (Can be BTreeMap, HashMap, etc)
    /// let mut map: BTreeMap<&str, &str> = BTreeMap::new();
    /// map.insert("ab", "cd");
    /// map.insert("ef", "gh");
    /// map.insert("ij", "kl");
    /// ```
    ///
    /// ```text
    /// XADD key [NOMKSTREAM] [<MAXLEN|MINID> [~|=] threshold [LIMIT count]] <* | ID> field value [field value] ...
    /// ```
    /// [Redis Docs](https://redis.io/commands/XADD)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xadd_options<
        K: ToRedisArgs, ID: ToRedisArgs, I: ToRedisArgs
    >(
        key: K,
        id: ID,
        items: I,
        options: &'a streams::StreamAddOptions
    ) -> (Option<String>) {
        cmd("XADD")
            .arg(key)
            .arg(options)
            .arg(id)
            .arg(items)
    }


    /// Add a stream message while capping the stream at a maxlength.
    ///
    /// ```text
    /// XADD key [MAXLEN [~|=] <count>] <ID or *> [field value] [field value] ...
    /// ```
    /// [Redis Docs](https://redis.io/commands/XADD)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xadd_maxlen<
        K: ToRedisArgs,
        ID: ToRedisArgs,
        F: ToRedisArgs,
        V: ToRedisArgs
    >(
        key: K,
        maxlen: streams::StreamMaxlen,
        id: ID,
        items: &'a [(F, V)]
    ) -> (Option<String>) {
        cmd("XADD")
            .arg(key)
            .arg(maxlen)
            .arg(id)
            .arg(items)
    }


    /// BTreeMap variant for adding a stream message while capping the stream at a maxlength.
    ///
    /// ```text
    /// XADD key [MAXLEN [~|=] <count>] <ID or *> [rust BTreeMap] ...
    /// ```
    /// [Redis Docs](https://redis.io/commands/XADD)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xadd_maxlen_map<K: ToRedisArgs, ID: ToRedisArgs, BTM: ToRedisArgs>(
        key: K,
        maxlen: streams::StreamMaxlen,
        id: ID,
        map: BTM
    ) -> (Option<String>) {
        cmd("XADD")
            .arg(key)
            .arg(maxlen)
            .arg(id)
            .arg(map)
    }

    /// Perform a combined xpending and xclaim flow.
    ///
    /// ```no_run
    /// use redis::{Connection,Commands,RedisResult};
    /// use redis::streams::{StreamAutoClaimOptions, StreamAutoClaimReply};
    /// let client = redis::Client::open("redis://127.0.0.1/0").unwrap();
    /// let mut con = client.get_connection().unwrap();
    ///
    /// let opts = StreamAutoClaimOptions::default();
    /// let results : RedisResult<StreamAutoClaimReply> = con.xautoclaim_options("k1", "g1", "c1", 10, "0-0", opts);
    /// ```
    ///
    /// ```text
    /// XAUTOCLAIM <key> <group> <consumer> <min-idle-time> <start> [COUNT <count>] [JUSTID]
    /// ```
    /// [Redis Docs](https://redis.io/commands/XAUTOCLAIM)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xautoclaim_options<
        K: ToRedisArgs,
        G: ToRedisArgs,
        C: ToRedisArgs,
        MIT: ToRedisArgs,
        S: ToRedisArgs
    >(
        key: K,
        group: G,
        consumer: C,
        min_idle_time: MIT,
        start: S,
        options: streams::StreamAutoClaimOptions
    ) -> (String, Vec<String>, Vec<String>) {
        cmd("XAUTOCLAIM")
            .arg(key)
            .arg(group)
            .arg(consumer)
            .arg(min_idle_time)
            .arg(start)
            .arg(options)
    }

    /// Claim pending, unacked messages, after some period of time,
    /// currently checked out by another consumer.
    ///
    /// This method only accepts the must-have arguments for claiming messages.
    /// If optional arguments are required, see `xclaim_options` below.
    ///
    /// ```text
    /// XCLAIM <key> <group> <consumer> <min-idle-time> [<ID-1> <ID-2>]
    /// ```
    /// [Redis Docs](https://redis.io/commands/XCLAIM)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xclaim<K: ToRedisArgs, G: ToRedisArgs, C: ToRedisArgs, MIT: ToRedisArgs, ID: ToRedisArgs>(
        key: K,
        group: G,
        consumer: C,
        min_idle_time: MIT,
        ids: &'a [ID]
    ) -> (Vec<(String, Value)>) {
        cmd("XCLAIM")
            .arg(key)
            .arg(group)
            .arg(consumer)
            .arg(min_idle_time)
            .arg(ids)
    }

    /// This is the optional arguments version for claiming unacked, pending messages
    /// currently checked out by another consumer.
    ///
    /// ```no_run
    /// use redis::{Connection,Commands,RedisResult};
    /// use redis::streams::{StreamClaimOptions,StreamClaimReply};
    /// let client = redis::Client::open("redis://127.0.0.1/0").unwrap();
    /// let mut con = client.get_connection().unwrap();
    ///
    /// // Claim all pending messages for key "k1",
    /// // from group "g1", checked out by consumer "c1"
    /// // for 10ms with RETRYCOUNT 2 and FORCE
    ///
    /// let opts = StreamClaimOptions::default()
    ///     .with_force()
    ///     .retry(2);
    /// let results: RedisResult<StreamClaimReply> =
    ///     con.xclaim_options("k1", "g1", "c1", 10, &["0"], opts);
    ///
    /// // All optional arguments return a `Result<StreamClaimReply>` with one exception:
    /// // Passing JUSTID returns only the message `id` and omits the HashMap for each message.
    ///
    /// let opts = StreamClaimOptions::default()
    ///     .with_justid();
    /// let results: RedisResult<Vec<String>> =
    ///     con.xclaim_options("k1", "g1", "c1", 10, &["0"], opts);
    /// ```
    ///
    /// ```text
    /// XCLAIM <key> <group> <consumer> <min-idle-time> <ID-1> <ID-2>
    ///     [IDLE <milliseconds>] [TIME <mstime>] [RETRYCOUNT <count>]
    ///     [FORCE] [JUSTID] [LASTID <lastid>]
    /// ```
    /// [Redis Docs](https://redis.io/commands/XCLAIM)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xclaim_options<
        K: ToRedisArgs,
        G: ToRedisArgs,
        C: ToRedisArgs,
        MIT: ToRedisArgs,
        ID: ToRedisArgs
    >(
        key: K,
        group: G,
        consumer: C,
        min_idle_time: MIT,
        ids: &'a [ID],
        options: streams::StreamClaimOptions
    ) -> Generic {
        cmd("XCLAIM")
            .arg(key)
            .arg(group)
            .arg(consumer)
            .arg(min_idle_time)
            .arg(ids)
            .arg(options)
    }


    /// Deletes a list of `id`s for a given stream `key`.
    ///
    /// ```text
    /// XDEL <key> [<ID1> <ID2> ... <IDN>]
    /// ```
    /// [Redis Docs](https://redis.io/commands/XDEL)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xdel<K: ToRedisArgs, ID: ToRedisArgs>(
        key: K,
        ids: &'a [ID]
    ) -> (usize) {
        cmd("XDEL").arg(key).arg(ids)
    }


    /// This command is used for creating a consumer `group`. It expects the stream key
    /// to already exist. Otherwise, use `xgroup_create_mkstream` if it doesn't.
    /// The `id` is the starting message id all consumers should read from. Use `$` If you want
    /// all consumers to read from the last message added to stream.
    ///
    /// ```text
    /// XGROUP CREATE <key> <groupname> <id or $>
    /// ```
    /// [Redis Docs](https://redis.io/commands/XGROUP)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup_create<K: ToRedisArgs, G: ToRedisArgs, ID: ToRedisArgs>(
        key: K,
        group: G,
        id: ID
    ) -> () {
        cmd("XGROUP")
            .arg("CREATE")
            .arg(key)
            .arg(group)
            .arg(id)
    }

    /// This creates a `consumer` explicitly (vs implicit via XREADGROUP)
    /// for given stream `key.
    ///
    /// The return value is either a 0 or a 1 for the number of consumers created
    /// 0 means the consumer already exists
    ///
    /// ```text
    /// XGROUP CREATECONSUMER <key> <groupname> <consumername>
    /// ```
    /// [Redis Docs](https://redis.io/commands/XGROUP)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup_createconsumer<K: ToRedisArgs, G: ToRedisArgs, C: ToRedisArgs>(
        key: K,
        group: G,
        consumer: C
    ) -> bool {
        cmd("XGROUP")
            .arg("CREATECONSUMER")
            .arg(key)
            .arg(group)
            .arg(consumer)
    }

    /// This is the alternate version for creating a consumer `group`
    /// which makes the stream if it doesn't exist.
    ///
    /// ```text
    /// XGROUP CREATE <key> <groupname> <id or $> [MKSTREAM]
    /// ```
    /// [Redis Docs](https://redis.io/commands/XGROUP)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup_create_mkstream<
        K: ToRedisArgs,
        G: ToRedisArgs,
        ID: ToRedisArgs
    >(
        key: K,
        group: G,
        id: ID
    ) -> () {
        cmd("XGROUP")
            .arg("CREATE")
            .arg(key)
            .arg(group)
            .arg(id)
            .arg("MKSTREAM")
    }


    /// Alter which `id` you want consumers to begin reading from an existing
    /// consumer `group`.
    ///
    /// ```text
    /// XGROUP SETID <key> <groupname> <id or $>
    /// ```
    /// [Redis Docs](https://redis.io/commands/XGROUP)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup_setid<K: ToRedisArgs, G: ToRedisArgs, ID: ToRedisArgs>(
        key: K,
        group: G,
        id: ID
    ) -> () {
        cmd("XGROUP")
            .arg("SETID")
            .arg(key)
            .arg(group)
            .arg(id)
    }


    /// Destroy an existing consumer `group` for a given stream `key`
    ///
    /// ```text
    /// XGROUP SETID <key> <groupname> <id or $>
    /// ```
    /// [Redis Docs](https://redis.io/commands/XGROUP)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup_destroy<K: ToRedisArgs, G: ToRedisArgs>(
        key: K,
        group: G
    ) -> bool {
        cmd("XGROUP").arg("DESTROY").arg(key).arg(group)
    }

    /// This deletes a `consumer` from an existing consumer `group`
    /// for given stream `key.
    ///
    /// ```text
    /// XGROUP DELCONSUMER <key> <groupname> <consumername>
    /// ```
    /// [Redis Docs](https://redis.io/commands/XGROUP)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup_delconsumer<K: ToRedisArgs, G: ToRedisArgs, C: ToRedisArgs>(
        key: K,
        group: G,
        consumer: C
    ) -> usize {
        cmd("XGROUP")
            .arg("DELCONSUMER")
            .arg(key)
            .arg(group)
            .arg(consumer)
    }


    /// This returns all info details about
    /// which consumers have read messages for given consumer `group`.
    /// Take note of the StreamInfoConsumersReply return type.
    ///
    /// *It's possible this return value might not contain new fields
    /// added by Redis in future versions.*
    ///
    /// ```text
    /// XINFO CONSUMERS <key> <group>
    /// ```
    /// [Redis Docs](https://redis.io/commands/XINFO")
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xinfo_consumers<K: ToRedisArgs, G: ToRedisArgs>(
        key: K,
        group: G
    ) -> (Vec<std::collections::HashMap<String, Value>>) {
        cmd("XINFO")
            .arg("CONSUMERS")
            .arg(key)
            .arg(group)
    }


    /// Returns all consumer `group`s created for a given stream `key`.
    /// Take note of the StreamInfoGroupsReply return type.
    ///
    /// *It's possible this return value might not contain new fields
    /// added by Redis in future versions.*
    ///
    /// ```text
    /// XINFO GROUPS <key>
    /// ```
    /// [Redis Docs](https://redis.io/commands/XINFO")
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    /// [Redis Docs](https://redis.io/commands/XINFO").arg("GROUPS)
    fn xinfo_groups<K: ToRedisArgs>(key: K) -> (Vec<std::collections::HashMap<String, Value>>) {
        cmd("XINFO").arg("GROUPS").arg(key)
    }


    /// Returns info about high-level stream details
    /// (first & last message `id`, length, number of groups, etc.)
    /// Take note of the StreamInfoStreamReply return type.
    ///
    /// *It's possible this return value might not contain new fields
    /// added by Redis in future versions.*
    ///
    /// ```text
    /// XINFO STREAM <key>
    /// ```
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    /// [Redis Docs](https://redis.io/commands/XINFO").arg("STREAM)
    fn xinfo_stream<K: ToRedisArgs>(key: K) -> Generic {
        cmd("XINFO").arg("STREAM").arg(key)
    }

    /// Returns the number of messages for a given stream `key`.
    ///
    /// ```text
    /// XLEN <key>
    /// ```
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    /// [Redis Docs](https://redis.io/commands/XLEN)
    fn xlen<K: ToRedisArgs>(key: K) -> usize {
        cmd("XLEN").arg(key)
    }


    /// This is a basic version of making XPENDING command calls which only
    /// passes a stream `key` and consumer `group` and it
    /// returns details about which consumers have pending messages
    /// that haven't been acked.
    ///
    /// You can use this method along with
    /// `xclaim` or `xclaim_options` for determining which messages
    /// need to be retried.
    ///
    /// Take note of the StreamPendingReply return type.
    ///
    /// ```text
    /// XPENDING <key> <group> [<start> <stop> <count> [<consumer>]]
    /// ```
    /// [Redis Docs](https://redis.io/commands/XPENDING)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xpending<K: ToRedisArgs, G: ToRedisArgs>(
        key: K,
        group: G
    ) -> Generic {
        cmd("XPENDING").arg(key).arg(group)
    }


    /// This XPENDING version returns a list of all messages over the range.
    /// You can use this for paginating pending messages (but without the message HashMap).
    ///
    /// Start and end follow the same rules `xrange` args. Set start to `-`
    /// and end to `+` for the entire stream.
    ///
    /// Take note of the StreamPendingCountReply return type.
    ///
    /// ```text
    /// XPENDING <key> <group> <start> <stop> <count>
    /// ```
    /// [Redis Docs](https://redis.io/commands/XPENDING)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xpending_count<
        K: ToRedisArgs,
        G: ToRedisArgs,
        S: ToRedisArgs,
        E: ToRedisArgs,
        C: ToRedisArgs
    >(
        key: K,
        group: G,
        start: S,
        end: E,
        count: C
    ) -> Generic {
        cmd("XPENDING")
            .arg(key)
            .arg(group)
            .arg(start)
            .arg(end)
            .arg(count)
    }


    /// An alternate version of `xpending_count` which filters by `consumer` name.
    ///
    /// Start and end follow the same rules `xrange` args. Set start to `-`
    /// and end to `+` for the entire stream.
    ///
    /// Take note of the StreamPendingCountReply return type.
    ///
    /// ```text
    /// XPENDING <key> <group> <start> <stop> <count> <consumer>
    /// ```
    /// [Redis Docs](https://redis.io/commands/XPENDING)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xpending_consumer_count<
        K: ToRedisArgs,
        G: ToRedisArgs,
        S: ToRedisArgs,
        E: ToRedisArgs,
        C: ToRedisArgs,
        CN: ToRedisArgs
    >(
        key: K,
        group: G,
        start: S,
        end: E,
        count: C,
        consumer: CN
    ) -> Generic {
        cmd("XPENDING")
            .arg(key)
            .arg(group)
            .arg(start)
            .arg(end)
            .arg(count)
            .arg(consumer)
    }

    /// Returns a range of messages in a given stream `key`.
    ///
    /// Set `start` to `-` to begin at the first message.
    /// Set `end` to `+` to end the most recent message.
    /// You can pass message `id` to both `start` and `end`.
    ///
    /// Take note of the StreamRangeReply return type.
    ///
    /// ```text
    /// XRANGE key start end
    /// ```
    /// [Redis Docs](https://redis.io/commands/XRANGE)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xrange<K: ToRedisArgs, S: ToRedisArgs, E: ToRedisArgs>(
        key: K,
        start: S,
        end: E
    ) -> Generic {
        cmd("XRANGE").arg(key).arg(start).arg(end)
    }


    /// A helper method for automatically returning all messages in a stream by `key`.
    /// **Use with caution!**
    ///
    /// ```text
    /// XRANGE key - +
    /// ```
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    /// [Redis Docs](https://redis.io/commands/XRANGE").arg(key).arg("-").arg("+)
    fn xrange_all<K: ToRedisArgs>(key: K) -> Generic {
        cmd("XRANGE").arg(key).arg("-").arg("+")
    }


    /// A method for paginating a stream by `key`.
    ///
    /// ```text
    /// XRANGE key start end [COUNT <n>]
    /// ```
    /// [Redis Docs](https://redis.io/commands/XRANGE)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xrange_count<K: ToRedisArgs, S: ToRedisArgs, E: ToRedisArgs, C: ToRedisArgs>(
        key: K,
        start: S,
        end: E,
        count: C
    ) -> Generic {
        cmd("XRANGE")
            .arg(key)
            .arg(start)
            .arg(end)
            .arg("COUNT")
            .arg(count)
    }


    /// Read a list of `id`s for each stream `key`.
    /// This is the basic form of reading streams.
    /// For more advanced control, like blocking, limiting, or reading by consumer `group`,
    /// see `xread_options`.
    ///
    /// ```text
    /// XREAD STREAMS key_1 key_2 ... key_N ID_1 ID_2 ... ID_N
    /// ```
    /// [Redis Docs](https://redis.io/commands/XREAD)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xread<K: ToRedisArgs, ID: ToRedisArgs>(
        keys: &'a [K],
        ids: &'a [ID]
    ) -> (Option<std::collections::HashMap<String, Value>>) {
        cmd("XREAD").arg("STREAMS").arg(keys).arg(ids)
    }

    /// This method handles setting optional arguments for
    /// `XREAD` or `XREADGROUP` Redis commands.
    /// ```no_run
    /// use redis::{Connection,RedisResult,Commands};
    /// use redis::streams::{StreamReadOptions,StreamReadReply};
    /// let client = redis::Client::open("redis://127.0.0.1/0").unwrap();
    /// let mut con = client.get_connection().unwrap();
    ///
    /// // Read 10 messages from the start of the stream,
    /// // without registering as a consumer group.
    ///
    /// let opts = StreamReadOptions::default()
    ///     .count(10);
    /// let results: RedisResult<StreamReadReply> =
    ///     con.xread_options(&["k1"], &["0"], &opts);
    ///
    /// // Read all undelivered messages for a given
    /// // consumer group. Be advised: the consumer group must already
    /// // exist before making this call. Also note: we're passing
    /// // '>' as the id here, which means all undelivered messages.
    ///
    /// let opts = StreamReadOptions::default()
    ///     .group("group-1", "consumer-1");
    /// let results: RedisResult<StreamReadReply> =
    ///     con.xread_options(&["k1"], &[">"], &opts);
    /// ```
    ///
    /// ```text
    /// XREAD [BLOCK <milliseconds>] [COUNT <count>]
    ///     STREAMS key_1 key_2 ... key_N
    ///     ID_1 ID_2 ... ID_N
    ///
    /// XREADGROUP [GROUP group-name consumer-name] [BLOCK <milliseconds>] [COUNT <count>] [NOACK]
    ///     STREAMS key_1 key_2 ... key_N
    ///     ID_1 ID_2 ... ID_N
    /// ```
    /// [Redis Docs](https://redis.io/commands/XREAD)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xread_options<K: ToRedisArgs, ID: ToRedisArgs>(
        keys: &'a [K],
        ids: &'a [ID],
        options: &'a streams::StreamReadOptions
    ) -> (Option<std::collections::HashMap<String, Value>>) {
        cmd(if options.read_only() {
            "XREAD"
        } else {
            "XREADGROUP"
        })
        .arg(options)
        .arg("STREAMS")
        .arg(keys)
        .arg(ids)
    }

    /// This is the reverse version of `xrange`.
    /// The same rules apply for `start` and `end` here.
    ///
    /// ```text
    /// XREVRANGE key end start
    /// ```
    /// [Redis Docs](https://redis.io/commands/XREVRANGE)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xrevrange<K: ToRedisArgs, E: ToRedisArgs, S: ToRedisArgs>(
        key: K,
        end: E,
        start: S
    ) -> (Vec<Value>) {
        cmd("XREVRANGE").arg(key).arg(end).arg(start)
    }

    /// This is the reverse version of `xrange_all`.
    /// The same rules apply for `start` and `end` here.
    ///
    /// ```text
    /// XREVRANGE key + -
    /// ```
    /// [Redis Docs](https://redis.io/commands/XREVRANGE").arg(key).arg("+").arg("-)
    fn xrevrange_all<K: ToRedisArgs>(key: K) -> (Vec<Value>) {
        cmd("XREVRANGE").arg(key).arg("+").arg("-")
    }

    /// This is the reverse version of `xrange_count`.
    /// The same rules apply for `start` and `end` here.
    ///
    /// ```text
    /// XREVRANGE key end start [COUNT <n>]
    /// ```
    /// [Redis Docs](https://redis.io/commands/XREVRANGE)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xrevrange_count<K: ToRedisArgs, E: ToRedisArgs, S: ToRedisArgs, C: ToRedisArgs>(
        key: K,
        end: E,
        start: S,
        count: C
    ) -> (Vec<Value>) {
        cmd("XREVRANGE")
            .arg(key)
            .arg(end)
            .arg(start)
            .arg("COUNT")
            .arg(count)
    }

    /// Trim a stream `key` to a MAXLEN count.
    ///
    /// ```text
    /// XTRIM <key> MAXLEN [~|=] <count>  (Same as XADD MAXLEN option)
    /// ```
    /// [Redis Docs](https://redis.io/commands/XTRIM)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xtrim<K: ToRedisArgs>(
        key: K,
        maxlen: streams::StreamMaxlen
    ) -> usize {
        cmd("XTRIM").arg(key).arg(maxlen)
    }

     /// Trim a stream `key` with full options
     ///
     /// ```text
     /// XTRIM <key> <MAXLEN|MINID> [~|=] <threshold> [LIMIT <count>]  (Same as XADD MAXID|MINID options)
     /// ```
     /// [Redis Docs](https://redis.io/commands/XTRIM)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xtrim_options<K: ToRedisArgs>(
        key: K,
        options: &'a streams::StreamTrimOptions
    ) -> usize {
        cmd("XTRIM").arg(key).arg(options)
    }

    // script commands

    /// Adds a prepared script command to the pipeline.
    ///
    /// Note: unlike a call to [`invoke`](crate::ScriptInvocation::invoke), if the script isn't loaded during the pipeline operation,
    /// it will not automatically be loaded and retried. The script can be loaded using the
    /// [`load`](crate::ScriptInvocation::load) operation.
    #[cfg_attr(feature = "script", doc = r##"

# Examples:

```rust,no_run
# fn do_something() -> redis::RedisResult<()> {
# let client = redis::Client::open("redis://127.0.0.1/").unwrap();
# let mut con = client.get_connection().unwrap();
let script = redis::Script::new(r"
    return tonumber(ARGV[1]) + tonumber(ARGV[2]);
");
script.prepare_invoke().load(&mut con)?;
let (a, b): (isize, isize) = redis::pipe()
    .invoke_script(script.arg(1).arg(2))
    .invoke_script(script.arg(2).arg(3))
    .query(&mut con)?;

assert_eq!(a, 3);
assert_eq!(b, 5);
# Ok(()) }
```
"##)]
    #[cfg(feature = "script")]
    #[cfg_attr(docsrs, doc(cfg(feature = "script")))]
    fn invoke_script<>(invocation: &'a crate::ScriptInvocation<'a>) -> Generic {
        &mut invocation.eval_cmd()
    }

    // cleanup commands

    /// Deletes all the keys of all databases
    ///
    /// Whether the flushing happens asynchronously or synchronously depends on the configuration
    /// of your Redis server.
    ///
    /// To enforce a flush mode, use [`Commands::flushall_options`].
    ///
    /// ```text
    /// FLUSHALL
    /// ```
    /// [Redis Docs](https://redis.io/commands/FLUSHALL)
    fn flushall<>() -> () {
        &mut cmd("FLUSHALL")
    }

    /// Deletes all the keys of all databases with options
    ///
    /// ```text
    /// FLUSHALL [ASYNC|SYNC]
    /// ```
    /// [Redis Docs](https://redis.io/commands/FLUSHALL)
    fn flushall_options<>(options: &'a FlushAllOptions) -> () {
        cmd("FLUSHALL").arg(options)
    }

    /// Deletes all the keys of the current database
    ///
    /// Whether the flushing happens asynchronously or synchronously depends on the configuration
    /// of your Redis server.
    ///
    /// To enforce a flush mode, use [`Commands::flushdb_options`].
    ///
    /// ```text
    /// FLUSHDB
    /// ```
    /// [Redis Docs](https://redis.io/commands/FLUSHDB)
    fn flushdb<>() -> () {
        &mut cmd("FLUSHDB")
    }

    /// Deletes all the keys of the current database with options
    ///
    /// ```text
    /// FLUSHDB [ASYNC|SYNC]
    /// ```
    /// [Redis Docs](https://redis.io/commands/FLUSHDB)
    fn flushdb_options<>(options: &'a FlushDbOptions) -> () {
        cmd("FLUSHDB").arg(options)
    }
}

/// Allows pubsub callbacks to stop receiving messages.
///
/// Arbitrary data may be returned from `Break`.
pub enum ControlFlow<U> {
    /// Continues.
    Continue,
    /// Breaks with a value.
    Break(U),
}

/// The PubSub trait allows subscribing to one or more channels
/// and receiving a callback whenever a message arrives.
///
/// Each method handles subscribing to the list of keys, waiting for
/// messages, and unsubscribing from the same list of channels once
/// a ControlFlow::Break is encountered.
///
/// Once (p)subscribe returns Ok(U), the connection is again safe to use
/// for calling other methods.
///
/// # Examples
///
/// ```rust,no_run
/// # fn do_something() -> redis::RedisResult<()> {
/// use redis::{PubSubCommands, ControlFlow};
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let mut con = client.get_connection()?;
/// let mut count = 0;
/// con.subscribe(&["foo"], |msg| {
///     // do something with message
///     assert_eq!(msg.get_channel(), Ok(String::from("foo")));
///
///     // increment messages seen counter
///     count += 1;
///     match count {
///         // stop after receiving 10 messages
///         10 => ControlFlow::Break(()),
///         _ => ControlFlow::Continue,
///     }
/// })?;
/// # Ok(()) }
/// ```
// TODO In the future, it would be nice to implement Try such that `?` will work
//      within the closure.
pub trait PubSubCommands: Sized {
    /// Subscribe to a list of channels using SUBSCRIBE and run the provided
    /// closure for each message received.
    ///
    /// For every `Msg` passed to the provided closure, either
    /// `ControlFlow::Break` or `ControlFlow::Continue` must be returned. This
    /// method will not return until `ControlFlow::Break` is observed.
    fn subscribe<C, F, U>(&mut self, _: C, _: F) -> RedisResult<U>
    where
        F: FnMut(Msg) -> ControlFlow<U>,
        C: ToRedisArgs;

    /// Subscribe to a list of channels using PSUBSCRIBE and run the provided
    /// closure for each message received.
    ///
    /// For every `Msg` passed to the provided closure, either
    /// `ControlFlow::Break` or `ControlFlow::Continue` must be returned. This
    /// method will not return until `ControlFlow::Break` is observed.
    fn psubscribe<P, F, U>(&mut self, _: P, _: F) -> RedisResult<U>
    where
        F: FnMut(Msg) -> ControlFlow<U>,
        P: ToRedisArgs;
}

impl<T> Commands for T where T: ConnectionLike {}

#[cfg(feature = "aio")]
impl<T> AsyncCommands for T where T: crate::aio::ConnectionLike + Send + Sync + Sized {}

impl<T> TypedCommands for T where T: ConnectionLike {}

#[cfg(feature = "aio")]
impl<T> AsyncTypedCommands for T where T: crate::aio::ConnectionLike + Send + Sync + Sized {}

impl PubSubCommands for Connection {
    fn subscribe<C, F, U>(&mut self, channels: C, mut func: F) -> RedisResult<U>
    where
        F: FnMut(Msg) -> ControlFlow<U>,
        C: ToRedisArgs,
    {
        let mut pubsub = self.as_pubsub();
        pubsub.subscribe(channels)?;

        loop {
            let msg = pubsub.get_message()?;
            match func(msg) {
                ControlFlow::Continue => continue,
                ControlFlow::Break(value) => return Ok(value),
            }
        }
    }

    fn psubscribe<P, F, U>(&mut self, patterns: P, mut func: F) -> RedisResult<U>
    where
        F: FnMut(Msg) -> ControlFlow<U>,
        P: ToRedisArgs,
    {
        let mut pubsub = self.as_pubsub();
        pubsub.psubscribe(patterns)?;

        loop {
            let msg = pubsub.get_message()?;
            match func(msg) {
                ControlFlow::Continue => continue,
                ControlFlow::Break(value) => return Ok(value),
            }
        }
    }
}

/// Options for the [SCAN](https://redis.io/commands/scan) command
///
/// # Example
///
/// ```rust
/// use redis::{Commands, RedisResult, ScanOptions, Iter};
/// fn force_fetching_every_matching_key<'a, T: redis::FromRedisValue>(
///     con: &'a mut redis::Connection,
///     pattern: &'a str,
///     count: usize,
/// ) -> RedisResult<Iter<'a, T>> {
///     let opts = ScanOptions::default()
///         .with_pattern(pattern)
///         .with_count(count);
///     con.scan_options(opts)
/// }
/// ```
#[derive(Default)]
pub struct ScanOptions {
    pattern: Option<String>,
    count: Option<usize>,
    scan_type: Option<String>,
}

impl ScanOptions {
    /// Limit the results to the first N matching items.
    pub fn with_count(mut self, n: usize) -> Self {
        self.count = Some(n);
        self
    }

    /// Pattern for scan
    pub fn with_pattern(mut self, p: impl Into<String>) -> Self {
        self.pattern = Some(p.into());
        self
    }

    /// Limit the results to those with the given Redis type
    pub fn with_type(mut self, t: impl Into<String>) -> Self {
        self.scan_type = Some(t.into());
        self
    }
}

impl ToRedisArgs for ScanOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(p) = &self.pattern {
            out.write_arg(b"MATCH");
            out.write_arg_fmt(p);
        }

        if let Some(n) = self.count {
            out.write_arg(b"COUNT");
            out.write_arg_fmt(n);
        }

        if let Some(t) = &self.scan_type {
            out.write_arg(b"TYPE");
            out.write_arg_fmt(t);
        }
    }

    fn num_of_args(&self) -> usize {
        let mut len = 0;
        if self.pattern.is_some() {
            len += 2;
        }
        if self.count.is_some() {
            len += 2;
        }
        if self.scan_type.is_some() {
            len += 2;
        }
        len
    }
}

/// Options for the [LPOS](https://redis.io/commands/lpos) command
///
/// # Example
///
/// ```rust,no_run
/// use redis::{Commands, RedisResult, LposOptions};
/// fn fetch_list_position(
///     con: &mut redis::Connection,
///     key: &str,
///     value: &str,
///     count: usize,
///     rank: isize,
///     maxlen: usize,
/// ) -> RedisResult<Vec<usize>> {
///     let opts = LposOptions::default()
///         .count(count)
///         .rank(rank)
///         .maxlen(maxlen);
///     con.lpos(key, value, opts)
/// }
/// ```
#[derive(Default)]
pub struct LposOptions {
    count: Option<usize>,
    maxlen: Option<usize>,
    rank: Option<isize>,
}

impl LposOptions {
    /// Limit the results to the first N matching items.
    pub fn count(mut self, n: usize) -> Self {
        self.count = Some(n);
        self
    }

    /// Return the value of N from the matching items.
    pub fn rank(mut self, n: isize) -> Self {
        self.rank = Some(n);
        self
    }

    /// Limit the search to N items in the list.
    pub fn maxlen(mut self, n: usize) -> Self {
        self.maxlen = Some(n);
        self
    }
}

impl ToRedisArgs for LposOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(n) = self.count {
            out.write_arg(b"COUNT");
            out.write_arg_fmt(n);
        }

        if let Some(n) = self.rank {
            out.write_arg(b"RANK");
            out.write_arg_fmt(n);
        }

        if let Some(n) = self.maxlen {
            out.write_arg(b"MAXLEN");
            out.write_arg_fmt(n);
        }
    }

    fn num_of_args(&self) -> usize {
        let mut len = 0;
        if self.count.is_some() {
            len += 2;
        }
        if self.rank.is_some() {
            len += 2;
        }
        if self.maxlen.is_some() {
            len += 2;
        }
        len
    }
}

/// Enum for the LEFT | RIGHT args used by some commands
pub enum Direction {
    /// Targets the first element (head) of the list
    Left,
    /// Targets the last element (tail) of the list
    Right,
}

impl ToRedisArgs for Direction {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let s: &[u8] = match self {
            Direction::Left => b"LEFT",
            Direction::Right => b"RIGHT",
        };
        out.write_arg(s);
    }
}

/// Options for the [COPY](https://redis.io/commands/copy) command
///
/// # Example
/// ```rust,no_run
/// use redis::{Commands, RedisResult, CopyOptions, SetExpiry, ExistenceCheck};
/// fn copy_value(
///     con: &mut redis::Connection,
///     old: &str,
///     new: &str,
/// ) -> RedisResult<Vec<usize>> {
///     let opts = CopyOptions::default()
///         .db("my_other_db")
///         .replace(true);
///     con.copy(old, new, opts)
/// }
/// ```
#[derive(Clone, Copy, Debug)]
pub struct CopyOptions<Db: ToString> {
    db: Option<Db>,
    replace: bool,
}

impl Default for CopyOptions<&'static str> {
    fn default() -> Self {
        CopyOptions {
            db: None,
            replace: false,
        }
    }
}

impl<Db: ToString> CopyOptions<Db> {
    /// Set the target database for the copy operation
    pub fn db<Db2: ToString>(self, db: Db2) -> CopyOptions<Db2> {
        CopyOptions {
            db: Some(db),
            replace: self.replace,
        }
    }

    /// Set the replace option for the copy operation
    pub fn replace(mut self, replace: bool) -> Self {
        self.replace = replace;
        self
    }
}

impl<Db: ToString> ToRedisArgs for CopyOptions<Db> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(db) = &self.db {
            out.write_arg(b"DB");
            out.write_arg(db.to_string().as_bytes());
        }
        if self.replace {
            out.write_arg(b"REPLACE");
        }
    }
}

/// Options for the [SET](https://redis.io/commands/set) command
///
/// # Example
/// ```rust,no_run
/// use redis::{Commands, RedisResult, SetOptions, SetExpiry, ExistenceCheck};
/// fn set_key_value(
///     con: &mut redis::Connection,
///     key: &str,
///     value: &str,
/// ) -> RedisResult<Vec<usize>> {
///     let opts = SetOptions::default()
///         .conditional_set(ExistenceCheck::NX)
///         .get(true)
///         .with_expiration(SetExpiry::EX(60));
///     con.set_options(key, value, opts)
/// }
/// ```
#[derive(Clone, Copy, Default)]
pub struct SetOptions {
    conditional_set: Option<ExistenceCheck>,
    get: bool,
    expiration: Option<SetExpiry>,
}

impl SetOptions {
    /// Set the existence check for the SET command
    pub fn conditional_set(mut self, existence_check: ExistenceCheck) -> Self {
        self.conditional_set = Some(existence_check);
        self
    }

    /// Set the GET option for the SET command
    pub fn get(mut self, get: bool) -> Self {
        self.get = get;
        self
    }

    /// Set the expiration for the SET command
    pub fn with_expiration(mut self, expiration: SetExpiry) -> Self {
        self.expiration = Some(expiration);
        self
    }
}

impl ToRedisArgs for SetOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref conditional_set) = self.conditional_set {
            match conditional_set {
                ExistenceCheck::NX => {
                    out.write_arg(b"NX");
                }
                ExistenceCheck::XX => {
                    out.write_arg(b"XX");
                }
            }
        }
        if self.get {
            out.write_arg(b"GET");
        }
        if let Some(ref expiration) = self.expiration {
            match expiration {
                SetExpiry::EX(secs) => {
                    out.write_arg(b"EX");
                    out.write_arg(format!("{}", secs).as_bytes());
                }
                SetExpiry::PX(millis) => {
                    out.write_arg(b"PX");
                    out.write_arg(format!("{}", millis).as_bytes());
                }
                SetExpiry::EXAT(unix_time) => {
                    out.write_arg(b"EXAT");
                    out.write_arg(format!("{}", unix_time).as_bytes());
                }
                SetExpiry::PXAT(unix_time) => {
                    out.write_arg(b"PXAT");
                    out.write_arg(format!("{}", unix_time).as_bytes());
                }
                SetExpiry::KEEPTTL => {
                    out.write_arg(b"KEEPTTL");
                }
            }
        }
    }
}

/// Options for the [FLUSHALL](https://redis.io/commands/flushall) command
///
/// # Example
/// ```rust,no_run
/// use redis::{Commands, RedisResult, FlushAllOptions};
/// fn flushall_sync(
///     con: &mut redis::Connection,
/// ) -> RedisResult<()> {
///     let opts = FlushAllOptions{blocking: true};
///     con.flushall_options(&opts)
/// }
/// ```
#[derive(Clone, Copy, Default)]
pub struct FlushAllOptions {
    /// Blocking (`SYNC`) waits for completion, non-blocking (`ASYNC`) runs in the background
    pub blocking: bool,
}

impl FlushAllOptions {
    /// Set whether to run blocking (`SYNC`) or non-blocking (`ASYNC`) flush
    pub fn blocking(mut self, blocking: bool) -> Self {
        self.blocking = blocking;
        self
    }
}

impl ToRedisArgs for FlushAllOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if self.blocking {
            out.write_arg(b"SYNC");
        } else {
            out.write_arg(b"ASYNC");
        };
    }
}

/// Options for the [FLUSHDB](https://redis.io/commands/flushdb) command
pub type FlushDbOptions = FlushAllOptions;

/// Options for the HSETEX command
#[derive(Clone, Copy, Default)]
pub struct HashFieldExpirationOptions {
    existence_check: Option<FieldExistenceCheck>,
    expiration: Option<SetExpiry>,
}

impl HashFieldExpirationOptions {
    /// Set the field(s) existence check for the HSETEX command
    pub fn set_existence_check(mut self, field_existence_check: FieldExistenceCheck) -> Self {
        self.existence_check = Some(field_existence_check);
        self
    }

    /// Set the expiration option for the field(s) in the HSETEX command
    pub fn set_expiration(mut self, expiration: SetExpiry) -> Self {
        self.expiration = Some(expiration);
        self
    }
}

impl ToRedisArgs for HashFieldExpirationOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref existence_check) = self.existence_check {
            match existence_check {
                FieldExistenceCheck::FNX => out.write_arg(b"FNX"),
                FieldExistenceCheck::FXX => out.write_arg(b"FXX"),
            }
        }

        if let Some(ref expiration) = self.expiration {
            match expiration {
                SetExpiry::EX(secs) => {
                    out.write_arg(b"EX");
                    out.write_arg(format!("{}", secs).as_bytes());
                }
                SetExpiry::PX(millis) => {
                    out.write_arg(b"PX");
                    out.write_arg(format!("{}", millis).as_bytes());
                }
                SetExpiry::EXAT(unix_time) => {
                    out.write_arg(b"EXAT");
                    out.write_arg(format!("{}", unix_time).as_bytes());
                }
                SetExpiry::PXAT(unix_time) => {
                    out.write_arg(b"PXAT");
                    out.write_arg(format!("{}", unix_time).as_bytes());
                }
                SetExpiry::KEEPTTL => {
                    out.write_arg(b"KEEPTTL");
                }
            }
        }
    }
}

impl ToRedisArgs for Expiry {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Expiry::EX(sec) => {
                out.write_arg(b"EX");
                out.write_arg(sec.to_string().as_bytes());
            }
            Expiry::PX(ms) => {
                out.write_arg(b"PX");
                out.write_arg(ms.to_string().as_bytes());
            }
            Expiry::EXAT(timestamp_sec) => {
                out.write_arg(b"EXAT");
                out.write_arg(timestamp_sec.to_string().as_bytes());
            }
            Expiry::PXAT(timestamp_ms) => {
                out.write_arg(b"PXAT");
                out.write_arg(timestamp_ms.to_string().as_bytes());
            }
            Expiry::PERSIST => {
                out.write_arg(b"PERSIST");
            }
        }
    }
}

/// Helper enum that is used to define update checks
#[derive(Clone, Copy)]
pub enum UpdateCheck {
    /// LT -- Only update if the new score is less than the current.
    LT,
    /// GT -- Only update if the new score is greater than the current.
    GT,
}

/// Options for the [ZADD](https://redis.io/commands/zadd) command
#[derive(Clone, Copy, Default)]
pub struct SortedSetAddOptions {
    conditional_set: Option<ExistenceCheck>,
    conditional_update: Option<UpdateCheck>,
    include_changed: bool,
    increment: bool,
}

impl SortedSetAddOptions {
    /// Sets the NX option for the ZADD command
    /// Only add a member if it does not already exist.
    pub fn add_only() -> Self {
        Self {
            conditional_set: Some(ExistenceCheck::NX),
            ..Default::default()
        }
    }

    /// Sets the XX option and optionally the GT/LT option for the ZADD command
    /// Only update existing members
    pub fn update_only(conditional_update: Option<UpdateCheck>) -> Self {
        Self {
            conditional_set: Some(ExistenceCheck::XX),
            conditional_update,
            ..Default::default()
        }
    }

    /// Optionally sets the GT/LT option for the ZADD command
    /// Add new member or update existing
    pub fn add_or_update(conditional_update: Option<UpdateCheck>) -> Self {
        Self {
            conditional_update,
            ..Default::default()
        }
    }

    /// Sets the CH option for the ZADD command
    /// Return the number of elements changed (not just added).
    pub fn include_changed_count(mut self) -> Self {
        self.include_changed = true;
        self
    }

    /// Sets the INCR option for the ZADD command
    /// Increment the score of the member instead of setting it.
    pub fn increment_score(mut self) -> Self {
        self.increment = true;
        self
    }
}

impl ToRedisArgs for SortedSetAddOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref conditional_set) = self.conditional_set {
            match conditional_set {
                ExistenceCheck::NX => {
                    out.write_arg(b"NX");
                }
                ExistenceCheck::XX => {
                    out.write_arg(b"XX");
                }
            }
        }

        if let Some(ref conditional_update) = self.conditional_update {
            match conditional_update {
                UpdateCheck::LT => {
                    out.write_arg(b"LT");
                }
                UpdateCheck::GT => {
                    out.write_arg(b"GT");
                }
            }
        }
        if self.include_changed {
            out.write_arg(b"CH")
        }
        if self.increment {
            out.write_arg(b"INCR")
        }
    }
}

/// Creates HELLO command for RESP3 with RedisConnectionInfo
/// [Redis Docs](https://redis.io/commands/HELLO)
pub fn resp3_hello(connection_info: &RedisConnectionInfo) -> Cmd {
    let mut hello_cmd = cmd("HELLO");
    hello_cmd.arg("3");
    if let Some(password) = &connection_info.password {
        let username: &str = match connection_info.username.as_ref() {
            None => "default",
            Some(username) => username,
        };
        hello_cmd.arg("AUTH").arg(username).arg(password);
    }

    hello_cmd
}
