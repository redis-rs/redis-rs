//! Commands and types for working with the RedisJSON module.

use crate::types::{ExistenceCheck, RedisWrite, ToRedisArgs};

/// Storage-precision tag for the `FPHA` form of `JSON.SET`.
///
/// Applied via [`JsonSetOptions::fpha`] instructs the server to pack any floating-point arrays in the payload using the chosen lane precision.
/// Values that fall outside the chosen type's representable range cause the server to reject the command with `ERR value out of range for <TYPE>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
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
            Self::Bf16 => out.write_arg(b"BF16"),
            Self::Fp16 => out.write_arg(b"FP16"),
            Self::Fp32 => out.write_arg(b"FP32"),
            Self::Fp64 => out.write_arg(b"FP64"),
        }
    }
}

/// Options for the [`JSON.SET`](https://redis.io/commands/json.set) command.
///
/// Carries the optional `NX`/`XX` existence check and the optional `FPHA <TYPE>` storage hint.
///
/// # Example
/// ```rust,no_run
/// use redis::json::{FphaType, JsonSetOptions};
/// use redis::{ExistenceCheck, Commands};
/// use serde_json::json;
/// # fn do_something() -> redis::RedisResult<()> {
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let mut con = client.get_connection()?;
/// let opts = JsonSetOptions::default()
///     .conditional_set(ExistenceCheck::NX)
///     .fpha(FphaType::Fp32);
/// let _: () = con.json_set_options("my_key", "$", &[1.0_f32, 2.0], &opts)?;
/// # Ok(()) }
/// ```
#[derive(Clone, Default)]
pub struct JsonSetOptions {
    conditional_set: Option<ExistenceCheck>,
    fpha_type: Option<FphaType>,
}

impl JsonSetOptions {
    /// Apply an `NX` or `XX` existence check to the command.
    pub fn conditional_set(mut self, existence_check: ExistenceCheck) -> Self {
        self.conditional_set = Some(existence_check);
        self
    }

    /// Add an `FPHA <TYPE>` storage hint to the command.
    pub fn fpha(mut self, fpha_type: FphaType) -> Self {
        self.fpha_type = Some(fpha_type);
        self
    }
}

impl ToRedisArgs for JsonSetOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref conditional_set) = self.conditional_set {
            conditional_set.write_redis_args(out);
        }
        if let Some(ref ty) = self.fpha_type {
            out.write_arg(b"FPHA");
            ty.write_redis_args(out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cmd::{Arg, Cmd, cmd};
    use serde::ser::Serialize;

    fn simple_args(c: &Cmd) -> Vec<Vec<u8>> {
        c.args_iter()
            .map(|a| match a {
                Arg::Simple(b) => b.to_vec(),
                Arg::Cursor => b"<CURSOR>".to_vec(),
            })
            .collect()
    }

    fn build<V: Serialize + ?Sized>(value: &V, opts: &JsonSetOptions) -> Vec<Vec<u8>> {
        let mut c = cmd("JSON.SET");
        c.arg("k")
            .arg("$")
            .arg(serde_json::to_string(value).unwrap())
            .arg(opts);
        simple_args(&c)
    }

    #[test]
    fn json_value_with_default_options_writes_serialized_document_only() {
        assert_eq!(
            build(&serde_json::json!({"a": 1}), &JsonSetOptions::default()),
            vec![
                b"JSON.SET".to_vec(),
                b"k".to_vec(),
                b"$".to_vec(),
                br#"{"a":1}"#.to_vec(),
            ],
        );
    }

    #[test]
    fn json_set_options_builder_is_order_independent() {
        let a = JsonSetOptions::default()
            .conditional_set(ExistenceCheck::NX)
            .fpha(FphaType::Fp64);
        let b = JsonSetOptions::default()
            .fpha(FphaType::Fp64)
            .conditional_set(ExistenceCheck::NX);
        assert_eq!(build(&[1.0_f64], &a), build(&[1.0_f64], &b));
    }

    #[test]
    fn fpha_type_writes_expected_bytes() {
        for (ty, expected) in [
            (FphaType::Bf16, b"BF16".as_slice()),
            (FphaType::Fp16, b"FP16".as_slice()),
            (FphaType::Fp32, b"FP32".as_slice()),
            (FphaType::Fp64, b"FP64".as_slice()),
        ] {
            let args = build(&[0.0_f32], &JsonSetOptions::default().fpha(ty));
            assert_eq!(args.len(), 6);
            assert_eq!(args[4], b"FPHA");
            assert_eq!(args[5], expected);
        }
    }

    #[test]
    fn conditional_set_nx_appends_existence_check() {
        let args = build(
            &serde_json::json!(1),
            &JsonSetOptions::default().conditional_set(ExistenceCheck::NX),
        );
        assert_eq!(args.len(), 5);
        assert_eq!(args.last().unwrap(), b"NX");
    }

    #[test]
    fn fpha_with_existence_check_orders_value_then_existence_check_then_fpha_type() {
        let args = build(
            &[1.0_f32, -0.5, 1234.5],
            &JsonSetOptions::default()
                .conditional_set(ExistenceCheck::XX)
                .fpha(FphaType::Fp32),
        );
        // [JSON.SET, k, $, <json>, XX, FPHA, FP32]
        assert_eq!(args.len(), 7);
        assert_eq!(args[3], b"[1.0,-0.5,1234.5]");
        assert_eq!(args[4], b"XX");
        assert_eq!(args[5], b"FPHA");
        assert_eq!(args[6], b"FP32");
    }

    #[test]
    fn fpha_empty_payload_still_emits_fpha_type() {
        let args = build(&[0_f32; 0], &JsonSetOptions::default().fpha(FphaType::Fp32));
        assert_eq!(args.len(), 6);
        assert_eq!(args[3], b"[]");
        assert_eq!(args[4], b"FPHA");
        assert_eq!(args[5], b"FP32");
    }

    #[test]
    fn fpha_with_matrix_emits_nested_json_and_fpha_type() {
        let matrix: &[&[f32]] = &[&[1.0, 2.5], &[3.0, 4.0]];
        let args = build(matrix, &JsonSetOptions::default().fpha(FphaType::Bf16));
        assert_eq!(args.len(), 6);
        assert_eq!(args[3], b"[[1.0,2.5],[3.0,4.0]]");
        assert_eq!(args[4], b"FPHA");
        assert_eq!(args[5], b"BF16");
    }

    #[test]
    fn json_object_properly_serialized_with_value_and_fpha_type() {
        let value = serde_json::json!({"weights": [1.0, 2.0], "bias": [0.5]});
        let args = build(&value, &JsonSetOptions::default().fpha(FphaType::Fp16));
        assert_eq!(args[3], br#"{"bias":[0.5],"weights":[1.0,2.0]}"#);
        assert_eq!(args[4], b"FPHA");
        assert_eq!(args[5], b"FP16");
    }
}
