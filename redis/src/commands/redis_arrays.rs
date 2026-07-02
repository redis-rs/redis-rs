//! Defines types to use with the array commands (Redis 8.8+).

use crate::{RedisWrite, ToRedisArgs};
use std::num::NonZeroUsize;

/// A range bound for the `ARGREP` command.
///
/// Unlike the other array range commands (which take plain integer indices), `ARGREP` also accepts open-ended sentinels:
/// [`ArrayBound::Start`] (`-`) for the first index and [`ArrayBound::End`] (`+`) for the last.
/// Using `End` as the start and `Start` as the end iterates the range in reverse.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum ArrayBound {
    /// The first index of the array (`-`).
    Start,
    /// The last index of the array (`+`).
    End,
    /// A specific zero-based index.
    Index(usize),
}

impl From<usize> for ArrayBound {
    fn from(index: usize) -> Self {
        ArrayBound::Index(index)
    }
}

impl ToRedisArgs for ArrayBound {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            ArrayBound::Start => out.write_arg(b"-"),
            ArrayBound::End => out.write_arg(b"+"),
            ArrayBound::Index(index) => index.write_redis_args(out),
        }
    }
}

/// A textual predicate for the `ARGREP` command.
///
/// Any number of predicates may be supplied to a single `ARGREP` call since they are combined with a single logical combinator
/// (see [`ArrayGrepOptions::set_combinator`]), which defaults to `OR`.
///
/// # Composing boolean queries
///
/// The combinator is **flat**: one `AND` or `OR` applies to the entire predicate list.
/// Because grouping and operator precedence are not supported, expressions such as `(red OR blue) AND triangle` cannot be constructed using combinators alone.
/// The server does not support queries that mix `AND` and `OR`, so such expressions yield no matches.
/// There are two ways to express grouped logic:
///
/// 1. A single [`Re`](ArrayPredicate::Re) predicate, using alternation for the `OR` group.
///    For elements shaped like `"<color> <shape>"`:
///
///    ```text
///    (red OR blue) AND triangle   ->  Re("(red|blue) triangle")
///    red AND (circle OR triangle) ->  Re("red (circle|triangle)")
///    ```
///
/// 2. Two `ARGREP` calls combined client-side: e.g. union the indices of `[Match("red"), Match("blue")]` (with `OR`)
///    and intersect that set with the indices of `[Match("triangle")]`.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum ArrayPredicate<'a> {
    /// Matches elements equal to the given value (`EXACT`).
    Exact(&'a str),
    /// Matches elements that contain the given substring (`MATCH`).
    Match(&'a str),
    /// Matches elements against a glob-style pattern (`GLOB`).
    Glob(&'a str),
    /// Matches elements against a regular expression (`RE`).
    ///
    /// Inline flags such as `(?i)` are supported, but **backreferences and lookaround are not** - both raise an error.
    /// Character classes like `\w` and `\d` are **ASCII-only** and do not match non-ASCII letters or digits (e.g. Arabic-Indic digits), consistent with an RE2-style engine.
    Re(&'a str),
}

impl ToRedisArgs for ArrayPredicate<'_> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let (token, value): (&[u8], &str) = match self {
            ArrayPredicate::Exact(value) => (b"EXACT", value),
            ArrayPredicate::Match(value) => (b"MATCH", value),
            ArrayPredicate::Glob(pattern) => (b"GLOB", pattern),
            ArrayPredicate::Re(pattern) => (b"RE", pattern),
        };
        out.write_arg(token);
        out.write_arg(value.as_bytes());
    }
}

/// How multiple [`ArrayPredicate`]s are combined in an `ARGREP` query.
///
/// A single combinator applies to the whole predicate list since the server does not support mixed boolean expressions.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum ArrayGrepCombinator {
    /// An element matches only if it satisfies every predicate (`AND`).
    And,
    /// An element matches if it satisfies any predicate (`OR`).
    /// This is the server's default when no combinator is specified.
    Or,
}

/// Optional parameters for the `ARGREP` command.
///
/// # Example
/// ```rust,no_run
/// use redis::{Commands, RedisResult, redis_arrays::*};
/// fn grep(con: &mut redis::Connection, key: &str) -> RedisResult<Vec<(usize, String)>> {
///     let opts = ArrayGrepOptions::default()
///         .set_combinator(ArrayGrepCombinator::Or)
///         .set_limit(10)
///         .set_with_values(true)
///         .set_nocase(true);
///     con.argrep_options(
///         key,
///         ArrayBound::Start,
///         ArrayBound::End,
///         &[
///             ArrayPredicate::Glob("warn:*"),
///             ArrayPredicate::Glob("error:*"),
///             ArrayPredicate::Glob("fatal:*"),
///         ],
///         &opts,
///     )
/// }
/// ```
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ArrayGrepOptions {
    /// How to combine multiple predicates. `None` uses the server default (`OR`).
    combinator: Option<ArrayGrepCombinator>,
    /// Cap the number of matches returned.
    limit: Option<NonZeroUsize>,
    /// Return index-value pairs instead of bare indices.
    with_values: bool,
    /// Perform case-insensitive matching.
    nocase: bool,
}

impl ArrayGrepOptions {
    /// Set how multiple predicates are combined (`AND` / `OR`).
    pub fn set_combinator(mut self, combinator: ArrayGrepCombinator) -> Self {
        self.combinator = Some(combinator);
        self
    }

    /// Cap the number of matches returned.
    ///
    /// # Panics
    /// Panics if `limit` is zero - the server requires a positive `LIMIT`.
    pub fn set_limit(mut self, limit: usize) -> Self {
        self.limit = Some(NonZeroUsize::new(limit).unwrap());
        self
    }

    /// Return index-value pairs instead of bare indices (`WITHVALUES`).
    pub fn set_with_values(mut self, with_values: bool) -> Self {
        self.with_values = with_values;
        self
    }

    /// Perform case-insensitive matching (`NOCASE`).
    ///
    /// Applies to every predicate in the call, across all predicate types.
    /// Case folding is **ASCII-only**: non-ASCII characters such as `é`/`É` are matched literally and are not folded.
    pub fn set_nocase(mut self, nocase: bool) -> Self {
        self.nocase = nocase;
        self
    }
}

impl ToRedisArgs for ArrayGrepOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self.combinator {
            Some(ArrayGrepCombinator::And) => out.write_arg(b"AND"),
            Some(ArrayGrepCombinator::Or) => out.write_arg(b"OR"),
            None => {}
        }
        if let Some(limit) = self.limit {
            out.write_arg(b"LIMIT");
            limit.write_redis_args(out);
        }
        if self.with_values {
            out.write_arg(b"WITHVALUES");
        }
        if self.nocase {
            out.write_arg(b"NOCASE");
        }
    }
}

/// Optional parameters for the `ARSCAN` command.
///
/// # Example
/// ```rust,no_run
/// use redis::{Commands, RedisResult, redis_arrays::*};
/// fn scan_page(con: &mut redis::Connection, key: &str) -> RedisResult<Vec<(usize, String)>> {
///     let opts = ArrayScanOptions::default().set_limit(100);
///     con.arscan_options(key, 0, 1000, &opts)
/// }
/// ```
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ArrayScanOptions {
    /// Cap the number of pairs returned.
    limit: Option<NonZeroUsize>,
}

impl ArrayScanOptions {
    /// Return at most `limit` pairs.
    ///
    /// # Panics
    /// Panics if `limit` is zero - the server requires a positive `LIMIT`.
    pub fn set_limit(mut self, limit: usize) -> Self {
        self.limit = Some(NonZeroUsize::new(limit).unwrap());
        self
    }
}

impl ToRedisArgs for ArrayScanOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(limit) = self.limit {
            out.write_arg(b"LIMIT");
            limit.write_redis_args(out);
        }
    }
}

/// An aggregate operation for the `AROP` command.
///
/// The reply type depends on the operation, which is why `arop` returns a generic value the caller interprets:
/// - [`Sum`](ArrayAggregateOp::Sum) is an integer or float depending on the data.
/// - [`Min`](ArrayAggregateOp::Min) / [`Max`](ArrayAggregateOp::Max) return the element value or Nil when the range holds no numeric elements.
/// - [`And`](ArrayAggregateOp::And) / [`Or`](ArrayAggregateOp::Or) / [`Xor`](ArrayAggregateOp::Xor) are integer bitwise reductions.
/// - [`Match`](ArrayAggregateOp::Match) and [`Used`](ArrayAggregateOp::Used) return integer counts.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum ArrayAggregateOp<'a> {
    /// Sum of the numeric elements in the range (`SUM`).
    Sum,
    /// Minimum numeric element in the range (`MIN`).
    Min,
    /// Maximum numeric element in the range (`MAX`).
    Max,
    /// Bitwise AND of the elements in the range (`AND`).
    And,
    /// Bitwise OR of the elements in the range (`OR`).
    Or,
    /// Bitwise XOR of the elements in the range (`XOR`).
    Xor,
    /// Count of elements in the range exactly equal to the given value (`MATCH`).
    ///
    /// This comparison is **case-sensitive**; unlike [`ArrayPredicate::Match`] there is no case-folding option for `AROP`.
    Match(&'a str),
    /// Count of populated (non-empty) slots in the range (`USED`).
    Used,
}

impl ToRedisArgs for ArrayAggregateOp<'_> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            ArrayAggregateOp::Sum => out.write_arg(b"SUM"),
            ArrayAggregateOp::Min => out.write_arg(b"MIN"),
            ArrayAggregateOp::Max => out.write_arg(b"MAX"),
            ArrayAggregateOp::And => out.write_arg(b"AND"),
            ArrayAggregateOp::Or => out.write_arg(b"OR"),
            ArrayAggregateOp::Xor => out.write_arg(b"XOR"),
            ArrayAggregateOp::Match(value) => {
                out.write_arg(b"MATCH");
                out.write_arg(value.as_bytes());
            }
            ArrayAggregateOp::Used => out.write_arg(b"USED"),
        }
    }
}
