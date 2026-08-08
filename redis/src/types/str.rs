//! [`Str`]: the cheaply-cloneable UTF-8 string used by [`Value`](crate::Value).

use bytes::Bytes;
use std::fmt;
use std::ops::Deref;
use std::str::from_utf8;

/// A cheaply-cloneable, UTF-8 string backed by [`bytes::Bytes`].
///
/// `Str` is used by [`Value`](crate::Value) for textual responses (simple strings, verbatim
/// strings, push kinds, …). It holds a `Bytes` buffer that is guaranteed to be
/// valid UTF-8 by construction, so dereferencing to `&str` is zero-cost.
///
/// Because the backing storage is `Bytes`, cloning a `Str` is a cheap
/// reference-count bump rather than an allocation, and the parser can produce
/// one as a zero-copy slice into the response buffer.
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Str(Bytes);

impl std::hash::Hash for Str {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash as a `str`, not as the raw `Bytes`. `<str as Hash>` and
        // `<[u8] as Hash>` produce different values (str writes a trailing
        // `0xff`, the slice writes a length prefix), so deriving `Hash` from
        // `Bytes` while also implementing `Borrow<str>` would break the
        // `Borrow`/`Hash` contract and make `HashMap<Str, _>::get(&str)` miss
        // present keys.
        self.as_str().hash(state);
    }
}

impl Str {
    /// Wraps a `Bytes` buffer as a `Str`, validating that it is UTF-8.
    pub fn from_utf8(bytes: Bytes) -> Result<Self, std::str::Utf8Error> {
        from_utf8(&bytes)?;
        Ok(Str(bytes))
    }

    /// Creates a `Str` from a static string slice without copying.
    pub const fn from_static(s: &'static str) -> Self {
        Str(Bytes::from_static(s.as_bytes()))
    }

    /// Wraps a `Bytes` buffer as a `Str` without checking that it is UTF-8.
    ///
    /// # Safety
    /// The caller must ensure that `bytes` contains valid UTF-8.
    pub(crate) unsafe fn from_utf8_unchecked(bytes: Bytes) -> Self {
        Str(bytes)
    }

    /// Returns the string contents as a `&str`.
    #[inline]
    pub fn as_str(&self) -> &str {
        // SAFETY: the `Bytes` are guaranteed to be valid UTF-8 by construction.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }

    /// Returns the underlying bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the `Str`, returning the underlying `Bytes`.
    #[inline]
    pub fn into_bytes(self) -> Bytes {
        self.0
    }
}

impl Deref for Str {
    type Target = str;
    #[inline]
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for Str {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for Str {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl std::borrow::Borrow<str> for Str {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for Str {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for Str {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl From<&str> for Str {
    fn from(s: &str) -> Self {
        Str(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<String> for Str {
    fn from(s: String) -> Self {
        Str(Bytes::from(s.into_bytes()))
    }
}

impl From<&String> for Str {
    fn from(s: &String) -> Self {
        Str::from(s.as_str())
    }
}

impl From<Str> for String {
    fn from(s: Str) -> Self {
        // SAFETY: the `Bytes` are guaranteed to be valid UTF-8 by construction.
        unsafe { String::from_utf8_unchecked(s.0.into()) }
    }
}

impl From<Str> for Bytes {
    fn from(s: Str) -> Self {
        s.0
    }
}

impl PartialEq<str> for Str {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for Str {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for Str {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<Str> for str {
    fn eq(&self, other: &Str) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<Str> for &str {
    fn eq(&self, other: &Str) -> bool {
        *self == other.as_str()
    }
}

impl PartialEq<Str> for String {
    fn eq(&self, other: &Str) -> bool {
        self.as_str() == other.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::Str;
    use std::borrow::Borrow;
    use std::cmp::Ordering;
    use std::collections::{BTreeMap, HashMap};

    #[test]
    fn hashmap_lookup_by_str_borrow() {
        // `Str: Borrow<str>` requires `str`-consistent hashing; otherwise a
        // `HashMap<Str, _>` lookup by `&str` would miss the present key.
        let mut map: HashMap<Str, i32> = HashMap::new();
        map.insert(Str::from("hello"), 1);
        assert_eq!(map.get("hello"), Some(&1));
    }

    #[test]
    fn str_is_ordered_like_str() {
        let (apple, banana) = (Str::from("apple"), Str::from("banana"));
        assert!(apple < banana);
        // Usable as a BTreeMap key, matching what `String` previously allowed.
        let mut m: BTreeMap<Str, i32> = BTreeMap::new();
        m.insert(Str::from("b"), 2);
        m.insert(Str::from("a"), 1);
        assert_eq!(m.keys().map(|s| s.as_str()).collect::<Vec<_>>(), ["a", "b"]);
    }

    #[test]
    fn str_from_redis_value_is_zero_copy() {
        use crate::types::{FromRedisValue, Value};
        use bytes::Bytes;

        // Owned BulkString -> Str reuses the same allocation (no copy).
        let bytes = Bytes::from_static(b"payload");
        let ptr = bytes.as_ptr();
        let s = Str::from_redis_value(Value::BulkString(bytes)).unwrap();
        assert_eq!(s.as_str(), "payload");
        assert_eq!(s.as_bytes().as_ptr(), ptr, "BulkString -> Str copied");

        // SimpleString is already a Str: moved out unchanged.
        assert_eq!(
            Str::from_redis_value(Value::SimpleString("OK".into())).unwrap(),
            Str::from("OK")
        );
        // Numeric and Okay conversions still work.
        assert_eq!(
            Str::from_redis_value(Value::Int(7)).unwrap(),
            Str::from("7")
        );
        assert_eq!(Str::from_redis_value(Value::Okay).unwrap(), Str::from("OK"));
        // Non-string types are rejected.
        assert!(Str::from_redis_value(Value::Nil).is_err());
    }

    #[test]
    fn from_utf8_validates() {
        use bytes::Bytes;

        // Valid, including multi-byte sequences.
        assert_eq!(
            Str::from_utf8(Bytes::from_static("héllo → 🌍".as_bytes()))
                .unwrap()
                .as_str(),
            "héllo → 🌍"
        );
        // Empty input is valid.
        assert_eq!(Str::from_utf8(Bytes::new()).unwrap().as_str(), "");

        // A lone continuation byte is rejected.
        let err = Str::from_utf8(Bytes::from_static(b"a\xff")).unwrap_err();
        assert_eq!(err.valid_up_to(), 1);
        // A truncated multi-byte sequence is rejected (boundary case: the first
        // two bytes of a three-byte sequence).
        let err = Str::from_utf8(Bytes::from_static(&[0xE2, 0x82])).unwrap_err();
        assert_eq!(err.valid_up_to(), 0);
        // An over-long / invalid lead byte is rejected.
        assert!(Str::from_utf8(Bytes::from_static(&[0xC0, 0x80])).is_err());
    }

    #[test]
    fn from_utf8_unchecked_round_trips_without_copying() {
        use bytes::Bytes;

        let bytes = Bytes::from_static("verbatim".as_bytes());
        let ptr = bytes.as_ptr();
        // SAFETY: the literal above is valid UTF-8.
        let s = unsafe { Str::from_utf8_unchecked(bytes) };
        assert_eq!(s.as_str(), "verbatim");
        assert_eq!(s.as_bytes(), b"verbatim");
        assert_eq!(s.as_bytes().as_ptr(), ptr, "from_utf8_unchecked copied");
        assert_eq!(s.clone().into_bytes(), Bytes::from_static(b"verbatim"));
    }

    #[test]
    fn from_static_is_zero_copy() {
        const LIT: &str = "static payload";
        let s = Str::from_static(LIT);
        assert_eq!(s.as_str(), LIT);
        assert_eq!(s.as_bytes().as_ptr(), LIT.as_ptr());
    }

    #[test]
    fn clone_does_not_reallocate() {
        let a = Str::from("some reasonably long payload to defeat any inlining");
        let b = a.clone();
        assert_eq!(a, b);
        assert_eq!(
            a.as_bytes().as_ptr(),
            b.as_bytes().as_ptr(),
            "cloning a Str reallocated; it must be a refcount bump"
        );
    }

    #[test]
    fn hash_matches_str_hash() {
        use std::hash::{BuildHasher, RandomState};

        // The `Borrow<str>`/`Hash` contract: `Str` and `&str` must hash alike.
        // Deriving `Hash` (i.e. hashing the `Bytes`) would break this.
        let state = RandomState::new();
        for case in ["", "a", "héllo → 🌍", "with\0nul"] {
            assert_eq!(
                state.hash_one(Str::from(case)),
                state.hash_one(case),
                "Str and str hash differently for {case:?}"
            );
        }
    }

    #[test]
    fn hashmap_lookup_by_str_for_multibyte_and_empty_keys() {
        use std::collections::HashMap;

        let mut map: HashMap<Str, i32> = HashMap::new();
        map.insert(Str::from(""), 0);
        map.insert(Str::from("héllo"), 1);
        map.insert(Str::from("🌍"), 2);
        assert_eq!(map.get(""), Some(&0));
        assert_eq!(map.get("héllo"), Some(&1));
        assert_eq!(map.get("🌍"), Some(&2));
        assert_eq!(map.get("missing"), None);
    }

    #[test]
    fn ordering_matches_str_ordering() {
        // `Ord` is derived from `Bytes` (bytewise); for UTF-8 that is the same
        // order as `str`, including across multi-byte sequences.
        let mut inputs = ["", "a", "ab", "b", "Z", "é", "🌍"];
        let mut as_strs = inputs;
        inputs.sort_by_key(|s| Str::from(*s));
        as_strs.sort();
        assert_eq!(inputs, as_strs);

        assert_eq!(Str::from("a").cmp(&Str::from("b")), Ordering::Less);
        assert_eq!(Str::from("b").cmp(&Str::from("a")), Ordering::Greater);
        assert_eq!(Str::from("a").cmp(&Str::from("a")), Ordering::Equal);
    }

    #[test]
    fn partial_eq_cross_impls() {
        let s = Str::from("abc");
        // Bound to locals so `clippy::cmp_owned` does not fire; the point is to
        // exercise the `String` impls, not to compare against a temporary.
        let same = String::from("abc");
        let different = String::from("abd");

        // `Str` on the left: PartialEq<str>, PartialEq<&str>, PartialEq<String>.
        assert!(s == *"abc");
        assert!(s == "abc");
        assert!(s == same);
        // `Str` on the right: the reflected impls.
        assert!(*"abc" == s);
        assert!("abc" == s);
        assert!(same == s);
        // …and the negative side of each, so a wrong impl cannot pass by
        // always returning `true`.
        assert!(s != *"abd");
        assert!(s != "abd");
        assert!(s != different);
        assert!(*"abd" != s);
        assert!("abd" != s);
        assert!(different != s);
        // Str vs Str.
        assert_eq!(s, Str::from("abc"));
        assert_ne!(s, Str::from("abd"));
    }

    #[test]
    fn deref_borrow_and_as_ref() {
        let s = Str::from("héllo");
        // Deref to str gives the inherent `str` methods for free.
        assert_eq!(s.len(), 6);
        assert!(s.starts_with('h'));
        assert_eq!(s.to_uppercase(), "HÉLLO");
        // Both `AsRef` impls.
        assert_eq!(<Str as AsRef<str>>::as_ref(&s), "héllo");
        assert_eq!(<Str as AsRef<[u8]>>::as_ref(&s), "héllo".as_bytes());
        // Borrow<str>.
        assert_eq!(<Str as Borrow<str>>::borrow(&s), "héllo");
    }

    #[test]
    fn display_and_debug() {
        let s = Str::from("a\"b\nc");
        // Display is the raw contents…
        assert_eq!(s.to_string(), "a\"b\nc");
        // …Debug is quoted and escaped, exactly like `str`'s.
        assert_eq!(format!("{s:?}"), "\"a\\\"b\\nc\"");
        assert_eq!(format!("{s:?}"), format!("{:?}", "a\"b\nc"));

        // Empty and multi-byte.
        assert_eq!(Str::default().to_string(), "");
        assert_eq!(format!("{:?}", Str::default()), "\"\"");
        assert_eq!(Str::from("🌍").to_string(), "🌍");
        assert_eq!(format!("{:?}", Str::from("🌍")), "\"🌍\"");
    }

    #[test]
    fn conversions() {
        use bytes::Bytes;

        // Into `Str`.
        assert_eq!(Str::from("x").as_str(), "x");
        assert_eq!(Str::from(String::from("x")).as_str(), "x");
        assert_eq!(Str::from(&String::from("x")).as_str(), "x");
        // Out of `Str`.
        assert_eq!(String::from(Str::from("x")), "x");
        assert_eq!(Bytes::from(Str::from("x")), Bytes::from_static(b"x"));

        // `String::from` must not lose multi-byte content.
        assert_eq!(String::from(Str::from("héllo → 🌍")), "héllo → 🌍");
        // Default is empty.
        assert_eq!(Str::default(), Str::from(""));
        assert!(Str::default().as_bytes().is_empty());
    }
}
