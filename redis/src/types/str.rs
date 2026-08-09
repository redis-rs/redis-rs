//! [`Str`]: the cheaply-cloneable UTF-8 string used by [`Value`](crate::Value).

use bytes::Bytes;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::convert::Infallible;
use std::fmt;
use std::ops::Deref;
use std::str::{FromStr, from_utf8};

/// A cheaply-cloneable, immutable UTF-8 string backed by [`bytes::Bytes`].
///
/// `Str` is used by [`Value`](crate::Value) for textual responses (simple strings, verbatim
/// strings, push kinds, …). It holds a `Bytes` buffer that is guaranteed to be
/// valid UTF-8 by construction, so dereferencing to `&str` is zero-cost.
///
/// Because the backing storage is `Bytes`, cloning a `Str` is a cheap
/// reference-count bump rather than an allocation, and the parser can produce
/// one as a zero-copy slice into the response buffer.
///
/// # Immutability
///
/// `Str` is deliberately immutable. The backing `Bytes` may be shared with other
/// `Str`s and with the response buffer it was sliced from, so there is no
/// `DerefMut` and no in-place mutation API (no `push_str`, no `Add`, no
/// `Extend`, no [`fmt::Write`]). To build or edit text, go through [`String`]:
/// `let mut s = String::from(str_value);` … `Str::from(s)`.
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
        Ok(Self(bytes))
    }

    /// Creates a `Str` from a static string slice without copying.
    pub const fn from_static(s: &'static str) -> Self {
        Self(Bytes::from_static(s.as_bytes()))
    }

    /// Wraps a `Bytes` buffer as a `Str` without checking that it is UTF-8.
    ///
    /// # Safety
    /// The caller must ensure that `bytes` contains valid UTF-8.
    pub(crate) unsafe fn from_utf8_unchecked(bytes: Bytes) -> Self {
        Self(bytes)
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
        Self(Bytes::copy_from_slice(s.as_bytes()))
    }
}

impl From<String> for Str {
    fn from(s: String) -> Self {
        Self(Bytes::from(s.into_bytes()))
    }
}

impl From<&String> for Str {
    fn from(s: &String) -> Self {
        Self::from(s.as_str())
    }
}

impl From<Cow<'_, str>> for Str {
    fn from(s: Cow<'_, str>) -> Self {
        match s {
            // The owned half moves its allocation into the `Bytes`; only the
            // borrowed half has to copy.
            Cow::Owned(s) => Self::from(s),
            Cow::Borrowed(s) => Self::from(s),
        }
    }
}

impl From<char> for Str {
    fn from(c: char) -> Self {
        let mut buf = [0u8; 4];
        Self(Bytes::copy_from_slice(c.encode_utf8(&mut buf).as_bytes()))
    }
}

impl FromStr for Str {
    /// Parsing a `Str` from a `&str` cannot fail, exactly as for [`String`].
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(s))
    }
}

impl From<Str> for String {
    fn from(s: Str) -> Self {
        // SAFETY: the `Bytes` are guaranteed to be valid UTF-8 by construction.
        unsafe { Self::from_utf8_unchecked(s.0.into()) }
    }
}

impl From<Str> for Bytes {
    fn from(s: Str) -> Self {
        s.0
    }
}

impl From<Str> for Vec<u8> {
    fn from(s: Str) -> Self {
        // `Bytes` reuses the allocation when it is uniquely owned.
        s.0.into()
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

impl PartialEq<Cow<'_, str>> for Str {
    fn eq(&self, other: &Cow<'_, str>) -> bool {
        self.as_str() == &**other
    }
}

impl PartialEq<Str> for Cow<'_, str> {
    fn eq(&self, other: &Str) -> bool {
        &**self == other.as_str()
    }
}

// The `PartialOrd` cross-impls mirror the `PartialEq` ones above, so that
// `Str` orders against borrowed and owned strings as well as it compares.
// `bytes::Bytes`, which `Str` wraps, provides the same matched set.

impl PartialOrd<str> for Str {
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        Some(self.as_str().cmp(other))
    }
}

impl PartialOrd<&str> for Str {
    fn partial_cmp(&self, other: &&str) -> Option<Ordering> {
        Some(self.as_str().cmp(*other))
    }
}

impl PartialOrd<String> for Str {
    fn partial_cmp(&self, other: &String) -> Option<Ordering> {
        Some(self.as_str().cmp(other.as_str()))
    }
}

impl PartialOrd<Cow<'_, str>> for Str {
    fn partial_cmp(&self, other: &Cow<'_, str>) -> Option<Ordering> {
        Some(self.as_str().cmp(&**other))
    }
}

impl PartialOrd<Str> for str {
    fn partial_cmp(&self, other: &Str) -> Option<Ordering> {
        Some(self.cmp(other.as_str()))
    }
}

impl PartialOrd<Str> for &str {
    fn partial_cmp(&self, other: &Str) -> Option<Ordering> {
        Some((*self).cmp(other.as_str()))
    }
}

impl PartialOrd<Str> for String {
    fn partial_cmp(&self, other: &Str) -> Option<Ordering> {
        Some(self.as_str().cmp(other.as_str()))
    }
}

impl PartialOrd<Str> for Cow<'_, str> {
    fn partial_cmp(&self, other: &Str) -> Option<Ordering> {
        Some((**self).cmp(other.as_str()))
    }
}

#[cfg(test)]
mod tests {
    use super::Str;
    use std::borrow::{Borrow, Cow};
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
    fn btreemap_lookup_by_str() {
        use std::ops::Bound;

        // The `Ord`/`Borrow<str>` twin of `hashmap_lookup_by_str_borrow`: a
        // `BTreeMap<Str, _>` lookup by `&str` needs `Str`'s ordering to agree
        // with `str`'s, or the binary search walks the wrong way.
        let mut map: BTreeMap<Str, i32> = BTreeMap::new();
        for (i, key) in ["", "hello", "héllo", "🌍", "with\0nul"]
            .into_iter()
            .enumerate()
        {
            map.insert(Str::from(key), i as i32);
        }
        assert_eq!(map.get(""), Some(&0));
        assert_eq!(map.get("hello"), Some(&1));
        assert_eq!(map.get("héllo"), Some(&2));
        assert_eq!(map.get("🌍"), Some(&3));
        assert_eq!(map.get("with\0nul"), Some(&4));
        assert_eq!(map.get("missing"), None);
        // Range queries by `&str` work for the same reason.
        assert_eq!(
            map.range::<str, _>((Bound::Included("h"), Bound::Excluded("i")))
                .map(|(k, _)| k.as_str())
                .collect::<Vec<_>>(),
            ["hello", "héllo"]
        );
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
        Str::from_redis_value(Value::Nil).unwrap_err();
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
        Str::from_utf8(Bytes::from_static(&[0xC0, 0x80])).unwrap_err();
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
        assert_eq!(s.into_bytes(), Bytes::from_static(b"verbatim"));
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

    #[test]
    fn sliced_str_converts_and_formats() {
        use bytes::Bytes;

        // The shape the parser actually produces: a `Str` over a *slice* of a
        // larger, still-shared response buffer. The other conversion tests only
        // cover `from_static`/`From<&str>`-backed values, whose `Bytes` span the
        // whole allocation.
        let backing = Bytes::from(b"##hello##".to_vec());
        let s = Str::from_utf8(backing.slice(2..7)).unwrap();

        // Zero-copy: the slice points two bytes into the original allocation.
        assert_eq!(
            s.as_bytes().as_ptr() as usize - backing.as_ptr() as usize,
            2
        );
        assert_eq!(s.len(), 5);

        // Every conversion must see only the slice, never its neighbours.
        assert_eq!(s.as_str(), "hello");
        assert_eq!(s.as_bytes(), b"hello");
        assert_eq!(String::from(s.clone()), "hello");
        assert_eq!(Vec::<u8>::from(s.clone()), b"hello".to_vec());
        assert_eq!(Bytes::from(s.clone()), Bytes::from_static(b"hello"));
        assert_eq!(s.to_string(), "hello");
        assert_eq!(format!("{s:?}"), "\"hello\"");
        assert_eq!(s, Str::from("hello"));
        // `backing` is still alive here, so the `Bytes` above was shared and the
        // owning conversions had to copy out of it rather than take it over.
        assert_eq!(backing, Bytes::from_static(b"##hello##"));

        // Same again with a multi-byte payload, sliced on char boundaries.
        let backing = Bytes::from("##héllo → 🌍##".as_bytes().to_vec());
        let end = backing.len() - 2;
        let s = Str::from_utf8(backing.slice(2..end)).unwrap();
        assert_eq!(s.as_str(), "héllo → 🌍");
        assert_eq!(String::from(s.clone()), "héllo → 🌍");
        assert_eq!(Vec::<u8>::from(s.clone()), "héllo → 🌍".as_bytes().to_vec());
        assert_eq!(s.to_string(), "héllo → 🌍");
        assert_eq!(format!("{s:?}"), "\"héllo → 🌍\"");

        // A slice that splits a multi-byte sequence is rejected, not accepted
        // and later transmuted by `as_str`.
        Str::from_utf8(backing.slice(2..4)).unwrap_err();
    }

    #[test]
    fn into_vec_u8() {
        // `let v: Vec<u8> = s.into()` used to work when this was a `String`.
        let v: Vec<u8> = Str::from("héllo").into();
        assert_eq!(v, "héllo".as_bytes().to_vec());
        assert_eq!(Vec::<u8>::from(Str::default()), Vec::<u8>::new());
        // Matches `into_bytes()`, which is the `Bytes`-returning sibling.
        assert_eq!(
            Vec::<u8>::from(Str::from("x")),
            Str::from("x").into_bytes().to_vec()
        );
    }

    #[test]
    fn from_char_cow_and_from_str() {
        use std::str::FromStr;

        // `From<char>`, including a multi-byte one.
        assert_eq!(Str::from('x'), Str::from("x"));
        assert_eq!(Str::from('🌍'), Str::from("🌍"));
        assert_eq!(Str::from('é').as_bytes(), "é".as_bytes());

        // `From<Cow<str>>`, both halves.
        assert_eq!(Str::from(Cow::Borrowed("borrowed")), Str::from("borrowed"));
        assert_eq!(
            Str::from(Cow::<str>::Owned(String::from("owned"))),
            Str::from("owned")
        );
        // The owned half must not copy: it moves the `String`'s allocation.
        let owned = String::from("a reasonably long owned payload, not inlined");
        let ptr = owned.as_ptr();
        assert_eq!(Str::from(Cow::<str>::Owned(owned)).as_bytes().as_ptr(), ptr);

        // `FromStr`, so `"…".parse()` works as it does for `String`.
        assert_eq!(Str::from_str("parsed").unwrap(), Str::from("parsed"));
        assert_eq!("parsed".parse::<Str>().unwrap(), Str::from("parsed"));
        assert_eq!("".parse::<Str>().unwrap(), Str::default());
        // The error type is uninhabited, so parsing can never fail.
        let never: Result<Str, std::convert::Infallible> = "x".parse();
        never.unwrap();
    }

    #[test]
    fn partial_eq_cow_cross_impls() {
        let s = Str::from("abc");
        // Bound to locals so `clippy::cmp_owned` does not fire.
        let same: Cow<'_, str> = Cow::Borrowed("abc");
        let same_owned: Cow<'_, str> = Cow::Owned(String::from("abc"));
        let different: Cow<'_, str> = Cow::Borrowed("abd");

        assert!(s == same);
        assert!(s == same_owned);
        assert!(same == s);
        assert!(same_owned == s);
        assert!(s != different);
        assert!(different != s);
    }

    #[test]
    fn partial_ord_cross_impls() {
        let s = Str::from("abc");
        // Bound to locals so `clippy::cmp_owned` does not fire; the point is to
        // exercise the owned impls, not to compare against a temporary.
        let lesser_string = String::from("abb");
        let equal_string = String::from("abc");
        let greater_string = String::from("abd");
        let equal_cow: Cow<'_, str> = Cow::Borrowed("abc");
        let greater_cow: Cow<'_, str> = Cow::Owned(String::from("abd"));

        // Each impl is also called fully qualified, so that method resolution
        // cannot silently route the operator forms below to a single impl and
        // leave the others untested.
        // `Str` on the left: PartialOrd<str>, <&str>, <String>, <Cow<str>>.
        assert_eq!(
            <Str as PartialOrd<str>>::partial_cmp(&s, "abb"),
            Some(Ordering::Greater)
        );
        assert_eq!(
            <Str as PartialOrd<str>>::partial_cmp(&s, "abc"),
            Some(Ordering::Equal)
        );
        assert_eq!(
            <Str as PartialOrd<&str>>::partial_cmp(&s, &"abd"),
            Some(Ordering::Less)
        );
        assert_eq!(
            <Str as PartialOrd<String>>::partial_cmp(&s, &lesser_string),
            Some(Ordering::Greater)
        );
        assert_eq!(
            <Str as PartialOrd<Cow<'_, str>>>::partial_cmp(&s, &greater_cow),
            Some(Ordering::Less)
        );

        // `Str` on the right: the reflected impls.
        assert_eq!(
            <str as PartialOrd<Str>>::partial_cmp("abb", &s),
            Some(Ordering::Less)
        );
        assert_eq!(
            <&str as PartialOrd<Str>>::partial_cmp(&"abd", &s),
            Some(Ordering::Greater)
        );
        assert_eq!(
            <String as PartialOrd<Str>>::partial_cmp(&equal_string, &s),
            Some(Ordering::Equal)
        );
        assert_eq!(
            <Cow<'_, str> as PartialOrd<Str>>::partial_cmp(&equal_cow, &s),
            Some(Ordering::Equal)
        );

        // The operator forms, which are what callers actually write. Kept as
        // one assertion each so that `clippy::double_comparisons` and
        // `clippy::manual_range_contains` do not fire on `>=` / `<=` pairs.
        assert!(s > *"abb");
        assert!(s < *"abd");
        assert!(s > "abb");
        assert!(s < "abd");
        assert!(s >= "abc");
        assert!(s <= "abc");
        assert!(s > lesser_string);
        assert!(s < greater_string);
        assert!(s >= equal_string);
        assert!(s <= equal_string);
        assert!(s == equal_cow);
        assert!(s < greater_cow);
        assert!(*"abb" < s);
        assert!(*"abd" > s);
        assert!("abb" < s);
        assert!("abd" > s);
        assert!("abc" <= s);
        assert!("abc" >= s);
        assert!(lesser_string < s);
        assert!(greater_string > s);
        assert!(equal_string <= s);
        assert!(equal_string >= s);
        assert!(equal_cow <= s);
        assert!(greater_cow > s);

        // Multi-byte comparisons keep `str`'s (code-point) order, and the
        // cross-impls agree with `Str`'s own `Ord`.
        for (a, b) in [("", "a"), ("a", "a"), ("b", "a"), ("é", "🌍"), ("e", "é")] {
            let (lhs, rhs) = (Str::from(a), Str::from(b));
            let expected = lhs.cmp(&rhs);
            assert_eq!(
                <Str as PartialOrd<str>>::partial_cmp(&lhs, b),
                Some(expected)
            );
            assert_eq!(
                <str as PartialOrd<Str>>::partial_cmp(b, &lhs),
                Some(expected.reverse())
            );
        }
    }
}
