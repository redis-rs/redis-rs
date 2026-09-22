//! Implementations related to lists, maps, and collections in general

use super::{
    from_redis_value, from_redis_value_ref, get_inner_value, get_owned_inner_value, vec_to_array,
};
use crate::{
    FromRedisValue, NumericBehavior, ParsingError, RedisWrite, ToRedisArgs, ToSingleRedisArg, Value,
};
use std::hash::{BuildHasher, Hash};
use std::{fmt, io};

/// @note: Redis cannot store empty sets so the application has to
/// check whether the set is empty and if so, not attempt to use that
/// result
macro_rules! impl_to_redis_args_for_set {
    (for <$($TypeParam:ident),+> $SetType:ty, where ($($WhereClause:tt)+) ) => {
        impl< $($TypeParam),+ > ToRedisArgs for $SetType
        where
            $($WhereClause)+
        {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                ToRedisArgs::make_arg_iter_ref(self.iter(), out)
            }

            #[inline]
            fn num_of_args(&self) -> usize {
                self.num_of_args_and_size().0
            }

            #[inline]
            fn args_size(&self) -> usize {
                self.num_of_args_and_size().1
            }

            #[inline]
            fn num_of_args_and_size(&self) -> (usize, usize) {
                self.iter()
                    .map(|item| item.num_of_args_and_size())
                    .fold((0, 0), |(args, size), (item_args, item_size)| {
                        (args + item_args, size + item_size)
                    })
            }
        }
    };
}

impl_to_redis_args_for_set!(
    for <T, S> std::collections::HashSet<T, S>,
    where (T: ToRedisArgs)
);

impl_to_redis_args_for_set!(
    for <T> std::collections::BTreeSet<T>,
    where (T: ToRedisArgs)
);

#[cfg(feature = "hashbrown")]
impl_to_redis_args_for_set!(
    for <T, S> hashbrown::HashSet<T, S>,
    where (T: ToRedisArgs)
);

#[cfg(feature = "ahash")]
impl_to_redis_args_for_set!(
    for <T, S> ahash::AHashSet<T, S>,
    where (T: ToRedisArgs)
);

/// @note: Redis cannot store empty maps so the application has to
/// check whether the set is empty and if so, not attempt to use that
/// result
macro_rules! impl_to_redis_args_for_map {
    (
        $(#[$meta:meta])*
        for <$($TypeParam:ident),+> $MapType:ty,
        where ($($WhereClause:tt)+)
    ) => {
        $(#[$meta])*
        impl< $($TypeParam),+ > ToRedisArgs for $MapType
        where
            $($WhereClause)+
        {
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                for (key, value) in self {
                    // Ensure key and value produce a single argument each
                    assert!(key.num_of_args() <= 1 && value.num_of_args() <= 1);
                    key.write_redis_args(out);
                    value.write_redis_args(out);
                }
            }

            #[inline]
            fn num_of_args(&self) -> usize {
                self.num_of_args_and_size().0
            }

            #[inline]
            fn args_size(&self) -> usize {
                self.num_of_args_and_size().1
            }

            #[inline]
            fn num_of_args_and_size(&self) -> (usize, usize) {
                self.iter()
                    .map(|(key, value)| {
                        let (key_args, key_size) = key.num_of_args_and_size();
                        let (value_args, value_size) = value.num_of_args_and_size();
                        (key_args + value_args, key_size + value_size)
                    })
                    .fold((0, 0), |(args, size), (item_args, item_size)| {
                        (args + item_args, size + item_size)
                    })
            }
        }
    };
}

impl_to_redis_args_for_map!(
    for <K, V, S> std::collections::HashMap<K, V, S>,
    where (K: ToRedisArgs, V: ToRedisArgs)
);

impl_to_redis_args_for_map!(
    /// this flattens BTreeMap into something that goes well with HMSET
    for <K, V> std::collections::BTreeMap<K, V>,
    where (K: ToRedisArgs, V: ToRedisArgs)
);

#[cfg(feature = "hashbrown")]
impl_to_redis_args_for_map!(
    for <K, V, S> hashbrown::HashMap<K, V, S>,
    where (K: ToRedisArgs, V: ToRedisArgs)
);

#[cfg(feature = "ahash")]
impl_to_redis_args_for_map!(
    for <K, V, S> ahash::AHashMap<K, V, S>,
    where (K: ToRedisArgs, V: ToRedisArgs)
);

macro_rules! to_redis_args_for_tuple {
    () => ();
    ($(#[$meta:meta],)*$($name:ident,)+) => (
        $(#[$meta])*
        impl<$($name: ToRedisArgs),*> ToRedisArgs for ($($name,)*) {
            // we have local variables named T1 as dummies and those
            // variables are unused.
            #[allow(non_snake_case, unused_variables)]
            fn write_redis_args<W>(&self, out: &mut W) where W: ?Sized + RedisWrite {
                let ($(ref $name,)*) = *self;
                $($name.write_redis_args(out);)*
            }

            #[allow(non_snake_case, unused_variables)]
            #[inline] fn num_of_args(&self) -> usize {
                let mut n: usize = 0;
                $(let $name = (); n += 1;)*
                n
            }

            #[allow(non_snake_case, unused_variables)]
            #[inline] fn args_size(&self) -> usize {
                let ($(ref $name,)*) = *self;
                0 $( + $name.args_size())*
            }

            #[allow(non_snake_case, unused_variables)]
            #[inline] fn num_of_args_and_size(&self) -> (usize, usize) {
                let ($(ref $name,)*) = *self;
                let args_count = 0usize $( + { let $name = (); 1usize })*;
                let args_size = 0usize $( + $name.args_size())*;
                (args_count, args_size)
            }
        }
    )
}

to_redis_args_for_tuple! { #[cfg_attr(docsrs, doc(fake_variadic))], #[doc = "This trait is implemented for tuples up to 12 items long."], T, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, }
to_redis_args_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, }

impl<T: ToRedisArgs, const N: usize> ToRedisArgs for &[T; N] {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        ToRedisArgs::write_args_from_slice(self.as_slice(), out);
    }

    #[inline]
    fn num_of_args(&self) -> usize {
        self.num_of_args_and_size().0
    }

    #[inline]
    fn args_size(&self) -> usize {
        self.num_of_args_and_size().1
    }

    #[inline]
    fn num_of_args_and_size(&self) -> (usize, usize) {
        <T as ToRedisArgs>::num_of_args_and_size_for_array(self)
    }
}
impl<const N: usize> ToSingleRedisArg for &[u8; N] {}

impl<T: FromRedisValue, const N: usize> FromRedisValue for [T; N] {
    fn from_redis_value_ref(v: &Value) -> Result<[T; N], ParsingError> {
        match *v {
            Value::BulkString(ref bytes) => match FromRedisValue::from_byte_slice(bytes) {
                Some(items) => vec_to_array(items, v),
                None => {
                    let msg = format!(
                        "Conversion to Array[{}; {N}] failed",
                        std::any::type_name::<T>()
                    );
                    crate::errors::invalid_type_error!(v, msg)
                }
            },
            Value::Array(ref items) => {
                let items = FromRedisValue::from_redis_value_refs(items)?;
                vec_to_array(items, v)
            }
            Value::Nil => vec_to_array(vec![], v),
            _ => crate::errors::invalid_type_error!(v, "Response type not array compatible"),
        }
    }

    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        Self::from_redis_value_ref(&v)
    }
}

/// Implement `FromRedisValue` for `$Type` (which should use the generic parameter `$T`).
///
/// The implementation parses the value into a vec, and then passes the value through `$convert`.
/// If `$convert` is omitted, it defaults to `Into::into`.
macro_rules! from_vec_from_redis_value {
    (<$T:ident> $Type:ty) => {
        from_vec_from_redis_value!(<$T> $Type; Into::into);
    };

    (<$T:ident> $Type:ty; $convert:expr) => {
        impl<$T: FromRedisValue> FromRedisValue for $Type {
            fn from_redis_value_ref(v: &Value) -> Result<$Type, ParsingError> {
                match v {
                    // All binary data except u8 will try to parse into a single element vector.
                    // u8 has its own implementation of from_byte_slice.
                    Value::BulkString(bytes) => match FromRedisValue::from_byte_slice(bytes) {
                        Some(x) => Ok($convert(x)),
                        None => crate::errors::invalid_type_error!(
                            v,
                            format!("Conversion to {} failed.", std::any::type_name::<$Type>())
                        ),
                    },
                    Value::Array(items) => FromRedisValue::from_redis_value_refs(items).map($convert),
                    Value::Set(items) => FromRedisValue::from_redis_value_refs(items).map($convert),
                    Value::Map(items) => {
                        let mut n: Vec<T> = vec![];
                        for item in items {
                            match FromRedisValue::from_redis_value_ref(&Value::Map(vec![item.clone()])) {
                                Ok(v) => {
                                    n.push(v);
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                            }
                        }
                        Ok($convert(n))
                    }
                    Value::Nil => Ok($convert(Vec::new())),
                    _ => crate::errors::invalid_type_error!(v, "Response type not vector compatible."),
                }
            }
            fn from_redis_value(v: Value) -> Result<$Type, ParsingError> {
                match v {
                    // Binary data is parsed into a single-element vector, except
                    // for the element type `u8`, which directly consumes the entire
                    // array of bytes.
                    Value::BulkString(bytes) => FromRedisValue::from_byte_vec(bytes).map($convert),
                    Value::Array(items) => FromRedisValue::from_redis_values(items).map($convert),
                    Value::Set(items) => FromRedisValue::from_redis_values(items).map($convert),
                    Value::Map(items) => {
                        let mut n: Vec<T> = vec![];
                        for item in items {
                            match FromRedisValue::from_redis_value(Value::Map(vec![item])) {
                                Ok(v) => {
                                    n.push(v);
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                            }
                        }
                        Ok($convert(n))
                    }
                    Value::Nil => Ok($convert(Vec::new())),
                    _ => crate::errors::invalid_type_error!(v, "Response type not vector compatible."),
                }
            }
        }
    };
}

from_vec_from_redis_value!(<T> Vec<T>);
from_vec_from_redis_value!(<T> std::sync::Arc<[T]>);
from_vec_from_redis_value!(<T> Box<[T]>; Vec::into_boxed_slice);

macro_rules! impl_from_redis_value_for_map {
    (for <$($TypeParam:ident),+> $MapType:ty, where ($($WhereClause:tt)+)) => {
        impl< $($TypeParam),+ > FromRedisValue for $MapType
        where
            $($WhereClause)+
        {
            fn from_redis_value_ref(v: &Value) -> Result<$MapType, ParsingError> {
                let v = get_inner_value(v);
                match *v {
                    Value::Nil => Ok(Default::default()),
                    _ => v
                        .as_map_iter()
                        .ok_or_else(|| crate::errors::invalid_type_error_inner!(v, "Response type not map compatible"))?
                        .map(|(k, v)| {
                            Ok((from_redis_value_ref(k)?, from_redis_value_ref(v)?))
                        })
                        .collect(),
                }
            }

            fn from_redis_value(v: Value) -> Result<$MapType, ParsingError> {
                let v = get_owned_inner_value(v);
                match v {
                    Value::Nil => Ok(Default::default()),
                    _ => v
                        .into_map_iter()
                        .map_err(|v| crate::errors::invalid_type_error_inner!(v, "Response type not map compatible"))?
                        .map(|(k, v)| {
                            Ok((from_redis_value(k)?, from_redis_value(v)?))
                        })
                        .collect(),
                }
            }
        }
    };
}

impl_from_redis_value_for_map!(
    for <K, V, S> std::collections::HashMap<K, V, S>,
    where (K: FromRedisValue + Eq + Hash, V: FromRedisValue, S: BuildHasher + Default)
);

impl_from_redis_value_for_map!(
    for <K, V> std::collections::BTreeMap<K, V>,
    where (K: FromRedisValue + Eq + Ord, V: FromRedisValue)
);

#[cfg(feature = "hashbrown")]
impl_from_redis_value_for_map!(
    for <K, V, S> hashbrown::HashMap<K, V, S>,
    where (K: FromRedisValue + Eq + Hash, V: FromRedisValue, S: BuildHasher + Default)
);

// `AHashMap::default` is not generic over `S` param so we can't be generic over it as well.
#[cfg(feature = "ahash")]
impl_from_redis_value_for_map!(
    for <K, V> ahash::AHashMap<K, V>,
    where (K: FromRedisValue + Eq + Hash, V: FromRedisValue)
);

macro_rules! impl_from_redis_value_for_set {
    (for <$($TypeParam:ident),+> $SetType:ty, where ($($WhereClause:tt)+)) => {
        impl< $($TypeParam),+ > FromRedisValue for $SetType
        where
            $($WhereClause)+
        {
            fn from_redis_value_ref(v: &Value) -> Result<$SetType, ParsingError> {
                let v = get_inner_value(v);
                let items = v
                    .as_sequence()
                    .ok_or_else(|| crate::errors::invalid_type_error_inner!(v, "Response type not map compatible"))?;
                items.iter().map(|item| from_redis_value_ref(item)).collect()
            }

            fn from_redis_value(v: Value) -> Result<$SetType, ParsingError> {
                let v = get_owned_inner_value(v);
                let items = v
                    .into_sequence()
                    .map_err(|v| crate::errors::invalid_type_error_inner!(v, "Response type not map compatible"))?;
                items
                    .into_iter()
                    .map(|item| from_redis_value(item))
                    .collect()
            }
        }
    };
}

impl_from_redis_value_for_set!(
    for <T, S> std::collections::HashSet<T, S>,
    where (T: FromRedisValue + Eq + Hash, S: BuildHasher + Default)
);

impl_from_redis_value_for_set!(
    for <T> std::collections::BTreeSet<T>,
    where (T: FromRedisValue + Ord)
);

#[cfg(feature = "hashbrown")]
impl_from_redis_value_for_set!(
    for <T, S> hashbrown::HashSet<T, S>,
    where (T: FromRedisValue + Eq + Hash, S: BuildHasher + Default)
);

// `AHashSet::from_iter` is not generic over `S` param so we can't be generic over it as well.
#[cfg(feature = "ahash")]
impl_from_redis_value_for_set!(
    for <T> ahash::AHashSet<T>,
    where (T: FromRedisValue + Eq + Hash)
);

macro_rules! from_redis_value_for_tuple {
    () => ();
    ($(#[$meta:meta],)*$($name:ident,)+) => (
        $(#[$meta])*
        impl<$($name: FromRedisValue),*> FromRedisValue for ($($name,)*) {
            // we have local variables named T1 as dummies and those
            // variables are unused.
            #[allow(non_snake_case, unused_variables)]
            fn from_redis_value_ref(v: &Value) -> Result<($($name,)*), ParsingError> {
                let v = get_inner_value(v);
                // hacky way to count the tuple size
                let mut n = 0;
                $(let $name = (); n += 1;)*

                match *v {
                    Value::Array(ref items) => {
                        if items.len() != n {
                            crate::errors::invalid_type_error!(v, "Array response of wrong dimension")
                        }

                        // The { i += 1; i - 1} is rust's postfix increment :)
                        let mut i = 0;
                        Ok(($({let $name = (); from_redis_value_ref(
                             &items[{ i += 1; i - 1 }])?},)*))
                    }

                    Value::Set(ref items) => {
                        if items.len() != n {
                            crate::errors::invalid_type_error!(v, "Set response of wrong dimension")
                        }

                        // The { i += 1; i - 1} is rust's postfix increment :)
                        let mut i = 0;
                        Ok(($({let $name = (); from_redis_value_ref(
                             &items[{ i += 1; i - 1 }])?},)*))
                    }

                    Value::Map(ref items) => {
                        if n != items.len() * 2 {
                            crate::errors::invalid_type_error!(v, "Map response of wrong dimension")
                        }

                        let mut flatten_items = items.iter().map(|(a,b)|[a,b]).flatten();

                        Ok(($({let $name = (); from_redis_value_ref(
                             &flatten_items.next().unwrap())?},)*))
                    }

                    _ => crate::errors::invalid_type_error!(v, "Not a Array response")
                }
            }

            // we have local variables named T1 as dummies and those
            // variables are unused.
            #[allow(non_snake_case, unused_variables)]
            fn from_redis_value(v: Value) -> Result<($($name,)*), ParsingError> {
                let v = get_owned_inner_value(v);
                // hacky way to count the tuple size
                let mut n = 0;
                $(let $name = (); n += 1;)*
                match v {
                    Value::Array(mut items) => {
                        if items.len() != n {
                            crate::errors::invalid_type_error!(Value::Array(items), "Array response of wrong dimension")
                        }

                        // The { i += 1; i - 1} is rust's postfix increment :)
                        let mut i = 0;
                        Ok(($({let $name = (); from_redis_value(
                            ::std::mem::replace(&mut items[{ i += 1; i - 1 }], Value::Nil)
                        )?},)*))
                    }

                    Value::Set(mut items) => {
                        if items.len() != n {
                            crate::errors::invalid_type_error!(Value::Array(items), "Set response of wrong dimension")
                        }

                        // The { i += 1; i - 1} is rust's postfix increment :)
                        let mut i = 0;
                        Ok(($({let $name = (); from_redis_value(
                            ::std::mem::replace(&mut items[{ i += 1; i - 1 }], Value::Nil)
                        )?},)*))
                    }

                    Value::Map(items) => {
                        if n != items.len() * 2 {
                            crate::errors::invalid_type_error!(Value::Map(items), "Map response of wrong dimension")
                        }

                        let mut flatten_items = items.into_iter().map(|(a,b)|[a,b]).flatten();

                        Ok(($({let $name = (); from_redis_value(
                            ::std::mem::replace(&mut flatten_items.next().unwrap(), Value::Nil)
                        )?},)*))
                    }

                    _ => crate::errors::invalid_type_error!(v, "Not a Array response")
                }
            }

            #[allow(non_snake_case, unused_variables)]
            fn from_redis_value_refs(items: &[Value]) -> Result<Vec<($($name,)*)>, ParsingError> {
                // hacky way to count the tuple size
                let mut n = 0;
                $(let $name = (); n += 1;)*
                if items.len() == 0 {
                    return Ok(vec![]);
                }

                if items.iter().all(|item| item.is_collection_of_len(n)) {
                    return items.iter().map(|item| from_redis_value_ref(item)).collect();
                }

                let mut rv = Vec::with_capacity(items.len() / n);
                if let [$($name),*] = items {
                    rv.push(($(from_redis_value_ref($name)?,)*));
                    return Ok(rv);
                }
                for chunk in items.chunks(n) {
                    match chunk {
                        [$($name),*] => rv.push(($(from_redis_value_ref($name)?,)*)),
                         _ => return Err(format!("Vector of length {} doesn't have arity of {n}", items.len()).into()),
                    }
                }
                Ok(rv)
            }

            #[allow(non_snake_case, unused_variables)]
            fn from_each_redis_values(mut items: Vec<Value>) -> Vec<Result<($($name,)*), ParsingError>> {
                #[allow(unused_parens)]
                let extract = |val: ($(Result<$name, ParsingError>,)*)| -> Result<($($name,)*), ParsingError> {
                    let ($($name,)*) = val;
                    Ok(($($name?,)*))
                };

                // hacky way to count the tuple size
                let mut n = 0;
                $(let $name = (); n += 1;)*

                // let mut rv = vec![];
                if items.len() == 0 {
                    return vec![];
                }
                if items.iter().all(|item| item.is_collection_of_len(n)) {
                    return items.into_iter().map(|item| from_redis_value(item).map_err(|err|err.into())).collect();
                }

                let mut rv = Vec::with_capacity(items.len() / n);

                for chunk in items.chunks_mut(n) {
                    match chunk {
                        // Take each element out of the chunk with `std::mem::replace`, leaving a `Value::Nil`
                        // in its place. This allows each `Value` to be parsed without being copied.
                        // Since `items` is consumed by this function and not used later, this replacement
                        // is not observable to the rest of the code.
                        [$($name),*] => rv.push(extract(($(from_redis_value(std::mem::replace($name, Value::Nil)).into(),)*))),
                         _ => return vec![Err(format!("Vector of length {} doesn't have arity of {n}", items.len()).into())],
                    }
                }
                rv
            }

            #[allow(non_snake_case, unused_variables)]
            fn from_redis_values(mut items: Vec<Value>) -> Result<Vec<($($name,)*)>, ParsingError> {
                // hacky way to count the tuple size
                let mut n = 0;
                $(let $name = (); n += 1;)*

                // let mut rv = vec![];
                if items.len() == 0 {
                    return Ok(vec![])
                }
                if items.iter().all(|item| item.is_collection_of_len(n)) {
                    return items.into_iter().map(|item| from_redis_value(item)).collect();
                }

                let mut rv = Vec::with_capacity(items.len() / n);
                for chunk in items.chunks_mut(n) {
                    match chunk {
                        // Take each element out of the chunk with `std::mem::replace`, leaving a `Value::Nil`
                        // in its place. This allows each `Value` to be parsed without being copied.
                        // Since `items` is consume by this function and not used later, this replacement
                        // is not observable to the rest of the code.
                        [$($name),*] => rv.push(($(from_redis_value(std::mem::replace($name, Value::Nil))?,)*)),
                         _ => return Err(format!("Vector of length {} doesn't have arity of {n}", items.len()).into()),
                    }
                }
                Ok(rv)
            }
        }
    )
}

from_redis_value_for_tuple! { #[cfg_attr(docsrs, doc(fake_variadic))], #[doc = "This trait is implemented for tuples up to 12 items long."], T, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, }
from_redis_value_for_tuple! { #[doc(hidden)], T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, }

impl RedisWrite for Vec<Vec<u8>> {
    fn write_arg(&mut self, arg: &[u8]) {
        self.push(arg.to_owned());
    }

    fn write_arg_fmt(&mut self, arg: impl fmt::Display) {
        self.push(arg.to_string().into_bytes());
    }

    fn writer_for_next_arg(&mut self) -> impl io::Write + '_ {
        self.push(Vec::new());
        self.last_mut().unwrap()
    }

    fn reserve_space_for_args(&mut self, additional: impl IntoIterator<Item = usize>) {
        // It would be nice to do this, but there's no way to store where we currently are.
        // Checking for the first empty Vec is not possible, as it's valid to write empty args.
        // self.extend(additional.iter().copied().map(Vec::with_capacity));
        // So we just reserve space for the extra args and have to forgo the extra optimisation
        self.reserve(additional.into_iter().count());
    }

    #[cfg(feature = "bytes")]
    fn bufmut_for_next_arg(&mut self, capacity: usize) -> impl bytes::BufMut + '_ {
        self.push(Vec::with_capacity(capacity));
        self.last_mut().unwrap()
    }
}

macro_rules! impl_write_redis_args_for_collection {
    ($type:ty) => {
        impl<'a, T> ToRedisArgs for $type
        where
            T: ToRedisArgs,
        {
            #[inline]
            fn write_redis_args<W>(&self, out: &mut W)
            where
                W: ?Sized + RedisWrite,
            {
                ToRedisArgs::write_args_from_slice(self, out)
            }

            #[inline]
            fn num_of_args(&self) -> usize {
                self.num_of_args_and_size().0
            }

            #[inline]
            fn args_size(&self) -> usize {
                self.num_of_args_and_size().1
            }

            #[inline]
            fn num_of_args_and_size(&self) -> (usize, usize) {
                <T as ToRedisArgs>::num_of_args_and_size_for_slice(self)
            }

            #[inline]
            fn describe_numeric_behavior(&self) -> NumericBehavior {
                NumericBehavior::NonNumeric
            }
        }
    };
}
impl_write_redis_args_for_collection! {&'a [T]}
impl_write_redis_args_for_collection! {&'a mut [T]}
impl_write_redis_args_for_collection! {Box<[T]>}
impl_write_redis_args_for_collection! {std::sync::Arc<[T]>}
impl_write_redis_args_for_collection! {std::rc::Rc<[T]>}
impl_write_redis_args_for_collection! {Vec<T>}

impl ToSingleRedisArg for &[u8] {}
impl ToSingleRedisArg for &mut [u8] {}
impl ToSingleRedisArg for Vec<u8> {}
impl ToSingleRedisArg for Box<[u8]> {}
impl ToSingleRedisArg for std::rc::Rc<[u8]> {}
impl ToSingleRedisArg for std::sync::Arc<[u8]> {}
