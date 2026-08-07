//! Defines the schema passed to the FT.CREATE command.
use super::fields::SchemaTextField;
use crate::{RedisWrite, ToRedisArgs};
use std::marker::PhantomData;

/// Field definition for schema
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum FieldDefinition {
    /// Text field
    Text(SchemaTextField),
}

impl ToRedisArgs for FieldDefinition {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            FieldDefinition::Text(tf) => tf.write_redis_args(out),
        }
    }
}

impl From<SchemaTextField> for FieldDefinition {
    fn from(field: SchemaTextField) -> Self {
        FieldDefinition::Text(field)
    }
}

/// Marker type indicating an empty schema (no fields added yet).
pub struct Empty;

/// Marker type indicating a non-empty schema (at least one field added).
pub struct NonEmpty;

/// The search schema declaring which fields to index.
///
/// Uses the typestate pattern to enforce at compile time that a schema
/// has at least one field before it can be used with a command.
///
/// # Type States
/// - `SearchSchema<Empty>` - No fields added yet, cannot be used with commands
/// - `SearchSchema<NonEmpty>` - At least one field added, can be used with commands
///
/// # Example
/// ```rust
/// use redis::{schema, search::*};
///
/// // Using the macro (recommended)
/// let schema = schema! {
///     "title" => SchemaTextField::new(),
///     "subtitle" => SchemaTextField::new()
/// };
///
/// // Using the builder pattern
/// let schema = SearchSchema::new()
///     .insert("title", SchemaTextField::new())
///     .insert("subtitle", SchemaTextField::new());
/// ```
#[must_use = "Schema has no effect unless passed to a command"]
#[derive(Debug, Clone)]
pub struct SearchSchema<State = Empty> {
    fields: Vec<(String, FieldDefinition)>,
    _state: PhantomData<State>,
}

impl SearchSchema<Empty> {
    /// Create a new empty schema.
    pub fn new() -> Self {
        SearchSchema {
            fields: Vec::new(),
            _state: PhantomData,
        }
    }

    /// Insert the first field into the schema.
    ///
    /// This transitions the schema from `Empty` to `NonEmpty` state.
    pub fn insert<K: Into<String>, V: Into<FieldDefinition>>(
        mut self,
        key: K,
        value: V,
    ) -> SearchSchema<NonEmpty> {
        self.fields.push((key.into(), value.into()));
        SearchSchema {
            fields: self.fields,
            _state: PhantomData,
        }
    }
}

impl Default for SearchSchema<Empty> {
    fn default() -> Self {
        Self::new()
    }
}

impl SearchSchema<NonEmpty> {
    /// Insert an additional field into the schema.
    pub fn insert<K: Into<String>, V: Into<FieldDefinition>>(mut self, key: K, value: V) -> Self {
        self.fields.push((key.into(), value.into()));
        self
    }
}

impl ToRedisArgs for SearchSchema<NonEmpty> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        for (key, field) in &self.fields {
            key.write_redis_args(out);
            field.write_redis_args(out);
        }
    }
}

/// Creates a non-empty [`SearchSchema`].
///
/// This macro offers a concise syntax for defining schemas and guarantees
/// at compile time that at least one field is specified. Empty schemas are
/// not allowed, and invoking the macro with no fields (`schema! {}`) will
/// result in a compile-time error.
///
/// # Example
/// ```rust
/// use redis::{schema, search::*};
///
/// let schema = schema! {
///     "title" => SchemaTextField::new().weight(2.0),
///     "subtitle" => SchemaTextField::new(),
/// };
/// ```
#[macro_export]
macro_rules! schema {
    // The `+` repetition requires at least one field - empty invocation won't match
    ($($key:expr => $value:expr),+ $(,)?) => {{
        $crate::search::SearchSchema::new()
            $(
                .insert($key, $value)
            )+
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::FtCreateCommand;

    static INDEX_NAME: &str = "index";
    static TEXT_FIELD_NAME: &str = "title";

    #[test]
    fn test_multiple_fields() {
        let schema = SearchSchema::new()
            .insert(TEXT_FIELD_NAME, SchemaTextField::new().weight(2.0))
            .insert("subtitle", SchemaTextField::new());

        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT WEIGHT 2.0 subtitle TEXT"
        );
    }

    #[test]
    fn test_macro_and_builder_produce_the_same_schema() {
        let from_macro = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().weight(2.0),
            "subtitle" => SchemaTextField::new(),
        });
        let from_builder = FtCreateCommand::new(INDEX_NAME).schema(
            SearchSchema::new()
                .insert(TEXT_FIELD_NAME, SchemaTextField::new().weight(2.0))
                .insert("subtitle", SchemaTextField::new()),
        );

        assert_eq!(from_macro.into_args(), from_builder.into_args());
    }

    #[test]
    fn test_macro_accepts_a_single_field_without_a_trailing_comma() {
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            TEXT_FIELD_NAME => SchemaTextField::new()
        });
        assert_eq!(ft_create.into_args(), "FT.CREATE index SCHEMA title TEXT");
    }

    /// The same attribute may be indexed more than once under different aliases.
    #[test]
    fn test_the_same_field_name_can_be_inserted_twice() {
        let ft_create = FtCreateCommand::new(INDEX_NAME).schema(schema! {
            "sku" => SchemaTextField::new().alias("sku_text"),
            "sku" => SchemaTextField::new().alias("sku_other"),
        });
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA sku AS sku_text TEXT sku AS sku_other TEXT"
        );
    }
}
