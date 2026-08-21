//! Defines the schema passed to the FT.CREATE command.
use super::fields::{
    SchemaGeoField, SchemaGeoShapeField, SchemaNumericField, SchemaTagField, SchemaTextField,
};
use crate::{RedisWrite, ToRedisArgs};

/// Field definition for schema
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum FieldDefinition {
    /// Text field
    Text(SchemaTextField),
    /// Numeric field
    Numeric(SchemaNumericField),
    /// Geo field
    Geo(SchemaGeoField),
    /// Tag field
    Tag(SchemaTagField),
    /// Geo shape field
    GeoShape(SchemaGeoShapeField),
}

impl ToRedisArgs for FieldDefinition {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Self::Text(tf) => tf.write_redis_args(out),
            Self::Numeric(nf) => nf.write_redis_args(out),
            Self::Geo(gf) => gf.write_redis_args(out),
            Self::Tag(tf) => tf.write_redis_args(out),
            Self::GeoShape(gs) => gs.write_redis_args(out),
        }
    }
}

impl From<SchemaTextField> for FieldDefinition {
    fn from(field: SchemaTextField) -> Self {
        Self::Text(field)
    }
}

impl From<SchemaNumericField> for FieldDefinition {
    fn from(field: SchemaNumericField) -> Self {
        Self::Numeric(field)
    }
}

impl From<SchemaGeoField> for FieldDefinition {
    fn from(field: SchemaGeoField) -> Self {
        Self::Geo(field)
    }
}

impl From<SchemaTagField> for FieldDefinition {
    fn from(field: SchemaTagField) -> Self {
        Self::Tag(field)
    }
}

impl From<SchemaGeoShapeField> for FieldDefinition {
    fn from(field: SchemaGeoShapeField) -> Self {
        Self::GeoShape(field)
    }
}

/// The search schema declaring which fields to index.
///
/// A schema must contain at least one field.
/// [`SearchSchema::new`] takes the first field, so an empty schema cannot be constructed.
/// This is required by the server - `FT.CREATE` rejects a `SCHEMA` with no fields.
///
/// # Example
/// ```rust
/// use redis::{schema, search::*};
///
/// // Using the macro (recommended)
/// let schema = schema! {
///     "title" => SchemaTextField::new(),
///     "price" => SchemaNumericField::new()
/// };
///
/// // Using the builder pattern
/// let schema = SearchSchema::new("title", SchemaTextField::new())
///     .insert("price", SchemaNumericField::new());
/// ```
#[must_use = "Schema has no effect unless passed to a command"]
#[derive(Debug, Clone)]
pub struct SearchSchema {
    fields: Vec<(String, FieldDefinition)>,
}

impl SearchSchema {
    /// Create a new schema with a field.
    pub fn new<K: Into<String>, V: Into<FieldDefinition>>(key: K, value: V) -> Self {
        Self {
            fields: vec![(key.into(), value.into())],
        }
    }

    /// Insert an additional field into the schema.
    pub fn insert<K: Into<String>, V: Into<FieldDefinition>>(mut self, key: K, value: V) -> Self {
        self.fields.push((key.into(), value.into()));
        self
    }
}

impl ToRedisArgs for SearchSchema {
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

/// Creates a [`SearchSchema`].
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
///     "price" => SchemaNumericField::new(),
///     "tags" => SchemaTagField::new().separator(','),
/// };
/// ```
#[macro_export]
macro_rules! schema {
    // The first field is matched outside the repetition so it can be passed to `new`,
    // which is also what makes an empty invocation fail to match.
    ($first_key:expr => $first_value:expr $(, $key:expr => $value:expr)* $(,)?) => {{
        $crate::search::SearchSchema::new($first_key, $first_value)
            $(
                .insert($key, $value)
            )*
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::FtCreateCommand;

    static INDEX_NAME: &str = "index";
    static TEXT_FIELD_NAME: &str = "title";
    static NUMERIC_FIELD_NAME: &str = "price";
    static TAG_FIELD_NAME: &str = "condition";

    #[test]
    fn test_multiple_fields() {
        let schema = SearchSchema::new(TEXT_FIELD_NAME, SchemaTextField::new().weight(2.0))
            .insert(NUMERIC_FIELD_NAME, SchemaNumericField::new())
            .insert(TAG_FIELD_NAME, SchemaTagField::new().separator(','));

        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT WEIGHT 2.0 price NUMERIC condition TAG SEPARATOR ,"
        );
    }

    #[test]
    fn test_macro_and_builder_produce_the_same_schema() {
        let from_macro = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                TEXT_FIELD_NAME => SchemaTextField::new().weight(2.0),
                "subtitle" => SchemaTextField::new(),
            },
        );
        let from_builder = FtCreateCommand::new(
            INDEX_NAME,
            SearchSchema::new(TEXT_FIELD_NAME, SchemaTextField::new().weight(2.0))
                .insert("subtitle", SchemaTextField::new()),
        );

        assert_eq!(from_macro.into_args(), from_builder.into_args());
    }

    #[test]
    fn test_macro_accepts_a_single_field_without_a_trailing_comma() {
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            },
        );
        assert_eq!(ft_create.into_args(), "FT.CREATE index SCHEMA title TEXT");
    }

    #[test]
    fn test_macro_accepts_a_single_field_with_a_trailing_comma() {
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                TEXT_FIELD_NAME => SchemaTextField::new(),
            },
        );
        assert_eq!(ft_create.into_args(), "FT.CREATE index SCHEMA title TEXT");
    }

    /// The same attribute may be indexed more than once under different aliases.
    #[test]
    fn test_the_same_field_name_can_be_inserted_twice() {
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                "sku" => SchemaTextField::new().alias("sku_text"),
                "sku" => SchemaTextField::new().alias("sku_other"),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA sku AS sku_text TEXT sku AS sku_other TEXT"
        );
    }
}
