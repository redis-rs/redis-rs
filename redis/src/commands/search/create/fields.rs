//! Defines the schema field types used with the FT.CREATE command.
use crate::{RedisWrite, ToRedisArgs};

/// Field type for schema definition.
/// More information at: <https://redis.io/docs/latest/commands/ft.create/#required-arguments>
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum FieldType {
    /// Allows full-text search queries against the value in this attribute.
    Text,
    /// Allows exact-match queries, such as categories or primary keys, against the value in this attribute.
    Tag,
    /// Allows numeric range queries against the value in this attribute.
    Numeric,
    /// Allows radius range queries against the value (point) in this attribute.
    Geo,
    /// Allows vector queries against the value in this attribute. This requires query dialect 2 or above (introduced in RediSearch v2.4).
    Vector,
    /// Allows polygon queries against the value in this attribute.
    GeoShape,
}

impl ToRedisArgs for FieldType {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            Self::Text => b"TEXT",
            Self::Tag => b"TAG",
            Self::Numeric => b"NUMERIC",
            Self::Geo => b"GEO",
            Self::Vector => b"VECTOR",
            Self::GeoShape => b"GEOSHAPE",
        });
    }
}

impl FieldType {
    /// Returns whether the field type is sortable.
    pub fn is_sortable(&self) -> bool {
        matches!(self, Self::Text | Self::Tag | Self::Numeric | Self::Geo)
    }
}

/// Enumeration for sortable fields
/// <https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/sorting/>
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Sortable {
    /// Apply sortable
    Yes,
    /// Apply sortable with un-normalized form
    Unf,
}

impl ToRedisArgs for Sortable {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Self::Yes => out.write_arg(b"SORTABLE"),
            Self::Unf => {
                out.write_arg(b"SORTABLE");
                out.write_arg(b"UNF");
            }
        }
    }
}

/// Declaring a text attribute as PHONETIC will perform phonetic matching on it in searches by default.
/// The obligatory argument specifies the phonetic algorithm and language used.
/// <https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/phonetic_matching/>
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Phonetic {
    /// Double metaphone for English
    DmEnglish,
    /// Double metaphone for French
    DmFrench,
    /// Double metaphone for Portuguese
    DmPortuguese,
    /// Double metaphone for Spanish
    DmSpanish,
}

impl ToRedisArgs for Phonetic {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            Self::DmEnglish => b"dm:en",
            Self::DmFrench => b"dm:fr",
            Self::DmPortuguese => b"dm:pt",
            Self::DmSpanish => b"dm:es",
        });
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) struct BaseSchemaField {
    pub field_type: FieldType,
    pub alias: Option<String>,
    pub index_missing: bool,
}

impl BaseSchemaField {
    pub(crate) fn new(field_type: FieldType) -> Self {
        Self {
            field_type,
            alias: None,
            index_missing: false,
        }
    }

    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn index_missing(mut self, index_missing: bool) -> Self {
        self.index_missing = index_missing;
        self
    }
}

impl ToRedisArgs for BaseSchemaField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(alias) = &self.alias {
            out.write_arg(b"AS");
            alias.write_redis_args(out);
        }

        self.field_type.write_redis_args(out);

        if self.index_missing {
            out.write_arg(b"INDEXMISSING");
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) struct SchemaCommonField {
    pub base: BaseSchemaField,
    pub sortable: Option<Sortable>,
    pub no_index: bool,
}

impl SchemaCommonField {
    pub(crate) fn new(field_type: FieldType) -> Self {
        Self {
            base: BaseSchemaField::new(field_type),
            sortable: None,
            no_index: false,
        }
    }

    pub fn sortable(mut self, sortable: Sortable) -> Self {
        if self.base.field_type.is_sortable() {
            self.sortable = Some(sortable);
        } else {
            unreachable!("Field type {:?} is not sortable", self.base.field_type);
        }
        self
    }

    pub fn no_index(mut self, no_index: bool) -> Self {
        self.no_index = no_index;
        self
    }

    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.base = self.base.alias(alias);
        self
    }

    pub fn index_missing(mut self, index_missing: bool) -> Self {
        self.base = self.base.index_missing(index_missing);
        self
    }
}

impl ToRedisArgs for SchemaCommonField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(sortable) = &self.sortable {
            sortable.write_redis_args(out);
        }

        if self.no_index {
            out.write_arg(b"NOINDEX");
        }
    }
}

/// Represents a text field in the schema.
#[must_use = "Text field has no effect unless inserted into a schema"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SchemaTextField {
    pub(crate) common: SchemaCommonField,
    no_stem: bool,
    weight: Option<f64>,
    phonetic: Option<Phonetic>,
    with_suffix_trie: bool,
    index_empty: bool,
}

impl SchemaTextField {
    /// Create a new TEXT field.
    pub fn new() -> Self {
        Self {
            common: SchemaCommonField::new(FieldType::Text),
            no_stem: false,
            weight: None,
            phonetic: None,
            with_suffix_trie: false,
            index_empty: false,
        }
    }

    /// Disables stemming when indexing the text field's values. This may be ideal for things like proper names.
    /// <https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/stemming/>
    pub fn no_stem(mut self, no_stem: bool) -> Self {
        self.no_stem = no_stem;
        self
    }

    /// Declares the importance of this attribute when calculating result accuracy.
    /// This is a multiplication factor and defaults to 1 if not specified.
    pub fn weight(mut self, weight: f64) -> Self {
        self.weight = Some(weight);
        self
    }

    /// Declaring a text attribute as PHONETIC will perform phonetic matching on it in searches by default.
    /// <https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/phonetic_matching/>
    pub fn phonetic(mut self, phonetic: Phonetic) -> Self {
        self.phonetic = Some(phonetic);
        self
    }

    /// Keeps a suffix trie with all terms which match the suffix. It is used to optimize contains (foo) and suffix (*foo) queries.
    /// Otherwise, a brute-force search on the trie is performed. If suffix trie exists for some fields, these queries will be disabled for other fields.
    pub fn with_suffix_trie(mut self, with_suffix_trie: bool) -> Self {
        self.with_suffix_trie = with_suffix_trie;
        self
    }

    /// Index empty strings. This allows searching for empty values - documents that do not contain a specific field.
    /// By default, empty strings are not indexed.
    pub fn index_empty(mut self, index_empty: bool) -> Self {
        self.index_empty = index_empty;
        self
    }

    /// Mark the field as sortable.
    pub fn sortable(mut self, sortable: Sortable) -> Self {
        self.common = self.common.sortable(sortable);
        self
    }

    /// Mark the field as no index. This means that the field will not be indexed.
    pub fn no_index(mut self, no_index: bool) -> Self {
        self.common = self.common.no_index(no_index);
        self
    }

    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.common = self.common.alias(alias);
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        self.common = self.common.index_missing(index_missing);
        self
    }
}

impl ToRedisArgs for SchemaTextField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        self.common.base.write_redis_args(out);

        if self.no_stem {
            out.write_arg(b"NOSTEM");
        }
        if let Some(weight) = self.weight {
            out.write_arg(b"WEIGHT");
            weight.write_redis_args(out);
        }
        if let Some(phonetic) = &self.phonetic {
            out.write_arg(b"PHONETIC");
            phonetic.write_redis_args(out);
        }
        if self.with_suffix_trie {
            out.write_arg(b"WITHSUFFIXTRIE");
        }
        if self.index_empty {
            out.write_arg(b"INDEXEMPTY");
        }

        self.common.write_redis_args(out);
    }
}

impl Default for SchemaTextField {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a tag field in the schema.
#[must_use = "Tag field has no effect unless inserted into a schema"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SchemaTagField {
    pub(crate) common: SchemaCommonField,
    separator: Option<char>,
    case_sensitive: bool,
    with_suffix_trie: bool,
    index_empty: bool,
}

impl SchemaTagField {
    /// Create a new TAG field.
    pub fn new() -> Self {
        Self {
            common: SchemaCommonField::new(FieldType::Tag),
            separator: None,
            case_sensitive: false,
            with_suffix_trie: false,
            index_empty: false,
        }
    }

    /// Indicates how the text contained in the attribute is to be split into individual tags.
    /// The value must be a single character.
    /// The default is ','.
    pub fn separator(mut self, separator: char) -> Self {
        self.separator = Some(separator);
        self
    }

    /// Keeps the original letter cases of the tags. If not specified, the characters are converted to lowercase.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }

    /// Keeps a suffix trie with all terms which match the suffix. It is used to optimize contains (foo) and suffix (*foo) queries.
    /// Otherwise, a brute-force search on the trie is performed. If suffix trie exists for some fields, these queries will be disabled for other fields.
    pub fn with_suffix_trie(mut self, with_suffix_trie: bool) -> Self {
        self.with_suffix_trie = with_suffix_trie;
        self
    }

    /// Index empty strings. This allows searching for empty values - documents that do not contain a specific field. By default, empty strings are not indexed.
    pub fn index_empty(mut self, index_empty: bool) -> Self {
        self.index_empty = index_empty;
        self
    }

    /// Mark the field as sortable.
    pub fn sortable(mut self, sortable: Sortable) -> Self {
        self.common = self.common.sortable(sortable);
        self
    }

    /// Mark the field as no index. This means that the field will not be indexed.
    pub fn no_index(mut self, no_index: bool) -> Self {
        self.common = self.common.no_index(no_index);
        self
    }

    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.common = self.common.alias(alias);
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        self.common = self.common.index_missing(index_missing);
        self
    }
}

impl ToRedisArgs for SchemaTagField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        self.common.base.write_redis_args(out);

        if let Some(separator) = self.separator {
            out.write_arg(b"SEPARATOR");
            out.write_arg(separator.to_string().as_bytes());
        }
        if self.case_sensitive {
            out.write_arg(b"CASESENSITIVE");
        }
        if self.with_suffix_trie {
            out.write_arg(b"WITHSUFFIXTRIE");
        }
        if self.index_empty {
            out.write_arg(b"INDEXEMPTY");
        }

        self.common.write_redis_args(out);
    }
}

impl Default for SchemaTagField {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a numeric field in the schema.
#[must_use = "Numeric field has no effect unless inserted into a schema"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SchemaNumericField {
    pub(crate) common: SchemaCommonField,
}

impl SchemaNumericField {
    /// Create a new NUMERIC field.
    pub fn new() -> Self {
        Self {
            common: SchemaCommonField::new(FieldType::Numeric),
        }
    }

    /// Mark the field as sortable.
    pub fn sortable(mut self, sortable: Sortable) -> Self {
        self.common = self.common.sortable(sortable);
        self
    }

    /// Mark the field as no index. This means that the field will not be indexed.
    pub fn no_index(mut self, no_index: bool) -> Self {
        self.common = self.common.no_index(no_index);
        self
    }

    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.common = self.common.alias(alias);
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        self.common = self.common.index_missing(index_missing);
        self
    }
}

impl ToRedisArgs for SchemaNumericField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        self.common.base.write_redis_args(out);
        self.common.write_redis_args(out);
    }
}

impl Default for SchemaNumericField {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a geo field in the schema.
#[must_use = "Geo field has no effect unless inserted into a schema"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SchemaGeoField {
    pub(crate) common: SchemaCommonField,
}

impl SchemaGeoField {
    /// Create a new GEO field.
    pub fn new() -> Self {
        Self {
            common: SchemaCommonField::new(FieldType::Geo),
        }
    }

    /// Mark the field as sortable.
    pub fn sortable(mut self, sortable: Sortable) -> Self {
        self.common = self.common.sortable(sortable);
        self
    }

    /// Mark the field as no index. This means that the field will not be indexed.
    pub fn no_index(mut self, no_index: bool) -> Self {
        self.common = self.common.no_index(no_index);
        self
    }

    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.common = self.common.alias(alias);
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        self.common = self.common.index_missing(index_missing);
        self
    }
}

impl ToRedisArgs for SchemaGeoField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        self.common.base.write_redis_args(out);
        self.common.write_redis_args(out);
    }
}

impl Default for SchemaGeoField {
    fn default() -> Self {
        Self::new()
    }
}

/// Coordinate system for geo shape fields
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CoordSystem {
    /// Geographic longitude and latitude coordinates
    Spherical,
    /// Cartesian X Y coordinates
    Flat,
}

impl ToRedisArgs for CoordSystem {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            Self::Spherical => b"SPHERICAL",
            Self::Flat => b"FLAT",
        });
    }
}

/// Represents a geo shape field in the schema.
#[must_use = "Geo shape field has no effect unless inserted into a schema"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SchemaGeoShapeField {
    pub(crate) common: SchemaCommonField,
    coord_system: CoordSystem,
}

impl SchemaGeoShapeField {
    /// Create a new GEO SHAPE field.
    pub fn new() -> Self {
        Self {
            common: SchemaCommonField::new(FieldType::GeoShape),
            // The default coordinate system is SPHERICAL.
            coord_system: CoordSystem::Spherical,
        }
    }

    /// Set the coordinate system for the field.
    pub fn coord_system(mut self, coord_system: CoordSystem) -> Self {
        self.coord_system = coord_system;
        self
    }

    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.common = self.common.alias(alias);
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        self.common = self.common.index_missing(index_missing);
        self
    }

    // Sortable is not applicable to geo shape fields.

    /// Mark the field as no index. This means that the field will not be indexed.
    pub fn no_index(mut self, no_index: bool) -> Self {
        self.common = self.common.no_index(no_index);
        self
    }
}

impl ToRedisArgs for SchemaGeoShapeField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        // The coordinate system has to be placed immediately after the field type,
        // which prevents the use of the common base implementation.
        if let Some(alias) = &self.common.base.alias {
            out.write_arg(b"AS");
            alias.write_redis_args(out);
        }

        self.common.base.field_type.write_redis_args(out);
        self.coord_system.write_redis_args(out);

        if self.common.base.index_missing {
            out.write_arg(b"INDEXMISSING");
        }

        self.common.write_redis_args(out);
    }
}

impl Default for SchemaGeoShapeField {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema;
    use crate::search::FtCreateCommand;

    static INDEX_NAME: &str = "index";
    static TEXT_FIELD_NAME: &str = "title";
    static NUMERIC_FIELD_NAME: &str = "price";
    static TAG_FIELD_NAME: &str = "condition";
    static GEO_FIELD_NAME: &str = "location";
    static GEOSHAPE_FIELD_NAME: &str = "area";
    static CUSTOM_ALIAS: &str = "custom_alias";

    // ============================================================================
    // TEXT Field Tests
    // ============================================================================
    #[test]
    fn test_text_field() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(ft_create.into_args(), "FT.CREATE index SCHEMA title TEXT");
    }

    #[test]
    fn test_text_field_with_nostem() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().no_stem(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT NOSTEM"
        );
    }

    #[test]
    fn test_text_field_with_weight() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().weight(1.0),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT WEIGHT 1.0"
        );
    }

    #[test]
    fn test_text_field_with_phonetic() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().phonetic(Phonetic::DmEnglish),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT PHONETIC dm:en"
        );
    }

    #[test]
    fn test_text_field_with_withsuffixtrie() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().with_suffix_trie(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT WITHSUFFIXTRIE"
        );
    }

    #[test]
    fn test_text_field_with_indexempty() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().index_empty(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT INDEXEMPTY"
        );
    }

    #[test]
    fn test_text_field_with_alias() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().alias(CUSTOM_ALIAS),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title AS custom_alias TEXT"
        );
    }

    #[test]
    fn test_text_field_with_indexmissing() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().index_missing(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT INDEXMISSING"
        );
    }

    #[test]
    fn test_text_field_with_sortable() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().sortable(Sortable::Yes),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT SORTABLE"
        );
    }

    #[test]
    fn test_text_field_with_sortable_unf() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().sortable(Sortable::Unf),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT SORTABLE UNF"
        );
    }

    #[test]
    fn test_text_field_with_noindex() {
        let schema = schema! {
            TEXT_FIELD_NAME => SchemaTextField::new().no_index(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title TEXT NOINDEX"
        );
    }

    #[test]
    fn test_text_field_with_all_options() {
        let field = SchemaTextField::new()
            .no_stem(true)
            .weight(1.0)
            .phonetic(Phonetic::DmEnglish)
            .with_suffix_trie(true)
            .index_empty(true)
            .alias(CUSTOM_ALIAS)
            .index_missing(true)
            .sortable(Sortable::Unf);
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                TEXT_FIELD_NAME => field.clone(),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title AS custom_alias TEXT INDEXMISSING NOSTEM WEIGHT 1.0 PHONETIC dm:en WITHSUFFIXTRIE INDEXEMPTY SORTABLE UNF"
        );
        // Index missing and no index are mutually exclusive
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                TEXT_FIELD_NAME => field.index_missing(false).no_index(true),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA title AS custom_alias TEXT NOSTEM WEIGHT 1.0 PHONETIC dm:en WITHSUFFIXTRIE INDEXEMPTY SORTABLE UNF NOINDEX"
        );
    }

    // ============================================================================
    // NUMERIC Field Tests
    // ============================================================================
    #[test]
    fn test_numeric_field() {
        let schema = schema! {
            NUMERIC_FIELD_NAME => SchemaNumericField::new(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price NUMERIC"
        );
    }

    #[test]
    fn test_numeric_field_with_alias() {
        let schema = schema! {
            NUMERIC_FIELD_NAME => SchemaNumericField::new().alias(CUSTOM_ALIAS),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price AS custom_alias NUMERIC"
        );
    }

    #[test]
    fn test_numeric_field_with_indexmissing() {
        let schema = schema! {
            NUMERIC_FIELD_NAME => SchemaNumericField::new().index_missing(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price NUMERIC INDEXMISSING"
        );
    }

    #[test]
    fn test_numeric_field_with_sortable() {
        let schema = schema! {
            NUMERIC_FIELD_NAME => SchemaNumericField::new().sortable(Sortable::Yes),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price NUMERIC SORTABLE"
        );
    }

    #[test]
    fn test_numeric_field_with_sortable_unf() {
        let schema = schema! {
            NUMERIC_FIELD_NAME => SchemaNumericField::new().sortable(Sortable::Unf),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price NUMERIC SORTABLE UNF"
        );
    }

    #[test]
    fn test_numeric_field_with_noindex() {
        let schema = schema! {
            NUMERIC_FIELD_NAME => SchemaNumericField::new().no_index(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price NUMERIC NOINDEX"
        );
    }

    #[test]
    fn test_numeric_field_with_all_options() {
        let field = SchemaNumericField::new()
            .alias(CUSTOM_ALIAS)
            .index_missing(true)
            .sortable(Sortable::Unf);
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                NUMERIC_FIELD_NAME => field.clone(),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price AS custom_alias NUMERIC INDEXMISSING SORTABLE UNF"
        );
        // Index missing and no index are mutually exclusive
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                NUMERIC_FIELD_NAME => field.index_missing(false).no_index(true),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA price AS custom_alias NUMERIC SORTABLE UNF NOINDEX"
        );
    }

    // ============================================================================
    // GEO Field Tests
    // ============================================================================
    #[test]
    fn test_geo_field() {
        let schema = schema! {
            GEO_FIELD_NAME => SchemaGeoField::new(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(ft_create.into_args(), "FT.CREATE index SCHEMA location GEO");
    }

    #[test]
    fn test_geo_field_with_alias() {
        let schema = schema! {
            GEO_FIELD_NAME => SchemaGeoField::new().alias(CUSTOM_ALIAS),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location AS custom_alias GEO"
        );
    }

    #[test]
    fn test_geo_field_with_indexmissing() {
        let schema = schema! {
            GEO_FIELD_NAME => SchemaGeoField::new().index_missing(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location GEO INDEXMISSING"
        );
    }

    #[test]
    fn test_geo_field_with_sortable() {
        let schema = schema! {
            GEO_FIELD_NAME => SchemaGeoField::new().sortable(Sortable::Yes),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location GEO SORTABLE"
        );
    }

    #[test]
    fn test_geo_field_with_sortable_unf() {
        let schema = schema! {
            GEO_FIELD_NAME => SchemaGeoField::new().sortable(Sortable::Unf),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location GEO SORTABLE UNF"
        );
    }

    #[test]
    fn test_geo_field_with_noindex() {
        let schema = schema! {
            GEO_FIELD_NAME => SchemaGeoField::new().no_index(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location GEO NOINDEX"
        );
    }

    #[test]
    fn test_geo_field_with_all_options() {
        let field = SchemaGeoField::new()
            .alias(CUSTOM_ALIAS)
            .index_missing(true)
            .sortable(Sortable::Unf);
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                GEO_FIELD_NAME => field.clone(),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location AS custom_alias GEO INDEXMISSING SORTABLE UNF"
        );
        // Index missing and no index are mutually exclusive
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                GEO_FIELD_NAME => field.index_missing(false).no_index(true),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA location AS custom_alias GEO SORTABLE UNF NOINDEX"
        );
    }

    // ============================================================================
    // TAG Field Tests
    // ============================================================================
    #[test]
    fn test_tag_field() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG"
        );
    }

    #[test]
    fn test_tag_field_with_separator() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().separator(','),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG SEPARATOR ,"
        );
    }

    #[test]
    fn test_tag_field_with_casesensitive() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().case_sensitive(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG CASESENSITIVE"
        );
    }

    #[test]
    fn test_tag_field_with_withsuffixtrie() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().with_suffix_trie(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG WITHSUFFIXTRIE"
        );
    }

    #[test]
    fn test_tag_field_with_indexempty() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().index_empty(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG INDEXEMPTY"
        );
    }

    #[test]
    fn test_tag_field_with_alias() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().alias(CUSTOM_ALIAS),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition AS custom_alias TAG"
        );
    }

    #[test]
    fn test_tag_field_with_indexmissing() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().index_missing(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG INDEXMISSING"
        );
    }

    #[test]
    fn test_tag_field_with_sortable() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().sortable(Sortable::Yes),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG SORTABLE"
        );
    }

    #[test]
    fn test_tag_field_with_sortable_unf() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().sortable(Sortable::Unf),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG SORTABLE UNF"
        );
    }

    #[test]
    fn test_tag_field_with_noindex() {
        let schema = schema! {
            TAG_FIELD_NAME => SchemaTagField::new().no_index(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition TAG NOINDEX"
        );
    }

    #[test]
    fn test_tag_field_with_all_options() {
        let field = SchemaTagField::new()
            .alias(CUSTOM_ALIAS)
            .separator(',')
            .case_sensitive(true)
            .with_suffix_trie(true)
            .index_empty(true)
            .sortable(Sortable::Unf);
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                TAG_FIELD_NAME => field.clone(),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition AS custom_alias TAG SEPARATOR , CASESENSITIVE WITHSUFFIXTRIE INDEXEMPTY SORTABLE UNF"
        );
        // Index missing and no index are mutually exclusive
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                TAG_FIELD_NAME => field.index_missing(false).no_index(true),
            },
        );
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA condition AS custom_alias TAG SEPARATOR , CASESENSITIVE WITHSUFFIXTRIE INDEXEMPTY SORTABLE UNF NOINDEX"
        );
    }

    // ============================================================================
    // GEOSHAPE Field Tests
    // ============================================================================
    #[test]
    fn test_geoshape_field_without_options_defaults_coord_system_to_spherical() {
        let schema = schema! {
            GEOSHAPE_FIELD_NAME => SchemaGeoShapeField::new(),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA area GEOSHAPE SPHERICAL"
        );
    }

    #[test]
    fn test_geoshape_field_with_flat_coord_system() {
        let schema = schema! {
            GEOSHAPE_FIELD_NAME => SchemaGeoShapeField::new().coord_system(CoordSystem::Flat),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA area GEOSHAPE FLAT"
        );
    }

    #[test]
    fn test_geoshape_field_with_alias() {
        let schema = schema! {
            GEOSHAPE_FIELD_NAME => SchemaGeoShapeField::new().alias(CUSTOM_ALIAS),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA area AS custom_alias GEOSHAPE SPHERICAL"
        );
    }

    #[test]
    fn test_geoshape_field_with_indexmissing() {
        let schema = schema! {
            GEOSHAPE_FIELD_NAME => SchemaGeoShapeField::new().index_missing(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA area GEOSHAPE SPHERICAL INDEXMISSING"
        );
    }

    #[test]
    fn test_geoshape_field_with_noindex() {
        let schema = schema! {
            GEOSHAPE_FIELD_NAME => SchemaGeoShapeField::new().no_index(true),
        };
        let ft_create = FtCreateCommand::new(INDEX_NAME, schema);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index SCHEMA area GEOSHAPE SPHERICAL NOINDEX"
        );
    }
}
