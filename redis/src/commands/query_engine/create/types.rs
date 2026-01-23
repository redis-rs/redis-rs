//! Defines the types used with the FT.CREATE command.
//!
//! This module offers a type-safe mechanisms for constructing RediSearch schemas
//! and configuring the options passed to the command.
//!
//! # Examples
//!
//! ## Building a schema with the macro
//!
//! ```rust
//! use redis::{schema, search::*};
//!
//! let schema = schema! {
//!     "title" => SchemaTextField::new().weight(2.0),
//!     "price" => SchemaNumericField::new(),
//!     "condition" => SchemaTagField::new().separator(',')
//! };
//! ```
//!
//! ## Building a schema manually
//!
//! ```rust
//! use redis::search::*;
//!
//! let mut schema = RediSearchSchema::new();
//! schema.insert("title", SchemaTextField::new().weight(2.0));
//! schema.insert("price", SchemaNumericField::new());
//! schema.insert("condition", SchemaTagField::new().separator(','));
//! ```
use crate::{RedisWrite, ToRedisArgs};
use log::warn;

/// Data type for indexing
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum IndexDataType {
    /// Index hash data structures
    Hash,
    /// Index JSON data structures
    Json,
}

impl ToRedisArgs for IndexDataType {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            IndexDataType::Hash => b"HASH",
            IndexDataType::Json => b"JSON",
        });
    }
}

/// Generates an enum with the supported languages for search
macro_rules! search_languages {
    ($($name:ident),* $(,)?) => {
        /// Supported languages for search
        #[derive(Debug, Clone, Copy)]
        #[non_exhaustive]
        #[allow(missing_docs)]
        pub enum SearchLanguage {
            $($name),*
        }

        impl std::fmt::Display for SearchLanguage {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(SearchLanguage::$name => write!(f, "{}", stringify!($name).to_uppercase()),)*
                }
            }
        }
    };
}

search_languages!(
    Arabic, Armenian, Basque, Catalan, Danish, Dutch, English, Finnish, French, German, Greek,
    Hindi, Hungarian, Indonesian, Irish, Italian, Lithuanian, Nepali, Norwegian, Portuguese,
    Romanian, Russian, Serbian, Spanish, Swedish, Tamil, Turkish, Yiddish, Chinese,
);

impl ToRedisArgs for SearchLanguage {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.to_string().as_bytes());
    }
}

/// [Optional arguments](https://redis.io/docs/latest/commands/ft.create/#optional-arguments) for the FT.CREATE command
#[must_use = "Options have no effect unless passed to a command"]
#[derive(Default, Clone)]
#[non_exhaustive]
pub struct CreateOptions {
    on: Option<IndexDataType>,
    prefixes: Vec<String>,
    filter: Option<String>,
    language: Option<SearchLanguage>,
    language_field: Option<String>,
    score: Option<f64>,
    score_field: Option<String>,
    max_text_fields: bool,
    no_offsets: bool,
    temporary: Option<u64>,
    no_highlight: bool,
    no_fields: bool,
    no_freqs: bool,
    stopwords: Vec<String>,
    skip_initial_scan: bool,
}

impl CreateOptions {
    /// Create a new CreateOptions
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the index data type. Currently HASH (default) and JSON are supported.
    pub fn on(mut self, index_type: IndexDataType) -> Self {
        self.on = Some(index_type);
        self
    }

    /// Tell the index which keys it should index. Several prefixes can be specified.
    /// Because the argument is optional, the default is * (all keys).
    pub fn prefix<S: Into<String>>(mut self, prefix: S) -> Self {
        self.prefixes.push(prefix.into());
        self
    }

    /// A filter expression with the full RediSearch aggregation expression language.
    /// It is possible to use @__key to access the key that was just added/changed.
    pub fn filter<S: Into<String>>(mut self, filter: S) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Indicate the default language for documents in the index.
    /// The default is English.
    pub fn language(mut self, language: SearchLanguage) -> Self {
        self.language = Some(language);
        self
    }

    /// Specify a document attribute that is set as the document language.
    pub fn language_field<S: Into<String>>(mut self, language_field: S) -> Self {
        self.language_field = Some(language_field.into());
        self
    }

    /// Specify a default score for documents in the index.
    /// The default is 1.0.
    pub fn score(mut self, score: f64) -> Self {
        self.score = Some(score);
        self
    }

    /// Specify a document attribute that is set as the document's rank.
    /// Ranking must be between 0.0 and 1.0. If not set, the default score is 1.
    pub fn score_field<S: Into<String>>(mut self, score_field: S) -> Self {
        self.score_field = Some(score_field.into());
        self
    }

    /// Force RediSearch to encode indices as if there were more than 32 text attributes.
    /// This allows additional attributes (beyond 32) to be added using FT.ALTER.
    /// For efficiency, RediSearch encodes indices differently if they are created with less than 32 text attributes.
    pub fn max_text_fields(mut self) -> Self {
        self.max_text_fields = true;
        self
    }

    /// Do not store term offsets for documents. It saves memory, but does not allow exact searches or highlighting.
    /// It implies NOHL.
    pub fn no_offsets(mut self) -> Self {
        self.no_offsets = true;
        self
    }

    /// Create a lightweight temporary index that expires after a specified period of inactivity (in seconds).
    /// The internal idle timer is reset whenever the index is searched or added to.
    pub fn temporary(mut self, secs: u64) -> Self {
        self.temporary = Some(secs);
        self
    }

    /// Conserve storage space and memory by disabling highlighting support.
    /// If set, the corresponding byte offsets for term positions are not stored.
    /// NOHL is also implied by NOOFFSETS.
    pub fn no_highlight(mut self) -> Self {
        self.no_highlight = true;
        self
    }

    /// Do not store attribute bits for each term. It saves memory, but it does not allow filtering by specific attributes.
    pub fn no_fields(mut self) -> Self {
        self.no_fields = true;
        self
    }

    /// Avoid saving the term frequencies in the index. It saves memory, but does not allow sorting based on the frequencies of a given term within the document.
    pub fn no_freqs(mut self) -> Self {
        self.no_freqs = true;
        self
    }

    /// Set the index with a custom stopword list, to be ignored during indexing and search time.
    pub fn stopword<S: Into<String>>(mut self, stopword: S) -> Self {
        self.stopwords.push(stopword.into());
        self
    }

    /// Do not scan and index.
    pub fn skip_initial_scan(mut self) -> Self {
        self.skip_initial_scan = true;
        self
    }
}

impl ToRedisArgs for CreateOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref on) = self.on {
            out.write_arg(b"ON");
            on.write_redis_args(out);
        }

        if !self.prefixes.is_empty() {
            out.write_arg(b"PREFIX");
            self.prefixes.len().write_redis_args(out);
            for prefix in &self.prefixes {
                prefix.write_redis_args(out);
            }
        }

        if let Some(ref filter) = self.filter {
            out.write_arg(b"FILTER");
            format!("'{}'", filter).write_redis_args(out);
        }

        if let Some(ref language) = self.language {
            out.write_arg(b"LANGUAGE");
            language.write_redis_args(out);
        }

        if let Some(ref language_field) = self.language_field {
            out.write_arg(b"LANGUAGE_FIELD");
            language_field.write_redis_args(out);
        }

        if let Some(score) = self.score {
            out.write_arg(b"SCORE");
            score.write_redis_args(out);
        }

        if let Some(ref score_field) = self.score_field {
            out.write_arg(b"SCORE_FIELD");
            score_field.write_redis_args(out);
        }

        if self.max_text_fields {
            out.write_arg(b"MAXTEXTFIELDS");
        }

        if self.no_offsets {
            out.write_arg(b"NOOFFSETS");
        }

        if let Some(temporary) = self.temporary {
            out.write_arg(b"TEMPORARY");
            temporary.write_redis_args(out);
        }

        if self.no_highlight {
            out.write_arg(b"NOHL");
        }

        if self.no_fields {
            out.write_arg(b"NOFIELDS");
        }

        if self.no_freqs {
            out.write_arg(b"NOFREQS");
        }

        if !self.stopwords.is_empty() {
            out.write_arg(b"STOPWORDS");
            self.stopwords.len().write_redis_args(out);
            for stopword in &self.stopwords {
                stopword.write_redis_args(out);
            }
        }

        if self.skip_initial_scan {
            out.write_arg(b"SKIPINITIALSCAN");
        }
    }
}

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
            FieldType::Text => b"TEXT",
            FieldType::Tag => b"TAG",
            FieldType::Numeric => b"NUMERIC",
            FieldType::Geo => b"GEO",
            FieldType::Vector => b"VECTOR",
            FieldType::GeoShape => b"GEOSHAPE",
        });
    }
}

impl FieldType {
    /// Returns whether the field type is sortable.
    pub fn is_sortable(&self) -> bool {
        matches!(
            self,
            FieldType::Text | FieldType::Tag | FieldType::Numeric | FieldType::Geo
        )
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
            Sortable::Yes => out.write_arg(b"SORTABLE"),
            Sortable::Unf => {
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
    DmEn,
    /// Double metaphone for French
    DmFr,
    /// Double metaphone for Portuguese
    DmPt,
    /// Double metaphone for Spanish
    DmEs,
}

impl ToRedisArgs for Phonetic {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            Phonetic::DmEn => b"dm:en",
            Phonetic::DmFr => b"dm:fr",
            Phonetic::DmPt => b"dm:pt",
            Phonetic::DmEs => b"dm:es",
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

#[derive(Debug, Clone)]
#[non_exhaustive]
pub(crate) enum VectorAlgorithm {
    Flat,
    Hnsw,
    Vamana,
}

impl ToRedisArgs for VectorAlgorithm {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            VectorAlgorithm::Flat => b"FLAT",
            VectorAlgorithm::Hnsw => b"HNSW",
            VectorAlgorithm::Vamana => b"SVS-VAMANA",
        });
    }
}

/// Vector type for vector fields
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum VectorType {
    Float32,
    Float64,
    BFloat16,
    Float16,
    Int8,
    UInt8,
}

impl ToRedisArgs for VectorType {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            VectorType::Float32 => b"FLOAT32",
            VectorType::Float64 => b"FLOAT64",
            VectorType::BFloat16 => b"BFLOAT16",
            VectorType::Float16 => b"FLOAT16",
            VectorType::Int8 => b"INT8",
            VectorType::UInt8 => b"UINT8",
        });
    }
}

/// Vector types supported by the VAMANA algorithm.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum VamanaVectorType {
    Float16,
    Float32,
}

impl From<VamanaVectorType> for VectorType {
    fn from(vt: VamanaVectorType) -> Self {
        match vt {
            VamanaVectorType::Float16 => VectorType::Float16,
            VamanaVectorType::Float32 => VectorType::Float32,
        }
    }
}

/// [Distance metric](https://redis.io/docs/latest/develop/ai/search-and-query/vectors/#distance-metrics/) for vector fields
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
pub enum DistanceMetric {
    /// Euclidean distance between two vectors.
    L2,
    /// Inner product of two vectors.
    IP,
    /// Cosine distance of two vectors.
    Cosine,
}

impl ToRedisArgs for DistanceMetric {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            DistanceMetric::L2 => b"L2",
            DistanceMetric::IP => b"IP",
            DistanceMetric::Cosine => b"COSINE",
        });
    }
}

/// Compression algorithm for VAMANA vector indexes.
/// <https://redis.io/docs/latest/develop/ai/search-and-query/vectors/svs-compression/>
///
/// Note: Intel's proprietary LVQ and LeanVec optimizations are not available in Redis Open Source.
/// On non-Intel platforms, these will fall back to basic 8-bit scalar quantization.
#[derive(Debug, Copy, Clone)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum CompressionType {
    LVQ8,
    LVQ4,
    LVQ4x4,
    LVQ4x8,
    LeanVec4x8,
    LeanVec8x8,
}

impl ToRedisArgs for CompressionType {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(match self {
            CompressionType::LVQ8 => b"LVQ8",
            CompressionType::LVQ4 => b"LVQ4",
            CompressionType::LVQ4x4 => b"LVQ4x4",
            CompressionType::LVQ4x8 => b"LVQ4x8",
            CompressionType::LeanVec4x8 => b"LeanVec4x8",
            CompressionType::LeanVec8x8 => b"LeanVec8x8",
        })
    }
}

/// Represents a vector field in the schema.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SchemaVectorField {
    base: BaseSchemaField,
    algorithm: VectorAlgorithm,
    vector_type: VectorType,
    dim: u32,
    distance_metric: DistanceMetric,
}

impl ToRedisArgs for SchemaVectorField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(alias) = &self.base.alias {
            out.write_arg(b"AS");
            alias.write_redis_args(out);
        }

        self.base.field_type.write_redis_args(out);

        self.algorithm.write_redis_args(out);
        // Note: The attribute count will be written by the VectorField implementation
        // which knows about both base and algorithm-specific attributes
        // That is:
        /*
            out.write_arg(b"TYPE");
            self.vector_type.write_redis_args(out);
            out.write_arg(b"DIM");
            self.dim.write_redis_args(out);
            out.write_arg(b"DISTANCE_METRIC");
            self.distance_metric.write_redis_args(out);
        */
    }

    fn num_of_args(&self) -> usize {
        // Count the number of attribute pairs (key-value pairs) for this vector field.
        // Base attributes are: TYPE, DIM, DISTANCE_METRIC (3 pairs = 6 args)
        6
    }
}

/// Options for vectors using the FLAT indexing algorithm
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FlatVectorOptions {
    block_size: Option<u32>,
}

impl ToRedisArgs for FlatVectorOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(block_size) = self.block_size {
            out.write_arg(b"BLOCK_SIZE");
            block_size.write_redis_args(out);
        }
    }

    fn num_of_args(&self) -> usize {
        let mut count = 0;
        if self.block_size.is_some() {
            count += 2;
        }
        count
    }
}

/// Options for vectors using the HNSW indexing algorithm
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HnswVectorOptions {
    m: Option<u32>,
    ef_construction: Option<u32>,
    ef_runtime: Option<u32>,
    epsilon: Option<f64>,
}

impl ToRedisArgs for HnswVectorOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(m) = self.m {
            out.write_arg(b"M");
            m.write_redis_args(out);
        }
        if let Some(ef_construction) = self.ef_construction {
            out.write_arg(b"EF_CONSTRUCTION");
            ef_construction.write_redis_args(out);
        }
        if let Some(ef_runtime) = self.ef_runtime {
            out.write_arg(b"EF_RUNTIME");
            ef_runtime.write_redis_args(out);
        }
        if let Some(epsilon) = self.epsilon {
            out.write_arg(b"EPSILON");
            epsilon.write_redis_args(out);
        }
    }

    fn num_of_args(&self) -> usize {
        let mut count = 0;
        if self.m.is_some() {
            count += 2;
        }
        if self.ef_construction.is_some() {
            count += 2;
        }
        if self.ef_runtime.is_some() {
            count += 2;
        }
        if self.epsilon.is_some() {
            count += 2;
        }
        count
    }
}

/// Options for vectors using the VAMANA indexing algorithm
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct VamanaVectorOptions {
    compression: Option<CompressionType>,
    construction_window_size: Option<u32>,
    graph_max_degree: Option<u32>,
    search_window_size: Option<u32>,
    epsilon: Option<f64>,
    training_threshold: Option<u32>,
    reduce: Option<u32>,
}

impl ToRedisArgs for VamanaVectorOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(compression) = &self.compression {
            out.write_arg(b"COMPRESSION");
            compression.write_redis_args(out);
        }
        if let Some(construction_window_size) = self.construction_window_size {
            out.write_arg(b"CONSTRUCTION_WINDOW_SIZE");
            construction_window_size.write_redis_args(out);
        }
        if let Some(graph_max_degree) = self.graph_max_degree {
            out.write_arg(b"GRAPH_MAX_DEGREE");
            graph_max_degree.write_redis_args(out);
        }
        if let Some(search_window_size) = self.search_window_size {
            out.write_arg(b"SEARCH_WINDOW_SIZE");
            search_window_size.write_redis_args(out);
        }
        if let Some(epsilon) = self.epsilon {
            out.write_arg(b"EPSILON");
            epsilon.write_redis_args(out);
        }
        if let Some(training_threshold) = self.training_threshold {
            out.write_arg(b"TRAINING_THRESHOLD");
            training_threshold.write_redis_args(out);
        }
        if let Some(reduce) = self.reduce {
            out.write_arg(b"REDUCE");
            reduce.write_redis_args(out);
        }
    }

    fn num_of_args(&self) -> usize {
        let mut count = 0;
        if self.compression.is_some() {
            count += 2;
        }
        if self.construction_window_size.is_some() {
            count += 2;
        }
        if self.graph_max_degree.is_some() {
            count += 2;
        }
        if self.search_window_size.is_some() {
            count += 2;
        }
        if self.epsilon.is_some() {
            count += 2;
        }
        if self.training_threshold.is_some() {
            count += 2;
        }
        if self.reduce.is_some() {
            count += 2;
        }
        count
    }
}

/// Vector field definition for semantic similarity search.
///
/// Represents a vector field in a RediSearch index with one of three supported indexing algorithms.
/// Each algorithm offers different trade-offs between accuracy, performance, and memory usage.
///
/// # Algorithms
///
/// - **FLAT**: Brute-force exact search. Best for small datasets (< 1M vectors) where perfect accuracy is required.
/// - **HNSW**: Hierarchical Navigable Small World graph-based approximate search. Best for large datasets (> 1M vectors)
///   where search performance and scalability are more important than perfect accuracy.
/// - **SVS-VAMANA**: Intel's Scalable Vector Search with graph-based approximate search and compression support.
///   Best when you need high performance with reduced memory usage, especially on Intel hardware.
///   More information at: <https://intel.github.io/ScalableVectorSearch/intro.html>
///
/// # Examples
///
/// ```rust
/// use redis::search::*;
///
/// // FLAT index for exact search
/// let flat_field = VectorField::flat(VectorType::Float32, 128, DistanceMetric::Cosine)
///     .block_size(1000)
///     .build();
///
/// // HNSW index for approximate search
/// let hnsw_field = VectorField::hnsw(VectorType::Float32, 128, DistanceMetric::Cosine)
///     .m(16)
///     .ef_construction(200)
///     .build();
///
/// // VAMANA index with compression (note: uses VamanaVectorType for type safety)
/// let vamana_field = VectorField::vamana(VamanaVectorType::Float32, 128, DistanceMetric::Cosine)
///     .compression(CompressionType::LVQ8)
///     .graph_max_degree(64)
///     .build();
/// ```
#[must_use = "Vector field has no effect unless inserted into a schema"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum VectorField {
    /// FLAT (brute-force) vector index for exact nearest neighbor search.
    /// Best for small datasets (< 1M vectors) where perfect accuracy is required.
    Flat(SchemaVectorField, FlatVectorOptions),

    /// HNSW (Hierarchical Navigable Small World) vector index for approximate nearest neighbor search.
    /// Best for large datasets (> 1M vectors) where performance is more important than perfect accuracy.
    Hnsw(SchemaVectorField, HnswVectorOptions),

    /// SVS-VAMANA vector index with compression support for memory-efficient approximate search.
    /// Best when you need high performance with reduced memory usage, especially on Intel hardware.
    Vamana(SchemaVectorField, VamanaVectorOptions),
}

impl VectorField {
    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        match self {
            VectorField::Flat(ref mut base, _)
            | VectorField::Hnsw(ref mut base, _)
            | VectorField::Vamana(ref mut base, _) => base.base = base.base.clone().alias(alias),
        };
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        match self {
            VectorField::Flat(ref mut base, _)
            | VectorField::Hnsw(ref mut base, _)
            | VectorField::Vamana(ref mut base, _) => {
                base.base = base.base.clone().index_missing(index_missing)
            }
        };
        self
    }
}

impl ToRedisArgs for VectorField {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let base = match self {
            VectorField::Flat(base, _)
            | VectorField::Hnsw(base, _)
            | VectorField::Vamana(base, _) => base,
        };
        base.write_redis_args(out);

        let attributes_count = match self {
            VectorField::Flat(base, flat_vector_options) => {
                base.num_of_args() + flat_vector_options.num_of_args()
            }
            VectorField::Hnsw(base, hnsw_vector_options) => {
                base.num_of_args() + hnsw_vector_options.num_of_args()
            }
            VectorField::Vamana(base, vamana_vector_options) => {
                base.num_of_args() + vamana_vector_options.num_of_args()
            }
        };
        attributes_count.write_redis_args(out);

        out.write_arg(b"TYPE");
        base.vector_type.write_redis_args(out);
        out.write_arg(b"DIM");
        base.dim.write_redis_args(out);
        out.write_arg(b"DISTANCE_METRIC");
        base.distance_metric.write_redis_args(out);

        // Write algorithm-specific attributes
        match self {
            VectorField::Flat(_, flat_vector_options) => {
                flat_vector_options.write_redis_args(out);
            }
            VectorField::Hnsw(_, hnsw_vector_options) => {
                hnsw_vector_options.write_redis_args(out);
            }
            VectorField::Vamana(_, vamana_vector_options) => {
                vamana_vector_options.write_redis_args(out);
            }
        }

        if base.base.index_missing {
            out.write_arg(b"INDEXMISSING");
        }
    }
}

impl VectorField {
    /// Create a new FLAT vector field
    pub fn flat(
        vector_type: VectorType,
        dim: u32,
        distance_metric: DistanceMetric,
    ) -> FlatVectorFieldBuilder {
        assert!(
            dim > 0,
            "Vector dimension must be positive (greater than 0)"
        );

        FlatVectorFieldBuilder {
            base: SchemaVectorField {
                base: BaseSchemaField::new(FieldType::Vector),
                algorithm: VectorAlgorithm::Flat,
                vector_type,
                dim,
                distance_metric,
            },
            block_size: None,
        }
    }

    /// Create a new HNSW vector field
    pub fn hnsw(
        vector_type: VectorType,
        dim: u32,
        distance_metric: DistanceMetric,
    ) -> HnswVectorFieldBuilder {
        assert!(
            dim > 0,
            "Vector dimension must be positive (greater than 0)"
        );

        HnswVectorFieldBuilder {
            base: SchemaVectorField {
                base: BaseSchemaField::new(FieldType::Vector),
                algorithm: VectorAlgorithm::Hnsw,
                vector_type,
                dim,
                distance_metric,
            },
            m: None,
            ef_construction: None,
            ef_runtime: None,
            epsilon: None,
        }
    }

    /// Create a new VAMANA vector field
    pub fn vamana(
        vector_type: VamanaVectorType,
        dim: u32,
        distance_metric: DistanceMetric,
    ) -> VamanaVectorFieldBuilder {
        assert!(
            dim > 0,
            "Vector dimension must be positive (greater than 0)"
        );

        VamanaVectorFieldBuilder {
            base: SchemaVectorField {
                base: BaseSchemaField::new(FieldType::Vector),
                algorithm: VectorAlgorithm::Vamana,
                vector_type: vector_type.into(),
                dim,
                distance_metric,
            },
            compression: None,
            construction_window_size: None,
            graph_max_degree: None,
            search_window_size: None,
            epsilon: None,
            training_threshold: None,
            reduce: None,
        }
    }
}

/// Builder for FLAT vector fields
#[must_use = "The builder has no effect until .build() is called"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FlatVectorFieldBuilder {
    base: SchemaVectorField,
    block_size: Option<u32>,
}

impl FlatVectorFieldBuilder {
    /// Sets the block size for the FLAT index.
    ///
    /// The block size determines how vectors are organized in memory. BLOCK_SIZE amount of vectors are stored in a contiguous array.
    /// This is useful when the index is dynamic with respect to addition and deletion.
    /// The default block size is 1024.
    pub fn block_size(mut self, block_size: u32) -> Self {
        self.block_size = Some(block_size);
        self
    }

    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.base.base = self.base.base.alias(alias);
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        self.base.base = self.base.base.index_missing(index_missing);
        self
    }

    /// Build the vector field.
    pub fn build(self) -> VectorField {
        VectorField::Flat(
            self.base,
            FlatVectorOptions {
                block_size: self.block_size,
            },
        )
    }
}

/// Builder for HNSW vector fields
#[must_use = "The builder has no effect until .build() is called"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HnswVectorFieldBuilder {
    base: SchemaVectorField,
    m: Option<u32>,
    ef_construction: Option<u32>,
    ef_runtime: Option<u32>,
    epsilon: Option<f64>,
}

impl HnswVectorFieldBuilder {
    /// Max number of outgoing edges (connections) for each node in a graph layer. On layer zero, the max number of connections will be 2 * M.
    /// Higher values increase accuracy, but also increase memory usage and index build time.
    /// The default is 16.
    pub fn m(mut self, m: u32) -> Self {
        self.m = Some(m);
        self
    }

    /// Max number of connected neighbors to consider during graph building.
    /// Higher values increase accuracy, but also increase index build time.
    /// The default is 200.
    pub fn ef_construction(mut self, ef_construction: u32) -> Self {
        self.ef_construction = Some(ef_construction);
        self
    }

    /// Max top candidates during KNN search. Higher values increase accuracy, but also increase search latency.
    /// The default is 10.
    pub fn ef_runtime(mut self, ef_runtime: u32) -> Self {
        self.ef_runtime = Some(ef_runtime);
        self
    }

    /// Relative factor that sets the boundaries in which a range query may search for candidates.
    /// That is, vector candidates whose distance from the query vector is radius * (1 + EPSILON) are potentially scanned,
    /// allowing more extensive search and more accurate results (on the expense of runtime).
    /// The default is 0.01.
    pub fn epsilon(mut self, epsilon: f64) -> Self {
        self.epsilon = Some(epsilon);
        self
    }

    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.base.base = self.base.base.alias(alias);
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        self.base.base = self.base.base.index_missing(index_missing);
        self
    }

    /// Build the vector field.
    pub fn build(self) -> VectorField {
        VectorField::Hnsw(
            self.base,
            HnswVectorOptions {
                m: self.m,
                ef_construction: self.ef_construction,
                ef_runtime: self.ef_runtime,
                epsilon: self.epsilon,
            },
        )
    }
}

const DEFAULT_BLOCK_SIZE: u32 = 1024;
/// Maximum value for `training_threshold` parameter (102,400)
pub const MAX_TRAINING_THRESHOLD: u32 = 100 * DEFAULT_BLOCK_SIZE;

/// Builder for VAMANA vector fields
#[must_use = "The builder has no effect until .build() is called"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct VamanaVectorFieldBuilder {
    base: SchemaVectorField,
    compression: Option<CompressionType>,
    construction_window_size: Option<u32>,
    graph_max_degree: Option<u32>,
    search_window_size: Option<u32>,
    epsilon: Option<f64>,
    training_threshold: Option<u32>,
    reduce: Option<u32>,
}

impl VamanaVectorFieldBuilder {
    /// Set the compression algorithm for the VAMANA index.
    ///
    /// Compression reduces memory usage at the cost of some accuracy.
    ///
    /// Note: Intel's proprietary LVQ and LeanVec optimizations are not available in Redis Open Source.
    /// On non-Intel platforms, this will fall back to basic 8-bit scalar quantization.
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.compression = Some(compression);
        self
    }

    /// The search window size to use during graph construction.
    /// A higher search window size will yield a higher quality graph since more overall vertexes are considered, but will increase construction time.
    /// The default is 200.
    pub fn construction_window_size(mut self, construction_window_size: u32) -> Self {
        self.construction_window_size = Some(construction_window_size);
        self
    }

    /// Sets the maximum number of edges per node; equivalent to HNSW’s M*2.
    /// A higher max degree may yield a higher quality graph in terms of recall for performance, but the memory footprint of the graph is directly proportional to the maximum degree.
    /// The default is 32.
    pub fn graph_max_degree(mut self, graph_max_degree: u32) -> Self {
        self.graph_max_degree = Some(graph_max_degree);
        self
    }

    /// The size of the search window; the same as HSNW's EF_RUNTIME (Max top candidates during KNN search).
    /// Increasing the search window size and capacity generally yields more accurate but slower search results.
    /// The default is 10.
    pub fn search_window_size(mut self, search_window_size: u32) -> Self {
        self.search_window_size = Some(search_window_size);
        self
    }

    /// The range search approximation factor; the same as HSNW's EPSILON.
    /// The default is 0.01.
    pub fn epsilon(mut self, epsilon: f64) -> Self {
        self.epsilon = Some(epsilon);
        self
    }

    /// Number of vectors needed to learn compression parameters.
    /// Applicable only when used with COMPRESSION. Increase if recall is low.
    /// Note: setting this too high may slow down search. If a value is provided, it must be less than 100 * DEFAULT_BLOCK_SIZE, where DEFAULT_BLOCK_SIZE is 1024.
    /// The default is 10 * DEFAULT_BLOCK_SIZE.
    pub fn training_threshold(mut self, training_threshold: u32) -> Self {
        if self.compression.is_some() {
            let clamped = std::cmp::min(training_threshold, MAX_TRAINING_THRESHOLD);
            if clamped != training_threshold {
                warn!(
                    "training_threshold exceeded the maximum allowed value; clamped from {} to {}.",
                    training_threshold, clamped
                );
            }
            self.training_threshold = Some(clamped);
        } else {
            warn!("training_threshold ignored: applies only when compression is enabled.");
        }
        self
    }

    /// The dimension used when using LeanVec4x8 or LeanVec8x8 compression for dimensionality reduction.
    /// If a value is provided, it should be less than DIM. Lowering it can speed up search and reduce memory use.
    /// The default is DIM / 2.
    pub fn reduce(mut self, reduce: u32) -> Self {
        if self
            .compression
            .is_some_and(|c| matches!(c, CompressionType::LeanVec4x8 | CompressionType::LeanVec8x8))
        {
            let max_reduce = self.base.dim.saturating_sub(1).max(1);
            let clamped = std::cmp::min(reduce, max_reduce).max(1);
            if clamped != reduce {
                warn!(
                    "reduce value {} out of valid range 1..={}; clamped to {}.",
                    reduce, max_reduce, clamped
                );
            }
            self.reduce = Some(clamped);
        } else {
            warn!("reduce ignored: applies only to LeanVec4x8 and LeanVec8x8 compression types.");
        }
        self
    }

    /// Set the alias for the field.
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.base.base = self.base.base.alias(alias);
        self
    }

    /// Set index missing. This allows searching for missing values - documents that do not contain a specific field.
    pub fn index_missing(mut self, index_missing: bool) -> Self {
        self.base.base = self.base.base.index_missing(index_missing);
        self
    }

    /// Build the vector field.
    pub fn build(self) -> VectorField {
        VectorField::Vamana(
            self.base,
            VamanaVectorOptions {
                compression: self.compression,
                construction_window_size: self.construction_window_size,
                graph_max_degree: self.graph_max_degree,
                search_window_size: self.search_window_size,
                epsilon: self.epsilon,
                training_threshold: self.training_threshold,
                reduce: self.reduce,
            },
        )
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
            CoordSystem::Spherical => b"SPHERICAL",
            CoordSystem::Flat => b"FLAT",
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
    /// Vector field
    Vector(VectorField),
    /// Geo shape field
    GeoShape(SchemaGeoShapeField),
    /// Simple type
    JustType(FieldType),
}

impl ToRedisArgs for FieldDefinition {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            FieldDefinition::Text(tf) => tf.write_redis_args(out),
            FieldDefinition::Numeric(nf) => nf.write_redis_args(out),
            FieldDefinition::Geo(gf) => gf.write_redis_args(out),
            FieldDefinition::Tag(tf) => tf.write_redis_args(out),
            FieldDefinition::Vector(v) => v.write_redis_args(out),
            FieldDefinition::GeoShape(gs) => gs.write_redis_args(out),
            FieldDefinition::JustType(t) => t.write_redis_args(out),
        }
    }
}

impl From<SchemaTextField> for FieldDefinition {
    fn from(field: SchemaTextField) -> Self {
        FieldDefinition::Text(field)
    }
}

impl From<SchemaNumericField> for FieldDefinition {
    fn from(field: SchemaNumericField) -> Self {
        FieldDefinition::Numeric(field)
    }
}

impl From<SchemaGeoField> for FieldDefinition {
    fn from(field: SchemaGeoField) -> Self {
        FieldDefinition::Geo(field)
    }
}

impl From<SchemaTagField> for FieldDefinition {
    fn from(field: SchemaTagField) -> Self {
        FieldDefinition::Tag(field)
    }
}

impl From<VectorField> for FieldDefinition {
    fn from(field: VectorField) -> Self {
        FieldDefinition::Vector(field)
    }
}

impl From<SchemaGeoShapeField> for FieldDefinition {
    fn from(field: SchemaGeoShapeField) -> Self {
        FieldDefinition::GeoShape(field)
    }
}

impl From<FieldType> for FieldDefinition {
    fn from(field_type: FieldType) -> Self {
        FieldDefinition::JustType(field_type)
    }
}

/// The RediSearch schema declaring which fields to index.
#[must_use = "Schema has no effect unless passed to a command"]
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RediSearchSchema(Vec<(String, FieldDefinition)>);

impl RediSearchSchema {
    /// Create a new schema.
    pub fn new() -> Self {
        RediSearchSchema(Vec::new())
    }

    /// Insert a new field into the schema.
    pub fn insert<K: Into<String>, V: Into<FieldDefinition>>(&mut self, key: K, value: V) {
        self.0.push((key.into(), value.into()));
    }

    /// Returns whether the schema is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl ToRedisArgs for RediSearchSchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        for (key, field) in &self.0 {
            key.write_redis_args(out);
            field.write_redis_args(out);
        }
    }
}

impl Default for RediSearchSchema {
    fn default() -> Self {
        Self::new()
    }
}

/// Allows schemas to be created in a more concise way.
#[macro_export]
macro_rules! schema {
    ($($key:expr => $value:expr),* $(,)?) => {{
        let mut schema = $crate::search::RediSearchSchema::new();
        $(
            schema.insert($key, $value);
        )*
        schema
    }};
}
