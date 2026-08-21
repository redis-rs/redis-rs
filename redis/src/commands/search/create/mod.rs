//! Provides a type-safe way to generate [FT.CREATE](https://redis.io/docs/latest/commands/ft.create/) commands programmatically.
//!
//! The pieces fit together as follows:
//!
//! ```text
//! SchemaTextField (and the other field builders)
//!       │ into()
//!       ▼
//! FieldDefinition
//!       │ inserted by the schema! macro or by insert()
//!       ▼
//! SearchSchema (always holds at least one field)
//!       │
//!       │ every type along the way implements ToRedisArgs
//!       ▼
//! FtCreateCommand
//!       │
//!       └── into_cmd() yields a redis::Cmd
//! ```
//!
//! # Examples
//!
//! ```rust
//! use redis::{schema, search::*};
//!
//! // Build a schema using the schema! macro
//! let schema = schema! {
//!     "title" => SchemaTextField::new().weight(2.0),
//!     "price" => SchemaNumericField::new(),
//!     "condition" => SchemaTagField::new().separator(',')
//! };
//!
//! // Create an FT.CREATE command
//! let ft_create = FtCreateCommand::new("index", schema)
//!     .options(
//!         CreateOptions::new()
//!             .on(IndexDataType::Hash)
//!             .prefix("doc:")
//!     );
//! ```
mod fields;
mod options;
mod schema;

pub use fields::*;
pub use options::*;
pub use schema::*;

use crate::Cmd;

/// FT.CREATE command builder.
///
/// The schema is required by `FT.CREATE`, so [`FtCreateCommand::new`] takes it
/// as a mandatory argument rather than exposing it through a builder method.
/// Together with [`SearchSchema`] requiring at least one field, this makes the
/// server's requirements compile-time guarantees: the schema is always present
/// and never empty.
///
/// # Example
/// ```rust
/// use redis::{schema, search::*};
///
/// let cmd = FtCreateCommand::new("my_index", schema! { "title" => SchemaTextField::new() })
///     .options(CreateOptions::new().on(IndexDataType::Hash))
///     .into_cmd();
/// ```
pub struct FtCreateCommand {
    index: String,
    options: CreateOptions,
    schema: SearchSchema,
}

impl FtCreateCommand {
    /// Create a new FT.CREATE command for the given index and schema.
    pub fn new<S: Into<String>>(index: S, schema: SearchSchema) -> Self {
        Self {
            index: index.into(),
            options: CreateOptions::default(),
            schema,
        }
    }

    /// Set the options for the command
    pub fn options(mut self, options: CreateOptions) -> Self {
        self.options = options;
        self
    }

    /// Consume the builder and convert it into a `redis::Cmd`.
    pub fn into_cmd(self) -> Cmd {
        let mut cmd = crate::cmd("FT.CREATE");
        cmd.arg(&self.index);
        cmd.arg(&self.options);
        cmd.arg("SCHEMA");
        cmd.arg(self.schema);

        cmd
    }

    /// Consume the builder and convert it into a string for testing purposes.
    #[cfg(test)]
    pub(crate) fn into_args(self) -> String {
        use crate::cmd::Arg;
        self.into_cmd()
            .args_iter()
            .map(|arg| match arg {
                Arg::Simple(bytes) => bytes.to_vec(),
                Arg::Cursor => panic!("Cursor not expected in FT.CREATE command"),
            })
            .map(|arg| String::from_utf8_lossy(&arg).to_string())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema;

    static INDEX_NAME: &str = "index";
    static TEXT_FIELD_NAME: &str = "title";

    #[test]
    fn test_empty_index_name() {
        // Empty index names are valid in Redis
        let ft_create = FtCreateCommand::new(
            "",
            schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            },
        );
        assert_eq!(ft_create.into_args(), "FT.CREATE  SCHEMA title TEXT");
    }

    #[test]
    fn test_options_are_emitted_before_the_schema() {
        let ft_create = FtCreateCommand::new(
            INDEX_NAME,
            schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            },
        )
        .options(CreateOptions::new().on(IndexDataType::Hash));
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE index ON HASH SCHEMA title TEXT"
        );
    }

    // ============================================================================
    // Website examples
    // <https://redis.io/docs/latest/commands/ft.create/#examples>
    // ============================================================================
    #[test]
    fn test_create_blog_post_index() {
        /*
        Create an index that stores the title, publication date, and categories of blog post hashes whose keys start with blog:post: (for example, blog:post:1).
        FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA title TEXT SORTABLE published_at NUMERIC SORTABLE category TAG SORTABLE
        */
        let schema = schema! {
            "title" =>  SchemaTextField::new().sortable(Sortable::Yes),
            "published_at" => SchemaNumericField::new().sortable(Sortable::Yes),
            "category" => SchemaTagField::new().sortable(Sortable::Yes),
        };
        let options = CreateOptions::new()
            .on(IndexDataType::Hash)
            .prefix("blog:post:");

        let ft_create = FtCreateCommand::new("idx", schema).options(options);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA title TEXT SORTABLE published_at NUMERIC SORTABLE category TAG SORTABLE"
        );
    }

    #[test]
    fn test_attribute_with_alias_and_dual_index() {
        /*
        Index the sku attribute from a hash as both a TAG and as TEXT.
        FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA sku AS sku_text TEXT sku AS sku_tag TAG SORTABLE
        */
        let sku_text_field = SchemaTextField::new().alias("sku_text");

        let sku_tag_field = SchemaTagField::new()
            .alias("sku_tag")
            .sortable(Sortable::Yes);

        let schema = schema! {
            "sku" => sku_text_field,
            "sku" => sku_tag_field,
        };

        let options = CreateOptions::new()
            .on(IndexDataType::Hash)
            .prefix("blog:post:");

        let ft_create = FtCreateCommand::new("idx", schema).options(options);
        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE idx ON HASH PREFIX 1 blog:post: SCHEMA sku AS sku_text TEXT sku AS sku_tag TAG SORTABLE"
        );
    }

    #[test]
    fn test_index_two_hashes_within_the_same_index() {
        /*
        Index two different hashes, one containing author data and one containing books, in the same index.
        FT.CREATE author-books-idx ON HASH PREFIX 2 author:details: book:details: SCHEMA author_id TAG SORTABLE author_ids TAG title TEXT name TEXT
        */
        let ft_create = FtCreateCommand::new(
            "author-books-idx",
            schema! {
                "author_id" =>  SchemaTagField::new().sortable(Sortable::Yes),
                "author_ids" => SchemaTagField::new(),
                "title" => SchemaTextField::new(),
                "name" => SchemaTextField::new(),
            },
        )
        .options(
            CreateOptions::new()
                .on(IndexDataType::Hash)
                .prefix("author:details:")
                .prefix("book:details:"),
        );

        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE author-books-idx ON HASH PREFIX 2 author:details: book:details: SCHEMA author_id TAG SORTABLE author_ids TAG title TEXT name TEXT"
        );
    }

    #[test]
    fn test_index_with_filter() {
        // In this example, keys for author data use the key pattern author:details:<id> while keys for book data use the pattern book:details:<id>.

        /*
        Index authors whose names start with G.
        FT.CREATE g-authors-idx ON HASH PREFIX 1 author:details FILTER 'startswith(@name, "G")' SCHEMA name TEXT
        */
        let ft_create = FtCreateCommand::new(
            "g-authors-idx",
            schema! {
                "name" =>  SchemaTextField::new(),
            },
        )
        .options(
            CreateOptions::new()
                .on(IndexDataType::Hash)
                .prefix("author:details")
                .filter("startswith(@name, \"G\")"),
        );

        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE g-authors-idx ON HASH PREFIX 1 author:details FILTER 'startswith(@name, \"G\")' SCHEMA name TEXT"
        );

        /*
        Index only books that have a subtitle.
        FT.CREATE subtitled-books-idx ON HASH PREFIX 1 book:details FILTER '@subtitle != ""' SCHEMA title TEXT
        */
        let ft_create = FtCreateCommand::new(
            "subtitled-books-idx",
            schema! {
                "title" =>  SchemaTextField::new(),
            },
        )
        .options(
            CreateOptions::new()
                .on(IndexDataType::Hash)
                .prefix("book:details")
                .filter("@subtitle != \"\""),
        );

        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE subtitled-books-idx ON HASH PREFIX 1 book:details FILTER '@subtitle != \"\"' SCHEMA title TEXT"
        );
    }

    #[test]
    fn test_index_with_separator() {
        /*
        In this example, keys for author data use the key pattern author:details:<id> while keys for book data use the pattern book:details:<id>.
        Index books that have a "categories" attribute where each category is separated by a ; character.
        FT.CREATE books-idx ON HASH PREFIX 1 book:details SCHEMA title TEXT categories TAG SEPARATOR ;
        */
        let ft_create = FtCreateCommand::new(
            "books-idx",
            schema! {
                "title" =>  SchemaTextField::new(),
                "categories" => SchemaTagField::new().separator(';'),
            },
        )
        .options(
            CreateOptions::new()
                .on(IndexDataType::Hash)
                .prefix("book:details"),
        );

        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE books-idx ON HASH PREFIX 1 book:details SCHEMA title TEXT categories TAG SEPARATOR ;"
        );
    }

    #[test]
    fn test_index_json() {
        /*
        Index a JSON document using a JSON Path expression
        The following example uses data similar to the hash examples above but uses JSON instead.
        FT.CREATE idx ON JSON SCHEMA $.title AS title TEXT $.categories AS categories TAG
        */
        let ft_create = FtCreateCommand::new(
            "idx",
            schema! {
                "$.title" =>  SchemaTextField::new().alias("title"),
                "$.categories" => SchemaTagField::new().alias("categories"),
            },
        )
        .options(CreateOptions::new().on(IndexDataType::Json));

        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE idx ON JSON SCHEMA $.title AS title TEXT $.categories AS categories TAG"
        );
    }
}
