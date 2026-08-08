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
//! SearchSchema<Empty> ──insert()──> SearchSchema<NonEmpty>
//!       │
//!       │ every type along the way implements ToRedisArgs
//!       ▼
//! FtCreateCommand<NoSchema> ──schema()──> FtCreateCommand<SearchSchema<NonEmpty>>
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
//!     "subtitle" => SchemaTextField::new()
//! };
//!
//! // Create an FT.CREATE command
//! let ft_create = FtCreateCommand::new("index")
//!     .options(
//!         CreateOptions::new()
//!             .on(IndexDataType::Hash)
//!             .prefix("doc:")
//!     )
//!     .schema(schema);
//! ```
mod fields;
mod options;
mod schema;

pub use fields::*;
pub use options::*;
pub use schema::*;

use crate::Cmd;

/// Marker type indicating no schema has been set yet.
pub struct NoSchema;

/// FT.CREATE command builder.
///
/// Uses the typestate pattern to enforce at compile time that a schema
/// is set before the command can be built. The schema state is encoded
/// directly in the generic parameter: before a schema is set the builder
/// holds a [`NoSchema`] marker, and after the schema is set it holds the
/// [`SearchSchema<NonEmpty>`] itself.
///
/// # Type States
/// - `FtCreateCommand<NoSchema>` - No schema set yet, `into_cmd()` not available
/// - `FtCreateCommand<SearchSchema<NonEmpty>>` - Schema set, `into_cmd()` available
///
/// # Example
/// ```rust
/// use redis::{schema, search::*};
///
/// let cmd = FtCreateCommand::new("my_index")
///     .options(CreateOptions::new().on(IndexDataType::Hash))
///     .schema(schema! { "title" => SchemaTextField::new() })
///     .into_cmd();
/// ```
pub struct FtCreateCommand<S = NoSchema> {
    index: String,
    options: CreateOptions,
    schema: S,
}

impl FtCreateCommand<NoSchema> {
    /// Create a new FT.CREATE command for the given index
    pub fn new<S: Into<String>>(index: S) -> Self {
        Self {
            index: index.into(),
            options: CreateOptions::default(),
            schema: NoSchema,
        }
    }

    /// Set the options for the command
    pub fn options(mut self, options: CreateOptions) -> Self {
        self.options = options;
        self
    }

    /// Set the schema for the command.
    ///
    /// The schema must be non-empty (contain at least one field).
    /// This is enforced at compile time by the type system.
    ///
    /// This transitions the builder from `FtCreateCommand<NoSchema>` to
    /// `FtCreateCommand<SearchSchema<NonEmpty>>`, making `into_cmd()` available.
    pub fn schema(self, schema: SearchSchema<NonEmpty>) -> FtCreateCommand<SearchSchema<NonEmpty>> {
        FtCreateCommand {
            index: self.index,
            options: self.options,
            schema,
        }
    }
}

impl FtCreateCommand<SearchSchema<NonEmpty>> {
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
        let ft_create = FtCreateCommand::new("").schema(schema! {
            TEXT_FIELD_NAME => SchemaTextField::new()
        });
        assert_eq!(ft_create.into_args(), "FT.CREATE  SCHEMA title TEXT");
    }

    #[test]
    fn test_options_can_be_set_after_the_schema() {
        let ft_create = FtCreateCommand::new(INDEX_NAME)
            .schema(schema! {
                TEXT_FIELD_NAME => SchemaTextField::new()
            })
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
    fn test_index_with_filter() {
        // In this example, keys for author data use the key pattern author:details:<id> while keys for book data use the pattern book:details:<id>.

        /*
        Index authors whose names start with G.
        FT.CREATE g-authors-idx ON HASH PREFIX 1 author:details FILTER 'startswith(@name, "G")' SCHEMA name TEXT
        */
        let ft_create = FtCreateCommand::new("g-authors-idx")
            .options(
                CreateOptions::new()
                    .on(IndexDataType::Hash)
                    .prefix("author:details")
                    .filter("startswith(@name, \"G\")"),
            )
            .schema(schema! {
                "name" =>  SchemaTextField::new(),
            });

        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE g-authors-idx ON HASH PREFIX 1 author:details FILTER 'startswith(@name, \"G\")' SCHEMA name TEXT"
        );

        /*
        Index only books that have a subtitle.
        FT.CREATE subtitled-books-idx ON HASH PREFIX 1 book:details FILTER '@subtitle != ""' SCHEMA title TEXT
        */
        let ft_create = FtCreateCommand::new("subtitled-books-idx")
            .options(
                CreateOptions::new()
                    .on(IndexDataType::Hash)
                    .prefix("book:details")
                    .filter("@subtitle != \"\""),
            )
            .schema(schema! {
                "title" =>  SchemaTextField::new(),
            });

        assert_eq!(
            ft_create.into_args(),
            "FT.CREATE subtitled-books-idx ON HASH PREFIX 1 book:details FILTER '@subtitle != \"\"' SCHEMA title TEXT"
        );
    }
}
