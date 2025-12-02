//! Provides a type-safe way to generate [FT.CREATE](https://redis.io/docs/latest/commands/ft.create/) commands programmatically.
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
//! let ft_create = FtCreateCommand::new("index")
//!     .options(
//!         CreateOptions::new()
//!             .on(IndexDataType::Hash)
//!             .prefix("doc:")
//!     )
//!     .schema(schema);
//! ```
use crate::Cmd;
use crate::search::*;

/// FT.CREATE command builder.
pub struct FtCreateCommand {
    index: String,
    options: CreateOptions,
    schema: RediSearchSchema,
}

impl FtCreateCommand {
    /// Create a new FT.CREATE command for the given index
    pub fn new<S: Into<String>>(index: S) -> Self {
        Self {
            index: index.into(),
            options: CreateOptions::default(),
            schema: RediSearchSchema::new(),
        }
    }

    /// Set the options for the command
    pub fn options(mut self, options: CreateOptions) -> Self {
        self.options = options;
        self
    }

    /// Set the schema for the command
    pub fn schema(mut self, schema: RediSearchSchema) -> Self {
        self.schema = schema;
        self
    }

    /// Consume the builder and convert it into a `redis::Cmd`.
    pub fn into_cmd(self) -> Cmd {
        assert!(
            !self.index.is_empty(),
            "FT.CREATE command requires a non-empty index name"
        );
        assert!(
            !self.schema.is_empty(),
            "FT.CREATE command requires at least one field in the schema"
        );

        let mut cmd = crate::cmd("FT.CREATE");
        cmd.arg(&self.index);
        cmd.arg(&self.options);
        cmd.arg("SCHEMA");
        cmd.arg(&self.schema);

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
#[path = "tests.rs"]
mod tests;
