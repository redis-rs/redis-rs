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

/// Marker type indicating no schema has been set yet.
pub struct NoSchema;

/// FT.CREATE command builder.
///
/// Uses the typestate pattern to enforce at compile time that a schema
/// is set before the command can be built. The schema state is encoded
/// directly in the generic parameter: before a schema is set the builder
/// holds a [`NoSchema`] marker, and after the schema is set it holds the
/// [`RediSearchSchema<NonEmpty>`] itself.
///
/// # Type States
/// - `FtCreateCommand<NoSchema>` - No schema set yet, `into_cmd()` not available
/// - `FtCreateCommand<RediSearchSchema<NonEmpty>>` - Schema set, `into_cmd()` available
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
    /// `FtCreateCommand<RediSearchSchema<NonEmpty>>`, making `into_cmd()` available.
    pub fn schema(
        self,
        schema: RediSearchSchema<NonEmpty>,
    ) -> FtCreateCommand<RediSearchSchema<NonEmpty>> {
        FtCreateCommand {
            index: self.index,
            options: self.options,
            schema,
        }
    }
}

impl FtCreateCommand<RediSearchSchema<NonEmpty>> {
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
#[path = "tests.rs"]
mod tests;
