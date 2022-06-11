use std::fmt;

pub enum Type {
    Concrete(String),
    Trait { generic_ident: String, name: String },
}

/// An abstract type representing a code generation unit for a argument for a command
///
/// This bundles the name with the assigned trait ident and trait
/// This is overly generic, to be able to generate more than the default ToRedisArgs trait bounds
pub(crate) struct Argument {
    pub name: String,
    pub r#type: Type,
    pub optional: bool,
}

impl Argument {
    pub(crate) fn new_concrete(name: String, r#type: String, optional: bool) -> Self {
        Self {
            name,
            r#type: Type::Concrete(r#type),

            optional,
        }
    }

    pub(crate) fn new_generic(
        name: String,
        generic_ident: String,
        r#trait: String,

        optional: bool,
    ) -> Self {
        Self {
            name,
            r#type: Type::Trait {
                generic_ident,
                name: r#trait,
            },

            optional,
        }
    }

    pub(crate) fn trait_bound(&self) -> Option<String> {
        match &self.r#type {
            Type::Concrete(_) => None,
            Type::Trait {
                generic_ident,
                name,
            } => Some(format!("{}: {}", generic_ident, name)),
        }
    }
}

impl fmt::Display for Argument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.optional {
            match &self.r#type {
                Type::Concrete(name) => write!(f, "{}: Option<{}>", self.name, name),
                Type::Trait {
                    generic_ident,
                    name: _,
                } => write!(f, "{}: Option<{}>", self.name, generic_ident),
            }
        } else {
            match &self.r#type {
                Type::Concrete(name) => write!(f, "{}: {}", self.name, name),
                Type::Trait {
                    generic_ident,
                    name: _,
                } => write!(f, "{}: {}", self.name, generic_ident),
            }
        }
    }
}
