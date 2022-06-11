use super::arguments::Argument;
use super::COMMAND_NAME_OVERWRITE;
use crate::commands::{CommandArgument, CommandDefinition, CommandGroup};
use crate::ident::to_snake;
use crate::commands::ArgType;

/// An abstract type representing a code generation unit for a command
pub(crate) struct Command {
    name: String,
    docs: Vec<String>,
    group: CommandGroup,
    args: Vec<Argument>,
}

impl Command {
    pub(crate) fn new(name: String, definition: &CommandDefinition) -> Self {
        let mut kv_index: (u8, u8) = (0, 0);

        let args = definition
            .arguments
            .iter()
            .filter_map(|x| map_argument(&mut kv_index, x))
            .collect::<Vec<_>>();

        let docs = build_docs(&name, definition);

        Self {
            name,
            docs,
            group: definition.group,
            args,
        }
    }

    pub(crate) fn fn_name(&self) -> String {
        if let Some(&(_, name)) = COMMAND_NAME_OVERWRITE
            .iter()
            .find(|(name, _)| name == &self.name)
        {
            return name.to_owned();
        }
        to_snake(&self.name)
    }

    pub(crate) fn command(&self) -> &str {
        &self.name
    }

    pub(crate) fn arguments(&self) -> impl Iterator<Item = &Argument> + ExactSizeIterator {
        self.args.iter()
    }

    pub(crate) fn group(&self) -> CommandGroup {
        self.group
    }

    pub(crate) fn docs(&self) -> &[String] {
        &self.docs
    }
}

// Todo handle key_specs correctly
fn map_argument((key_id, value_id): &mut (u8, u8), arg: &CommandArgument) -> Option<Argument> {
    // TODO Ignore argument until we have proper token handling.
    // We propably want to generate a type for each token that implements ToRedisArgs and expands to the wrapped type and the Token
    if arg.token.is_some() {
        return None;
    }

    match arg.r#type {
        ArgType::Key { key_spec_index: _ } => {
            let idx = *key_id;
            *key_id += 1;

            let name = to_snake(&arg.name);


            
            let r#trait = "ToRedisArgs".to_string();

            Some(Argument::new_generic(
                name,
                format!("K{}", idx),
                r#trait,
                arg.optional,
            ))
        }
        ArgType::String
        | ArgType::Integer
        | ArgType::Double => {
            let idx = *value_id;
            *value_id += 1;

            let name = to_snake(&arg.name);

            // ToRedis is implemented for Vec this it currently does not make much sense to specialize the trait bound for multiple.
            // Else something like this could be useful?
            // let r#trait = if arg.multiple {
            //     "Iterator<Item = impl ToRedisArgs>".to_string()
            // } else {
            //     "ToRedisArgs".to_string()
            // };
            let r#trait = "ToRedisArgs".to_string();

            Some(Argument::new_generic(
                name,
                format!("T{}", idx),
                r#trait,
                arg.optional,
            ))
        }
        ArgType::Pattern => {
            let idx = *key_id;
            *key_id += 1;

            let name = to_snake(&arg.name);

            let r#trait = "ToRedisArgs".to_string();

            Some(Argument::new_generic(
                name,
                format!("K{}", idx),
                r#trait,
                arg.optional,
            ))
        }
        // These should be tuples. ToRedisArgs should take care of it
        ArgType::Block { arguments: _ } => {
            let idx = *value_id;
            *value_id += 1;

            let name = to_snake(&arg.name);

            let r#trait = "ToRedisArgs".to_string();

            Some(Argument::new_generic(
                name,
                format!("T{}", idx),
                r#trait,
                arg.optional,
            ))
        }

        _ => None,
    }
}

fn build_docs(command: &str, definition: &CommandDefinition) -> Vec<String> {
    let mut docs = vec![
        command.to_string(),
        String::new(),
        definition.summary.clone(),
        String::new(),
        format!("Since: Redis {}", definition.since),
        format!("Group: {}", definition.group),
    ];

    if let Some(replaced_by) = &definition.replaced_by {
        docs.push(format!("Replaced By: {}", replaced_by))
    }

    if let Some(complexity) = &definition.complexity {
        docs.push(format!("Complexity: {}", complexity))
    }

    if let Some(replaced_by) = &definition.replaced_by {
        docs.push(format!("Replaced By: {}", replaced_by))
    }

    docs
}
