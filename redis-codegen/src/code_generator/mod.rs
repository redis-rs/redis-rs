use crate::commands::DocFlag;
use crate::commands::{CommandDefinition, CommandSet};
use crate::feature_gates::FeatureGate;
use commands::Command;
use itertools::Itertools;

mod arguments;
mod commands;

pub static BLACKLIST: &[&str] = &["SCAN", "HSCAN", "SSCAN", "ZSCAN", "CLIENT KILL"];
pub static COMMAND_NAME_OVERWRITE: &[(&str, &str)] = &[];
pub static COMMAND_COMPATIBILITY: &[(&str, &str)] = &[("GETDEL", "GET_DEL"), ("ZREMRANGEBYLEX", "ZREMBYLEX")];

pub struct CodeGenerator<'a> {
    depth: u8,
    buf: &'a mut String,
}

fn push_indent(buf: &mut String, depth: u8) {
    for _ in 0..depth {
        buf.push_str("    ");
    }
}

impl<'a> CodeGenerator<'a> {
    pub fn generate(commands: CommandSet, buf: &mut String) {
        let mut code_gen = CodeGenerator { depth: 0, buf };

        code_gen.buf.push_str("implement_commands! {\n");
        code_gen.depth += 1;
        code_gen.push_indent();
        code_gen.buf.push_str("'a\n\n");

        // Group commands by group and then by command name
        for (command, definition) in commands
            .into_iter()
            .sorted_by(|x, y| Ord::cmp(&x.1.group, &y.1.group).then(Ord::cmp(&x.0, &y.0)))
        {
            if !BLACKLIST.contains(&&*command) {
                code_gen.append_command(command.clone(), definition.clone());
            }

            if let Some(backwarts_compatible_name) = COMMAND_COMPATIBILITY
                .iter()
                .find(|(name, _)| name == &&command)
            {
                code_gen.append_command(backwarts_compatible_name.1.to_string(), definition);
            }
        }

        code_gen.depth -= 1;
        code_gen.buf.push_str("}\n");
    }

    fn push_indent(&mut self) {
        push_indent(self.buf, self.depth);
    }

    fn append_command(&mut self, name: String, definition: CommandDefinition) {
        log::debug!("  command: {:?}", name);

        let command = Command::new(name, &definition);

        self.append_doc(&command);
        self.append_feature_gate(&command);

        if definition.doc_flags.contains(&DocFlag::Deprecated) {
            self.push_indent();
            // Including the version might be hard here, as we want to put the crate version here in which this got deprecated.
            self.buf.push_str("#[deprecated]");
        }

        self.append_fn_decl(&command);
        self.depth += 1;

        self.append_fn_body(&command);

        self.depth -= 1;
        self.buf.push('\n');
        self.push_indent();
        self.buf.push_str("}\n");
        self.buf.push('\n');
    }

    // Todo Improve docs. Add complexity group etc.
    fn append_doc(&mut self, command: &Command) {
        let docs = command.docs().to_owned();
        let doc_comment = Comment(docs);
        doc_comment.append_with_indent(self.depth, self.buf);
    }

    /// Appends the function declaration with trait bounds and arguments
    fn append_fn_decl(&mut self, command: &Command) {
        self.push_indent();

        let mut trait_bounds = vec![];
        let mut args = vec![];

        for arg in command.arguments() {
            trait_bounds.push(arg.trait_bound());
            args.push(arg.to_string())
        }

        self.buf.push_str(&format!(
            "fn {}<{}>({}) {{\n",
            command.fn_name(),
            trait_bounds
                .iter()
                .filter_map(|x| x.as_ref())
                .map(|x| x.as_str())
                .join(", "),
            args.join(", ")
        ));
    }

    /// Appends the function body
    fn append_fn_body(&mut self, command: &Command) {
        self.push_indent();

        let mut args = command.arguments().map(|arg| format!(".arg({})", arg.name));
        if args.len() == 0 {
            self.buf
                .push_str(&format!("cmd(\"{}\").as_mut()", command.command(),));
        } else {
            self.buf
                .push_str(&format!("cmd(\"{}\"){}", command.command(), args.join("")));
        }
    }

    fn append_feature_gate(&mut self, command: &Command) {
        let group = command.group();
        let command = command.command();

        if let Some(feature) = group.to_feature().or_else(|| command.to_feature()) {
            self.push_indent();
            self.buf
                .push_str(&format!("#[cfg(feature = \"{}\")]\n", feature));
            self.push_indent();
            self.buf.push_str(&format!(
                "#[cfg_attr(docsrs, doc(cfg(feature = \"{}\")))]\n",
                feature
            ));
        }
    }
}

struct Comment(pub Vec<String>);

impl Comment {
    pub fn append_with_indent(&self, indent_level: u8, buf: &mut String) {
        for line in &self.0 {
            for _ in 0..indent_level {
                buf.push_str("    ");
            }
            buf.push_str("/// ");
            // TODO prost sanitizes comments first. Should we do this here as well?
            buf.push_str(line);
            buf.push('\n');
        }
    }
}
