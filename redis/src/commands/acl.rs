//! Defines types to use with the ACL commands.

use crate::errors::ParsingError;
use crate::types::{FromRedisValue, RedisWrite, ToRedisArgs, Value};

macro_rules! not_convertible_error {
    ($v:expr, $det:expr) => {
        ParsingError::from(format!("{:?} (response was {:?})", $det, $v))
    };
}

/// ACL rules are used in order to activate or remove a flag, or to perform a
/// given change to the user ACL, which under the hood are just single words.
#[derive(Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Rule {
    /// Enable the user: it is possible to authenticate as this user.
    On,
    /// Disable the user: it's no longer possible to authenticate with this
    /// user, however the already authenticated connections will still work.
    Off,

    /// Add the command to the list of commands the user can call.
    AddCommand(String),
    /// Remove the command to the list of commands the user can call.
    RemoveCommand(String),
    /// Add all the commands in such category to be called by the user.
    AddCategory(String),
    /// Remove the commands from such category the client can call.
    RemoveCategory(String),
    /// Alias for `+@all`. Note that it implies the ability to execute all the
    /// future commands loaded via the modules system.
    AllCommands,
    /// Alias for `-@all`.
    NoCommands,

    /// Add this password to the list of valid password for the user.
    AddPass(String),
    /// Remove this password from the list of valid passwords.
    RemovePass(String),
    /// Add this SHA-256 hash value to the list of valid passwords for the user.
    AddHashedPass(String),
    /// Remove this hash value from from the list of valid passwords
    RemoveHashedPass(String),
    /// All the set passwords of the user are removed, and the user is flagged
    /// as requiring no password: it means that every password will work
    /// against this user.
    NoPass,
    /// Flush the list of allowed passwords. Moreover removes the _nopass_ status.
    ResetPass,

    /// Add a pattern of keys that can be mentioned as part of commands.
    Pattern(String),
    /// Alias for `~*`.
    AllKeys,
    /// Flush the list of allowed keys patterns.
    ResetKeys,

    /// Pattern for pub/sub channels (returned prefixed with `&` by Redis)
    Channel(String),
    /// Selector entries (returned by Redis under `(selectors)`).
    Selector(Vec<String>),

    /// Performs the following actions: `resetpass`, `resetkeys`, `off`, `-@all`.
    /// The user returns to the same state it has immediately after its creation.
    Reset,

    /// Raw text of [`ACL rule`][1]  that not enumerated above.
    ///
    /// [1]: https://redis.io/docs/manual/security/acl
    Other(String),
}

impl ToRedisArgs for Rule {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        use self::Rule::*;

        match self {
            On => out.write_arg(b"on"),
            Off => out.write_arg(b"off"),

            AddCommand(cmd) => out.write_arg_fmt(format_args!("+{cmd}")),
            RemoveCommand(cmd) => out.write_arg_fmt(format_args!("-{cmd}")),
            AddCategory(cat) => out.write_arg_fmt(format_args!("+@{cat}")),
            RemoveCategory(cat) => out.write_arg_fmt(format_args!("-@{cat}")),
            AllCommands => out.write_arg(b"allcommands"),
            NoCommands => out.write_arg(b"nocommands"),

            AddPass(pass) => out.write_arg_fmt(format_args!(">{pass}")),
            RemovePass(pass) => out.write_arg_fmt(format_args!("<{pass}")),
            AddHashedPass(pass) => out.write_arg_fmt(format_args!("#{pass}")),
            RemoveHashedPass(pass) => out.write_arg_fmt(format_args!("!{pass}")),
            NoPass => out.write_arg(b"nopass"),
            ResetPass => out.write_arg(b"resetpass"),

            Pattern(pat) => out.write_arg_fmt(format_args!("~{pat}")),
            AllKeys => out.write_arg(b"allkeys"),
            ResetKeys => out.write_arg(b"resetkeys"),
            Channel(pat) => out.write_arg_fmt(format_args!("&{pat}")),
            Selector(sel) => out.write_arg_fmt(format_args!("({})", sel.join(" "))),

            Reset => out.write_arg(b"reset"),

            Other(rule) => out.write_arg(rule.as_bytes()),
        };
    }
}

/// An info dictionary type storing Redis ACL information as multiple `Rule`.
/// This type collects key/value data returned by the [`ACL GETUSER`][1] command.
///
/// [1]: https://redis.io/commands/acl-getuser
#[derive(Debug, Eq, PartialEq)]
pub struct AclInfo {
    /// Describes flag rules for the user. Represented by [`Rule::On`][1],
    /// [`Rule::Off`][2], [`Rule::AllKeys`][3], [`Rule::AllCommands`][4] and
    /// [`Rule::NoPass`][5].
    ///
    /// [1]: ./enum.Rule.html#variant.On
    /// [2]: ./enum.Rule.html#variant.Off
    /// [3]: ./enum.Rule.html#variant.AllKeys
    /// [4]: ./enum.Rule.html#variant.AllCommands
    /// [5]: ./enum.Rule.html#variant.NoPass
    pub flags: Vec<Rule>,
    /// Describes the user's passwords. Represented by [`Rule::AddHashedPass`][1].
    ///
    /// [1]: ./enum.Rule.html#variant.AddHashedPass
    pub passwords: Vec<Rule>,
    /// Describes capabilities of which commands the user can call.
    /// Represented by [`Rule::AddCommand`][1], [`Rule::AddCategory`][2],
    /// [`Rule::RemoveCommand`][3] and [`Rule::RemoveCategory`][4].
    ///
    /// [1]: ./enum.Rule.html#variant.AddCommand
    /// [2]: ./enum.Rule.html#variant.AddCategory
    /// [3]: ./enum.Rule.html#variant.RemoveCommand
    /// [4]: ./enum.Rule.html#variant.RemoveCategory
    pub commands: Vec<Rule>,
    /// Describes patterns of keys which the user can access. Represented by
    /// [`Rule::Pattern`][1].
    ///
    /// [1]: ./enum.Rule.html#variant.Pattern
    pub keys: Vec<Rule>,
    /// Describes pub/sub channel patterns. Represented by [`Rule::Channel`].
    pub channels: Vec<Rule>,
    /// Describes selectors. Represented by [`Rule::Selector`].
    pub selectors: Vec<Rule>,
}

impl FromRedisValue for AclInfo {
    fn from_redis_value(v: Value) -> Result<Self, ParsingError> {
        // The response is an array with alternating key names and values,
        // for example: ["flags", [...], "passwords", [...], "commands", "...", "keys", ...]
        let seq = v
            .as_sequence()
            .ok_or_else(|| not_convertible_error!(v, ""))?;

        // Prepare default empty fields
        let mut flags: Vec<Rule> = Vec::new();
        let mut passwords: Vec<Rule> = Vec::new();
        let mut commands: Vec<Rule> = Vec::new();
        let mut keys: Vec<Rule> = Vec::new();
        let mut channels: Vec<Rule> = Vec::new();
        let mut selectors: Vec<Rule> = Vec::new();

        let mut i = 0usize;
        while i < seq.len() {
            let name = &seq[i];
            let value = seq
                .get(i + 1)
                .ok_or_else(|| not_convertible_error!(v, "Malformed ACL GETUSER response"))?;
            // Expect name to be a bulk string
            let key = match name {
                Value::BulkString(bs) => std::str::from_utf8(bs)?,
                _ => {
                    return Err(not_convertible_error!(
                        name,
                        "Expect a bulk string key name"
                    ));
                }
            };

            match key {
                "flags" => {
                    let f = value
                        .as_sequence()
                        .ok_or_else(|| {
                            not_convertible_error!(value, "Expect an array response of ACL flags")
                        })?
                        .iter()
                        .map(|flag| match flag {
                            Value::BulkString(flag) => match flag.as_slice() {
                                b"on" => Ok(Rule::On),
                                b"off" => Ok(Rule::Off),
                                b"allkeys" => Ok(Rule::AllKeys),
                                b"allcommands" => Ok(Rule::AllCommands),
                                b"nopass" => Ok(Rule::NoPass),
                                other => {
                                    Ok(Rule::Other(String::from_utf8_lossy(other).into_owned()))
                                }
                            },
                            _ => Err(not_convertible_error!(
                                flag,
                                "Expect an arbitrary binary data"
                            )),
                        })
                        .collect::<Result<_, _>>()?;
                    flags = f;
                }
                "passwords" => {
                    let p = value
                        .as_sequence()
                        .ok_or_else(|| {
                            not_convertible_error!(
                                value,
                                "Expect an array response of ACL passwords"
                            )
                        })?
                        .iter()
                        .map(|pass| {
                            let s = String::from_redis_value_ref(pass)?;
                            Ok(Rule::AddHashedPass(s))
                        })
                        .collect::<Result<_, ParsingError>>()?;
                    passwords = p;
                }
                "commands" => {
                    let cmds = match value {
                        Value::BulkString(cmd) => std::str::from_utf8(cmd)?,
                        _ => {
                            return Err(not_convertible_error!(
                                value,
                                "Expect a valid UTF8 string for commands"
                            ));
                        }
                    }
                    .split_terminator(' ')
                    .map(|cmd| match cmd {
                        x if x.starts_with("+@") => Ok(Rule::AddCategory(x[2..].to_owned())),
                        x if x.starts_with("-@") => Ok(Rule::RemoveCategory(x[2..].to_owned())),
                        x if x.starts_with('+') => Ok(Rule::AddCommand(x[1..].to_owned())),
                        x if x.starts_with('-') => Ok(Rule::RemoveCommand(x[1..].to_owned())),
                        _ => Err(not_convertible_error!(
                            cmd,
                            "Expect a command addition/removal"
                        )),
                    })
                    .collect::<Result<_, _>>()?;
                    commands = cmds;
                }
                "keys" => {
                    let parsed = match value {
                        Value::Array(arr) => arr
                            .iter()
                            .map(|pat| {
                                let s = String::from_redis_value_ref(pat)?;
                                Ok(Rule::Pattern(s))
                            })
                            .collect::<Result<_, ParsingError>>()?,
                        Value::BulkString(bs) => {
                            let mut s = std::str::from_utf8(bs)?;
                            s = s.trim();
                            if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
                                s = &s[1..s.len() - 1];
                            }
                            s.split_whitespace()
                                .map(|tok| {
                                    let tok = if let Some(tok) = tok.strip_prefix('~') {
                                        tok
                                    } else {
                                        tok
                                    };
                                    Ok(Rule::Pattern(tok.to_owned()))
                                })
                                .collect::<Result<_, ParsingError>>()?
                        }
                        other => {
                            return Err(not_convertible_error!(
                                other,
                                "Expect an array or bulk-string of keys"
                            ));
                        }
                    };
                    keys = parsed;
                }
                "channels" => {
                    let parsed = match value {
                        Value::Array(arr) => arr
                            .iter()
                            .map(|pat| {
                                let s = String::from_redis_value_ref(pat)?;
                                let s = if let Some(s) = s.strip_prefix('&') {
                                    s.to_owned()
                                } else {
                                    s
                                };
                                Ok(Rule::Channel(s))
                            })
                            .collect::<Result<_, ParsingError>>()?,
                        Value::BulkString(bs) => {
                            let mut s = std::str::from_utf8(bs)?;
                            s = s.trim();
                            if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
                                s = &s[1..s.len() - 1];
                            }
                            s.split_whitespace()
                                .map(|tok| {
                                    let tok = if let Some(tok) = tok.strip_prefix('&') {
                                        tok
                                    } else {
                                        tok
                                    };
                                    Ok(Rule::Channel(tok.to_owned()))
                                })
                                .collect::<Result<_, ParsingError>>()?
                        }
                        other => {
                            return Err(not_convertible_error!(
                                other,
                                "Expect an array or bulk-string of channels"
                            ));
                        }
                    };
                    channels = parsed;
                }
                "selectors" => {
                    let parsed = match value {
                        // selectors can be returned as an array of bulk-strings, or as
                        // an array of arrays where each inner array contains alternating
                        // key/value bulk-strings describing the selector. Accept both.
                        Value::Array(arr) => arr
                            .iter()
                            .map(|pat| {
                                match pat {
                                    Value::BulkString(_) => {
                                        // simple bulk-string selector
                                        let s = String::from_redis_value_ref(pat)?;
                                        // Trim optional surrounding quotes
                                        let s = {
                                            let t = s.trim();
                                            if t.len() >= 2
                                                && t.starts_with('"')
                                                && t.ends_with('"')
                                            {
                                                t[1..t.len() - 1].to_owned()
                                            } else {
                                                t.to_owned()
                                            }
                                        };
                                        Ok(Rule::Selector(
                                            s.split(' ').map(|x| x.to_owned()).collect(),
                                        ))
                                    }
                                    Value::Array(inner) => {
                                        // Join inner bulk-strings into a single selector string
                                        let mut parts: Vec<String> = Vec::new();
                                        for item in inner.iter() {
                                            let s = String::from_redis_value_ref(item)?;
                                            let t = s.trim();
                                            let t = if t.len() >= 2
                                                && t.starts_with('"')
                                                && t.ends_with('"')
                                            {
                                                &t[1..t.len() - 1]
                                            } else {
                                                t
                                            };
                                            parts.push(t.to_owned());
                                        }
                                        Ok(Rule::Selector(parts))
                                    }
                                    other => {
                                        // Unexpected shape for a selector entry
                                        let s = String::from_redis_value_ref(other)?;
                                        Ok(Rule::Selector(
                                            s.split(' ').map(|x| x.to_owned()).collect(),
                                        ))
                                    }
                                }
                            })
                            .collect::<Result<_, ParsingError>>()?,
                        Value::BulkString(bs) => {
                            let mut s = std::str::from_utf8(bs)?;
                            s = s.trim();
                            if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
                                s = &s[1..s.len() - 1];
                            }
                            s.split_whitespace()
                                .map(|tok| Ok(Rule::Selector(vec![tok.to_owned()])))
                                .collect::<Result<_, ParsingError>>()?
                        }
                        other => {
                            return Err(not_convertible_error!(
                                other,
                                "Expect an array or bulk-string of selectors"
                            ));
                        }
                    };
                    selectors = parsed;
                }
                _ => {}
            }

            i += 2;
        }

        Ok(Self {
            flags,
            passwords,
            commands,
            keys,
            channels,
            selectors,
        })
    }
}
