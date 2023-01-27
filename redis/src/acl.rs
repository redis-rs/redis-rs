//! Defines types to use with the ACL commands.

use crate::types::{
    ErrorKind, FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs, Value,
};

macro_rules! not_convertible_error {
    ($v:expr, $det:expr) => {
        RedisError::from((
            ErrorKind::TypeError,
            "Response type not convertible",
            format!("{:?} (response was {:?})", $det, $v),
        ))
    };
}

/// ACL rules are used in order to activate or remove a flag, or to perform a
/// given change to the user ACL, which under the hood are just single words.
#[derive(Debug, Eq, PartialEq)]
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
}

impl FromRedisValue for AclInfo {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let mut it = v
            .as_sequence()
            .ok_or_else(|| not_convertible_error!(v, ""))?
            .iter()
            .skip(1)
            .step_by(2);

        let (flags, passwords, commands, keys) = match (it.next(), it.next(), it.next(), it.next())
        {
            (Some(flags), Some(passwords), Some(commands), Some(keys)) => {
                // Parse flags
                // Ref: https://github.com/redis/redis/blob/0cabe0cfa7290d9b14596ec38e0d0a22df65d1df/src/acl.c#L83-L90
                let flags = flags
                    .as_sequence()
                    .ok_or_else(|| {
                        not_convertible_error!(flags, "Expect a bulk response of ACL flags")
                    })?
                    .iter()
                    .map(|flag| match flag {
                        Value::Data(flag) => match flag.as_slice() {
                            b"on" => Ok(Rule::On),
                            b"off" => Ok(Rule::Off),
                            b"allkeys" => Ok(Rule::AllKeys),
                            b"allcommands" => Ok(Rule::AllCommands),
                            b"nopass" => Ok(Rule::NoPass),
                            other => Ok(Rule::Other(String::from_utf8_lossy(other).into_owned())),
                        },
                        _ => Err(not_convertible_error!(
                            flag,
                            "Expect an arbitrary binary data"
                        )),
                    })
                    .collect::<RedisResult<_>>()?;

                let passwords = passwords
                    .as_sequence()
                    .ok_or_else(|| {
                        not_convertible_error!(flags, "Expect a bulk response of ACL flags")
                    })?
                    .iter()
                    .map(|pass| Ok(Rule::AddHashedPass(String::from_redis_value(pass)?)))
                    .collect::<RedisResult<_>>()?;

                let commands = match commands {
                    Value::Data(cmd) => std::str::from_utf8(cmd)?,
                    _ => {
                        return Err(not_convertible_error!(
                            commands,
                            "Expect a valid UTF8 string"
                        ))
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
                .collect::<RedisResult<_>>()?;

                let keys = keys
                    .as_sequence()
                    .ok_or_else(|| not_convertible_error!(keys, ""))?
                    .iter()
                    .map(|pat| Ok(Rule::Pattern(String::from_redis_value(pat)?)))
                    .collect::<RedisResult<_>>()?;

                (flags, passwords, commands, keys)
            }
            _ => {
                return Err(not_convertible_error!(
                    v,
                    "Expect a resposne from `ACL GETUSER`"
                ))
            }
        };

        Ok(Self {
            flags,
            passwords,
            commands,
            keys,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_args {
        ($rule:expr, $arg:expr) => {
            assert_eq!($rule.to_redis_args(), vec![$arg.to_vec()]);
        };
    }

    #[test]
    fn test_rule_to_arg() {
        use self::Rule::*;

        assert_args!(On, b"on");
        assert_args!(Off, b"off");
        assert_args!(AddCommand("set".to_owned()), b"+set");
        assert_args!(RemoveCommand("set".to_owned()), b"-set");
        assert_args!(AddCategory("hyperloglog".to_owned()), b"+@hyperloglog");
        assert_args!(RemoveCategory("hyperloglog".to_owned()), b"-@hyperloglog");
        assert_args!(AllCommands, b"allcommands");
        assert_args!(NoCommands, b"nocommands");
        assert_args!(AddPass("mypass".to_owned()), b">mypass");
        assert_args!(RemovePass("mypass".to_owned()), b"<mypass");
        assert_args!(
            AddHashedPass(
                "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2".to_owned()
            ),
            b"#c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"
        );
        assert_args!(
            RemoveHashedPass(
                "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2".to_owned()
            ),
            b"!c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"
        );
        assert_args!(NoPass, b"nopass");
        assert_args!(Pattern("pat:*".to_owned()), b"~pat:*");
        assert_args!(AllKeys, b"allkeys");
        assert_args!(ResetKeys, b"resetkeys");
        assert_args!(Reset, b"reset");
        assert_args!(Other("resetchannels".to_owned()), b"resetchannels");
    }

    #[test]
    fn test_from_redis_value() {
        let redis_value = Value::Bulk(vec![
            Value::Data("flags".into()),
            Value::Bulk(vec![
                Value::Data("on".into()),
                Value::Data("allchannels".into()),
            ]),
            Value::Data("passwords".into()),
            Value::Bulk(vec![]),
            Value::Data("commands".into()),
            Value::Data("-@all +get".into()),
            Value::Data("keys".into()),
            Value::Bulk(vec![Value::Data("pat:*".into())]),
        ]);
        let acl_info = AclInfo::from_redis_value(&redis_value).expect("Parse successfully");

        assert_eq!(
            acl_info,
            AclInfo {
                flags: vec![Rule::On, Rule::Other("allchannels".into())],
                passwords: vec![],
                commands: vec![
                    Rule::RemoveCategory("all".to_owned()),
                    Rule::AddCommand("get".to_owned()),
                ],
                keys: vec![Rule::Pattern("pat:*".to_owned())],
            }
        );
    }
}
