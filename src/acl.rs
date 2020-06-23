//! Defines types to use with the ACL commands.

use crate::types::{ErrorKind, FromRedisValue, RedisResult, RedisWrite, ToRedisArgs, Value};

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
}

impl ToRedisArgs for Rule {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        use self::Rule::*;

        let rule = match self {
            On => "on".to_owned(),
            Off => "off".to_owned(),

            AddCommand(cmd) => "+".to_owned() + cmd,
            RemoveCommand(cmd) => "-".to_owned() + cmd,
            AddCategory(cat) => "+@".to_owned() + cat,
            RemoveCategory(cat) => "-@".to_owned() + cat,
            AllCommands => "allcommands".to_owned(),
            NoCommands => "nocommands".to_owned(),

            AddPass(pass) => ">".to_owned() + pass,
            RemovePass(pass) => "<".to_owned() + pass,
            AddHashedPass(pass) => "#".to_owned() + pass,
            RemoveHashedPass(pass) => "!".to_owned() + pass,
            NoPass => "nopass".to_owned(),
            ResetPass => "resetpass".to_owned(),

            Pattern(pat) => "~".to_owned() + pat,
            AllKeys => "allkeys".to_owned(),
            ResetKeys => "resetkeys".to_owned(),

            Reset => "reset".to_owned(),
        };

        out.write_arg(rule.as_bytes());
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_args {
        ($rule:expr, $arg:expr) => {
            assert_eq!($rule.to_redis_args(), vec![$arg.to_vec()]);
        }
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
            AddHashedPass("c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2".to_owned()),
            b"#c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"
        );
        assert_args!(
            RemoveHashedPass("c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2".to_owned()),
            b"!c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"
        );
        assert_args!(NoPass, b"nopass");
        assert_args!(Pattern("pat:*".to_owned()), b"~pat:*");
        assert_args!(AllKeys, b"allkeys");
        assert_args!(ResetKeys, b"resetkeys");
        assert_args!(Reset, b"reset");
    }
}
