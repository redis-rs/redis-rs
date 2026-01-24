#![cfg(feature = "acl")]

use redis::TypedCommands;
use redis::acl::{AclInfo, Rule};
use std::collections::HashSet;

mod support;
use crate::support::*;

#[test]
fn test_acl_whoami() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    assert_eq!(con.acl_whoami(), Ok("default".to_owned()));
}

#[test]
fn test_acl_help() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let res = con.acl_help().expect("Got help manual");
    assert!(!res.is_empty());
}

//TODO: do we need this test?
#[test]
#[ignore]
fn test_acl_getsetdel_users() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    assert_eq!(
        con.acl_list(),
        Ok(vec!["user default on nopass ~* +@all".to_owned()])
    );
    assert_eq!(con.acl_users(), Ok(vec!["default".to_owned()]));
    // bob
    assert_eq!(con.acl_setuser("bob"), Ok(()));
    assert_eq!(
        con.acl_users(),
        Ok(vec!["bob".to_owned(), "default".to_owned()])
    );

    // ACL SETUSER bob on ~redis:* +set
    assert_eq!(
        con.acl_setuser_rules(
            "bob",
            &[
                Rule::On,
                Rule::AddHashedPass(
                    "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2".to_owned()
                ),
                Rule::Pattern("redis:*".to_owned()),
                Rule::AddCommand("set".to_owned())
            ],
        ),
        Ok(())
    );
    let acl_info = con.acl_getuser("bob").expect("Got user").unwrap();
    assert_eq!(
        acl_info,
        AclInfo {
            flags: vec![Rule::On],
            passwords: vec![Rule::AddHashedPass(
                "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2".to_owned()
            )],
            commands: vec![
                Rule::RemoveCategory("all".to_owned()),
                Rule::AddCommand("set".to_owned())
            ],
            keys: vec![Rule::Pattern("redis:*".to_owned())],
            channels: vec![],
            selectors: vec![],
        }
    );
    assert_eq!(
        con.acl_list(),
        Ok(vec![
            "user bob on #c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2 ~redis:* -@all +set".to_owned(),
            "user default on nopass ~* +@all".to_owned(),
        ])
    );

    // ACL SETUSER eve
    assert_eq!(con.acl_setuser("eve"), Ok(()));
    assert_eq!(
        con.acl_users(),
        Ok(vec![
            "bob".to_owned(),
            "default".to_owned(),
            "eve".to_owned()
        ])
    );
    assert_eq!(con.acl_deluser(&["bob", "eve"]), Ok(2));
    assert_eq!(con.acl_users(), Ok(vec!["default".to_owned()]));
}

#[test]
fn test_acl_cat() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let res: HashSet<String> = con.acl_cat().expect("Got categories");
    let expects = vec![
        "keyspace",
        "read",
        "write",
        "set",
        "sortedset",
        "list",
        "hash",
        "string",
        "bitmap",
        "hyperloglog",
        "geo",
        "stream",
        "pubsub",
        "admin",
        "fast",
        "slow",
        "blocking",
        "dangerous",
        "connection",
        "transaction",
        "scripting",
    ];
    for cat in expects.iter() {
        assert!(res.contains(*cat), "Category `{cat}` does not exist");
    }

    let expects = ["pfmerge", "pfcount", "pfselftest", "pfadd"];
    let res = con
        .acl_cat_categoryname("hyperloglog")
        .expect("Got commands of a category");
    for cmd in expects.iter() {
        assert!(res.contains(*cmd), "Command `{cmd}` does not exist");
    }
}

#[test]
fn test_acl_genpass() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let pass: String = con.acl_genpass().expect("Got password");
    assert_eq!(pass.len(), 64);

    let pass: String = con.acl_genpass_bits(1024).expect("Got password");
    assert_eq!(pass.len(), 256);
}

#[test]
fn test_acl_log() {
    let ctx = TestContext::new();
    let mut con = ctx.connection();
    let logs: Vec<String> = con.acl_log(1).expect("Got logs");
    assert_eq!(logs.len(), 0);
    assert_eq!(con.acl_log_reset(), Ok(()));
}

#[test]
fn test_acl_dryrun() {
    let ctx = TestContext::new();
    run_test_if_version_supported!(&(7, 0, 0));

    let mut con = ctx.connection();

    redis::cmd("ACL")
        .arg("SETUSER")
        .arg("VIRGINIA")
        .arg("+SET")
        .arg("~*")
        .exec(&mut con)
        .unwrap();

    assert_eq!(
        con.acl_dryrun(b"VIRGINIA", String::from("SET"), &["foo", "bar"])
            .unwrap(),
        "OK"
    );

    let res: String = con
        .acl_dryrun(b"VIRGINIA", String::from("GET"), "foo")
        .unwrap();
    assert_eq!(
        res,
        "User VIRGINIA has no permissions to run the 'get' command"
    );
}
#[test]
fn test_acl_info() {
    let ctx = TestContext::new();
    let mut conn = ctx.connection();
    let username = "tenant";
    let password = "securepassword123";
    const DEFAULT_QUEUE_NAME: &str = "default";
    let rules = vec![
        // Basic permissions: on, +@all, -@dangerous, +keys, -info
        Rule::On,
        Rule::RestChannels,
        Rule::AllCommands,
        Rule::RemoveCategory("dangerous".to_string()),
        Rule::AddCommand("keys".to_string()),
        Rule::RemoveCommand("info".to_string()),
        // Database restrictions: -select
        Rule::RemoveCommand("select".to_string()),
        // Password
        Rule::AddPass(password.to_string()),
        // Add default queue pattern - uses hashtag {DEFAULT_QUEUE_NAME} for Redis cluster routing
        Rule::Pattern(format!("asynq:{{{}}}:*", DEFAULT_QUEUE_NAME)),
        // Add tenant-specific key patterns
        Rule::Pattern(format!("asynq:{{{}:*", username)),
        // Add default key patterns
        Rule::Pattern("asynq:queues".to_string()),
        Rule::Pattern("asynq:servers:*".to_string()),
        Rule::Pattern("asynq:servers".to_string()),
        Rule::Pattern("asynq:workers".to_string()),
        Rule::Pattern("asynq:workers:*".to_string()),
        Rule::Pattern("asynq:schedulers".to_string()),
        Rule::Pattern("asynq:schedulers:*".to_string()),
        Rule::Channel("asynq:cancel".to_string()),
    ];
    assert_eq!(conn.acl_setuser_rules(username, &rules), Ok(()));
    let info = conn.acl_getuser(username).expect("Got user");
    assert!(info.is_some());
    let info = info.expect("Got asynq");
    assert_eq!(
        info.flags,
        vec![Rule::On, Rule::Other("sanitize-payload".to_string())]
    );
    assert_eq!(
        info.passwords,
        vec![Rule::AddHashedPass(
            "dda69783f28fdf6f1c5a83e8400f2472e9300887d1dffffe12a07b92a3d0aa25".to_string()
        )]
    );
    assert_eq!(
        info.commands,
        vec![
            Rule::AddCategory("all".to_string()),
            Rule::RemoveCategory("dangerous".to_string()),
            Rule::AddCommand("keys".to_string()),
            Rule::RemoveCommand("info".to_string()),
            Rule::RemoveCommand("select".to_string()),
        ]
    );
    assert_eq!(
        info.keys,
        vec![
            Rule::Pattern("asynq:{default}:*".to_string()),
            Rule::Pattern("asynq:{tenant:*".to_string()),
            Rule::Pattern("asynq:queues".to_string()),
            Rule::Pattern("asynq:servers:*".to_string()),
            Rule::Pattern("asynq:servers".to_string()),
            Rule::Pattern("asynq:workers".to_string()),
            Rule::Pattern("asynq:workers:*".to_string()),
            Rule::Pattern("asynq:schedulers".to_string()),
            Rule::Pattern("asynq:schedulers:*".to_string()),
        ]
    );
    assert_eq!(
        info.channels,
        vec![Rule::Channel("asynq:cancel".to_string())]
    );
    assert_eq!(info.selectors, vec![]);
}
#[test]
fn test_acl_sample_info() {
    let ctx = TestContext::new();
    let mut conn = ctx.connection();
    let sample_rule = vec![
        Rule::On,
        Rule::NoPass,
        Rule::AddCommand("GET".to_string()),
        Rule::AllKeys,
        Rule::Channel("*".to_string()),
        // Rule::Selector(vec!["+SET".to_string(), "~key2".to_string()]),
    ];
    conn.acl_setuser_rules("sample", &sample_rule)
        .expect("Set sample user");
    let sample_user = conn.acl_getuser("sample").expect("Got user");
    let sample_user = sample_user.expect("Got sample user");
    assert_eq!(
        sample_user.flags,
        vec![
            Rule::On,
            Rule::NoPass,
            Rule::Other("sanitize-payload".to_string())
        ]
    );
    assert_eq!(sample_user.passwords, vec![]);
    assert_eq!(
        sample_user.commands,
        vec![
            Rule::RemoveCategory("all".to_string()),
            Rule::AddCommand("get".to_string()),
        ]
    );
    assert_eq!(sample_user.keys, vec![Rule::AllKeys]);
    assert_eq!(sample_user.channels, vec![Rule::Channel("*".to_string())]);
    // assert_eq!(
    //     sample_user.selectors,
    //     vec![
    //         Rule::Selector(vec!["commands".to_string()]),
    //         Rule::Selector(vec!["-@all".to_string(), "+set".to_string()]),
    //         Rule::Selector(vec!["keys".to_string()]),
    //         Rule::Selector(vec!["~key2".to_string()]),
    //         Rule::Selector(vec!["channels".to_string()]),
    //         Rule::Selector(vec!["".to_string()])
    //     ]
    // );
}
