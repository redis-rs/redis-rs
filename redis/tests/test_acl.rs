#![cfg(feature = "acl")]

use std::collections::HashSet;

use redis::acl::{AclInfo, Rule};
use redis::{Commands, Value};

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
    let res: Vec<String> = con.acl_help().expect("Got help manual");
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
    let acl_info: AclInfo = con.acl_getuser("bob").expect("Got user");
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
    let res: HashSet<String> = con
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
    let logs: Vec<Value> = con.acl_log(1).expect("Got logs");
    assert_eq!(logs.len(), 0);
    assert_eq!(con.acl_log_reset(), Ok(()));
}
