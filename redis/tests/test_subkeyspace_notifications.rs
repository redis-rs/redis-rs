#![allow(clippy::let_unit_value)]

//! Integration tests for Redis 8.8 subkey notifications.
//!
//! Four channel families are exercised.
//! Each is gated by a single letter in `notify-keyspace-events`:
//!
//! | Flag | Channel pattern                              | Payload                              |
//! |------|----------------------------------------------|--------------------------------------|
//! | `S`  | `__subkeyspace@<db>__:<key>`                 | `<event>|<len>:<sk>[,<len>:<sk>...]` |
//! | `T`  | `__subkeyevent@<db>__:<event>`               | `<keylen>:<key>|<len>:<sk>[,...]`    |
//! | `I`  | `__subkeyspaceitem@<db>__:<key>\n<sk>`       | `<event>`                            |
//! | `V`  | `__subkeyspaceevent@<db>__:<event>|<key>`    | `<len>:<sk>[,...]`                   |

#[macro_use]
mod support;

#[cfg(test)]
mod subkeyspace_notifications_tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use crate::support::*;
    use redis::{Commands, cmd};

    // ---------- payload / channel parsers ---------------------------------

    /// Parses length-prefixed, comma-separated lists (e.g. `<len>:<sk>[,<len>:<sk>...]`).
    fn parse_length_prefixed_list(mut rest: &[u8]) -> Option<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        while !rest.is_empty() {
            let colon = rest.iter().position(|&b| b == b':')?;
            let len: usize = std::str::from_utf8(&rest[..colon]).ok()?.parse().ok()?;
            rest = &rest[colon + 1..];
            if rest.len() < len {
                return None;
            }
            out.push(rest[..len].to_vec());
            rest = &rest[len..];
            if rest.first() == Some(&b',') {
                rest = &rest[1..];
            }
        }
        Some(out)
    }

    /// `__subkeyspace@<db>__:<key>` payload -> (event, subkeys).
    fn parse_subkeyspace_payload(p: &[u8]) -> Option<(String, Vec<Vec<u8>>)> {
        let bar = p.iter().position(|&b| b == b'|')?;
        let event = std::str::from_utf8(&p[..bar]).ok()?.to_owned();
        Some((event, parse_length_prefixed_list(&p[bar + 1..])?))
    }

    /// `__subkeyevent@<db>__:<event>` payload -> (key, subkeys).
    fn parse_subkeyevent_payload(p: &[u8]) -> Option<(Vec<u8>, Vec<Vec<u8>>)> {
        let bar = p.iter().position(|&b| b == b'|')?;
        let head = &p[..bar];
        let colon = head.iter().position(|&b| b == b':')?;
        let keylen: usize = std::str::from_utf8(&head[..colon]).ok()?.parse().ok()?;
        if head.len() != colon + 1 + keylen {
            return None;
        }
        Some((
            head[colon + 1..].to_vec(),
            parse_length_prefixed_list(&p[bar + 1..])?,
        ))
    }

    /// `__subkeyspaceitem@<db>__:<key>\n<sk>` channel -> (key, subkey).
    fn parse_subkeyspaceitem_channel(channel: &str, db: u32) -> Option<(&str, &str)> {
        let suffix = channel.strip_prefix(&format!("__subkeyspaceitem@{db}__:"))?;
        let nl = suffix.find('\n')?;
        Some((&suffix[..nl], &suffix[nl + 1..]))
    }

    /// `__subkeyspaceevent@<db>__:<event>|<key>` channel -> (event, key).
    fn parse_subkeyspaceevent_channel(channel: &str, db: u32) -> Option<(&str, &str)> {
        let suffix = channel.strip_prefix(&format!("__subkeyspaceevent@{db}__:"))?;
        let bar = suffix.find('|')?;
        Some((&suffix[..bar], &suffix[bar + 1..]))
    }

    // ---------- parser unit tests -----------------------------------------

    #[test]
    fn subkeyspace_payload_single_subkey() {
        let (event, sks) = parse_subkeyspace_payload(b"hset|6:field1").unwrap();
        assert_eq!(event, "hset");
        assert_eq!(sks, vec![b"field1".to_vec()]);
    }

    #[test]
    fn subkeyspace_payload_multi_subkeys() {
        let (event, sks) = parse_subkeyspace_payload(b"hset|3:abc,2:xx,5:hello").unwrap();
        assert_eq!(event, "hset");
        assert_eq!(
            sks,
            vec![b"abc".to_vec(), b"xx".to_vec(), b"hello".to_vec()]
        );
    }

    #[test]
    fn subkeyspace_payload_length_prefix_protects_delimiters() {
        let (event, sks) = parse_subkeyspace_payload(b"hset|5:a,b:c,3:xyz").unwrap();
        assert_eq!(event, "hset");
        assert_eq!(sks, vec![b"a,b:c".to_vec(), b"xyz".to_vec()]);
    }

    #[test]
    fn subkeyspace_payload_rejects_malformed() {
        assert!(parse_subkeyspace_payload(b"no_pipe").is_none());
        assert!(parse_subkeyspace_payload(b"hset|99:abc").is_none());
        assert!(parse_subkeyspace_payload(b"hset|x:abc").is_none());
    }

    #[test]
    fn subkeyevent_payload_round_trip() {
        let (key, sks) = parse_subkeyevent_payload(b"10:__fuzz_h__|3:abc,2:xx").unwrap();
        assert_eq!(key, b"__fuzz_h__".to_vec());
        assert_eq!(sks, vec![b"abc".to_vec(), b"xx".to_vec()]);
    }

    #[test]
    fn subkeyspaceitem_channel_round_trip() {
        let (k, sk) =
            parse_subkeyspaceitem_channel("__subkeyspaceitem@0__:myhash\nfield1", 0).unwrap();
        assert_eq!(k, "myhash");
        assert_eq!(sk, "field1");
    }

    #[test]
    fn subkeyspaceevent_channel_round_trip() {
        let (event, key) =
            parse_subkeyspaceevent_channel("__subkeyspaceevent@0__:hset|myhash", 0).unwrap();
        assert_eq!(event, "hset");
        assert_eq!(key, "myhash");
    }

    /// Enables `KEA<extra>` on the server and asserts the extra flag was kept.
    fn enable_flags(con: &mut redis::Connection, extra: char) {
        let flags = format!("KEA{extra}");
        cmd("CONFIG")
            .arg("SET")
            .arg("notify-keyspace-events")
            .arg(&flags)
            .exec(con)
            .unwrap();
        // Decode as a tuple both RESP2 (array of [name, value])
        // and RESP3 (single-entry map) replies are accepted.
        let (_, stored): (String, String) = cmd("CONFIG")
            .arg("GET")
            .arg("notify-keyspace-events")
            .query(con)
            .unwrap();
        assert!(
            stored.contains(extra),
            "server accepted '{flags}' but did not retain '{extra}' (stored={stored:?})"
        );
    }

    fn pubsub_conn(ctx: &TestContext) -> redis::Connection {
        let con = ctx.connection();
        // Safeguard get_message() so failures don't hang CI.
        con.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        con
    }

    #[test]
    fn test_subkeyspace_flag_s() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8);
        let key = "__test_subks_s__";
        let mut con = ctx.connection();
        enable_flags(&mut con, 'S');

        let mut sub_conn = pubsub_conn(&ctx);
        let mut pubsub = sub_conn.as_pubsub();
        pubsub
            .psubscribe(format!("__subkeyspace@0__:{key}"))
            .unwrap();

        let _: () = con
            .hset_multiple(key, &[("abc", "x"), ("xx", "y"), ("hello", "z")])
            .unwrap();
        let _: usize = con.hdel(key, "abc").unwrap();

        let payload1: Vec<u8> = pubsub.get_message().unwrap().get_payload().unwrap();
        let (e1, mut s1) = parse_subkeyspace_payload(&payload1)
            .unwrap_or_else(|| panic!("could not parse hset payload {payload1:?}"));
        assert_eq!(e1, "hset");
        s1.sort();
        assert_eq!(s1, vec![b"abc".to_vec(), b"hello".to_vec(), b"xx".to_vec()]);

        let payload2: Vec<u8> = pubsub.get_message().unwrap().get_payload().unwrap();
        let (e2, s2) = parse_subkeyspace_payload(&payload2)
            .unwrap_or_else(|| panic!("could not parse hdel payload {payload2:?}"));
        assert_eq!(e2, "hdel");
        assert_eq!(s2, vec![b"abc".to_vec()]);
    }

    #[test]
    fn test_subkeyevent_flag_t() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8);
        let key = "__test_subks_t__";
        let mut con = ctx.connection();
        enable_flags(&mut con, 'T');

        let mut sub_conn = pubsub_conn(&ctx);
        let mut pubsub = sub_conn.as_pubsub();
        pubsub.psubscribe("__subkeyevent@0__:hset").unwrap();

        let _: () = con
            .hset_multiple(key, &[("abc", "x"), ("xx", "y")])
            .unwrap();

        let msg = pubsub.get_message().unwrap();
        assert_eq!(msg.get_channel_name(), "__subkeyevent@0__:hset");
        let payload: Vec<u8> = msg.get_payload().unwrap();
        let (notification_key, mut sks) = parse_subkeyevent_payload(&payload)
            .unwrap_or_else(|| panic!("could not parse {payload:?}"));
        assert_eq!(notification_key, key.as_bytes());
        sks.sort();
        assert_eq!(sks, vec![b"abc".to_vec(), b"xx".to_vec()]);
    }

    #[test]
    fn test_subkeyspaceitem_flag_i() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8);
        let key = "__test_subks_i__";
        let mut con = ctx.connection();
        enable_flags(&mut con, 'I');

        let mut sub_conn = pubsub_conn(&ctx);
        let mut pubsub = sub_conn.as_pubsub();
        pubsub.psubscribe("__subkeyspaceitem@0__:*").unwrap();

        let _: () = con
            .hset_multiple(key, &[("abc", "x"), ("xx", "y")])
            .unwrap();

        // Expect one notification per subkey in arbitrary order.
        let mut seen: BTreeMap<String, String> = BTreeMap::new();
        for _ in 0..2 {
            let msg = pubsub.get_message().unwrap();
            let chan = msg.get_channel_name().to_owned();
            let (notification_key, sk) = parse_subkeyspaceitem_channel(&chan, 0)
                .unwrap_or_else(|| panic!("could not parse channel {chan:?}"));
            assert_eq!(notification_key, key);
            seen.insert(sk.to_owned(), msg.get_payload::<String>().unwrap());
        }
        assert_eq!(
            seen.keys().cloned().collect::<Vec<_>>(),
            vec!["abc".to_owned(), "xx".to_owned()]
        );
        assert!(
            seen.values().all(|e| e == "hset"),
            "all payloads should be 'hset', got {seen:?}"
        );
    }

    #[test]
    fn test_subkeyspaceevent_flag_v() {
        let ctx = run_test_if_version_supported!(&REDIS_VERSION_CE_8_8);
        let key = "__test_subks_v__";
        let mut con = ctx.connection();
        enable_flags(&mut con, 'V');

        let mut sub_conn = pubsub_conn(&ctx);
        let mut pubsub = sub_conn.as_pubsub();
        pubsub
            .psubscribe(format!("__subkeyspaceevent@0__:*|{key}"))
            .unwrap();

        let _: () = con
            .hset_multiple(key, &[("abc", "x"), ("xx", "y"), ("hello", "z")])
            .unwrap();

        let msg = pubsub.get_message().unwrap();
        let chan = msg.get_channel_name().to_owned();
        let (event, notification_key) = parse_subkeyspaceevent_channel(&chan, 0)
            .unwrap_or_else(|| panic!("could not parse channel {chan:?}"));
        assert_eq!(event, "hset");
        assert_eq!(notification_key, key);

        let payload: Vec<u8> = msg.get_payload().unwrap();
        let mut sks = parse_length_prefixed_list(&payload)
            .unwrap_or_else(|| panic!("could not parse {payload:?}"));
        sks.sort();
        assert_eq!(
            sks,
            vec![b"abc".to_vec(), b"hello".to_vec(), b"xx".to_vec()]
        );
    }
}
