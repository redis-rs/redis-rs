#![allow(clippy::let_unit_value)]
#![cfg(feature = "bloom")]

mod support;

#[cfg(test)]
mod bloom {
    use crate::assert_args;
    use crate::support::*;
    use redis::bloom::ScalingOptions::{ExpansionRate, NonScaling};
    use redis::bloom::{DumpIterator, DumpResult, InfoType, InsertOptions, SingleInfoResponse};
    use redis::{bloom::AllInfoResponse, Commands, RedisResult, ToRedisArgs};
    use std::collections::VecDeque;
    use std::vec;

    #[test]
    fn test_module_bloom_add_exists() {
        let ctx = TestContext::with_modules(&[Module::Bloom], false);
        let mut con = ctx.connection();

        assert_eq!(con.bf_add("usernames", "funnyfred"), Ok(true));
        assert_eq!(con.bf_exists("usernames", "funnyfred"), Ok(true));
        assert_eq!(con.bf_exists("usernames", "sadjoe"), Ok(false));
    }

    #[test]
    fn test_module_bloom_madd_mexists() {
        let ctx = TestContext::with_modules(&[Module::Bloom], false);
        let mut con = ctx.connection();

        assert_eq!(
            con.bf_madd("bloomf", &["element1", "element2"]),
            Ok(vec![true, true])
        );
        assert_eq!(
            con.bf_mexists("bloomf", &["element1", "element2", "element3"]),
            Ok(vec![true, true, false])
        );
    }

    #[test]
    fn test_module_bloom_cardinality() {
        let ctx = TestContext::with_modules(&[Module::Bloom], false);
        let mut con = ctx.connection();

        assert_eq!(con.bf_add("usernames", "funnyfred"), Ok(true));
        assert_eq!(con.bf_cardinality("usernames"), Ok(1));

        assert_eq!(con.bf_cardinality("bf_new"), Ok(0));
    }

    #[test]
    fn test_module_bloom_reserve_options() {
        let ctx = TestContext::with_modules(&[Module::Bloom], false);
        let mut con = ctx.connection();

        assert_eq!(
            con.bf_reserve_options("bloomf", 0.001, 10000, ExpansionRate(2)),
            Ok(true)
        );
        assert_eq!(
            con.bf_reserve_options("bloomf2", 0.001, 10000, NonScaling),
            Ok(true)
        );
    }

    #[test]
    fn test_module_bloom_info_all() {
        let ctx = TestContext::with_modules(&[Module::Bloom], false);
        let mut con = ctx.connection();

        assert_eq!(
            con.bf_reserve_options("bloomf", 0.001, 10000, ExpansionRate(2)),
            Ok(true),
            "bf_reserve_options failed"
        );
        let results: Vec<bool> = con.bf_madd("bloomf", &["element1", "element2"]).unwrap();
        assert_eq!(results, vec![true, true], "bf_madd failed");
        let items: AllInfoResponse = con.bf_info_all("bloomf").unwrap();
        assert_eq!(items.capacity, 10000);
        assert_eq!(items.number_of_items_inserted, 2);
        assert_eq!(items.expansion_rate, Some(2));
    }

    #[test]
    fn test_module_bloom_info_nonscaling() {
        let ctx = TestContext::with_modules(&[Module::Bloom], false);
        let mut con = ctx.connection();

        assert_eq!(
            con.bf_reserve_options("bloomf", 0.001, 10000, NonScaling),
            Ok(true)
        );
        assert_eq!(
            con.bf_madd("bloomf", &["element1", "element2"]),
            Ok(vec![true, true])
        );
        let items: AllInfoResponse = con.bf_info_all("bloomf").unwrap();
        assert_eq!(items.capacity, 10000);
        assert_eq!(items.number_of_items_inserted, 2);
        assert_eq!(items.expansion_rate, None);
    }

    #[test]
    fn test_module_bloom_info_single_when_expansion_set() {
        let ctx = TestContext::with_modules(&[Module::Bloom], false);
        let mut con = ctx.connection();

        assert_eq!(
            con.bf_reserve_options("bloomf", 0.001, 10000, ExpansionRate(2)),
            Ok(true)
        );
        assert_eq!(
            con.bf_madd("bloomf", &["element1", "element2"]),
            Ok(vec![true, true])
        );
        let capacity: SingleInfoResponse = con.bf_info("bloomf", InfoType::Capacity).unwrap();
        assert_eq!(capacity.value, Some(10000));
        let number_of_items_inserted: SingleInfoResponse =
            con.bf_info("bloomf", InfoType::Items).unwrap();
        assert_eq!(number_of_items_inserted.value, Some(2));
        let expansion_rate: SingleInfoResponse =
            con.bf_info("bloomf", InfoType::Expansion).unwrap();
        assert_eq!(expansion_rate.value, Some(2));
    }

    #[test]
    fn test_module_bloom_info_single_when_nonscaling_set() {
        let ctx = TestContext::with_modules(&[Module::Bloom], false);
        let mut con = ctx.connection();

        assert_eq!(
            con.bf_reserve_options("bloomf", 0.001, 10000, NonScaling),
            Ok(true)
        );
        assert_eq!(
            con.bf_madd("bloomf", &["element1", "element2"]),
            Ok(vec![true, true])
        );
        let expansion_rate: SingleInfoResponse =
            con.bf_info("bloomf", InfoType::Expansion).unwrap();
        assert_eq!(expansion_rate.value, None);
    }

    #[test]
    fn test_module_bloom_insert_options() {
        let empty = InsertOptions::default();
        assert_eq!(ToRedisArgs::to_redis_args(&empty).len(), 0);

        let opts = InsertOptions::default()
            .with_nocreate()
            .with_scale(ExpansionRate(2))
            .with_error_rate(0.001);

        assert_args!(&opts, "NOCREATE", "EXPANSION", "2", "ERROR", "0.001");
    }
    #[test]
    fn test_module_bloom_insert() {
        let ctx = TestContext::with_modules(&[Module::Bloom], false);
        let mut con = ctx.connection();
        let opts = InsertOptions::default()
            // .nocreate()
            .with_capacity(100)
            .with_scale(ExpansionRate(2))
            .with_error_rate(0.001);

        assert_eq!(
            con.bf_insert("bloomf2", &["element1", "element2"], opts),
            Ok(vec![true, true])
        );
        let items: AllInfoResponse = con.bf_info_all("bloomf2").unwrap();
        assert_eq!(items.capacity, 100);
        assert_eq!(items.number_of_items_inserted, 2);
        assert_eq!(items.expansion_rate, Some(2));

        let opts = InsertOptions::default()
            // .nocreate()
            .with_capacity(100)
            .with_scale(ExpansionRate(2))
            .with_error_rate(0.001);

        assert_eq!(
            con.bf_insert("bloomf2", &["element3", "element4"], opts),
            Ok(vec![true, true])
        );
        let items: AllInfoResponse = con.bf_info_all("bloomf2").unwrap();
        assert_eq!(items.capacity, 100);
        assert_eq!(items.number_of_items_inserted, 4);
        assert_eq!(items.expansion_rate, Some(2));
    }
    #[test]
    fn test_module_bloom_insert_nocreate() {
        // if no create set redis returns an error if filter does not exists
        let ctx = TestContext::with_modules(&[Module::Bloom], false);
        let mut con = ctx.connection();
        let opts = InsertOptions::default().with_nocreate();

        let resp: RedisResult<Vec<String>> =
            con.bf_insert("bloomf3", &["element1", "element2"], opts);
        match resp {
            Ok(_) => {
                panic!("Expected an error but got Ok()");
            }
            Err(e) => {
                assert_eq!(
                    e.to_string(),
                    "An error was signalled by the server - ResponseError: not found"
                );
            }
        }
    }

    #[test]
    fn test_module_bloom_dump_and_load() {
        let ctx = TestContext::with_modules(&[Module::Bloom], false);
        let mut con = ctx.connection();
        let opts = InsertOptions::default()
            // .nocreate()
            .with_capacity(100)
            .with_scale(ExpansionRate(2))
            .with_error_rate(0.001);

        assert_eq!(
            con.bf_insert("bloomf2", &["element1", "element2"], opts),
            Ok(vec![true, true])
        );

        let original_info: AllInfoResponse = con.bf_info_all("bloomf2").unwrap();

        let mut iterator: i64 = 0;
        let mut chunks: VecDeque<DumpResult> = VecDeque::new();

        // dump the filter
        loop {
            let dump: DumpResult = con.bf_scandump("bloomf2", iterator).unwrap();

            if dump.iterator == 0 {
                break;
            } else {
                iterator = dump.iterator;
                chunks.push_back(dump);
            }
        }
        println!("completed dump for bloomf2");

        let delete_succeeded: bool = con.del("bloomf2").unwrap();
        assert!(delete_succeeded);

        // load the filter

        while let Some(dump_result) = chunks.pop_front() {
            let load_succeeded: bool = con
                .bf_loadchunk("bloomf3", dump_result.iterator, dump_result.data)
                .unwrap();
            assert!(load_succeeded);
        }

        println!("completed load for bloomf3");

        let new_filter_info: AllInfoResponse = con.bf_info_all("bloomf3").unwrap();
        assert_eq!(new_filter_info, original_info);

        // check if the elements are in the new filter
        let results: Vec<bool> = con
            .bf_mexists("bloomf3", &["element1", "element2"])
            .unwrap();
        assert_eq!(results, vec![true, true]);
    }

    #[test]
    fn test_module_bloom_dump_iterator() {
        let ctx = TestContext::with_modules(&[Module::Bloom], false);
        let mut con = ctx.connection();

        // create a filter
        let opts = InsertOptions::default()
            // .nocreate()
            .with_capacity(100)
            .with_scale(ExpansionRate(2))
            .with_error_rate(0.001);

        assert_eq!(
            con.bf_insert("bloomf2", &["element1", "element2"], opts),
            Ok(vec![true, true])
        );

        let original_info: AllInfoResponse = con.bf_info_all("bloomf2").unwrap();

        let mut chunks: VecDeque<DumpResult> = VecDeque::new();

        let dump_iterator = DumpIterator::new(&mut con, "bloomf2");
        for chunk_res in dump_iterator {
            match chunk_res {
                Ok(chunk) => chunks.push_back(chunk),
                Err(e) => panic!("Error while scanning dump: {:?}", e),
            }
        }
        println!("completed dump for bloomf2");

        let delete_succeeded: bool = con.del("bloomf2").unwrap();
        assert!(delete_succeeded);

        // load the filter

        while let Some(dump_result) = chunks.pop_front() {
            let load_succeeded: bool = con
                .bf_loadchunk("bloomf3", dump_result.iterator, &dump_result.data)
                .unwrap();
            assert!(load_succeeded);
        }

        println!("completed load for bloomf3");

        let new_filter_info: AllInfoResponse = con.bf_info_all("bloomf3").unwrap();
        assert_eq!(new_filter_info, original_info);
    }
}
