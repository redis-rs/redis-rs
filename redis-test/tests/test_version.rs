use redis_test::{AvailableComponents, REDIS_CE_6_0, TestContext};

/// Tries to assure that the current server allows to parse the versions
#[test]
fn live_server() {
    let ctx = TestContext::default();
    let mut conn = ctx.connection();

    let components = AvailableComponents::from(&mut conn);

    // Check that the minimum supported version is available
    assert!(components.supports(REDIS_CE_6_0));

    let non_existent_component = ("foo", (23, 42, 4711));
    assert!(!components.supports(non_existent_component));
}
