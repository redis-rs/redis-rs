use afl::fuzz;

use redis::parse_redis_value;

fn main() {
    fuzz!(|data: &[u8]| {
        let _ = parse_redis_value(data);
    });
}
