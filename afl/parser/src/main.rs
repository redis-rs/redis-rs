use afl::fuzz;

use redis::{Parser, parse_redis_value};

fn main() {
    fuzz!(|data: &[u8]| {
        let _ = parse_redis_value(data);
        let mut parser = Parser::new().set_max_response_bytes(Some(1024));
        let mut reader = std::io::Cursor::new(data);
        let _ = parser.parse_value(&mut reader);
        let _ = parser.parse_value(&mut reader);
    });
}
