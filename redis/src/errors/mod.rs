mod parsing_error;
mod redis_error;
mod server_error;

pub use parsing_error::*;
pub use redis_error::*;
pub use server_error::*;

mod test {

    #[test]
    fn test_size() {
        println!(
            "Parsing error: {}",
            std::mem::size_of::<crate::ParsingError>()
        );
        println!(
            "Server error: {}",
            std::mem::size_of::<super::ServerError>()
        );
        println!("Redis error: {}", std::mem::size_of::<crate::RedisError>());
        println!("Value: {}", std::mem::size_of::<crate::Value>());
        println!(
            "Redis result: {}",
            std::mem::size_of::<crate::RedisResult<crate::Value>>()
        );
        println!("io::Error: {}", std::mem::size_of::<std::io::Error>());
        println!(
            "tuple: {}",
            std::mem::size_of::<(crate::ErrorKind, &'static str, arcstr::ArcStr)>()
        );
        println!(
            "tuple2: {}",
            std::mem::size_of::<(crate::ErrorKind, arcstr::ArcStr, arcstr::ArcStr)>()
        );
    }
}
