use redis::{Commands, ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use std::env;

fn do_redis_code(connection_info: ConnectionInfo) -> redis::RedisResult<()> {
    let client = redis::Client::open(connection_info)?;

    let mut con = client.get_connection()?;

    let keys: Vec<String> = con.keys("*")?;
    println!("Redis keys: {:?}", keys);

    Ok(())
}

fn main() {
    let ca_cert = if env::args().nth(1) == Some("--cacert".into()) {
        let file_path = env::args().nth(2).unwrap();
        Some(
            std::fs::read_to_string(file_path)
                .expect("Should have been able to read the cacert file"),
        )
    } else {
        None
    };

    let info = ConnectionInfo {
        addr: ConnectionAddr::TcpTls {
            host: "127.0.0.1".into(),
            port: 6380,
            insecure: false,
            ca_cert: ca_cert,
        },
        redis: RedisConnectionInfo {
            db: 0,
            username: None,
            password: None,
        },
    };

    if let Err(err) = do_redis_code(info) {
        println!("Could not execute example:");
        println!("  {}: {}", err.category(), err);
    }
}
