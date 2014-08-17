use url::Url;
use std::io::{IoResult, IoError, InvalidInput};

use connection::Connection;


pub struct Client {
    host: String,
    port: u16,
    db: i64,
}

impl Client {

    pub fn open(uri: &str) -> IoResult<Client> {
        let u = try_unwrap!(from_str::<Url>(uri), Err(IoError {
            kind: InvalidInput,
            desc: "Redis URL did not parse",
            detail: None,
        }));
        ensure!(u.scheme.as_slice() == "redis", Err(IoError {
            kind: InvalidInput,
            desc: "URL provided is not a redis URL",
            detail: None,
        }));

        Ok(Client {
            host: u.host,
            port: u.port.unwrap_or(6379),
            db: match u.path.to_string().as_slice().trim_chars('/') {
                "" => 0,
                path => try_unwrap!(from_str::<i64>(path), Err(IoError {
                    kind: InvalidInput,
                    desc: "Path is not a valid redis database number",
                    detail: None,
                }))
            },
        })
    }

    pub fn get_connection(&self) -> IoResult<Connection> {
        Connection::new(self.host.as_slice(), self.port, self.db)
    }
}
