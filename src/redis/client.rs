use std::io::net::ip::SocketAddr;
use std::io::net::get_host_addresses;
use std::io::net::tcp::TcpStream;
use std::from_str::from_str;

use url::Url;

use enums::*;
use connection::Connection;

pub struct Client {
    addr: SocketAddr,
    db: i64,
}

impl Client {

    /// creates a client.  The client will immediately connect but it will
    /// close the connection again until get_connection() is called.  The name
    /// resolution currently only happens initially.
    pub fn open(uri: &str) -> Result<Client, ConnectFailure> {
        let parsed_uri = try_unwrap!(from_str::<Url>(uri), Err(InvalidURI));
        ensure!(parsed_uri.scheme == "redis".to_string(), Err(InvalidURI));

        let ip_addrs = match get_host_addresses(parsed_uri.host.as_slice()) {
            Ok(x) => x,
            Err(_) => { return Err(InvalidURI); }
        };
        let ip_addr = try_unwrap!(ip_addrs.iter().next(), Err(HostNotFound));
        let port = parsed_uri.port.clone().unwrap_or(6379u16);
        let db = from_str::<i64>(parsed_uri.path.path.as_slice().trim_chars('/')).unwrap_or(0);

        let addr = SocketAddr {
            ip: *ip_addr,
            port: port
        };

        // make sure we can connect.
        match TcpStream::connect(addr.ip.to_string().as_slice(), addr.port) {
            Err(_) => { return Err(ConnectionRefused); }
            Ok(_) => {}
        }

        Ok(Client {
            addr: addr,
            db: db,
        })
    }

    /// returns an independent connection for this client.  This currently
    /// does not put it into a pool.
    pub fn get_connection(&self) -> Result<Connection, ConnectFailure> {
        Connection::new(self.addr, self.db)
    }
}
