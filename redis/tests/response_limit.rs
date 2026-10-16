use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use redis::{cmd, pipe, Client, ConnectionConfig, ErrorKind, Value};

const RESPONSE_LIMIT: usize = 32;
const LARGE_PAYLOAD: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

fn start_server() -> (String, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    (address.to_string(), listener)
}

fn read_command(reader: &mut BufReader<TcpStream>) -> Vec<Vec<u8>> {
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line).unwrap();
    assert!(line.starts_with(b"*"), "expected RESP array, got {line:?}");
    let count: usize = std::str::from_utf8(&line[1..line.len() - 2])
        .unwrap()
        .parse()
        .unwrap();

    (0..count)
        .map(|_| {
            line.clear();
            reader.read_until(b'\n', &mut line).unwrap();
            assert!(
                line.starts_with(b"$"),
                "expected bulk argument, got {line:?}"
            );
            let len: usize = std::str::from_utf8(&line[1..line.len() - 2])
                .unwrap()
                .parse()
                .unwrap();
            let mut argument = vec![0; len];
            reader.read_exact(&mut argument).unwrap();
            let mut crlf = [0; 2];
            reader.read_exact(&mut crlf).unwrap();
            assert_eq!(&crlf, b"\r\n");
            argument
        })
        .collect()
}

fn assert_command(reader: &mut BufReader<TcpStream>, expected: &[&[u8]]) {
    let actual = read_command(reader);
    assert_eq!(actual, expected);
}

fn oversized_bulk_frame() -> Vec<u8> {
    let mut frame = format!("${}\r\n", LARGE_PAYLOAD.len()).into_bytes();
    frame.extend_from_slice(LARGE_PAYLOAD);
    frame.extend_from_slice(b"\r\n");
    assert!(frame.len() > RESPONSE_LIMIT);
    frame
}

fn resp2_client(address: &str) -> Client {
    Client::open(format!("redis://{address}/?protocol=resp2")).unwrap()
}

fn finish_resp2_setup(reader: &mut BufReader<TcpStream>) {
    #[cfg(feature = "disable-client-setinfo")]
    let _ = reader;
    #[cfg(not(feature = "disable-client-setinfo"))]
    {
        assert_command(reader, &[b"CLIENT", b"SETINFO", b"LIB-NAME", b"redis-rs"]);
        assert_command(
            reader,
            &[
                b"CLIENT",
                b"SETINFO",
                b"LIB-VER",
                env!("CARGO_PKG_VERSION").as_bytes(),
            ],
        );
        reader.get_mut().write_all(b"+OK\r\n+OK\r\n").unwrap();
    }
}

#[test]
fn sync_connection_drops_oversized_response_and_reads_next_response() {
    let (address, listener) = start_server();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream);
        finish_resp2_setup(&mut reader);
        assert_command(&mut reader, &[b"GET", b"large"]);
        let mut response = oversized_bulk_frame();
        response.extend_from_slice(b"+PONG\r\n");
        reader.get_mut().write_all(&response).unwrap();
        assert_command(&mut reader, &[b"PING"]);
    });

    let client = resp2_client(&address);
    let mut connection = client
        .get_connection_with_config(
            &ConnectionConfig::new().set_max_response_bytes(Some(RESPONSE_LIMIT)),
        )
        .unwrap();
    let error = cmd("GET")
        .arg("large")
        .query::<Value>(&mut connection)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ResponseTooLarge, "{error:?}");
    let pong: String = cmd("PING").query(&mut connection).unwrap();
    assert_eq!(pong, "PONG");
    server.join().unwrap();
}

#[test]
fn sync_pipeline_drains_oversized_response_and_keeps_following_responses_aligned() {
    let (address, listener) = start_server();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream);
        finish_resp2_setup(&mut reader);
        assert_command(&mut reader, &[b"GET", b"large"]);
        assert_command(&mut reader, &[b"GET", b"small"]);
        let mut responses = oversized_bulk_frame();
        responses.extend_from_slice(b"+SMALL\r\n");
        reader.get_mut().write_all(&responses).unwrap();
        assert_command(&mut reader, &[b"PING"]);
        reader.get_mut().write_all(b"+PONG\r\n").unwrap();
    });

    let client = resp2_client(&address);
    let mut connection = client
        .get_connection_with_config(
            &ConnectionConfig::new().set_max_response_bytes(Some(RESPONSE_LIMIT)),
        )
        .unwrap();
    let error = pipe()
        .cmd("GET")
        .arg("large")
        .cmd("GET")
        .arg("small")
        .query::<Vec<Value>>(&mut connection)
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ResponseTooLarge, "{error:?}");
    let pong: String = cmd("PING").query(&mut connection).unwrap();
    assert_eq!(pong, "PONG");
    server.join().unwrap();
}
