use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use redis::{cmd, pipe, Client, ConnectionConfig, ErrorKind, Value};

#[cfg(feature = "aio")]
use redis::AsyncConnectionConfig;

const RESPONSE_LIMIT: usize = 32;
const LARGE_PAYLOAD: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

fn start_server() -> (String, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock Redis listener");
    let address = listener.local_addr().expect("mock Redis address");
    (address.to_string(), listener)
}

fn read_command(reader: &mut BufReader<TcpStream>) -> Vec<Vec<u8>> {
    let mut line = Vec::new();
    reader
        .read_until(b'\n', &mut line)
        .expect("read array header");
    assert!(line.starts_with(b"*"), "expected RESP array, got {line:?}");
    let count: usize = std::str::from_utf8(&line[1..line.len() - 2])
        .expect("array length is UTF-8")
        .parse()
        .expect("array length is numeric");

    (0..count)
        .map(|_| {
            line.clear();
            reader
                .read_until(b'\n', &mut line)
                .expect("read bulk header");
            assert!(
                line.starts_with(b"$"),
                "expected bulk argument, got {line:?}"
            );
            let len: usize = std::str::from_utf8(&line[1..line.len() - 2])
                .expect("bulk length is UTF-8")
                .parse()
                .expect("bulk length is numeric");
            let mut argument = vec![0; len];
            reader
                .read_exact(&mut argument)
                .expect("read command argument");
            let mut crlf = [0; 2];
            reader
                .read_exact(&mut crlf)
                .expect("read argument terminator");
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
    Client::open(format!("redis://{address}/?protocol=resp2")).expect("create RESP2 client")
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
        reader
            .get_mut()
            .write_all(b"+OK\r\n+OK\r\n")
            .expect("write RESP2 setup responses");
    }
}

#[test]
fn sync_connection_drops_oversized_response_and_reads_next_response() {
    let (address, listener) = start_server();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept sync client");
        let mut reader = BufReader::new(stream);
        finish_resp2_setup(&mut reader);
        assert_command(&mut reader, &[b"GET", b"large"]);
        let mut response = oversized_bulk_frame();
        response.extend_from_slice(b"+PONG\r\n");
        reader
            .get_mut()
            .write_all(&response)
            .expect("write oversized and following responses");
        assert_command(&mut reader, &[b"PING"]);
    });

    let client = resp2_client(&address);
    let mut connection = client
        .get_connection_with_config(
            &ConnectionConfig::new().set_max_response_bytes(Some(RESPONSE_LIMIT)),
        )
        .expect("connect with response limit");
    let error = cmd("GET")
        .arg("large")
        .query::<Value>(&mut connection)
        .expect_err("oversized response must be rejected");
    assert_eq!(error.kind(), ErrorKind::ResponseTooLarge, "{error:?}");
    let pong: String = cmd("PING")
        .query(&mut connection)
        .expect("connection remains usable after dropping oversized frame");
    assert_eq!(pong, "PONG");
    server.join().expect("join mock Redis server");
}

#[test]
fn sync_pipeline_drains_oversized_response_and_keeps_following_responses_aligned() {
    let (address, listener) = start_server();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept sync client");
        let mut reader = BufReader::new(stream);
        finish_resp2_setup(&mut reader);
        assert_command(&mut reader, &[b"GET", b"large"]);
        assert_command(&mut reader, &[b"GET", b"small"]);
        let mut responses = oversized_bulk_frame();
        responses.extend_from_slice(b"+SMALL\r\n");
        reader
            .get_mut()
            .write_all(&responses)
            .expect("write pipeline responses");
        assert_command(&mut reader, &[b"PING"]);
        reader
            .get_mut()
            .write_all(b"+PONG\r\n")
            .expect("write PING response");
    });

    let client = resp2_client(&address);
    let mut connection = client
        .get_connection_with_config(
            &ConnectionConfig::new().set_max_response_bytes(Some(RESPONSE_LIMIT)),
        )
        .expect("connect with response limit");
    let error = pipe()
        .cmd("GET")
        .arg("large")
        .cmd("GET")
        .arg("small")
        .query::<Vec<Value>>(&mut connection)
        .expect_err("pipeline containing an oversized response must fail");
    assert_eq!(error.kind(), ErrorKind::ResponseTooLarge, "{error:?}");
    let pong: String = cmd("PING")
        .query(&mut connection)
        .expect("pipeline must leave the connection aligned and usable");
    assert_eq!(pong, "PONG");
    server.join().expect("join mock Redis server");
}

#[cfg(feature = "tokio-comp")]
fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build Tokio runtime")
}

#[cfg(feature = "tokio-comp")]
fn read_resp3_setup(reader: &mut BufReader<TcpStream>) {
    assert_command(reader, &[b"HELLO", b"3"]);
    reader
        .get_mut()
        .write_all(b"%1\r\n+server\r\n+redis\r\n")
        .expect("write HELLO response");
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
        reader
            .get_mut()
            .write_all(b"+OK\r\n+OK\r\n")
            .expect("write RESP3 setup responses");
    }
}

#[cfg(feature = "tokio-comp")]
fn resp3_client(address: &str) -> Client {
    Client::open(format!("redis://{address}/?protocol=resp3")).expect("create RESP3 client")
}

#[cfg(feature = "tokio-comp")]
#[test]
fn multiplexed_connection_drops_oversized_response_and_reads_next_response() {
    let runtime = current_thread_runtime();
    runtime.block_on(async {
        let (address, listener) = start_server();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept async client");
            let mut reader = BufReader::new(stream);
            read_resp3_setup(&mut reader);
            assert_command(&mut reader, &[b"GET", b"large"]);
            let mut response = oversized_bulk_frame();
            response.extend_from_slice(b"+PONG\r\n");
            reader
                .get_mut()
                .write_all(&response)
                .expect("write oversized response");
            assert_command(&mut reader, &[b"PING"]);
            reader
                .get_mut()
                .write_all(b"+PONG\r\n")
                .expect("write PING response");
        });

        let client = resp3_client(&address);
        let config = AsyncConnectionConfig::new()
            .set_max_response_bytes(Some(RESPONSE_LIMIT))
            .set_response_timeout(std::time::Duration::from_secs(2));
        let mut connection = client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .expect("connect multiplexed client");
        let error = cmd("GET")
            .arg("large")
            .query_async::<Value>(&mut connection)
            .await
            .expect_err("oversized response must be rejected");
        assert_eq!(error.kind(), ErrorKind::ResponseTooLarge, "{error:?}");
        let pong: String = cmd("PING")
            .query_async(&mut connection)
            .await
            .expect("multiplexed connection remains usable");
        assert_eq!(pong, "PONG");
        server.join().expect("join mock Redis server");
    });
}

#[cfg(feature = "tokio-comp")]
#[test]
fn multiplexed_pipeline_drains_oversized_response_and_keeps_following_responses_aligned() {
    let runtime = current_thread_runtime();
    runtime.block_on(async {
        let (address, listener) = start_server();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept async client");
            let mut reader = BufReader::new(stream);
            read_resp3_setup(&mut reader);
            assert_command(&mut reader, &[b"GET", b"large"]);
            assert_command(&mut reader, &[b"GET", b"small"]);
            let mut responses = oversized_bulk_frame();
            responses.extend_from_slice(b"+SMALL\r\n");
            reader
                .get_mut()
                .write_all(&responses)
                .expect("write pipeline responses");
            assert_command(&mut reader, &[b"PING"]);
            reader
                .get_mut()
                .write_all(b"+PONG\r\n")
                .expect("write PING response");
        });

        let client = resp3_client(&address);
        let config = AsyncConnectionConfig::new()
            .set_max_response_bytes(Some(RESPONSE_LIMIT))
            .set_response_timeout(std::time::Duration::from_secs(2));
        let mut connection = client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .expect("connect multiplexed client");
        let error = pipe()
            .cmd("GET")
            .arg("large")
            .cmd("GET")
            .arg("small")
            .query_async::<Vec<Value>>(&mut connection)
            .await
            .expect_err("pipeline containing an oversized response must fail");
        assert_eq!(error.kind(), ErrorKind::ResponseTooLarge, "{error:?}");
        let pong: String = cmd("PING")
            .query_async(&mut connection)
            .await
            .expect("pipeline must leave the multiplexed connection aligned");
        assert_eq!(pong, "PONG");
        server.join().expect("join mock Redis server");
    });
}

#[cfg(feature = "tokio-comp")]
#[test]
fn multiplexed_connection_drops_oversized_pubsub_message_and_receives_next_message() {
    let runtime = current_thread_runtime();
    runtime.block_on(async {
        let (address, listener) = start_server();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept async client");
            let mut reader = BufReader::new(stream);
            read_resp3_setup(&mut reader);
            assert_command(&mut reader, &[b"SUBSCRIBE", b"ch"]);
            reader
                .get_mut()
                .write_all(b">3\r\n+subscribe\r\n$2\r\nch\r\n:1\r\n")
                .expect("write subscription acknowledgement");

            let mut responses =
                format!(">3\r\n+message\r\n$2\r\nch\r\n${}\r\n", LARGE_PAYLOAD.len()).into_bytes();
            responses.extend_from_slice(LARGE_PAYLOAD);
            responses.extend_from_slice(b"\r\n>3\r\n+message\r\n$2\r\nch\r\n$2\r\nok\r\n");
            reader
                .get_mut()
                .write_all(&responses)
                .expect("write oversized and valid pubsub messages");

            assert_command(&mut reader, &[b"PING"]);
            reader
                .get_mut()
                .write_all(b"+PONG\r\n")
                .expect("write PING response");
        });

        let client = resp3_client(&address);
        let (push_sender, mut push_receiver) = tokio::sync::mpsc::unbounded_channel();
        let config = AsyncConnectionConfig::new()
            .set_max_response_bytes(Some(RESPONSE_LIMIT))
            .set_response_timeout(std::time::Duration::from_secs(2))
            .set_push_sender(push_sender);
        let mut connection = client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .expect("connect multiplexed client with push sender");
        connection
            .subscribe("ch")
            .await
            .expect("subscribe to test channel");
        let subscribe =
            tokio::time::timeout(std::time::Duration::from_secs(2), push_receiver.recv())
                .await
                .expect("subscription acknowledgement arrives")
                .expect("push channel remains open");
        assert_eq!(subscribe.kind, redis::PushKind::Subscribe);

        let message = tokio::time::timeout(std::time::Duration::from_secs(2), push_receiver.recv())
            .await
            .expect("valid message arrives after oversized message was dropped")
            .expect("push channel remains open");
        assert_eq!(message.kind, redis::PushKind::Message);
        assert_eq!(message.data[1], Value::BulkString(b"ok".to_vec()));

        let pong: String = cmd("PING")
            .query_async(&mut connection)
            .await
            .expect("multiplexed connection remains usable after dropping pubsub frame");
        assert_eq!(pong, "PONG");
        server.join().expect("join mock Redis server");
    });
}
