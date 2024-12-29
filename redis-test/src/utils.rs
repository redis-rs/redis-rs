use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::{fs, process};

use socket2::{Domain, Socket, Type};
use tempfile::TempDir;

#[derive(Clone, Debug)]
pub struct TlsFilePaths {
    pub redis_crt: PathBuf,
    pub redis_key: PathBuf,
    pub ca_crt: PathBuf,
}

pub fn build_keys_and_certs_for_tls(tempdir: &TempDir) -> TlsFilePaths {
    // Based on shell script in redis's server tests
    // https://github.com/redis/redis/blob/8c291b97b95f2e011977b522acf77ead23e26f55/utils/gen-test-certs.sh
    let ca_crt = tempdir.path().join("ca.crt");
    let ca_key = tempdir.path().join("ca.key");
    let ca_serial = tempdir.path().join("ca.txt");
    let redis_crt = tempdir.path().join("redis.crt");
    let redis_key = tempdir.path().join("redis.key");
    let ext_file = tempdir.path().join("openssl.cnf");

    fn make_key<S: AsRef<std::ffi::OsStr>>(name: S, size: usize) {
        process::Command::new("openssl")
            .arg("genrsa")
            .arg("-out")
            .arg(name)
            .arg(format!("{size}"))
            .stdout(process::Stdio::piped())
            .stderr(process::Stdio::piped())
            .spawn()
            .expect("failed to spawn openssl")
            .wait()
            .expect("failed to create key");
    }

    // Build CA Key
    make_key(&ca_key, 4096);

    // Build redis key
    make_key(&redis_key, 2048);

    // Build CA Cert
    process::Command::new("openssl")
        .arg("req")
        .arg("-x509")
        .arg("-new")
        .arg("-nodes")
        .arg("-sha256")
        .arg("-key")
        .arg(&ca_key)
        .arg("-days")
        .arg("3650")
        .arg("-subj")
        .arg("/O=Redis Test/CN=Certificate Authority")
        .arg("-out")
        .arg(&ca_crt)
        .stdout(process::Stdio::piped())
        .stderr(process::Stdio::piped())
        .spawn()
        .expect("failed to spawn openssl")
        .wait()
        .expect("failed to create CA cert");

    // Build x509v3 extensions file
    fs::write(
        &ext_file,
        b"keyUsage = digitalSignature, keyEncipherment\n\
    subjectAltName = @alt_names\n\
    [alt_names]\n\
    IP.1 = 127.0.0.1\n",
    )
    .expect("failed to create x509v3 extensions file");

    // Read redis key
    let mut key_cmd = process::Command::new("openssl")
        .arg("req")
        .arg("-new")
        .arg("-sha256")
        .arg("-subj")
        .arg("/O=Redis Test/CN=Generic-cert")
        .arg("-key")
        .arg(&redis_key)
        .stdout(process::Stdio::piped())
        .stderr(process::Stdio::piped())
        .spawn()
        .expect("failed to spawn openssl");

    // build redis cert
    process::Command::new("openssl")
        .arg("x509")
        .arg("-req")
        .arg("-sha256")
        .arg("-CA")
        .arg(&ca_crt)
        .arg("-CAkey")
        .arg(&ca_key)
        .arg("-CAserial")
        .arg(&ca_serial)
        .arg("-CAcreateserial")
        .arg("-days")
        .arg("365")
        .arg("-extfile")
        .arg(&ext_file)
        .arg("-out")
        .arg(&redis_crt)
        .stdin(key_cmd.stdout.take().expect("should have stdout"))
        .stdout(process::Stdio::piped())
        .stderr(process::Stdio::piped())
        .spawn()
        .expect("failed to spawn openssl")
        .wait()
        .expect("failed to create redis cert");

    key_cmd.wait().expect("failed to create redis key");

    TlsFilePaths {
        redis_crt,
        redis_key,
        ca_crt,
    }
}

pub fn get_listener_on_free_port() -> TcpListener {
    let addr = &"127.0.0.1:0".parse::<SocketAddr>().unwrap().into();
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
    socket.set_reuse_address(true).unwrap();
    socket.bind(addr).unwrap();
    socket.listen(1).unwrap();
    TcpListener::from(socket)
}

/// Finds a random open port available for listening at, by spawning a TCP server with
/// port "zero" (which prompts the OS to just use any available port). Between calling
/// this function and trying to bind to this port, the port may be given to another
/// process, so this must be used with care (since here we only use it for tests, it's
/// mostly okay).
pub fn get_random_available_port() -> u16 {
    for _ in 0..10000 {
        let listener = get_listener_on_free_port();
        let port = listener.local_addr().unwrap().port();
        if port < 55535 {
            return port;
        }
    }
    panic!("Couldn't get a valid port");
}
