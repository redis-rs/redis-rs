use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::{fs, process};

use socket2::{Domain, Socket, Type};
use tempfile::TempDir;

#[derive(Clone, Debug)]
pub struct TlsFilePaths {
    pub redis_crt: PathBuf,
    pub redis_key: PathBuf,
    pub ca_crt: PathBuf,
}

/// Client certificate and key paths for mTLS authentication
#[derive(Clone, Debug)]
pub struct ClientCertPaths {
    pub client_crt: PathBuf,
    pub client_key: PathBuf,
}

pub fn build_keys_and_certs_for_tls(tempdir: &TempDir) -> TlsFilePaths {
    build_keys_and_certs_for_tls_ext(tempdir, true)
}

pub fn build_keys_and_certs_for_tls_ext(tempdir: &TempDir, with_ip_alts: bool) -> TlsFilePaths {
    build_keys_and_certs_for_tls_with_hostname(tempdir, with_ip_alts, None)
}

pub fn build_keys_and_certs_for_tls_with_hostname(
    tempdir: &TempDir,
    with_ip_alts: bool,
    dns_hostname: Option<&str>,
) -> TlsFilePaths {
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
    let status = process::Command::new("openssl")
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
    assert!(
        status.success(),
        "`openssl req` failed to create CA cert: {status}"
    );

    let hostname = dns_hostname.unwrap_or("localhost.example.com");

    // Build x509v3 extensions file
    let ext = if with_ip_alts {
        "\
            keyUsage = digitalSignature, keyEncipherment\n\
            subjectAltName = @alt_names\n\
            [alt_names]\n\
            IP.1 = 127.0.0.1\n\
            "
        .to_string()
    } else {
        format!(
            "\
            [req]\n\
            distinguished_name = req_distinguished_name\n\
            x509_extensions = v3_req\n\
            prompt = no\n\
            \n\
            [req_distinguished_name]\n\
            CN = {hostname}\n\
            \n\
            [v3_req]\n\
            basicConstraints = CA:FALSE\n\
            keyUsage = nonRepudiation, digitalSignature, keyEncipherment\n\
            subjectAltName = @alt_names\n\
            \n\
            [alt_names]\n\
            DNS.1 = {hostname}\n\
            "
        )
    };
    fs::write(&ext_file, ext).expect("failed to create x509v3 extensions file");

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
    let mut command2 = process::Command::new("openssl");
    command2
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
        .arg(&ext_file);
    if !with_ip_alts {
        command2.arg("-extensions").arg("v3_req");
    }
    let status2 = command2
        .arg("-out")
        .arg(&redis_crt)
        .stdin(key_cmd.stdout.take().expect("should have stdout"))
        .spawn()
        .expect("failed to spawn openssl")
        .wait()
        .expect("failed to create redis cert");

    let status = key_cmd.wait().expect("failed to create redis key");
    assert!(
        status.success(),
        "`openssl req` failed to create request for Redis cert: {status}"
    );
    assert!(
        status2.success(),
        "`openssl x509` failed to create Redis cert: {status2}"
    );

    TlsFilePaths {
        redis_crt,
        redis_key,
        ca_crt,
    }
}

/// Build a client certificate with a custom common name (CN) field
/// Redis 8.6+ allows certificate-based authentication where the common name (CN)
/// is mapped to an ACL username
pub fn build_client_cert_with_custom_cn(
    tempdir: &TempDir,
    common_name: &str,
    ca_crt: &PathBuf,
    ca_key: &PathBuf,
) -> ClientCertPaths {
    let client_crt = tempdir.path().join(format!("{common_name}.crt"));
    let client_key = tempdir.path().join(format!("{common_name}.key"));
    let ca_serial = tempdir.path().join("ca.txt");

    // Generate client private key
    let status = process::Command::new("openssl")
        .arg("genrsa")
        .arg("-out")
        .arg(&client_key)
        .arg("2048")
        .stdout(process::Stdio::piped())
        .stderr(process::Stdio::piped())
        .spawn()
        .expect("failed to spawn openssl")
        .wait()
        .expect("failed to create client key");
    assert!(
        status.success(),
        "`openssl genrsa` failed to create client key: {status}"
    );

    // Create a basic extensions file for X.509 v3 client certificate
    let client_ext_file = tempdir.path().join("client_ext.cnf");
    let client_ext_content = "\
        basicConstraints = CA:FALSE\n\
        keyUsage = digitalSignature, keyEncipherment\n\
    ";
    fs::write(&client_ext_file, client_ext_content)
        .expect("failed to create client extensions file");

    // Create certificate signing request with custom CN
    let mut csr_cmd = process::Command::new("openssl")
        .arg("req")
        .arg("-new")
        .arg("-sha256")
        .arg("-subj")
        .arg(format!("/O=Redis Test/CN={common_name}"))
        .arg("-key")
        .arg(&client_key)
        .stdout(process::Stdio::piped())
        .stderr(process::Stdio::piped())
        .spawn()
        .expect("failed to spawn openssl for CSR");

    // Sign the certificate with CA (X.509 v3)
    let cert_status = process::Command::new("openssl")
        .arg("x509")
        .arg("-req")
        .arg("-sha256")
        .arg("-CA")
        .arg(ca_crt)
        .arg("-CAkey")
        .arg(ca_key)
        .arg("-CAserial")
        .arg(&ca_serial)
        .arg("-CAcreateserial")
        .arg("-days")
        .arg("365")
        .arg("-extfile")
        .arg(&client_ext_file)
        .arg("-out")
        .arg(&client_crt)
        .stdin(csr_cmd.stdout.take().expect("should have stdout"))
        .spawn()
        .expect("failed to spawn openssl for certificate signing")
        .wait()
        .expect("failed to sign client certificate");

    let csr_status = csr_cmd.wait().expect("failed to create CSR");
    assert!(
        csr_status.success(),
        "`openssl req` failed to create CSR: {csr_status}"
    );
    assert!(
        cert_status.success(),
        "`openssl x509` failed to sign client certificate"
    );

    ClientCertPaths {
        client_crt,
        client_key,
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

/// The cluster bus of a Redis/Valkey cluster node listens at `port + 10000`.
pub const CLUSTER_BUS_PORT_OFFSET: u16 = 10_000;

/// First port of the range split between nextest's test slots.
const SLOT_PORTS_START: u16 = 10_000;
/// End (exclusive) of the slot range. Every slot port's cluster bus port then lands in
/// `20000..30000`, where no slot port lives, so one test's bus ports can never collide
/// with another test's server ports. Both ranges are below the Linux (`32768+`) and
/// Windows (`49152+`) ephemeral ranges, so outgoing connections are not assigned them
/// either.
const SLOT_PORTS_END: u16 = SLOT_PORTS_START + CLUSTER_BUS_PORT_OFFSET;
/// Ports owned by each slot. A test that allocates more than this cycles back through
/// its slot, skipping ports that are still bound.
const PORTS_PER_SLOT: u16 = 64;
// The lowest cluster bus port must be past every slot's ports.
const _: () = assert!(SLOT_PORTS_START + CLUSTER_BUS_PORT_OFFSET >= SLOT_PORTS_END);

/// The ports owned by the currently running test, if it runs under nextest.
///
/// nextest gives every running test a `NEXTEST_TEST_GLOBAL_SLOT` (since 0.9.90): a
/// small integer that is unique among the tests running at the same time and is
/// reused once its test finishes. Mapping each slot to its own port range means two
/// concurrently running tests are never handed the same port, which a "bind to port 0
/// and close" probe cannot guarantee, as the OS may hand the freed port to anyone.
fn slot_ports() -> Option<std::ops::Range<u16>> {
    static SLOT_PORTS: std::sync::LazyLock<Option<std::ops::Range<u16>>> =
        std::sync::LazyLock::new(|| {
            let slot: u16 = std::env::var("NEXTEST_TEST_GLOBAL_SLOT")
                .ok()?
                .parse()
                .ok()?;
            let ports = ports_of_slot(slot);
            if ports.is_none() {
                eprintln!(
                    "nextest slot {slot} is past the test port range, falling back to random ports"
                );
            }
            ports
        });
    SLOT_PORTS.clone()
}

fn ports_of_slot(slot: u16) -> Option<std::ops::Range<u16>> {
    let start = slot
        .checked_mul(PORTS_PER_SLOT)
        .and_then(|offset| SLOT_PORTS_START.checked_add(offset))
        .filter(|start| start + PORTS_PER_SLOT <= SLOT_PORTS_END)?;
    Some(start..start + PORTS_PER_SLOT)
}

/// Finds a port where `port` and `port + offset` for every offset are free.
///
/// Under nextest, the port comes from the current test's slot (see [`slot_ports`]), so
/// it can only collide with something outside the test run; the bind probe skips such
/// ports. Elsewhere, e.g. under `cargo test`, this falls back to a random port, which
/// is racy: between this call and the server binding it, the port may be taken by
/// another process.
fn find_available_port(offsets: &[u16]) -> u16 {
    let all_free = |port: u16| {
        port_is_free(port)
            && offsets
                .iter()
                .all(|offset| port.checked_add(*offset).is_some_and(port_is_free))
    };

    if let Some(ports) = slot_ports() {
        static NEXT_INDEX: AtomicU16 = AtomicU16::new(0);
        for _ in 0..PORTS_PER_SLOT {
            // `u16::MAX + 1` is a multiple of `PORTS_PER_SLOT`, so wrapping keeps cycling.
            let index = NEXT_INDEX.fetch_add(1, Ordering::Relaxed) % PORTS_PER_SLOT;
            let port = ports.start + index;
            if all_free(port) {
                return port;
            }
        }
        panic!("all ports of this test's slot ({ports:?}) are in use");
    }

    let max_port = u16::MAX
        - offsets
            .iter()
            .copied()
            .max()
            .unwrap_or(0)
            .max(CLUSTER_BUS_PORT_OFFSET);
    for _ in 0..10000 {
        let port = if offsets.is_empty() {
            // Let the OS pick, which is cheap and avoids ports in use.
            get_listener_on_free_port().local_addr().unwrap().port()
        } else {
            rand::random_range(1024..=max_port)
        };
        if port <= max_port && all_free(port) {
            return port;
        }
    }
    panic!("Couldn't get a valid port with offsets {offsets:?}");
}

/// Finds a port available for listening at.
///
/// Under `cargo nextest` (0.9.90 or newer) the port is unique among the concurrently
/// running tests. Otherwise it is a random free port, which another process may take
/// between this call and the server binding it.
pub fn get_random_available_port() -> u16 {
    find_available_port(&[])
}

/// Finds a port where both `port` and `port + offset` are free. Use it for servers
/// that also listen on a second port at a fixed offset from the main one.
/// See [`get_random_available_port`] for the guarantees.
pub fn get_random_available_port_with_offset(offset: u16) -> u16 {
    find_available_port(&[offset])
}

/// Finds a port for a cluster node, which also listens at `port + offset` for each of
/// `extra_offsets` and on the cluster bus port.
pub(crate) fn get_available_cluster_port(extra_offsets: &[u16]) -> u16 {
    let mut offsets = vec![CLUSTER_BUS_PORT_OFFSET];
    offsets.extend_from_slice(extra_offsets);
    find_available_port(&offsets)
}

/// Whether a server could listen on `port` right now.
fn port_is_free(port: u16) -> bool {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
    // Bind the way `redis-server` does, with `SO_REUSEADDR`, so connections lingering
    // in `TIME_WAIT` from a previous server on the port don't count as using it. Slot
    // ports are reused by every test that runs in the slot, so without this a busy slot
    // runs out of ports. On Windows `SO_REUSEADDR` would also allow binding over a live
    // listener, and `TIME_WAIT` doesn't block binding there anyway.
    #[cfg(not(windows))]
    socket.set_reuse_address(true).unwrap();
    socket.bind(&addr.into()).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_get_disjoint_port_ranges() {
        let first = ports_of_slot(0).unwrap();
        let second = ports_of_slot(1).unwrap();
        assert_eq!(first, SLOT_PORTS_START..SLOT_PORTS_START + PORTS_PER_SLOT);
        assert_eq!(second.start, first.end);
    }

    #[test]
    fn the_last_slot_fits_below_the_cluster_bus_ports() {
        let last_slot = (SLOT_PORTS_END - SLOT_PORTS_START) / PORTS_PER_SLOT - 1;
        let last = ports_of_slot(last_slot).unwrap();
        assert!(last.end <= SLOT_PORTS_END);
        assert!(last.start + CLUSTER_BUS_PORT_OFFSET >= SLOT_PORTS_END);
    }

    #[test]
    fn ports_with_a_listener_are_not_free() {
        for ip in ["127.0.0.1", "0.0.0.0"] {
            let listener = TcpListener::bind((ip, 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            assert!(!port_is_free(port), "listener on {ip}:{port} not detected");
            drop(listener);
            assert!(port_is_free(port), "{ip}:{port} still taken after closing");
        }
    }

    #[test]
    fn slots_past_the_range_have_no_ports() {
        let first_missing = (SLOT_PORTS_END - SLOT_PORTS_START) / PORTS_PER_SLOT;
        assert!(ports_of_slot(first_missing).is_none());
        assert!(ports_of_slot(u16::MAX).is_none());
    }
}
