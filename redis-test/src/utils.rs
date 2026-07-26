use socket2::{Domain, Socket, Type};
use std::ffi::OsStr;
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Command, Output};
use std::{fs, process};
use tempfile::TempDir;

pub trait CommandMultiArgs {
    /// Appends a new argument to the command
    fn arg<S: AsRef<OsStr>>(&mut self, arg: S) -> &mut Self;

    /// Appends two new arguments to the command
    ///
    /// This method is purely convenience to get more readable argument setting as it allows to
    /// re-write
    ///
    /// ```rust,no_run
    /// # use redis_test::server::RedisServerCommand;
    /// # use redis_test::utils::CommandMultiArgs;
    /// # let mut redis_cmd = RedisServerCommand::new();
    /// redis_cmd
    ///     .arg("--foo")
    ///     .arg("some-value-for-foo")
    ///     .arg("--bar")
    ///     .arg("some-value-for-bar")
    ///     .arg("--baz")
    ///     .arg("some-value-for-baz");
    /// ```
    ///
    /// in a more readable fashion:
    ///
    /// ```rust,no_run
    /// # use redis_test::server::RedisServerCommand;
    /// # use redis_test::utils::CommandMultiArgs;
    /// # let mut redis_cmd = RedisServerCommand::new();
    /// redis_cmd
    ///     .arg2("--foo", "some-value-for-foo")
    ///     .arg2("--bar", "some-value-for-bar")
    ///     .arg2("--baz", "some-value-for-baz");
    /// ```
    fn arg2<S1: AsRef<OsStr>, S2: AsRef<OsStr>>(&mut self, arg: S1, arg2: S2) -> &mut Self {
        self.arg(arg).arg(arg2);
        self
    }

    /// Appends three new arguments to the command
    ///
    /// This method is purely convenience to get more readable argument setting (cf. [`arg2`](Self::arg2)).
    fn arg3<S1: AsRef<OsStr>, S2: AsRef<OsStr>, S3: AsRef<OsStr>>(
        &mut self,
        arg: S1,
        arg2: S2,
        arg3: S3,
    ) -> &mut Self {
        self.arg(arg).arg(arg2).arg(arg3);
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TlsFilePaths {
    pub redis_crt: PathBuf,
    pub redis_key: PathBuf,
    pub ca_crt: PathBuf,
}

/// Client certificate and key paths for mTLS authentication
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ClientCertPaths {
    pub client_crt: PathBuf,
    pub client_key: PathBuf,
}

pub struct OpensslCommand {
    purpose: String,
    cmd: Command,
    stdin: Option<Vec<u8>>,
}

impl OpensslCommand {
    pub fn new(purpose: &str) -> Self {
        Self {
            purpose: purpose.to_string(),
            cmd: Command::new("openssl"),
            stdin: None,
        }
    }

    pub fn stdin(&mut self, stdin: Vec<u8>) -> &mut Self {
        self.stdin = Some(stdin);
        self
    }

    pub fn spawn(&mut self) -> Output {
        // Spawn the child process
        let mut child = self
            .cmd
            .stdin(process::Stdio::piped())
            .stdout(process::Stdio::piped())
            .stderr(process::Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("failed to spawn openssl ({}): {e}", self.purpose));

        // Feed in stdin
        if let Some(stdin_data) = self.stdin.take() {
            let mut child_stdin = child
                .stdin
                .take()
                .unwrap_or_else(|| panic!("failed to get openssl's stdin ({})", self.purpose));
            let purpose = self.purpose.clone();
            let _ = std::thread::spawn(move || {
                child_stdin.write_all(&stdin_data).unwrap_or_else(|e| {
                    panic!("failed to write to openssl's stdin ({purpose}): {e}")
                });
            });
        };

        // Wait until the program finishes
        let output = child
            .wait_with_output()
            .unwrap_or_else(|e| panic!("failed to wait for openssl ({}): {e}", self.purpose));

        // Check exit code
        assert!(
            output.status.success(),
            "openssl returned {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }
}

impl CommandMultiArgs for OpensslCommand {
    fn arg<S: AsRef<OsStr>>(&mut self, arg: S) -> &mut Self {
        self.cmd.arg(arg.as_ref());
        self
    }
}

/// Generate an RSA key
///
/// # Arguments:
///
/// * `file` - The key gets written to this file
/// * `size` - Generate a key with this bit size
/// * `purpose` - The name to use for this key in error messages
/// * `env_name` - If there is an environment variable of this name that has a non-empty value, load
///   the key from the file at this value instead of generating the key afresh.
// The `env_name` could get inferred from `purpose`. But we don't infer it to allow grepping for the
// full environment variable and landing at the callers.
fn generate_key(file: &PathBuf, size: usize, purpose: &str, env_name: &str) {
    if let Ok(env_value) = std::env::var(env_name)
        && !env_value.is_empty()
    {
        // The environment signals to re-use an existing key instead of generating a new one.

        // Read the key data
        let key = fs::read_to_string(&env_value)
            .unwrap_or_else(|e| panic!("failed to read {purpose} key from '{env_value}': {e}"));

        // Write the key data to the expected place
        fs::write(file, key)
            .unwrap_or_else(|e| panic!("failed to write {purpose} key to '{file:?}': {e}"));

        return;
    }

    // Call OpenSSL to generate the key
    OpensslCommand::new(&format!("generate {purpose} key"))
        .arg("genrsa")
        .arg2("-out", file)
        .arg(size.to_string())
        .spawn();
}

/// Builds CA and server certs keys etc. for TLS connections with `CN` and `alt_names`
///
/// The server certificate will have a `CN` and `alt_name` `DNS.1` of `localhost.example.com`
///
/// # Caveat
///
/// This function creates new keys for each call. On entropy/resource-limited hosts, this quickly
/// becomes time consuming. See the [TLS helpers section](crate#tls-helpers) on how to precompute
/// keys once and then re-use them to speed up calls.
pub fn build_keys_and_certs_for_tls(tempdir: &TempDir) -> TlsFilePaths {
    build_keys_and_certs_for_tls_ext(tempdir, true)
}

/// Builds CA and server certs keys etc. for TLS connections and optional `alt_names`/`CN`
///
/// If `with_ip_alts` is `true`, the server certificate will have a `CN` and `alt_name` `DNS.1` of
/// localhost.example.com`. Otherwise, only `alt_names` `IP.1` of `127.0.0.1`.
///
/// # Caveat
///
/// This function creates new keys for each call. On entropy/resource-limited hosts, this quickly
/// becomes time consuming. See the [TLS helpers section](crate#tls-helpers) on how to precompute
/// keys once and then re-use them to speed up calls.
pub fn build_keys_and_certs_for_tls_ext(tempdir: &TempDir, with_ip_alts: bool) -> TlsFilePaths {
    build_keys_and_certs_for_tls_with_hostname(tempdir, with_ip_alts, None)
}

/// Builds CA and server certs keys etc. for TLS connections and optional `alt_names` and `hostname`
///
/// The given `dns_hostname` is only respected if `with_ip_alts` is `true`.
///
/// # Caveat
///
/// This function creates new keys for each call. On entropy/resource-limited hosts, this quickly
/// becomes time consuming. See the [TLS helpers section](crate#tls-helpers) on how to precompute
/// keys once and then re-use them to speed up calls.
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

    // Generate the key for the CA
    generate_key(&ca_key, 4096, "CA", "REDISRS_TLS_KEY_CA");

    // Generate the key for the Redis server
    generate_key(&redis_key, 2048, "server", "REDISRS_TLS_KEY_SERVER");

    // Build CA Cert
    OpensslCommand::new("self-certify CA")
        .arg("req")
        .arg("-x509")
        .arg("-new")
        .arg("-nodes")
        .arg("-sha256")
        .arg2("-key", &ca_key)
        .arg2("-days", "3650")
        .arg2("-subj", "/O=Redis Test/CN=Certificate Authority")
        .arg2("-out", &ca_crt)
        .spawn();

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
    let key_cmd = OpensslCommand::new("request server key certification")
        .arg("req")
        .arg("-new")
        .arg("-sha256")
        .arg2("-subj", "/O=Redis Test/CN=Generic-cert")
        .arg2("-key", &redis_key)
        .spawn();

    // build redis cert
    let mut command2 = OpensslCommand::new("sign server key certification request");
    command2
        .arg("x509")
        .arg("-req")
        .arg("-sha256")
        .arg2("-CA", &ca_crt)
        .arg2("-CAkey", &ca_key)
        .arg2("-CAserial", &ca_serial)
        .arg("-CAcreateserial")
        .arg2("-days", "365")
        .arg2("-extfile", &ext_file);
    if !with_ip_alts {
        command2.arg2("-extensions", "v3_req");
    }
    command2
        .arg2("-out", &redis_crt)
        .stdin(key_cmd.stdout)
        .spawn();

    TlsFilePaths {
        redis_crt,
        redis_key,
        ca_crt,
    }
}

/// Build a client certificate with a custom common name (CN) field
///
/// Redis 8.6+ allows certificate-based authentication where the common name (CN)
/// is mapped to an ACL username
///
/// # Caveat
///
/// This function creates new keys for each call. On entropy/resource-limited hosts, this quickly
/// becomes time consuming. See the [TLS helpers section](crate#tls-helpers) on how to precompute
/// keys once and then re-use them to speed up calls.
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
    generate_key(&client_key, 2048, "client", "REDISRS_TLS_KEY_CLIENT");

    // Create a basic extensions file for X.509 v3 client certificate
    let client_ext_file = tempdir.path().join("client_ext.cnf");
    let client_ext_content = "\
        basicConstraints = CA:FALSE\n\
        keyUsage = digitalSignature, keyEncipherment\n\
    ";
    fs::write(&client_ext_file, client_ext_content)
        .expect("failed to create client extensions file");

    // Create certificate signing request with custom CN
    let csr_cmd = OpensslCommand::new("request client key certification")
        .arg("req")
        .arg("-new")
        .arg("-sha256")
        .arg2("-subj", format!("/O=Redis Test/CN={common_name}"))
        .arg2("-key", &client_key)
        .spawn();

    // Sign the certificate with CA (X.509 v3)
    OpensslCommand::new("sign client key certification request")
        .arg("x509")
        .arg("-req")
        .arg("-sha256")
        .arg2("-CA", ca_crt)
        .arg2("-CAkey", ca_key)
        .arg2("-CAserial", &ca_serial)
        .arg("-CAcreateserial")
        .arg2("-days", "365")
        .arg2("-extfile", &client_ext_file)
        .arg2("-out", &client_crt)
        .stdin(csr_cmd.stdout)
        .spawn();

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
