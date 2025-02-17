use std::collections::VecDeque;
use std::ops::DerefMut;
use std::str::from_utf8;
use std::time::{Duration, Instant};

use crate::cmd::{cmd, pipe, Cmd};
use crate::parser::Parser;
use crate::pipeline::Pipeline;
use crate::types::{
    from_redis_value, ErrorKind, FromRedisValue, PushKind, RedisError, RedisResult, ServerError,
    ServerErrorKind, SyncPushSender, ToRedisArgs, Value,
};
use crate::{from_owned_redis_value, ProtocolVersion};

mod addr;
pub use addr::ConnectionAddr;

mod info;
pub use info::{parse_redis_url, ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo};

/// TlsMode indicates use or do not use verification of certification.
///
/// Check [ConnectionAddr](ConnectionAddr::TcpTls::insecure) for more.
#[derive(Clone, Copy, PartialEq)]
pub enum TlsMode {
    /// Secure verify certification.
    Secure,
    /// Insecure do not verify certification.
    Insecure,
}

pub(crate) mod io;
pub(crate) use io::ActualConnection;
use io::ConnectionDriver;

use crate::commands::resp3_hello;
use crate::PushInfo;

/// Represents a stateful redis TCP connection.
pub struct Connection(GenericConnection<ActualConnection>);

/// Generic version of [`Connection`]
pub struct GenericConnection<IO> {
    con: Option<IO>,

    parser: Parser,
    db: i64,

    /// Flag indicating whether the connection was left in the PubSub state after dropping `PubSub`.
    ///
    /// This flag is checked when attempting to send a command, and if it's raised, we attempt to
    /// exit the pubsub state before executing the new request.
    pubsub: bool,

    // Field indicating which protocol to use for server communications.
    protocol: ProtocolVersion,

    /// This is used to manage Push messages in RESP3 mode.
    push_sender: Option<SyncPushSender>,

    /// The number of messages that are expected to be returned from the server,
    /// but the user no longer waits for - answers for requests that already returned a transient error.
    messages_to_skip: usize,
}

/// Represents a pubsub connection.
pub struct PubSub<'a>(GenericPubSub<'a, ActualConnection>);

/// Represents a generic pubsub connection.
pub struct GenericPubSub<'a, IO: io::ConnectionDriver> {
    con: &'a mut GenericConnection<IO>,
    waiting_messages: VecDeque<Msg>,
}

/// Represents a pubsub message.
#[derive(Debug, Clone)]
pub struct Msg {
    payload: Value,
    channel: Value,
    pattern: Option<Value>,
}

fn authenticate_cmd(
    connection_info: &RedisConnectionInfo,
    check_username: bool,
    password: &str,
) -> Cmd {
    let mut command = cmd("AUTH");
    if check_username {
        if let Some(username) = &connection_info.username {
            command.arg(username);
        }
    }
    command.arg(password);
    command
}

pub fn connect(
    connection_info: &ConnectionInfo,
    timeout: Option<Duration>,
) -> RedisResult<Connection> {
    let driver = ActualConnection::establish_conn(&connection_info.addr, timeout)?;
    Ok(Connection(connect_with_io(
        driver,
        connection_info,
        timeout,
    )?))
}

pub fn connect_with_io<IO: io::ConnectionDriver>(
    driver: IO,
    connection_info: &ConnectionInfo,
    timeout: Option<Duration>,
) -> RedisResult<GenericConnection<IO>> {
    let start = Instant::now();

    // we temporarily set the timeout, and will remove it after finishing setup.
    let remaining_timeout = timeout.and_then(|timeout| timeout.checked_sub(start.elapsed()));
    // TLS could run logic that doesn't contain a timeout, and should fail if it takes too long.
    if timeout.is_some() && remaining_timeout.is_none() {
        return Err(RedisError::from(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "Connection timed out",
        )));
    }
    driver.set_read_timeout(remaining_timeout)?;
    driver.set_write_timeout(remaining_timeout)?;

    let con = setup_connection(
        driver,
        &connection_info.redis,
        #[cfg(feature = "cache-aio")]
        None,
    )?;

    // remove the temporary timeout.
    con.set_read_timeout(None)?;
    con.set_write_timeout(None)?;

    Ok(con)
}

pub(crate) struct ConnectionSetupComponents {
    resp3_auth_cmd_idx: Option<usize>,
    resp2_auth_cmd_idx: Option<usize>,
    select_cmd_idx: Option<usize>,
    #[cfg(feature = "cache-aio")]
    cache_cmd_idx: Option<usize>,
}

pub(crate) fn connection_setup_pipeline(
    connection_info: &RedisConnectionInfo,
    check_username: bool,
    #[cfg(feature = "cache-aio")] cache_config: Option<crate::caching::CacheConfig>,
) -> (crate::Pipeline, ConnectionSetupComponents) {
    let mut last_cmd_index = 0;

    let mut get_next_command_index = |condition| {
        if condition {
            last_cmd_index += 1;
            Some(last_cmd_index - 1)
        } else {
            None
        }
    };

    let authenticate_with_resp3_cmd_index =
        get_next_command_index(connection_info.protocol != ProtocolVersion::RESP2);
    let authenticate_with_resp2_cmd_index = get_next_command_index(
        authenticate_with_resp3_cmd_index.is_none() && connection_info.password.is_some(),
    );
    let select_db_cmd_index = get_next_command_index(connection_info.db != 0);
    #[cfg(feature = "cache-aio")]
    let cache_cmd_index = get_next_command_index(
        connection_info.protocol != ProtocolVersion::RESP2 && cache_config.is_some(),
    );

    let mut pipeline = pipe();

    if authenticate_with_resp3_cmd_index.is_some() {
        pipeline.add_command(resp3_hello(connection_info));
    } else if authenticate_with_resp2_cmd_index.is_some() {
        pipeline.add_command(authenticate_cmd(
            connection_info,
            check_username,
            connection_info.password.as_ref().unwrap(),
        ));
    }

    if select_db_cmd_index.is_some() {
        pipeline.cmd("SELECT").arg(connection_info.db);
    }

    // result is ignored, as per the command's instructions.
    // https://redis.io/commands/client-setinfo/
    #[cfg(not(feature = "disable-client-setinfo"))]
    pipeline
        .cmd("CLIENT")
        .arg("SETINFO")
        .arg("LIB-NAME")
        .arg("redis-rs")
        .ignore();
    #[cfg(not(feature = "disable-client-setinfo"))]
    pipeline
        .cmd("CLIENT")
        .arg("SETINFO")
        .arg("LIB-VER")
        .arg(env!("CARGO_PKG_VERSION"))
        .ignore();

    #[cfg(feature = "cache-aio")]
    if cache_cmd_index.is_some() {
        let cache_config = cache_config.expect(
            "It's expected to have cache_config if cache_cmd_index is Some, please create an issue about this.",
        );
        pipeline.cmd("CLIENT").arg("TRACKING").arg("ON");
        match cache_config.mode {
            crate::caching::CacheMode::All => {}
            crate::caching::CacheMode::OptIn => {
                pipeline.arg("OPTIN");
            }
        }
    }

    (
        pipeline,
        ConnectionSetupComponents {
            resp3_auth_cmd_idx: authenticate_with_resp3_cmd_index,
            resp2_auth_cmd_idx: authenticate_with_resp2_cmd_index,
            select_cmd_idx: select_db_cmd_index,
            #[cfg(feature = "cache-aio")]
            cache_cmd_idx: cache_cmd_index,
        },
    )
}

fn check_resp3_auth(result: &Value) -> RedisResult<()> {
    if let Value::ServerError(err) = result {
        return Err(get_resp3_hello_command_error(err.clone().into()));
    }
    Ok(())
}

#[derive(PartialEq)]
pub(crate) enum AuthResult {
    Succeeded,
    ShouldRetryWithoutUsername,
}

fn check_resp2_auth(result: &Value) -> RedisResult<AuthResult> {
    let err = match result {
        Value::Okay => {
            return Ok(AuthResult::Succeeded);
        }
        Value::ServerError(err) => err,
        _ => {
            return Err((
                ErrorKind::ResponseError,
                "Redis server refused to authenticate, returns Ok() != Value::Okay",
            )
                .into());
        }
    };

    let err_msg = err.details().ok_or((
        ErrorKind::AuthenticationFailed,
        "Password authentication failed",
    ))?;
    if !err_msg.contains("wrong number of arguments for 'auth' command") {
        return Err((
            ErrorKind::AuthenticationFailed,
            "Password authentication failed",
        )
            .into());
    }
    Ok(AuthResult::ShouldRetryWithoutUsername)
}

fn check_db_select(value: &Value) -> RedisResult<()> {
    let Value::ServerError(err) = value else {
        return Ok(());
    };

    match err.details() {
        Some(err_msg) => Err((
            ErrorKind::ResponseError,
            "Redis server refused to switch database",
            err_msg.to_string(),
        )
            .into()),
        None => Err((
            ErrorKind::ResponseError,
            "Redis server refused to switch database",
        )
            .into()),
    }
}

#[cfg(feature = "cache-aio")]
fn check_caching(result: &Value) -> RedisResult<()> {
    match result {
        Value::Okay => Ok(()),
        _ => Err((
            ErrorKind::ResponseError,
            "Client-side caching returned unknown response",
        )
            .into()),
    }
}

pub(crate) fn check_connection_setup(
    results: Vec<Value>,
    ConnectionSetupComponents {
        resp3_auth_cmd_idx,
        resp2_auth_cmd_idx,
        select_cmd_idx,
        #[cfg(feature = "cache-aio")]
        cache_cmd_idx,
    }: ConnectionSetupComponents,
) -> RedisResult<AuthResult> {
    // can't have both values set
    assert!(!(resp2_auth_cmd_idx.is_some() && resp3_auth_cmd_idx.is_some()));

    if let Some(index) = resp3_auth_cmd_idx {
        let Some(value) = results.get(index) else {
            return Err((ErrorKind::ClientError, "Missing RESP3 auth response").into());
        };
        check_resp3_auth(value)?;
    } else if let Some(index) = resp2_auth_cmd_idx {
        let Some(value) = results.get(index) else {
            return Err((ErrorKind::ClientError, "Missing RESP2 auth response").into());
        };
        if check_resp2_auth(value)? == AuthResult::ShouldRetryWithoutUsername {
            return Ok(AuthResult::ShouldRetryWithoutUsername);
        }
    }

    if let Some(index) = select_cmd_idx {
        let Some(value) = results.get(index) else {
            return Err((ErrorKind::ClientError, "Missing SELECT DB response").into());
        };
        check_db_select(value)?;
    }

    #[cfg(feature = "cache-aio")]
    if let Some(index) = cache_cmd_idx {
        let Some(value) = results.get(index) else {
            return Err((ErrorKind::ClientError, "Missing Caching response").into());
        };
        check_caching(value)?;
    }

    Ok(AuthResult::Succeeded)
}

fn execute_connection_pipeline<IO: io::ConnectionDriver>(
    rv: &mut GenericConnection<IO>,
    (pipeline, instructions): (crate::Pipeline, ConnectionSetupComponents),
) -> RedisResult<AuthResult> {
    if pipeline.len() == 0 {
        return Ok(AuthResult::Succeeded);
    }
    let results = rv.req_packed_commands(&pipeline.get_packed_pipeline(), 0, pipeline.len())?;

    check_connection_setup(results, instructions)
}

fn setup_connection<IO: io::ConnectionDriver>(
    con: IO,
    connection_info: &RedisConnectionInfo,
    #[cfg(feature = "cache-aio")] cache_config: Option<crate::caching::CacheConfig>,
) -> RedisResult<GenericConnection<IO>> {
    let mut rv = GenericConnection {
        con: Some(con),
        parser: Parser::new(),
        db: connection_info.db,
        pubsub: false,
        protocol: connection_info.protocol,
        push_sender: None,
        messages_to_skip: 0,
    };

    if execute_connection_pipeline(
        &mut rv,
        connection_setup_pipeline(
            connection_info,
            true,
            #[cfg(feature = "cache-aio")]
            cache_config,
        ),
    )? == AuthResult::ShouldRetryWithoutUsername
    {
        execute_connection_pipeline(
            &mut rv,
            connection_setup_pipeline(
                connection_info,
                false,
                #[cfg(feature = "cache-aio")]
                cache_config,
            ),
        )?;
    }

    Ok(rv)
}

/// Implements the "stateless" part of the connection interface that is used by the
/// different objects in redis-rs.
///
/// Primarily it obviously applies to `Connection` object but also some other objects
///  implement the interface (for instance whole clients or certain redis results).
///
/// Generally clients and connections (as well as redis results of those) implement
/// this trait.  Actual connections provide more functionality which can be used
/// to implement things like `PubSub` but they also can modify the intrinsic
/// state of the TCP connection.  This is not possible with `ConnectionLike`
/// implementors because that functionality is not exposed.
pub trait ConnectionLike {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// reads the single response from it.
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value>;

    /// Sends multiple already encoded (packed) command into the TCP socket
    /// and reads `count` responses from it.  This is used to implement
    /// pipelining.
    /// Important - this function is meant for internal usage, since it's
    /// easy to pass incorrect `offset` & `count` parameters, which might
    /// cause the connection to enter an erroneous state. Users shouldn't
    /// call it, instead using the Pipeline::query function.
    #[doc(hidden)]
    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>>;

    /// Sends a [Cmd] into the TCP socket and reads a single response from it.
    fn req_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        let pcmd = cmd.get_packed_command();
        self.req_packed_command(&pcmd)
    }

    /// Returns the database this connection is bound to.  Note that this
    /// information might be unreliable because it's initially cached and
    /// also might be incorrect if the connection like object is not
    /// actually connected.
    fn get_db(&self) -> i64;

    /// Does this connection support pipelining?
    #[doc(hidden)]
    fn supports_pipelining(&self) -> bool {
        true
    }

    /// Check that all connections it has are available (`PING` internally).
    fn check_connection(&mut self) -> bool;

    /// Returns the connection status.
    ///
    /// The connection is open until any `read` call received an
    /// invalid response from the server (most likely a closed or dropped
    /// connection, otherwise a Redis protocol error). When using unix
    /// sockets the connection is open until writing a command failed with a
    /// `BrokenPipe` error.
    fn is_open(&self) -> bool;
}

/// A connection is an object that represents a single redis connection.  It
/// provides basic support for sending encoded commands into a redis connection
/// and to read a response from it.  It's bound to a single database and can
/// only be created from the client.
///
/// You generally do not much with this object other than passing it to
/// `Cmd` objects.
impl Connection {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// does not read a response.  This is useful for commands like
    /// `MONITOR` which yield multiple items.  This needs to be used with
    /// care because it changes the state of the connection.
    #[inline]
    pub fn send_packed_command(&mut self, cmd: &[u8]) -> RedisResult<()> {
        self.0.send_packed_command(cmd)
    }

    /// Fetches a single response from the connection.  This is useful
    /// if used in combination with `send_packed_command`.
    #[inline]
    pub fn recv_response(&mut self) -> RedisResult<Value> {
        self.0.recv_response()
    }

    /// Sets the write timeout for the connection.
    ///
    /// If the provided value is `None`, then `send_packed_command` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    #[inline]
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        self.0.set_write_timeout(dur)
    }

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `recv_response` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    #[inline]
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        self.0.set_read_timeout(dur)
    }

    /// Creates a [`PubSub`] instance for this connection.
    pub fn as_pubsub(&mut self) -> PubSub<'_> {
        // NOTE: The pubsub flag is intentionally not raised at this time since
        // running commands within the pubsub state should not try and exit from
        // the pubsub state.
        PubSub(GenericPubSub::new(&mut self.0))
    }

    /// Sets sender channel for push values.
    #[inline]
    pub fn set_push_sender(&mut self, sender: SyncPushSender) {
        self.0.set_push_sender(sender);
    }
}

impl ConnectionLike for Connection {
    /// Sends a [Cmd] into the TCP socket and reads a single response from it.
    #[inline]
    fn req_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        self.0.req_command(cmd)
    }

    #[inline]
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        self.0.req_packed_command(cmd)
    }

    #[inline]
    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        self.0.req_packed_commands(cmd, offset, count)
    }

    #[inline]
    fn get_db(&self) -> i64 {
        self.0.get_db()
    }

    #[inline]
    fn check_connection(&mut self) -> bool {
        self.0.check_connection()
    }

    #[inline]
    fn is_open(&self) -> bool {
        self.0.is_open()
    }
}

impl<IO: io::ConnectionDriver> GenericConnection<IO> {
    /// Sends an already encoded (packed) command into the TCP socket and
    /// does not read a response.  This is useful for commands like
    /// `MONITOR` which yield multiple items.  This needs to be used with
    /// care because it changes the state of the connection.
    pub fn send_packed_command(&mut self, cmd: &[u8]) -> RedisResult<()> {
        self.send_bytes(cmd)?;
        Ok(())
    }

    /// Fetches a single response from the connection.  This is useful
    /// if used in combination with `send_packed_command`.
    pub fn recv_response(&mut self) -> RedisResult<Value> {
        self.read(true)
    }

    /// Sets the write timeout for the connection.
    ///
    /// If the provided value is `None`, then `send_packed_command` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        match self.con.as_ref() {
            Some(con) => con.set_write_timeout(dur),
            None => Err(RedisError::from(std::io::Error::from(
                std::io::ErrorKind::BrokenPipe,
            ))),
        }
    }

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `recv_response` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        match self.con.as_ref() {
            Some(con) => con.set_read_timeout(dur),
            None => Err(RedisError::from(std::io::Error::from(
                std::io::ErrorKind::BrokenPipe,
            ))),
        }
    }

    /// Creates a [`PubSub`] instance for this connection.
    pub fn as_pubsub(&mut self) -> GenericPubSub<'_, IO> {
        // NOTE: The pubsub flag is intentionally not raised at this time since
        // running commands within the pubsub state should not try and exit from
        // the pubsub state.
        GenericPubSub::new(self)
    }

    fn exit_pubsub(&mut self) -> RedisResult<()> {
        let res = self.clear_active_subscriptions();
        if res.is_ok() {
            self.pubsub = false;
        } else {
            // Raise the pubsub flag to indicate the connection is "stuck" in that state.
            self.pubsub = true;
        }

        res
    }

    /// Get the inner connection out of a PubSub
    ///
    /// Any active subscriptions are unsubscribed. In the event of an error, the connection is
    /// dropped.
    fn clear_active_subscriptions(&mut self) -> RedisResult<()> {
        // Responses to unsubscribe commands return in a 3-tuple with values
        // ("unsubscribe" or "punsubscribe", name of subscription removed, count of remaining subs).
        // The "count of remaining subs" includes both pattern subscriptions and non pattern
        // subscriptions. Thus, to accurately drain all unsubscribe messages received from the
        // server, both commands need to be executed at once.
        {
            // Prepare both unsubscribe commands
            let unsubscribe = cmd("UNSUBSCRIBE").get_packed_command();
            let punsubscribe = cmd("PUNSUBSCRIBE").get_packed_command();

            // Execute commands
            self.send_bytes(&unsubscribe)?;
            self.send_bytes(&punsubscribe)?;
        }

        // Receive responses
        //
        // There will be at minimum two responses - 1 for each of punsubscribe and unsubscribe
        // commands. There may be more responses if there are active subscriptions. In this case,
        // messages are received until the _subscription count_ in the responses reach zero.
        let mut received_unsub = false;
        let mut received_punsub = false;

        loop {
            let resp = self.recv_response()?;

            match resp {
                Value::Push { kind, data } => {
                    if data.len() >= 2 {
                        if let Value::Int(num) = data[1] {
                            if resp3_is_pub_sub_state_cleared(
                                &mut received_unsub,
                                &mut received_punsub,
                                &kind,
                                num as isize,
                            ) {
                                break;
                            }
                        }
                    }
                }
                Value::ServerError(err) => {
                    // a new error behavior, introduced in valkey 8.
                    // https://github.com/valkey-io/valkey/pull/759
                    if err.kind() == Some(ServerErrorKind::NoSub) {
                        if no_sub_err_is_pub_sub_state_cleared(
                            &mut received_unsub,
                            &mut received_punsub,
                            &err,
                        ) {
                            break;
                        } else {
                            continue;
                        }
                    }

                    return Err(err.into());
                }
                Value::Array(vec) => {
                    let res: (Vec<u8>, (), isize) = from_owned_redis_value(Value::Array(vec))?;
                    if resp2_is_pub_sub_state_cleared(
                        &mut received_unsub,
                        &mut received_punsub,
                        &res.0,
                        res.2,
                    ) {
                        break;
                    }
                }
                _ => {
                    return Err((
                        ErrorKind::ClientError,
                        "Unexpected unsubscribe response",
                        format!("{resp:?}"),
                    )
                        .into())
                }
            }
        }

        // Finally, the connection is back in its normal state since all subscriptions were
        // cancelled *and* all unsubscribe messages were received.
        Ok(())
    }

    fn send_push(&self, push: PushInfo) {
        if let Some(sender) = &self.push_sender {
            let _ = sender.send(push);
        }
    }

    fn try_send(&self, value: &RedisResult<Value>) {
        if let Ok(Value::Push { kind, data }) = value {
            self.send_push(PushInfo {
                kind: kind.clone(),
                data: data.clone(),
            });
        }
    }

    fn send_disconnect(&self) {
        self.send_push(PushInfo::disconnect())
    }

    fn close_connection(&mut self) {
        // Notify the PushManager that the connection was lost
        self.send_disconnect();
        self.con = None;
    }

    /// Fetches a single message from the connection. If the message is a response,
    /// increment `messages_to_skip` if it wasn't received before a timeout.
    fn read(&mut self, is_response: bool) -> RedisResult<Value> {
        loop {
            let result = match self.con.as_mut() {
                Some(con) => con.read_value(&mut self.parser),
                None => Err(RedisError::from(std::io::Error::from(
                    std::io::ErrorKind::BrokenPipe,
                ))),
            };
            self.try_send(&result);

            let Err(err) = &result else {
                if self.messages_to_skip > 0 {
                    self.messages_to_skip -= 1;
                    continue;
                }
                return result;
            };
            let Some(io_error) = err.as_io_error() else {
                if self.messages_to_skip > 0 {
                    self.messages_to_skip -= 1;
                    continue;
                }
                return result;
            };
            // shutdown connection on protocol error
            if io_error.kind() == std::io::ErrorKind::UnexpectedEof {
                self.close_connection();
            } else if is_response {
                self.messages_to_skip += 1;
            }

            return result;
        }
    }

    /// Sets sender channel for push values.
    pub fn set_push_sender(&mut self, sender: SyncPushSender) {
        self.push_sender = Some(sender);
    }

    fn send_bytes(&mut self, bytes: &[u8]) -> RedisResult<Value> {
        let result = match self.con.as_mut() {
            Some(con) => con.send_bytes(bytes),
            None => Err(RedisError::from(std::io::Error::from(
                std::io::ErrorKind::BrokenPipe,
            ))),
        };
        if self.protocol != ProtocolVersion::RESP2 {
            if let Err(e) = &result {
                if e.is_connection_dropped() {
                    self.send_disconnect();
                }
            }
        }
        result
    }
}

impl<IO: io::ConnectionDriver> ConnectionLike for GenericConnection<IO> {
    /// Sends a [Cmd] into the TCP socket and reads a single response from it.
    fn req_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        let pcmd = cmd.get_packed_command();
        if self.pubsub {
            self.exit_pubsub()?;
        }

        self.send_bytes(&pcmd)?;
        if cmd.is_no_response() {
            return Ok(Value::Nil);
        }
        loop {
            match self.read(true)? {
                Value::Push {
                    kind: _kind,
                    data: _data,
                } => continue,
                val => return Ok(val),
            }
        }
    }
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        if self.pubsub {
            self.exit_pubsub()?;
        }

        self.send_bytes(cmd)?;
        loop {
            match self.read(true)? {
                Value::Push {
                    kind: _kind,
                    data: _data,
                } => continue,
                val => return Ok(val),
            }
        }
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        if self.pubsub {
            self.exit_pubsub()?;
        }
        self.send_bytes(cmd)?;
        let mut rv = vec![];
        let mut first_err = None;
        let mut count = count;
        let mut idx = 0;
        while idx < (offset + count) {
            // When processing a transaction, some responses may be errors.
            // We need to keep processing the rest of the responses in that case,
            // so bailing early with `?` would not be correct.
            // See: https://github.com/redis-rs/redis-rs/issues/436
            let response = self.read(true);
            match response {
                Ok(Value::ServerError(err)) => {
                    if idx < offset {
                        if first_err.is_none() {
                            first_err = Some(err.into());
                        }
                    } else {
                        rv.push(Value::ServerError(err));
                    }
                }
                Ok(item) => {
                    // RESP3 can insert push data between command replies
                    if let Value::Push {
                        kind: _kind,
                        data: _data,
                    } = item
                    {
                        // if that is the case we have to extend the loop and handle push data
                        count += 1;
                    } else if idx >= offset {
                        rv.push(item);
                    }
                }
                Err(err) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
            idx += 1;
        }

        first_err.map_or(Ok(rv), Err)
    }

    fn get_db(&self) -> i64 {
        self.db
    }

    fn check_connection(&mut self) -> bool {
        cmd("PING").query::<String>(self).is_ok()
    }

    fn is_open(&self) -> bool {
        self.con.as_ref().map(|c| c.is_open()).unwrap_or_default()
    }
}

impl<C, T> ConnectionLike for T
where
    C: ConnectionLike,
    T: DerefMut<Target = C>,
{
    fn req_packed_command(&mut self, cmd: &[u8]) -> RedisResult<Value> {
        self.deref_mut().req_packed_command(cmd)
    }

    fn req_packed_commands(
        &mut self,
        cmd: &[u8],
        offset: usize,
        count: usize,
    ) -> RedisResult<Vec<Value>> {
        self.deref_mut().req_packed_commands(cmd, offset, count)
    }

    fn req_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
        self.deref_mut().req_command(cmd)
    }

    fn get_db(&self) -> i64 {
        self.deref().get_db()
    }

    fn supports_pipelining(&self) -> bool {
        self.deref().supports_pipelining()
    }

    fn check_connection(&mut self) -> bool {
        self.deref_mut().check_connection()
    }

    fn is_open(&self) -> bool {
        self.deref().is_open()
    }
}

/// The pubsub object provides convenient access to the redis pubsub
/// system.  Once created you can subscribe and unsubscribe from channels
/// and listen in on messages.
///
/// Example:
///
/// ```rust,no_run
/// # fn do_something() -> redis::RedisResult<()> {
/// let client = redis::Client::open("redis://127.0.0.1/")?;
/// let mut con = client.get_connection()?;
/// let mut pubsub = con.as_pubsub();
/// pubsub.subscribe("channel_1")?;
/// pubsub.subscribe("channel_2")?;
///
/// loop {
///     let msg = pubsub.get_message()?;
///     let payload : String = msg.get_payload()?;
///     println!("channel '{}': {}", msg.get_channel_name(), payload);
/// }
/// # }
/// ```
impl<'a> PubSub<'a> {
    /// Subscribes to a new channel.
    #[inline]
    pub fn subscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        self.0.subscribe(channel)
    }

    /// Subscribes to a new channel with a pattern.
    #[inline]
    pub fn psubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        self.0.psubscribe(pchannel)
    }

    /// Unsubscribes from a channel.
    #[inline]
    pub fn unsubscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        self.0.unsubscribe(channel)
    }

    /// Unsubscribes from a channel with a pattern.
    #[inline]
    pub fn punsubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        self.0.punsubscribe(pchannel)
    }

    /// Sends a ping with a message to the server
    #[inline]
    pub fn ping_message<T: FromRedisValue>(&mut self, message: impl ToRedisArgs) -> RedisResult<T> {
        self.0.ping_message(message)
    }

    /// Sends a ping to the server
    #[inline]
    pub fn ping<T: FromRedisValue>(&mut self) -> RedisResult<T> {
        self.0.ping()
    }

    /// Fetches the next message from the pubsub connection.  Blocks until
    /// a message becomes available.  This currently does not provide a
    /// wait not to block :(
    ///
    /// The message itself is still generic and can be converted into an
    /// appropriate type through the helper methods on it.
    #[inline]
    pub fn get_message(&mut self) -> RedisResult<Msg> {
        self.0.get_message()
    }

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `get_message` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    #[inline]
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        self.0.set_read_timeout(dur)
    }
}

impl<'a, IO: io::ConnectionDriver> GenericPubSub<'a, IO> {
    fn new(con: &'a mut GenericConnection<IO>) -> Self {
        Self {
            con,
            waiting_messages: VecDeque::new(),
        }
    }

    fn cache_messages_until_received_response(
        &mut self,
        cmd: &mut Cmd,
        is_sub_unsub: bool,
    ) -> RedisResult<Value> {
        let ignore_response = self.con.protocol != ProtocolVersion::RESP2 && is_sub_unsub;
        cmd.set_no_response(ignore_response);

        self.con.send_packed_command(&cmd.get_packed_command())?;

        loop {
            let response = self.con.recv_response()?;
            if let Some(msg) = Msg::from_value(&response) {
                self.waiting_messages.push_back(msg);
            } else {
                return Ok(response);
            }
        }
    }

    /// Subscribes to a new channel(s).
    pub fn subscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        self.cache_messages_until_received_response(cmd("SUBSCRIBE").arg(channel), true)?;
        Ok(())
    }

    /// Subscribes to new channel(s) with pattern(s).
    pub fn psubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        self.cache_messages_until_received_response(cmd("PSUBSCRIBE").arg(pchannel), true)?;
        Ok(())
    }

    /// Unsubscribes from a channel(s).
    pub fn unsubscribe<T: ToRedisArgs>(&mut self, channel: T) -> RedisResult<()> {
        self.cache_messages_until_received_response(cmd("UNSUBSCRIBE").arg(channel), true)?;
        Ok(())
    }

    /// Unsubscribes from channel pattern(s).
    pub fn punsubscribe<T: ToRedisArgs>(&mut self, pchannel: T) -> RedisResult<()> {
        self.cache_messages_until_received_response(cmd("PUNSUBSCRIBE").arg(pchannel), true)?;
        Ok(())
    }

    /// Sends a ping with a message to the server
    pub fn ping_message<T: FromRedisValue>(&mut self, message: impl ToRedisArgs) -> RedisResult<T> {
        from_owned_redis_value(
            self.cache_messages_until_received_response(cmd("PING").arg(message), false)?,
        )
    }
    /// Sends a ping to the server
    pub fn ping<T: FromRedisValue>(&mut self) -> RedisResult<T> {
        from_owned_redis_value(
            self.cache_messages_until_received_response(&mut cmd("PING"), false)?,
        )
    }

    /// Fetches the next message from the pubsub connection.  Blocks until
    /// a message becomes available.  This currently does not provide a
    /// wait not to block :(
    ///
    /// The message itself is still generic and can be converted into an
    /// appropriate type through the helper methods on it.
    pub fn get_message(&mut self) -> RedisResult<Msg> {
        if let Some(msg) = self.waiting_messages.pop_front() {
            return Ok(msg);
        }
        loop {
            if let Some(msg) = Msg::from_owned_value(self.con.read(false)?) {
                return Ok(msg);
            } else {
                continue;
            }
        }
    }

    /// Sets the read timeout for the connection.
    ///
    /// If the provided value is `None`, then `get_message` call will
    /// block indefinitely. It is an error to pass the zero `Duration` to this
    /// method.
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> RedisResult<()> {
        self.con.set_read_timeout(dur)
    }
}

impl<IO: io::ConnectionDriver> Drop for GenericPubSub<'_, IO> {
    fn drop(&mut self) {
        let _ = self.con.exit_pubsub();
    }
}

/// This holds the data that comes from listening to a pubsub
/// connection.  It only contains actual message data.
impl Msg {
    /// Tries to convert provided [`Value`] into [`Msg`].
    pub fn from_value(value: &Value) -> Option<Self> {
        Self::from_owned_value(value.clone())
    }

    /// Tries to convert provided [`Value`] into [`Msg`].
    pub fn from_owned_value(value: Value) -> Option<Self> {
        let mut pattern = None;
        let payload;
        let channel;

        if let Value::Push { kind, data } = value {
            return Self::from_push_info(PushInfo { kind, data });
        } else {
            let raw_msg: Vec<Value> = from_owned_redis_value(value).ok()?;
            let mut iter = raw_msg.into_iter();
            let msg_type: String = from_owned_redis_value(iter.next()?).ok()?;
            if msg_type == "message" {
                channel = iter.next()?;
                payload = iter.next()?;
            } else if msg_type == "pmessage" {
                pattern = Some(iter.next()?);
                channel = iter.next()?;
                payload = iter.next()?;
            } else {
                return None;
            }
        };
        Some(Msg {
            payload,
            channel,
            pattern,
        })
    }

    /// Tries to convert provided [`PushInfo`] into [`Msg`].
    pub fn from_push_info(push_info: PushInfo) -> Option<Self> {
        let mut pattern = None;
        let payload;
        let channel;

        let mut iter = push_info.data.into_iter();
        if push_info.kind == PushKind::Message || push_info.kind == PushKind::SMessage {
            channel = iter.next()?;
            payload = iter.next()?;
        } else if push_info.kind == PushKind::PMessage {
            pattern = Some(iter.next()?);
            channel = iter.next()?;
            payload = iter.next()?;
        } else {
            return None;
        }

        Some(Msg {
            payload,
            channel,
            pattern,
        })
    }

    /// Returns the channel this message came on.
    pub fn get_channel<T: FromRedisValue>(&self) -> RedisResult<T> {
        from_redis_value(&self.channel)
    }

    /// Convenience method to get a string version of the channel.  Unless
    /// your channel contains non utf-8 bytes you can always use this
    /// method.  If the channel is not a valid string (which really should
    /// not happen) then the return value is `"?"`.
    pub fn get_channel_name(&self) -> &str {
        match self.channel {
            Value::BulkString(ref bytes) => from_utf8(bytes).unwrap_or("?"),
            _ => "?",
        }
    }

    /// Returns the message's payload in a specific format.
    pub fn get_payload<T: FromRedisValue>(&self) -> RedisResult<T> {
        from_redis_value(&self.payload)
    }

    /// Returns the bytes that are the message's payload.  This can be used
    /// as an alternative to the `get_payload` function if you are interested
    /// in the raw bytes in it.
    pub fn get_payload_bytes(&self) -> &[u8] {
        match self.payload {
            Value::BulkString(ref bytes) => bytes,
            _ => b"",
        }
    }

    /// Returns true if the message was constructed from a pattern
    /// subscription.
    #[allow(clippy::wrong_self_convention)]
    pub fn from_pattern(&self) -> bool {
        self.pattern.is_some()
    }

    /// If the message was constructed from a message pattern this can be
    /// used to find out which one.  It's recommended to match against
    /// an `Option<String>` so that you do not need to use `from_pattern`
    /// to figure out if a pattern was set.
    pub fn get_pattern<T: FromRedisValue>(&self) -> RedisResult<T> {
        match self.pattern {
            None => from_redis_value(&Value::Nil),
            Some(ref x) => from_redis_value(x),
        }
    }
}

/// This function simplifies transaction management slightly.  What it
/// does is automatically watching keys and then going into a transaction
/// loop util it succeeds.  Once it goes through the results are
/// returned.
///
/// To use the transaction two pieces of information are needed: a list
/// of all the keys that need to be watched for modifications and a
/// closure with the code that should be execute in the context of the
/// transaction.  The closure is invoked with a fresh pipeline in atomic
/// mode.  To use the transaction the function needs to return the result
/// from querying the pipeline with the connection.
///
/// The end result of the transaction is then available as the return
/// value from the function call.
///
/// Example:
///
/// ```rust,no_run
/// use redis::Commands;
/// # fn do_something() -> redis::RedisResult<()> {
/// # let client = redis::Client::open("redis://127.0.0.1/").unwrap();
/// # let mut con = client.get_connection().unwrap();
/// let key = "the_key";
/// let (new_val,) : (isize,) = redis::transaction(&mut con, &[key], |con, pipe| {
///     let old_val : isize = con.get(key)?;
///     pipe
///         .set(key, old_val + 1).ignore()
///         .get(key).query(con)
/// })?;
/// println!("The incremented number is: {}", new_val);
/// # Ok(()) }
/// ```
pub fn transaction<
    C: ConnectionLike,
    K: ToRedisArgs,
    T,
    F: FnMut(&mut C, &mut Pipeline) -> RedisResult<Option<T>>,
>(
    con: &mut C,
    keys: &[K],
    func: F,
) -> RedisResult<T> {
    let mut func = func;
    loop {
        cmd("WATCH").arg(keys).exec(con)?;
        let mut p = pipe();
        let response: Option<T> = func(con, p.atomic())?;
        match response {
            None => {
                continue;
            }
            Some(response) => {
                // make sure no watch is left in the connection, even if
                // someone forgot to use the pipeline.
                cmd("UNWATCH").exec(con)?;
                return Ok(response);
            }
        }
    }
}
//TODO: for both clearing logic support sharded channels.

/// Common logic for clearing subscriptions in RESP2 async/sync
pub fn resp2_is_pub_sub_state_cleared(
    received_unsub: &mut bool,
    received_punsub: &mut bool,
    kind: &[u8],
    num: isize,
) -> bool {
    match kind.first() {
        Some(&b'u') => *received_unsub = true,
        Some(&b'p') => *received_punsub = true,
        _ => (),
    };
    *received_unsub && *received_punsub && num == 0
}

/// Common logic for clearing subscriptions in RESP3 async/sync
pub fn resp3_is_pub_sub_state_cleared(
    received_unsub: &mut bool,
    received_punsub: &mut bool,
    kind: &PushKind,
    num: isize,
) -> bool {
    match kind {
        PushKind::Unsubscribe => *received_unsub = true,
        PushKind::PUnsubscribe => *received_punsub = true,
        _ => (),
    };
    *received_unsub && *received_punsub && num == 0
}

pub fn no_sub_err_is_pub_sub_state_cleared(
    received_unsub: &mut bool,
    received_punsub: &mut bool,
    err: &ServerError,
) -> bool {
    let details = err.details();
    *received_unsub = *received_unsub
        || details
            .map(|details| details.starts_with("'unsub"))
            .unwrap_or_default();
    *received_punsub = *received_punsub
        || details
            .map(|details| details.starts_with("'punsub"))
            .unwrap_or_default();
    *received_unsub && *received_punsub
}

/// Common logic for checking real cause of hello3 command error
pub fn get_resp3_hello_command_error(err: RedisError) -> RedisError {
    if let Some(detail) = err.detail() {
        if detail.starts_with("unknown command `HELLO`") {
            return (
                ErrorKind::RESP3NotSupported,
                "Redis Server doesn't support HELLO command therefore resp3 cannot be used",
            )
                .into();
        }
    }
    err
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_redis_url() {
        let cases = vec![
            ("redis://127.0.0.1", true),
            ("redis://[::1]", true),
            ("redis+unix:///run/redis.sock", true),
            ("unix:///run/redis.sock", true),
            ("http://127.0.0.1", false),
            ("tcp://127.0.0.1", false),
        ];
        for (url, expected) in cases.into_iter() {
            let res = parse_redis_url(url);
            assert_eq!(
                res.is_some(),
                expected,
                "Parsed result of `{url}` is not expected",
            );
        }
    }

    #[test]
    fn test_url_to_tcp_connection_info() {
        let cases = vec![
            (
                url::Url::parse("redis://127.0.0.1").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379),
                    redis: Default::default(),
                },
            ),
            (
                url::Url::parse("redis://[::1]").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Tcp("::1".to_string(), 6379),
                    redis: Default::default(),
                },
            ),
            (
                url::Url::parse("redis://%25johndoe%25:%23%40%3C%3E%24@example.com/2").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Tcp("example.com".to_string(), 6379),
                    redis: RedisConnectionInfo {
                        db: 2,
                        username: Some("%johndoe%".to_string()),
                        password: Some("#@<>$".to_string()),
                        ..Default::default()
                    },
                },
            ),
            (
                url::Url::parse("redis://127.0.0.1/?protocol=2").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379),
                    redis: Default::default(),
                },
            ),
            (
                url::Url::parse("redis://127.0.0.1/?protocol=resp3").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 6379),
                    redis: RedisConnectionInfo {
                        protocol: ProtocolVersion::RESP3,
                        ..Default::default()
                    },
                },
            ),
        ];
        for (url, expected) in cases.into_iter() {
            let res = info::url_to_tcp_connection_info(url.clone()).unwrap();
            assert_eq!(res.addr, expected.addr, "addr of {url} is not expected");
            assert_eq!(
                res.redis.db, expected.redis.db,
                "db of {url} is not expected",
            );
            assert_eq!(
                res.redis.username, expected.redis.username,
                "username of {url} is not expected",
            );
            assert_eq!(
                res.redis.password, expected.redis.password,
                "password of {url} is not expected",
            );
        }
    }

    #[test]
    fn test_url_to_tcp_connection_info_failed() {
        let cases = vec![
            (
                url::Url::parse("redis://").unwrap(),
                "Missing hostname",
                None,
            ),
            (
                url::Url::parse("redis://127.0.0.1/db").unwrap(),
                "Invalid database number",
                None,
            ),
            (
                url::Url::parse("redis://C3%B0@127.0.0.1").unwrap(),
                "Username is not valid UTF-8 string",
                None,
            ),
            (
                url::Url::parse("redis://:C3%B0@127.0.0.1").unwrap(),
                "Password is not valid UTF-8 string",
                None,
            ),
            (
                url::Url::parse("redis://127.0.0.1/?protocol=4").unwrap(),
                "Invalid protocol version",
                Some("4"),
            ),
        ];
        for (url, expected, detail) in cases.into_iter() {
            let res = info::url_to_tcp_connection_info(url).unwrap_err();
            assert_eq!(
                res.kind(),
                crate::ErrorKind::InvalidClientConfig,
                "{}",
                &res,
            );
            #[allow(deprecated)]
            let desc = std::error::Error::description(&res);
            assert_eq!(desc, expected, "{}", &res);
            assert_eq!(res.detail(), detail, "{}", &res);
        }
    }

    #[test]
    #[cfg(unix)]
    fn test_url_to_unix_connection_info() {
        let cases = vec![
            (
                url::Url::parse("unix:///var/run/redis.sock").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Unix("/var/run/redis.sock".into()),
                    redis: RedisConnectionInfo {
                        db: 0,
                        username: None,
                        password: None,
                        protocol: ProtocolVersion::RESP2,
                    },
                },
            ),
            (
                url::Url::parse("redis+unix:///var/run/redis.sock?db=1").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Unix("/var/run/redis.sock".into()),
                    redis: RedisConnectionInfo {
                        db: 1,
                        ..Default::default()
                    },
                },
            ),
            (
                url::Url::parse(
                    "unix:///example.sock?user=%25johndoe%25&pass=%23%40%3C%3E%24&db=2",
                )
                .unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Unix("/example.sock".into()),
                    redis: RedisConnectionInfo {
                        db: 2,
                        username: Some("%johndoe%".to_string()),
                        password: Some("#@<>$".to_string()),
                        ..Default::default()
                    },
                },
            ),
            (
                url::Url::parse(
                    "redis+unix:///example.sock?pass=%26%3F%3D+%2A%2B&db=2&user=%25johndoe%25",
                )
                .unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Unix("/example.sock".into()),
                    redis: RedisConnectionInfo {
                        db: 2,
                        username: Some("%johndoe%".to_string()),
                        password: Some("&?= *+".to_string()),
                        ..Default::default()
                    },
                },
            ),
            (
                url::Url::parse("redis+unix:///var/run/redis.sock?protocol=3").unwrap(),
                ConnectionInfo {
                    addr: ConnectionAddr::Unix("/var/run/redis.sock".into()),
                    redis: RedisConnectionInfo {
                        protocol: ProtocolVersion::RESP3,
                        ..Default::default()
                    },
                },
            ),
        ];
        for (url, expected) in cases.into_iter() {
            assert_eq!(
                ConnectionAddr::Unix(url.to_file_path().unwrap()),
                expected.addr,
                "addr of {url} is not expected",
            );
            let res = info::url_to_unix_connection_info(url.clone()).unwrap();
            assert_eq!(res.addr, expected.addr, "addr of {url} is not expected");
            assert_eq!(
                res.redis.db, expected.redis.db,
                "db of {url} is not expected",
            );
            assert_eq!(
                res.redis.username, expected.redis.username,
                "username of {url} is not expected",
            );
            assert_eq!(
                res.redis.password, expected.redis.password,
                "password of {url} is not expected",
            );
        }
    }
}
