use crate::cluster_slotmap::ReadFromReplicaStrategy;
#[cfg(feature = "cluster-async")]
use crate::cluster_topology::{
    DEFAULT_SLOTS_REFRESH_MAX_JITTER_MILLI, DEFAULT_SLOTS_REFRESH_WAIT_DURATION,
};
use crate::connection::{ConnectionAddr, ConnectionInfo, IntoConnectionInfo};
use crate::types::{ErrorKind, ProtocolVersion, RedisError, RedisResult};
use crate::{cluster, cluster::TlsMode};
use crate::{PubSubSubscriptionInfo, PushInfo};
use rand::Rng;
#[cfg(feature = "cluster-async")]
use std::ops::Add;
use std::time::Duration;

#[cfg(feature = "tls-rustls")]
use crate::tls::TlsConnParams;

#[cfg(not(feature = "tls-rustls"))]
use crate::connection::TlsConnParams;

#[cfg(feature = "cluster-async")]
use crate::cluster_async;

#[cfg(feature = "tls-rustls")]
use crate::tls::{retrieve_tls_certificates, TlsCertificates};

use tokio::sync::mpsc;

/// Parameters specific to builder, so that
/// builder parameters may have different types
/// than final ClusterParams
#[derive(Default)]
struct BuilderParams {
    password: Option<String>,
    username: Option<String>,
    read_from_replicas: ReadFromReplicaStrategy,
    tls: Option<TlsMode>,
    #[cfg(feature = "tls-rustls")]
    certs: Option<TlsCertificates>,
    retries_configuration: RetryParams,
    connection_timeout: Option<Duration>,
    #[cfg(feature = "cluster-async")]
    topology_checks_interval: Option<Duration>,
    #[cfg(feature = "cluster-async")]
    connections_validation_interval: Option<Duration>,
    #[cfg(feature = "cluster-async")]
    slots_refresh_rate_limit: SlotsRefreshRateLimit,
    client_name: Option<String>,
    response_timeout: Option<Duration>,
    protocol: ProtocolVersion,
    pubsub_subscriptions: Option<PubSubSubscriptionInfo>,
}

#[derive(Clone)]
pub(crate) struct RetryParams {
    pub(crate) number_of_retries: u32,
    max_wait_time: u64,
    min_wait_time: u64,
    exponent_base: u64,
    factor: u64,
}

impl Default for RetryParams {
    fn default() -> Self {
        const DEFAULT_RETRIES: u32 = 16;
        const DEFAULT_MAX_RETRY_WAIT_TIME: u64 = 655360;
        const DEFAULT_MIN_RETRY_WAIT_TIME: u64 = 1280;
        const DEFAULT_EXPONENT_BASE: u64 = 2;
        const DEFAULT_FACTOR: u64 = 10;
        Self {
            number_of_retries: DEFAULT_RETRIES,
            max_wait_time: DEFAULT_MAX_RETRY_WAIT_TIME,
            min_wait_time: DEFAULT_MIN_RETRY_WAIT_TIME,
            exponent_base: DEFAULT_EXPONENT_BASE,
            factor: DEFAULT_FACTOR,
        }
    }
}

impl RetryParams {
    pub(crate) fn wait_time_for_retry(&self, retry: u32) -> Duration {
        let base_wait = self.exponent_base.pow(retry) * self.factor;
        let clamped_wait = base_wait
            .min(self.max_wait_time)
            .max(self.min_wait_time + 1);
        let jittered_wait = rand::thread_rng().gen_range(self.min_wait_time..clamped_wait);
        Duration::from_millis(jittered_wait)
    }
}

/// Configuration for rate limiting slot refresh operations in a Redis cluster.
///
/// This struct defines the interval duration between consecutive slot refresh
/// operations and an additional jitter to introduce randomness in the refresh intervals.
///
/// # Fields
///
/// * `interval_duration`: The minimum duration to wait between consecutive slot refresh operations.
/// * `max_jitter_milli`: The maximum jitter in milliseconds to add to the interval duration.
#[cfg(feature = "cluster-async")]
#[derive(Clone, Copy)]
pub(crate) struct SlotsRefreshRateLimit {
    pub(crate) interval_duration: Duration,
    pub(crate) max_jitter_milli: u64,
}

#[cfg(feature = "cluster-async")]
impl Default for SlotsRefreshRateLimit {
    fn default() -> Self {
        Self {
            interval_duration: DEFAULT_SLOTS_REFRESH_WAIT_DURATION,
            max_jitter_milli: DEFAULT_SLOTS_REFRESH_MAX_JITTER_MILLI,
        }
    }
}

#[cfg(feature = "cluster-async")]
impl SlotsRefreshRateLimit {
    pub(crate) fn wait_duration(&self) -> Duration {
        let duration_jitter = match self.max_jitter_milli {
            0 => Duration::from_millis(0),
            _ => Duration::from_millis(rand::thread_rng().gen_range(0..self.max_jitter_milli)),
        };
        self.interval_duration.add(duration_jitter)
    }
}
/// Redis cluster specific parameters.
#[derive(Default, Clone)]
#[doc(hidden)]
pub struct ClusterParams {
    pub(crate) password: Option<String>,
    pub(crate) username: Option<String>,
    pub(crate) read_from_replicas: ReadFromReplicaStrategy,
    /// tls indicates tls behavior of connections.
    /// When Some(TlsMode), connections use tls and verify certification depends on TlsMode.
    /// When None, connections do not use tls.
    pub(crate) tls: Option<TlsMode>,
    pub(crate) retry_params: RetryParams,
    #[cfg(feature = "cluster-async")]
    pub(crate) topology_checks_interval: Option<Duration>,
    #[cfg(feature = "cluster-async")]
    pub(crate) slots_refresh_rate_limit: SlotsRefreshRateLimit,
    #[cfg(feature = "cluster-async")]
    pub(crate) connections_validation_interval: Option<Duration>,
    pub(crate) tls_params: Option<TlsConnParams>,
    pub(crate) client_name: Option<String>,
    pub(crate) connection_timeout: Duration,
    pub(crate) response_timeout: Duration,
    pub(crate) protocol: ProtocolVersion,
    pub(crate) pubsub_subscriptions: Option<PubSubSubscriptionInfo>,
}

impl ClusterParams {
    fn from(value: BuilderParams) -> RedisResult<Self> {
        #[cfg(not(feature = "tls-rustls"))]
        let tls_params = None;

        #[cfg(feature = "tls-rustls")]
        let tls_params = {
            let retrieved_tls_params = value.certs.clone().map(retrieve_tls_certificates);

            retrieved_tls_params.transpose()?
        };

        Ok(Self {
            password: value.password,
            username: value.username,
            read_from_replicas: value.read_from_replicas,
            tls: value.tls,
            retry_params: value.retries_configuration,
            connection_timeout: value.connection_timeout.unwrap_or(Duration::MAX),
            #[cfg(feature = "cluster-async")]
            topology_checks_interval: value.topology_checks_interval,
            #[cfg(feature = "cluster-async")]
            slots_refresh_rate_limit: value.slots_refresh_rate_limit,
            #[cfg(feature = "cluster-async")]
            connections_validation_interval: value.connections_validation_interval,
            tls_params,
            client_name: value.client_name,
            response_timeout: value.response_timeout.unwrap_or(Duration::MAX),
            protocol: value.protocol,
            pubsub_subscriptions: value.pubsub_subscriptions,
        })
    }
}

/// Used to configure and build a [`ClusterClient`].
pub struct ClusterClientBuilder {
    initial_nodes: RedisResult<Vec<ConnectionInfo>>,
    builder_params: BuilderParams,
}

impl ClusterClientBuilder {
    /// Creates a new `ClusterClientBuilder` with the provided initial_nodes.
    ///
    /// This is the same as `ClusterClient::builder(initial_nodes)`.
    pub fn new<T: IntoConnectionInfo>(
        initial_nodes: impl IntoIterator<Item = T>,
    ) -> ClusterClientBuilder {
        ClusterClientBuilder {
            initial_nodes: initial_nodes
                .into_iter()
                .map(|x| x.into_connection_info())
                .collect(),
            builder_params: Default::default(),
        }
    }

    /// Creates a new [`ClusterClient`] from the parameters.
    ///
    /// This does not create connections to the Redis Cluster, but only performs some basic checks
    /// on the initial nodes' URLs and passwords/usernames.
    ///
    /// When the `tls-rustls` feature is enabled and TLS credentials are provided, they are set for
    /// each cluster connection.
    ///
    /// # Errors
    ///
    /// Upon failure to parse initial nodes or if the initial nodes have different passwords or
    /// usernames, an error is returned.
    pub fn build(self) -> RedisResult<ClusterClient> {
        let initial_nodes = self.initial_nodes?;

        let first_node = match initial_nodes.first() {
            Some(node) => node,
            None => {
                return Err(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "Initial nodes can't be empty.",
                )))
            }
        };

        let mut cluster_params = ClusterParams::from(self.builder_params)?;
        let password = if cluster_params.password.is_none() {
            cluster_params
                .password
                .clone_from(&first_node.redis.password);
            &cluster_params.password
        } else {
            &None
        };
        let username = if cluster_params.username.is_none() {
            cluster_params
                .username
                .clone_from(&first_node.redis.username);
            &cluster_params.username
        } else {
            &None
        };
        if cluster_params.tls.is_none() {
            cluster_params.tls = match first_node.addr {
                ConnectionAddr::TcpTls {
                    host: _,
                    port: _,
                    insecure,
                    tls_params: _,
                } => Some(match insecure {
                    false => TlsMode::Secure,
                    true => TlsMode::Insecure,
                }),
                _ => None,
            };
        }

        let mut nodes = Vec::with_capacity(initial_nodes.len());
        for mut node in initial_nodes {
            if let ConnectionAddr::Unix(_) = node.addr {
                return Err(RedisError::from((ErrorKind::InvalidClientConfig,
                                             "This library cannot use unix socket because Redis's cluster command returns only cluster's IP and port.")));
            }

            if password.is_some() && node.redis.password != *password {
                return Err(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "Cannot use different password among initial nodes.",
                )));
            }

            if username.is_some() && node.redis.username != *username {
                return Err(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "Cannot use different username among initial nodes.",
                )));
            }

            if node.redis.client_name.is_some()
                && node.redis.client_name != cluster_params.client_name
            {
                return Err(RedisError::from((
                    ErrorKind::InvalidClientConfig,
                    "Cannot use different client_name among initial nodes.",
                )));
            }

            node.redis.protocol = cluster_params.protocol;
            nodes.push(node);
        }

        Ok(ClusterClient {
            initial_nodes: nodes,
            cluster_params,
        })
    }

    /// Sets client name for the new ClusterClient.
    pub fn client_name(mut self, client_name: String) -> ClusterClientBuilder {
        self.builder_params.client_name = Some(client_name);
        self
    }

    /// Sets password for the new ClusterClient.
    pub fn password(mut self, password: String) -> ClusterClientBuilder {
        self.builder_params.password = Some(password);
        self
    }

    /// Sets username for the new ClusterClient.
    pub fn username(mut self, username: String) -> ClusterClientBuilder {
        self.builder_params.username = Some(username);
        self
    }

    /// Sets number of retries for the new ClusterClient.
    pub fn retries(mut self, retries: u32) -> ClusterClientBuilder {
        self.builder_params.retries_configuration.number_of_retries = retries;
        self
    }

    /// Sets maximal wait time in millisceonds between retries for the new ClusterClient.
    pub fn max_retry_wait(mut self, max_wait: u64) -> ClusterClientBuilder {
        self.builder_params.retries_configuration.max_wait_time = max_wait;
        self
    }

    /// Sets minimal wait time in millisceonds between retries for the new ClusterClient.
    pub fn min_retry_wait(mut self, min_wait: u64) -> ClusterClientBuilder {
        self.builder_params.retries_configuration.min_wait_time = min_wait;
        self
    }

    /// Sets the factor and exponent base for the retry wait time.
    /// The formula for the wait is rand(min_wait_retry .. min(max_retry_wait , factor * exponent_base ^ retry))ms.
    pub fn retry_wait_formula(mut self, factor: u64, exponent_base: u64) -> ClusterClientBuilder {
        self.builder_params.retries_configuration.factor = factor;
        self.builder_params.retries_configuration.exponent_base = exponent_base;
        self
    }

    /// Sets TLS mode for the new ClusterClient.
    ///
    /// It is extracted from the first node of initial_nodes if not set.
    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    pub fn tls(mut self, tls: TlsMode) -> ClusterClientBuilder {
        self.builder_params.tls = Some(tls);
        self
    }

    /// Sets raw TLS certificates for the new ClusterClient.
    ///
    /// When set, enforces the connection must be TLS secured.
    ///
    /// All certificates must be provided as byte streams loaded from PEM files their consistency is
    /// checked during `build()` call.
    ///
    /// - `certificates` - `TlsCertificates` structure containing:
    ///   -- `client_tls` - Optional `ClientTlsConfig` containing byte streams for
    ///   --- `client_cert` - client's byte stream containing client certificate in PEM format
    ///   --- `client_key` - client's byte stream containing private key in PEM format
    ///   -- `root_cert` - Optional byte stream yielding PEM formatted file for root certificates.
    ///
    /// If `ClientTlsConfig` ( cert+key pair ) is not provided, then client-side authentication is not enabled.
    /// If `root_cert` is not provided, then system root certificates are used instead.
    #[cfg(feature = "tls-rustls")]
    pub fn certs(mut self, certificates: TlsCertificates) -> ClusterClientBuilder {
        self.builder_params.tls = Some(TlsMode::Secure);
        self.builder_params.certs = Some(certificates);
        self
    }

    /// Enables reading from replicas for all new connections (default is disabled).
    ///
    /// If enabled, then read queries will go to the replica nodes & write queries will go to the
    /// primary nodes. If there are no replica nodes, then all queries will go to the primary nodes.
    pub fn read_from_replicas(mut self) -> ClusterClientBuilder {
        self.builder_params.read_from_replicas = ReadFromReplicaStrategy::RoundRobin;
        self
    }

    /// Enables periodic topology checks for this client.
    ///
    /// If enabled, periodic topology checks will be executed at the configured intervals to examine whether there
    /// have been any changes in the cluster's topology. If a change is detected, it will trigger a slot refresh.
    /// Unlike slot refreshments, the periodic topology checks only examine a limited number of nodes to query their
    /// topology, ensuring that the check remains quick and efficient.
    #[cfg(feature = "cluster-async")]
    pub fn periodic_topology_checks(mut self, interval: Duration) -> ClusterClientBuilder {
        self.builder_params.topology_checks_interval = Some(interval);
        self
    }

    /// Enables periodic connections checks for this client.
    /// If enabled, the conenctions to the cluster nodes will be validated periodicatly, per configured interval.
    /// In addition, for tokio runtime, passive disconnections could be detected instantly,
    /// triggering reestablishemnt, w/o waiting for the next periodic check.
    #[cfg(feature = "cluster-async")]
    pub fn periodic_connections_checks(mut self, interval: Duration) -> ClusterClientBuilder {
        self.builder_params.connections_validation_interval = Some(interval);
        self
    }

    /// Sets the rate limit for slot refresh operations in the cluster.
    ///
    /// This method configures the interval duration between consecutive slot
    /// refresh operations and an additional jitter to introduce randomness
    /// in the refresh intervals.
    ///
    /// # Parameters
    ///
    /// * `interval_duration`: The minimum duration to wait between consecutive slot refresh operations.
    /// * `max_jitter_milli`: The maximum jitter in milliseconds to add to the interval duration.
    ///
    /// # Defaults
    ///
    /// If not set, the slots refresh rate limit configurations will be set with the default values:
    /// ```
    /// #[cfg(feature = "cluster-async")]
    /// use redis::cluster_topology::{DEFAULT_SLOTS_REFRESH_MAX_JITTER_MILLI, DEFAULT_SLOTS_REFRESH_WAIT_DURATION};
    /// ```
    ///
    /// - `interval_duration`: `DEFAULT_SLOTS_REFRESH_WAIT_DURATION`
    /// - `max_jitter_milli`: `DEFAULT_SLOTS_REFRESH_MAX_JITTER_MILLI`
    ///
    #[cfg(feature = "cluster-async")]
    pub fn slots_refresh_rate_limit(
        mut self,
        interval_duration: Duration,
        max_jitter_milli: u64,
    ) -> ClusterClientBuilder {
        self.builder_params.slots_refresh_rate_limit = SlotsRefreshRateLimit {
            interval_duration,
            max_jitter_milli,
        };
        self
    }

    /// Enables timing out on slow connection time.
    ///
    /// If enabled, the cluster will only wait the given time on each connection attempt to each node.
    pub fn connection_timeout(mut self, connection_timeout: Duration) -> ClusterClientBuilder {
        self.builder_params.connection_timeout = Some(connection_timeout);
        self
    }

    /// Enables timing out on slow responses.
    ///
    /// If enabled, the cluster will only wait the given time to each response from each node.
    pub fn response_timeout(mut self, response_timeout: Duration) -> ClusterClientBuilder {
        self.builder_params.response_timeout = Some(response_timeout);
        self
    }

    /// Sets the protocol with which the client should communicate with the server.
    pub fn use_protocol(mut self, protocol: ProtocolVersion) -> ClusterClientBuilder {
        self.builder_params.protocol = protocol;
        self
    }

    /// Use `build()`.
    #[deprecated(since = "0.22.0", note = "Use build()")]
    pub fn open(self) -> RedisResult<ClusterClient> {
        self.build()
    }

    /// Use `read_from_replicas()`.
    #[deprecated(since = "0.22.0", note = "Use read_from_replicas()")]
    pub fn readonly(mut self, read_from_replicas: bool) -> ClusterClientBuilder {
        self.builder_params.read_from_replicas = if read_from_replicas {
            ReadFromReplicaStrategy::RoundRobin
        } else {
            ReadFromReplicaStrategy::AlwaysFromPrimary
        };
        self
    }

    /// Sets the pubsub configuration for the new ClusterClient.
    pub fn pubsub_subscriptions(
        mut self,
        pubsub_subscriptions: PubSubSubscriptionInfo,
    ) -> ClusterClientBuilder {
        self.builder_params.pubsub_subscriptions = Some(pubsub_subscriptions);
        self
    }
}

/// This is a Redis Cluster client.
#[derive(Clone)]
pub struct ClusterClient {
    initial_nodes: Vec<ConnectionInfo>,
    cluster_params: ClusterParams,
}

impl ClusterClient {
    /// Creates a `ClusterClient` with the default parameters.
    ///
    /// This does not create connections to the Redis Cluster, but only performs some basic checks
    /// on the initial nodes' URLs and passwords/usernames.
    ///
    /// # Errors
    ///
    /// Upon failure to parse initial nodes or if the initial nodes have different passwords or
    /// usernames, an error is returned.
    pub fn new<T: IntoConnectionInfo>(
        initial_nodes: impl IntoIterator<Item = T>,
    ) -> RedisResult<ClusterClient> {
        Self::builder(initial_nodes).build()
    }

    /// Creates a [`ClusterClientBuilder`] with the provided initial_nodes.
    pub fn builder<T: IntoConnectionInfo>(
        initial_nodes: impl IntoIterator<Item = T>,
    ) -> ClusterClientBuilder {
        ClusterClientBuilder::new(initial_nodes)
    }

    /// Creates new connections to Redis Cluster nodes and returns a
    /// [`cluster::ClusterConnection`].
    ///
    /// # Errors
    ///
    /// An error is returned if there is a failure while creating connections or slots.
    pub fn get_connection(
        &self,
        push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
    ) -> RedisResult<cluster::ClusterConnection> {
        cluster::ClusterConnection::new(
            self.cluster_params.clone(),
            self.initial_nodes.clone(),
            push_sender,
        )
    }

    /// Creates new connections to Redis Cluster nodes and returns a
    /// [`cluster_async::ClusterConnection`].
    ///
    /// # Errors
    ///
    /// An error is returned if there is a failure while creating connections or slots.
    #[cfg(feature = "cluster-async")]
    pub async fn get_async_connection(
        &self,
        push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
    ) -> RedisResult<cluster_async::ClusterConnection> {
        cluster_async::ClusterConnection::new(
            &self.initial_nodes,
            self.cluster_params.clone(),
            push_sender,
        )
        .await
    }

    #[doc(hidden)]
    pub fn get_generic_connection<C>(
        &self,
        push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
    ) -> RedisResult<cluster::ClusterConnection<C>>
    where
        C: crate::ConnectionLike + crate::cluster::Connect + Send,
    {
        cluster::ClusterConnection::new(
            self.cluster_params.clone(),
            self.initial_nodes.clone(),
            push_sender,
        )
    }

    #[doc(hidden)]
    #[cfg(feature = "cluster-async")]
    pub async fn get_async_generic_connection<C>(
        &self,
    ) -> RedisResult<cluster_async::ClusterConnection<C>>
    where
        C: crate::aio::ConnectionLike
            + cluster_async::Connect
            + Clone
            + Send
            + Sync
            + Unpin
            + 'static,
    {
        cluster_async::ClusterConnection::new(
            &self.initial_nodes,
            self.cluster_params.clone(),
            None,
        )
        .await
    }

    /// Use `new()`.
    #[deprecated(since = "0.22.0", note = "Use new()")]
    pub fn open<T: IntoConnectionInfo>(initial_nodes: Vec<T>) -> RedisResult<ClusterClient> {
        Self::new(initial_nodes)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "cluster-async")]
    use crate::cluster_topology::{
        DEFAULT_SLOTS_REFRESH_MAX_JITTER_MILLI, DEFAULT_SLOTS_REFRESH_WAIT_DURATION,
    };

    use super::{ClusterClient, ClusterClientBuilder, ConnectionInfo, IntoConnectionInfo};

    fn get_connection_data() -> Vec<ConnectionInfo> {
        vec![
            "redis://127.0.0.1:6379".into_connection_info().unwrap(),
            "redis://127.0.0.1:6378".into_connection_info().unwrap(),
            "redis://127.0.0.1:6377".into_connection_info().unwrap(),
        ]
    }

    fn get_connection_data_with_password() -> Vec<ConnectionInfo> {
        vec![
            "redis://:password@127.0.0.1:6379"
                .into_connection_info()
                .unwrap(),
            "redis://:password@127.0.0.1:6378"
                .into_connection_info()
                .unwrap(),
            "redis://:password@127.0.0.1:6377"
                .into_connection_info()
                .unwrap(),
        ]
    }

    fn get_connection_data_with_username_and_password() -> Vec<ConnectionInfo> {
        vec![
            "redis://user1:password@127.0.0.1:6379"
                .into_connection_info()
                .unwrap(),
            "redis://user1:password@127.0.0.1:6378"
                .into_connection_info()
                .unwrap(),
            "redis://user1:password@127.0.0.1:6377"
                .into_connection_info()
                .unwrap(),
        ]
    }

    #[test]
    fn give_no_password() {
        let client = ClusterClient::new(get_connection_data()).unwrap();
        assert_eq!(client.cluster_params.password, None);
    }

    #[test]
    fn give_password_by_initial_nodes() {
        let client = ClusterClient::new(get_connection_data_with_password()).unwrap();
        assert_eq!(client.cluster_params.password, Some("password".to_string()));
    }

    #[test]
    fn give_username_and_password_by_initial_nodes() {
        let client = ClusterClient::new(get_connection_data_with_username_and_password()).unwrap();
        assert_eq!(client.cluster_params.password, Some("password".to_string()));
        assert_eq!(client.cluster_params.username, Some("user1".to_string()));
    }

    #[test]
    fn give_different_password_by_initial_nodes() {
        let result = ClusterClient::new(vec![
            "redis://:password1@127.0.0.1:6379",
            "redis://:password2@127.0.0.1:6378",
            "redis://:password3@127.0.0.1:6377",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn give_different_username_by_initial_nodes() {
        let result = ClusterClient::new(vec![
            "redis://user1:password@127.0.0.1:6379",
            "redis://user2:password@127.0.0.1:6378",
            "redis://user1:password@127.0.0.1:6377",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn give_username_password_by_method() {
        let client = ClusterClientBuilder::new(get_connection_data_with_password())
            .password("pass".to_string())
            .username("user1".to_string())
            .build()
            .unwrap();
        assert_eq!(client.cluster_params.password, Some("pass".to_string()));
        assert_eq!(client.cluster_params.username, Some("user1".to_string()));
    }

    #[test]
    fn give_empty_initial_nodes() {
        let client = ClusterClient::new(Vec::<String>::new());
        assert!(client.is_err())
    }

    #[cfg(feature = "cluster-async")]
    #[test]
    fn give_slots_refresh_rate_limit_configurations() {
        let interval_dur = std::time::Duration::from_secs(20);
        let client = ClusterClientBuilder::new(get_connection_data())
            .slots_refresh_rate_limit(interval_dur, 500)
            .build()
            .unwrap();
        assert_eq!(
            client
                .cluster_params
                .slots_refresh_rate_limit
                .interval_duration,
            interval_dur
        );
        assert_eq!(
            client
                .cluster_params
                .slots_refresh_rate_limit
                .max_jitter_milli,
            500
        );
    }

    #[cfg(feature = "cluster-async")]
    #[test]
    fn dont_give_slots_refresh_rate_limit_configurations_uses_defaults() {
        let client = ClusterClientBuilder::new(get_connection_data())
            .build()
            .unwrap();
        assert_eq!(
            client
                .cluster_params
                .slots_refresh_rate_limit
                .interval_duration,
            DEFAULT_SLOTS_REFRESH_WAIT_DURATION
        );
        assert_eq!(
            client
                .cluster_params
                .slots_refresh_rate_limit
                .max_jitter_milli,
            DEFAULT_SLOTS_REFRESH_MAX_JITTER_MILLI
        );
    }
}
