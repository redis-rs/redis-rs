use crate::cluster::ClusterConnection;

use super::{
    ConnectionAddr, ConnectionInfo, ErrorKind, IntoConnectionInfo, RedisError, RedisResult,
};

/// Used to configure and build a [ClusterClient](ClusterClient).
pub struct ClusterClientBuilder {
    initial_nodes: RedisResult<Vec<ConnectionInfo>>,
    readonly: bool,
    password: Option<String>,
}

impl ClusterClientBuilder {
    /// Generate the base configuration for new Client.
    pub fn new<T: IntoConnectionInfo>(initial_nodes: Vec<T>) -> ClusterClientBuilder {
        ClusterClientBuilder {
            initial_nodes: initial_nodes
                .into_iter()
                .map(|x| x.into_connection_info())
                .collect(),
            readonly: false,
            password: None,
        }
    }

    /// Builds a [ClusterClient](ClusterClient). Despite the name, this does not actually open
    /// a connection to Redis Cluster, but will perform some basic checks of the initial
    /// nodes' URLs and passwords.
    ///
    /// # Errors
    ///
    /// Upon failure to parse initial nodes or if the initial nodes have different passwords,
    /// an error is returned.
    pub fn open(self) -> RedisResult<ClusterClient> {
        ClusterClient::build(self)
    }

    /// Set password for new ClusterClient.
    pub fn password(mut self, password: String) -> ClusterClientBuilder {
        self.password = Some(password);
        self
    }

    /// Set read only mode for new ClusterClient (default is false).
    /// If readonly is true, all queries will go to replica nodes. If there are no replica nodes,
    /// queries will be issued to the primary nodes.
    pub fn readonly(mut self, readonly: bool) -> ClusterClientBuilder {
        self.readonly = readonly;
        self
    }
}

/// This is a Redis cluster client.
pub struct ClusterClient {
    initial_nodes: Vec<ConnectionInfo>,
    readonly: bool,
    password: Option<String>,
}

impl ClusterClient {
    /// Create a [ClusterClient](ClusterClient) with the default configuration. Despite the name,
    /// this does not actually open a connection to Redis Cluster, but only performs some basic
    /// checks of the initial nodes' URLs and passwords.
    ///
    /// # Errors
    ///
    /// Upon failure to parse initial nodes or if the initial nodes have different passwords,
    /// an error is returned.
    pub fn open<T: IntoConnectionInfo>(initial_nodes: Vec<T>) -> RedisResult<ClusterClient> {
        ClusterClientBuilder::new(initial_nodes).open()
    }

    /// Opens connections to Redis Cluster nodes and returns a
    /// [ClusterConnection](ClusterConnection).
    ///
    /// # Errors
    ///
    /// An error is returned if there is a failure to open connections or to create slots.
    pub fn get_connection(&self) -> RedisResult<ClusterConnection> {
        ClusterConnection::new(
            self.initial_nodes.clone(),
            self.readonly,
            self.password.clone(),
        )
    }

    fn build(builder: ClusterClientBuilder) -> RedisResult<ClusterClient> {
        let initial_nodes = builder.initial_nodes?;
        let mut nodes = Vec::with_capacity(initial_nodes.len());
        let mut connection_info_password = None::<String>;

        for (index, info) in initial_nodes.into_iter().enumerate() {
            if let ConnectionAddr::Unix(_) = info.addr {
                return Err(RedisError::from((ErrorKind::InvalidClientConfig,
                                             "This library cannot use unix socket because Redis's cluster command returns only cluster's IP and port.")));
            }

            if builder.password.is_none() {
                if index == 0 {
                    connection_info_password = info.redis.password.clone();
                } else if connection_info_password != info.redis.password {
                    return Err(RedisError::from((
                        ErrorKind::InvalidClientConfig,
                        "Cannot use different password among initial nodes.",
                    )));
                }
            }

            nodes.push(info);
        }

        Ok(ClusterClient {
            initial_nodes: nodes,
            readonly: builder.readonly,
            password: builder.password.or(connection_info_password),
        })
    }
}

impl Clone for ClusterClient {
    fn clone(&self) -> ClusterClient {
        ClusterClient::open(self.initial_nodes.clone()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterClient, ClusterClientBuilder};
    use super::{ConnectionInfo, IntoConnectionInfo};

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

    #[test]
    fn give_no_password() {
        let client = ClusterClient::open(get_connection_data()).unwrap();
        assert_eq!(client.password, None);
    }

    #[test]
    fn give_password_by_initial_nodes() {
        let client = ClusterClient::open(get_connection_data_with_password()).unwrap();
        assert_eq!(client.password, Some("password".to_string()));
    }

    #[test]
    fn give_different_password_by_initial_nodes() {
        let result = ClusterClient::open(vec![
            "redis://:password1@127.0.0.1:6379",
            "redis://:password2@127.0.0.1:6378",
            "redis://:password3@127.0.0.1:6377",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn give_password_by_method() {
        let client = ClusterClientBuilder::new(get_connection_data_with_password())
            .password("pass".to_string())
            .open()
            .unwrap();
        assert_eq!(client.password, Some("pass".to_string()));
    }
}
