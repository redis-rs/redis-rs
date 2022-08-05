macro_rules! implement_commands {
    (
        $lifetime: lifetime
        $(
            $(#[$attr:meta])+
            fn $name:ident<$($tyargs:ident : $ty:ident),*>(
                $($argname:ident: $argty:ty),*) $body:block
        )*
    ) =>
    (
        /// Implements common redis commands for connection like objects.  This
        /// allows you to send commands straight to a connection or client.  It
        /// is also implemented for redis results of clients which makes for
        /// very convenient access in some basic cases.
        ///
        /// This allows you to use nicer syntax for some common operations.
        /// For instance this code:
        ///
        /// ```rust,no_run
        /// # fn do_something() -> redis::RedisResult<()> {
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_connection()?;
        /// redis::cmd("SET").arg("my_key").arg(42).execute(&mut con);
        /// assert_eq!(redis::cmd("GET").arg("my_key").query(&mut con), Ok(42));
        /// # Ok(()) }
        /// ```
        ///
        /// Will become this:
        ///
        /// ```rust,no_run
        /// # fn do_something() -> redis::RedisResult<()> {
        /// use redis::Commands;
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_connection()?;
        /// con.set("my_key", 42)?;
        /// assert_eq!(con.get("my_key"), Ok(42));
        /// # Ok(()) }
        /// ```
        pub trait Commands : ConnectionLike+Sized {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty, )* RV: FromRedisValue>(
                    &mut self $(, $argname: $argty)*) -> RedisResult<RV>
                    { Cmd::$name($($argname),*).query(self) }
            )*

            /// Incrementally iterate the keys space.
            #[inline]
            fn scan<RV: FromRedisValue>(&mut self) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("SCAN");
                c.cursor_arg(0);
                c.iter(self)
            }

            /// Incrementally iterate the keys space for keys matching a pattern.
            #[inline]
            fn scan_match<P: ToRedisArgs, RV: FromRedisValue>(&mut self, pattern: P) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("SCAN");
                c.cursor_arg(0).arg("MATCH").arg(pattern);
                c.iter(self)
            }

            /// Incrementally iterate hash fields and associated values.
            #[inline]
            fn hscan<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("HSCAN");
                c.arg(key).cursor_arg(0);
                c.iter(self)
            }

            /// Incrementally iterate hash fields and associated values for
            /// field names matching a pattern.
            #[inline]
            fn hscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&mut self, key: K, pattern: P) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("HSCAN");
                c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
                c.iter(self)
            }

            /// Incrementally iterate set elements.
            #[inline]
            fn sscan<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("SSCAN");
                c.arg(key).cursor_arg(0);
                c.iter(self)
            }

            /// Incrementally iterate set elements for elements matching a pattern.
            #[inline]
            fn sscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&mut self, key: K, pattern: P) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("SSCAN");
                c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
                c.iter(self)
            }

            /// Incrementally iterate sorted set elements.
            #[inline]
            fn zscan<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("ZSCAN");
                c.arg(key).cursor_arg(0);
                c.iter(self)
            }

            /// Incrementally iterate sorted set elements for elements matching a pattern.
            #[inline]
            fn zscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&mut self, key: K, pattern: P) -> RedisResult<Iter<'_, RV>> {
                let mut c = cmd("ZSCAN");
                c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
                c.iter(self)
            }
        }

        impl Cmd {
            $(
                $(#[$attr])*
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                pub fn $name<$lifetime, $($tyargs: $ty),*>($($argname: $argty),*) -> Self {
                    ::std::mem::replace($body, Cmd::new())
                }
            )*
        }

        /// Implements common redis commands over asynchronous connections. This
        /// allows you to send commands straight to a connection or client.
        ///
        /// This allows you to use nicer syntax for some common operations.
        /// For instance this code:
        ///
        /// ```rust,no_run
        /// use redis::AsyncCommands;
        /// # async fn do_something() -> redis::RedisResult<()> {
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_async_connection().await?;
        /// redis::cmd("SET").arg("my_key").arg(42i32).query_async(&mut con).await?;
        /// assert_eq!(redis::cmd("GET").arg("my_key").query_async(&mut con).await, Ok(42i32));
        /// # Ok(()) }
        /// ```
        ///
        /// Will become this:
        ///
        /// ```rust,no_run
        /// use redis::AsyncCommands;
        /// # async fn do_something() -> redis::RedisResult<()> {
        /// use redis::Commands;
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_async_connection().await?;
        /// con.set("my_key", 42i32).await?;
        /// assert_eq!(con.get("my_key").await, Ok(42i32));
        /// # Ok(()) }
        /// ```
        #[cfg(feature = "aio")]
        pub trait AsyncCommands : crate::aio::ConnectionLike + Send + Sized {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty + Send + Sync + $lifetime,)* RV>(
                    & $lifetime mut self
                    $(, $argname: $argty)*
                ) -> crate::types::RedisFuture<'a, RV>
                where
                    RV: FromRedisValue,
                {
                    Box::pin(async move { ($body).query_async(self).await })
                }
            )*

            /// Incrementally iterate the keys space.
            #[inline]
            fn scan<RV: FromRedisValue>(&mut self) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("SCAN");
                c.cursor_arg(0);
                Box::pin(async move { c.iter_async(self).await })
            }

            /// Incrementally iterate set elements for elements matching a pattern.
            #[inline]
            fn scan_match<P: ToRedisArgs, RV: FromRedisValue>(&mut self, pattern: P) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("SCAN");
                c.cursor_arg(0).arg("MATCH").arg(pattern);
                Box::pin(async move { c.iter_async(self).await })
            }

            /// Incrementally iterate hash fields and associated values.
            #[inline]
            fn hscan<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("HSCAN");
                c.arg(key).cursor_arg(0);
                Box::pin(async move {c.iter_async(self).await })
            }

            /// Incrementally iterate hash fields and associated values for
            /// field names matching a pattern.
            #[inline]
            fn hscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&mut self, key: K, pattern: P) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("HSCAN");
                c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
                Box::pin(async move {c.iter_async(self).await })
            }

            /// Incrementally iterate set elements.
            #[inline]
            fn sscan<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("SSCAN");
                c.arg(key).cursor_arg(0);
                Box::pin(async move {c.iter_async(self).await })
            }

            /// Incrementally iterate set elements for elements matching a pattern.
            #[inline]
            fn sscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&mut self, key: K, pattern: P) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("SSCAN");
                c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
                Box::pin(async move {c.iter_async(self).await })
            }

            /// Incrementally iterate sorted set elements.
            #[inline]
            fn zscan<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("ZSCAN");
                c.arg(key).cursor_arg(0);
                Box::pin(async move {c.iter_async(self).await })
            }

            /// Incrementally iterate sorted set elements for elements matching a pattern.
            #[inline]
            fn zscan_match<K: ToRedisArgs, P: ToRedisArgs, RV: FromRedisValue>
                    (&mut self, key: K, pattern: P) -> crate::types::RedisFuture<crate::cmd::AsyncIter<'_, RV>> {
                let mut c = cmd("ZSCAN");
                c.arg(key).cursor_arg(0).arg("MATCH").arg(pattern);
                Box::pin(async move {c.iter_async(self).await })
            }
        }

        /// Implements common redis commands for pipelines.  Unlike the regular
        /// commands trait, this returns the pipeline rather than a result
        /// directly.  Other than that it works the same however.
        impl Pipeline {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                pub fn $name<$lifetime, $($tyargs: $ty),*>(
                    &mut self $(, $argname: $argty)*
                ) -> &mut Self {
                    self.add_command(::std::mem::replace($body, Cmd::new()))
                }
            )*
        }

        // Implements common redis commands for cluster pipelines.  Unlike the regular
        // commands trait, this returns the cluster pipeline rather than a result
        // directly.  Other than that it works the same however.
        #[cfg(feature = "cluster")]
        impl ClusterPipeline {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                pub fn $name<$lifetime, $($tyargs: $ty),*>(
                    &mut self $(, $argname: $argty)*
                ) -> &mut Self {
                    self.add_command(::std::mem::replace($body, Cmd::new()))
                }
            )*
        }
    )
}
