macro_rules! implement_json_commands {
    (
        $lifetime: lifetime
        $(
            $(#[$attr:meta])+
            fn $name:ident<$($tyargs:ident : $ty:ident),*>(
                $($argname:ident: $argty:ty),*) $body:block
        )*
    ) => (

        /// Implements RedisJSON commands for connection like objects.  This
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
        pub trait JsonCommands : ConnectionLike + Sized {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty, )* RV: FromRedisValue>(
                    &mut self $(, $argname: $argty)*) -> RedisResult<RV>
                    { Cmd::$name($($argname),*)?.query(self) }
            )*
        }

        impl Cmd {
            $(
                $(#[$attr])*
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                pub fn $name<$lifetime, $($tyargs: $ty),*>($($argname: $argty),*) -> RedisResult<Self> {
					$body
                }
            )*
        }

		/// Implements RedisJSON commands over asynchronous connections. This
        /// allows you to send commands straight to a connection or client.
        ///
        /// This allows you to use nicer syntax for some common operations.
        /// For instance this code:
        ///
        /// ```rust,no_run
        /// use redis::JsonAsyncCommands;
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
        /// use redis::JsonAsyncCommands;
		/// use serde_json::json;
        /// # async fn do_something() -> redis::RedisResult<()> {
        /// use redis::Commands;
        /// let client = redis::Client::open("redis://127.0.0.1/")?;
        /// let mut con = client.get_async_connection().await?;
        /// con.json_set("my_key", "$", &json!({"item": 42i32})).await?;
        /// assert_eq!(con.json_get("my_key", "$").await, Ok(String::from(r#"[{"item":42}]"#)));
        /// # Ok(()) }
        /// ```
		#[cfg(feature = "aio")]
        pub trait JsonAsyncCommands : crate::aio::ConnectionLike + Send + Sized {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                fn $name<$lifetime, $($tyargs: $ty + Send + Sync + $lifetime,)* RV>(
                    & $lifetime mut self
                    $(, $argname: $argty)*
                ) -> $crate::types::RedisFuture<'a, RV>
                where
                    RV: FromRedisValue,
                {
                    Box::pin(async move { 
                        $body?.query_async(self).await
                    })
                }
            )*
		}

		/// Implements RedisJSON commands for pipelines.  Unlike the regular
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
					if let Ok(mut command) = $body {
						self.add_command(::std::mem::replace(&mut command, Cmd::new()));
					}

					self
                }
            )*
        }

		/// Implements RedisJSON commands for cluster pipelines.  Unlike the regular
        /// commands trait, this returns the cluster pipeline rather than a result
        /// directly.  Other than that it works the same however.
        #[cfg(feature = "cluster")]
        impl ClusterPipeline {
            $(
                $(#[$attr])*
                #[inline]
                #[allow(clippy::extra_unused_lifetimes, clippy::needless_lifetimes)]
                pub fn $name<$lifetime, $($tyargs: $ty),*>(
                    &mut self $(, $argname: $argty)*
                ) -> &mut Self {
					if let Ok(mut command) = $body {
						self.add_command(::std::mem::replace(&mut command, Cmd::new()));
					}

					self
                }
            )*
        }

    )
}
