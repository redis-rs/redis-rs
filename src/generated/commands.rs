implement_commands! {
    'a

    /// COPY
    /// 
    /// Copy a key
    /// 
    /// Since: Redis 6.2.0
    /// Group: Generic
    /// Complexity: O(N) worst case for collections, where N is the number of nested items. O(1) for string values.
    fn copy<K0: ToRedisArgs, K1: ToRedisArgs>(source: K0, destination: K1) {
        cmd("COPY").arg(source).arg(destination)
    }

    /// DEL
    /// 
    /// Delete a key
    /// 
    /// Since: Redis 1.0.0
    /// Group: Generic
    /// Complexity: O(N) where N is the number of keys that will be removed. When a key to remove holds a value other than a string, the individual complexity for this key is O(M) where M is the number of elements in the list, set, sorted set or hash. Removing a single key that holds a string value is O(1).
    fn del<K0: ToRedisArgs>(key: K0) {
        cmd("DEL").arg(key)
    }

    /// DUMP
    /// 
    /// Return a serialized version of the value stored at the specified key.
    /// 
    /// Since: Redis 2.6.0
    /// Group: Generic
    /// Complexity: O(1) to access the key and additional O(N*M) to serialize it, where N is the number of Redis objects composing the value and M their average size. For small string values the time complexity is thus O(1)+O(1*M) where M is small, so simply O(1).
    fn dump<K0: ToRedisArgs>(key: K0) {
        cmd("DUMP").arg(key)
    }

    /// EXISTS
    /// 
    /// Determine if a key exists
    /// 
    /// Since: Redis 1.0.0
    /// Group: Generic
    /// Complexity: O(N) where N is the number of keys to check.
    fn exists<K0: ToRedisArgs>(key: K0) {
        cmd("EXISTS").arg(key)
    }

    /// EXPIRE
    /// 
    /// Set a key's time to live in seconds
    /// 
    /// Since: Redis 1.0.0
    /// Group: Generic
    /// Complexity: O(1)
    fn expire<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, seconds: T0) {
        cmd("EXPIRE").arg(key).arg(seconds)
    }

    /// EXPIREAT
    /// 
    /// Set the expiration for a key as a UNIX timestamp
    /// 
    /// Since: Redis 1.2.0
    /// Group: Generic
    /// Complexity: O(1)
    fn expireat<K0: ToRedisArgs>(key: K0) {
        cmd("EXPIREAT").arg(key)
    }

    /// EXPIRETIME
    /// 
    /// Get the expiration Unix timestamp for a key
    /// 
    /// Since: Redis 7.0.0
    /// Group: Generic
    /// Complexity: O(1)
    fn expiretime<K0: ToRedisArgs>(key: K0) {
        cmd("EXPIRETIME").arg(key)
    }

    /// KEYS
    /// 
    /// Find all keys matching the given pattern
    /// 
    /// Since: Redis 1.0.0
    /// Group: Generic
    /// Complexity: O(N) with N being the number of keys in the database, under the assumption that the key names in the database and the given pattern have limited length.
    fn keys<K0: ToRedisArgs>(pattern: K0) {
        cmd("KEYS").arg(pattern)
    }

    /// MIGRATE
    /// 
    /// Atomically transfer a key from a Redis instance to another one.
    /// 
    /// Since: Redis 2.6.0
    /// Group: Generic
    /// Complexity: This command actually executes a DUMP+DEL in the source instance, and a RESTORE in the target instance. See the pages of these commands for time complexity. Also an O(N) data transfer between the two instances is performed.
    fn migrate<T0: ToRedisArgs, T1: ToRedisArgs, T2: ToRedisArgs, T3: ToRedisArgs>(host: T0, port: T1, destination_db: T2, timeout: T3) {
        cmd("MIGRATE").arg(host).arg(port).arg(destination_db).arg(timeout)
    }

    /// MOVE
    /// 
    /// Move a key to another database
    /// 
    /// Since: Redis 1.0.0
    /// Group: Generic
    /// Complexity: O(1)
    fn r#move<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, db: T0) {
        cmd("MOVE").arg(key).arg(db)
    }

    /// OBJECT
    /// 
    /// A container for object introspection commands
    /// 
    /// Since: Redis 2.2.3
    /// Group: Generic
    /// Complexity: Depends on subcommand.
    fn object<>() {
        cmd("OBJECT").as_mut()
    }

    /// OBJECT ENCODING
    /// 
    /// Inspect the internal encoding of a Redis object
    /// 
    /// Since: Redis 2.2.3
    /// Group: Generic
    /// Complexity: O(1)
    fn object_encoding<K0: ToRedisArgs>(key: K0) {
        cmd("OBJECT ENCODING").arg(key)
    }

    /// OBJECT FREQ
    /// 
    /// Get the logarithmic access frequency counter of a Redis object
    /// 
    /// Since: Redis 4.0.0
    /// Group: Generic
    /// Complexity: O(1)
    fn object_freq<K0: ToRedisArgs>(key: K0) {
        cmd("OBJECT FREQ").arg(key)
    }

    /// OBJECT HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 6.2.0
    /// Group: Generic
    /// Complexity: O(1)
    fn object_help<>() {
        cmd("OBJECT HELP").as_mut()
    }

    /// OBJECT IDLETIME
    /// 
    /// Get the time since a Redis object was last accessed
    /// 
    /// Since: Redis 2.2.3
    /// Group: Generic
    /// Complexity: O(1)
    fn object_idletime<K0: ToRedisArgs>(key: K0) {
        cmd("OBJECT IDLETIME").arg(key)
    }

    /// OBJECT REFCOUNT
    /// 
    /// Get the number of references to the value of the key
    /// 
    /// Since: Redis 2.2.3
    /// Group: Generic
    /// Complexity: O(1)
    fn object_refcount<K0: ToRedisArgs>(key: K0) {
        cmd("OBJECT REFCOUNT").arg(key)
    }

    /// PERSIST
    /// 
    /// Remove the expiration from a key
    /// 
    /// Since: Redis 2.2.0
    /// Group: Generic
    /// Complexity: O(1)
    fn persist<K0: ToRedisArgs>(key: K0) {
        cmd("PERSIST").arg(key)
    }

    /// PEXPIRE
    /// 
    /// Set a key's time to live in milliseconds
    /// 
    /// Since: Redis 2.6.0
    /// Group: Generic
    /// Complexity: O(1)
    fn pexpire<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, milliseconds: T0) {
        cmd("PEXPIRE").arg(key).arg(milliseconds)
    }

    /// PEXPIREAT
    /// 
    /// Set the expiration for a key as a UNIX timestamp specified in milliseconds
    /// 
    /// Since: Redis 2.6.0
    /// Group: Generic
    /// Complexity: O(1)
    fn pexpireat<K0: ToRedisArgs>(key: K0) {
        cmd("PEXPIREAT").arg(key)
    }

    /// PEXPIRETIME
    /// 
    /// Get the expiration Unix timestamp for a key in milliseconds
    /// 
    /// Since: Redis 7.0.0
    /// Group: Generic
    /// Complexity: O(1)
    fn pexpiretime<K0: ToRedisArgs>(key: K0) {
        cmd("PEXPIRETIME").arg(key)
    }

    /// PTTL
    /// 
    /// Get the time to live for a key in milliseconds
    /// 
    /// Since: Redis 2.6.0
    /// Group: Generic
    /// Complexity: O(1)
    fn pttl<K0: ToRedisArgs>(key: K0) {
        cmd("PTTL").arg(key)
    }

    /// RANDOMKEY
    /// 
    /// Return a random key from the keyspace
    /// 
    /// Since: Redis 1.0.0
    /// Group: Generic
    /// Complexity: O(1)
    fn randomkey<>() {
        cmd("RANDOMKEY").as_mut()
    }

    /// RENAME
    /// 
    /// Rename a key
    /// 
    /// Since: Redis 1.0.0
    /// Group: Generic
    /// Complexity: O(1)
    fn rename<K0: ToRedisArgs, K1: ToRedisArgs>(key: K0, newkey: K1) {
        cmd("RENAME").arg(key).arg(newkey)
    }

    /// RENAMENX
    /// 
    /// Rename a key, only if the new key does not exist
    /// 
    /// Since: Redis 1.0.0
    /// Group: Generic
    /// Complexity: O(1)
    fn renamenx<K0: ToRedisArgs, K1: ToRedisArgs>(key: K0, newkey: K1) {
        cmd("RENAMENX").arg(key).arg(newkey)
    }

    /// RESTORE
    /// 
    /// Create a key using the provided serialized value, previously obtained using DUMP.
    /// 
    /// Since: Redis 2.6.0
    /// Group: Generic
    /// Complexity: O(1) to create the new key and additional O(N*M) to reconstruct the serialized value, where N is the number of Redis objects composing the value and M their average size. For small string values the time complexity is thus O(1)+O(1*M) where M is small, so simply O(1). However for sorted set values the complexity is O(N*M*log(N)) because inserting values into sorted sets is O(log(N)).
    fn restore<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, ttl: T0, serialized_value: T1) {
        cmd("RESTORE").arg(key).arg(ttl).arg(serialized_value)
    }

    /// SORT
    /// 
    /// Sort the elements in a list, set or sorted set
    /// 
    /// Since: Redis 1.0.0
    /// Group: Generic
    /// Complexity: O(N+M*log(M)) where N is the number of elements in the list or set to sort, and M the number of returned elements. When the elements are not sorted, complexity is O(N).
    fn sort<K0: ToRedisArgs>(key: K0) {
        cmd("SORT").arg(key)
    }

    /// SORT_RO
    /// 
    /// Sort the elements in a list, set or sorted set. Read-only variant of SORT.
    /// 
    /// Since: Redis 7.0.0
    /// Group: Generic
    /// Complexity: O(N+M*log(M)) where N is the number of elements in the list or set to sort, and M the number of returned elements. When the elements are not sorted, complexity is O(N).
    fn sort_ro<K0: ToRedisArgs>(key: K0) {
        cmd("SORT_RO").arg(key)
    }

    /// TOUCH
    /// 
    /// Alters the last access time of a key(s). Returns the number of existing keys specified.
    /// 
    /// Since: Redis 3.2.1
    /// Group: Generic
    /// Complexity: O(N) where N is the number of keys that will be touched.
    fn touch<K0: ToRedisArgs>(key: K0) {
        cmd("TOUCH").arg(key)
    }

    /// TTL
    /// 
    /// Get the time to live for a key in seconds
    /// 
    /// Since: Redis 1.0.0
    /// Group: Generic
    /// Complexity: O(1)
    fn ttl<K0: ToRedisArgs>(key: K0) {
        cmd("TTL").arg(key)
    }

    /// TYPE
    /// 
    /// Determine the type stored at key
    /// 
    /// Since: Redis 1.0.0
    /// Group: Generic
    /// Complexity: O(1)
    fn r#type<K0: ToRedisArgs>(key: K0) {
        cmd("TYPE").arg(key)
    }

    /// UNLINK
    /// 
    /// Delete a key asynchronously in another thread. Otherwise it is just as DEL, but non blocking.
    /// 
    /// Since: Redis 4.0.0
    /// Group: Generic
    /// Complexity: O(1) for each key removed regardless of its size. Then the command does O(N) work in a different thread in order to reclaim memory, where N is the number of allocations the deleted objects where composed of.
    fn unlink<K0: ToRedisArgs>(key: K0) {
        cmd("UNLINK").arg(key)
    }

    /// WAIT
    /// 
    /// Wait for the synchronous replication of all the write commands sent in the context of the current connection
    /// 
    /// Since: Redis 3.0.0
    /// Group: Generic
    /// Complexity: O(1)
    fn wait<T0: ToRedisArgs, T1: ToRedisArgs>(numreplicas: T0, timeout: T1) {
        cmd("WAIT").arg(numreplicas).arg(timeout)
    }

    /// APPEND
    /// 
    /// Append a value to a key
    /// 
    /// Since: Redis 2.0.0
    /// Group: String
    /// Complexity: O(1). The amortized time complexity is O(1) assuming the appended value is small and the already present value is of any size, since the dynamic string library used by Redis will double the free space available on every reallocation.
    fn append<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, value: T0) {
        cmd("APPEND").arg(key).arg(value)
    }

    /// DECR
    /// 
    /// Decrement the integer value of a key by one
    /// 
    /// Since: Redis 1.0.0
    /// Group: String
    /// Complexity: O(1)
    fn decr<K0: ToRedisArgs>(key: K0) {
        cmd("DECR").arg(key)
    }

    /// DECRBY
    /// 
    /// Decrement the integer value of a key by the given number
    /// 
    /// Since: Redis 1.0.0
    /// Group: String
    /// Complexity: O(1)
    fn decrby<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, decrement: T0) {
        cmd("DECRBY").arg(key).arg(decrement)
    }

    /// GET
    /// 
    /// Get the value of a key
    /// 
    /// Since: Redis 1.0.0
    /// Group: String
    /// Complexity: O(1)
    fn get<K0: ToRedisArgs>(key: K0) {
        cmd("GET").arg(key)
    }

    /// GETDEL
    /// 
    /// Get the value of a key and delete the key
    /// 
    /// Since: Redis 6.2.0
    /// Group: String
    /// Complexity: O(1)
    fn getdel<K0: ToRedisArgs>(key: K0) {
        cmd("GETDEL").arg(key)
    }

    /// GET_DEL
    /// 
    /// Get the value of a key and delete the key
    /// 
    /// Since: Redis 6.2.0
    /// Group: String
    /// Complexity: O(1)
    fn get_del<K0: ToRedisArgs>(key: K0) {
        cmd("GET_DEL").arg(key)
    }

    /// GETEX
    /// 
    /// Get the value of a key and optionally set its expiration
    /// 
    /// Since: Redis 6.2.0
    /// Group: String
    /// Complexity: O(1)
    fn getex<K0: ToRedisArgs>(key: K0) {
        cmd("GETEX").arg(key)
    }

    /// GETRANGE
    /// 
    /// Get a substring of the string stored at a key
    /// 
    /// Since: Redis 2.4.0
    /// Group: String
    /// Complexity: O(N) where N is the length of the returned string. The complexity is ultimately determined by the returned length, but because creating a substring from an existing string is very cheap, it can be considered O(1) for small strings.
    fn getrange<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, start: T0, end: T1) {
        cmd("GETRANGE").arg(key).arg(start).arg(end)
    }

    /// GETSET
    /// 
    /// Set the string value of a key and return its old value
    /// 
    /// Since: Redis 1.0.0
    /// Group: String
    /// Replaced By: `SET` with the `!GET` argument
    /// Complexity: O(1)
    /// Replaced By: `SET` with the `!GET` argument
    #[deprecated]    fn getset<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, value: T0) {
        cmd("GETSET").arg(key).arg(value)
    }

    /// INCR
    /// 
    /// Increment the integer value of a key by one
    /// 
    /// Since: Redis 1.0.0
    /// Group: String
    /// Complexity: O(1)
    fn incr<K0: ToRedisArgs>(key: K0) {
        cmd("INCR").arg(key)
    }

    /// INCRBY
    /// 
    /// Increment the integer value of a key by the given amount
    /// 
    /// Since: Redis 1.0.0
    /// Group: String
    /// Complexity: O(1)
    fn incrby<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, increment: T0) {
        cmd("INCRBY").arg(key).arg(increment)
    }

    /// INCRBYFLOAT
    /// 
    /// Increment the float value of a key by the given amount
    /// 
    /// Since: Redis 2.6.0
    /// Group: String
    /// Complexity: O(1)
    fn incrbyfloat<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, increment: T0) {
        cmd("INCRBYFLOAT").arg(key).arg(increment)
    }

    /// LCS
    /// 
    /// Find longest common substring
    /// 
    /// Since: Redis 7.0.0
    /// Group: String
    /// Complexity: O(N*M) where N and M are the lengths of s1 and s2, respectively
    fn lcs<K0: ToRedisArgs, K1: ToRedisArgs>(key1: K0, key2: K1) {
        cmd("LCS").arg(key1).arg(key2)
    }

    /// MGET
    /// 
    /// Get the values of all the given keys
    /// 
    /// Since: Redis 1.0.0
    /// Group: String
    /// Complexity: O(N) where N is the number of keys to retrieve.
    fn mget<K0: ToRedisArgs>(key: K0) {
        cmd("MGET").arg(key)
    }

    /// MSET
    /// 
    /// Set multiple keys to multiple values
    /// 
    /// Since: Redis 1.0.1
    /// Group: String
    /// Complexity: O(N) where N is the number of keys to set.
    fn mset<T0: ToRedisArgs>(key_value: T0) {
        cmd("MSET").arg(key_value)
    }

    /// MSETNX
    /// 
    /// Set multiple keys to multiple values, only if none of the keys exist
    /// 
    /// Since: Redis 1.0.1
    /// Group: String
    /// Complexity: O(N) where N is the number of keys to set.
    fn msetnx<T0: ToRedisArgs>(key_value: T0) {
        cmd("MSETNX").arg(key_value)
    }

    /// PSETEX
    /// 
    /// Set the value and expiration in milliseconds of a key
    /// 
    /// Since: Redis 2.6.0
    /// Group: String
    /// Complexity: O(1)
    fn psetex<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, milliseconds: T0, value: T1) {
        cmd("PSETEX").arg(key).arg(milliseconds).arg(value)
    }

    /// SET
    /// 
    /// Set the string value of a key
    /// 
    /// Since: Redis 1.0.0
    /// Group: String
    /// Complexity: O(1)
    fn set<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, value: T0) {
        cmd("SET").arg(key).arg(value)
    }

    /// SETEX
    /// 
    /// Set the value and expiration of a key
    /// 
    /// Since: Redis 2.0.0
    /// Group: String
    /// Complexity: O(1)
    fn setex<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, seconds: T0, value: T1) {
        cmd("SETEX").arg(key).arg(seconds).arg(value)
    }

    /// SETNX
    /// 
    /// Set the value of a key, only if the key does not exist
    /// 
    /// Since: Redis 1.0.0
    /// Group: String
    /// Complexity: O(1)
    fn setnx<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, value: T0) {
        cmd("SETNX").arg(key).arg(value)
    }

    /// SETRANGE
    /// 
    /// Overwrite part of a string at key starting at the specified offset
    /// 
    /// Since: Redis 2.2.0
    /// Group: String
    /// Complexity: O(1), not counting the time taken to copy the new string in place. Usually, this string is very small so the amortized complexity is O(1). Otherwise, complexity is O(M) with M being the length of the value argument.
    fn setrange<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, offset: T0, value: T1) {
        cmd("SETRANGE").arg(key).arg(offset).arg(value)
    }

    /// STRLEN
    /// 
    /// Get the length of the value stored in a key
    /// 
    /// Since: Redis 2.2.0
    /// Group: String
    /// Complexity: O(1)
    fn strlen<K0: ToRedisArgs>(key: K0) {
        cmd("STRLEN").arg(key)
    }

    /// SUBSTR
    /// 
    /// Get a substring of the string stored at a key
    /// 
    /// Since: Redis 1.0.0
    /// Group: String
    /// Replaced By: `GETRANGE`
    /// Complexity: O(N) where N is the length of the returned string. The complexity is ultimately determined by the returned length, but because creating a substring from an existing string is very cheap, it can be considered O(1) for small strings.
    /// Replaced By: `GETRANGE`
    #[deprecated]    fn substr<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, start: T0, end: T1) {
        cmd("SUBSTR").arg(key).arg(start).arg(end)
    }

    /// BLMOVE
    /// 
    /// Pop an element from a list, push it to another list and return it; or block until one is available
    /// 
    /// Since: Redis 6.2.0
    /// Group: List
    /// Complexity: O(1)
    fn blmove<K0: ToRedisArgs, K1: ToRedisArgs, T0: ToRedisArgs>(source: K0, destination: K1, timeout: T0) {
        cmd("BLMOVE").arg(source).arg(destination).arg(timeout)
    }

    /// BLMPOP
    /// 
    /// Pop elements from a list, or block until one is available
    /// 
    /// Since: Redis 7.0.0
    /// Group: List
    /// Complexity: O(N+M) where N is the number of provided keys and M is the number of elements returned.
    fn blmpop<T0: ToRedisArgs, T1: ToRedisArgs, K0: ToRedisArgs>(timeout: T0, numkeys: T1, key: K0) {
        cmd("BLMPOP").arg(timeout).arg(numkeys).arg(key)
    }

    /// BLPOP
    /// 
    /// Remove and get the first element in a list, or block until one is available
    /// 
    /// Since: Redis 2.0.0
    /// Group: List
    /// Complexity: O(N) where N is the number of provided keys.
    fn blpop<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, timeout: T0) {
        cmd("BLPOP").arg(key).arg(timeout)
    }

    /// BRPOP
    /// 
    /// Remove and get the last element in a list, or block until one is available
    /// 
    /// Since: Redis 2.0.0
    /// Group: List
    /// Complexity: O(N) where N is the number of provided keys.
    fn brpop<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, timeout: T0) {
        cmd("BRPOP").arg(key).arg(timeout)
    }

    /// BRPOPLPUSH
    /// 
    /// Pop an element from a list, push it to another list and return it; or block until one is available
    /// 
    /// Since: Redis 2.2.0
    /// Group: List
    /// Replaced By: `BLMOVE` with the `RIGHT` and `LEFT` arguments
    /// Complexity: O(1)
    /// Replaced By: `BLMOVE` with the `RIGHT` and `LEFT` arguments
    #[deprecated]    fn brpoplpush<K0: ToRedisArgs, K1: ToRedisArgs, T0: ToRedisArgs>(source: K0, destination: K1, timeout: T0) {
        cmd("BRPOPLPUSH").arg(source).arg(destination).arg(timeout)
    }

    /// LINDEX
    /// 
    /// Get an element from a list by its index
    /// 
    /// Since: Redis 1.0.0
    /// Group: List
    /// Complexity: O(N) where N is the number of elements to traverse to get to the element at index. This makes asking for the first or the last element of the list O(1).
    fn lindex<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, index: T0) {
        cmd("LINDEX").arg(key).arg(index)
    }

    /// LINSERT
    /// 
    /// Insert an element before or after another element in a list
    /// 
    /// Since: Redis 2.2.0
    /// Group: List
    /// Complexity: O(N) where N is the number of elements to traverse before seeing the value pivot. This means that inserting somewhere on the left end on the list (head) can be considered O(1) and inserting somewhere on the right end (tail) is O(N).
    fn linsert<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, pivot: T0, element: T1) {
        cmd("LINSERT").arg(key).arg(pivot).arg(element)
    }

    /// LLEN
    /// 
    /// Get the length of a list
    /// 
    /// Since: Redis 1.0.0
    /// Group: List
    /// Complexity: O(1)
    fn llen<K0: ToRedisArgs>(key: K0) {
        cmd("LLEN").arg(key)
    }

    /// LMOVE
    /// 
    /// Pop an element from a list, push it to another list and return it
    /// 
    /// Since: Redis 6.2.0
    /// Group: List
    /// Complexity: O(1)
    fn lmove<K0: ToRedisArgs, K1: ToRedisArgs>(source: K0, destination: K1) {
        cmd("LMOVE").arg(source).arg(destination)
    }

    /// LMPOP
    /// 
    /// Pop elements from a list
    /// 
    /// Since: Redis 7.0.0
    /// Group: List
    /// Complexity: O(N+M) where N is the number of provided keys and M is the number of elements returned.
    fn lmpop<T0: ToRedisArgs, K0: ToRedisArgs>(numkeys: T0, key: K0) {
        cmd("LMPOP").arg(numkeys).arg(key)
    }

    /// LPOP
    /// 
    /// Remove and get the first elements in a list
    /// 
    /// Since: Redis 1.0.0
    /// Group: List
    /// Complexity: O(N) where N is the number of elements returned
    fn lpop<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, count: Option<T0>) {
        cmd("LPOP").arg(key).arg(count)
    }

    /// LPOS
    /// 
    /// Return the index of matching elements on a list
    /// 
    /// Since: Redis 6.0.6
    /// Group: List
    /// Complexity: O(N) where N is the number of elements in the list, for the average case. When searching for elements near the head or the tail of the list, or when the MAXLEN option is provided, the command may run in constant time.
    fn lpos<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, element: T0) {
        cmd("LPOS").arg(key).arg(element)
    }

    /// LPUSH
    /// 
    /// Prepend one or multiple elements to a list
    /// 
    /// Since: Redis 1.0.0
    /// Group: List
    /// Complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
    fn lpush<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, element: T0) {
        cmd("LPUSH").arg(key).arg(element)
    }

    /// LPUSHX
    /// 
    /// Prepend an element to a list, only if the list exists
    /// 
    /// Since: Redis 2.2.0
    /// Group: List
    /// Complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
    fn lpushx<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, element: T0) {
        cmd("LPUSHX").arg(key).arg(element)
    }

    /// LRANGE
    /// 
    /// Get a range of elements from a list
    /// 
    /// Since: Redis 1.0.0
    /// Group: List
    /// Complexity: O(S+N) where S is the distance of start offset from HEAD for small lists, from nearest end (HEAD or TAIL) for large lists; and N is the number of elements in the specified range.
    fn lrange<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, start: T0, stop: T1) {
        cmd("LRANGE").arg(key).arg(start).arg(stop)
    }

    /// LREM
    /// 
    /// Remove elements from a list
    /// 
    /// Since: Redis 1.0.0
    /// Group: List
    /// Complexity: O(N+M) where N is the length of the list and M is the number of elements removed.
    fn lrem<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, count: T0, element: T1) {
        cmd("LREM").arg(key).arg(count).arg(element)
    }

    /// LSET
    /// 
    /// Set the value of an element in a list by its index
    /// 
    /// Since: Redis 1.0.0
    /// Group: List
    /// Complexity: O(N) where N is the length of the list. Setting either the first or the last element of the list is O(1).
    fn lset<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, index: T0, element: T1) {
        cmd("LSET").arg(key).arg(index).arg(element)
    }

    /// LTRIM
    /// 
    /// Trim a list to the specified range
    /// 
    /// Since: Redis 1.0.0
    /// Group: List
    /// Complexity: O(N) where N is the number of elements to be removed by the operation.
    fn ltrim<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, start: T0, stop: T1) {
        cmd("LTRIM").arg(key).arg(start).arg(stop)
    }

    /// RPOP
    /// 
    /// Remove and get the last elements in a list
    /// 
    /// Since: Redis 1.0.0
    /// Group: List
    /// Complexity: O(N) where N is the number of elements returned
    fn rpop<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, count: Option<T0>) {
        cmd("RPOP").arg(key).arg(count)
    }

    /// RPOPLPUSH
    /// 
    /// Remove the last element in a list, prepend it to another list and return it
    /// 
    /// Since: Redis 1.2.0
    /// Group: List
    /// Replaced By: `LMOVE` with the `RIGHT` and `LEFT` arguments
    /// Complexity: O(1)
    /// Replaced By: `LMOVE` with the `RIGHT` and `LEFT` arguments
    #[deprecated]    fn rpoplpush<K0: ToRedisArgs, K1: ToRedisArgs>(source: K0, destination: K1) {
        cmd("RPOPLPUSH").arg(source).arg(destination)
    }

    /// RPUSH
    /// 
    /// Append one or multiple elements to a list
    /// 
    /// Since: Redis 1.0.0
    /// Group: List
    /// Complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
    fn rpush<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, element: T0) {
        cmd("RPUSH").arg(key).arg(element)
    }

    /// RPUSHX
    /// 
    /// Append an element to a list, only if the list exists
    /// 
    /// Since: Redis 2.2.0
    /// Group: List
    /// Complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
    fn rpushx<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, element: T0) {
        cmd("RPUSHX").arg(key).arg(element)
    }

    /// SADD
    /// 
    /// Add one or more members to a set
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
    fn sadd<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, member: T0) {
        cmd("SADD").arg(key).arg(member)
    }

    /// SCARD
    /// 
    /// Get the number of members in a set
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: O(1)
    fn scard<K0: ToRedisArgs>(key: K0) {
        cmd("SCARD").arg(key)
    }

    /// SDIFF
    /// 
    /// Subtract multiple sets
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: O(N) where N is the total number of elements in all given sets.
    fn sdiff<K0: ToRedisArgs>(key: K0) {
        cmd("SDIFF").arg(key)
    }

    /// SDIFFSTORE
    /// 
    /// Subtract multiple sets and store the resulting set in a key
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: O(N) where N is the total number of elements in all given sets.
    fn sdiffstore<K0: ToRedisArgs, K1: ToRedisArgs>(destination: K0, key: K1) {
        cmd("SDIFFSTORE").arg(destination).arg(key)
    }

    /// SINTER
    /// 
    /// Intersect multiple sets
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets.
    fn sinter<K0: ToRedisArgs>(key: K0) {
        cmd("SINTER").arg(key)
    }

    /// SINTERCARD
    /// 
    /// Intersect multiple sets and return the cardinality of the result
    /// 
    /// Since: Redis 7.0.0
    /// Group: Set
    /// Complexity: O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets.
    fn sintercard<T0: ToRedisArgs, K0: ToRedisArgs>(numkeys: T0, key: K0) {
        cmd("SINTERCARD").arg(numkeys).arg(key)
    }

    /// SINTERSTORE
    /// 
    /// Intersect multiple sets and store the resulting set in a key
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets.
    fn sinterstore<K0: ToRedisArgs, K1: ToRedisArgs>(destination: K0, key: K1) {
        cmd("SINTERSTORE").arg(destination).arg(key)
    }

    /// SISMEMBER
    /// 
    /// Determine if a given value is a member of a set
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: O(1)
    fn sismember<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, member: T0) {
        cmd("SISMEMBER").arg(key).arg(member)
    }

    /// SMEMBERS
    /// 
    /// Get all the members in a set
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: O(N) where N is the set cardinality.
    fn smembers<K0: ToRedisArgs>(key: K0) {
        cmd("SMEMBERS").arg(key)
    }

    /// SMISMEMBER
    /// 
    /// Returns the membership associated with the given elements for a set
    /// 
    /// Since: Redis 6.2.0
    /// Group: Set
    /// Complexity: O(N) where N is the number of elements being checked for membership
    fn smismember<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, member: T0) {
        cmd("SMISMEMBER").arg(key).arg(member)
    }

    /// SMOVE
    /// 
    /// Move a member from one set to another
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: O(1)
    fn smove<K0: ToRedisArgs, K1: ToRedisArgs, T0: ToRedisArgs>(source: K0, destination: K1, member: T0) {
        cmd("SMOVE").arg(source).arg(destination).arg(member)
    }

    /// SPOP
    /// 
    /// Remove and return one or multiple random members from a set
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: Without the count argument O(1), otherwise O(N) where N is the value of the passed count.
    fn spop<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, count: Option<T0>) {
        cmd("SPOP").arg(key).arg(count)
    }

    /// SRANDMEMBER
    /// 
    /// Get one or multiple random members from a set
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: Without the count argument O(1), otherwise O(N) where N is the absolute value of the passed count.
    fn srandmember<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, count: Option<T0>) {
        cmd("SRANDMEMBER").arg(key).arg(count)
    }

    /// SREM
    /// 
    /// Remove one or more members from a set
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: O(N) where N is the number of members to be removed.
    fn srem<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, member: T0) {
        cmd("SREM").arg(key).arg(member)
    }

    /// SUNION
    /// 
    /// Add multiple sets
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: O(N) where N is the total number of elements in all given sets.
    fn sunion<K0: ToRedisArgs>(key: K0) {
        cmd("SUNION").arg(key)
    }

    /// SUNIONSTORE
    /// 
    /// Add multiple sets and store the resulting set in a key
    /// 
    /// Since: Redis 1.0.0
    /// Group: Set
    /// Complexity: O(N) where N is the total number of elements in all given sets.
    fn sunionstore<K0: ToRedisArgs, K1: ToRedisArgs>(destination: K0, key: K1) {
        cmd("SUNIONSTORE").arg(destination).arg(key)
    }

    /// BZMPOP
    /// 
    /// Remove and return members with scores in a sorted set or block until one is available
    /// 
    /// Since: Redis 7.0.0
    /// Group: SortedSet
    /// Complexity: O(K) + O(N*log(M)) where K is the number of provided keys, N being the number of elements in the sorted set, and M being the number of elements popped.
    fn bzmpop<T0: ToRedisArgs, T1: ToRedisArgs, K0: ToRedisArgs>(timeout: T0, numkeys: T1, key: K0) {
        cmd("BZMPOP").arg(timeout).arg(numkeys).arg(key)
    }

    /// BZPOPMAX
    /// 
    /// Remove and return the member with the highest score from one or more sorted sets, or block until one is available
    /// 
    /// Since: Redis 5.0.0
    /// Group: SortedSet
    /// Complexity: O(log(N)) with N being the number of elements in the sorted set.
    fn bzpopmax<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, timeout: T0) {
        cmd("BZPOPMAX").arg(key).arg(timeout)
    }

    /// BZPOPMIN
    /// 
    /// Remove and return the member with the lowest score from one or more sorted sets, or block until one is available
    /// 
    /// Since: Redis 5.0.0
    /// Group: SortedSet
    /// Complexity: O(log(N)) with N being the number of elements in the sorted set.
    fn bzpopmin<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, timeout: T0) {
        cmd("BZPOPMIN").arg(key).arg(timeout)
    }

    /// ZADD
    /// 
    /// Add one or more members to a sorted set, or update its score if it already exists
    /// 
    /// Since: Redis 1.2.0
    /// Group: SortedSet
    /// Complexity: O(log(N)) for each item added, where N is the number of elements in the sorted set.
    fn zadd<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, score_member: T0) {
        cmd("ZADD").arg(key).arg(score_member)
    }

    /// ZCARD
    /// 
    /// Get the number of members in a sorted set
    /// 
    /// Since: Redis 1.2.0
    /// Group: SortedSet
    /// Complexity: O(1)
    fn zcard<K0: ToRedisArgs>(key: K0) {
        cmd("ZCARD").arg(key)
    }

    /// ZCOUNT
    /// 
    /// Count the members in a sorted set with scores within the given values
    /// 
    /// Since: Redis 2.0.0
    /// Group: SortedSet
    /// Complexity: O(log(N)) with N being the number of elements in the sorted set.
    fn zcount<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, min: T0, max: T1) {
        cmd("ZCOUNT").arg(key).arg(min).arg(max)
    }

    /// ZDIFF
    /// 
    /// Subtract multiple sorted sets
    /// 
    /// Since: Redis 6.2.0
    /// Group: SortedSet
    /// Complexity: O(L + (N-K)log(N)) worst case where L is the total number of elements in all the sets, N is the size of the first set, and K is the size of the result set.
    fn zdiff<T0: ToRedisArgs, K0: ToRedisArgs>(numkeys: T0, key: K0) {
        cmd("ZDIFF").arg(numkeys).arg(key)
    }

    /// ZDIFFSTORE
    /// 
    /// Subtract multiple sorted sets and store the resulting sorted set in a new key
    /// 
    /// Since: Redis 6.2.0
    /// Group: SortedSet
    /// Complexity: O(L + (N-K)log(N)) worst case where L is the total number of elements in all the sets, N is the size of the first set, and K is the size of the result set.
    fn zdiffstore<K0: ToRedisArgs, T0: ToRedisArgs, K1: ToRedisArgs>(destination: K0, numkeys: T0, key: K1) {
        cmd("ZDIFFSTORE").arg(destination).arg(numkeys).arg(key)
    }

    /// ZINCRBY
    /// 
    /// Increment the score of a member in a sorted set
    /// 
    /// Since: Redis 1.2.0
    /// Group: SortedSet
    /// Complexity: O(log(N)) where N is the number of elements in the sorted set.
    fn zincrby<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, increment: T0, member: T1) {
        cmd("ZINCRBY").arg(key).arg(increment).arg(member)
    }

    /// ZINTER
    /// 
    /// Intersect multiple sorted sets
    /// 
    /// Since: Redis 6.2.0
    /// Group: SortedSet
    /// Complexity: O(N*K)+O(M*log(M)) worst case with N being the smallest input sorted set, K being the number of input sorted sets and M being the number of elements in the resulting sorted set.
    fn zinter<T0: ToRedisArgs, K0: ToRedisArgs>(numkeys: T0, key: K0) {
        cmd("ZINTER").arg(numkeys).arg(key)
    }

    /// ZINTERCARD
    /// 
    /// Intersect multiple sorted sets and return the cardinality of the result
    /// 
    /// Since: Redis 7.0.0
    /// Group: SortedSet
    /// Complexity: O(N*K) worst case with N being the smallest input sorted set, K being the number of input sorted sets.
    fn zintercard<T0: ToRedisArgs, K0: ToRedisArgs>(numkeys: T0, key: K0) {
        cmd("ZINTERCARD").arg(numkeys).arg(key)
    }

    /// ZINTERSTORE
    /// 
    /// Intersect multiple sorted sets and store the resulting sorted set in a new key
    /// 
    /// Since: Redis 2.0.0
    /// Group: SortedSet
    /// Complexity: O(N*K)+O(M*log(M)) worst case with N being the smallest input sorted set, K being the number of input sorted sets and M being the number of elements in the resulting sorted set.
    fn zinterstore<K0: ToRedisArgs, T0: ToRedisArgs, K1: ToRedisArgs>(destination: K0, numkeys: T0, key: K1) {
        cmd("ZINTERSTORE").arg(destination).arg(numkeys).arg(key)
    }

    /// ZLEXCOUNT
    /// 
    /// Count the number of members in a sorted set between a given lexicographical range
    /// 
    /// Since: Redis 2.8.9
    /// Group: SortedSet
    /// Complexity: O(log(N)) with N being the number of elements in the sorted set.
    fn zlexcount<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, min: T0, max: T1) {
        cmd("ZLEXCOUNT").arg(key).arg(min).arg(max)
    }

    /// ZMPOP
    /// 
    /// Remove and return members with scores in a sorted set
    /// 
    /// Since: Redis 7.0.0
    /// Group: SortedSet
    /// Complexity: O(K) + O(N*log(M)) where K is the number of provided keys, N being the number of elements in the sorted set, and M being the number of elements popped.
    fn zmpop<T0: ToRedisArgs, K0: ToRedisArgs>(numkeys: T0, key: K0) {
        cmd("ZMPOP").arg(numkeys).arg(key)
    }

    /// ZMSCORE
    /// 
    /// Get the score associated with the given members in a sorted set
    /// 
    /// Since: Redis 6.2.0
    /// Group: SortedSet
    /// Complexity: O(N) where N is the number of members being requested.
    fn zmscore<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, member: T0) {
        cmd("ZMSCORE").arg(key).arg(member)
    }

    /// ZPOPMAX
    /// 
    /// Remove and return members with the highest scores in a sorted set
    /// 
    /// Since: Redis 5.0.0
    /// Group: SortedSet
    /// Complexity: O(log(N)*M) with N being the number of elements in the sorted set, and M being the number of elements popped.
    fn zpopmax<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, count: Option<T0>) {
        cmd("ZPOPMAX").arg(key).arg(count)
    }

    /// ZPOPMIN
    /// 
    /// Remove and return members with the lowest scores in a sorted set
    /// 
    /// Since: Redis 5.0.0
    /// Group: SortedSet
    /// Complexity: O(log(N)*M) with N being the number of elements in the sorted set, and M being the number of elements popped.
    fn zpopmin<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, count: Option<T0>) {
        cmd("ZPOPMIN").arg(key).arg(count)
    }

    /// ZRANDMEMBER
    /// 
    /// Get one or multiple random elements from a sorted set
    /// 
    /// Since: Redis 6.2.0
    /// Group: SortedSet
    /// Complexity: O(N) where N is the number of elements returned
    fn zrandmember<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, options: Option<T0>) {
        cmd("ZRANDMEMBER").arg(key).arg(options)
    }

    /// ZRANGE
    /// 
    /// Return a range of members in a sorted set
    /// 
    /// Since: Redis 1.2.0
    /// Group: SortedSet
    /// Complexity: O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned.
    fn zrange<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, min: T0, max: T1) {
        cmd("ZRANGE").arg(key).arg(min).arg(max)
    }

    /// ZRANGEBYLEX
    /// 
    /// Return a range of members in a sorted set, by lexicographical range
    /// 
    /// Since: Redis 2.8.9
    /// Group: SortedSet
    /// Replaced By: `ZRANGE` with the `BYLEX` argument
    /// Complexity: O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned. If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
    /// Replaced By: `ZRANGE` with the `BYLEX` argument
    #[deprecated]    fn zrangebylex<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, min: T0, max: T1) {
        cmd("ZRANGEBYLEX").arg(key).arg(min).arg(max)
    }

    /// ZRANGEBYSCORE
    /// 
    /// Return a range of members in a sorted set, by score
    /// 
    /// Since: Redis 1.0.5
    /// Group: SortedSet
    /// Replaced By: `ZRANGE` with the `BYSCORE` argument
    /// Complexity: O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned. If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
    /// Replaced By: `ZRANGE` with the `BYSCORE` argument
    #[deprecated]    fn zrangebyscore<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, min: T0, max: T1) {
        cmd("ZRANGEBYSCORE").arg(key).arg(min).arg(max)
    }

    /// ZRANGESTORE
    /// 
    /// Store a range of members from sorted set into another key
    /// 
    /// Since: Redis 6.2.0
    /// Group: SortedSet
    /// Complexity: O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements stored into the destination key.
    fn zrangestore<K0: ToRedisArgs, K1: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(dst: K0, src: K1, min: T0, max: T1) {
        cmd("ZRANGESTORE").arg(dst).arg(src).arg(min).arg(max)
    }

    /// ZRANK
    /// 
    /// Determine the index of a member in a sorted set
    /// 
    /// Since: Redis 2.0.0
    /// Group: SortedSet
    /// Complexity: O(log(N))
    fn zrank<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, member: T0) {
        cmd("ZRANK").arg(key).arg(member)
    }

    /// ZREM
    /// 
    /// Remove one or more members from a sorted set
    /// 
    /// Since: Redis 1.2.0
    /// Group: SortedSet
    /// Complexity: O(M*log(N)) with N being the number of elements in the sorted set and M the number of elements to be removed.
    fn zrem<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, member: T0) {
        cmd("ZREM").arg(key).arg(member)
    }

    /// ZREMRANGEBYLEX
    /// 
    /// Remove all members in a sorted set between the given lexicographical range
    /// 
    /// Since: Redis 2.8.9
    /// Group: SortedSet
    /// Complexity: O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements removed by the operation.
    fn zremrangebylex<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, min: T0, max: T1) {
        cmd("ZREMRANGEBYLEX").arg(key).arg(min).arg(max)
    }

    /// ZREMRANGEBYRANK
    /// 
    /// Remove all members in a sorted set within the given indexes
    /// 
    /// Since: Redis 2.0.0
    /// Group: SortedSet
    /// Complexity: O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements removed by the operation.
    fn zremrangebyrank<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, start: T0, stop: T1) {
        cmd("ZREMRANGEBYRANK").arg(key).arg(start).arg(stop)
    }

    /// ZREMRANGEBYSCORE
    /// 
    /// Remove all members in a sorted set within the given scores
    /// 
    /// Since: Redis 1.2.0
    /// Group: SortedSet
    /// Complexity: O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements removed by the operation.
    fn zremrangebyscore<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, min: T0, max: T1) {
        cmd("ZREMRANGEBYSCORE").arg(key).arg(min).arg(max)
    }

    /// ZREVRANGE
    /// 
    /// Return a range of members in a sorted set, by index, with scores ordered from high to low
    /// 
    /// Since: Redis 1.2.0
    /// Group: SortedSet
    /// Replaced By: `ZRANGE` with the `REV` argument
    /// Complexity: O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned.
    /// Replaced By: `ZRANGE` with the `REV` argument
    #[deprecated]    fn zrevrange<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, start: T0, stop: T1) {
        cmd("ZREVRANGE").arg(key).arg(start).arg(stop)
    }

    /// ZREVRANGEBYLEX
    /// 
    /// Return a range of members in a sorted set, by lexicographical range, ordered from higher to lower strings.
    /// 
    /// Since: Redis 2.8.9
    /// Group: SortedSet
    /// Replaced By: `ZRANGE` with the `REV` and `BYLEX` arguments
    /// Complexity: O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned. If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
    /// Replaced By: `ZRANGE` with the `REV` and `BYLEX` arguments
    #[deprecated]    fn zrevrangebylex<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, max: T0, min: T1) {
        cmd("ZREVRANGEBYLEX").arg(key).arg(max).arg(min)
    }

    /// ZREVRANGEBYSCORE
    /// 
    /// Return a range of members in a sorted set, by score, with scores ordered from high to low
    /// 
    /// Since: Redis 2.2.0
    /// Group: SortedSet
    /// Replaced By: `ZRANGE` with the `REV` and `BYSCORE` arguments
    /// Complexity: O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned. If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N)).
    /// Replaced By: `ZRANGE` with the `REV` and `BYSCORE` arguments
    #[deprecated]    fn zrevrangebyscore<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, max: T0, min: T1) {
        cmd("ZREVRANGEBYSCORE").arg(key).arg(max).arg(min)
    }

    /// ZREVRANK
    /// 
    /// Determine the index of a member in a sorted set, with scores ordered from high to low
    /// 
    /// Since: Redis 2.0.0
    /// Group: SortedSet
    /// Complexity: O(log(N))
    fn zrevrank<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, member: T0) {
        cmd("ZREVRANK").arg(key).arg(member)
    }

    /// ZSCORE
    /// 
    /// Get the score associated with the given member in a sorted set
    /// 
    /// Since: Redis 1.2.0
    /// Group: SortedSet
    /// Complexity: O(1)
    fn zscore<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, member: T0) {
        cmd("ZSCORE").arg(key).arg(member)
    }

    /// ZUNION
    /// 
    /// Add multiple sorted sets
    /// 
    /// Since: Redis 6.2.0
    /// Group: SortedSet
    /// Complexity: O(N)+O(M*log(M)) with N being the sum of the sizes of the input sorted sets, and M being the number of elements in the resulting sorted set.
    fn zunion<T0: ToRedisArgs, K0: ToRedisArgs>(numkeys: T0, key: K0) {
        cmd("ZUNION").arg(numkeys).arg(key)
    }

    /// ZUNIONSTORE
    /// 
    /// Add multiple sorted sets and store the resulting sorted set in a new key
    /// 
    /// Since: Redis 2.0.0
    /// Group: SortedSet
    /// Complexity: O(N)+O(M log(M)) with N being the sum of the sizes of the input sorted sets, and M being the number of elements in the resulting sorted set.
    fn zunionstore<K0: ToRedisArgs, T0: ToRedisArgs, K1: ToRedisArgs>(destination: K0, numkeys: T0, key: K1) {
        cmd("ZUNIONSTORE").arg(destination).arg(numkeys).arg(key)
    }

    /// HDEL
    /// 
    /// Delete one or more hash fields
    /// 
    /// Since: Redis 2.0.0
    /// Group: Hash
    /// Complexity: O(N) where N is the number of fields to be removed.
    fn hdel<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, field: T0) {
        cmd("HDEL").arg(key).arg(field)
    }

    /// HEXISTS
    /// 
    /// Determine if a hash field exists
    /// 
    /// Since: Redis 2.0.0
    /// Group: Hash
    /// Complexity: O(1)
    fn hexists<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, field: T0) {
        cmd("HEXISTS").arg(key).arg(field)
    }

    /// HGET
    /// 
    /// Get the value of a hash field
    /// 
    /// Since: Redis 2.0.0
    /// Group: Hash
    /// Complexity: O(1)
    fn hget<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, field: T0) {
        cmd("HGET").arg(key).arg(field)
    }

    /// HGETALL
    /// 
    /// Get all the fields and values in a hash
    /// 
    /// Since: Redis 2.0.0
    /// Group: Hash
    /// Complexity: O(N) where N is the size of the hash.
    fn hgetall<K0: ToRedisArgs>(key: K0) {
        cmd("HGETALL").arg(key)
    }

    /// HINCRBY
    /// 
    /// Increment the integer value of a hash field by the given number
    /// 
    /// Since: Redis 2.0.0
    /// Group: Hash
    /// Complexity: O(1)
    fn hincrby<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, field: T0, increment: T1) {
        cmd("HINCRBY").arg(key).arg(field).arg(increment)
    }

    /// HINCRBYFLOAT
    /// 
    /// Increment the float value of a hash field by the given amount
    /// 
    /// Since: Redis 2.6.0
    /// Group: Hash
    /// Complexity: O(1)
    fn hincrbyfloat<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, field: T0, increment: T1) {
        cmd("HINCRBYFLOAT").arg(key).arg(field).arg(increment)
    }

    /// HKEYS
    /// 
    /// Get all the fields in a hash
    /// 
    /// Since: Redis 2.0.0
    /// Group: Hash
    /// Complexity: O(N) where N is the size of the hash.
    fn hkeys<K0: ToRedisArgs>(key: K0) {
        cmd("HKEYS").arg(key)
    }

    /// HLEN
    /// 
    /// Get the number of fields in a hash
    /// 
    /// Since: Redis 2.0.0
    /// Group: Hash
    /// Complexity: O(1)
    fn hlen<K0: ToRedisArgs>(key: K0) {
        cmd("HLEN").arg(key)
    }

    /// HMGET
    /// 
    /// Get the values of all the given hash fields
    /// 
    /// Since: Redis 2.0.0
    /// Group: Hash
    /// Complexity: O(N) where N is the number of fields being requested.
    fn hmget<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, field: T0) {
        cmd("HMGET").arg(key).arg(field)
    }

    /// HMSET
    /// 
    /// Set multiple hash fields to multiple values
    /// 
    /// Since: Redis 2.0.0
    /// Group: Hash
    /// Replaced By: `HSET` with multiple field-value pairs
    /// Complexity: O(N) where N is the number of fields being set.
    /// Replaced By: `HSET` with multiple field-value pairs
    #[deprecated]    fn hmset<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, field_value: T0) {
        cmd("HMSET").arg(key).arg(field_value)
    }

    /// HRANDFIELD
    /// 
    /// Get one or multiple random fields from a hash
    /// 
    /// Since: Redis 6.2.0
    /// Group: Hash
    /// Complexity: O(N) where N is the number of fields returned
    fn hrandfield<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, options: Option<T0>) {
        cmd("HRANDFIELD").arg(key).arg(options)
    }

    /// HSET
    /// 
    /// Set the string value of a hash field
    /// 
    /// Since: Redis 2.0.0
    /// Group: Hash
    /// Complexity: O(1) for each field/value pair added, so O(N) to add N field/value pairs when the command is called with multiple field/value pairs.
    fn hset<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, field_value: T0) {
        cmd("HSET").arg(key).arg(field_value)
    }

    /// HSETNX
    /// 
    /// Set the value of a hash field, only if the field does not exist
    /// 
    /// Since: Redis 2.0.0
    /// Group: Hash
    /// Complexity: O(1)
    fn hsetnx<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, field: T0, value: T1) {
        cmd("HSETNX").arg(key).arg(field).arg(value)
    }

    /// HSTRLEN
    /// 
    /// Get the length of the value of a hash field
    /// 
    /// Since: Redis 3.2.0
    /// Group: Hash
    /// Complexity: O(1)
    fn hstrlen<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, field: T0) {
        cmd("HSTRLEN").arg(key).arg(field)
    }

    /// HVALS
    /// 
    /// Get all the values in a hash
    /// 
    /// Since: Redis 2.0.0
    /// Group: Hash
    /// Complexity: O(N) where N is the size of the hash.
    fn hvals<K0: ToRedisArgs>(key: K0) {
        cmd("HVALS").arg(key)
    }

    /// PSUBSCRIBE
    /// 
    /// Listen for messages published to channels matching the given patterns
    /// 
    /// Since: Redis 2.0.0
    /// Group: Pubsub
    /// Complexity: O(N) where N is the number of patterns the client is already subscribed to.
    fn psubscribe<T0: ToRedisArgs>(pattern: T0) {
        cmd("PSUBSCRIBE").arg(pattern)
    }

    /// PUBLISH
    /// 
    /// Post a message to a channel
    /// 
    /// Since: Redis 2.0.0
    /// Group: Pubsub
    /// Complexity: O(N+M) where N is the number of clients subscribed to the receiving channel and M is the total number of subscribed patterns (by any client).
    fn publish<T0: ToRedisArgs, T1: ToRedisArgs>(channel: T0, message: T1) {
        cmd("PUBLISH").arg(channel).arg(message)
    }

    /// PUBSUB
    /// 
    /// A container for Pub/Sub commands
    /// 
    /// Since: Redis 2.8.0
    /// Group: Pubsub
    /// Complexity: Depends on subcommand.
    fn pubsub<>() {
        cmd("PUBSUB").as_mut()
    }

    /// PUBSUB CHANNELS
    /// 
    /// List active channels
    /// 
    /// Since: Redis 2.8.0
    /// Group: Pubsub
    /// Complexity: O(N) where N is the number of active channels, and assuming constant time pattern matching (relatively short channels and patterns)
    fn pubsub_channels<K0: ToRedisArgs>(pattern: Option<K0>) {
        cmd("PUBSUB CHANNELS").arg(pattern)
    }

    /// PUBSUB HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 6.2.0
    /// Group: Pubsub
    /// Complexity: O(1)
    fn pubsub_help<>() {
        cmd("PUBSUB HELP").as_mut()
    }

    /// PUBSUB NUMPAT
    /// 
    /// Get the count of unique patterns pattern subscriptions
    /// 
    /// Since: Redis 2.8.0
    /// Group: Pubsub
    /// Complexity: O(1)
    fn pubsub_numpat<>() {
        cmd("PUBSUB NUMPAT").as_mut()
    }

    /// PUBSUB NUMSUB
    /// 
    /// Get the count of subscribers for channels
    /// 
    /// Since: Redis 2.8.0
    /// Group: Pubsub
    /// Complexity: O(N) for the NUMSUB subcommand, where N is the number of requested channels
    fn pubsub_numsub<T0: ToRedisArgs>(channel: Option<T0>) {
        cmd("PUBSUB NUMSUB").arg(channel)
    }

    /// PUBSUB SHARDCHANNELS
    /// 
    /// List active shard channels
    /// 
    /// Since: Redis 7.0.0
    /// Group: Pubsub
    /// Complexity: O(N) where N is the number of active shard channels, and assuming constant time pattern matching (relatively short shard channels).
    fn pubsub_shardchannels<K0: ToRedisArgs>(pattern: Option<K0>) {
        cmd("PUBSUB SHARDCHANNELS").arg(pattern)
    }

    /// PUBSUB SHARDNUMSUB
    /// 
    /// Get the count of subscribers for shard channels
    /// 
    /// Since: Redis 7.0.0
    /// Group: Pubsub
    /// Complexity: O(N) for the SHARDNUMSUB subcommand, where N is the number of requested shard channels
    fn pubsub_shardnumsub<T0: ToRedisArgs>(shardchannel: Option<T0>) {
        cmd("PUBSUB SHARDNUMSUB").arg(shardchannel)
    }

    /// PUNSUBSCRIBE
    /// 
    /// Stop listening for messages posted to channels matching the given patterns
    /// 
    /// Since: Redis 2.0.0
    /// Group: Pubsub
    /// Complexity: O(N+M) where N is the number of patterns the client is already subscribed and M is the number of total patterns subscribed in the system (by any client).
    fn punsubscribe<K0: ToRedisArgs>(pattern: Option<K0>) {
        cmd("PUNSUBSCRIBE").arg(pattern)
    }

    /// SPUBLISH
    /// 
    /// Post a message to a shard channel
    /// 
    /// Since: Redis 7.0.0
    /// Group: Pubsub
    /// Complexity: O(N) where N is the number of clients subscribed to the receiving shard channel.
    fn spublish<T0: ToRedisArgs, T1: ToRedisArgs>(shardchannel: T0, message: T1) {
        cmd("SPUBLISH").arg(shardchannel).arg(message)
    }

    /// SSUBSCRIBE
    /// 
    /// Listen for messages published to the given shard channels
    /// 
    /// Since: Redis 7.0.0
    /// Group: Pubsub
    /// Complexity: O(N) where N is the number of shard channels to subscribe to.
    fn ssubscribe<T0: ToRedisArgs>(shardchannel: T0) {
        cmd("SSUBSCRIBE").arg(shardchannel)
    }

    /// SUBSCRIBE
    /// 
    /// Listen for messages published to the given channels
    /// 
    /// Since: Redis 2.0.0
    /// Group: Pubsub
    /// Complexity: O(N) where N is the number of channels to subscribe to.
    fn subscribe<T0: ToRedisArgs>(channel: T0) {
        cmd("SUBSCRIBE").arg(channel)
    }

    /// SUNSUBSCRIBE
    /// 
    /// Stop listening for messages posted to the given shard channels
    /// 
    /// Since: Redis 7.0.0
    /// Group: Pubsub
    /// Complexity: O(N) where N is the number of clients already subscribed to a shard channel.
    fn sunsubscribe<T0: ToRedisArgs>(shardchannel: Option<T0>) {
        cmd("SUNSUBSCRIBE").arg(shardchannel)
    }

    /// UNSUBSCRIBE
    /// 
    /// Stop listening for messages posted to the given channels
    /// 
    /// Since: Redis 2.0.0
    /// Group: Pubsub
    /// Complexity: O(N) where N is the number of clients already subscribed to a channel.
    fn unsubscribe<T0: ToRedisArgs>(channel: Option<T0>) {
        cmd("UNSUBSCRIBE").arg(channel)
    }

    /// DISCARD
    /// 
    /// Discard all commands issued after MULTI
    /// 
    /// Since: Redis 2.0.0
    /// Group: Transactions
    /// Complexity: O(N), when N is the number of queued commands
    fn discard<>() {
        cmd("DISCARD").as_mut()
    }

    /// EXEC
    /// 
    /// Execute all commands issued after MULTI
    /// 
    /// Since: Redis 1.2.0
    /// Group: Transactions
    /// Complexity: Depends on commands in the transaction
    fn exec<>() {
        cmd("EXEC").as_mut()
    }

    /// MULTI
    /// 
    /// Mark the start of a transaction block
    /// 
    /// Since: Redis 1.2.0
    /// Group: Transactions
    /// Complexity: O(1)
    fn multi<>() {
        cmd("MULTI").as_mut()
    }

    /// UNWATCH
    /// 
    /// Forget about all watched keys
    /// 
    /// Since: Redis 2.2.0
    /// Group: Transactions
    /// Complexity: O(1)
    fn unwatch<>() {
        cmd("UNWATCH").as_mut()
    }

    /// WATCH
    /// 
    /// Watch the given keys to determine execution of the MULTI/EXEC block
    /// 
    /// Since: Redis 2.2.0
    /// Group: Transactions
    /// Complexity: O(1) for every key.
    fn watch<K0: ToRedisArgs>(key: K0) {
        cmd("WATCH").arg(key)
    }

    /// AUTH
    /// 
    /// Authenticate to the server
    /// 
    /// Since: Redis 1.0.0
    /// Group: Connection
    /// Complexity: O(N) where N is the number of passwords defined for the user
    fn auth<T0: ToRedisArgs, T1: ToRedisArgs>(username: Option<T0>, password: T1) {
        cmd("AUTH").arg(username).arg(password)
    }

    /// CLIENT
    /// 
    /// A container for client connection commands
    /// 
    /// Since: Redis 2.4.0
    /// Group: Connection
    /// Complexity: Depends on subcommand.
    fn client<>() {
        cmd("CLIENT").as_mut()
    }

    /// CLIENT CACHING
    /// 
    /// Instruct the server about tracking or not keys in the next request
    /// 
    /// Since: Redis 6.0.0
    /// Group: Connection
    /// Complexity: O(1)
    fn client_caching<>() {
        cmd("CLIENT CACHING").as_mut()
    }

    /// CLIENT GETNAME
    /// 
    /// Get the current connection name
    /// 
    /// Since: Redis 2.6.9
    /// Group: Connection
    /// Complexity: O(1)
    fn client_getname<>() {
        cmd("CLIENT GETNAME").as_mut()
    }

    /// CLIENT GETREDIR
    /// 
    /// Get tracking notifications redirection client ID if any
    /// 
    /// Since: Redis 6.0.0
    /// Group: Connection
    /// Complexity: O(1)
    fn client_getredir<>() {
        cmd("CLIENT GETREDIR").as_mut()
    }

    /// CLIENT HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 5.0.0
    /// Group: Connection
    /// Complexity: O(1)
    fn client_help<>() {
        cmd("CLIENT HELP").as_mut()
    }

    /// CLIENT ID
    /// 
    /// Returns the client ID for the current connection
    /// 
    /// Since: Redis 5.0.0
    /// Group: Connection
    /// Complexity: O(1)
    fn client_id<>() {
        cmd("CLIENT ID").as_mut()
    }

    /// CLIENT INFO
    /// 
    /// Returns information about the current client connection.
    /// 
    /// Since: Redis 6.2.0
    /// Group: Connection
    /// Complexity: O(1)
    fn client_info<>() {
        cmd("CLIENT INFO").as_mut()
    }

    /// CLIENT LIST
    /// 
    /// Get the list of client connections
    /// 
    /// Since: Redis 2.4.0
    /// Group: Connection
    /// Complexity: O(N) where N is the number of client connections
    fn client_list<>() {
        cmd("CLIENT LIST").as_mut()
    }

    /// CLIENT NO-EVICT
    /// 
    /// Set client eviction mode for the current connection
    /// 
    /// Since: Redis 7.0.0
    /// Group: Connection
    /// Complexity: O(1)
    fn client_no_evict<>() {
        cmd("CLIENT NO-EVICT").as_mut()
    }

    /// CLIENT PAUSE
    /// 
    /// Stop processing commands from clients for some time
    /// 
    /// Since: Redis 2.9.50
    /// Group: Connection
    /// Complexity: O(1)
    fn client_pause<T0: ToRedisArgs>(timeout: T0) {
        cmd("CLIENT PAUSE").arg(timeout)
    }

    /// CLIENT REPLY
    /// 
    /// Instruct the server whether to reply to commands
    /// 
    /// Since: Redis 3.2.0
    /// Group: Connection
    /// Complexity: O(1)
    fn client_reply<>() {
        cmd("CLIENT REPLY").as_mut()
    }

    /// CLIENT SETNAME
    /// 
    /// Set the current connection name
    /// 
    /// Since: Redis 2.6.9
    /// Group: Connection
    /// Complexity: O(1)
    fn client_setname<T0: ToRedisArgs>(connection_name: T0) {
        cmd("CLIENT SETNAME").arg(connection_name)
    }

    /// CLIENT TRACKING
    /// 
    /// Enable or disable server assisted client side caching support
    /// 
    /// Since: Redis 6.0.0
    /// Group: Connection
    /// Complexity: O(1). Some options may introduce additional complexity.
    fn client_tracking<>() {
        cmd("CLIENT TRACKING").as_mut()
    }

    /// CLIENT TRACKINGINFO
    /// 
    /// Return information about server assisted client side caching for the current connection
    /// 
    /// Since: Redis 6.2.0
    /// Group: Connection
    /// Complexity: O(1)
    fn client_trackinginfo<>() {
        cmd("CLIENT TRACKINGINFO").as_mut()
    }

    /// CLIENT UNBLOCK
    /// 
    /// Unblock a client blocked in a blocking command from a different connection
    /// 
    /// Since: Redis 5.0.0
    /// Group: Connection
    /// Complexity: O(log N) where N is the number of client connections
    fn client_unblock<T0: ToRedisArgs>(client_id: T0) {
        cmd("CLIENT UNBLOCK").arg(client_id)
    }

    /// CLIENT UNPAUSE
    /// 
    /// Resume processing of clients that were paused
    /// 
    /// Since: Redis 6.2.0
    /// Group: Connection
    /// Complexity: O(N) Where N is the number of paused clients
    fn client_unpause<>() {
        cmd("CLIENT UNPAUSE").as_mut()
    }

    /// ECHO
    /// 
    /// Echo the given string
    /// 
    /// Since: Redis 1.0.0
    /// Group: Connection
    /// Complexity: O(1)
    fn echo<T0: ToRedisArgs>(message: T0) {
        cmd("ECHO").arg(message)
    }

    /// HELLO
    /// 
    /// Handshake with Redis
    /// 
    /// Since: Redis 6.0.0
    /// Group: Connection
    /// Complexity: O(1)
    fn hello<T0: ToRedisArgs>(arguments: Option<T0>) {
        cmd("HELLO").arg(arguments)
    }

    /// PING
    /// 
    /// Ping the server
    /// 
    /// Since: Redis 1.0.0
    /// Group: Connection
    /// Complexity: O(1)
    fn ping<T0: ToRedisArgs>(message: Option<T0>) {
        cmd("PING").arg(message)
    }

    /// QUIT
    /// 
    /// Close the connection
    /// 
    /// Since: Redis 1.0.0
    /// Group: Connection
    /// Complexity: O(1)
    fn quit<>() {
        cmd("QUIT").as_mut()
    }

    /// RESET
    /// 
    /// Reset the connection
    /// 
    /// Since: Redis 6.2.0
    /// Group: Connection
    /// Complexity: O(1)
    fn reset<>() {
        cmd("RESET").as_mut()
    }

    /// SELECT
    /// 
    /// Change the selected database for the current connection
    /// 
    /// Since: Redis 1.0.0
    /// Group: Connection
    /// Complexity: O(1)
    fn select<T0: ToRedisArgs>(index: T0) {
        cmd("SELECT").arg(index)
    }

    /// ACL
    /// 
    /// A container for Access List Control commands 
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: Depends on subcommand.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl<>() {
        cmd("ACL").as_mut()
    }

    /// ACL CAT
    /// 
    /// List the ACL categories or the commands inside a category
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: O(1) since the categories and commands are a fixed set.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_cat<T0: ToRedisArgs>(categoryname: Option<T0>) {
        cmd("ACL CAT").arg(categoryname)
    }

    /// ACL DELUSER
    /// 
    /// Remove the specified ACL users and the associated rules
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: O(1) amortized time considering the typical user.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_deluser<T0: ToRedisArgs>(username: T0) {
        cmd("ACL DELUSER").arg(username)
    }

    /// ACL DRYRUN
    /// 
    /// Returns whether the user can execute the given command without executing the command.
    /// 
    /// Since: Redis 7.0.0
    /// Group: Server
    /// Complexity: O(1).
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_dryrun<T0: ToRedisArgs, T1: ToRedisArgs, T2: ToRedisArgs>(username: T0, command: T1, arg: Option<T2>) {
        cmd("ACL DRYRUN").arg(username).arg(command).arg(arg)
    }

    /// ACL GENPASS
    /// 
    /// Generate a pseudorandom secure password to use for ACL users
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: O(1)
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_genpass<T0: ToRedisArgs>(bits: Option<T0>) {
        cmd("ACL GENPASS").arg(bits)
    }

    /// ACL GETUSER
    /// 
    /// Get the rules for a specific ACL user
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: O(N). Where N is the number of password, command and pattern rules that the user has.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_getuser<T0: ToRedisArgs>(username: T0) {
        cmd("ACL GETUSER").arg(username)
    }

    /// ACL HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: O(1)
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_help<>() {
        cmd("ACL HELP").as_mut()
    }

    /// ACL LIST
    /// 
    /// List the current ACL rules in ACL config file format
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: O(N). Where N is the number of configured users.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_list<>() {
        cmd("ACL LIST").as_mut()
    }

    /// ACL LOAD
    /// 
    /// Reload the ACLs from the configured ACL file
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: O(N). Where N is the number of configured users.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_load<>() {
        cmd("ACL LOAD").as_mut()
    }

    /// ACL LOG
    /// 
    /// List latest events denied because of ACLs in place
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: O(N) with N being the number of entries shown.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_log<>() {
        cmd("ACL LOG").as_mut()
    }

    /// ACL SAVE
    /// 
    /// Save the current ACL rules in the configured ACL file
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: O(N). Where N is the number of configured users.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_save<>() {
        cmd("ACL SAVE").as_mut()
    }

    /// ACL SETUSER
    /// 
    /// Modify or create the rules for a specific ACL user
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: O(N). Where N is the number of rules provided.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_setuser<T0: ToRedisArgs, T1: ToRedisArgs>(username: T0, rule: Option<T1>) {
        cmd("ACL SETUSER").arg(username).arg(rule)
    }

    /// ACL USERS
    /// 
    /// List the username of all the configured ACL rules
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: O(N). Where N is the number of configured users.
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_users<>() {
        cmd("ACL USERS").as_mut()
    }

    /// ACL WHOAMI
    /// 
    /// Return the name of the user associated to the current connection
    /// 
    /// Since: Redis 6.0.0
    /// Group: Server
    /// Complexity: O(1)
    #[cfg(feature = "acl")]
    #[cfg_attr(docsrs, doc(cfg(feature = "acl")))]
    fn acl_whoami<>() {
        cmd("ACL WHOAMI").as_mut()
    }

    /// BGREWRITEAOF
    /// 
    /// Asynchronously rewrite the append-only file
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn bgrewriteaof<>() {
        cmd("BGREWRITEAOF").as_mut()
    }

    /// BGSAVE
    /// 
    /// Asynchronously save the dataset to disk
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn bgsave<>() {
        cmd("BGSAVE").as_mut()
    }

    /// COMMAND
    /// 
    /// Get array of Redis command details
    /// 
    /// Since: Redis 2.8.13
    /// Group: Server
    /// Complexity: O(N) where N is the total number of Redis commands
    fn command<>() {
        cmd("COMMAND").as_mut()
    }

    /// COMMAND COUNT
    /// 
    /// Get total number of Redis commands
    /// 
    /// Since: Redis 2.8.13
    /// Group: Server
    /// Complexity: O(1)
    fn command_count<>() {
        cmd("COMMAND COUNT").as_mut()
    }

    /// COMMAND DOCS
    /// 
    /// Get array of specific Redis command documentation
    /// 
    /// Since: Redis 7.0.0
    /// Group: Server
    /// Complexity: O(N) where N is the number of commands to look up
    fn command_docs<T0: ToRedisArgs>(command_name: Option<T0>) {
        cmd("COMMAND DOCS").arg(command_name)
    }

    /// COMMAND GETKEYS
    /// 
    /// Extract keys given a full Redis command
    /// 
    /// Since: Redis 2.8.13
    /// Group: Server
    /// Complexity: O(N) where N is the number of arguments to the command
    fn command_getkeys<>() {
        cmd("COMMAND GETKEYS").as_mut()
    }

    /// COMMAND GETKEYSANDFLAGS
    /// 
    /// Extract keys and access flags given a full Redis command
    /// 
    /// Since: Redis 7.0.0
    /// Group: Server
    /// Complexity: O(N) where N is the number of arguments to the command
    fn command_getkeysandflags<>() {
        cmd("COMMAND GETKEYSANDFLAGS").as_mut()
    }

    /// COMMAND HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 5.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn command_help<>() {
        cmd("COMMAND HELP").as_mut()
    }

    /// COMMAND INFO
    /// 
    /// Get array of specific Redis command details, or all when no argument is given.
    /// 
    /// Since: Redis 2.8.13
    /// Group: Server
    /// Complexity: O(N) where N is the number of commands to look up
    fn command_info<T0: ToRedisArgs>(command_name: Option<T0>) {
        cmd("COMMAND INFO").arg(command_name)
    }

    /// COMMAND LIST
    /// 
    /// Get an array of Redis command names
    /// 
    /// Since: Redis 7.0.0
    /// Group: Server
    /// Complexity: O(N) where N is the total number of Redis commands
    fn command_list<>() {
        cmd("COMMAND LIST").as_mut()
    }

    /// CONFIG
    /// 
    /// A container for server configuration commands
    /// 
    /// Since: Redis 2.0.0
    /// Group: Server
    /// Complexity: Depends on subcommand.
    fn config<>() {
        cmd("CONFIG").as_mut()
    }

    /// CONFIG GET
    /// 
    /// Get the values of configuration parameters
    /// 
    /// Since: Redis 2.0.0
    /// Group: Server
    /// Complexity: O(N) when N is the number of configuration parameters provided
    fn config_get<T0: ToRedisArgs>(parameter: T0) {
        cmd("CONFIG GET").arg(parameter)
    }

    /// CONFIG HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 5.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn config_help<>() {
        cmd("CONFIG HELP").as_mut()
    }

    /// CONFIG RESETSTAT
    /// 
    /// Reset the stats returned by INFO
    /// 
    /// Since: Redis 2.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn config_resetstat<>() {
        cmd("CONFIG RESETSTAT").as_mut()
    }

    /// CONFIG REWRITE
    /// 
    /// Rewrite the configuration file with the in memory configuration
    /// 
    /// Since: Redis 2.8.0
    /// Group: Server
    /// Complexity: O(1)
    fn config_rewrite<>() {
        cmd("CONFIG REWRITE").as_mut()
    }

    /// CONFIG SET
    /// 
    /// Set configuration parameters to the given values
    /// 
    /// Since: Redis 2.0.0
    /// Group: Server
    /// Complexity: O(N) when N is the number of configuration parameters provided
    fn config_set<T0: ToRedisArgs>(parameter_value: T0) {
        cmd("CONFIG SET").arg(parameter_value)
    }

    /// DBSIZE
    /// 
    /// Return the number of keys in the selected database
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn dbsize<>() {
        cmd("DBSIZE").as_mut()
    }

    /// DEBUG
    /// 
    /// A container for debugging commands
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    /// Complexity: Depends on subcommand.
    fn debug<>() {
        cmd("DEBUG").as_mut()
    }

    /// FAILOVER
    /// 
    /// Start a coordinated failover between this server and one of its replicas.
    /// 
    /// Since: Redis 6.2.0
    /// Group: Server
    /// Complexity: O(1)
    fn failover<>() {
        cmd("FAILOVER").as_mut()
    }

    /// FLUSHALL
    /// 
    /// Remove all keys from all databases
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    /// Complexity: O(N) where N is the total number of keys in all databases
    fn flushall<>() {
        cmd("FLUSHALL").as_mut()
    }

    /// FLUSHDB
    /// 
    /// Remove all keys from the current database
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    /// Complexity: O(N) where N is the number of keys in the selected database
    fn flushdb<>() {
        cmd("FLUSHDB").as_mut()
    }

    /// INFO
    /// 
    /// Get information and statistics about the server
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn info<T0: ToRedisArgs>(section: Option<T0>) {
        cmd("INFO").arg(section)
    }

    /// LASTSAVE
    /// 
    /// Get the UNIX time stamp of the last successful save to disk
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn lastsave<>() {
        cmd("LASTSAVE").as_mut()
    }

    /// LATENCY
    /// 
    /// A container for latency diagnostics commands
    /// 
    /// Since: Redis 2.8.13
    /// Group: Server
    /// Complexity: Depends on subcommand.
    fn latency<>() {
        cmd("LATENCY").as_mut()
    }

    /// LATENCY DOCTOR
    /// 
    /// Return a human readable latency analysis report.
    /// 
    /// Since: Redis 2.8.13
    /// Group: Server
    /// Complexity: O(1)
    fn latency_doctor<>() {
        cmd("LATENCY DOCTOR").as_mut()
    }

    /// LATENCY GRAPH
    /// 
    /// Return a latency graph for the event.
    /// 
    /// Since: Redis 2.8.13
    /// Group: Server
    /// Complexity: O(1)
    fn latency_graph<T0: ToRedisArgs>(event: T0) {
        cmd("LATENCY GRAPH").arg(event)
    }

    /// LATENCY HELP
    /// 
    /// Show helpful text about the different subcommands.
    /// 
    /// Since: Redis 2.8.13
    /// Group: Server
    /// Complexity: O(1)
    fn latency_help<>() {
        cmd("LATENCY HELP").as_mut()
    }

    /// LATENCY HISTOGRAM
    /// 
    /// Return the cumulative distribution of latencies of a subset of commands or all.
    /// 
    /// Since: Redis 7.0.0
    /// Group: Server
    /// Complexity: O(N) where N is the number of commands with latency information being retrieved.
    fn latency_histogram<T0: ToRedisArgs>(command: Option<T0>) {
        cmd("LATENCY HISTOGRAM").arg(command)
    }

    /// LATENCY HISTORY
    /// 
    /// Return timestamp-latency samples for the event.
    /// 
    /// Since: Redis 2.8.13
    /// Group: Server
    /// Complexity: O(1)
    fn latency_history<T0: ToRedisArgs>(event: T0) {
        cmd("LATENCY HISTORY").arg(event)
    }

    /// LATENCY LATEST
    /// 
    /// Return the latest latency samples for all events.
    /// 
    /// Since: Redis 2.8.13
    /// Group: Server
    /// Complexity: O(1)
    fn latency_latest<>() {
        cmd("LATENCY LATEST").as_mut()
    }

    /// LATENCY RESET
    /// 
    /// Reset latency data for one or more events.
    /// 
    /// Since: Redis 2.8.13
    /// Group: Server
    /// Complexity: O(1)
    fn latency_reset<T0: ToRedisArgs>(event: Option<T0>) {
        cmd("LATENCY RESET").arg(event)
    }

    /// LOLWUT
    /// 
    /// Display some computer art and the Redis version
    /// 
    /// Since: Redis 5.0.0
    /// Group: Server
    fn lolwut<>() {
        cmd("LOLWUT").as_mut()
    }

    /// MEMORY
    /// 
    /// A container for memory diagnostics commands
    /// 
    /// Since: Redis 4.0.0
    /// Group: Server
    /// Complexity: Depends on subcommand.
    fn memory<>() {
        cmd("MEMORY").as_mut()
    }

    /// MEMORY DOCTOR
    /// 
    /// Outputs memory problems report
    /// 
    /// Since: Redis 4.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn memory_doctor<>() {
        cmd("MEMORY DOCTOR").as_mut()
    }

    /// MEMORY HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 4.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn memory_help<>() {
        cmd("MEMORY HELP").as_mut()
    }

    /// MEMORY MALLOC-STATS
    /// 
    /// Show allocator internal stats
    /// 
    /// Since: Redis 4.0.0
    /// Group: Server
    /// Complexity: Depends on how much memory is allocated, could be slow
    fn memory_malloc_stats<>() {
        cmd("MEMORY MALLOC-STATS").as_mut()
    }

    /// MEMORY PURGE
    /// 
    /// Ask the allocator to release memory
    /// 
    /// Since: Redis 4.0.0
    /// Group: Server
    /// Complexity: Depends on how much memory is allocated, could be slow
    fn memory_purge<>() {
        cmd("MEMORY PURGE").as_mut()
    }

    /// MEMORY STATS
    /// 
    /// Show memory usage details
    /// 
    /// Since: Redis 4.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn memory_stats<>() {
        cmd("MEMORY STATS").as_mut()
    }

    /// MEMORY USAGE
    /// 
    /// Estimate the memory usage of a key
    /// 
    /// Since: Redis 4.0.0
    /// Group: Server
    /// Complexity: O(N) where N is the number of samples.
    fn memory_usage<K0: ToRedisArgs>(key: K0) {
        cmd("MEMORY USAGE").arg(key)
    }

    /// MODULE
    /// 
    /// A container for module commands
    /// 
    /// Since: Redis 4.0.0
    /// Group: Server
    /// Complexity: Depends on subcommand.
    fn module<>() {
        cmd("MODULE").as_mut()
    }

    /// MODULE HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 5.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn module_help<>() {
        cmd("MODULE HELP").as_mut()
    }

    /// MODULE LIST
    /// 
    /// List all modules loaded by the server
    /// 
    /// Since: Redis 4.0.0
    /// Group: Server
    /// Complexity: O(N) where N is the number of loaded modules.
    fn module_list<>() {
        cmd("MODULE LIST").as_mut()
    }

    /// MODULE LOAD
    /// 
    /// Load a module
    /// 
    /// Since: Redis 4.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn module_load<T0: ToRedisArgs, T1: ToRedisArgs>(path: T0, arg: Option<T1>) {
        cmd("MODULE LOAD").arg(path).arg(arg)
    }

    /// MODULE LOADEX
    /// 
    /// Load a module with extended parameters
    /// 
    /// Since: Redis 7.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn module_loadex<T0: ToRedisArgs>(path: T0) {
        cmd("MODULE LOADEX").arg(path)
    }

    /// MODULE UNLOAD
    /// 
    /// Unload a module
    /// 
    /// Since: Redis 4.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn module_unload<T0: ToRedisArgs>(name: T0) {
        cmd("MODULE UNLOAD").arg(name)
    }

    /// MONITOR
    /// 
    /// Listen for all requests received by the server in real time
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    fn monitor<>() {
        cmd("MONITOR").as_mut()
    }

    /// PSYNC
    /// 
    /// Internal command used for replication
    /// 
    /// Since: Redis 2.8.0
    /// Group: Server
    fn psync<T0: ToRedisArgs, T1: ToRedisArgs>(replicationid: T0, offset: T1) {
        cmd("PSYNC").arg(replicationid).arg(offset)
    }

    /// REPLCONF
    /// 
    /// An internal command for configuring the replication stream
    /// 
    /// Since: Redis 3.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn replconf<>() {
        cmd("REPLCONF").as_mut()
    }

    /// REPLICAOF
    /// 
    /// Make the server a replica of another instance, or promote it as master.
    /// 
    /// Since: Redis 5.0.0
    /// Group: Server
    /// Complexity: O(1)
    fn replicaof<T0: ToRedisArgs, T1: ToRedisArgs>(host: T0, port: T1) {
        cmd("REPLICAOF").arg(host).arg(port)
    }

    /// RESTORE-ASKING
    /// 
    /// An internal command for migrating keys in a cluster
    /// 
    /// Since: Redis 3.0.0
    /// Group: Server
    /// Complexity: O(1) to create the new key and additional O(N*M) to reconstruct the serialized value, where N is the number of Redis objects composing the value and M their average size. For small string values the time complexity is thus O(1)+O(1*M) where M is small, so simply O(1). However for sorted set values the complexity is O(N*M*log(N)) because inserting values into sorted sets is O(log(N)).
    fn restore_asking<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, ttl: T0, serialized_value: T1) {
        cmd("RESTORE-ASKING").arg(key).arg(ttl).arg(serialized_value)
    }

    /// ROLE
    /// 
    /// Return the role of the instance in the context of replication
    /// 
    /// Since: Redis 2.8.12
    /// Group: Server
    /// Complexity: O(1)
    fn role<>() {
        cmd("ROLE").as_mut()
    }

    /// SAVE
    /// 
    /// Synchronously save the dataset to disk
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    /// Complexity: O(N) where N is the total number of keys in all databases
    fn save<>() {
        cmd("SAVE").as_mut()
    }

    /// SHUTDOWN
    /// 
    /// Synchronously save the dataset to disk and then shut down the server
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    /// Complexity: O(N) when saving, where N is the total number of keys in all databases when saving data, otherwise O(1)
    fn shutdown<>() {
        cmd("SHUTDOWN").as_mut()
    }

    /// SLAVEOF
    /// 
    /// Make the server a replica of another instance, or promote it as master.
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    /// Replaced By: `REPLICAOF`
    /// Complexity: O(1)
    /// Replaced By: `REPLICAOF`
    #[deprecated]    fn slaveof<T0: ToRedisArgs, T1: ToRedisArgs>(host: T0, port: T1) {
        cmd("SLAVEOF").arg(host).arg(port)
    }

    /// SLOWLOG
    /// 
    /// A container for slow log commands
    /// 
    /// Since: Redis 2.2.12
    /// Group: Server
    /// Complexity: Depends on subcommand.
    fn slowlog<>() {
        cmd("SLOWLOG").as_mut()
    }

    /// SLOWLOG GET
    /// 
    /// Get the slow log's entries
    /// 
    /// Since: Redis 2.2.12
    /// Group: Server
    /// Complexity: O(N) where N is the number of entries returned
    fn slowlog_get<T0: ToRedisArgs>(count: Option<T0>) {
        cmd("SLOWLOG GET").arg(count)
    }

    /// SLOWLOG HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 6.2.0
    /// Group: Server
    /// Complexity: O(1)
    fn slowlog_help<>() {
        cmd("SLOWLOG HELP").as_mut()
    }

    /// SLOWLOG LEN
    /// 
    /// Get the slow log's length
    /// 
    /// Since: Redis 2.2.12
    /// Group: Server
    /// Complexity: O(1)
    fn slowlog_len<>() {
        cmd("SLOWLOG LEN").as_mut()
    }

    /// SLOWLOG RESET
    /// 
    /// Clear all entries from the slow log
    /// 
    /// Since: Redis 2.2.12
    /// Group: Server
    /// Complexity: O(N) where N is the number of entries in the slowlog
    fn slowlog_reset<>() {
        cmd("SLOWLOG RESET").as_mut()
    }

    /// SWAPDB
    /// 
    /// Swaps two Redis databases
    /// 
    /// Since: Redis 4.0.0
    /// Group: Server
    /// Complexity: O(N) where N is the count of clients watching or blocking on keys from both databases.
    fn swapdb<T0: ToRedisArgs, T1: ToRedisArgs>(index1: T0, index2: T1) {
        cmd("SWAPDB").arg(index1).arg(index2)
    }

    /// SYNC
    /// 
    /// Internal command used for replication
    /// 
    /// Since: Redis 1.0.0
    /// Group: Server
    fn sync<>() {
        cmd("SYNC").as_mut()
    }

    /// TIME
    /// 
    /// Return the current server time
    /// 
    /// Since: Redis 2.6.0
    /// Group: Server
    /// Complexity: O(1)
    fn time<>() {
        cmd("TIME").as_mut()
    }

    /// EVAL
    /// 
    /// Execute a Lua script server side
    /// 
    /// Since: Redis 2.6.0
    /// Group: Scripting
    /// Complexity: Depends on the script that is executed.
    fn eval<T0: ToRedisArgs, T1: ToRedisArgs, K0: ToRedisArgs, T2: ToRedisArgs>(script: T0, numkeys: T1, key: Option<K0>, arg: Option<T2>) {
        cmd("EVAL").arg(script).arg(numkeys).arg(key).arg(arg)
    }

    /// EVALSHA
    /// 
    /// Execute a Lua script server side
    /// 
    /// Since: Redis 2.6.0
    /// Group: Scripting
    /// Complexity: Depends on the script that is executed.
    fn evalsha<T0: ToRedisArgs, T1: ToRedisArgs, K0: ToRedisArgs, T2: ToRedisArgs>(sha1: T0, numkeys: T1, key: Option<K0>, arg: Option<T2>) {
        cmd("EVALSHA").arg(sha1).arg(numkeys).arg(key).arg(arg)
    }

    /// EVALSHA_RO
    /// 
    /// Execute a read-only Lua script server side
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: Depends on the script that is executed.
    fn evalsha_ro<T0: ToRedisArgs, T1: ToRedisArgs, K0: ToRedisArgs, T2: ToRedisArgs>(sha1: T0, numkeys: T1, key: K0, arg: T2) {
        cmd("EVALSHA_RO").arg(sha1).arg(numkeys).arg(key).arg(arg)
    }

    /// EVAL_RO
    /// 
    /// Execute a read-only Lua script server side
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: Depends on the script that is executed.
    fn eval_ro<T0: ToRedisArgs, T1: ToRedisArgs, K0: ToRedisArgs, T2: ToRedisArgs>(script: T0, numkeys: T1, key: K0, arg: T2) {
        cmd("EVAL_RO").arg(script).arg(numkeys).arg(key).arg(arg)
    }

    /// FCALL
    /// 
    /// Invoke a function
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: Depends on the function that is executed.
    fn fcall<T0: ToRedisArgs, T1: ToRedisArgs, K0: ToRedisArgs, T2: ToRedisArgs>(function: T0, numkeys: T1, key: K0, arg: T2) {
        cmd("FCALL").arg(function).arg(numkeys).arg(key).arg(arg)
    }

    /// FCALL_RO
    /// 
    /// Invoke a read-only function
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: Depends on the function that is executed.
    fn fcall_ro<T0: ToRedisArgs, T1: ToRedisArgs, K0: ToRedisArgs, T2: ToRedisArgs>(function: T0, numkeys: T1, key: K0, arg: T2) {
        cmd("FCALL_RO").arg(function).arg(numkeys).arg(key).arg(arg)
    }

    /// FUNCTION
    /// 
    /// A container for function commands
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: Depends on subcommand.
    fn function<>() {
        cmd("FUNCTION").as_mut()
    }

    /// FUNCTION DELETE
    /// 
    /// Delete a function by name
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: O(1)
    fn function_delete<T0: ToRedisArgs>(library_name: T0) {
        cmd("FUNCTION DELETE").arg(library_name)
    }

    /// FUNCTION DUMP
    /// 
    /// Dump all functions into a serialized binary payload
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: O(N) where N is the number of functions
    fn function_dump<>() {
        cmd("FUNCTION DUMP").as_mut()
    }

    /// FUNCTION FLUSH
    /// 
    /// Deleting all functions
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: O(N) where N is the number of functions deleted
    fn function_flush<>() {
        cmd("FUNCTION FLUSH").as_mut()
    }

    /// FUNCTION HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: O(1)
    fn function_help<>() {
        cmd("FUNCTION HELP").as_mut()
    }

    /// FUNCTION KILL
    /// 
    /// Kill the function currently in execution.
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: O(1)
    fn function_kill<>() {
        cmd("FUNCTION KILL").as_mut()
    }

    /// FUNCTION LIST
    /// 
    /// List information about all the functions
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: O(N) where N is the number of functions
    fn function_list<>() {
        cmd("FUNCTION LIST").as_mut()
    }

    /// FUNCTION LOAD
    /// 
    /// Create a function with the given arguments (name, code, description)
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: O(1) (considering compilation time is redundant)
    fn function_load<T0: ToRedisArgs>(function_code: T0) {
        cmd("FUNCTION LOAD").arg(function_code)
    }

    /// FUNCTION RESTORE
    /// 
    /// Restore all the functions on the given payload
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: O(N) where N is the number of functions on the payload
    fn function_restore<T0: ToRedisArgs>(serialized_value: T0) {
        cmd("FUNCTION RESTORE").arg(serialized_value)
    }

    /// FUNCTION STATS
    /// 
    /// Return information about the function currently running (name, description, duration)
    /// 
    /// Since: Redis 7.0.0
    /// Group: Scripting
    /// Complexity: O(1)
    fn function_stats<>() {
        cmd("FUNCTION STATS").as_mut()
    }

    /// SCRIPT
    /// 
    /// A container for Lua scripts management commands
    /// 
    /// Since: Redis 2.6.0
    /// Group: Scripting
    /// Complexity: Depends on subcommand.
    fn script<>() {
        cmd("SCRIPT").as_mut()
    }

    /// SCRIPT DEBUG
    /// 
    /// Set the debug mode for executed scripts.
    /// 
    /// Since: Redis 3.2.0
    /// Group: Scripting
    /// Complexity: O(1)
    fn script_debug<>() {
        cmd("SCRIPT DEBUG").as_mut()
    }

    /// SCRIPT EXISTS
    /// 
    /// Check existence of scripts in the script cache.
    /// 
    /// Since: Redis 2.6.0
    /// Group: Scripting
    /// Complexity: O(N) with N being the number of scripts to check (so checking a single script is an O(1) operation).
    fn script_exists<T0: ToRedisArgs>(sha1: T0) {
        cmd("SCRIPT EXISTS").arg(sha1)
    }

    /// SCRIPT FLUSH
    /// 
    /// Remove all the scripts from the script cache.
    /// 
    /// Since: Redis 2.6.0
    /// Group: Scripting
    /// Complexity: O(N) with N being the number of scripts in cache
    fn script_flush<>() {
        cmd("SCRIPT FLUSH").as_mut()
    }

    /// SCRIPT HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 5.0.0
    /// Group: Scripting
    /// Complexity: O(1)
    fn script_help<>() {
        cmd("SCRIPT HELP").as_mut()
    }

    /// SCRIPT KILL
    /// 
    /// Kill the script currently in execution.
    /// 
    /// Since: Redis 2.6.0
    /// Group: Scripting
    /// Complexity: O(1)
    fn script_kill<>() {
        cmd("SCRIPT KILL").as_mut()
    }

    /// SCRIPT LOAD
    /// 
    /// Load the specified Lua script into the script cache.
    /// 
    /// Since: Redis 2.6.0
    /// Group: Scripting
    /// Complexity: O(N) with N being the length in bytes of the script body.
    fn script_load<T0: ToRedisArgs>(script: T0) {
        cmd("SCRIPT LOAD").arg(script)
    }

    /// PFADD
    /// 
    /// Adds the specified elements to the specified HyperLogLog.
    /// 
    /// Since: Redis 2.8.9
    /// Group: Hyperloglog
    /// Complexity: O(1) to add every element.
    fn pfadd<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, element: Option<T0>) {
        cmd("PFADD").arg(key).arg(element)
    }

    /// PFCOUNT
    /// 
    /// Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).
    /// 
    /// Since: Redis 2.8.9
    /// Group: Hyperloglog
    /// Complexity: O(1) with a very small average constant time when called with a single key. O(N) with N being the number of keys, and much bigger constant times, when called with multiple keys.
    fn pfcount<K0: ToRedisArgs>(key: K0) {
        cmd("PFCOUNT").arg(key)
    }

    /// PFDEBUG
    /// 
    /// Internal commands for debugging HyperLogLog values
    /// 
    /// Since: Redis 2.8.9
    /// Group: Hyperloglog
    /// Complexity: N/A
    fn pfdebug<T0: ToRedisArgs, K0: ToRedisArgs>(subcommand: T0, key: K0) {
        cmd("PFDEBUG").arg(subcommand).arg(key)
    }

    /// PFMERGE
    /// 
    /// Merge N different HyperLogLogs into a single one.
    /// 
    /// Since: Redis 2.8.9
    /// Group: Hyperloglog
    /// Complexity: O(N) to merge N HyperLogLogs, but with high constant times.
    fn pfmerge<K0: ToRedisArgs, K1: ToRedisArgs>(destkey: K0, sourcekey: K1) {
        cmd("PFMERGE").arg(destkey).arg(sourcekey)
    }

    /// PFSELFTEST
    /// 
    /// An internal command for testing HyperLogLog values
    /// 
    /// Since: Redis 2.8.9
    /// Group: Hyperloglog
    /// Complexity: N/A
    fn pfselftest<>() {
        cmd("PFSELFTEST").as_mut()
    }

    /// ASKING
    /// 
    /// Sent by cluster clients after an -ASK redirect
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn asking<>() {
        cmd("ASKING").as_mut()
    }

    /// CLUSTER
    /// 
    /// A container for cluster commands
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: Depends on subcommand.
    fn cluster<>() {
        cmd("CLUSTER").as_mut()
    }

    /// CLUSTER ADDSLOTS
    /// 
    /// Assign new hash slots to receiving node
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(N) where N is the total number of hash slot arguments
    fn cluster_addslots<T0: ToRedisArgs>(slot: T0) {
        cmd("CLUSTER ADDSLOTS").arg(slot)
    }

    /// CLUSTER ADDSLOTSRANGE
    /// 
    /// Assign new hash slots to receiving node
    /// 
    /// Since: Redis 7.0.0
    /// Group: Cluster
    /// Complexity: O(N) where N is the total number of the slots between the start slot and end slot arguments.
    fn cluster_addslotsrange<T0: ToRedisArgs>(start_slot_end_slot: T0) {
        cmd("CLUSTER ADDSLOTSRANGE").arg(start_slot_end_slot)
    }

    /// CLUSTER BUMPEPOCH
    /// 
    /// Advance the cluster config epoch
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_bumpepoch<>() {
        cmd("CLUSTER BUMPEPOCH").as_mut()
    }

    /// CLUSTER COUNT-FAILURE-REPORTS
    /// 
    /// Return the number of failure reports active for a given node
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(N) where N is the number of failure reports
    fn cluster_count_failure_reports<T0: ToRedisArgs>(node_id: T0) {
        cmd("CLUSTER COUNT-FAILURE-REPORTS").arg(node_id)
    }

    /// CLUSTER COUNTKEYSINSLOT
    /// 
    /// Return the number of local keys in the specified hash slot
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_countkeysinslot<T0: ToRedisArgs>(slot: T0) {
        cmd("CLUSTER COUNTKEYSINSLOT").arg(slot)
    }

    /// CLUSTER DELSLOTS
    /// 
    /// Set hash slots as unbound in receiving node
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(N) where N is the total number of hash slot arguments
    fn cluster_delslots<T0: ToRedisArgs>(slot: T0) {
        cmd("CLUSTER DELSLOTS").arg(slot)
    }

    /// CLUSTER DELSLOTSRANGE
    /// 
    /// Set hash slots as unbound in receiving node
    /// 
    /// Since: Redis 7.0.0
    /// Group: Cluster
    /// Complexity: O(N) where N is the total number of the slots between the start slot and end slot arguments.
    fn cluster_delslotsrange<T0: ToRedisArgs>(start_slot_end_slot: T0) {
        cmd("CLUSTER DELSLOTSRANGE").arg(start_slot_end_slot)
    }

    /// CLUSTER FAILOVER
    /// 
    /// Forces a replica to perform a manual failover of its master.
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_failover<>() {
        cmd("CLUSTER FAILOVER").as_mut()
    }

    /// CLUSTER FLUSHSLOTS
    /// 
    /// Delete a node's own slots information
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_flushslots<>() {
        cmd("CLUSTER FLUSHSLOTS").as_mut()
    }

    /// CLUSTER FORGET
    /// 
    /// Remove a node from the nodes table
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_forget<T0: ToRedisArgs>(node_id: T0) {
        cmd("CLUSTER FORGET").arg(node_id)
    }

    /// CLUSTER GETKEYSINSLOT
    /// 
    /// Return local key names in the specified hash slot
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(log(N)) where N is the number of requested keys
    fn cluster_getkeysinslot<T0: ToRedisArgs, T1: ToRedisArgs>(slot: T0, count: T1) {
        cmd("CLUSTER GETKEYSINSLOT").arg(slot).arg(count)
    }

    /// CLUSTER HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 5.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_help<>() {
        cmd("CLUSTER HELP").as_mut()
    }

    /// CLUSTER INFO
    /// 
    /// Provides info about Redis Cluster node state
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_info<>() {
        cmd("CLUSTER INFO").as_mut()
    }

    /// CLUSTER KEYSLOT
    /// 
    /// Returns the hash slot of the specified key
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(N) where N is the number of bytes in the key
    fn cluster_keyslot<T0: ToRedisArgs>(key: T0) {
        cmd("CLUSTER KEYSLOT").arg(key)
    }

    /// CLUSTER LINKS
    /// 
    /// Returns a list of all TCP links to and from peer nodes in cluster
    /// 
    /// Since: Redis 7.0.0
    /// Group: Cluster
    /// Complexity: O(N) where N is the total number of Cluster nodes
    fn cluster_links<>() {
        cmd("CLUSTER LINKS").as_mut()
    }

    /// CLUSTER MEET
    /// 
    /// Force a node cluster to handshake with another node
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_meet<T0: ToRedisArgs, T1: ToRedisArgs>(ip: T0, port: T1) {
        cmd("CLUSTER MEET").arg(ip).arg(port)
    }

    /// CLUSTER MYID
    /// 
    /// Return the node id
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_myid<>() {
        cmd("CLUSTER MYID").as_mut()
    }

    /// CLUSTER NODES
    /// 
    /// Get Cluster config for the node
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(N) where N is the total number of Cluster nodes
    fn cluster_nodes<>() {
        cmd("CLUSTER NODES").as_mut()
    }

    /// CLUSTER REPLICAS
    /// 
    /// List replica nodes of the specified master node
    /// 
    /// Since: Redis 5.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_replicas<T0: ToRedisArgs>(node_id: T0) {
        cmd("CLUSTER REPLICAS").arg(node_id)
    }

    /// CLUSTER REPLICATE
    /// 
    /// Reconfigure a node as a replica of the specified master node
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_replicate<T0: ToRedisArgs>(node_id: T0) {
        cmd("CLUSTER REPLICATE").arg(node_id)
    }

    /// CLUSTER RESET
    /// 
    /// Reset a Redis Cluster node
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(N) where N is the number of known nodes. The command may execute a FLUSHALL as a side effect.
    fn cluster_reset<>() {
        cmd("CLUSTER RESET").as_mut()
    }

    /// CLUSTER SAVECONFIG
    /// 
    /// Forces the node to save cluster state on disk
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_saveconfig<>() {
        cmd("CLUSTER SAVECONFIG").as_mut()
    }

    /// CLUSTER SET-CONFIG-EPOCH
    /// 
    /// Set the configuration epoch in a new node
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_set_config_epoch<T0: ToRedisArgs>(config_epoch: T0) {
        cmd("CLUSTER SET-CONFIG-EPOCH").arg(config_epoch)
    }

    /// CLUSTER SETSLOT
    /// 
    /// Bind a hash slot to a specific node
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn cluster_setslot<T0: ToRedisArgs>(slot: T0) {
        cmd("CLUSTER SETSLOT").arg(slot)
    }

    /// CLUSTER SHARDS
    /// 
    /// Get array of cluster slots to node mappings
    /// 
    /// Since: Redis 7.0.0
    /// Group: Cluster
    /// Complexity: O(N) where N is the total number of cluster nodes
    fn cluster_shards<>() {
        cmd("CLUSTER SHARDS").as_mut()
    }

    /// CLUSTER SLAVES
    /// 
    /// List replica nodes of the specified master node
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Replaced By: `CLUSTER REPLICAS`
    /// Complexity: O(1)
    /// Replaced By: `CLUSTER REPLICAS`
    #[deprecated]    fn cluster_slaves<T0: ToRedisArgs>(node_id: T0) {
        cmd("CLUSTER SLAVES").arg(node_id)
    }

    /// CLUSTER SLOTS
    /// 
    /// Get array of Cluster slot to node mappings
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Replaced By: `CLUSTER SHARDS`
    /// Complexity: O(N) where N is the total number of Cluster nodes
    /// Replaced By: `CLUSTER SHARDS`
    #[deprecated]    fn cluster_slots<>() {
        cmd("CLUSTER SLOTS").as_mut()
    }

    /// READONLY
    /// 
    /// Enables read queries for a connection to a cluster replica node
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn readonly<>() {
        cmd("READONLY").as_mut()
    }

    /// READWRITE
    /// 
    /// Disables read queries for a connection to a cluster replica node
    /// 
    /// Since: Redis 3.0.0
    /// Group: Cluster
    /// Complexity: O(1)
    fn readwrite<>() {
        cmd("READWRITE").as_mut()
    }

    /// GEOADD
    /// 
    /// Add one or more geospatial items in the geospatial index represented using a sorted set
    /// 
    /// Since: Redis 3.2.0
    /// Group: Geo
    /// Complexity: O(log(N)) for each item added, where N is the number of elements in the sorted set.
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geoadd<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, longitude_latitude_member: T0) {
        cmd("GEOADD").arg(key).arg(longitude_latitude_member)
    }

    /// GEODIST
    /// 
    /// Returns the distance between two members of a geospatial index
    /// 
    /// Since: Redis 3.2.0
    /// Group: Geo
    /// Complexity: O(log(N))
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geodist<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, member1: T0, member2: T1) {
        cmd("GEODIST").arg(key).arg(member1).arg(member2)
    }

    /// GEOHASH
    /// 
    /// Returns members of a geospatial index as standard geohash strings
    /// 
    /// Since: Redis 3.2.0
    /// Group: Geo
    /// Complexity: O(log(N)) for each member requested, where N is the number of elements in the sorted set.
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geohash<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, member: T0) {
        cmd("GEOHASH").arg(key).arg(member)
    }

    /// GEOPOS
    /// 
    /// Returns longitude and latitude of members of a geospatial index
    /// 
    /// Since: Redis 3.2.0
    /// Group: Geo
    /// Complexity: O(N) where N is the number of members requested.
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geopos<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, member: T0) {
        cmd("GEOPOS").arg(key).arg(member)
    }

    /// GEORADIUS
    /// 
    /// Query a sorted set representing a geospatial index to fetch members matching a given maximum distance from a point
    /// 
    /// Since: Redis 3.2.0
    /// Group: Geo
    /// Replaced By: `GEOSEARCH` and `GEOSEARCHSTORE` with the `BYRADIUS` argument
    /// Complexity: O(N+log(M)) where N is the number of elements inside the bounding box of the circular area delimited by center and radius and M is the number of items inside the index.
    /// Replaced By: `GEOSEARCH` and `GEOSEARCHSTORE` with the `BYRADIUS` argument
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    #[deprecated]    fn georadius<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs, T2: ToRedisArgs, T3: ToRedisArgs>(key: K0, longitude: T0, latitude: T1, radius: T2, count: Option<T3>) {
        cmd("GEORADIUS").arg(key).arg(longitude).arg(latitude).arg(radius).arg(count)
    }

    /// GEORADIUSBYMEMBER
    /// 
    /// Query a sorted set representing a geospatial index to fetch members matching a given maximum distance from a member
    /// 
    /// Since: Redis 3.2.0
    /// Group: Geo
    /// Replaced By: `GEOSEARCH` and `GEOSEARCHSTORE` with the `BYRADIUS` and `FROMMEMBER` arguments
    /// Complexity: O(N+log(M)) where N is the number of elements inside the bounding box of the circular area delimited by center and radius and M is the number of items inside the index.
    /// Replaced By: `GEOSEARCH` and `GEOSEARCHSTORE` with the `BYRADIUS` and `FROMMEMBER` arguments
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    #[deprecated]    fn georadiusbymember<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs, T2: ToRedisArgs>(key: K0, member: T0, radius: T1, count: Option<T2>) {
        cmd("GEORADIUSBYMEMBER").arg(key).arg(member).arg(radius).arg(count)
    }

    /// GEORADIUSBYMEMBER_RO
    /// 
    /// A read-only variant for GEORADIUSBYMEMBER
    /// 
    /// Since: Redis 3.2.10
    /// Group: Geo
    /// Replaced By: `GEOSEARCH` with the `BYRADIUS` and `FROMMEMBER` arguments
    /// Complexity: O(N+log(M)) where N is the number of elements inside the bounding box of the circular area delimited by center and radius and M is the number of items inside the index.
    /// Replaced By: `GEOSEARCH` with the `BYRADIUS` and `FROMMEMBER` arguments
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    #[deprecated]    fn georadiusbymember_ro<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs, T2: ToRedisArgs>(key: K0, member: T0, radius: T1, count: Option<T2>) {
        cmd("GEORADIUSBYMEMBER_RO").arg(key).arg(member).arg(radius).arg(count)
    }

    /// GEORADIUS_RO
    /// 
    /// A read-only variant for GEORADIUS
    /// 
    /// Since: Redis 3.2.10
    /// Group: Geo
    /// Replaced By: `GEOSEARCH` with the `BYRADIUS` argument
    /// Complexity: O(N+log(M)) where N is the number of elements inside the bounding box of the circular area delimited by center and radius and M is the number of items inside the index.
    /// Replaced By: `GEOSEARCH` with the `BYRADIUS` argument
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    #[deprecated]    fn georadius_ro<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs, T2: ToRedisArgs, T3: ToRedisArgs>(key: K0, longitude: T0, latitude: T1, radius: T2, count: Option<T3>) {
        cmd("GEORADIUS_RO").arg(key).arg(longitude).arg(latitude).arg(radius).arg(count)
    }

    /// GEOSEARCH
    /// 
    /// Query a sorted set representing a geospatial index to fetch members inside an area of a box or a circle.
    /// 
    /// Since: Redis 6.2.0
    /// Group: Geo
    /// Complexity: O(N+log(M)) where N is the number of elements in the grid-aligned bounding box area around the shape provided as the filter and M is the number of items inside the shape
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geosearch<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, count: Option<T0>) {
        cmd("GEOSEARCH").arg(key).arg(count)
    }

    /// GEOSEARCHSTORE
    /// 
    /// Query a sorted set representing a geospatial index to fetch members inside an area of a box or a circle, and store the result in another key.
    /// 
    /// Since: Redis 6.2.0
    /// Group: Geo
    /// Complexity: O(N+log(M)) where N is the number of elements in the grid-aligned bounding box area around the shape provided as the filter and M is the number of items inside the shape
    #[cfg(feature = "geospatial")]
    #[cfg_attr(docsrs, doc(cfg(feature = "geospatial")))]
    fn geosearchstore<K0: ToRedisArgs, K1: ToRedisArgs, T0: ToRedisArgs>(destination: K0, source: K1, count: Option<T0>) {
        cmd("GEOSEARCHSTORE").arg(destination).arg(source).arg(count)
    }

    /// XACK
    /// 
    /// Marks a pending message as correctly processed, effectively removing it from the pending entries list of the consumer group. Return value of the command is the number of messages successfully acknowledged, that is, the IDs we were actually able to resolve in the PEL.
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1) for each message ID processed.
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xack<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, group: T0, id: T1) {
        cmd("XACK").arg(key).arg(group).arg(id)
    }

    /// XADD
    /// 
    /// Appends a new entry to a stream
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1) when adding a new entry, O(N) when trimming where N being the number of entries evicted.
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xadd<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, trim: Option<T0>, field_value: T1) {
        cmd("XADD").arg(key).arg(trim).arg(field_value)
    }

    /// XAUTOCLAIM
    /// 
    /// Changes (or acquires) ownership of messages in a consumer group, as if the messages were delivered to the specified consumer.
    /// 
    /// Since: Redis 6.2.0
    /// Group: Stream
    /// Complexity: O(1) if COUNT is small.
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xautoclaim<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs, T2: ToRedisArgs, T3: ToRedisArgs>(key: K0, group: T0, consumer: T1, min_idle_time: T2, start: T3) {
        cmd("XAUTOCLAIM").arg(key).arg(group).arg(consumer).arg(min_idle_time).arg(start)
    }

    /// XCLAIM
    /// 
    /// Changes (or acquires) ownership of a message in a consumer group, as if the message was delivered to the specified consumer.
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(log N) with N being the number of messages in the PEL of the consumer group.
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xclaim<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs, T2: ToRedisArgs, T3: ToRedisArgs>(key: K0, group: T0, consumer: T1, min_idle_time: T2, id: T3) {
        cmd("XCLAIM").arg(key).arg(group).arg(consumer).arg(min_idle_time).arg(id)
    }

    /// XDEL
    /// 
    /// Removes the specified entries from the stream. Returns the number of items actually deleted, that may be different from the number of IDs passed in case certain IDs do not exist.
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1) for each single item to delete in the stream, regardless of the stream size.
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xdel<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, id: T0) {
        cmd("XDEL").arg(key).arg(id)
    }

    /// XGROUP
    /// 
    /// A container for consumer groups commands
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: Depends on subcommand.
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup<>() {
        cmd("XGROUP").as_mut()
    }

    /// XGROUP CREATE
    /// 
    /// Create a consumer group.
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup_create<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, groupname: T0) {
        cmd("XGROUP CREATE").arg(key).arg(groupname)
    }

    /// XGROUP CREATECONSUMER
    /// 
    /// Create a consumer in a consumer group.
    /// 
    /// Since: Redis 6.2.0
    /// Group: Stream
    /// Complexity: O(1)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup_createconsumer<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, groupname: T0, consumername: T1) {
        cmd("XGROUP CREATECONSUMER").arg(key).arg(groupname).arg(consumername)
    }

    /// XGROUP DELCONSUMER
    /// 
    /// Delete a consumer from a consumer group.
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup_delconsumer<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, groupname: T0, consumername: T1) {
        cmd("XGROUP DELCONSUMER").arg(key).arg(groupname).arg(consumername)
    }

    /// XGROUP DESTROY
    /// 
    /// Destroy a consumer group.
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(N) where N is the number of entries in the group's pending entries list (PEL).
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup_destroy<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, groupname: T0) {
        cmd("XGROUP DESTROY").arg(key).arg(groupname)
    }

    /// XGROUP HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup_help<>() {
        cmd("XGROUP HELP").as_mut()
    }

    /// XGROUP SETID
    /// 
    /// Set a consumer group to an arbitrary last delivered ID value.
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xgroup_setid<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, groupname: T0) {
        cmd("XGROUP SETID").arg(key).arg(groupname)
    }

    /// XINFO
    /// 
    /// A container for stream introspection commands
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: Depends on subcommand.
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xinfo<>() {
        cmd("XINFO").as_mut()
    }

    /// XINFO CONSUMERS
    /// 
    /// List the consumers in a consumer group
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xinfo_consumers<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, groupname: T0) {
        cmd("XINFO CONSUMERS").arg(key).arg(groupname)
    }

    /// XINFO GROUPS
    /// 
    /// List the consumer groups of a stream
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xinfo_groups<K0: ToRedisArgs>(key: K0) {
        cmd("XINFO GROUPS").arg(key)
    }

    /// XINFO HELP
    /// 
    /// Show helpful text about the different subcommands
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xinfo_help<>() {
        cmd("XINFO HELP").as_mut()
    }

    /// XINFO STREAM
    /// 
    /// Get information about a stream
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xinfo_stream<K0: ToRedisArgs>(key: K0) {
        cmd("XINFO STREAM").arg(key)
    }

    /// XLEN
    /// 
    /// Return the number of entries in a stream
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xlen<K0: ToRedisArgs>(key: K0) {
        cmd("XLEN").arg(key)
    }

    /// XPENDING
    /// 
    /// Return information and entries from a stream consumer group pending entries list, that are messages fetched but never acknowledged.
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(N) with N being the number of elements returned, so asking for a small fixed number of entries per call is O(1). O(M), where M is the total number of entries scanned when used with the IDLE filter. When the command returns just the summary and the list of consumers is small, it runs in O(1) time; otherwise, an additional O(N) time for iterating every consumer.
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xpending<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, group: T0, filters: Option<T1>) {
        cmd("XPENDING").arg(key).arg(group).arg(filters)
    }

    /// XRANGE
    /// 
    /// Return a range of elements in a stream, with IDs matching the specified IDs interval
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(N) with N being the number of elements being returned. If N is constant (e.g. always asking for the first 10 elements with COUNT), you can consider it O(1).
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xrange<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, start: T0, end: T1) {
        cmd("XRANGE").arg(key).arg(start).arg(end)
    }

    /// XREAD
    /// 
    /// Return never seen elements in multiple streams, with IDs greater than the ones reported by the caller for each stream. Can block.
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: For each stream mentioned: O(N) with N being the number of elements being returned, it means that XREAD-ing with a fixed COUNT is O(1). Note that when the BLOCK option is used, XADD will pay O(M) time in order to serve the M clients blocked on the stream getting new data.
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xread<>() {
        cmd("XREAD").as_mut()
    }

    /// XREADGROUP
    /// 
    /// Return new entries from a stream using a consumer group, or access the history of the pending entries for a given consumer. Can block.
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: For each stream mentioned: O(M) with M being the number of elements returned. If M is constant (e.g. always asking for the first 10 elements with COUNT), you can consider it O(1). On the other side when XREADGROUP blocks, XADD will pay the O(N) time in order to serve the N clients blocked on the stream getting new data.
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xreadgroup<>() {
        cmd("XREADGROUP").as_mut()
    }

    /// XREVRANGE
    /// 
    /// Return a range of elements in a stream, with IDs matching the specified IDs interval, in reverse order (from greater to smaller IDs) compared to XRANGE
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(N) with N being the number of elements returned. If N is constant (e.g. always asking for the first 10 elements with COUNT), you can consider it O(1).
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xrevrange<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, end: T0, start: T1) {
        cmd("XREVRANGE").arg(key).arg(end).arg(start)
    }

    /// XSETID
    /// 
    /// An internal command for replicating stream values
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(1)
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xsetid<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, last_id: T0) {
        cmd("XSETID").arg(key).arg(last_id)
    }

    /// XTRIM
    /// 
    /// Trims the stream to (approximately if '~' is passed) a certain size
    /// 
    /// Since: Redis 5.0.0
    /// Group: Stream
    /// Complexity: O(N), with N being the number of evicted entries. Constant times are very small however, since entries are organized in macro nodes containing multiple entries that can be released with a single deallocation.
    #[cfg(feature = "streams")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streams")))]
    fn xtrim<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, trim: T0) {
        cmd("XTRIM").arg(key).arg(trim)
    }

    /// BITCOUNT
    /// 
    /// Count set bits in a string
    /// 
    /// Since: Redis 2.6.0
    /// Group: Bitmap
    /// Complexity: O(N)
    fn bitcount<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, index: Option<T0>) {
        cmd("BITCOUNT").arg(key).arg(index)
    }

    /// BITFIELD
    /// 
    /// Perform arbitrary bitfield integer operations on strings
    /// 
    /// Since: Redis 3.2.0
    /// Group: Bitmap
    /// Complexity: O(1) for each subcommand specified
    fn bitfield<K0: ToRedisArgs>(key: K0) {
        cmd("BITFIELD").arg(key)
    }

    /// BITFIELD_RO
    /// 
    /// Perform arbitrary bitfield integer operations on strings. Read-only variant of BITFIELD
    /// 
    /// Since: Redis 6.2.0
    /// Group: Bitmap
    /// Complexity: O(1) for each subcommand specified
    fn bitfield_ro<K0: ToRedisArgs>(key: K0) {
        cmd("BITFIELD_RO").arg(key)
    }

    /// BITOP
    /// 
    /// Perform bitwise operations between strings
    /// 
    /// Since: Redis 2.6.0
    /// Group: Bitmap
    /// Complexity: O(N)
    fn bitop<T0: ToRedisArgs, K0: ToRedisArgs, K1: ToRedisArgs>(operation: T0, destkey: K0, key: K1) {
        cmd("BITOP").arg(operation).arg(destkey).arg(key)
    }

    /// BITPOS
    /// 
    /// Find first bit set or clear in a string
    /// 
    /// Since: Redis 2.8.7
    /// Group: Bitmap
    /// Complexity: O(N)
    fn bitpos<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, bit: T0, index: Option<T1>) {
        cmd("BITPOS").arg(key).arg(bit).arg(index)
    }

    /// GETBIT
    /// 
    /// Returns the bit value at offset in the string value stored at key
    /// 
    /// Since: Redis 2.2.0
    /// Group: Bitmap
    /// Complexity: O(1)
    fn getbit<K0: ToRedisArgs, T0: ToRedisArgs>(key: K0, offset: T0) {
        cmd("GETBIT").arg(key).arg(offset)
    }

    /// SETBIT
    /// 
    /// Sets or clears the bit at offset in the string value stored at key
    /// 
    /// Since: Redis 2.2.0
    /// Group: Bitmap
    /// Complexity: O(1)
    fn setbit<K0: ToRedisArgs, T0: ToRedisArgs, T1: ToRedisArgs>(key: K0, offset: T0, value: T1) {
        cmd("SETBIT").arg(key).arg(offset).arg(value)
    }

}
