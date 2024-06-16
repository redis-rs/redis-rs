use crate::aio::ConnectionLike;
use crate::cluster_async::{ClusterConnInner, Connect, Core, InnerCore};
use crate::cluster_routing::SlotAddr;
use crate::cluster_topology::SLOT_SIZE;
use crate::{cmd, from_redis_value, Cmd, ErrorKind, RedisError, RedisResult, Value};
use async_trait::async_trait;
use std::sync::Arc;

/// This module contains the implementation of scanning operations in a Redis cluster.
///
/// The `ClusterScanArgs` struct represents the arguments for a cluster scan operation,
/// including the scan state reference, match pattern, count, and object type.
///
/// The `ScanStateCursor` struct is a wrapper for managing the state of a scan operation in a cluster.
/// It holds a reference to the scan state and provides methods for accessing the state.
///
/// The `ClusterInScan` trait defines the methods for interacting with a Redis cluster during scanning,
/// including retrieving address information, refreshing slot mapping, and routing commands to specific addresss.
///
/// The `ScanState` struct represents the state of a scan operation in a Redis cluster.
/// It holds information about the current scan state, including the cursor position, scanned slots map,
/// address being scanned, and address's epoch.

const BITS_PER_U64: usize = u64::BITS as usize;
const NUM_OF_SLOTS: usize = SLOT_SIZE as usize;
const BITS_ARRAY_SIZE: usize = NUM_OF_SLOTS / BITS_PER_U64;
const END_OF_SCAN: u16 = NUM_OF_SLOTS as u16 + 1;
type SlotsBitsArray = [u64; BITS_ARRAY_SIZE];

#[derive(PartialEq, Debug, Clone)]
pub(crate) struct ScanState {
    // the real cursor in the scan operation
    cursor: u64,
    // a map of the slots that have been scanned
    scanned_slots_map: SlotsBitsArray,
    // the address that is being scanned currently, based on the next slot set to 0 in the scanned_slots_map, and the address "own" the slot
    // in the SlotMap
    pub(crate) address_in_scan: String,
    // epoch represent the version of the address, when a failover happens or slots migrate in the epoch will be updated to +1
    address_epoch: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ClusterScanArgs {
    pub(crate) scan_state_cursor: ScanStateCursor,
    match_pattern: Option<String>,
    count: Option<usize>,
    object_type: Option<ObjectType>,
}

#[derive(Debug, Clone)]
/// Represents the type of an object in Redis.
pub enum ObjectType {
    /// Represents a string object in Redis.
    STRING,
    /// Represents a list object in Redis.
    LIST,
    /// Represents a set object in Redis.
    SET,
    /// Represents a sorted set object in Redis.
    ZSET,
    /// Represents a hash object in Redis.
    HASH,
}

impl std::fmt::Display for ObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ObjectType::STRING => write!(f, "string"),
            ObjectType::LIST => write!(f, "list"),
            ObjectType::SET => write!(f, "set"),
            ObjectType::ZSET => write!(f, "zset"),
            ObjectType::HASH => write!(f, "hash"),
        }
    }
}

impl ClusterScanArgs {
    pub(crate) fn new(
        scan_state_cursor: ScanStateCursor,
        match_pattern: Option<String>,
        count: Option<usize>,
        object_type: Option<ObjectType>,
    ) -> Self {
        Self {
            scan_state_cursor,
            match_pattern,
            count,
            object_type,
        }
    }
}

#[derive(Debug, Clone, Default)]
/// A wrapper struct for managing the state of a scan operation in a cluster.
pub struct ScanStateCursor {
    scan_state_ref: Arc<Option<ScanState>>,
}

impl ScanStateCursor {
    /// Creates a new instance of `ScanStateCursor` from a given `ScanState`.
    fn from_scan_state(scan_state: &ScanState) -> Self {
        Self {
            scan_state_ref: Arc::new(Some(scan_state.clone())),
        }
    }

    /// Creates a new instance of `ScanStateCursor`.
    ///
    /// This method initializes the `ScanStateCursor` with a reference to a `ScanState` that is initially set to `None`.
    /// An empty ScanState is equivilant to a 0 cursor.
    pub fn new() -> Self {
        Self {
            scan_state_ref: Arc::new(None),
        }
    }

    /// Returns `true` if the scan state is empty, indicating that the scan operation has finished or has not started.
    pub fn is_none(&self) -> bool {
        self.scan_state_ref.is_none()
    }

    /// Returns a clone of the scan state, if it exist.
    pub(crate) fn get_state_from_wrraper(&self) -> Option<ScanState> {
        if self.is_none() {
            None
        } else {
            self.scan_state_ref.as_ref().clone()
        }
    }
}

/// This trait defines the methods for interacting with a Redis cluster during scanning.
#[async_trait]
pub(crate) trait ClusterInScan {
    /// Retrieves the address address associated with a given slot in the cluster.
    async fn get_address_by_slot(&self, slot: u16) -> RedisResult<String>;

    /// Retrieves the epoch of a given address in the cluster.
    async fn get_address_epoch(&self, address: &str) -> Result<u64, RedisError>;

    /// Retrieves the slots assigned to a given address in the cluster.
    async fn get_slots_of_address(&self, address: &str) -> Vec<u16>;

    /// Refreshes the slot mapping of the cluster.
    async fn refresh_slots(&self) -> RedisResult<()>;

    /// Routes a Redis command to a specific address in the cluster.
    async fn route_command(&self, cmd: &Cmd, address: &str) -> RedisResult<Value>;

    /// Chaeck if all slots are covered by the cluster
    async fn is_all_slots_covered(&self) -> bool;
}

/// Represents the state of a scan operation in a Redis cluster.
///
/// This struct holds information about the current scan state, including the cursor position,
/// the scanned slots map, the address being scanned, and the address's epoch.
impl ScanState {
    /// Create a new instance of ScanState.
    ///
    /// # Arguments
    ///
    /// * `cursor` - The cursor position.
    /// * `scanned_slots_map` - The scanned slots map.
    /// * `address_in_scan` - The address being scanned.
    /// * `address_epoch` - The epoch of the address being scanned.
    ///
    /// # Returns
    ///
    /// A new instance of ScanState.
    pub fn create(
        cursor: u64,
        scanned_slots_map: SlotsBitsArray,
        address_in_scan: String,
        address_epoch: u64,
    ) -> Self {
        Self {
            cursor,
            scanned_slots_map,
            address_in_scan,
            address_epoch,
        }
    }

    /// Initiate a scan operation by creating a new ScanState.
    ///
    /// This method creates a new ScanState with the initial values for a scan operation.
    ///
    /// # Arguments
    ///
    /// * `connection` - The connection to the Redis cluster.
    ///
    /// # Returns
    ///
    /// The initial ScanState.
    pub(crate) async fn initiate_scan<C: ClusterInScan + ?Sized>(
        connection: &C,
    ) -> RedisResult<ScanState> {
        let new_scanned_slots_map: SlotsBitsArray = [0; BITS_ARRAY_SIZE];
        let new_cursor = 0;
        let address_in_scan = connection.get_address_by_slot(0).await;
        match address_in_scan {
            Ok(address) => {
                let address_epoch = connection.get_address_epoch(&address).await.unwrap_or(0);
                Ok(ScanState::create(
                    new_cursor,
                    new_scanned_slots_map,
                    address,
                    address_epoch,
                ))
            }
            Err(err) => Err(err),
        }
    }

    /// Get the next slot that hasn't been scanned.
    ///
    /// This method iterates over the scanned slots map and finds the next slot that is set to 0.
    ///
    /// # Arguments
    ///
    /// * `scanned_slots_map` - The scanned slots map.
    ///
    /// # Returns
    ///
    /// The next slot that hasn't been scanned, or `None` if all slots have been scanned.
    pub fn get_next_slot(&self, scanned_slots_map: &SlotsBitsArray) -> Option<u16> {
        let all_slots_scanned = scanned_slots_map.iter().all(|&word| word == u64::MAX);
        if all_slots_scanned {
            return Some(END_OF_SCAN);
        }
        for (i, slot) in scanned_slots_map.iter().enumerate() {
            let mut mask = 1;
            for j in 0..BITS_PER_U64 {
                if (slot & mask) == 0 {
                    return Some((i * BITS_PER_U64 + j) as u16);
                }
                mask <<= 1;
            }
        }
        None
    }

    /// Update the next address to be scanned without updating the scanned slots map.
    /// This method is used when the address epoch has changed and we can't know which slots are new or when slots are missing.
    /// In this case we will skip updating the scanned_slots_map and will just update the address and the cursor.
    ///
    /// # Arguments
    ///
    /// * `connection` - The connection to the Redis cluster.
    ///
    /// # Returns
    ///
    /// The updated ScanState.
    pub(crate) async fn update_scan_state_without_updating_scanned_map<
        C: ClusterInScan + ?Sized,
    >(
        &self,
        connection: &C,
    ) -> RedisResult<ScanState> {
        let next_slot = self.get_next_slot(&self.scanned_slots_map).unwrap();
        let new_address = if next_slot == END_OF_SCAN {
            Ok("".to_string())
        } else {
            connection.get_address_by_slot(next_slot).await
        };
        match new_address {
            Ok(address) => {
                let new_epoch = connection.get_address_epoch(&address).await.unwrap_or(0);
                let new_cursor = 0;
                Ok(ScanState::create(
                    new_cursor,
                    self.scanned_slots_map,
                    address,
                    new_epoch,
                ))
            }
            Err(err) => Err(err),
        }
    }

    /// Update the next address to be scanned.
    ///
    /// This method updates the scan state by updating the scanned slots map, determining the next slot to be scanned,
    /// retrieving the address for that slot and the epoch of that address, and creating a new ScanState.
    ///
    /// # Arguments
    ///
    /// * `connection` - The connection to the Redis cluster.
    ///
    /// # Returns
    ///
    /// The updated ScanState.
    async fn update_scan_state_and_get_next_address<C: ClusterInScan + ?Sized>(
        &mut self,
        connection: &C,
    ) -> RedisResult<ScanState> {
        let _ = connection.refresh_slots().await;
        let mut scanned_slots_map = self.scanned_slots_map;
        // If the address epoch changed it mean that some slots in the address are new, so we cant know which slots been there from the begining and which are new, or out and in later.
        // In this case we will skip updating the scanned_slots_map and will just update the address and the cursor
        let new_address_epoch = connection
            .get_address_epoch(&self.address_in_scan)
            .await
            .unwrap_or(0);
        if new_address_epoch != self.address_epoch {
            return self
                .update_scan_state_without_updating_scanned_map(connection)
                .await;
        }
        // If epoch wasn't changed, the slots owned by the address after the refresh are all valid as slots that been scanned
        // So we will update the scanned_slots_map with the slots owned by the address
        let slots_scanned = connection.get_slots_of_address(&self.address_in_scan).await;
        for slot in slots_scanned {
            let slot_index = slot as usize / BITS_PER_U64;
            let slot_bit = slot as usize % BITS_PER_U64;
            scanned_slots_map[slot_index] |= 1 << slot_bit;
        }
        // Get the next address to scan and its param base on the next slot set to 0 in the scanned_slots_map
        let next_slot = self.get_next_slot(&scanned_slots_map).unwrap();
        let new_address = if next_slot == END_OF_SCAN {
            Ok("".to_string())
        } else {
            connection.get_address_by_slot(next_slot).await
        };
        match new_address {
            Ok(new_address) => {
                let new_epoch = connection
                    .get_address_epoch(&new_address)
                    .await
                    .unwrap_or(0);
                let new_cursor = 0;
                Ok(ScanState::create(
                    new_cursor,
                    scanned_slots_map,
                    new_address,
                    new_epoch,
                ))
            }
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_creation_of_empty_scan_wrapper() {
        let scan_state_wrapper = ScanStateCursor::new();
        assert!(scan_state_wrapper.is_none());
    }

    #[test]
    fn test_creation_of_scan_state_wrapper_from() {
        let scan_state = ScanState {
            cursor: 0,
            scanned_slots_map: [0; BITS_ARRAY_SIZE],
            address_in_scan: String::from("address1"),
            address_epoch: 1,
        };

        let scan_state_wrapper = ScanStateCursor::from_scan_state(&scan_state);
        assert!(!scan_state_wrapper.is_none());
    }

    #[test]
    // Test the get_next_slot method
    fn test_scan_state_get_next_slot() {
        let scanned_slots_map: SlotsBitsArray = [0; BITS_ARRAY_SIZE];
        let scan_state = ScanState {
            cursor: 0,
            scanned_slots_map,
            address_in_scan: String::from("address1"),
            address_epoch: 1,
        };
        let next_slot = scan_state.get_next_slot(&scanned_slots_map);
        assert_eq!(next_slot, Some(0));
        // Set the first slot to 1
        let mut scanned_slots_map: SlotsBitsArray = [0; BITS_ARRAY_SIZE];
        scanned_slots_map[0] = 1;
        let scan_state = ScanState {
            cursor: 0,
            scanned_slots_map,
            address_in_scan: String::from("address1"),
            address_epoch: 1,
        };
        let next_slot = scan_state.get_next_slot(&scanned_slots_map);
        assert_eq!(next_slot, Some(1));
    }
    // Create a mock connection
    struct MockConnection;
    #[async_trait]
    impl ClusterInScan for MockConnection {
        async fn get_address_by_slot(&self, _slot: u16) -> RedisResult<String> {
            Ok("mock_address".to_string())
        }
        async fn get_address_epoch(&self, _address: &str) -> Result<u64, RedisError> {
            Ok(0)
        }
        async fn get_slots_of_address(&self, address: &str) -> Vec<u16> {
            if address == "mock_address" {
                vec![3, 4, 5]
            } else {
                vec![0, 1, 2]
            }
        }
        async fn refresh_slots(&self) -> RedisResult<()> {
            Ok(())
        }
        async fn route_command(&self, _: &Cmd, _: &str) -> RedisResult<Value> {
            unimplemented!()
        }
        async fn is_all_slots_covered(&self) -> bool {
            true
        }
    }
    // Test the initiate_scan function
    #[tokio::test]
    async fn test_initiate_scan() {
        let connection = MockConnection;
        let scan_state = ScanState::initiate_scan(&connection).await.unwrap();

        // Assert that the scan state is initialized correctly
        assert_eq!(scan_state.cursor, 0);
        assert_eq!(scan_state.scanned_slots_map, [0; BITS_ARRAY_SIZE]);
        assert_eq!(scan_state.address_in_scan, "mock_address");
        assert_eq!(scan_state.address_epoch, 0);
    }

    // Test the get_next_slot function
    #[test]
    fn test_get_next_slot() {
        let scan_state = ScanState {
            cursor: 0,
            scanned_slots_map: [0; BITS_ARRAY_SIZE],
            address_in_scan: "".to_string(),
            address_epoch: 0,
        };
        // Test when all first bits of each u6 are set to 1, the next slots should be 1
        let scanned_slots_map: SlotsBitsArray = [1; BITS_ARRAY_SIZE];
        let next_slot = scan_state.get_next_slot(&scanned_slots_map);
        assert_eq!(next_slot, Some(1));

        // Test when all slots are scanned, the next slot should be 0
        let scanned_slots_map: SlotsBitsArray = [u64::MAX; BITS_ARRAY_SIZE];
        let next_slot = scan_state.get_next_slot(&scanned_slots_map);
        assert_eq!(next_slot, Some(16385));

        // Test when first, second, fourth, sixth and eighth slots scanned, the next slot should be 2
        let mut scanned_slots_map: SlotsBitsArray = [0; BITS_ARRAY_SIZE];
        scanned_slots_map[0] = 171; // 10101011
        let next_slot = scan_state.get_next_slot(&scanned_slots_map);
        assert_eq!(next_slot, Some(2));
    }

    // Test the update_scan_state_and_get_next_address function
    #[tokio::test]
    async fn test_update_scan_state_and_get_next_address() {
        let connection = MockConnection;
        let scan_state = ScanState::initiate_scan(&connection).await;
        let updated_scan_state = scan_state
            .unwrap()
            .update_scan_state_and_get_next_address(&connection)
            .await
            .unwrap();

        // cursor should be reset to 0
        assert_eq!(updated_scan_state.cursor, 0);

        // address_in_scan should be updated to the new address
        assert_eq!(updated_scan_state.address_in_scan, "mock_address");

        // address_epoch should be updated to the new address epoch
        assert_eq!(updated_scan_state.address_epoch, 0);
    }

    #[tokio::test]
    async fn test_update_scan_state_without_updating_scanned_map() {
        let connection = MockConnection;
        let scan_state = ScanState::create(0, [0; BITS_ARRAY_SIZE], "address".to_string(), 0);
        let scanned_slots_map = scan_state.scanned_slots_map;
        let updated_scan_state = scan_state
            .update_scan_state_without_updating_scanned_map(&connection)
            .await
            .unwrap();
        assert_eq!(updated_scan_state.scanned_slots_map, scanned_slots_map);
        assert_eq!(updated_scan_state.cursor, 0);
        assert_eq!(updated_scan_state.address_in_scan, "mock_address");
        assert_eq!(updated_scan_state.address_epoch, 0);
    }
}
