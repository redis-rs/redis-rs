use crate::aio::ConnectionLike;
use crate::cluster_async::{
    ClusterConnInner, Connect, Core, InternalRoutingInfo, InternalSingleNodeRouting, RefreshPolicy,
    Response,
};
use crate::cluster_routing::SlotAddr;
use crate::cluster_topology::SLOT_SIZE;
use crate::{cmd, from_redis_value, Cmd, ErrorKind, RedisError, RedisResult, Value};
use async_trait::async_trait;
use std::sync::Arc;
use strum_macros::Display;

/// This module contains the implementation of scanning operations in a Redis cluster.
///
/// The [`ClusterScanArgs`] struct represents the arguments for a cluster scan operation,
/// including the scan state reference, match pattern, count, and object type.
///
/// The [[`ScanStateRC`]] struct is a wrapper for managing the state of a scan operation in a cluster.
/// It holds a reference to the scan state and provides methods for accessing the state.
///
/// The [[`ClusterInScan`]] trait defines the methods for interacting with a Redis cluster during scanning,
/// including retrieving address information, refreshing slot mapping, and routing commands to specific address.
///
/// The [[`ScanState`]] struct represents the state of a scan operation in a Redis cluster.
/// It holds information about the current scan state, including the cursor position, scanned slots map,
/// address being scanned, and address's epoch.

const BITS_PER_U64: usize = u64::BITS as usize;
const NUM_OF_SLOTS: usize = SLOT_SIZE as usize;
const BITS_ARRAY_SIZE: usize = NUM_OF_SLOTS / BITS_PER_U64;
const END_OF_SCAN: u16 = NUM_OF_SLOTS as u16 + 1;
type SlotsBitsArray = [u64; BITS_ARRAY_SIZE];

#[derive(Debug, Clone)]
pub(crate) struct ClusterScanArgs {
    pub(crate) scan_state_cursor: ScanStateRC,
    match_pattern: Option<String>,
    count: Option<usize>,
    object_type: Option<ObjectType>,
}

#[derive(Debug, Clone, Display)]
/// Represents the type of an object in Redis.
pub enum ObjectType {
    /// Represents a string object in Redis.
    String,
    /// Represents a list object in Redis.
    List,
    /// Represents a set object in Redis.
    Set,
    /// Represents a sorted set object in Redis.
    ZSet,
    /// Represents a hash object in Redis.
    Hash,
}

impl ClusterScanArgs {
    pub(crate) fn new(
        scan_state_cursor: ScanStateRC,
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

#[derive(PartialEq, Debug, Clone, Default)]
pub enum ScanStateStage {
    #[default]
    Initiating,
    InProgress,
    Finished,
}

#[derive(Debug, Clone, Default)]
/// A wrapper struct for managing the state of a scan operation in a cluster.
/// It holds a reference to the scan state and provides methods for accessing the state.
/// The `status` field indicates the status of the scan operation.
pub struct ScanStateRC {
    scan_state_rc: Arc<Option<ScanState>>,
    status: ScanStateStage,
}

impl ScanStateRC {
    /// Creates a new instance of [`ScanStateRC`] from a given [`ScanState`].
    fn from_scan_state(scan_state: ScanState) -> Self {
        Self {
            scan_state_rc: Arc::new(Some(scan_state)),
            status: ScanStateStage::InProgress,
        }
    }

    /// Creates a new instance of [`ScanStateRC`].
    ///
    /// This method initializes the [`ScanStateRC`] with a reference to a [`ScanState`] that is initially set to `None`.
    /// An empty ScanState is equivalent to a 0 cursor.
    pub fn new() -> Self {
        Self {
            scan_state_rc: Arc::new(None),
            status: ScanStateStage::Initiating,
        }
    }
    /// create a new instance of [`ScanStateRC`] with finished state and empty scan state.
    fn create_finished() -> Self {
        Self {
            scan_state_rc: Arc::new(None),
            status: ScanStateStage::Finished,
        }
    }
    /// Returns `true` if the scan state is finished.
    pub fn is_finished(&self) -> bool {
        self.status == ScanStateStage::Finished
    }

    /// Returns a clone of the scan state, if it exist.
    pub(crate) fn get_state_from_wrapper(&self) -> Option<ScanState> {
        if self.status == ScanStateStage::Initiating || self.status == ScanStateStage::Finished {
            None
        } else {
            self.scan_state_rc.as_ref().clone()
        }
    }
}

/// This trait defines the methods for interacting with a Redis cluster during scanning.
#[async_trait]
pub(crate) trait ClusterInScan {
    /// Retrieves the address associated with a given slot in the cluster.
    async fn get_address_by_slot(&self, slot: u16) -> RedisResult<String>;

    /// Retrieves the epoch of a given address in the cluster.
    /// The epoch represents the version of the address, which is updated when a failover occurs or slots migrate in.
    async fn get_address_epoch(&self, address: &str) -> Result<u64, RedisError>;

    /// Retrieves the slots assigned to a given address in the cluster.
    async fn get_slots_of_address(&self, address: &str) -> Vec<u16>;

    /// Routes a Redis command to a specific address in the cluster.
    async fn route_command(&self, cmd: Cmd, address: &str) -> RedisResult<Value>;

    /// Check if all slots are covered by the cluster
    async fn are_all_slots_covered(&self) -> bool;

    /// Check if the topology of the cluster has changed and refresh the slots if needed
    async fn refresh_if_topology_changed(&self);
}

/// Represents the state of a scan operation in a Redis cluster.
///
/// This struct holds information about the current scan state, including the cursor position,
/// the scanned slots map, the address being scanned, and the address's epoch.
#[derive(PartialEq, Debug, Clone)]
pub(crate) struct ScanState {
    // the real cursor in the scan operation
    cursor: u64,
    // a map of the slots that have been scanned
    scanned_slots_map: SlotsBitsArray,
    // the address that is being scanned currently, based on the next slot set to 0 in the scanned_slots_map, and the address that "owns" the slot
    // in the SlotMap
    pub(crate) address_in_scan: String,
    // epoch represent the version of the address, when a failover happens or slots migrate in the epoch will be updated to +1
    address_epoch: u64,
    // the status of the scan operation
    scan_status: ScanStateStage,
}

impl ScanState {
    /// Create a new instance of ScanState.
    ///
    /// # Arguments
    ///
    /// * `cursor` - The cursor position.
    /// * `scanned_slots_map` - The scanned slots map.
    /// * `address_in_scan` - The address being scanned.
    /// * `address_epoch` - The epoch of the address being scanned.
    /// * `scan_status` - The status of the scan operation.
    ///
    /// # Returns
    ///
    /// A new instance of ScanState.
    pub fn new(
        cursor: u64,
        scanned_slots_map: SlotsBitsArray,
        address_in_scan: String,
        address_epoch: u64,
        scan_status: ScanStateStage,
    ) -> Self {
        Self {
            cursor,
            scanned_slots_map,
            address_in_scan,
            address_epoch,
            scan_status,
        }
    }

    fn create_finished_state() -> Self {
        Self {
            cursor: 0,
            scanned_slots_map: [0; BITS_ARRAY_SIZE],
            address_in_scan: String::new(),
            address_epoch: 0,
            scan_status: ScanStateStage::Finished,
        }
    }

    /// Initialize a new scan operation.
    /// This method creates a new scan state with the cursor set to 0, the scanned slots map initialized to 0,
    /// and the address set to the address associated with slot 0.
    /// The address epoch is set to the epoch of the address.
    /// If the address epoch cannot be retrieved, the method returns an error.
    async fn initiate_scan<C: ClusterInScan + ?Sized>(connection: &C) -> RedisResult<ScanState> {
        let new_scanned_slots_map: SlotsBitsArray = [0; BITS_ARRAY_SIZE];
        let new_cursor = 0;
        let address = connection.get_address_by_slot(0).await?;
        let address_epoch = connection.get_address_epoch(&address).await.unwrap_or(0);
        Ok(ScanState::new(
            new_cursor,
            new_scanned_slots_map,
            address,
            address_epoch,
            ScanStateStage::InProgress,
        ))
    }

    /// Get the next slot to be scanned based on the scanned slots map.
    /// If all slots have been scanned, the method returns [`END_OF_SCAN`].
    fn get_next_slot(&self, scanned_slots_map: &SlotsBitsArray) -> Option<u16> {
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

    /// Update the scan state without updating the scanned slots map.
    /// This method is used when the address epoch has changed, and we can't determine which slots are new.
    /// In this case, we skip updating the scanned slots map and only update the address and cursor.
    async fn creating_state_without_slot_changes<C: ClusterInScan + ?Sized>(
        &self,
        connection: &C,
    ) -> RedisResult<ScanState> {
        let next_slot = self.get_next_slot(&self.scanned_slots_map).unwrap_or(0);
        let new_address = if next_slot == END_OF_SCAN {
            return Ok(ScanState::create_finished_state());
        } else {
            connection.get_address_by_slot(next_slot).await
        };
        match new_address {
            Ok(address) => {
                let new_epoch = connection.get_address_epoch(&address).await.unwrap_or(0);
                Ok(ScanState::new(
                    0,
                    self.scanned_slots_map,
                    address,
                    new_epoch,
                    ScanStateStage::InProgress,
                ))
            }
            Err(err) => Err(err),
        }
    }

    /// Update the scan state and get the next address to scan.
    /// This method is called when the cursor reaches 0, indicating that the current address has been scanned.
    /// This method updates the scan state based on the scanned slots map and retrieves the next address to scan.
    /// If the address epoch has changed, the method skips updating the scanned slots map and only updates the address and cursor.
    /// If the address epoch has not changed, the method updates the scanned slots map with the slots owned by the address.
    /// The method returns the new scan state with the updated cursor, scanned slots map, address, and epoch.
    async fn create_updated_scan_state_for_completed_address<C: ClusterInScan + ?Sized>(
        &mut self,
        connection: &C,
    ) -> RedisResult<ScanState> {
        let _ = connection.refresh_if_topology_changed().await;
        let mut scanned_slots_map = self.scanned_slots_map;
        // If the address epoch changed it mean that some slots in the address are new, so we cant know which slots been there from the beginning and which are new, or out and in later.
        // In this case we will skip updating the scanned_slots_map and will just update the address and the cursor
        let new_address_epoch = connection
            .get_address_epoch(&self.address_in_scan)
            .await
            .unwrap_or(0);
        if new_address_epoch != self.address_epoch {
            return self.creating_state_without_slot_changes(connection).await;
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
        let next_slot = self.get_next_slot(&scanned_slots_map).unwrap_or(0);
        let new_address = if next_slot == END_OF_SCAN {
            return Ok(ScanState::create_finished_state());
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
                Ok(ScanState::new(
                    new_cursor,
                    scanned_slots_map,
                    new_address,
                    new_epoch,
                    ScanStateStage::InProgress,
                ))
            }
            Err(err) => Err(err),
        }
    }
}

// Implement the [`ClusterInScan`] trait for [`InnerCore`] of async cluster connection.
#[async_trait]
impl<C> ClusterInScan for Core<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + 'static,
{
    async fn get_address_by_slot(&self, slot: u16) -> RedisResult<String> {
        let address = self
            .get_address_from_slot(slot, SlotAddr::ReplicaRequired)
            .await;
        match address {
            Some(addr) => Ok(addr),
            None => {
                if self.are_all_slots_covered().await {
                    Err(RedisError::from((
                        ErrorKind::IoError,
                        "Failed to get connection to the node cover the slot, please check the cluster configuration ",
                    )))
                } else {
                    Err(RedisError::from((
                        ErrorKind::NotAllSlotsCovered,
                        "All slots are not covered by the cluster, please check the cluster configuration ",
                    )))
                }
            }
        }
    }

    async fn get_address_epoch(&self, address: &str) -> Result<u64, RedisError> {
        self.as_ref().get_address_epoch(address).await
    }
    async fn get_slots_of_address(&self, address: &str) -> Vec<u16> {
        self.as_ref().get_slots_of_address(address).await
    }
    async fn route_command(&self, cmd: Cmd, address: &str) -> RedisResult<Value> {
        let routing = InternalRoutingInfo::SingleNode(InternalSingleNodeRouting::ByAddress(
            address.to_string(),
        ));
        let core = self.to_owned();
        let response = ClusterConnInner::<C>::try_cmd_request(Arc::new(cmd), routing, core)
            .await
            .map_err(|err| err.1)?;
        match response {
            Response::Single(value) => Ok(value),
            _ => Err(RedisError::from((
                ErrorKind::ClientError,
                "Expected single response, got unexpected response",
            ))),
        }
    }
    async fn are_all_slots_covered(&self) -> bool {
        ClusterConnInner::<C>::check_if_all_slots_covered(&self.conn_lock.read().await.slot_map)
    }
    async fn refresh_if_topology_changed(&self) {
        ClusterConnInner::check_topology_and_refresh_if_diff(
            self.to_owned(),
            // The cluster SCAN implementation must refresh the slots when a topology change is found
            // to ensure the scan logic is correct.
            &RefreshPolicy::NotThrottable,
        )
        .await;
    }
}

/// Perform a cluster scan operation.
/// This function performs a scan operation in a Redis cluster using the given [`ClusterInScan`] connection.
/// It scans the cluster for keys based on the given `ClusterScanArgs` arguments.
/// The function returns a tuple containing the new scan state cursor and the keys found in the scan operation.
/// If the scan operation fails, an error is returned.
///
/// # Arguments
/// * `core` - The connection to the Redis cluster.
/// * `cluster_scan_args` - The arguments for the cluster scan operation.
///
/// # Returns
/// A tuple containing the new scan state cursor and the keys found in the scan operation.
/// If the scan operation fails, an error is returned.
pub(crate) async fn cluster_scan<C>(
    core: C,
    cluster_scan_args: ClusterScanArgs,
) -> RedisResult<(ScanStateRC, Vec<Value>)>
where
    C: ClusterInScan,
{
    let ClusterScanArgs {
        scan_state_cursor,
        match_pattern,
        count,
        object_type,
    } = cluster_scan_args;
    // If scan_state is None, meaning we start a new scan
    let scan_state = match scan_state_cursor.get_state_from_wrapper() {
        Some(state) => state,
        None => match ScanState::initiate_scan(&core).await {
            Ok(state) => state,
            Err(err) => {
                return Err(err);
            }
        },
    };
    // Send the actual scan command to the address in the scan_state
    let scan_result = send_scan(
        &scan_state,
        &core,
        match_pattern.clone(),
        count,
        object_type.clone(),
    )
    .await;
    let ((new_cursor, new_keys), mut scan_state): ((u64, Vec<Value>), ScanState) = match scan_result
    {
        Ok(scan_result) => (from_redis_value(&scan_result)?, scan_state.clone()),
        Err(err) => match err.kind() {
            // If the scan command failed to route to the address because the address is not found in the cluster or
            // the connection to the address cant be reached from different reasons, we will check we want to check if
            // the problem is problem that we can recover from like failover or scale down or some network issue
            // that we can retry the scan command to an address that own the next slot we are at.
            ErrorKind::IoError | ErrorKind::ClusterConnectionNotFound => {
                let retry =
                    retry_scan(&scan_state, &core, match_pattern, count, object_type).await?;
                (from_redis_value(&retry.0?)?, retry.1)
            }
            _ => return Err(err),
        },
    };

    // If the cursor is 0, meaning we finished scanning the address
    // we will update the scan state to get the next address to scan
    if new_cursor == 0 {
        scan_state = scan_state
            .create_updated_scan_state_for_completed_address(&core)
            .await?;
    }

    // If the address is empty, meaning we finished scanning all the address
    if scan_state.scan_status == ScanStateStage::Finished {
        return Ok((ScanStateRC::create_finished(), new_keys));
    }

    scan_state = ScanState::new(
        new_cursor,
        scan_state.scanned_slots_map,
        scan_state.address_in_scan,
        scan_state.address_epoch,
        ScanStateStage::InProgress,
    );
    Ok((ScanStateRC::from_scan_state(scan_state), new_keys))
}

// Send the scan command to the address in the scan_state
async fn send_scan<C>(
    scan_state: &ScanState,
    core: &C,
    match_pattern: Option<String>,
    count: Option<usize>,
    object_type: Option<ObjectType>,
) -> RedisResult<Value>
where
    C: ClusterInScan,
{
    let mut scan_command = cmd("SCAN");
    scan_command.arg(scan_state.cursor);
    if let Some(match_pattern) = match_pattern {
        scan_command.arg("MATCH").arg(match_pattern);
    }
    if let Some(count) = count {
        scan_command.arg("COUNT").arg(count);
    }
    if let Some(object_type) = object_type {
        scan_command.arg("TYPE").arg(object_type.to_string());
    }

    core.route_command(scan_command, &scan_state.address_in_scan)
        .await
}

// If the scan command failed to route to the address we will check we will first refresh the slots, we will check if all slots are covered by cluster,
// and if so we will try to get a new address to scan for handling case of failover.
// if all slots are not covered by the cluster we will return an error indicating that the cluster is not well configured.
// if all slots are covered by cluster but we failed to get a new address to scan we will return an error indicating that we failed to get a new address to scan.
// if we got a new address to scan but the scan command failed to route to the address we will return an error indicating that we failed to route the command.
async fn retry_scan<C>(
    scan_state: &ScanState,
    core: &C,
    match_pattern: Option<String>,
    count: Option<usize>,
    object_type: Option<ObjectType>,
) -> RedisResult<(RedisResult<Value>, ScanState)>
where
    C: ClusterInScan,
{
    // TODO: This mechanism of refreshing on failure to route to address should be part of the routing mechanism
    // After the routing mechanism is updated to handle this case, this refresh in the case bellow should be removed
    core.refresh_if_topology_changed().await;
    if !core.are_all_slots_covered().await {
        return Err(RedisError::from((
            ErrorKind::NotAllSlotsCovered,
            "Not all slots are covered by the cluster, please check the cluster configuration",
        )));
    }
    // If for some reason we failed to reach the address we don't know if its a scale down or a failover.
    // Since it might be scale down we cant just keep going with the current state we the same cursor as we are at
    // the same point in the new address, so we need to get the new address own the next slot that haven't been scanned
    // and start from the beginning of this address.
    let next_slot = scan_state
        .get_next_slot(&scan_state.scanned_slots_map)
        .unwrap_or(0);
    let address = core.get_address_by_slot(next_slot).await?;

    let new_epoch = core.get_address_epoch(&address).await.unwrap_or(0);
    let scan_state = &ScanState::new(
        0,
        scan_state.scanned_slots_map,
        address,
        new_epoch,
        ScanStateStage::InProgress,
    );
    let res = (
        send_scan(scan_state, core, match_pattern, count, object_type).await,
        scan_state.clone(),
    );
    Ok(res)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_creation_of_empty_scan_wrapper() {
        let scan_state_wrapper = ScanStateRC::new();
        assert!(scan_state_wrapper.status == ScanStateStage::Initiating);
    }

    #[test]
    fn test_creation_of_scan_state_wrapper_from() {
        let scan_state = ScanState {
            cursor: 0,
            scanned_slots_map: [0; BITS_ARRAY_SIZE],
            address_in_scan: String::from("address1"),
            address_epoch: 1,
            scan_status: ScanStateStage::InProgress,
        };

        let scan_state_wrapper = ScanStateRC::from_scan_state(scan_state);
        assert!(!scan_state_wrapper.is_finished());
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
            scan_status: ScanStateStage::InProgress,
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
            scan_status: ScanStateStage::InProgress,
        };
        let next_slot = scan_state.get_next_slot(&scanned_slots_map);
        assert_eq!(next_slot, Some(1));
    }
    // Create a mock connection
    struct MockConnection;
    #[async_trait]
    impl ClusterInScan for MockConnection {
        async fn refresh_if_topology_changed(&self) {}
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
        async fn route_command(&self, _: Cmd, _: &str) -> RedisResult<Value> {
            unimplemented!()
        }
        async fn are_all_slots_covered(&self) -> bool {
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
            scan_status: ScanStateStage::InProgress,
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
            .create_updated_scan_state_for_completed_address(&connection)
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
        let scan_state = ScanState::new(
            0,
            [0; BITS_ARRAY_SIZE],
            "address".to_string(),
            0,
            ScanStateStage::InProgress,
        );
        let scanned_slots_map = scan_state.scanned_slots_map;
        let updated_scan_state = scan_state
            .creating_state_without_slot_changes(&connection)
            .await
            .unwrap();
        assert_eq!(updated_scan_state.scanned_slots_map, scanned_slots_map);
        assert_eq!(updated_scan_state.cursor, 0);
        assert_eq!(updated_scan_state.address_in_scan, "mock_address");
        assert_eq!(updated_scan_state.address_epoch, 0);
    }
}
