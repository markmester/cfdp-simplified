use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Display,
    io::Read,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use error::DaemonResult;
use log::{error, info, warn};
use num_traits::FromPrimitive;
use tokio::{
    select,
    sync::{
        mpsc::{channel, Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
    time::MissedTickBehavior,
};

use crate::{
    filestore::{ChecksumType, FileStore},
    pdu::{
        header::{
            CRCFlag, Condition, DeliveryCode, Direction, FileSizeFlag, FileStatusCode, PDUHeader,
            SegmentedData, TransactionStatus, TransmissionMode,
        },
        ops::{EntityID, TransactionSeqNum},
        PDUEncode, PDUError, PDUResult, PDU,
    },
    transaction::FaultHandlerAction,
    transaction::{Metadata, TransactionConfig, TransactionID, TransactionState},
};

pub mod error;
pub mod segments;
pub mod timer;
pub mod transport;
pub mod user;

pub(crate) use timer::*;
pub use user::*;

use self::error::DaemonError;
use self::transport::PDUTransport;
use crate::transaction::{error::TransactionError, recv::RecvTransaction, send::SendTransaction};

#[derive(Debug, Clone, PartialEq, Eq)]
/// Necessary Configuration for a Put.Request operation
pub struct PutRequest {
    /// Bytes of the source filename, can be null if length is 0.
    pub source_filename: Utf8PathBuf,
    /// Bytes of the destination filename, can be null if length is 0.
    pub destination_filename: Utf8PathBuf,
    /// Destination ID of the Request
    pub destination_entity_id: EntityID,
    /// Whether to send in acknowledged or unacknowledged mode
    pub transmission_mode: TransmissionMode,
}

#[derive(Debug)]
/// Possible User Primitives sent from a end user application via the user primitive channel
pub enum UserPrimitive {
    /// Initiate a Put transaction with the specified [PutRequest] configuration.
    /// The channel is for the requesting entity to receive the unique transaction ID
    /// from the Daemon.
    Put(PutRequest, oneshot::Sender<TransactionID>),
    /// Cancel the give transaction.
    Cancel(TransactionID),
    /// Report progress of the given transaction.
    Report(TransactionID, oneshot::Sender<Report>),
}

/// Simple Status Report
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
/// Transaction status report
pub struct Report {
    /// The unique ID of the transaction.
    pub id: TransactionID,
    /// Current state of the transaction
    pub state: TransactionState,
    /// Current status of the transaction.
    pub status: TransactionStatus,
    /// Last known condition of the transaction.
    pub condition: Condition,
    /// File size of the transaction
    pub file_size: u64,
    /// Bytes received by the transaction
    pub file_bytes_received: Option<u64>,
    /// Bytes sent by the transaction
    pub file_bytes_sent: Option<u64>,
    /// Whether an empty NAK was received.
    pub empty_nak_received: bool,
    /// Transaction Direction
    pub direction: Option<Direction>,
    /// Name of file
    pub file_name: String,
    /// Date time file was initially submitted/received
    pub submit_date: DateTime<Utc>,
}
impl Report {
    pub fn encode(self) -> Vec<u8> {
        let mut buff = self.id.0.encode();
        buff.extend(self.id.1.encode());
        buff.push(self.state as u8);
        buff.push(self.status as u8);
        buff.push(self.condition as u8);
        buff
    }

    pub fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self> {
        let id = {
            let entity_id = EntityID::decode(buffer)?;
            let sequence_num = TransactionSeqNum::decode(buffer)?;

            TransactionID(entity_id, sequence_num)
        };

        let mut u8_buff = [0_u8; 1];

        let state = {
            buffer.read_exact(&mut u8_buff)?;
            let possible = u8_buff[0];
            TransactionState::from_u8(possible).ok_or(PDUError::InvalidState(possible))?
        };

        let status = {
            buffer.read_exact(&mut u8_buff)?;
            let possible = u8_buff[0];
            TransactionStatus::from_u8(possible)
                .ok_or(PDUError::InvalidTransactionStatus(possible))?
        };

        let condition = {
            buffer.read_exact(&mut u8_buff)?;
            let possible = u8_buff[0];
            Condition::from_u8(possible).ok_or(PDUError::InvalidCondition(possible))?
        };

        Ok(Self {
            id,
            state,
            status,
            condition,
            empty_nak_received: false,
            file_size: 0,
            file_bytes_received: None,
            file_bytes_sent: None,
            direction: None,
            file_name: String::new(),
            submit_date: Utc::now(),
        })
    }
}

#[derive(Debug, Clone)]
/// Indication sent from a Transaction when [Metadata](crate::transaction::Metadata) has been received
pub struct MetadataRecvIndication {
    pub id: TransactionID,
    /// source file name relative to the filestore root.
    pub source_filename: Utf8PathBuf,
    /// destination file name relative to the filestore root
    pub destination_filename: Utf8PathBuf,
    /// Size of the file in bytes
    pub file_size: u64,
    /// Which transmission mode will used in the transaction.
    pub transmission_mode: TransmissionMode,
}

#[derive(Debug, Clone)]
/// Indication of the amount of data received from a [FileDataPDU](crate::pdu::FileDataPDU)
pub struct FileSegmentIndication {
    /// Unique transaction ID for the file data.
    pub id: TransactionID,
    /// Byte index offset in the file.
    pub offset: u64,
    /// Length of the file data received.
    pub length: u64,
}

#[derive(Debug, Clone)]
/// Indication sent when a transaction has finished.
pub struct FinishedIndication {
    /// Unique transaction ID.
    pub id: TransactionID,
    /// Final report of the transaction before shutting down.
    pub report: Report,
    /// The status of the file delivered if applicable.
    pub file_status: FileStatusCode,
    /// The final delivery result.
    pub delivery_code: DeliveryCode,
}

#[derive(Debug, Clone)]
/// Indications how the Daemon and Transactions relay information back to the User application.
/// Indications are issued at necessary points in each Transaction's lifetime.
pub enum Indication {
    /// A new transaction has been initiated as a result of a [PutRequest]
    Transaction(TransactionID),
    /// End of File has been Sent
    EoFSent(TransactionID),
    /// End of File PDU has been received
    EoFRecv(TransactionID),
    /// A running transaction has reached the Finished state.
    /// Receipt of this indications starts and post transaction actions.
    Finished(FinishedIndication),
    /// Metadata has been received for a Receive Transaction
    MetadataRecv(MetadataRecvIndication),
    /// A new file segment has been received
    FileSegmentRecv(FileSegmentIndication),
    /// Last known status for the given transaction
    Report(Report),
}

/// The way the Nak procedure is implemented is the following:
///  - In Immediate mode, upon reception of each file data PDU, if the received segment is at the end of the file and
///    there is a gap between the previously received segment and the new segment, a nak is sent with the new gap but
///    only after delay has elapsed (if any delay was set).
///    If the NAK timer has timed out, the nak sent covers the gaps from the entire file, not only the last gap.
///    After the EOF is received, the procedure is the same as in deferred mode.
///  - In Deferred mode, a nak covering the gaps from the entire file is sent after the EOF has been received
///    and each time the nak timer times out.
///
/// The delay parameter is useful when PDUs come out of order to avoid sending NAKs prematurely. One scenario when this may
/// happen is when utilizing multiple links of different latencies. The delay should be set to cover the difference in latency
/// between the slowest link and the fastest link.
/// If the delay is greater than 0, the NAKs will not be sent immediately but only if the gap persists after the delay
/// has passed.
///
/// NAK timer (note that this is different and probably much larger than the delay parameter mentioned above):
/// - In Immediate mode the NAK timer is started at the beginning of the transaction.
/// - In Deferred mode  the NAK timer is started after EOF is received.
/// - If the NAK timer times out and it is determined that new data has been received since the last nak sending,
///   the timer counter is reset to 0.
/// - If the NAK timer expired more than the predefined limit (without any new data being received), the NakLimitReached
///   fault will be raised.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NakProcedure {
    Immediate(Duration /* delay*/),
    Deferred(Duration /* delay */),
}

#[derive(Clone)]
/// Configuration parameters for transactions which may change based on the receiving entity.
pub struct EntityConfig {
    /// Mapping to decide how each fault type should be handled
    pub fault_handler_override: HashMap<Condition, FaultHandlerAction>,
    /// Maximum file size fragment this entity can receive
    pub file_size_segment: u16,
    // The number of timeouts before a fault is issued on a transaction
    pub default_transaction_max_count: u32,
    // number of seconds for inactivity timers to wait
    pub inactivity_timeout: i64,
    // number of seconds for EOF timers to wait
    pub eof_timeout: i64,
    // number of seconds for NAK timers to wait
    pub nak_timeout: i64,
    /// Flag to determine if the CRC protocol should be used
    pub crc_flag: CRCFlag,
    /// The default ChecksumType to use for file transfers
    pub checksum_type: ChecksumType,
    // for recv transactions - when to send the NAKs (immediately when detected or after EOF)
    pub nak_procedure: NakProcedure,
    // Local entity ID of CFDP instance
    pub local_entity_id: u16,
    // Remote entity ID of CFDP instance
    pub remote_entity_id: u16,
    // Local CFDP server address
    pub local_server_addr: &'static str,
    // Remote CFDP server address
    pub remote_server_addr: &'static str,
    // Interval in seconds to send progress report
    pub progress_report_interval_secs: i64,
}

/// Lightweight commands the Daemon send to each Transaction
#[derive(Debug)]
pub enum Command {
    Pdu(PDU),
    Abandon,
    Report(oneshot::Sender<Report>),
}
impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
}

fn construct_metadata<T: FileStore + Send + 'static>(
    filestore: &Arc<T>,
    req: PutRequest,
    config: EntityConfig,
) -> DaemonResult<Metadata> {
    let file_size = match req.source_filename.file_name().is_none() {
        true => 0_u64,
        false => filestore
            .get_size(&req.source_filename)
            .map_err(DaemonError::SpawnSend)?,
    };
    Ok(Metadata {
        source_filename: req.source_filename,
        destination_filename: req.destination_filename,
        file_size,
        checksum_type: config.checksum_type,
    })
}

type RecvSpawnerTuple = (
    TransactionID,
    Sender<Command>,
    JoinHandle<Result<TransactionID, TransactionError>>,
);

type SendSpawnerTuple = (
    Sender<Command>,
    JoinHandle<Result<TransactionID, TransactionError>>,
);

/// The CFDP Daemon is responsible for connecting [PDUTransport](crate::transport::PDUTransport) implementation
/// with each individual [SendTransaction](crate::transaction::SendTransaction) and [RecvTransaction](crate::transaction::RecvTransaction).
/// When a PDUTransport implementation
/// sends a PDU through a channel, the Daemon distributes the PDU to the necessary Transaction.
/// PDUs are sent from each Transaction directly to their respective PDUTransport implementations.
pub struct Daemon<T: FileStore + Send + 'static> {
    // The collection of all current transactions
    transaction_handles: Vec<JoinHandle<Result<TransactionID, TransactionError>>>,
    // Mapping of unique transaction ids to channels used to talk to each transaction
    transaction_channels: HashMap<TransactionID, Sender<Command>>,
    // the vector of transportation tx channel connections
    transport_tx_map: HashMap<EntityID, Sender<(EntityID, PDU)>>,
    // the transport PDU rx channel connection
    transport_rx: Receiver<PDU>,
    // the underlying filestore used by this Daemon
    filestore: Arc<T>,
    // message sender channel used to send Indications from Transactions to the User
    indication_tx: Sender<Indication>,
    // a mapping of individual fault handler actions per remote entity
    entity_configs: HashMap<EntityID, EntityConfig>,
    // the default fault handling configuration
    default_config: EntityConfig,
    // the entity ID of this daemon
    entity_id: EntityID,
    // current running count of the sequence numbers of transaction initiated by this entity
    sequence_num: TransactionSeqNum,
    // termination signal sent to children threads
    terminate: Arc<AtomicBool>,
    // channel to receive user primitives from the implemented User
    primitive_rx: Receiver<UserPrimitive>,
}
impl<T: FileStore + Send + Sync + 'static> Daemon<T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        entity_id: EntityID,
        sequence_num: TransactionSeqNum,
        transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>>,
        filestore: Arc<T>,
        entity_configs: HashMap<EntityID, EntityConfig>,
        default_config: EntityConfig,
        primitive_rx: Receiver<UserPrimitive>,
        indication_tx: Sender<Indication>,
    ) -> Self {
        let mut transport_tx_map: HashMap<EntityID, Sender<(EntityID, PDU)>> = HashMap::new();
        let (pdu_send, pdu_receive) = channel(100);
        let terminate = Arc::new(AtomicBool::new(false));
        for (vec, mut transport) in transport_map.into_iter() {
            let (remote_send, remote_receive) = channel(1);

            vec.iter().for_each(|id| {
                transport_tx_map.insert(*id, remote_send.clone());
            });

            let signal = terminate.clone();
            let sender = pdu_send.clone();
            tokio::task::spawn(async move {
                transport.pdu_handler(signal, sender, remote_receive).await
            });
        }
        Self {
            transaction_handles: vec![],
            transaction_channels: HashMap::new(),
            transport_tx_map,
            transport_rx: pdu_receive,
            filestore,
            indication_tx,
            entity_configs,
            default_config,
            entity_id,
            sequence_num,
            terminate,
            primitive_rx,
        }
    }

    fn spawn_receive_transaction(
        header: &PDUHeader,
        transport_tx: Sender<(EntityID, PDU)>,
        entity_config: EntityConfig,
        filestore: Arc<T>,
        indication_tx: Sender<Indication>,
    ) -> RecvSpawnerTuple {
        let (transaction_tx, mut transaction_rx) = channel(100);

        let config = TransactionConfig {
            source_entity_id: header.source_entity_id,
            destination_entity_id: header.destination_entity_id,
            transmission_mode: header.transmission_mode,
            sequence_number: header.transaction_sequence_number,
            file_size_flag: header.large_file_flag,
            fault_handler_override: entity_config.fault_handler_override.clone(),
            file_size_segment: entity_config.file_size_segment,
            crc_flag: header.crc_flag,
            segment_metadata_flag: header.segment_metadata_flag,
            max_count: entity_config.default_transaction_max_count,
            inactivity_timeout: entity_config.inactivity_timeout,
            eof_timeout: entity_config.eof_timeout,
            nak_timeout: entity_config.nak_timeout,
            progress_report_interval_secs: entity_config.progress_report_interval_secs,
        };
        let mut transaction = RecvTransaction::new(
            config,
            entity_config.nak_procedure,
            filestore,
            indication_tx,
        );
        let id = transaction.id();

        // tokio tasks can have names but that seems an unstable feature
        let handle = tokio::task::spawn(async move {
            transaction.send_report(None)?;

            while transaction.get_state() != TransactionState::Terminated {
                let timeout = transaction.until_timeout();
                select! {
                    Ok(permit) = transport_tx.reserve(), if transaction.has_pdu_to_send() => {
                        transaction.send_pdu(permit)?
                    },
                    Some(command) = transaction_rx.recv() => {
                        match command {
                            Command::Pdu(pdu) => {
                                match transaction.process_pdu(pdu) {
                                    Ok(()) => {}
                                    Err(err @ TransactionError::UnexpectedPDU(..)) => {
                                        info!("Transaction {} Received Unexpected PDU: {err}", transaction.id());
                                        // log some info on the unexpected PDU?
                                    }
                                    Err(err) => return Err(err)
                                }
                            }
                            Command::Abandon => transaction.shutdown(),
                            Command::Report(sender) => {
                                transaction.send_report(Some(sender))?
                            }
                        }
                    }
                    _ = tokio::time::sleep(timeout) => {
                        transaction.handle_timeout()?;
                    }
                    else => {
                        if transport_tx.is_closed(){
                            log::error!("Channel to transport unexpectedly severed for transaction {}.", transaction.id());
                        }

                        break;
                    }
                };
            }

            transaction.send_report(None)?;
            Ok(transaction.id())
        });

        (id, transaction_tx, handle)
    }

    fn spawn_send_transaction(
        request: PutRequest,
        transaction_id: TransactionID,
        transport_tx: Sender<(EntityID, PDU)>,
        entity_config: EntityConfig,
        filestore: Arc<T>,
        indication_tx: Sender<Indication>,
    ) -> DaemonResult<SendSpawnerTuple> {
        let (transaction_tx, mut transaction_rx) = channel(10);

        let destination_entity_id = request.destination_entity_id;
        let transmission_mode = request.transmission_mode;
        let mut config = TransactionConfig {
            source_entity_id: transaction_id.0,
            destination_entity_id,
            transmission_mode,
            sequence_number: transaction_id.1,
            file_size_flag: FileSizeFlag::Small,
            fault_handler_override: entity_config.fault_handler_override.clone(),
            file_size_segment: entity_config.file_size_segment,
            crc_flag: entity_config.crc_flag,
            segment_metadata_flag: SegmentedData::NotPresent,
            max_count: entity_config.default_transaction_max_count,
            inactivity_timeout: entity_config.inactivity_timeout,
            eof_timeout: entity_config.eof_timeout,
            nak_timeout: entity_config.nak_timeout,
            progress_report_interval_secs: entity_config.progress_report_interval_secs,
        };
        let metadata = construct_metadata(&filestore, request, entity_config)?;

        let handle = tokio::task::spawn(async move {
            config.file_size_flag = match metadata.file_size <= u32::MAX.into() {
                true => FileSizeFlag::Small,
                false => FileSizeFlag::Large,
            };

            let mut transaction = SendTransaction::new(config, metadata, filestore, indication_tx)?;
            transaction.send_report(None)?;

            while transaction.get_state() != TransactionState::Terminated {
                let timeout = transaction.until_timeout();
                select! {
                    Ok(permit) = transport_tx.reserve(), if transaction.has_pdu_to_send()  => {
                        transaction.send_pdu(permit)?;
                    },

                    Some(command) = transaction_rx.recv() => {
                        match command {
                            Command::Pdu(pdu) => {
                                match transaction.process_pdu(pdu) {
                                    Ok(()) => {}
                                    Err(
                                        err @ TransactionError::UnexpectedPDU(..),
                                    ) => {
                                        info!("Received Unexpected PDU: {err}");
                                        // log some info on the unexpected PDU?
                                    }
                                    Err(err) => {
                                        return Err(err);
                                    }
                                }
                            }
                            Command::Abandon => transaction.shutdown(),
                            Command::Report(sender) => {
                                transaction.send_report(Some(sender))?
                            },
                        }
                    },
                    _ = tokio::time::sleep(timeout) => {
                        transaction.handle_timeout()?;
                    },
                    else => {
                        if transport_tx.is_closed(){
                            log::error!("Connection to transport unexpectedly severed for transaction {}.", transaction.id());
                        }
                        break;
                    }
                };
            }
            transaction.send_report(None)?;
            Ok(transaction_id)
        });
        Ok((transaction_tx, handle))
    }

    async fn process_primitive(&mut self, primitive: UserPrimitive) -> DaemonResult<()> {
        match primitive {
            UserPrimitive::Put(request, put_sender) => {
                let sequence_number = self.sequence_num.get_and_increment();

                let entity_config = self
                    .entity_configs
                    .get(&request.destination_entity_id)
                    .unwrap_or(&self.default_config)
                    .clone();

                if let Some(transport_tx) = self
                    .transport_tx_map
                    .get(&request.destination_entity_id)
                    .cloned()
                {
                    let id = TransactionID(self.entity_id, sequence_number);
                    let (sender, handle) = Self::spawn_send_transaction(
                        request,
                        id,
                        transport_tx,
                        entity_config,
                        self.filestore.clone(),
                        self.indication_tx.clone(),
                    )?;
                    self.transaction_handles.push(handle);
                    self.transaction_channels.insert(id, sender);

                    // ignore the possible error if the user disconnected;
                    let _ = put_sender.send(id);
                } else {
                    warn!(
                        "No Transport available for EntityID: {}. Skipping transaction creation.",
                        request.destination_entity_id
                    )
                }
            }
            UserPrimitive::Cancel(id) => {
                if let Some(channel) = self.transaction_channels.get(&id) {
                    channel
                        .send(Command::Abandon)
                        .await
                        .map_err(|err| DaemonError::from((id, err)))?;
                }
            }
            UserPrimitive::Report(id, report_sender) => {
                if let Some(channel) = self.transaction_channels.get(&id) {
                    channel
                        .send(Command::Report(report_sender))
                        .await
                        .map_err(|err| DaemonError::from((id, err)))?;
                }
            }
        };
        Ok(())
    }

    async fn forward_pdu(&mut self, pdu: PDU) -> DaemonResult<()> {
        // find the entity this entity will be sending too.
        // If this PDU is to the sender, we send to the destination
        // if this PDU is to the receiver, we send to the source
        let transport_entity = match &pdu.header.direction {
            Direction::ToSender => pdu.header.destination_entity_id,
            Direction::ToReceiver => pdu.header.source_entity_id,
        };

        let key = TransactionID(
            pdu.header.source_entity_id,
            pdu.header.transaction_sequence_number,
        );
        // hand pdu off to transaction
        let channel = match self.transaction_channels.entry(key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                if let Some(transport) = self.transport_tx_map.get(&transport_entity).cloned() {
                    // if this key is not in the channel list
                    // create a new transaction
                    let entity_config = self
                        .entity_configs
                        .get(&key.0)
                        .unwrap_or(&self.default_config)
                        .clone();
                    match &pdu.header.direction {
                        Direction::ToReceiver => {
                            let (_id, channel, handle) = Self::spawn_receive_transaction(
                                &pdu.header,
                                transport,
                                entity_config,
                                self.filestore.clone(),
                                self.indication_tx.clone(),
                            );

                            self.transaction_handles.push(handle);
                            entry.insert(channel)
                        }
                        // This is a very unlikely scenario.
                        // We have received a PDU sent back to the Sender but we do not have a transaction running.
                        // Likely causes are a system reboot in the middle of a transaction.
                        // Unfortunately there is not enough information in a PDU
                        // to completely re-create the transaction.
                        Direction::ToSender => {
                            error!("Received PDU sent back to sender but no transaction running. Unable to resume transaction.");
                            return Err(DaemonError::UnableToResume(TransactionID(
                                pdu.header.source_entity_id,
                                pdu.header.transaction_sequence_number,
                            )));
                        }
                    }
                } else {
                    warn!(
                        "No Transport available for EntityID: {}. Skipping Transaction creation.",
                        transport_entity
                    );
                    // skip to the next loop iteration
                    return Ok(());
                }
            }
        };

        if channel.send(Command::Pdu(pdu.clone())).await.is_err() {
            // the transaction is completed.
            // spawn a new one
            // this is very unlikely and only results
            // if a sender is re-using a transaction id
            match pdu.header.direction {
                Direction::ToReceiver => {
                    let entity_config = self
                        .entity_configs
                        .get(&key.0)
                        .unwrap_or(&self.default_config)
                        .clone();
                    if let Some(transport) = self.transport_tx_map.get(&transport_entity).cloned() {
                        let (id, new_channel, handle) = Self::spawn_receive_transaction(
                            &pdu.header,
                            transport,
                            entity_config,
                            self.filestore.clone(),
                            self.indication_tx.clone(),
                        );
                        self.transaction_handles.push(handle);
                        new_channel
                            .send(Command::Pdu(pdu.clone()))
                            .await
                            .map_err(|err| DaemonError::from((id, err)))?;
                        // update the dict to have the new channel
                        self.transaction_channels.insert(key, new_channel);
                    }
                }
                Direction::ToSender => {
                    // This is a very unlikely scenario.
                    // We have received a PDU sent back to the Sender
                    // but the transaction transaction stopped running in the background.
                    // Likely causes are a filestore error inside the transaction.
                    // Unfortunately there is not enough information in a PDU
                    // to completely re-create the transaction.
                    error!("Received PDU sent back to sender but no transaction running. Unable to resume transaction.");
                    return Err(DaemonError::UnableToResume(TransactionID(
                        pdu.header.source_entity_id,
                        pdu.header.transaction_sequence_number,
                    )));
                }
            };
        }

        Ok(())
    }

    async fn cleanup_transactions(&mut self) {
        // join any handles that have completed
        let mut ind = 0;
        while ind < self.transaction_handles.len() {
            if self.transaction_handles[ind].is_finished() {
                let handle = self.transaction_handles.remove(ind);
                match handle.await {
                    Ok(Ok(id)) => {
                        // remove the channel for this transaction if it is complete
                        let _ = self.transaction_channels.remove(&id);
                    }
                    Ok(Err(err)) => {
                        info!("Error occurred during transaction: {}", err)
                    }
                    Err(_) => error!("Unable to join handle!"),
                };
            } else {
                ind += 1;
            }
        }
    }

    /// This function will consist of the main logic loop in any daemon process.
    pub async fn manage_transactions(&mut self) -> DaemonResult<()> {
        let cleanup = {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            // Don't start counting another tick until the currrent one has been processed.
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            interval
        };
        tokio::pin!(cleanup);

        loop {
            select! {
                pdu = self.transport_rx.recv() => match pdu {
                    Some(pdu) => match self.forward_pdu(pdu).await{
                        Ok(_) => {},
                        Err(error @ DaemonError::TransactionCommunication(_, _)) => {
                            // This occcurs most likely if a user is attempting to
                            // interact with a transaction that is already finished.
                            warn!("{error}");
                        },
                        Err(err) => {
                            if !self.terminate.load(Ordering::Relaxed) {
                                self.terminate.store(true, Ordering::Relaxed);
                            }
                            return Err(err);
                        }
                    },
                    None => {
                        if !self.terminate.load(Ordering::Relaxed) {
                            error!("Transport unexpectedly disconnected from daemon.");
                            self.terminate.store(true, Ordering::Relaxed);
                        }
                        break;
                    }
                },
                primitive = self.primitive_rx.recv() => match primitive {
                    Some(primitive) => match self.process_primitive(primitive).await{
                        Ok(_) => {},
                        Err(error @ DaemonError::SpawnSend(_)) => {
                            // Unable to spawn a send transaction.
                            // There are lots of reasons this cound happen.
                            // Mostly if a user asked for a file that doesn't exist.
                            warn!("{error}");
                        },
                        Err(error @ DaemonError::TransactionCommunication(_, _)) => {
                            // This occcurs most likely if a user is attempting to
                            // interact with a transaction that is already finished.
                            warn!("{error}");
                        }
                        Err(err) => {
                            if !self.terminate.load(Ordering::Relaxed) {
                                self.terminate.store(true, Ordering::Relaxed);
                            }
                            return Err(err);
                        }
                    },
                    None => {
                        info!("User triggered daemon shutdown.");
                        if !self.terminate.load(Ordering::Relaxed) {
                            self.terminate.store(true, Ordering::Relaxed);
                        }
                        break;
                    }
                },
                _ = cleanup.tick() => self.cleanup_transactions().await,
            };
        }

        // a final cleanup
        while let Some(handle) = self.transaction_handles.pop() {
            match handle.await {
                Ok(Ok(id)) => {
                    // remove the channel for this transaction if it is complete
                    let _ = self.transaction_channels.remove(&id);
                }
                Ok(Err(err)) => {
                    info!("Error occurred during transaction: {}", err)
                }
                Err(_) => error!("Unable to join handle!"),
            };
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        daemon::NakProcedure,
        filestore::{ChecksumType, NativeFileStore},
        pdu::{self, CRCFlag, Condition, NegativeAcknowledgmentPDU, PDUPayload, U3},
        transaction::FaultHandlerAction,
    };

    use super::*;

    #[macro_export]
    macro_rules! assert_err{
        ($expression:expr, $($pattern:tt)+) => {
            match $expression {
                $($pattern)+ => {},
                ref e => panic!("expected {} but got {:?}", stringify!($($pattern)+), e)
            }
        }
    }

    #[tokio::test]
    async fn pdu_to_sender_no_transaction() {
        let (_send, recv) = channel(1);
        let (indication_tx, _indication_rx) = channel(1);
        let (_primitive_tx, primitive_rx) = channel(1);
        let filestore = Arc::new(NativeFileStore::new("."));
        let mut transport_tx_map = HashMap::<_, _>::new();

        let (transport_tx, _) = channel(10);

        transport_tx_map.insert(EntityID::from(1_u16), transport_tx);

        let mut daemon = Daemon {
            transaction_handles: vec![],
            transaction_channels: HashMap::<_, _>::new(),
            transport_tx_map,
            transport_rx: recv,
            filestore,
            indication_tx,
            entity_configs: HashMap::new(),
            default_config: EntityConfig {
                fault_handler_override: HashMap::from([(
                    Condition::PositiveLimitReached,
                    FaultHandlerAction::Abandon,
                )]),
                file_size_segment: 1024,
                default_transaction_max_count: 2,
                inactivity_timeout: 0,
                eof_timeout: 1,
                nak_timeout: 2,
                crc_flag: CRCFlag::NotPresent,
                checksum_type: ChecksumType::Modular,
                nak_procedure: NakProcedure::Deferred(Duration::from_secs(0)),
                local_entity_id: 0_u16,
                remote_entity_id: 1_u16,
                local_server_addr: "127.0.0.1:0",
                remote_server_addr: "127.0.0.1:0",
                progress_report_interval_secs: 1,
            },
            entity_id: EntityID::from(0_u16),
            sequence_num: TransactionSeqNum::from(0_u32),
            terminate: Arc::new(AtomicBool::new(false)),
            primitive_rx,
        };
        let payload = PDUPayload::Directive(pdu::Operations::Nak(NegativeAcknowledgmentPDU {
            start_of_scope: 0,
            end_of_scope: 1_000_000,
            segment_requests: vec![],
        }));
        let pdu = PDU {
            header: PDUHeader {
                version: U3::Zero,
                pdu_type: pdu::PDUType::FileDirective,
                direction: Direction::ToSender,
                transmission_mode: pdu::TransmissionMode::Acknowledged,
                crc_flag: CRCFlag::NotPresent,
                large_file_flag: FileSizeFlag::Small,
                pdu_data_field_length: payload.encoded_len(FileSizeFlag::Small),
                segmentation_control: pdu::SegmentationControl::NotPreserved,
                segment_metadata_flag: SegmentedData::NotPresent,
                source_entity_id: EntityID::from(0_u16),
                transaction_sequence_number: TransactionSeqNum::from(3_u32),
                destination_entity_id: EntityID::from(1_u16),
            },
            payload,
        };

        let res = daemon.forward_pdu(pdu).await;
        assert_err!(res, Err(DaemonError::UnableToResume(_)))
    }
}
