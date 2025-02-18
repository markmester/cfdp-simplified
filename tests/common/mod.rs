use std::{
    collections::HashMap,
    fs::{self},
    io::{Error as IoError, ErrorKind},
    marker::PhantomData,
    net::SocketAddr,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Once, OnceLock, PoisonError, RwLock, RwLockReadGuard,
    },
    time::Duration,
};

use async_trait::async_trait;
use bit_struct::u11;
use camino::Utf8PathBuf;
use cfdp_simplified::{
    ccsds::{encode_space_packet, CCSDS_HEADER_LENGTH, CCSDS_SECONDARY_HEADER_LENGTH},
    daemon::{
        EntityConfig, FinishedIndication, Indication, NakProcedure, PutRequest, Report,
        UserPrimitive,
    },
    filestore::{ChecksumType, FileStore, NativeFileStore},
    pdu::{
        CRCFlag, Condition, EntityID, PDUDirective, PDUEncode, PDUPayload, TransactionSeqNum, PDU,
    },
    transaction::{FaultHandlerAction, TransactionID},
};

use log::{debug, error};
use tempfile::TempDir;

use rstest::fixture;
use tokio::{
    net::{ToSocketAddrs, UdpSocket},
    runtime::{self},
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};

use cfdp_simplified::{
    daemon::transport::{PDUTransport, UdpTransport},
    daemon::Daemon,
};
use tracing_subscriber::EnvFilter;

static INIT: Once = Once::new();
pub fn initialize() {
    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
            .init();
    });
}

#[derive(Debug)]
pub(crate) struct JoD<'a, T> {
    handle: Vec<JoinHandle<T>>,
    phantom: PhantomData<&'a ()>,
}
impl<'a, T> From<JoinHandle<T>> for JoD<'a, T> {
    fn from(input: JoinHandle<T>) -> Self {
        Self {
            handle: vec![input],
            phantom: PhantomData,
        }
    }
}

type UserSplit = (TestUserHalf, Receiver<UserPrimitive>, Sender<Indication>);

pub(crate) struct TestUser {
    internal_tx: Sender<UserPrimitive>,
    internal_rx: Receiver<UserPrimitive>,
    // channel for daemon to indicate a finished transaction
    indication_tx: Sender<Indication>,
    // Indication listener thread
    indication_handle: JoinHandle<()>,
    history: Arc<RwLock<HashMap<TransactionID, Report>>>,
    tokio_handle: tokio::runtime::Handle,
}
impl TestUser {
    pub(crate) fn new<T: FileStore + Send + Sync + 'static>(_filestore: Arc<T>) -> Self {
        let (internal_tx, internal_rx) = mpsc::channel::<UserPrimitive>(1);
        let (indication_tx, mut indication_rx) = mpsc::channel::<Indication>(1000);
        let history = Arc::new(RwLock::new(HashMap::<TransactionID, Report>::new()));

        let auto_history = history.clone();

        let indication_handle = tokio::task::spawn(async move {
            while let Some(indication) = indication_rx.recv().await {
                match indication {
                    Indication::Finished(FinishedIndication {
                        id,
                        report,
                        file_status,
                        delivery_code,
                    }) => {
                        debug!("Finished indication received: id={} status={:?} delivery-code={:?} report={:?}",id, file_status, delivery_code, report);
                    }
                    Indication::Report(report) => {
                        debug!("Received Report indication: {:?}", report);
                        if let Err(e) = auto_history
                            .write()
                            .map(|mut guard| guard.insert(report.id, report.clone()))
                        {
                            error!("Failed to update history: {:?}", e);
                        }
                    }
                    // ignore everything else for now.
                    _ => continue,
                };
            }
        });

        Self {
            internal_tx,
            internal_rx,
            indication_tx,
            indication_handle,
            history,
            tokio_handle: runtime::Handle::current(),
        }
    }

    pub(crate) fn split(self) -> UserSplit {
        let TestUser {
            internal_tx,
            internal_rx,
            indication_tx,
            indication_handle,
            history,
            tokio_handle,
        } = self;
        (
            TestUserHalf {
                internal_tx,
                _indication_handle: indication_handle,
                history,
                tokio_handle,
            },
            internal_rx,
            indication_tx,
        )
    }
}

#[derive(Debug)]
pub struct TestUserHalf {
    internal_tx: Sender<UserPrimitive>,
    _indication_handle: JoinHandle<()>,
    history: Arc<RwLock<HashMap<TransactionID, Report>>>,
    tokio_handle: tokio::runtime::Handle,
}
impl TestUserHalf {
    #[allow(unused)]
    pub fn put(&self, request: PutRequest) -> Result<TransactionID, IoError> {
        self.tokio_handle.block_on(async {
            let (put_send, put_recv) = oneshot::channel();
            let primitive = UserPrimitive::Put(request, put_send);

            self.internal_tx.send(primitive).await.map_err(|_| {
                IoError::new(
                    ErrorKind::ConnectionReset,
                    " 1 Daemon Half of User disconnected.",
                )
            })?;
            put_recv.await.map_err(|_| {
                IoError::new(
                    ErrorKind::ConnectionReset,
                    "Daemon Half of User disconnected.",
                )
            })
        })
    }

    // this function is actually used in series_f1 but series_f2 and f3 generate an unused warning
    // apparently related https://github.com/rust-lang/rust/issues/46379
    #[allow(unused)]
    pub fn cancel(&self, transaction: TransactionID) -> Result<(), IoError> {
        self.tokio_handle.block_on(async {
            let primitive = UserPrimitive::Cancel(transaction);
            self.internal_tx.send(primitive).await.map_err(|_| {
                IoError::new(
                    ErrorKind::ConnectionReset,
                    "Daemon Half of User disconnected.",
                )
            })
        })
    }

    #[allow(unused)]
    pub fn report(&self, transaction: TransactionID) -> Result<Option<Report>, IoError> {
        self.tokio_handle.block_on(async {
            let (report_tx, report_rx) = oneshot::channel();
            let primitive = UserPrimitive::Report(transaction, report_tx);

            self.internal_tx.send(primitive).await.map_err(|err| {
                IoError::new(
                    ErrorKind::ConnectionReset,
                    format!("Daemon Half of User disconnected on send: {err}"),
                )
            })?;
            let response = match report_rx.await {
                Ok(report) => Some(report),
                // if the channel disconnects because the transaction is finished then just get from history.
                Err(_) => self.history.read().unwrap().get(&transaction).cloned(),
            };
            Ok(response)
        })
    }

    #[allow(unused)]
    pub fn history(
        &self,
    ) -> Result<Vec<Report>, PoisonError<RwLockReadGuard<'_, HashMap<TransactionID, Report>>>> {
        self.history.read().map(|history| {
            let mut histories: Vec<Report> = Vec::with_capacity(history.len());
            histories.extend(history.values().cloned());
            histories
        })
    }
}

impl<'a, T> Drop for JoD<'a, T> {
    fn drop(&mut self) {
        for handle in self.handle.drain(..) {
            handle.abort();
        }
    }
}

// Returns the local user, remote user, and handles for local and remote daemons.
type DaemonType = (
    TestUserHalf,
    TestUserHalf,
    JoD<'static, Result<(), String>>,
    JoD<'static, Result<(), String>>,
);

// Inactivity, EOF, NAK, Progress Interval
type Timeouts = [Option<i64>; 4];

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_daemons<T: FileStore + Sync + Send + 'static>(
    filestore: Arc<T>,
    local_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>>,
    remote_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>>,
    timeouts: Timeouts,
    config: Option<EntityConfig>,
) -> DaemonType {
    let config = config.unwrap_or(EntityConfig {
        fault_handler_override: HashMap::from([(
            Condition::PositiveLimitReached,
            FaultHandlerAction::Abandon,
        )]),
        file_size_segment: 1024,
        default_transaction_max_count: 2,
        inactivity_timeout: timeouts[0].unwrap_or(1),
        eof_timeout: timeouts[1].unwrap_or(1),
        nak_timeout: timeouts[2].unwrap_or(1),
        crc_flag: CRCFlag::NotPresent,
        checksum_type: ChecksumType::Modular,
        nak_procedure: NakProcedure::Deferred(Duration::ZERO),
        local_entity_id: 0_u16,
        remote_entity_id: 1_u16,
        local_server_addr: "127.0.0.1:0",
        remote_server_addr: "127.0.0.1:0",
        progress_report_interval_secs: timeouts[3].unwrap_or(1),
    });

    let remote_config = HashMap::from([
        (EntityID::from(0_u16), config.clone()),
        (EntityID::from(1_u16), config.clone()),
    ]);

    let local_filestore = filestore.clone();

    let local_user = TestUser::new(local_filestore.clone());
    let (local_userhalf, local_daemonhalf, indication_tx) = local_user.split();

    let mut local_daemon = Daemon::new(
        EntityID::from(0_u16),
        TransactionSeqNum::from(0_u32),
        local_transport_map,
        local_filestore,
        remote_config.clone(),
        config.clone(),
        local_daemonhalf,
        indication_tx,
    );

    let local_handle = tokio::task::spawn(async move {
        local_daemon
            .manage_transactions()
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    });

    let remote_filestore = filestore;
    let remote_user = TestUser::new(remote_filestore.clone());
    let (remote_userhalf, remote_daemonhalf, remote_indication_tx) = remote_user.split();

    let mut remote_daemon = Daemon::new(
        EntityID::from(1_u16),
        TransactionSeqNum::from(0_u32),
        remote_transport_map,
        remote_filestore,
        remote_config,
        config,
        remote_daemonhalf,
        remote_indication_tx,
    );

    let remote_handle = tokio::task::spawn(async move {
        remote_daemon
            .manage_transactions()
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    });

    let _local_h = JoD::from(local_handle);
    let _remote_h: JoD<_> = JoD::from(remote_handle);

    (local_userhalf, remote_userhalf, _local_h, _remote_h)
}

pub struct StaticAssets {
    //we need to keep the object here because the directory is removed as soon as the object is dropped
    _tempdir: TempDir,
    pub filestore: Arc<NativeFileStore>,
    tokio_runtime: tokio::runtime::Runtime,
}

#[fixture]
#[once]
pub fn static_assets() -> StaticAssets {
    let tempdir = TempDir::new().unwrap();
    let utf8_path = Utf8PathBuf::from(
        tempdir
            .path()
            .as_os_str()
            .to_str()
            .expect("Unable to coerce tmp path to String."),
    );

    let filestore = Arc::new(NativeFileStore::new(&utf8_path));
    filestore
        .create_directory("local")
        .expect("Unable to create local directory.");
    filestore
        .create_directory("remote")
        .expect("Unable to create local directory.");

    let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("resources");
    for filename in ["small.txt", "medium.txt", "large.txt"] {
        fs::copy(
            data_dir.join(filename),
            utf8_path.join("local").join(filename),
        )
        .expect("Unable to copy file.");
    }

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    StaticAssets {
        _tempdir: tempdir,
        filestore,
        tokio_runtime,
    }
}

// Returns the local user, remote user, filestore, and handles for both local and remote daemons.
pub(crate) type EntityConstructorReturn = (
    TestUserHalf,
    TestUserHalf,
    Arc<NativeFileStore>,
    JoD<'static, Result<(), String>>,
    JoD<'static, Result<(), String>>,
);

pub(crate) fn new_entities(
    static_assets: &StaticAssets,
    local_transport_issue: Option<TransportIssue>,
    remote_transport_issue: Option<TransportIssue>,
    timeouts: Timeouts,
    config: Option<EntityConfig>,
) -> EntityConstructorReturn {
    let (local_user, remote_user, local_handle, remote_handle) =
        static_assets.tokio_runtime.block_on(async move {
            let remote_udp = UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("Unable to bind remote UDP.");
            let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

            let local_udp = UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("Unable to bind local UDP.");
            let local_addr = local_udp.local_addr().expect("Cannot find local address.");

            let entity_map = HashMap::from([
                (EntityID::from(0_u16), local_addr),
                (EntityID::from(1_u16), remote_addr),
            ]);

            let local_transport = if let Some(issue) = local_transport_issue {
                Box::new(
                    LossyTransport::try_from((local_udp, entity_map.clone(), issue))
                        .expect("Unable to make Lossy Transport."),
                ) as Box<dyn PDUTransport + Send>
            } else {
                Box::new(
                    UdpTransport::try_from((local_udp, entity_map.clone()))
                        .expect("Unable to make UDP Transport."),
                ) as Box<dyn PDUTransport + Send>
            };

            let remote_transport = if let Some(issue) = remote_transport_issue {
                Box::new(
                    LossyTransport::try_from((remote_udp, entity_map.clone(), issue))
                        .expect("Unable to make Lossy Transport."),
                ) as Box<dyn PDUTransport + Send>
            } else {
                Box::new(
                    UdpTransport::try_from((remote_udp, entity_map.clone()))
                        .expect("Unable to make UDP Transport."),
                ) as Box<dyn PDUTransport + Send>
            };

            let remote_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
                HashMap::from([(vec![EntityID::from(0_u16)], remote_transport)]);

            let local_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
                HashMap::from([(vec![EntityID::from(1_u16)], local_transport)]);

            create_daemons(
                static_assets.filestore.clone(),
                local_transport_map,
                remote_transport_map,
                timeouts,
                config,
            )
            .await
        });

    (
        local_user,
        remote_user,
        static_assets.filestore.clone(),
        local_handle,
        remote_handle,
    )
}

#[fixture]
#[once]
fn make_entities(static_assets: &StaticAssets) -> EntityConstructorReturn {
    // Default entity test fixture
    new_entities(static_assets, None, None, [None; 4], None)
}

pub(crate) type UsersAndFilestore = (
    &'static TestUserHalf,
    &'static TestUserHalf,
    Arc<NativeFileStore>,
);
#[fixture]
#[once]
pub(crate) fn get_filestore(make_entities: &'static EntityConstructorReturn) -> UsersAndFilestore {
    (&make_entities.0, &make_entities.1, make_entities.2.clone())
}

#[allow(dead_code)]
pub(crate) enum TransportIssue {
    // Every Nth packet will be dropped
    Rate(usize),
    // Every Nth packet will be duplicated
    Duplicate(usize),
    // Stores eveyt Nth  PDU for sending out of order
    Reorder(usize),
    // This specific PDU is dropped the first time it is sent.
    Once(PDUDirective),
    // This PDU type is dropped every time,
    All(Vec<PDUDirective>),
    // Every single PDU should be dropped once.
    // except for EoF
    Every,
    // Recreates inactivity at sender
    Inactivity,
}
pub(crate) struct LossyTransport {
    pub(crate) socket: UdpSocket,
    entity_map: HashMap<EntityID, SocketAddr>,
    counter: usize,
    issue: TransportIssue,
    buffer: Vec<PDU>,
}
impl LossyTransport {
    #[allow(dead_code)]
    pub async fn new<T: ToSocketAddrs>(
        addr: T,
        entity_map: HashMap<EntityID, SocketAddr>,
        issue: TransportIssue,
    ) -> Result<Self, IoError> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self {
            socket,
            entity_map,
            counter: 1,
            issue,
            buffer: vec![],
        })
    }
}
impl TryFrom<(UdpSocket, HashMap<EntityID, SocketAddr>, TransportIssue)> for LossyTransport {
    type Error = IoError;

    fn try_from(
        inputs: (UdpSocket, HashMap<EntityID, SocketAddr>, TransportIssue),
    ) -> Result<Self, Self::Error> {
        let me = Self {
            socket: inputs.0,
            entity_map: inputs.1,
            counter: 1,
            issue: inputs.2,
            buffer: vec![],
        };
        Ok(me)
    }
}

#[async_trait]
impl PDUTransport for LossyTransport {
    fn get_apid(&self) -> &'static u11 {
        static CCSDS_APID: OnceLock<u11> = OnceLock::new();
        CCSDS_APID.get_or_init(|| u11!(0))
    }

    async fn request(&mut self, destination: EntityID, pdu: PDU) -> Result<(), IoError> {
        let space_pdu =
            encode_space_packet(pdu.clone().encode().as_ref(), *self.get_apid(), 1u8).unwrap();
        let addr = self
            .entity_map
            .get(&destination)
            .ok_or_else(|| IoError::from(ErrorKind::AddrNotAvailable))?;

        // send a delayed packet if there are any
        if !self.buffer.is_empty() {
            let pdu = self.buffer.remove(0);
            let spdu =
                encode_space_packet(pdu.clone().encode().as_ref(), *self.get_apid(), 1u8).unwrap();
            self.socket.send_to(&spdu, addr).await.map(|_n| ())?;
        }

        match &self.issue {
            TransportIssue::Rate(rate) => {
                if self.counter % rate == 0 {
                    self.counter += 1;
                    Ok(())
                } else {
                    self.counter += 1;
                    self.socket.send_to(&space_pdu, addr).await.map(|_n| ())
                }
            }
            TransportIssue::Duplicate(rate) => {
                if self.counter % rate == 0 {
                    self.counter += 1;
                    self.socket.send_to(&space_pdu, addr).await.map(|_n| ())?;
                    self.socket.send_to(&space_pdu, addr).await.map(|_n| ())
                } else {
                    self.counter += 1;
                    self.socket.send_to(&space_pdu, addr).await.map(|_n| ())
                }
            }
            TransportIssue::Reorder(rate) => {
                if self.counter % rate == 0 {
                    self.counter += 1;
                    self.buffer.push(pdu);
                    Ok(())
                } else {
                    self.counter += 1;
                    self.socket.send_to(&space_pdu, addr).await.map(|_n| ())
                }
            }
            TransportIssue::Once(skip_directive) => match &pdu.payload {
                PDUPayload::Directive(operation) => {
                    if self.counter == 1 && operation.get_directive() == *skip_directive {
                        self.counter += 1;
                        Ok(())
                    } else {
                        self.socket.send_to(&space_pdu, addr).await.map(|_n| ())
                    }
                }
                PDUPayload::FileData(_data) => {
                    self.socket.send_to(&space_pdu, addr).await.map(|_n| ())
                }
            },
            TransportIssue::All(skip_directive) => match &pdu.payload {
                PDUPayload::Directive(operation) => {
                    if skip_directive.contains(&operation.get_directive()) {
                        debug!(
                            "Skipping send for PDU directive: {:?}",
                            operation.get_directive()
                        );
                        Ok(())
                    } else {
                        self.socket.send_to(&space_pdu, addr).await.map(|_n| ())
                    }
                }
                PDUPayload::FileData(_data) => {
                    self.socket.send_to(&space_pdu, addr).await.map(|_n| ())
                }
            },
            // only drop the PDUs if we have not yet send EoF.
            // Flip the counter on EoF to signify we can send again.
            TransportIssue::Every => match &pdu.payload {
                PDUPayload::Directive(operation) => {
                    match (self.counter, operation.get_directive()) {
                        (1, PDUDirective::EoF) => {
                            self.counter += 1;
                            self.socket.send_to(&space_pdu, addr).await.map(|_n| ())
                        }
                        (1, PDUDirective::Nak) => {
                            self.counter += 1;
                            // increment counter but still don't send it
                            Ok(())
                        }
                        (1, _) => Ok(()),
                        (_, _) => self.socket.send_to(&space_pdu, addr).await.map(|_n| ()),
                    }
                }
                PDUPayload::FileData(_data) => {
                    if self.counter == 1 {
                        Ok(())
                    } else {
                        self.socket.send_to(&space_pdu, addr).await.map(|_n| ())
                    }
                }
            },
            TransportIssue::Inactivity => {
                // Send the Metadata PDU only, and nothing else.
                if self.counter == 1 {
                    self.counter += 1;
                    self.socket.send_to(&space_pdu, addr).await.map(|_n| ())
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn pdu_handler(
        &mut self,
        signal: Arc<AtomicBool>,
        sender: Sender<PDU>,
        mut recv: Receiver<(EntityID, PDU)>,
    ) -> Result<(), IoError> {
        while !signal.load(Ordering::Relaxed) {
            // this buffer will be 511 KiB, should be sufficiently small;
            let mut buffer = vec![0_u8; u16::MAX as usize];
            tokio::select! {
                Ok((_n, _addr)) = self.socket.recv_from(&mut buffer) => {
                    // Drain the CCSDS headers
                    buffer.drain(0..CCSDS_HEADER_LENGTH + CCSDS_SECONDARY_HEADER_LENGTH);
                    match PDU::decode(&mut buffer.as_slice()) {
                        Ok(pdu) => {
                            match sender.send(pdu).await {
                                Ok(()) => {}
                                Err(error) => {
                                    error!("Channel to daemon severed: {}", error);
                                    return Err(IoError::from(ErrorKind::ConnectionAborted));
                                }
                            };
                        }
                        Err(error) => {
                            error!("Error decoding PDU: {}", error);
                            // might need to stop depending on the error.
                            // some are recoverable though
                        }
                    }
                },
                Some((entity, pdu)) = recv.recv() => {
                    self.request(entity, pdu).await?;
                },
                else => {
                    log::info!("UdpSocket or Channel disconnected");
                    break
                }
            }
        }
        Ok(())
    }
}
