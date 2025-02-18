use std::{
    collections::HashMap,
    io::{Error as IoError, ErrorKind},
    marker::PhantomData,
    str::FromStr,
    sync::{Arc, PoisonError, RwLock, RwLockReadGuard},
    time::Duration,
};

use camino::Utf8PathBuf;
use log::{debug, error, info};
use tokio::{
    runtime::{self},
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};

use super::{transport::PDUTransport, Daemon};
use crate::{
    daemon::{EntityConfig, Indication, PutRequest, Report, UserPrimitive},
    filestore::{FileStore, NativeFileStore},
    pdu::{EntityID, TransactionSeqNum},
    transaction::TransactionID,
};

#[derive(Debug)]
pub struct JoD<'a, T> {
    pub handle: Vec<JoinHandle<T>>,
    phantom: PhantomData<&'a ()>,
}
#[allow(clippy::needless_lifetimes)]
impl<'a, T> From<JoinHandle<T>> for JoD<'a, T> {
    fn from(input: JoinHandle<T>) -> Self {
        Self {
            handle: vec![input],
            phantom: PhantomData,
        }
    }
}

#[allow(clippy::needless_lifetimes)]
impl<'a, T> Drop for JoD<'a, T> {
    fn drop(&mut self) {
        for handle in self.handle.drain(..) {
            handle.abort();
        }
    }
}

pub struct UserHalf {
    internal_tx: Sender<UserPrimitive>,
    _indication_handle: JoinHandle<()>,
    history: Arc<RwLock<HashMap<TransactionID, Report>>>,
    tokio_handle: tokio::runtime::Handle,
}

impl UserHalf {
    #[allow(unused)]
    pub async fn put(&self, request: PutRequest) -> Result<TransactionID, IoError> {
        let (put_send, put_recv) = oneshot::channel();
        let primitive = UserPrimitive::Put(request, put_send);

        self.internal_tx
            .send_timeout(primitive, Duration::from_secs(1))
            .await
            .map_err(|_| {
                IoError::new(
                    ErrorKind::ConnectionReset,
                    "1 Daemon Half of User disconnected.",
                )
            })?;
        put_recv.await.map_err(|_| {
            IoError::new(
                ErrorKind::ConnectionReset,
                "1 Daemon Half of User disconnected.",
            )
        })
    }

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
    pub async fn report(&self, transaction: TransactionID) -> Result<Option<Report>, IoError> {
        let (report_tx, mut report_rx) = oneshot::channel();
        let primitive = UserPrimitive::Report(transaction, report_tx);

        self.internal_tx
            .send_timeout(primitive, Duration::from_secs(1))
            .await
            .map_err(|err| {
                IoError::new(
                    ErrorKind::ConnectionReset,
                    format!("Daemon Half of User disconnected on send: {err}"),
                )
            })?;
        let response = match report_rx.try_recv() {
            Ok(report) => Some(report),
            // if the channel disconnects because the transaction is finished then just get from history.
            Err(_) => self.history.read().unwrap().get(&transaction).cloned(),
        };
        Ok(response)
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

    #[allow(unused)]
    pub fn get_local_report(&self, transaction: TransactionID) -> Result<Option<Report>, IoError> {
        self.history
            .read()
            .map(|history| history.get(&transaction).cloned())
            .map_err(|e| IoError::new(ErrorKind::Other, e.to_string()))
    }
}

pub struct StaticAssets {
    pub filestore: Arc<NativeFileStore>,
    _tokio_runtime: tokio::runtime::Runtime,
}

pub fn static_assets(filestore_path: &str) -> StaticAssets {
    let utf8_path = Utf8PathBuf::from_str(filestore_path).unwrap();
    let filestore = Arc::new(NativeFileStore::new(&utf8_path));
    let _tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    StaticAssets {
        filestore,
        _tokio_runtime,
    }
}

type UserSplit = (UserHalf, Receiver<UserPrimitive>, Sender<Indication>);

#[async_trait::async_trait]
pub trait IndicationHandler {
    async fn handle(&self, indication: Indication) -> anyhow::Result<Indication>;
}

struct User {
    internal_tx: Sender<UserPrimitive>,
    internal_rx: Receiver<UserPrimitive>,
    // channel for daemon to indicate a finished transaction
    indication_tx: Sender<Indication>,
    // Indication listener thread
    indication_handle: JoinHandle<()>,
    history: Arc<RwLock<HashMap<TransactionID, Report>>>,
    tokio_handle: tokio::runtime::Handle,
}
impl User {
    pub fn new<
        T: FileStore + Send + Sync + 'static,
        I: IndicationHandler + Send + Sync + 'static,
    >(
        _filestore: Arc<T>,
        indication_handler: Arc<I>,
    ) -> Self {
        let (internal_tx, internal_rx) = mpsc::channel::<UserPrimitive>(1);
        let (indication_tx, mut indication_rx) = mpsc::channel::<Indication>(1000);
        let history = Arc::new(RwLock::new(HashMap::<TransactionID, Report>::new()));
        let auto_history = Arc::clone(&history);

        let indication_handle = tokio::task::spawn(async move {
            while let Some(indication) = indication_rx.recv().await {
                match indication_handler.handle(indication).await {
                    Ok(indication) => {
                        debug!("Received indication: {:?}", indication);
                        if let Indication::Report(report) = &indication {
                            debug!("Received Report indication: {:?}", report);
                            if let Err(e) = auto_history
                                .write()
                                .map(|mut guard| guard.insert(report.id, report.clone()))
                            {
                                error!("Failed to update history: {:?}", e);
                            }
                        }
                    }
                    _ => continue,
                }
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

    fn split(self) -> UserSplit {
        let User {
            internal_tx,
            internal_rx,
            indication_tx,
            indication_handle,
            history,
            tokio_handle,
        } = self;
        (
            UserHalf {
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

#[allow(clippy::too_many_arguments)]
pub async fn create_daemon<
    T: FileStore + Sync + Send + 'static,
    I: IndicationHandler + Sync + Send + 'static,
>(
    filestore: Arc<T>,
    indication_handler: Arc<I>,
    transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>>,
    entity_config: EntityConfig,
) -> (UserHalf, JoD<'static, Result<(), String>>) {
    let filestore = filestore.clone();
    let user = User::new(filestore.clone(), indication_handler);
    let (userhalf, daemonhalf, indication_tx) = user.split();

    let mut daemon = Daemon::new(
        EntityID::from(entity_config.local_entity_id),
        TransactionSeqNum::from(0_u32),
        transport_map,
        filestore,
        HashMap::from([
            (
                EntityID::from(entity_config.local_entity_id),
                entity_config.clone(),
            ),
            (
                EntityID::from(entity_config.remote_entity_id),
                entity_config.clone(),
            ),
        ]),
        entity_config.clone(),
        daemonhalf,
        indication_tx,
    );

    let handle = tokio::task::spawn(async move {
        daemon
            .manage_transactions()
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    });
    info!("Daemon created.");
    (userhalf, JoD::from(handle))
}
