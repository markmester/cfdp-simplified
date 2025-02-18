mod fifo;

use fifo::Fifo;
use tracing_subscriber::EnvFilter;

use anyhow::Context;
use camino::Utf8PathBuf;
use cfdp_simplified::{
    daemon::{
        create_daemon,
        transport::{PDUTransport, UdpTransport},
        EntityConfig, Indication, IndicationHandler, NakProcedure, PutRequest, Report,
    },
    filestore::{ChecksumType, FileStore, NativeFileStore},
    pdu::{CRCFlag, Condition, EntityID, TransmissionMode},
    transaction::FaultHandlerAction,
};
use clap::Parser;
use log::{error, info};
use tokio::net::UdpSocket;

use std::{
    collections::HashMap,
    error::Error,
    fs::copy,
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    sync::mpsc::{self, Receiver, Sender},
    sync::Arc,
    thread,
    time::Duration,
};

pub struct ExternalIndicationHandler {}
#[async_trait::async_trait]
impl IndicationHandler for ExternalIndicationHandler {
    async fn handle(&self, indication: Indication) -> anyhow::Result<Indication> {
        info!("Received indication: {:?}", indication);
        Ok(indication)
    }
}

fn main() {
    if let Err(e) = try_main() {
        error!("Unexpected error running CFDP server: {}", e);
        if format!("{}", e)
            .to_lowercase()
            .contains("no such file or directory")
        {
            info!("Does your filestore directory exist?");
        }
        std::process::exit(1);
    }
}

#[tokio::main]
async fn try_main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_env("LOG_LEVEL"))
        .init();

    let args = Args::parse();
    let filestore_path: &str = args.filestore_path.as_str();

    let entity_config = EntityConfig {
        fault_handler_override: HashMap::from([(
            Condition::PositiveLimitReached,
            FaultHandlerAction::Abandon,
        )]),
        file_size_segment: 1024,
        default_transaction_max_count: 2,
        inactivity_timeout: args.inactivity_timeout,
        eof_timeout: args.eof_timeout,
        nak_timeout: args.nak_timeout,
        crc_flag: CRCFlag::NotPresent,
        checksum_type: ChecksumType::Modular,
        nak_procedure: NakProcedure::Deferred(Duration::ZERO),
        local_entity_id: args.local_entity_id,
        remote_entity_id: args.remote_entity_id,
        local_server_addr: args.local_server_addr.leak(),
        remote_server_addr: args.remote_server_addr.leak(),
        progress_report_interval_secs: args.progress_report_interval_secs,
    };

    let utf8_path =
        Utf8PathBuf::from_str(filestore_path).context("Unable to parse filestore path")?;
    let filestore = Arc::new(NativeFileStore::new(&utf8_path));
    info!("Starting server at: {}", entity_config.local_server_addr);
    let udp = UdpSocket::bind(entity_config.local_server_addr)
        .await
        .context("Unable to bind local UDP.")?;
    info!("Server started.");
    let addr = udp.local_addr().context("Cannot find local address.")?;
    let remote_addr = SocketAddr::from_str(entity_config.remote_server_addr)
        .context("Unable to parse remote server address")?;

    let entity_map = HashMap::from([
        (EntityID::from(entity_config.local_entity_id), addr),
        (EntityID::from(entity_config.remote_entity_id), remote_addr),
    ]);

    let transport = Box::new(
        UdpTransport::try_from((udp, entity_map.clone()))
            .context("Unable to make UDP Transport.")?,
    ) as Box<dyn PDUTransport + Send>;
    info!("Transport created.");
    let transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> = HashMap::from([(
        vec![EntityID::from(entity_config.remote_entity_id)],
        transport,
    )]);

    let handler = Arc::new(ExternalIndicationHandler {});
    let (user, _daemon) = create_daemon(
        Arc::clone(&filestore),
        handler,
        transport_map,
        entity_config,
    )
    .await;

    let (tx, rx): (Sender<SmdpRequest>, Receiver<SmdpRequest>) = mpsc::channel();

    let fifo = Fifo::new(PathBuf::from(args.fifo_path))?;
    // TODO: Graceful shutdown
    let _fifo_listener_handle = thread::spawn(move || -> Result<(), std::io::Error> {
        fifo.listen(tx)?;
        Ok(())
    });

    while let Ok(request) = rx.recv() {
        match request {
            SmdpRequest::Transfer(fifo_filename) => {
                info!("Transfer file: {:?}", fifo_filename);

                let fifo_file = Utf8PathBuf::from(&fifo_filename);
                let stripped_filename = fifo_file.file_name().context("Fifo filename not found")?;

                let staged_source_file = format!(
                    "{}/{}",
                    &filestore.get_native_path(&args.filestore_path),
                    stripped_filename
                );

                // Copy fifo file to local filestore
                if let Err(e) = copy(&fifo_file, &staged_source_file) {
                    error!("Failed to copy file: {:?}", e);
                }

                // Check to append destination filename suffix
                let mut dest_filename = Utf8PathBuf::from(stripped_filename);
                if let Some(suffix) = args.dest_file_suffix.as_ref() {
                    dest_filename = Utf8PathBuf::from(format!("{}.{}", stripped_filename, suffix));
                };

                let stage_file = Utf8PathBuf::from(&staged_source_file);
                match user
                    .put(PutRequest {
                        source_filename: stage_file,
                        destination_filename: dest_filename,
                        destination_entity_id: EntityID::from(args.remote_entity_id),
                        transmission_mode: TransmissionMode::Acknowledged,
                    })
                    .await
                {
                    Ok(_) => {
                        info!("Transaction:")
                    }
                    Err(e) => {
                        error!("Failed to put file: {:?}", e);
                    }
                }
            }
            _ => unimplemented!(),
        }
    }

    Ok(())
}

pub enum SmdpRequest {
    List,
    Health,
    Transfer(String),
}
#[derive(Clone, Debug)]
pub struct SmdpResponse {
    pub status: u16,
    pub data: Vec<Report>,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, help = "Local entity id")]
    pub local_entity_id: u16,
    #[arg(short, long, help = "Destination entity id")]
    pub remote_entity_id: u16,
    #[arg(long, help = "Local entity address. (Format: <ip>:<port>)")]
    pub local_server_addr: String,
    #[arg(long, help = "Destination entity address. (Format: <ip>:<port>")]
    pub remote_server_addr: String,
    #[arg(long, default_value_t=String::from("/tmp/cfdp/fifo"))]
    pub fifo_path: String,
    #[arg(long, default_value_t=String::from("/tmp/cfdp/transfers"), help = "Local filestore. (Default: /tmp/cfdp/transfers)")]
    pub filestore_path: String,
    #[arg(
        long,
        default_value_t = 10,
        help = "Transaction inactivity timeout in seconds."
    )]
    pub inactivity_timeout: i64,
    #[arg(
        long,
        default_value_t = 10,
        help = "Transaction eof timeout in seconds."
    )]
    pub eof_timeout: i64,
    #[arg(
        long,
        default_value_t = 10,
        help = "Transaction nak timeout in seconds."
    )]
    pub nak_timeout: i64,
    /// suffix for append to destination filename.
    /// Useful for debugging 2 servers using the same filestore on localhost.
    #[arg(
        long,
        help = "Suffix appended to destination filename. [default: None]"
    )]
    pub dest_file_suffix: Option<String>,
    #[arg(
        long,
        default_value_t = 10,
        help = "Interval to send progress report in seconds."
    )]
    pub progress_report_interval_secs: i64,
}
