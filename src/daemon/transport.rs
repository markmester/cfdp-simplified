use std::{
    collections::HashMap,
    env::var,
    fmt::Debug,
    io::{Error as IoError, ErrorKind},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, OnceLock,
    },
};

use async_trait::async_trait;
use bit_struct::u11;
use log::{error, warn};
use tokio::{
    net::{ToSocketAddrs, UdpSocket},
    sync::mpsc::{Receiver, Sender},
};
use tracing::trace;

use crate::{
    ccsds::{constants::*, decode_ccsds_header, encode_space_packet},
    pdu::{EntityID, PDUEncode, PDU},
};

/// Transports are designed to run in a thread in the background
/// inside a [Daemon](crate::Daemon) process
#[async_trait]
pub trait PDUTransport {
    /// Send input PDU to the remote
    /// The implementation must have a method to lookup an Entity's address from the ID
    async fn request(&mut self, destination: EntityID, pdu: PDU) -> Result<(), IoError>;

    /// Provides logic for listening for incoming PDUs and sending any outbound PDUs
    /// A transport implementation will send any received messages through the
    /// [Sender] channel to the [Daemon](crate::Daemon).
    /// The [Receiver] channel is used to recv PDUs from the Daemon and send them to their respective remote Entity.
    /// The [Daemon](crate::Daemon) is responsible for receiving messages and distribute them to each
    /// transaction [Send](crate::transaction::SendTransaction) or [Recv](crate::transaction::RecvTransaction)
    /// The signal is used to indicate a shutdown operation was requested.
    async fn pdu_handler(
        &mut self,
        signal: Arc<AtomicBool>,
        sender: Sender<PDU>,
        mut recv: Receiver<(EntityID, PDU)>,
    ) -> Result<(), IoError>;

    fn get_apid(&self) -> &'static u11;
}

/// A wrapper struct around a [UdpSocket] and a Mapping from
/// EntityIDs to [SocketAddr] instances.
pub struct UdpTransport {
    socket: UdpSocket,
    entity_map: HashMap<EntityID, SocketAddr>,
}
impl UdpTransport {
    pub async fn new<T: ToSocketAddrs + Debug>(
        addr: T,
        entity_map: HashMap<EntityID, SocketAddr>,
    ) -> Result<Self, IoError> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self { socket, entity_map })
    }
}
impl TryFrom<(UdpSocket, HashMap<EntityID, SocketAddr>)> for UdpTransport {
    type Error = IoError;
    fn try_from(inputs: (UdpSocket, HashMap<EntityID, SocketAddr>)) -> Result<Self, Self::Error> {
        let me = Self {
            socket: inputs.0,
            entity_map: inputs.1,
        };
        Ok(me)
    }
}

#[async_trait]
impl PDUTransport for UdpTransport {
    fn get_apid(&self) -> &'static u11 {
        static CCSDS_APID: OnceLock<u11> = OnceLock::new();
        CCSDS_APID.get_or_init(|| {
            var(OUTGOING_CCSDS_APID_ENV_VAR)
                .map(|s| s.parse::<u11>().unwrap_or(u11!(0)))
                .unwrap_or(u11!(0))
        })
    }
    async fn request(&mut self, destination: EntityID, pdu: PDU) -> Result<(), IoError> {
        let addr = self
            .entity_map
            .get(&destination)
            .ok_or_else(|| IoError::from(ErrorKind::AddrNotAvailable))?;
        let pdu = pdu.encode();

        match encode_space_packet(pdu.as_ref(), *self.get_apid(), 1u8) {
            Ok(space_pdu) => {
                self.socket.send_to(space_pdu.as_slice(), addr).await?;
            }
            Err(err) => {
                error!("Error encoding space packet: {}", err);
                return Err(IoError::from(ErrorKind::InvalidData));
            }
        }
        Ok(())
    }

    async fn pdu_handler(
        &mut self,
        signal: Arc<AtomicBool>,
        sender: Sender<PDU>,
        mut recv: Receiver<(EntityID, PDU)>,
    ) -> Result<(), IoError> {
        while !signal.load(Ordering::Relaxed) {
            {
                // this buffer will be 511 KiB, should be sufficiently small;
                let mut buffer = vec![0_u8; u16::MAX as usize];
                tokio::select! {
                    Ok((_n, _addr)) = self.socket.recv_from(&mut buffer) => {
                        // Only expecting CCSDS encoded packets with primary and secondary headers
                        if buffer.len() < CCSDS_HEADER_LENGTH + CCSDS_SECONDARY_HEADER_LENGTH {
                            warn!("Received packet without CCSDS primary and secondary headers; skipping packet");
                            continue;
                        }
                        match decode_ccsds_header(buffer.as_slice()) {
                            Err(err) => error!("Error decoding CCSDS headers: {}", err),
                            Ok((_primary_hdr, _secondary_hdr)) => {
                                // TODO: Do something with the decoded CCSDS headers
                                trace!("Primary header: {:?} Secondary header: {:?}", _primary_hdr, _secondary_hdr);
                            }
                        }
                       // Drain the CCSDS headers
                        buffer.drain(0..CCSDS_HEADER_LENGTH + CCSDS_SECONDARY_HEADER_LENGTH);

                        match PDU::decode(&mut buffer.as_slice()) {
                            Ok(pdu) => {
                                match sender.send(pdu.clone()).await {
                                    Ok(()) => {},
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
        }
        Ok(())
    }
}
