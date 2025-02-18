use std::num::TryFromIntError;

use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

use crate::filestore::FileStoreError;
use crate::pdu::EntityID;
use crate::pdu::{header::TransmissionMode, ops::TransactionSeqNum, PDU};

use super::TransactionID;

#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("FileStore error during Transaction: {0}")]
    FileStore(#[from] Box<FileStoreError>),

    #[error("Error Communicating with transport: {0}")]
    Transport(#[from] Box<SendError<(EntityID, PDU)>>),

    #[error("No open file in transaction: {0:?}")]
    NoFile(TransactionID),

    #[error("Error converting from integer: {0}")]
    IntConversion(#[from] TryFromIntError),

    #[error("Transaction (ID: {0:?}, Mode: {1:?}) received unexpected PDU {2:}.")]
    UnexpectedPDU(TransactionSeqNum, TransmissionMode, String),

    #[error("Metadata missing for transaction: {0:?}.")]
    MissingMetadata(TransactionID),

    #[error("No Checksum received. Cannot verify file integrity.")]
    NoChecksum,
}
impl From<FileStoreError> for TransactionError {
    fn from(error: FileStoreError) -> Self {
        Self::FileStore(Box::new(error))
    }
}
impl From<SendError<(EntityID, PDU)>> for TransactionError {
    fn from(error: SendError<(EntityID, PDU)>) -> Self {
        Self::Transport(Box::new(error))
    }
}
