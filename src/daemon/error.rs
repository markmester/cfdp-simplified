use thiserror::Error;

use crate::{filestore::FileStoreError, transaction::TransactionID};
use tokio::sync::mpsc::error::SendError;

use super::Command;

pub type DaemonResult<T> = Result<T, DaemonError>;
#[derive(Error, Debug)]
pub enum DaemonError {
    #[error("Error Spawning Send Transaction resulting from FileStore error: {0:}")]
    SpawnSend(FileStoreError),

    #[error("Error sending Command to Transaction {0}: {1}")]
    TransactionCommunication(TransactionID, Command),

    #[error(
        "Unable to initialize Send transaction {0}.
Received a PDU that identified this entity as the Sender.
Not enough information to resume the transaction. "
    )]
    UnableToResume(TransactionID),
}
impl From<(TransactionID, SendError<Command>)> for DaemonError {
    fn from(value: (TransactionID, SendError<Command>)) -> Self {
        Self::TransactionCommunication(value.0, value.1 .0)
    }
}
