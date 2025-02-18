pub mod error;
pub mod fault_handler;
pub mod recv;
pub mod send;

pub use fault_handler::*;

use std::{collections::HashMap, fmt};

use camino::Utf8PathBuf;
use error::TransactionError;
use num_derive::FromPrimitive;
use serde::Serialize;

use crate::{
    filestore::ChecksumType,
    pdu::header::{CRCFlag, Condition, FileSizeFlag, SegmentedData, TransmissionMode},
    pdu::ops::{EntityID, TransactionSeqNum},
};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct TransactionID(pub EntityID, pub TransactionSeqNum);

pub type TransactionResult<T> = Result<T, TransactionError>;

impl fmt::Display for TransactionID {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.0.to_u64(), self.1.to_u64())
    }
}

impl TransactionID {
    pub fn from(entity_id: EntityID, seq_num: TransactionSeqNum) -> Self {
        Self(entity_id, seq_num)
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, FromPrimitive, Serialize)]
pub enum TransactionState {
    Active,
    Suspended,
    Terminated,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Metadata {
    /// Bytes of the source filename, can be null if length is 0.
    pub source_filename: Utf8PathBuf,
    /// Bytes of the destination filename, can be null if length is 0.
    pub destination_filename: Utf8PathBuf,
    /// The size of the file being transfered in this transaction.
    pub file_size: u64,
    /// Flag to track what kind of [Checksum](crate::filestore::ChecksumType) will be used in this transaction.
    pub checksum_type: ChecksumType,
}

#[derive(Clone)]
pub struct TransactionConfig {
    /// Identification number of the source (this) entity.
    pub source_entity_id: EntityID,
    /// Identification number of the destination (remote) entity.
    pub destination_entity_id: EntityID,
    /// CFDP [TransmissionMode]
    pub transmission_mode: TransmissionMode,
    /// The sequence number in Big Endian Bytes.
    pub sequence_number: TransactionSeqNum,
    /// Flag to indicate whether or not the file size fits inside a [u32]
    pub file_size_flag: FileSizeFlag,
    /// A Mapping of actions to take when each condition is reached.
    /// See also [Condition] and [FaultHandlerAction].
    pub fault_handler_override: HashMap<Condition, FaultHandlerAction>,
    /// The maximum length a file segment sent to Destination can be.
    /// u16 will be larger than any possible CCSDS packet size.
    pub file_size_segment: u16,
    /// Flag indicating whether or not CRCs will be present on the PDUs
    pub crc_flag: CRCFlag,
    /// Flag indicating whether file metadata is included with FileData
    pub segment_metadata_flag: SegmentedData,
    /// Maximum count of timeouts on a timer before a fault is generated.
    pub max_count: u32,
    /// Maximum amount timeof without activity before the inactivity timer increments its count.
    pub inactivity_timeout: i64,
    /// Maximum amount timeof without activity before the NAK timer increments its count.
    pub nak_timeout: i64,
    /// Maximum amount time of without activity before the EOF timer increments its count.
    pub eof_timeout: i64,
    /// Interval in secs to send a progress report.
    pub progress_report_interval_secs: i64,
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    use crate::{
        pdu::header::{CRCFlag, FileSizeFlag, SegmentedData, TransmissionMode},
        transaction::TransactionConfig,
    };

    use rstest::fixture;

    #[fixture]
    #[once]
    pub(crate) fn default_config() -> TransactionConfig {
        TransactionConfig {
            source_entity_id: EntityID::from(12_u16),
            destination_entity_id: EntityID::from(15_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            sequence_number: TransactionSeqNum::from(3_u32),
            file_size_flag: FileSizeFlag::Small,
            fault_handler_override: HashMap::new(),
            file_size_segment: 16630_u16,
            crc_flag: CRCFlag::NotPresent,
            segment_metadata_flag: SegmentedData::NotPresent,
            max_count: 5_u32,
            inactivity_timeout: 300_i64,
            eof_timeout: 300_i64,
            nak_timeout: 300_i64,
            progress_report_interval_secs: 300_i64,
        }
    }

    #[test]
    fn id() {
        let id = TransactionID::from(EntityID::from(13_u16), TransactionSeqNum::from(541_u32));
        assert_eq!(
            TransactionID(EntityID::from(13), TransactionSeqNum::from(541)),
            id,
        )
    }
}
