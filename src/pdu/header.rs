use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use serde::Serialize;

use std::fmt;
use std::io::Read;

use super::{
    error::{PDUError, PDUResult},
    ops::{EntityID, TransactionSeqNum},
};

#[repr(u8)]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, FromPrimitive, Serialize)]
/// All possible conditions a transaction may be in
pub enum Condition {
    /// No errors occurred yet during the transaction.
    NoError = 0b0000,
    /// The positive acknowledgement limit has been reached.
    PositiveLimitReached = 0b0001,
    /// The transaction's transmission mode is not valid.
    InvalidTransmissionMode = 0b0011,
    /// The file could not be written by the filestore.
    FileStoreRejection = 0b0100,
    /// The final file did not pass the checksum verification.
    FileChecksumFailure = 0b0101,
    /// Received file was a different size than expected.
    FilesizeError = 0b0110,
    /// The negative acknowledgement limit has been reached.
    NakLimitReached = 0b0111,
    /// No activity was detected within the allowed time limit.
    InactivityDetected = 0b1000,
    /// File did not have the correct structure.
    InvalidFileStructure = 0b1001,
    /// The checksum method requested is not supported by this implementation.
    UnsupportedChecksumType = 0b1011,
    /// A command to cancel the transaction has been issued.
    CancelReceived = 0b1111,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
/// A 3 bit integer used in versioning to limit possible values.
pub enum U3 {
    Zero = 0b000,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
/// A flag to differentiate the payalod type of the PDU.
pub enum PDUType {
    /// Payload contains a file directive.
    FileDirective = 0,
    /// Payload contains file data.
    FileData = 1,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
/// The direction in which this PDU is heading.
pub enum Direction {
    ToReceiver = 0,
    ToSender = 1,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
/// The transmission mode of this transaction.
pub enum TransmissionMode {
    Acknowledged = 0,
    Unacknowledged = 1,
}
impl PDUEncode for TransmissionMode {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        1
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let possible_mode = u8_buff[0];
        Self::from_u8(possible_mode).ok_or(PDUError::InvalidTransmissionMode(possible_mode))
    }

    fn encode(self) -> Vec<u8> {
        vec![self as u8]
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
/// Flag determining the presence of a CRC appended to a PDU.
pub enum CRCFlag {
    NotPresent = 0,
    Present = 1,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
/// Flag indicating if the file size is less than the maximum u32 value.
pub enum FileSizeFlag {
    Small = 0,
    Large = 1,
}

impl FileSizeFlag {
    /// returns the size in bytes of the encoded file size (i.e. 4 for small and 8 for large)
    pub fn encoded_len(&self) -> u16 {
        match self {
            FileSizeFlag::Small => 4,
            FileSizeFlag::Large => 8,
        }
    }
}
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
/// Whether segmentation control is implemented on this transaction.
pub enum SegmentationControl {
    NotPreserved = 0,
    // Preserved = 1, // Not supported
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
/// Flag to determine if the file data in this PDU is segmented.
pub enum SegmentedData {
    NotPresent = 0,
    Present = 1,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
/// Final transaction delivery code.
pub enum DeliveryCode {
    Complete = 0,
    Incomplete = 1,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive)]
/// The resulting code of a file being written during a transaction.
pub enum FileStatusCode {
    Discarded = 0b00,
    FileStoreRejection = 0b01,
    Retained = 0b10,
    Unreported = 0b11,
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, FromPrimitive, Serialize)]
/// The current status of a running transaction
pub enum TransactionStatus {
    Undefined = 0b00,
    Active = 0b01,
    Terminated = 0b10,
    Unrecognized = 0b11,
}

/// Provides utility functions for encoding and decoding byte streams
pub trait PDUEncode {
    type PDUType;
    /// Gets the encoded length must fit in a u16 for PDUs
    fn encoded_len(&self) -> u16;

    /// Encodes the PDU to a byte stream
    fn encode(self) -> Vec<u8>;

    /// Attempts to decode a PDU from a byte stream
    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType>;
}

/// Provides utility functions for encoding and decoding byte streams
/// For PDUs which require knowledge of the file size
pub trait FSSEncode {
    type PDUType;

    /// Gets the encoded length must fit in a u16 for PDUs
    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16;

    /// Encodes the PDU to a byte stream
    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8>;

    /// Attempts to decode a PDU from a byte stream
    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType>;
}

/// Provides utility functions for encoding and decoding byte streams
/// For PDUs which require knowledge of the Segmentation state
pub trait SegmentEncode {
    type PDUType;

    /// Gets the encoded length must fit in a u16 for PDUs
    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16;

    /// Encodes the PDU to a byte stream
    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8>;

    /// Attempts to decode a PDU from a byte stream
    fn decode<T: Read>(
        buffer: &mut T,
        segmentation_flag: SegmentedData,
        file_size_flag: FileSizeFlag,
    ) -> PDUResult<Self::PDUType>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// The standard header attached to all CFDP PDUs.
pub struct PDUHeader {
    /// header version number.
    pub version: U3,

    /// The type of the underlying payload.
    pub pdu_type: PDUType,

    /// The direction in which this PDU is heading.
    pub direction: Direction,

    /// The mode of the transaction.
    pub transmission_mode: TransmissionMode,

    /// Whether a CRC is appended to the PDU byte stream.
    pub crc_flag: CRCFlag,

    /// Flag to indicate if the file size is less than the maximum u32 value.
    pub large_file_flag: FileSizeFlag,

    /// The length of attached payload.
    ///
    /// When the CRC flag is set to [CRCFlag::Present] this struct will automatically
    /// account for the additional length during encoding.
    pub pdu_data_field_length: u16,

    /// Flag to indicate if segmentation control is enabled for this transaction.
    pub segmentation_control: SegmentationControl,

    /// Flag to indicate if metadata segmentation is enabled for this transaction.
    pub segment_metadata_flag: SegmentedData,

    /// Source entity identification number.
    pub source_entity_id: EntityID,

    /// The sequence number of the transaction.
    pub transaction_sequence_number: TransactionSeqNum,

    /// Destination entity identification number.
    pub destination_entity_id: EntityID,
}
impl PDUEncode for PDUHeader {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        // version, type, direction, mode, crc_flag, file size
        1 +
            // pdu data length
            2
            // segmentation control, entity ID len, segment metadata flag, sequence_number len
            + 1
            + self.source_entity_id.encoded_len()
            + self.transaction_sequence_number.encoded_len()
            + self.destination_entity_id.encoded_len()
    }

    fn encode(self) -> Vec<u8> {
        let first_byte = ((self.version as u8) << 5)
            | ((self.pdu_type as u8) << 4)
            | ((self.direction as u8) << 3)
            | ((self.transmission_mode as u8) << 2)
            | ((self.crc_flag as u8) << 1)
            | self.large_file_flag as u8;
        let mut buffer = vec![first_byte];
        // if the CRC is expected add 2 to the length of the "data" field
        buffer.extend(match &self.crc_flag {
            CRCFlag::NotPresent => self.pdu_data_field_length.to_be_bytes(),
            CRCFlag::Present => (self.pdu_data_field_length + 2).to_be_bytes(),
        });
        buffer.push(
            ((self.segmentation_control as u8) << 7)
                | ((self.source_entity_id.encoded_len() as u8 - 1) << 4)
                | ((self.segment_metadata_flag as u8) << 3)
                | (self.transaction_sequence_number.encoded_len() as u8 - 1),
        );
        buffer.extend(self.source_entity_id.to_be_bytes());
        buffer.extend(self.transaction_sequence_number.to_be_bytes());
        buffer.extend(self.destination_entity_id.to_be_bytes());
        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0_u8; 1];
        buffer.read_exact(&mut u8_buff)?;

        let version = {
            let possible = (u8_buff[0] & 0xE0) >> 5;
            U3::from_u8(possible).ok_or(PDUError::InvalidVersion(possible))?
        };

        let pdu_type = {
            let possible = (u8_buff[0] & 0x10) >> 4;
            PDUType::from_u8(possible).ok_or(PDUError::InvalidPDUType(possible))?
        };

        let direction = {
            let possible = (u8_buff[0] & 0x8) >> 3;
            Direction::from_u8(possible).ok_or(PDUError::InvalidDirection(possible))?
        };

        let transmission_mode = {
            let possible = (u8_buff[0] & 0x4) >> 2;
            TransmissionMode::from_u8(possible)
                .ok_or(PDUError::InvalidTransmissionMode(possible))?
        };

        let crc_flag = {
            let possible = (u8_buff[0] & 0x2) >> 1;
            CRCFlag::from_u8(possible).ok_or(PDUError::InvalidCRCFlag(possible))?
        };

        let large_file_flag = {
            let possible = u8_buff[0] & 0x1;
            FileSizeFlag::from_u8(possible).ok_or(PDUError::InvalidFileSizeFlag(possible))?
        };

        let pdu_data_field_length = {
            let mut u16_buff = [0_u8; 2];
            buffer.read_exact(&mut u16_buff)?;
            // CRC length is _included_ in the data_field_length
            // but it is not actually part of the message.
            // strip the crc length to preserve the original message
            match &crc_flag {
                CRCFlag::NotPresent => u16::from_be_bytes(u16_buff),
                CRCFlag::Present => u16::from_be_bytes(u16_buff) - 2,
            }
        };

        buffer.read_exact(&mut u8_buff)?;

        let segmentation_control = {
            let possible = (u8_buff[0] & 0x80) >> 7;
            SegmentationControl::from_u8(possible)
                .ok_or(PDUError::InvalidSegmentControl(possible))?
        };

        let segment_metadata_flag = {
            let possible = (u8_buff[0] & 8) >> 3;
            SegmentedData::from_u8(possible)
                .ok_or(PDUError::InvalidSegmentMetadataFlag(possible))?
        };

        // CCSDS defines the lengths to be encoded as length - 1.
        // add one back to get actual value.
        let entity_id_length = ((u8_buff[0] & 0x70) >> 4) + 1;
        let transaction_sequence_length = (u8_buff[0] & 0x7) + 1;

        let source_entity_id: EntityID = {
            let mut buff = vec![0_u8; entity_id_length as usize];
            buffer.read_exact(buff.as_mut_slice())?;
            EntityID::try_from(buff.to_vec())?
        };

        let transaction_sequence_number = {
            let mut buff = vec![0_u8; transaction_sequence_length as usize];
            buffer.read_exact(buff.as_mut_slice())?;
            TransactionSeqNum::try_from(buff.to_vec())?
        };

        let destination_entity_id = {
            let mut buff = vec![0_u8; entity_id_length as usize];
            buffer.read_exact(buff.as_mut_slice())?;
            EntityID(u16::from_be_bytes(
                buff.try_into()
                    .expect("Unable to coerce vec into same sized array."),
            ))
        };

        Ok(Self {
            version,
            pdu_type,
            direction,
            transmission_mode,
            crc_flag,
            large_file_flag,
            pdu_data_field_length,
            segmentation_control,
            segment_metadata_flag,
            source_entity_id,
            transaction_sequence_number,
            destination_entity_id,
        })
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Condition::NoError => "No Error",
                Condition::InvalidTransmissionMode => "Invalid Transmission",
                Condition::FileStoreRejection => "Filestore Rejection",
                Condition::FileChecksumFailure => "Checksum Failure",
                Condition::FilesizeError => "Unexpected File Size",
                Condition::InactivityDetected => "Inactivity Detected",
                Condition::InvalidFileStructure => "Invalid File Structure",
                Condition::PositiveLimitReached => "Transaction Timeout",
                Condition::UnsupportedChecksumType => "Unsupported Checksum",
                _ => "Unknown",
            }
        )
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Direction::ToReceiver => "Outgoing",
                Direction::ToSender => "Incoming",
            }
        )
    }
}

/// Read a length-value (LV) pair from a byte stream and return the value.
pub fn read_length_value_pair<T: Read>(buffer: &mut T) -> PDUResult<Vec<u8>> {
    let mut u8_buff = [0u8; 1];
    buffer.read_exact(&mut u8_buff)?;
    let length = u8_buff[0];
    let mut vector = vec![0u8; length as usize];
    buffer.read_exact(vector.as_mut_slice())?;
    Ok(vector)
}

#[cfg(test)]
mod test {
    #![allow(clippy::too_many_arguments)]

    use std::{u16, u32};

    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(
        12_u16,
        EntityID::from(u16::MAX),
        TransactionSeqNum::from(u32::MAX),
        EntityID::from(u16::MAX)
    )]
    #[case(
        8745_u16,
        EntityID::from(u16::MAX),
        TransactionSeqNum::from(u32::MAX),
        EntityID::from(u16::MAX)
    )]
    #[case(
        65531_u16,
        EntityID::from(u16::MAX),
        TransactionSeqNum::from(u32::MAX),
        EntityID::from(u16::MAX)
    )]
    fn pdu_header(
        #[values(U3::Zero)] version: U3,
        #[values(PDUType::FileDirective, PDUType::FileData)] pdu_type: PDUType,
        #[values(Direction::ToReceiver, Direction::ToSender)] direction: Direction,
        #[values(TransmissionMode::Acknowledged, TransmissionMode::Unacknowledged)]
        transmission_mode: TransmissionMode,
        #[values(CRCFlag::NotPresent, CRCFlag::Present)] crc_flag: CRCFlag,
        #[values(FileSizeFlag::Small, FileSizeFlag::Large)] large_file_flag: FileSizeFlag,
        #[case] pdu_data_field_length: u16,
        #[case] source_entity_id: EntityID,
        #[case] transaction_sequence_number: TransactionSeqNum,
        #[case] destination_entity_id: EntityID,
    ) -> PDUResult<()> {
        let (segmentation_control, segment_metadata_flag) = match &pdu_type {
            PDUType::FileData => (SegmentationControl::NotPreserved, SegmentedData::Present),
            PDUType::FileDirective => (SegmentationControl::NotPreserved, SegmentedData::Present),
        };

        let expected = PDUHeader {
            version,
            pdu_type,
            direction,
            transmission_mode,
            crc_flag,
            large_file_flag,
            pdu_data_field_length,
            segmentation_control,
            segment_metadata_flag,
            source_entity_id,
            transaction_sequence_number,
            destination_entity_id,
        };
        let buffer = expected.clone().encode();
        let recovered = PDUHeader::decode(&mut buffer.as_slice())?;
        assert_eq!(expected, recovered);

        Ok(())
    }
}
