use byteorder::{BigEndian, ReadBytesExt};
use camino::Utf8PathBuf;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use std::{
    fmt::{self},
    io::Read,
};

use super::{
    error::{PDUError, PDUResult},
    header::{
        read_length_value_pair, Condition, FSSEncode, FileSizeFlag, PDUEncode, SegmentEncode,
        SegmentedData,
    },
};
use crate::filestore::ChecksumType;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]

pub struct EntityID(pub u16);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
pub struct TransactionSeqNum(pub u32);

impl From<u16> for EntityID {
    fn from(value: u16) -> Self {
        EntityID(value)
    }
}

impl From<u32> for TransactionSeqNum {
    fn from(value: u32) -> Self {
        TransactionSeqNum(value)
    }
}

impl TryFrom<Vec<u8>> for EntityID {
    type Error = PDUError;

    /// attempt to construct an ID from a Vec of big endian bytes.
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let length = value.len();
        match length {
            2 => Ok(EntityID(u16::from_be_bytes(
                value
                    .try_into()
                    .expect("Unable to coerce vec into same sized array."),
            ))),
            other => Err(PDUError::UnknownIDLength(other as u8)),
        }
    }
}

impl TryFrom<Vec<u8>> for TransactionSeqNum {
    type Error = PDUError;

    /// attempt to construct an ID from a Vec of big endian bytes.
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let length = value.len();
        match length {
            4 => Ok(TransactionSeqNum(u32::from_be_bytes(
                value
                    .try_into()
                    .expect("Unable to coerce vec into same sized array."),
            ))),
            other => Err(PDUError::UnknownIDLength(other as u8)),
        }
    }
}

impl EntityID {
    /// Increment the internal counter of the ID. This is useful for sequence numbers.
    pub fn increment(&mut self) {
        self.0 = self.0.overflowing_add(1).0;
    }

    /// Return the current counter value and then increment.
    pub fn get_and_increment(&mut self) -> Self {
        let current = self.0;
        self.increment();
        EntityID(current)
    }

    /// Convert the internal counter to Big endian bytes.
    pub fn to_be_bytes(self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }

    /// convert underlying ID to u64
    pub fn to_u64(&self) -> u64 {
        self.0 as u64
    }
}

impl TransactionSeqNum {
    /// Increment the internal counter of the ID. This is useful for sequence numbers.
    pub fn increment(&mut self) {
        self.0 = self.0.overflowing_add(1).0;
    }

    /// Return the current counter value and then increment.
    pub fn get_and_increment(&mut self) -> Self {
        let current = self.0;
        self.increment();
        TransactionSeqNum::from(current)
    }

    /// Convert the internal counter to Big endian bytes.
    pub fn to_be_bytes(self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }

    /// convert underlying ID to u64
    pub fn to_u64(&self) -> u64 {
        self.0 as u64
    }
}

impl PDUEncode for EntityID {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        std::mem::size_of::<u16>() as u16
    }

    fn encode(self) -> Vec<u8> {
        let mut buffer = vec![self.encoded_len() as u8 - 1_u8];
        buffer.extend(self.to_be_bytes());
        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;

        let length: u8 = u8_buff[0] + 1;
        let mut id = vec![0u8; length as usize];
        buffer.read_exact(id.as_mut_slice())?;
        Ok(EntityID(u16::from_be_bytes(
            id.try_into()
                .expect("Unable to coerce vec into same sized array."),
        )))
    }
}

impl PDUEncode for TransactionSeqNum {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        std::mem::size_of::<u32>() as u16
    }

    fn encode(self) -> Vec<u8> {
        let mut buffer = vec![self.encoded_len() as u8 - 1_u8];

        buffer.extend(self.to_be_bytes());
        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;

        let length: u8 = u8_buff[0] + 1;
        let mut id = vec![0u8; length as usize];
        buffer.read_exact(id.as_mut_slice())?;
        Ok(TransactionSeqNum(u32::from_be_bytes(
            id.try_into()
                .expect("Unable to coerce vec into same sized array."),
        )))
    }
}

impl fmt::Display for EntityID {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_u64())
    }
}

impl fmt::Display for TransactionSeqNum {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_u64())
    }
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
/// The possible directive types of a PDU, used to distinguish the PDUs.
pub enum PDUDirective {
    /// End of File PDU
    EoF = 0x04,
    /// Metadata PDU
    Metadata = 0x07,
    /// Negative Acknowledgement PDU
    Nak = 0x08,
}

#[repr(u8)]
#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
/// Subdirective codes for Positive acknowledgement PDUs.
pub enum ACKSubDirective {
    Other = 0b0000,
    Finished = 0b0001,
}

#[derive(Clone, Debug, PartialEq, Eq, FromPrimitive)]
/// Continuation state of a record.
pub enum RecordContinuationState {
    First = 0b01,
    Last = 0b10,
    Unsegmented = 0b11,
    Interim = 0b00,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Holds File data beginning at the given offset.
pub struct UnsegmentedFileData {
    /// Byte offset into the file where this data begins.
    pub offset: u64,
    pub file_data: Vec<u8>,
}
impl FSSEncode for UnsegmentedFileData {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        self.file_data.len() as u16 + file_size_flag.encoded_len()
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        let mut buffer = match file_size_flag {
            FileSizeFlag::Small => (self.offset as u32).to_be_bytes().to_vec(),
            FileSizeFlag::Large => self.offset.to_be_bytes().to_vec(),
        };
        buffer.extend(self.file_data);
        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let offset = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };

        let file_data: Vec<u8> = {
            let mut data: Vec<u8> = vec![];
            buffer.read_to_end(&mut data)?;
            data
        };
        Ok(Self { offset, file_data })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// A holder for both possible types of File data PDUs.
pub struct FileDataPDU(pub UnsegmentedFileData);
impl SegmentEncode for FileDataPDU {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        self.0.encoded_len(file_size_flag)
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        self.0.encode(file_size_flag)
    }

    fn decode<T: Read>(
        buffer: &mut T,
        segmentation_flag: SegmentedData,
        file_size_flag: FileSizeFlag,
    ) -> PDUResult<Self::PDUType> {
        match segmentation_flag {
            SegmentedData::Present => Err(PDUError::Unsupported(
                "Segmented packets not supported".to_string(),
            )),
            SegmentedData::NotPresent => Ok(FileDataPDU(UnsegmentedFileData::decode(
                buffer,
                file_size_flag,
            )?)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// All operations PDUs
pub enum Operations {
    EoF(EndOfFile),
    Metadata(MetadataPDU),
    Nak(NakPDU),
}
impl Operations {
    pub fn get_directive(&self) -> PDUDirective {
        match self {
            Self::EoF(_) => PDUDirective::EoF,
            Self::Metadata(_) => PDUDirective::Metadata,
            Self::Nak(_) => PDUDirective::Nak,
        }
    }
}
impl FSSEncode for Operations {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        1 + match self {
            Self::EoF(eof) => eof.encoded_len(file_size_flag),
            Self::Metadata(metadata) => metadata.encoded_len(file_size_flag),
            Self::Nak(nak) => nak.encoded_len(file_size_flag),
        }
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        let mut buffer: Vec<u8> = vec![self.get_directive() as u8];
        let message = match self {
            Self::EoF(msg) => msg.encode(file_size_flag),
            Self::Metadata(msg) => msg.encode(file_size_flag),
            Self::Nak(msg) => msg.encode(file_size_flag),
        };
        buffer.extend(message);
        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0_u8; 1];
        buffer.read_exact(&mut u8_buff)?;

        match PDUDirective::from_u8(u8_buff[0]).ok_or(PDUError::InvalidDirective(u8_buff[0]))? {
            PDUDirective::EoF => Ok(Self::EoF(EndOfFile::decode(buffer, file_size_flag)?)),
            PDUDirective::Metadata => {
                Ok(Self::Metadata(MetadataPDU::decode(buffer, file_size_flag)?))
            }
            PDUDirective::Nak => Ok(Self::Nak(NakPDU::decode(buffer, file_size_flag)?)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndOfFile {
    pub condition: Condition,
    pub checksum: u32,
    pub file_size: u64,
}
impl FSSEncode for EndOfFile {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        //  condition (4 bits + 4 bits spare)
        //  checksum (4 bytes)
        //  File_size (FSS)
        //  Fault Location (0 or TLV of fault location)
        5 + file_size_flag.encoded_len()
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        let first_byte = (self.condition as u8) << 4;
        let mut buffer = vec![first_byte];

        buffer.extend(self.checksum.to_be_bytes());
        match file_size_flag {
            FileSizeFlag::Small => buffer.extend((self.file_size as u32).to_be_bytes()),
            FileSizeFlag::Large => buffer.extend(self.file_size.to_be_bytes()),
        };
        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0_u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let condition = {
            let possible_conditon = (u8_buff[0] & 0xF0) >> 4;
            Condition::from_u8(possible_conditon)
                .ok_or(PDUError::InvalidCondition(possible_conditon))?
        };
        let checksum = {
            let mut u32_buff = [0_u8; 4];
            buffer.read_exact(&mut u32_buff)?;
            u32::from_be_bytes(u32_buff)
        };

        let file_size = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };

        Ok(Self {
            condition,
            checksum,
            file_size,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataPDU {
    pub checksum_type: ChecksumType,
    pub file_size: u64,
    pub source_filename: Utf8PathBuf,
    pub destination_filename: Utf8PathBuf,
}
impl FSSEncode for MetadataPDU {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        // closure + checksum (1 byte)
        // file size (FSS)
        // source filename (1 + len )
        // destination filename (1 + len)
        1 + file_size_flag.encoded_len()
            + 1
            + self.source_filename.as_str().len() as u16
            + 1
            + self.destination_filename.as_str().len() as u16
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        // Closure request flag always false
        let first_byte = ((false as u8) << 6) | (self.checksum_type as u8);
        let mut buffer = vec![first_byte];
        match file_size_flag {
            FileSizeFlag::Small => buffer.extend((self.file_size as u32).to_be_bytes()),
            FileSizeFlag::Large => buffer.extend(self.file_size.to_be_bytes()),
        };

        let source_name = self.source_filename.as_str().as_bytes();
        buffer.push(source_name.len() as u8);
        buffer.extend(source_name);

        let destination_name = self.destination_filename.as_str().as_bytes();
        buffer.push(destination_name.len() as u8);
        buffer.extend(destination_name);

        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0_u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];
        let checksum_type = {
            let possible = first_byte & 0xF;
            ChecksumType::from_u8(possible).ok_or(PDUError::InvalidChecksumType(possible))?
        };

        let file_size = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };

        let source_filename =
            Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);

        let destination_filename =
            Utf8PathBuf::from(String::from_utf8(read_length_value_pair(buffer)?)?);

        Ok(Self {
            checksum_type,
            file_size,
            source_filename,
            destination_filename,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SegmentRequestForm {
    pub start_offset: u64,
    pub end_offset: u64,
}
impl From<(u32, u32)> for SegmentRequestForm {
    fn from(vals: (u32, u32)) -> Self {
        Self {
            start_offset: vals.0.into(),
            end_offset: vals.1.into(),
        }
    }
}
impl From<(u64, u64)> for SegmentRequestForm {
    fn from(vals: (u64, u64)) -> Self {
        Self {
            start_offset: vals.0,
            end_offset: vals.1,
        }
    }
}
impl FSSEncode for SegmentRequestForm {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        2 * file_size_flag.encoded_len()
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        match file_size_flag {
            FileSizeFlag::Small => {
                let mut buffer = (self.start_offset as u32).to_be_bytes().to_vec();
                buffer.extend((self.end_offset as u32).to_be_bytes());
                buffer
            }
            FileSizeFlag::Large => {
                let mut buffer = self.start_offset.to_be_bytes().to_vec();
                buffer.extend(self.end_offset.to_be_bytes());
                buffer
            }
        }
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let start_offset = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };
        let end_offset = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };
        Ok(Self {
            start_offset,
            end_offset,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegativeAcknowledgmentPDU {
    pub start_of_scope: u64,
    pub end_of_scope: u64,
    // 2 x FileSizeSensitive x N length for N requests.
    pub segment_requests: Vec<SegmentRequestForm>,
}
pub(crate) type NakPDU = NegativeAcknowledgmentPDU;
impl FSSEncode for NegativeAcknowledgmentPDU {
    type PDUType = Self;

    fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        self.segment_requests
            .iter()
            .fold(0, |acc, seg| acc + seg.encoded_len(file_size_flag))
            + 2 * file_size_flag.encoded_len()
    }

    fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        let mut buffer = match file_size_flag {
            FileSizeFlag::Small => {
                let mut buffer = (self.start_of_scope as u32).to_be_bytes().to_vec();
                buffer.extend((self.end_of_scope as u32).to_be_bytes());
                buffer
            }
            FileSizeFlag::Large => {
                let mut buffer = self.start_of_scope.to_be_bytes().to_vec();
                buffer.extend(self.end_of_scope.to_be_bytes());
                buffer
            }
        };
        self.segment_requests
            .into_iter()
            .for_each(|req| buffer.extend(req.encode(file_size_flag)));

        buffer
    }

    fn decode<T: Read>(buffer: &mut T, file_size_flag: FileSizeFlag) -> PDUResult<Self::PDUType> {
        let start_of_scope = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };
        let end_of_scope = match file_size_flag {
            FileSizeFlag::Large => buffer.read_u64::<BigEndian>()?,
            FileSizeFlag::Small => buffer.read_u32::<BigEndian>()? as u64,
        };

        let mut segment_requests = vec![];

        let mut remaining_vec = vec![];
        buffer.read_to_end(&mut remaining_vec)?;
        let remaining_buffer = &mut remaining_vec.as_slice();

        while !remaining_buffer.is_empty() {
            segment_requests.push(SegmentRequestForm::decode(
                remaining_buffer,
                file_size_flag,
            )?)
        }

        Ok(Self {
            start_of_scope,
            end_of_scope,
            segment_requests,
        })
    }
}

impl NegativeAcknowledgmentPDU {
    /// returns the maximum number of nak segments which can fit into one PDU given the payload length
    pub fn max_nak_num(file_size_flag: FileSizeFlag, payload_len: u32) -> u32 {
        (payload_len - 2 * file_size_flag.encoded_len() as u32)
            / (2 * file_size_flag.encoded_len() as u32)
    }
}

#[cfg(test)]
mod test {
    use std::u16;

    use super::*;

    use rstest::rstest;

    #[rstest]
    #[case(EntityID(1_u16), EntityID(2_u16))]
    fn increment_entity_id(#[case] id: EntityID, #[case] expected: EntityID) {
        let mut id = id;
        id.increment();
        assert_eq!(expected, id)
    }

    #[rstest]
    #[case(TransactionSeqNum::from(1_u32), TransactionSeqNum::from(2_u32))]
    fn increment_seq_num(#[case] id: TransactionSeqNum, #[case] expected: TransactionSeqNum) {
        let mut id = id;
        id.increment();
        assert_eq!(expected, id)
    }

    #[test]
    fn get_and_increment_entity_id() {
        let mut id = EntityID(123_u16);
        let id2 = id.get_and_increment();
        assert_eq!(EntityID(123_u16), id2);
        assert_eq!(EntityID(124_u16), id);
    }

    #[test]
    fn get_and_increment_seq_num() {
        let mut id = TransactionSeqNum::from(123_u32);
        let id2 = id.get_and_increment();
        assert_eq!(TransactionSeqNum::from(123_u32), id2);
        assert_eq!(TransactionSeqNum::from(124_u32), id);
    }

    #[rstest]
    fn entity_id_encode(#[values(EntityID(1_u16))] id: EntityID) {
        let buff = id.encode();
        let recovered = EntityID::decode(&mut buff.as_slice()).expect("Unable to decode EntityID");
        assert_eq!(id, recovered)
    }

    #[rstest]
    fn seq_num_encode(#[values(TransactionSeqNum::from(1_u32))] id: TransactionSeqNum) {
        let buff = id.encode();
        let recovered = TransactionSeqNum::decode(&mut buff.as_slice())
            .expect("Unable to decode TransactionSeqNum");
        assert_eq!(id, recovered)
    }

    #[rstest]
    #[case(
        FileDataPDU(UnsegmentedFileData{
            offset: 34574292984_u64,
            file_data: (0..255).step_by(3).collect::<Vec<u8>>()
        })
    )]
    #[case(
        FileDataPDU(UnsegmentedFileData{
            offset: 12357_u64,
            file_data: (0..255).step_by(2).collect::<Vec<u8>>()
        })
    )]
    fn unsgemented_data(#[case] expected: FileDataPDU) {
        let file_size_flag = match &expected {
            FileDataPDU(UnsegmentedFileData { offset: val, .. }) if val <= &u32::MAX.into() => {
                FileSizeFlag::Small
            }
            _ => FileSizeFlag::Large,
        };

        let bytes = expected.clone().encode(file_size_flag);

        let recovered = FileDataPDU::decode(
            &mut bytes.as_slice(),
            SegmentedData::NotPresent,
            file_size_flag,
        )
        .unwrap();

        assert_eq!(expected, recovered)
    }

    #[rstest]
    fn end_of_file_no_error(#[values(7573910375_u64, 194885483_u64)] file_size: u64) {
        let file_size_flag = match file_size <= u32::MAX.into() {
            false => FileSizeFlag::Large,
            true => FileSizeFlag::Small,
        };

        let end_of_file = EndOfFile {
            condition: Condition::NoError,
            checksum: 7580274_u32,
            file_size,
        };
        let expected = Operations::EoF(end_of_file);

        let buffer = expected.clone().encode(file_size_flag);

        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag).unwrap();

        assert_eq!(expected, recovered)
    }

    #[rstest]
    fn end_of_file_with_error(
        #[values(
            Condition::PositiveLimitReached,
            Condition::InvalidTransmissionMode,
            Condition::FileStoreRejection,
            Condition::FileChecksumFailure,
            Condition::FilesizeError,
            Condition::NakLimitReached
        )]
        condition: Condition,
        #[values(7573910375_u64, 194885483_u64)] file_size: u64,
        #[values(EntityID(0u16), EntityID(18484u16), EntityID(u16::MAX))] _entity: EntityID,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file_size_flag = match file_size <= u32::MAX.into() {
            false => FileSizeFlag::Large,
            true => FileSizeFlag::Small,
        };

        let end_of_file = EndOfFile {
            condition,
            checksum: 857583994u32,
            file_size,
        };
        let expected = Operations::EoF(end_of_file);

        let buffer = expected.clone().encode(file_size_flag);

        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag)?;

        assert_eq!(expected, recovered);
        Ok(())
    }

    #[rstest]
    fn metadata_pdu(
        #[values(ChecksumType::Null, ChecksumType::Modular)] checksum_type: ChecksumType,
        #[values(184574_u64, 7574839485_u64)] file_size: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file_size_flag = match file_size <= u32::MAX.into() {
            true => FileSizeFlag::Small,
            false => FileSizeFlag::Large,
        };

        let expected = Operations::Metadata(MetadataPDU {
            checksum_type,
            file_size,
            source_filename: "/the/source/filename.txt".into(),
            destination_filename: "/the/destination/filename.dat".into(),
        });
        let buffer = expected.clone().encode(file_size_flag);
        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag)?;

        assert_eq!(expected, recovered);
        Ok(())
    }

    #[rstest]
    #[case(
        124_u64,
        412_u64,
        vec![
            SegmentRequestForm{
                start_offset: 124_u64,
                end_offset: 204_u64
            },
            SegmentRequestForm{
                start_offset: 312_u64,
                end_offset: 412_u64
            },
        ]
    )]
    #[case(
        32_u64,
        582872_u64,
        vec![
            SegmentRequestForm{
                start_offset: 32_u64,
                end_offset: 816_u64
            },
            SegmentRequestForm{
                start_offset: 1024_u64,
                end_offset: 1536_u64
            },
            SegmentRequestForm{
                start_offset: 582360_u64,
                end_offset: 582872_u64
            },
        ]
    )]
    fn nak_pdu(
        #[case] start_of_scope: u64,
        #[case] end_of_scope: u64,
        #[case] segment_requests: Vec<SegmentRequestForm>,
        #[values(FileSizeFlag::Small, FileSizeFlag::Large)] file_size_flag: FileSizeFlag,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected = Operations::Nak(NakPDU {
            start_of_scope,
            end_of_scope,
            segment_requests,
        });
        let buffer = expected.clone().encode(file_size_flag);
        let recovered = Operations::decode(&mut buffer.as_slice(), file_size_flag)?;

        assert_eq!(expected, recovered);
        Ok(())
    }
}
