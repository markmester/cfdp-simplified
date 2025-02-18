pub mod error;
pub mod header;
pub mod ops;

pub use header::*;
pub use ops::*;

use log::error;
use std::io::Read;
use tracing::trace;

#[doc(inline)]
pub use error::{PDUError, PDUResult};

#[derive(Clone, Debug, PartialEq, Eq)]
/// All possible payloads of a cfdp PDU.
pub enum PDUPayload {
    /// Any non file data related PDU
    Directive(Operations),
    /// File data only PDUs
    FileData(FileDataPDU),
}
impl PDUPayload {
    /// computes the total length of the payload without additional encoding/copying
    pub fn encoded_len(&self, file_size_flag: FileSizeFlag) -> u16 {
        match self {
            Self::Directive(operation) => operation.encoded_len(file_size_flag),
            Self::FileData(file_data) => file_data.encoded_len(file_size_flag),
        }
    }

    /// Encodes the payload to a byte stream
    pub fn encode(self, file_size_flag: FileSizeFlag) -> Vec<u8> {
        match self {
            Self::Directive(operation) => operation.encode(file_size_flag),
            Self::FileData(data) => data.encode(file_size_flag),
        }
    }

    /// Decodes from an input bytestream
    pub fn decode<T: std::io::Read>(
        buffer: &mut T,
        pdu_type: PDUType,
        file_size_flag: FileSizeFlag,
        segmentation_flag: SegmentedData,
    ) -> PDUResult<Self> {
        match pdu_type {
            PDUType::FileDirective => {
                Ok(Self::Directive(Operations::decode(buffer, file_size_flag)?))
            }
            PDUType::FileData => Ok(Self::FileData(FileDataPDU::decode(
                buffer,
                segmentation_flag,
                file_size_flag,
            )?)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// The Protocol Data Unit (PDU).
///
/// The main packet type for CFDP interactions.
pub struct PDU {
    /// Header information used to decode the rest of the packet.
    pub header: PDUHeader,
    /// Packet payload containing filedata or a directive.
    pub payload: PDUPayload,
}
impl PDUEncode for PDU {
    type PDUType = Self;

    fn encoded_len(&self) -> u16 {
        self.header.encoded_len() + self.payload.encoded_len(self.header.large_file_flag)
    }

    fn encode(self) -> Vec<u8> {
        trace!("Encoded PDU: {:?}", self);
        let crc_flag = self.header.crc_flag;
        let file_size_flag = self.header.large_file_flag;

        let mut buffer = self.header.encode();
        buffer.extend(self.payload.encode(file_size_flag));
        match crc_flag {
            CRCFlag::Present => buffer.extend(crc16_ibm_3740(buffer.as_slice()).to_be_bytes()),
            CRCFlag::NotPresent => {}
        }
        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let header = PDUHeader::decode(buffer)?;
        trace!("Decoded header: {:?}", header);

        let mut remaining_msg = vec![0_u8; header.pdu_data_field_length as usize];

        buffer.read_exact(remaining_msg.as_mut_slice())?;
        let remaining_buffer = &mut remaining_msg.as_slice();

        let payload = PDUPayload::decode(
            remaining_buffer,
            header.pdu_type.clone(),
            header.large_file_flag,
            header.segment_metadata_flag,
        )?;

        let received_pdu = Self { header, payload };

        match &received_pdu.header.crc_flag {
            CRCFlag::NotPresent => {}
            CRCFlag::Present => {
                let mut u16_buffer = [0_u8; 2];
                buffer.read_exact(&mut u16_buffer)?;
                let crc16 = u16::from_be_bytes(u16_buffer);
                let tmp_buffer = {
                    let input_pdu = received_pdu.clone();

                    let mut temp = input_pdu.encode();
                    // remove the crc from the temporary buffer
                    temp.truncate(temp.len() - 2);
                    temp
                };
                let crc = crc16_ibm_3740(tmp_buffer.as_slice());
                match crc == crc16 {
                    true => {}
                    false => {
                        error!(
                            "CRC FAILURE, {}, {}, {}",
                            crc,
                            crc16,
                            crc.overflowing_add(crc16).0
                        );
                        return Err(PDUError::CRCFailure(crc16, crc));
                    }
                }
            }
        }

        Ok(received_pdu)
    }
}

fn crc16_ibm_3740(message: &[u8]) -> u16 {
    message
        .iter()
        .fold(0xffff, |acc, digit| crc16(*digit as u16, acc))
}

fn crc16(in_char: u16, crc: u16) -> u16 {
    let poly = 0x1021;
    let shift_char = (in_char & 0x00FF) << 8;
    let mut crc = crc ^ shift_char;
    for _ in 0..8 {
        match crc & 0x8000 > 0 {
            true => crc = (crc << 1) ^ poly,
            false => crc <<= 1,
        };
    }
    crc
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::filestore::ChecksumType;

    use rstest::rstest;

    #[rstest]
    #[case("123456789".as_bytes(), 0x29b1_u16)]
    #[case(
        &[
            0x06, 0x00, 0x0c, 0xf0, 0x00, 0x04, 0x00, 0x55,
            0x88, 0x73, 0xc9, 0x00, 0x00, 0x05, 0x21
        ],
        0x75FB
    )]
    fn crc16(#[case] input: &[u8], #[case] expected: u16) {
        let recovered = crc16_ibm_3740(input);
        assert_eq!(expected, recovered)
    }

    #[rstest]
    #[case(PDUPayload::Directive(Operations::EoF(EndOfFile{
        condition: Condition::NoError,
        checksum: 13_u32,
        file_size: 12_u64,
    })))]
    #[case(PDUPayload::Directive(Operations::EoF(EndOfFile{
        condition: Condition::NoError,
        checksum: 13_u32,
        file_size: 12_u64,
    })))]
    #[case(PDUPayload::Directive(Operations::EoF(EndOfFile{
        condition: Condition::NoError,
        checksum: 13_u32,
        file_size: 12_u64,
    })))]
    #[case(PDUPayload::Directive(Operations::EoF(EndOfFile{
        condition: Condition::NoError,
        checksum: 13_u32,
        file_size: 12_u64,
    })))]
    #[case(PDUPayload::Directive(Operations::EoF(EndOfFile{
        condition: Condition::NoError,
        checksum: 13_u32,
        file_size: 12_u64,
    })))]
    #[case(PDUPayload::Directive(
        Operations::Metadata(
            MetadataPDU {
                checksum_type: ChecksumType::Modular,
                file_size: 55_u64,
                source_filename: "the input filename".into(),
                destination_filename: "the output filename".into(),
            }
        )

    ))]
    #[case(PDUPayload::Directive(
        Operations::Nak(
            NakPDU {
                start_of_scope: 12_u64,
                end_of_scope: 239585_u64,
                segment_requests: vec![
                    SegmentRequestForm{
                        start_offset:12,
                        end_offset: 64
                    },
                    SegmentRequestForm{
                        start_offset: 69,
                        end_offset: 4758
                    }
                ]
            }
        )

    ))]
    #[case(PDUPayload::FileData(
        FileDataPDU(
            UnsegmentedFileData {
                offset: 948,
                file_data: (0..12).collect()
            }
        )
    ))]
    fn pdu_len(
        #[case] payload: PDUPayload,
        #[values(FileSizeFlag::Small, FileSizeFlag::Large)] file_size_flag: FileSizeFlag,
    ) {
        assert_eq!(
            payload.encoded_len(file_size_flag),
            payload.encode(file_size_flag).len() as u16,
        )
    }

    #[rstest]
    #[case(
        PDUPayload::Directive(Operations::EoF(EndOfFile {
            condition: Condition::NoError,
            checksum: 123749_u32,
            file_size: 7738949_u64,
        }))
    )]
    #[case(
        PDUPayload::FileData(FileDataPDU(UnsegmentedFileData{
            offset: 16_u64,
            file_data: "test some information".as_bytes().to_vec(),
        }))
    )]
    fn pdu_encoding(
        #[case] payload: PDUPayload,
        #[values(CRCFlag::NotPresent, CRCFlag::Present)] crc_flag: CRCFlag,
    ) -> PDUResult<()> {
        let pdu_data_field_length = payload.encoded_len(FileSizeFlag::Large);
        let pdu_type = match &payload {
            PDUPayload::Directive(_) => PDUType::FileDirective,
            PDUPayload::FileData(_) => PDUType::FileData,
        };

        let expected: PDU = PDU {
            header: PDUHeader {
                version: U3::Zero,
                pdu_type,
                direction: Direction::ToReceiver,
                transmission_mode: TransmissionMode::Acknowledged,
                crc_flag,
                large_file_flag: FileSizeFlag::Large,
                pdu_data_field_length,
                segmentation_control: SegmentationControl::NotPreserved,
                segment_metadata_flag: SegmentedData::NotPresent,
                source_entity_id: EntityID::from(18_u16),
                transaction_sequence_number: TransactionSeqNum::from(7533_u32),
                destination_entity_id: EntityID::from(23_u16),
            },
            payload,
        };
        let buffer = expected.clone().encode();
        let recovered = PDU::decode(&mut buffer.as_slice())?;
        assert_eq!(expected, recovered);
        Ok(())
    }
}
