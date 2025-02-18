use std::string::FromUtf8Error;
use thiserror::Error;

pub type PDUResult<T> = Result<T, PDUError>;
#[derive(Error, Debug)]
/// Errors which can occur during a PDU encoding/decoding.
pub enum PDUError {
    #[error("Invalid Condition value: {0:}.")]
    /// Unknown transaction condition value.
    InvalidCondition(u8),

    #[error("Invalid ChecksumType: {0:}.")]
    /// Checksum type done not match a known value.
    InvalidChecksumType(u8),

    #[error("Invalid Direction value: {0:}.")]
    /// Unknown transaction direction value.
    InvalidDirection(u8),

    #[error("Invalid Directive value: {0:}.")]
    /// Unknown Transaction directive value.
    InvalidDirective(u8),

    #[error("Invalid Delivery Code: {0:}.")]
    /// Unknown transaction delivery code value.
    InvalidDeliveryCode(u8),

    #[error("Invalid TransactionState: {0:}.")]
    /// Unknown [TransactionState](crate::transaction::TransactionState) value.
    InvalidState(u8),

    #[error("Invalid File Status: {0:}.")]
    /// Unknown [FileStatusCode](crate::pdu::FileStatusCode) value
    InvalidFileStatus(u8),

    #[error("Invalid Transmission Mode {0:}.")]
    /// Unknown [TransmissionMode](crate::pdu::TransmissionMode) value.
    InvalidTransmissionMode(u8),

    #[error("Invalid Segment Control Mode {0:}.")]
    /// Unknown [SegmentationControl](crate::pdu::SegmentationControl) value.
    InvalidSegmentControl(u8),

    #[error("Invalid Transaction Status {0:}.")]
    /// Unknown [TransactionStatus](crate::pdu::TransactionStatus) value.
    InvalidTransactionStatus(u8),

    #[error("Invalid ACK SubDirective Code: {0:}.")]
    /// Unknown Acknowledged sub-directive code value.
    InvalidACKDirectiveSubType(u8),

    #[error("Invalid CCSDS Version Code: {0:}.")]
    /// Unknown CCSDS packet version.
    InvalidVersion(u8),

    #[error("Invalid Fault Handler Code: {0:}.")]
    /// Unknown [HandlerCode](crate::pdu::HandlerCode) value.
    InvalidFaultHandlerCode(u8),

    #[error("Invalid PDU Type {0:}.")]
    /// Received an unkown [PDUType](crate::pdu::PDUType) value.
    InvalidPDUType(u8),

    #[error("Invalid CRC Flag {0:}.")]
    /// Unknown [CRCFlag](crate::pdu::CRCFlag) value
    InvalidCRCFlag(u8),

    #[error("Invalid File Size Flag {0:}.")]
    /// Unknown flag value for [FileSizeFlag](crate::pdu::FileSizeFlag)
    InvalidFileSizeFlag(u8),

    #[error("Invalid Segment Metadata Flag {0:}.")]
    /// Unknown value for [SegmentedData](crate::pdu::SegmentedData) flag.
    InvalidSegmentMetadataFlag(u8),

    #[error("CRC Failure on PDU. Expected 0x{0:X} Receieved 0x{1:X}")]
    /// PDU crc16 failure
    CRCFailure(u16, u16),

    #[error("Error Reading PDU Buffer. {0:}")]
    /// [std::io::Error] occurred when reading PDU from input byte stream.
    ReadError(#[from] std::io::Error),

    #[error("Bad length for Variable Identifier (not a power a of 2) {0:}.")]
    /// Length of the ID variable identifier was not a power of 2.
    UnknownIDLength(u8),

    #[error("Unable to decode filename. {0}")]
    /// Error occurred converting bytes to UTF-8 compliant filename.
    InvalidFileName(#[from] FromUtf8Error),

    // Error indicating an unsupport feature
    #[error("Unsupported feature. {0}")]
    Unsupported(String),
}
