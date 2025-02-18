use std::{
    collections::{HashSet, VecDeque},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};
use log::{debug, info};
use tokio::sync::{
    mpsc::{Permit, Sender},
    oneshot,
};

use super::fault_handler::FaultHandlerAction;
use crate::{
    daemon::{FinishedIndication, Indication, Report},
    filestore::{FileChecksum, FileStore, FileStoreError},
    pdu::{
        Condition, DeliveryCode, Direction, EndOfFile, EntityID, FileDataPDU, FileStatusCode,
        MetadataPDU, Operations, PDUHeader, PDUPayload, PDUType, SegmentRequestForm,
        SegmentationControl, SegmentedData, TransactionStatus, TransmissionMode,
        UnsegmentedFileData, PDU, U3,
    },
    transaction::{Metadata, TransactionConfig, TransactionID, TransactionState},
};

use crate::{
    daemon::Timer,
    transaction::{TransactionError, TransactionResult},
};

#[derive(PartialEq, Debug)]
#[allow(clippy::enum_variant_names)]
enum SendState {
    // initial state
    // send metadata, then go to SendData (if file transfer) or directly to SendEof
    SendMetadata,
    // send data until finished, then go to SendEof
    SendData,
    // send EOF and wait for nak
    SendEof,
}

pub struct SendTransaction<T: FileStore> {
    /// The current status of the Transaction. See [TransactionStatus]
    status: TransactionStatus,
    /// Configuration Information for this Transaction. See [TransactionConfig]
    config: TransactionConfig,
    /// The [FileStore] implementation used to interact with files on disk.
    filestore: Arc<T>,
    /// The current file being worked by this Transaction.
    file_handle: Option<File>,
    /// The list of all missing information
    naks: VecDeque<SegmentRequestForm>,
    ///  The metadata of this Transaction
    pub(crate) metadata: Metadata,
    /// a cache of the header used for interactions in this transmission
    header: Option<PDUHeader>,
    /// The current condition of the transaction
    condition: Condition,
    // Track whether the transaction is complete or not
    delivery_code: DeliveryCode,
    // Status of the current File
    file_status: FileStatusCode,
    /// Timer used to track if the Nak limit has been reached
    /// inactivity has occurred
    /// or the ACK limit is reached
    timer: Timer,
    //. checksum cache to reduce I/0
    //. doubles as stored checksum in received mode
    checksum: Option<u32>,
    /// The current general state of the transaction.
    /// Used to determine when the thread should be killed
    state: TransactionState,
    /// The detailed sub-state valid when state = TransactionState::Active
    /// Used to control the state machine
    send_state: SendState,
    /// EoF prepared to be sent at the next opportunity if the bool is true
    eof: Option<(EndOfFile, bool)>,
    /// Channel for Indications to propagate back up
    indication_tx: Sender<Indication>,
    /// flag to track if the initial EoFSent Indication has been sent.
    /// This indication only needs to be delivered for the initial EoF transmission
    send_eof_indication: bool,
    // Empty Nak Received indication
    empty_nak_received: bool,
    /// File bytes sent
    file_bytes_sent: u64,
    /// Date transaction was submitted
    submit_date: DateTime<Utc>,
}
impl<T: FileStore> SendTransaction<T> {
    /// Start a new SendTransaction with the given [configuration](TransactionConfig)
    /// and [Filestore Implementation](FileStore)
    pub fn new(
        // Configuration of this Transaction.
        config: TransactionConfig,
        // Metadata of this Transaction
        metadata: Metadata,
        // Connection to the local FileStore implementation.
        filestore: Arc<T>,
        // Sender channel used to propagate Message To User back up to the Daemon Thread.
        indication_tx: Sender<Indication>,
    ) -> TransactionResult<Self> {
        let file_bytes_sent = 0_u64;
        let timer = Timer::new(
            config.inactivity_timeout,
            config.max_count,
            config.eof_timeout,
            config.max_count,
            config.nak_timeout,
            config.max_count,
            config.inactivity_timeout,
        );

        let me = Self {
            status: TransactionStatus::Undefined,
            config,
            filestore,
            file_handle: None,
            naks: VecDeque::new(),
            metadata,
            header: None,
            condition: Condition::NoError,
            delivery_code: DeliveryCode::Incomplete,
            file_status: FileStatusCode::Unreported,
            timer,
            checksum: None,
            send_state: SendState::SendMetadata,
            state: TransactionState::Active,
            eof: None,
            indication_tx,
            send_eof_indication: true,
            empty_nak_received: false,
            file_bytes_sent,
            submit_date: Utc::now(),
        };
        me.send_indication(Indication::Transaction(me.id()));
        Ok(me)
    }

    pub(crate) fn has_pdu_to_send(&self) -> bool {
        match self.send_state {
            SendState::SendMetadata | SendState::SendData => true,
            SendState::SendEof => !self.naks.is_empty() || self.eof.as_ref().is_some_and(|x| x.1),
        }
    }

    // returns the time until the first timeout
    pub(crate) fn until_timeout(&self) -> Duration {
        match self.send_state {
            SendState::SendEof => self.timer.until_timeout(),
            _ => Duration::MAX,
        }
    }

    pub(crate) fn send_pdu(
        &mut self,
        permit: Permit<'_, (EntityID, PDU)>,
    ) -> TransactionResult<()> {
        match self.send_state {
            SendState::SendMetadata => {
                self.send_metadata(permit)?;
                if self.is_file_transfer() {
                    self.send_state = SendState::SendData;
                } else {
                    self.prepare_eof()?;
                    self.send_state = SendState::SendEof;
                }
            }
            SendState::SendData => {
                if !self.naks.is_empty() {
                    // if we have received a NAK send the missing data
                    self.send_missing_data(permit)?;
                } else {
                    self.send_file_segment(None, None, permit)?
                }

                let handle = self.get_handle()?;
                if handle.stream_position().map_err(FileStoreError::IO)?
                    == handle.metadata().map_err(FileStoreError::IO)?.len()
                {
                    self.prepare_eof()?;
                    self.send_state = SendState::SendEof;
                }
            }
            SendState::SendEof => {
                if !self.naks.is_empty() {
                    // if we have received a NAK send the missing data
                    self.send_missing_data(permit)?;
                } else {
                    self.send_eof(permit)?;

                    // EoFSent indication only needs to be sent for the initial EoF transmission
                    if self.send_eof_indication {
                        self.send_indication(Indication::EoFSent(self.id()));
                        self.send_eof_indication = false;
                    }

                    if self.get_mode() == TransmissionMode::Unacknowledged {
                        // closure not supported, this transaction is finished.
                        // indicate as much to the User
                        self.send_indication(Indication::Finished(FinishedIndication {
                            id: self.id(),
                            report: self.generate_report(),
                            file_status: self.file_status,
                            delivery_code: self.delivery_code,
                        }));
                        {
                            self.shutdown()
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::single_match)]
    pub fn handle_timeout(&mut self) -> TransactionResult<()> {
        debug!("Handling timeout...");
        match self.send_state {
            SendState::SendEof => {
                if self.timer.inactivity.limit_reached() {
                    self.handle_fault(Condition::InactivityDetected)?
                }
                // EoF timer tracks if the sender has received a NAK from the receiver
                // The sender will attempt to send EOFs, up to PositiveLimitReached count
                if self.timer.eof.timeout_occurred() {
                    if self.timer.eof.limit_reached() {
                        self.handle_fault(Condition::PositiveLimitReached)?
                    } else {
                        self.set_eof_flag(true);
                    }
                }
            }
            _ => {}
        };

        Ok(())
    }

    pub fn get_status(&self) -> TransactionStatus {
        self.status
    }

    pub(crate) fn get_state(&self) -> TransactionState {
        self.state
    }

    pub fn get_mode(&self) -> TransmissionMode {
        self.config.transmission_mode
    }
    fn generate_report(&self) -> Report {
        Report {
            id: self.id(),
            state: self.get_state(),
            status: self.get_status(),
            condition: self.condition,
            empty_nak_received: self.empty_nak_received,
            file_bytes_received: None,
            file_bytes_sent: Some(self.file_bytes_sent),
            file_size: self.metadata.file_size,
            direction: self.header.as_ref().map(|h| h.direction),
            file_name: self
                .metadata
                .source_filename
                .file_name()
                .map(|file| file.to_string())
                .unwrap_or_default(),
            submit_date: self.submit_date,
        }
    }

    pub fn send_report(&self, sender: Option<oneshot::Sender<Report>>) -> TransactionResult<()> {
        let report = self.generate_report();

        if let Some(channel) = sender {
            let _ = channel.send(report.clone());
        }
        self.send_indication(Indication::Report(report));

        Ok(())
    }

    fn get_header(
        &mut self,
        direction: Direction,
        pdu_type: PDUType,
        pdu_data_field_length: u16,
        segmentation_control: SegmentationControl,
    ) -> PDUHeader {
        if let Some(header) = &self.header {
            // update the necessary fields but copy the rest
            // from the cache
            PDUHeader {
                pdu_type,
                pdu_data_field_length,
                segmentation_control,
                ..header.clone()
            }
        } else {
            let header = PDUHeader {
                version: U3::Zero,
                pdu_type,
                direction,
                transmission_mode: self.config.transmission_mode,
                crc_flag: self.config.crc_flag,
                large_file_flag: self.config.file_size_flag,
                pdu_data_field_length,
                segmentation_control,
                segment_metadata_flag: self.config.segment_metadata_flag,
                source_entity_id: self.config.source_entity_id,
                transaction_sequence_number: self.config.sequence_number,
                destination_entity_id: self.config.destination_entity_id,
            };
            self.header = Some(header.clone());
            header
        }
    }

    pub fn id(&self) -> TransactionID {
        TransactionID(self.config.source_entity_id, self.config.sequence_number)
    }

    fn get_checksum(&mut self) -> TransactionResult<u32> {
        match self.checksum {
            Some(val) => Ok(val),
            None => {
                let checksum = {
                    match self.is_file_transfer() {
                        true => {
                            let checksum_type = self.metadata.checksum_type;
                            self.get_handle()?.checksum(checksum_type)?
                        }
                        false => 0,
                    }
                };
                self.checksum = Some(checksum);
                Ok(checksum)
            }
        }
    }

    fn open_source_file(&mut self) -> TransactionResult<()> {
        self.file_handle = {
            let fname = &self.metadata.source_filename;
            Some(self.filestore.open(fname, OpenOptions::new().read(true))?)
        };
        Ok(())
    }

    fn get_handle(&mut self) -> TransactionResult<&mut File> {
        let id = self.id();
        if self.file_handle.is_none() {
            self.open_source_file()?
        };
        self.file_handle
            .as_mut()
            .ok_or(TransactionError::NoFile(id))
    }

    pub(crate) fn is_file_transfer(&self) -> bool {
        !self.metadata.source_filename.as_os_str().is_empty()
    }

    /// Read a segment of size `length` from position `offset` in the file.
    /// When offset is [None], reads from the current cursor location.
    /// when length is [None] uses the maximum length for the receiving Engine.
    /// This size is set by the `file_size_segment` field in the [TransactionConfig].
    fn get_file_segment(
        &mut self,
        offset: Option<u64>,
        length: Option<u16>,
    ) -> TransactionResult<(u64, Vec<u8>)> {
        // use the maximum size for the receiever if no length is given
        let length = length.unwrap_or(self.config.file_size_segment);
        let handle = self.get_handle()?;
        // if no offset given read from current cursor position
        let offset = offset.unwrap_or(handle.stream_position().map_err(FileStoreError::IO)?);

        // If the offset is not provided start at the current position

        handle
            .seek(SeekFrom::Start(offset))
            .map_err(FileStoreError::IO)?;

        // use take to limit the final segment from trying to read past the EoF.
        let data = {
            let mut buff = Vec::<u8>::with_capacity(length as usize);
            handle
                .take(length as u64)
                .read_to_end(&mut buff)
                .map_err(FileStoreError::IO)?;
            buff
        };

        Ok((offset, data))
    }

    /// Send a segment of size `length` from position `offset` in the file.
    /// When offset is [None], reads from the current cursor location.
    /// when length is [None] uses the maximum length for the receiving Engine.
    /// This size is set by the `file_size_segment` field in the [TransactionConfig].
    pub fn send_file_segment(
        &mut self,
        offset: Option<u64>,
        length: Option<u16>,
        permit: Permit<(EntityID, PDU)>,
    ) -> TransactionResult<()> {
        let (offset, file_data) = self.get_file_segment(offset, length)?;

        let (data, segmentation_control) = match self.config.segment_metadata_flag {
            SegmentedData::NotPresent => (
                FileDataPDU(UnsegmentedFileData { offset, file_data }),
                SegmentationControl::NotPreserved,
            ),
            SegmentedData::Present => unimplemented!(),
        };
        let destination = self.config.destination_entity_id;

        let payload = PDUPayload::FileData(data);

        let payload_len: u16 = payload.encoded_len(self.config.file_size_flag);

        self.file_bytes_sent += payload_len as u64;

        let header = self.get_header(
            Direction::ToReceiver,
            PDUType::FileData,
            payload_len,
            segmentation_control,
        );
        let pdu = PDU { header, payload };

        permit.send((destination, pdu));

        Ok(())
    }

    pub fn send_missing_data(&mut self, permit: Permit<(EntityID, PDU)>) -> TransactionResult<()> {
        match self.naks.pop_front() {
            Some(request) => {
                // only restart inactivity if we have something to do.
                self.timer.restart_inactivity();
                let (offset, length) = (
                    request.start_offset,
                    (request.end_offset - request.start_offset).try_into()?,
                );

                match offset == 0 && length == 0 {
                    true => self.send_metadata(permit),
                    false => {
                        let current_pos = {
                            let handle = self.get_handle()?;
                            handle.stream_position().map_err(FileStoreError::IO)?
                        };

                        self.send_file_segment(Some(offset), Some(length), permit)?;

                        // restore to original location in the file
                        let handle = self.get_handle()?;
                        handle
                            .seek(SeekFrom::Start(current_pos))
                            .map_err(FileStoreError::IO)?;
                        Ok(())
                    }
                }
            }
            None => Ok(()),
        }
    }

    fn prepare_eof(&mut self) -> TransactionResult<()> {
        self.eof = Some((
            EndOfFile {
                condition: self.condition,
                checksum: self.get_checksum()?,
                file_size: self.metadata.file_size,
            },
            true,
        ));

        Ok(())
    }

    pub fn send_eof(&mut self, permit: Permit<(EntityID, PDU)>) -> TransactionResult<()> {
        if let Some((eof, true)) = &self.eof {
            self.timer.restart_eof();

            let payload = PDUPayload::Directive(Operations::EoF(eof.clone()));
            let payload_len = payload.encoded_len(self.config.file_size_flag);
            let header = self.get_header(
                Direction::ToReceiver,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            let destination = header.destination_entity_id;
            let pdu = PDU { header, payload };

            permit.send((destination, pdu));
            debug!("Transaction {0} sent EndOfFile.", self.id());
            self.set_eof_flag(false);
        }
        Ok(())
    }

    // set the true flag on the eof such that it is sent at the next opportunity
    fn set_eof_flag(&mut self, flag: bool) {
        if let Some(x) = self.eof.as_mut() {
            x.1 = flag;
        }
    }

    pub fn shutdown(&mut self) {
        debug!("Transaction {0} shutting down.", self.id());
        self.status = TransactionStatus::Terminated;
        self.state = TransactionState::Terminated;
        self.timer.inactivity.pause();
        self.timer.eof.pause();
    }

    /// Take action according to the defined handler mapping.
    /// Returns a boolean indicating if the calling function should continue (true) or not (false.)
    fn handle_fault(&mut self, condition: Condition) -> TransactionResult<()> {
        self.condition = condition;
        let action = self
            .config
            .fault_handler_override
            .get(&self.condition)
            .unwrap_or(&FaultHandlerAction::Abandon);
        info!(
            "Transaction {} handling fault {:?}, action: {:?} ",
            self.id(),
            condition,
            action
        );

        match action {
            FaultHandlerAction::Ignore => {}
            FaultHandlerAction::Abandon => {
                self.shutdown();
            }
        }
        Ok(())
    }

    #[allow(clippy::single_match)]
    pub fn process_pdu(&mut self, pdu: PDU) -> TransactionResult<()> {
        if self.send_state == SendState::SendEof {
            self.timer.restart_inactivity();
            self.timer.reset_eof();
        }
        let PDU {
            header: _header,
            payload,
        } = pdu;
        match &self.config.transmission_mode {
            TransmissionMode::Acknowledged => {
                match payload {
                    // Receiver payload type - Not expected at sender
                    PDUPayload::FileData(_data) => Err(TransactionError::UnexpectedPDU(
                        self.config.sequence_number,
                        self.config.transmission_mode,
                        "File Data".to_owned(),
                    )),
                    PDUPayload::Directive(operation) => match operation {
                        // Receiver Opp - Not expected at sender
                        Operations::EoF(_eof) => Err(TransactionError::UnexpectedPDU(
                            self.config.sequence_number,
                            self.config.transmission_mode,
                            "End of File".to_owned(),
                        )),
                        // Receiver Opp - Not expected at sender
                        Operations::Metadata(_metadata) => Err(TransactionError::UnexpectedPDU(
                            self.config.sequence_number,
                            self.config.transmission_mode,
                            "Metadata PDU".to_owned(),
                        )),
                        Operations::Nak(nak) => {
                            info!(
                                "Received NAK with {} segment requests",
                                nak.segment_requests.len()
                            );
                            // check and filter for naks where the length of the request is greater than
                            // the maximum allowed file size
                            // as the Receiver we don't care if the length is too big
                            // But the Sender needs segments to fit in the expected size.

                            // If received nak list is empty, close the transaction
                            if nak.segment_requests.is_empty() {
                                debug!(
                                    "Transaction {} received empty NAK; moving transaction to Finished state",
                                    self.id(),
                                );

                                self.empty_nak_received = true;

                                self.send_indication(Indication::Finished(FinishedIndication {
                                    id: self.id(),
                                    report: self.generate_report(),
                                    file_status: self.file_status,
                                    delivery_code: self.delivery_code,
                                }));

                                {
                                    self.shutdown()
                                }
                                return Ok(());
                            }

                            let formatted_segments =
                                nak.segment_requests
                                    .into_iter()
                                    .flat_map(|form| match form {
                                        val @ SegmentRequestForm {
                                            start_offset: start,
                                            end_offset: end,
                                        } if start == end => vec![val],
                                        SegmentRequestForm {
                                            start_offset: start,
                                            end_offset: end,
                                        } => (start..end)
                                            .step_by(self.config.file_size_segment.into())
                                            .map(|num| {
                                                if num
                                                    < end.saturating_sub(
                                                        self.config.file_size_segment.into(),
                                                    )
                                                {
                                                    SegmentRequestForm {
                                                        start_offset: num,
                                                        end_offset: num
                                                            + self.config.file_size_segment as u64,
                                                    }
                                                } else {
                                                    SegmentRequestForm {
                                                        start_offset: num,
                                                        end_offset: end,
                                                    }
                                                }
                                            })
                                            .collect::<Vec<SegmentRequestForm>>(),
                                    });
                            self.naks.extend(formatted_segments);
                            // filter out any duplicated NAKS
                            let mut uniques = HashSet::new();
                            self.naks.retain(|element| uniques.insert(element.clone()));
                            Ok(())
                        }
                    },
                }
            }
            TransmissionMode::Unacknowledged => match payload {
                PDUPayload::FileData(_data) => Err(TransactionError::UnexpectedPDU(
                    self.config.sequence_number,
                    self.config.transmission_mode,
                    "File Data".to_owned(),
                )),
                PDUPayload::Directive(operation) => match operation {
                    Operations::EoF(_eof) => Err(TransactionError::UnexpectedPDU(
                        self.config.sequence_number,
                        self.config.transmission_mode,
                        "End of File".to_owned(),
                    )),
                    Operations::Metadata(_metadata) => Err(TransactionError::UnexpectedPDU(
                        self.config.sequence_number,
                        self.config.transmission_mode,
                        "Metadata PDU".to_owned(),
                    )),
                    Operations::Nak(_) => Err(TransactionError::UnexpectedPDU(
                        self.config.sequence_number,
                        self.config.transmission_mode,
                        "NAK PDU".to_owned(),
                    )),
                },
            },
        }
    }

    fn send_metadata(&mut self, permit: Permit<(EntityID, PDU)>) -> TransactionResult<()> {
        let destination = self.config.destination_entity_id;
        let metadata = MetadataPDU {
            checksum_type: self.metadata.checksum_type,
            file_size: self.metadata.file_size,
            source_filename: self.metadata.source_filename.clone(),
            destination_filename: self.metadata.destination_filename.clone(),
        };

        let payload = PDUPayload::Directive(Operations::Metadata(metadata));
        let payload_len: u16 = payload.encoded_len(self.config.file_size_flag);

        let header = self.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );

        let pdu = PDU { header, payload };

        permit.send((destination, pdu));
        debug!("Transaction {0} sent Metadata.", self.id());
        Ok(())
    }

    //send the indication in another task, no need to wait for it
    fn send_indication(&self, indication: Indication) {
        let tx = self.indication_tx.clone();
        tokio::task::spawn(async move { tx.send(indication).await });
    }
}

#[cfg(test)]
mod test {
    use crate::assert_err;
    use crate::{
        filestore::{ChecksumType, NativeFileStore},
        pdu::{
            CRCFlag, FileSizeFlag, NegativeAcknowledgmentPDU, SegmentedData, UnsegmentedFileData,
        },
    };

    use std::io::Write;
    use std::thread::sleep;

    use super::*;
    use crate::transaction::test::default_config;

    use camino::{Utf8Path, Utf8PathBuf};
    use rstest::{fixture, rstest};
    use tempfile::TempDir;
    use tokio::sync::mpsc::channel;
    #[fixture]
    #[once]
    fn tempdir_fixture() -> TempDir {
        TempDir::new().unwrap()
    }

    #[rstest]
    #[tokio::test]
    async fn header(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = channel(10);
        let metadata = test_metadata(10, Utf8PathBuf::from(""));
        let mut transaction = SendTransaction::new(config, metadata, filestore, indication_tx)
            .expect("unable to start transaction.");

        let payload_len = 12;
        let expected = PDUHeader {
            version: U3::Zero,
            pdu_type: PDUType::FileDirective,
            direction: Direction::ToSender,
            transmission_mode: TransmissionMode::Acknowledged,
            crc_flag: CRCFlag::NotPresent,
            large_file_flag: FileSizeFlag::Small,
            pdu_data_field_length: payload_len,
            segmentation_control: SegmentationControl::NotPreserved,
            segment_metadata_flag: SegmentedData::NotPresent,
            source_entity_id: default_config.source_entity_id,
            destination_entity_id: default_config.destination_entity_id,
            transaction_sequence_number: default_config.sequence_number,
        };
        assert_eq!(
            expected,
            transaction.get_header(
                Direction::ToSender,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved
            )
        )
    }

    #[rstest]
    #[tokio::test]
    async fn test_if_file_transfer(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = channel(10);
        let metadata = test_metadata(600_u64, Utf8PathBuf::from("a"));
        let transaction = SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        assert_eq!(
            TransactionStatus::Undefined,
            transaction.get_status().clone()
        );

        assert!(transaction.is_file_transfer())
    }

    #[rstest]
    #[tokio::test]
    async fn send_filedata(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, mut transport_rx) = channel(1);
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let path = Utf8PathBuf::from("testfile");

        let (indication_tx, _indication_rx) = channel(10);
        let metadata = test_metadata(10, path.clone());
        let mut transaction =
            SendTransaction::new(config, metadata, filestore.clone(), indication_tx).unwrap();

        let input = vec![0, 5, 255, 99];

        let payload = PDUPayload::FileData(FileDataPDU(UnsegmentedFileData {
            offset: 6,
            file_data: input.clone(),
        }));
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileData,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        tokio::task::spawn(async move {
            let fname = transaction.metadata.source_filename.clone();

            {
                let mut handle = transaction
                    .filestore
                    .open(fname, OpenOptions::new().create_new(true).write(true))
                    .unwrap();
                handle
                    .seek(SeekFrom::Start(6))
                    .expect("Cannot seek file cursor in thread.");
                handle
                    .write_all(input.as_slice())
                    .expect("Cannot write test file in thread.");
                handle.sync_all().expect("Bad file sync.");
            }

            let offset = 6;
            let length = 4;

            transaction
                .send_file_segment(
                    Some(offset),
                    Some(length as u16),
                    transport_tx.reserve().await.unwrap(),
                )
                .unwrap();
        });
        let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
        let expected_id = default_config.destination_entity_id;
        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
        filestore.delete_file(path).expect("cannot remove file");
    }

    #[rstest]
    #[case(SegmentRequestForm { start_offset: 6, end_offset: 10 })]
    #[case(SegmentRequestForm { start_offset: 0, end_offset: 0 })]
    #[tokio::test]
    async fn send_missing(
        #[case] nak: SegmentRequestForm,
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
    ) {
        let (transport_tx, mut transport_rx) = channel(1);
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let path = Utf8PathBuf::from("testfile_missing");
        let metadata = test_metadata(10, path.clone());

        let (indication_tx, _indication_rx) = channel(10);
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        let input = vec![0, 5, 255, 99];
        let pdu = match &nak {
            SegmentRequestForm {
                start_offset: 6,
                end_offset: 10,
            } => {
                let payload = PDUPayload::FileData(FileDataPDU(UnsegmentedFileData {
                    offset: 6,
                    file_data: input.clone(),
                }));
                let payload_len = payload.encoded_len(transaction.config.file_size_flag);

                let header = transaction.get_header(
                    Direction::ToReceiver,
                    PDUType::FileData,
                    payload_len,
                    SegmentationControl::NotPreserved,
                );
                PDU { header, payload }
            }
            _ => {
                let payload = PDUPayload::Directive(Operations::Metadata(MetadataPDU {
                    file_size: 10,
                    checksum_type: ChecksumType::Modular,
                    source_filename: path.clone(),
                    destination_filename: path.clone(),
                }));
                let payload_len = payload.encoded_len(transaction.config.file_size_flag);

                let header = transaction.get_header(
                    Direction::ToReceiver,
                    PDUType::FileDirective,
                    payload_len,
                    SegmentationControl::NotPreserved,
                );
                PDU { header, payload }
            }
        };

        tokio::task::spawn(async move {
            let fname = transaction.metadata.source_filename.clone();

            {
                let mut handle = transaction
                    .filestore
                    .open(
                        fname,
                        OpenOptions::new().create(true).truncate(true).write(true),
                    )
                    .unwrap();
                handle
                    .seek(SeekFrom::Start(6))
                    .expect("Cannot seek file cursor in thread.");
                handle
                    .write_all(input.as_slice())
                    .expect("Cannot write test file in thread.");
                handle.sync_all().expect("Bad file sync.");
            }

            transaction.naks.push_back(nak);
            transaction
                .send_missing_data(transport_tx.reserve().await.unwrap())
                .unwrap();

            transaction
                .filestore
                .delete_file(path.clone())
                .expect("cannot remove file");
        });
        sleep(Duration::from_millis(500));
        let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
        let expected_id = default_config.destination_entity_id;
        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
    }

    #[rstest]
    #[tokio::test]
    async fn checksum_cache(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = channel(1);
        let metadata = test_metadata(10, Utf8PathBuf::from(""));
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        let result = transaction
            .get_checksum()
            .expect("Cannot get file checksum");
        assert_eq!(0, result);
        assert!(transaction.checksum.is_some());
        let result = transaction
            .get_checksum()
            .expect("Cannot get file checksum");
        assert_eq!(0, result);
    }

    #[rstest]
    #[tokio::test]
    async fn send_eof(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, mut transport_rx) = channel(1);
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let input = "Here is some test data to write!$*#*.\n";

        let (indication_tx, _indication_rx) = channel(10);
        let path = Utf8PathBuf::from("test_eof.dat");
        let metadata = test_metadata(input.as_bytes().len() as u64, path.clone());
        let mut transaction =
            SendTransaction::new(config, metadata, filestore.clone(), indication_tx).unwrap();

        let (checksum, _overflow) =
            input
                .as_bytes()
                .chunks(4)
                .fold((0_u32, false), |accum: (u32, bool), chunk| {
                    let mut vec = vec![0_u8; 4];
                    vec[..chunk.len()].copy_from_slice(chunk);
                    accum
                        .0
                        .overflowing_add(u32::from_be_bytes(vec.try_into().unwrap()))
                });

        let payload = PDUPayload::Directive(Operations::EoF(EndOfFile {
            condition: Condition::NoError,
            checksum,
            file_size: input.as_bytes().len() as u64,
        }));
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        tokio::task::spawn(async move {
            let fname = transaction.metadata.source_filename.clone();

            {
                let mut handle = transaction
                    .filestore
                    .open(fname, OpenOptions::new().create_new(true).write(true))
                    .unwrap();
                handle
                    .write_all(input.as_bytes())
                    .expect("Cannot write test file in thread.");
                handle.sync_all().expect("Bad file sync.");
            }
            transaction.prepare_eof().unwrap();
            transaction
                .send_eof(transport_tx.reserve().await.unwrap())
                .unwrap()
        });

        let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
        let expected_id = default_config.destination_entity_id;

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);

        filestore.delete_file(path).expect("cannot remove file");
    }

    #[rstest]
    #[tokio::test]
    async fn pdu_error_unack_send(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(
            Operations::EoF(EndOfFile{
                condition: Condition::CancelReceived,
                checksum: 12_u32,
                file_size: 1022_u64,
            }),
            Operations::Metadata(MetadataPDU{
                checksum_type: ChecksumType::Modular,
                file_size: 1022_u64,
                source_filename: "test_filename".into(),
                destination_filename: "test_filename".into(),
            }),
            Operations::Nak(NegativeAcknowledgmentPDU{
                start_of_scope: 0_u64,
                end_of_scope: 1022_u64,
                segment_requests: vec![SegmentRequestForm::from((0_u32, 0_u32)), SegmentRequestForm::from((0_u32, 1022_u32))],
            }),
        )]
        operation: Operations,
    ) {
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Unacknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = channel(10);
        let metadata = test_metadata(600, Utf8PathBuf::from("Test_file.txt"));
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);
        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        let result = transaction.process_pdu(pdu);

        assert_err!(
            result,
            Err(TransactionError::UnexpectedPDU(
                _,
                TransmissionMode::Unacknowledged,
                _
            ))
        )
    }

    #[rstest]
    #[tokio::test]
    async fn pdu_error_ack_send(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(
            Operations::EoF(EndOfFile{
                condition: Condition::CancelReceived,
                checksum: 12_u32,
                file_size: 1022_u64,
            }),
            Operations::Metadata(MetadataPDU{
                checksum_type: ChecksumType::Modular,
                file_size: 1022_u64,
                source_filename: "test_filename".into(),
                destination_filename: "test_filename".into(),
            }),
        )]
        operation: Operations,
    ) {
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = channel(10);
        let metadata = test_metadata(600, Utf8PathBuf::from("Test_file.txt"));
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);
        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        let result = transaction.process_pdu(pdu);

        assert_err!(
            result,
            Err(TransactionError::UnexpectedPDU(
                _,
                TransmissionMode::Acknowledged,
                _
            ))
        )
    }

    #[rstest]
    #[tokio::test]
    async fn pdu_error_send_data(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = channel(10);
        let metadata = test_metadata(600, Utf8PathBuf::from("Test_file.txt"));
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        let payload = PDUPayload::FileData(FileDataPDU(UnsegmentedFileData {
            offset: 12_u64,
            file_data: (0..12_u8).collect::<Vec<u8>>(),
        }));
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);
        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileData,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        let result = transaction.process_pdu(pdu);

        assert_err!(result, Err(TransactionError::UnexpectedPDU(_, _, _)))
    }

    #[rstest]
    #[tokio::test]
    async fn recv_nak(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(2_u64, u32::MAX as u64 + 100_u64)] total_size: u64,
    ) {
        let file_size_flag = match total_size > u32::MAX.into() {
            true => FileSizeFlag::Large,
            false => FileSizeFlag::Small,
        };

        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;
        config.file_size_flag = file_size_flag;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let (indication_tx, _indication_rx) = channel(10);
        let metadata = test_metadata(total_size, Utf8PathBuf::from("test_file"));
        let mut transaction =
            SendTransaction::new(config, metadata, filestore, indication_tx).unwrap();

        let nak_pdu = {
            let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
                start_of_scope: 0,
                end_of_scope: total_size,
                segment_requests: vec![SegmentRequestForm {
                    start_offset: 0,
                    end_offset: total_size,
                }],
            }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

            let header = transaction.get_header(
                Direction::ToSender,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );

            PDU { header, payload }
        };

        let expected_naks: VecDeque<SegmentRequestForm> = (0..total_size)
            .step_by(transaction.config.file_size_segment.into())
            .map(|num| {
                if num < total_size.saturating_sub(transaction.config.file_size_segment.into()) {
                    SegmentRequestForm {
                        start_offset: num,
                        end_offset: num + transaction.config.file_size_segment as u64,
                    }
                } else {
                    SegmentRequestForm {
                        start_offset: num,
                        end_offset: total_size,
                    }
                }
            })
            .collect();

        transaction.process_pdu(nak_pdu).unwrap();
        assert_eq!(expected_naks, transaction.naks);
    }

    fn test_metadata(file_size: u64, path: Utf8PathBuf) -> Metadata {
        Metadata {
            file_size,
            source_filename: path.clone(),
            destination_filename: path,
            checksum_type: ChecksumType::Modular,
        }
    }
}
