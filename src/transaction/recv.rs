use std::{
    collections::VecDeque,
    fs::File,
    io::{self, Seek, SeekFrom, Write},
    sync::Arc,
    time::Duration,
};

use camino::Utf8PathBuf;
use chrono::{DateTime, Utc};
use log::{debug, info, warn};
use tokio::sync::{
    mpsc::{Permit, Sender},
    oneshot,
};

use super::fault_handler::FaultHandlerAction;
use crate::{
    daemon::{
        segments::Segments,
        timer::{Counter, Timer},
        FileSegmentIndication, FinishedIndication, Indication, MetadataRecvIndication,
        NakProcedure, Report,
    },
    filestore::{FileChecksum, FileStore, FileStoreError},
    pdu::{
        Condition, DeliveryCode, Direction, EntityID, FileDataPDU, FileStatusCode,
        NegativeAcknowledgmentPDU, Operations, PDUHeader, PDUPayload, PDUType, SegmentRequestForm,
        SegmentationControl, TransactionStatus, TransmissionMode, PDU, U3,
    },
    transaction::{
        Metadata, TransactionConfig, TransactionError, TransactionID, TransactionResult,
        TransactionState,
    },
};

#[derive(PartialEq, Debug)]
enum RecvState {
    // initial state
    // received data, missing data and EOF
    ReceiveData,
    // send Finished and wait for ack
    Finished,
}

pub struct RecvTransaction<T: FileStore> {
    /// The current status of the Transaction. See [TransactionStatus]
    status: TransactionStatus,
    /// Configuration Information for this Transaction. See [TransactionConfig]
    config: TransactionConfig,
    /// The [FileStore] implementation used to interact with files on disk.
    filestore: Arc<T>,
    /// Channel for Indications to propagate back up
    indication_tx: Sender<Indication>,
    /// The current file being worked by this Transaction.
    file_handle: Option<File>,
    /// A sorted list of contiguous (start offset, end offset) non overlapping received segments to monitor progress and detect NAKs.
    saved_segments: Segments,
    /// when to send NAKs - immediately after detection or after EOF
    nak_procedure: NakProcedure,
    /// Flag to check if metadata on the file has been received
    pub(crate) metadata: Option<Metadata>,
    /// Measurement of how much of the file has been received so far
    received_file_size: u64,
    /// a cache of the header used for interactions in this transmission
    pub header: Option<PDUHeader>,
    /// The current condition of the transaction
    condition: Condition,
    // Track whether the transaction is complete or not
    delivery_code: DeliveryCode,
    // Status of the current File
    file_status: FileStatusCode,
    // Timer used to track if the Nak limit has been reached
    // inactivity has occurred
    // or the ACK limit is reached
    timer: Timer,
    // checksum cache to reduce I/0
    // doubles as stored checksum in received mode
    checksum: Option<u32>,
    // The current state of the transaction.
    // Used to determine when the thread should be killed
    state: TransactionState,
    // recv sub-state, applicable when state = Active
    recv_state: RecvState,
    // File size received in the EOF, used also as an indication that EOF has been received
    file_size: Option<u64>,
    // Empty nak PDU prepared to be sent at the next opportunity, if the bool is true
    empty_nak: Option<(NegativeAcknowledgmentPDU, bool)>,
    // the list of gaps to include in the next NAK
    naks: VecDeque<SegmentRequestForm>,
    /// the received_file_size measured when the previous nak has been sent
    nak_received_file_size: u64,

    /// This is used in case the NAK procedure is Immediate(delta) with delta>0
    /// it stores a timer and start, stop offsets. When the timer expires,
    /// the (start, stop) file region is checked for completeness and if gaps still persists,
    /// a nack is added to the list
    /// It is also used in case the NAK procedure is Deferred(delta) with delta>0
    /// In that case the start, stop offsets are the whole file
    delayed_nack_timers: Vec<(Counter, u64, u64)>,

    /// Date transaction was initially received
    receive_date: DateTime<Utc>,
}

impl<T: FileStore> RecvTransaction<T> {
    /// Start a new SendTransaction with the given [configuration](TransactionConfig)
    /// and [Filestore Implementation](FileStore).
    ///
    /// The [NakProcedure] is most likely passed from [EntityConfig](crate::daemon::EntityConfig)
    pub fn new(
        // Configuration of this Transaction.
        config: TransactionConfig,
        // Desired NAK procedure. This is most likely passed from an EntityConfig
        nak_procedure: NakProcedure,
        // Connection to the local FileStore implementation.
        filestore: Arc<T>,
        // Sender channel used to propagate Message To User back up to the Daemon Thread.
        indication_tx: Sender<Indication>,
    ) -> Self {
        let received_file_size = 0_u64;
        let timer = Timer::new(
            config.inactivity_timeout,
            config.max_count,
            config.eof_timeout,
            config.max_count,
            config.nak_timeout,
            config.max_count,
            config.progress_report_interval_secs,
        );

        let mut transaction = Self {
            status: TransactionStatus::Undefined,
            config,
            filestore,
            indication_tx,
            file_handle: None,
            saved_segments: Segments::new(),
            nak_procedure,
            metadata: None,
            received_file_size,
            header: None,
            condition: Condition::NoError,
            delivery_code: DeliveryCode::Incomplete,
            file_status: FileStatusCode::Unreported,
            timer,
            checksum: None,
            state: TransactionState::Active,
            recv_state: RecvState::ReceiveData,
            empty_nak: None,
            file_size: None,
            naks: VecDeque::new(),
            nak_received_file_size: received_file_size,
            delayed_nack_timers: Vec::new(),
            receive_date: Utc::now(),
        };
        transaction.timer.restart_inactivity();
        transaction.timer.reset_progress_report();
        transaction
    }

    pub(crate) fn has_pdu_to_send(&self) -> bool {
        match self.recv_state {
            RecvState::ReceiveData => self.empty_nak.is_some() || !self.naks.is_empty(),
            RecvState::Finished => self.empty_nak.as_ref().is_some_and(|x| x.1),
        }
    }

    // returns the time until the first timeout
    pub(crate) fn until_timeout(&self) -> Duration {
        let d = self.timer.until_timeout();
        // we only check the first delayed nack timer (if any)
        // because they are ordered chronological
        self.delayed_nack_timers
            .first()
            .map_or(d, |(counter, _, _)| {
                std::cmp::min(d, counter.until_timeout())
            })
    }

    pub(crate) fn send_pdu(&mut self, permit: Permit<(EntityID, PDU)>) -> TransactionResult<()> {
        match self.recv_state {
            RecvState::ReceiveData => {
                if self.empty_nak.is_some() {
                    self.send_empty_nak(permit)?;
                } else if !self.naks.is_empty() {
                    self.send_naks(permit)?;
                }
            }
            RecvState::Finished => {
                if self.empty_nak.is_some() {
                    self.send_empty_nak(permit)?;
                }
            }
        }

        Ok(())
    }

    pub fn handle_timeout(&mut self) -> TransactionResult<()> {
        let mut idx = 0;
        for (i, (counter, _, _)) in self.delayed_nack_timers.iter_mut().enumerate() {
            if counter.timeout_occurred() {
                idx = i + 1;
            } else {
                break;
            }
        }

        if idx > 0 {
            for (_, start, end) in self.delayed_nack_timers.drain(0..idx) {
                for (start_offset, end_offset) in self.saved_segments.gaps(start, end) {
                    self.naks.push_back(SegmentRequestForm {
                        start_offset,
                        end_offset,
                    });
                }
            }
        }

        if self.timer.inactivity.limit_reached() {
            self.handle_fault(Condition::InactivityDetected)?;
        } else if self.timer.inactivity.timeout_occurred() {
            self.timer.restart_inactivity();
        }

        match self.recv_state {
            RecvState::ReceiveData => {
                if self.timer.nak.timeout_occurred() {
                    self.naks = self.get_all_naks();
                }
            }
            RecvState::Finished => {
                if self.timer.nak.limit_reached() {
                    self.handle_fault(Condition::PositiveLimitReached)?;
                } else if self.timer.nak.timeout_occurred() {
                    self.set_finished_flag(true);
                    self.timer.restart_nak();
                }
            }
        }

        Ok(())
    }

    // return true if:
    // - metadata has not been received
    // - there is a gap in the data received
    // - EOF has been received and either no data has been received
    //   or there is a gap between the last segment received and the end of file according to the file_size received in the EOF
    fn has_naks(&self) -> bool {
        self.metadata.is_none() || {
            if let Some(file_size) = self.file_size {
                //eof received
                !self.saved_segments.is_complete(file_size)
            } else {
                //eof not received
                self.saved_segments.len() > 1
            }
        }
    }

    fn eof_received(&self) -> bool {
        self.file_size.is_some()
    }

    pub fn get_status(&self) -> TransactionStatus {
        self.status
    }

    pub(crate) fn get_state(&self) -> TransactionState {
        self.state
    }

    fn generate_report(&self) -> Report {
        Report {
            id: self.id(),
            state: self.get_state(),
            status: self.get_status(),
            condition: self.condition,
            empty_nak_received: false,
            file_size: self.metadata.as_ref().map_or(0, |meta| meta.file_size),
            file_bytes_received: Some(self.received_file_size),
            file_bytes_sent: None,
            direction: self.header.as_ref().map(|h| h.direction),
            file_name: self
                .metadata
                .as_ref()
                .map_or("Unknown".to_string(), |meta| {
                    meta.destination_filename.to_string()
                }),
            submit_date: self.receive_date,
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
    fn initialize_tempfile(&mut self) -> TransactionResult<()> {
        self.file_handle = Some(self.filestore.open_tempfile()?);
        Ok(())
    }

    fn get_handle(&mut self) -> TransactionResult<&mut File> {
        let id = self.id();
        if self.file_handle.is_none() {
            self.initialize_tempfile()?
        };
        self.file_handle
            .as_mut()
            .ok_or(TransactionError::NoFile(id))
    }

    fn is_file_transfer(&self) -> bool {
        self.metadata
            .as_ref()
            .map(|meta| !meta.source_filename.as_os_str().is_empty())
            .unwrap_or(false)
    }

    fn store_file_data(&mut self, pdu: FileDataPDU) -> TransactionResult<(u64, usize)> {
        let (offset, file_data) = (pdu.0.offset, pdu.0.file_data);
        let length = file_data.len();

        if length > 0 {
            let handle = self.get_handle()?;
            handle
                .seek(SeekFrom::Start(offset))
                .map_err(FileStoreError::IO)?;
            handle
                .write_all(file_data.as_slice())
                .map_err(FileStoreError::IO)?;
            let new_data_received = self
                .saved_segments
                .merge((offset, offset + file_data.len() as u64));
            self.received_file_size += new_data_received;
        } else {
            warn!(
                "Received FileDataPDU with invalid file_data.length = {}; ignored",
                length
            );
        }

        Ok((offset, length))
    }

    fn finalize_file(&mut self) -> TransactionResult<FileStatusCode> {
        let id = self.id();
        {
            let mut outfile = self.filestore.open(
                self.metadata
                    .as_ref()
                    .map(|meta| meta.destination_filename.clone())
                    .ok_or(TransactionError::NoFile(id))?,
                File::options().create(true).write(true).truncate(true),
            )?;
            let handle = self.get_handle()?;
            // rewind to the beginning of the file.
            // this might not be necessary with the io call that follows
            handle.rewind().map_err(FileStoreError::IO)?;
            io::copy(handle, &mut outfile).map_err(FileStoreError::IO)?;
            outfile.sync_all().map_err(FileStoreError::IO)?;
        }
        // Drop the temporary file
        self.file_handle = None;

        Ok(FileStatusCode::Retained)
    }

    pub fn shutdown(&mut self) {
        debug!("Transaction {0} shutting down.", self.id());
        self.status = TransactionStatus::Terminated;
        self.state = TransactionState::Terminated;
        self.timer.nak.pause();
        self.timer.inactivity.pause();
    }

    /// Take action according to the defined handler mapping.
    /// Returns a boolean indicating if the calling function should continue (true) or not (false.)
    fn handle_fault(&mut self, condition: Condition) -> TransactionResult<bool> {
        self.condition = condition;
        warn!("Transaction {} Handling fault {:?}", self.id(), condition);
        match self
            .config
            .fault_handler_override
            .get(&self.condition)
            .unwrap_or(&FaultHandlerAction::Abandon)
        {
            FaultHandlerAction::Ignore => {
                // Log ignoring error
                Ok(true)
            }
            FaultHandlerAction::Abandon => {
                self.shutdown();
                Ok(false)
            }
        }
    }

    fn check_file_size(&mut self, file_size: u64) -> TransactionResult<()> {
        if self.saved_segments.end_or_0() > file_size {
            warn!(
                "EOF file size {} is smaller than file size received in file data {}",
                file_size, self.received_file_size
            );
            // we will always exit here anyway
            self.handle_fault(Condition::FilesizeError)?;
        }
        Ok(())
    }

    fn prepare_empty_nak(&mut self) {
        self.empty_nak = Some((
            NegativeAcknowledgmentPDU {
                start_of_scope: 0,
                end_of_scope: self.received_file_size,
                segment_requests: vec![],
            },
            true,
        ));
    }

    // set the true flag on the fin such that it is sent at the next opportunity
    fn set_finished_flag(&mut self, flag: bool) {
        if let Some(x) = self.empty_nak.as_mut() {
            x.1 = flag;
        }
    }

    fn get_all_naks(&self) -> VecDeque<SegmentRequestForm> {
        let mut naks: VecDeque<SegmentRequestForm> = VecDeque::new();

        let segments = &self.saved_segments;

        if self.metadata.is_none() {
            naks.push_back((0_u64, 0_u64).into());
        }
        for (start_offset, end_offset) in
            segments.gaps(0, self.file_size.unwrap_or(segments.end_or_0()))
        {
            naks.push_back(SegmentRequestForm {
                start_offset,
                end_offset,
            });
        }

        naks
    }

    fn send_empty_nak(&mut self, permit: Permit<(EntityID, PDU)>) -> TransactionResult<()> {
        let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
            start_of_scope: 0,
            end_of_scope: self.received_file_size,
            segment_requests: vec![],
        }));
        let payload_len = payload.encoded_len(self.config.file_size_flag);

        let header = self.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );

        let destination = header.source_entity_id;
        let pdu = PDU { header, payload };

        // TODO - figure out a way to send this periodically up to a timeout.
        // Currently, this will break the sender because the sender shutdowns on receipt of an empty NAK and the transport closes.
        info!("Sending empty NAK");
        permit.send((destination, pdu));
        self.shutdown();

        Ok(())
    }

    fn send_naks(&mut self, permit: Permit<(EntityID, PDU)>) -> TransactionResult<()> {
        if self.nak_received_file_size == self.received_file_size {
            if self.timer.nak.limit_reached() {
                self.handle_fault(Condition::NakLimitReached)?;
                return Ok(());
            }
            self.timer.restart_nak();
        } else {
            // new data has been received since last NAK was send; reset the counter to 0
            self.timer.reset_nak();
            self.nak_received_file_size = self.received_file_size;
        }

        let n = usize::min(
            self.naks.len(),
            NegativeAcknowledgmentPDU::max_nak_num(
                self.config.file_size_flag,
                self.config.file_size_segment as u32,
            ) as usize,
        );

        let segment_requests: Vec<SegmentRequestForm> = self.naks.drain(..n).collect();
        let scope_start = segment_requests
            .first()
            .map(|sr| sr.start_offset)
            .unwrap_or(0);
        let scope_end = segment_requests
            .last()
            .map(|sr| sr.end_offset)
            .unwrap_or(self.saved_segments.end().unwrap_or(0));

        let nak = NegativeAcknowledgmentPDU {
            start_of_scope: scope_start,
            end_of_scope: scope_end,
            segment_requests,
        };
        debug!("Transaction {}: sending NAK for {} segments", self.id(), n);

        let payload = PDUPayload::Directive(Operations::Nak(nak));
        let payload_len = payload.encoded_len(self.config.file_size_flag);

        let header = self.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let destination = header.source_entity_id;
        let pdu = PDU { header, payload };

        permit.send((destination, pdu));

        Ok(())
    }

    fn verify_checksum(&mut self, checksum: u32) -> TransactionResult<bool> {
        let checksum_type = self
            .metadata
            .as_ref()
            .map(|meta| meta.checksum_type)
            .ok_or_else(|| {
                let id = self.id();
                TransactionError::MissingMetadata(id)
            })?;
        let handle = self.get_handle()?;
        handle.sync_all().map_err(FileStoreError::IO)?;
        Ok(handle.checksum(checksum_type)? == checksum)
    }

    fn finalize_receive(&mut self) -> TransactionResult<()> {
        let checksum = self.checksum.ok_or(TransactionError::NoChecksum)?;
        self.delivery_code = DeliveryCode::Complete;

        self.file_status = if self.is_file_transfer() {
            if !self.verify_checksum(checksum)?
                && !self.handle_fault(Condition::FileChecksumFailure)?
            {
                return Ok(());
            }
            self.finalize_file()
                .unwrap_or(FileStatusCode::FileStoreRejection)
        } else {
            FileStatusCode::Unreported
        };
        // A filestore rejection is a failure mode for the entire transaction.
        if self.file_status == FileStatusCode::FileStoreRejection
            && !self.handle_fault(Condition::FileStoreRejection)?
        {
            return Ok(());
        }
        // send indication this transaction is finished.
        self.send_indication(Indication::Finished(FinishedIndication {
            id: self.id(),
            report: self.generate_report(),
            file_status: self.file_status,
            delivery_code: self.delivery_code,
        }));
        Ok(())
    }

    pub fn process_pdu(&mut self, pdu: PDU) -> TransactionResult<()> {
        self.timer.reset_inactivity();
        let PDU {
            header: _header,
            payload,
        } = pdu;
        if self.timer.progress_report.timeout_occurred() {
            self.timer.reset_progress_report();
            self.send_indication(Indication::Report(self.generate_report()));
        }

        match &self.config.transmission_mode {
            TransmissionMode::Acknowledged => {
                match payload {
                    PDUPayload::FileData(filedata) => {
                        // the end of the last segment
                        let prev_end = self.saved_segments.end().unwrap_or(0);

                        let (offset, length) = self.store_file_data(filedata)?;

                        self.send_indication(Indication::FileSegmentRecv(FileSegmentIndication {
                            id: self.id(),
                            offset,
                            length: length as u64,
                        }));

                        if let NakProcedure::Immediate(delay) = self.nak_procedure {
                            if !self.eof_received() {
                                if self.timer.nak.timeout_occurred() {
                                    // send all gaps at the next opportunity
                                    self.naks = self.get_all_naks();
                                    self.timer.restart_nak();
                                } else if offset > prev_end {
                                    // new gap
                                    if delay.is_zero() {
                                        // send it at the next opportunity
                                        self.naks.push_back(SegmentRequestForm {
                                            start_offset: prev_end,
                                            end_offset: offset,
                                        });
                                    } else {
                                        // start a timer to check if the gap still persists after the delta delay
                                        let mut timer = Counter::new(delay, 1);
                                        timer.start();
                                        self.delayed_nack_timers.push((timer, prev_end, offset));
                                    }
                                }
                            }
                        }
                        self.check_finished()?;

                        Ok(())
                    }
                    PDUPayload::Directive(operation) => {
                        match operation {
                            Operations::EoF(eof) => {
                                debug!("Transaction {0} received EndOfFile.", self.id());
                                self.condition = eof.condition;
                                self.checksum = Some(eof.checksum);

                                self.send_indication(Indication::EoFRecv(self.id()));

                                if self.condition == Condition::NoError {
                                    self.check_file_size(eof.file_size)?;
                                    self.file_size = Some(eof.file_size);

                                    self.check_finished()?;

                                    if self.has_naks() {
                                        let delay = match self.nak_procedure {
                                            NakProcedure::Immediate(d) => d,
                                            NakProcedure::Deferred(d) => d,
                                        };

                                        if delay.is_zero() {
                                            //send all gaps at the next opportunity
                                            self.naks = self.get_all_naks();
                                        } else {
                                            // we need to check/send the gaps only after this delta has expired
                                            // start a counter for that
                                            self.delayed_nack_timers.push((
                                                Counter::new(delay, 1),
                                                0,
                                                eof.file_size,
                                            ));
                                        }
                                    } else {
                                        self.prepare_empty_nak();
                                    }
                                } else {
                                    // Any other condition is essentially a
                                    // SHUTDOWN operation
                                    self.shutdown();
                                }
                                Ok(())
                            }
                            Operations::Metadata(metadata) => {
                                if self.metadata.is_none() {
                                    debug!("Transaction {0} received Metadata.", self.id());
                                    // push each request up to the Daemon
                                    let source_filename: Utf8PathBuf = metadata.source_filename;
                                    let destination_filename: Utf8PathBuf =
                                        metadata.destination_filename;

                                    self.send_indication(Indication::MetadataRecv(
                                        MetadataRecvIndication {
                                            id: self.id(),
                                            source_filename: source_filename.clone(),
                                            destination_filename: destination_filename.clone(),
                                            file_size: metadata.file_size,
                                            transmission_mode: self.config.transmission_mode,
                                        },
                                    ));

                                    self.metadata = Some(Metadata {
                                        source_filename,
                                        destination_filename,
                                        file_size: metadata.file_size,
                                        checksum_type: metadata.checksum_type,
                                    });
                                    self.check_finished()?;
                                }
                                Ok(())
                            }
                            Operations::Nak(_nak) => Err(TransactionError::UnexpectedPDU(
                                self.config.sequence_number,
                                self.config.transmission_mode,
                                "NAK PDU".to_owned(),
                            )),
                        }
                    }
                }
            }
            TransmissionMode::Unacknowledged => {
                match payload {
                    PDUPayload::FileData(filedata) => {
                        let (offset, length) = self.store_file_data(filedata)?;
                        self.send_indication(Indication::FileSegmentRecv(FileSegmentIndication {
                            id: self.id(),
                            offset,
                            length: length as u64,
                        }));
                        Ok(())
                    }
                    PDUPayload::Directive(operation) => {
                        match operation {
                            Operations::EoF(eof) => {
                                self.condition = eof.condition;
                                self.checksum = Some(eof.checksum);

                                self.send_indication(Indication::EoFRecv(self.id()));

                                if self.condition == Condition::NoError {
                                    self.check_file_size(eof.file_size)?;
                                    self.finalize_receive()?;
                                } else {
                                    // Any other condition is essentially a
                                    // SHUTDOWN operation
                                    self.shutdown()
                                }
                                Ok(())
                            }
                            Operations::Metadata(metadata) => {
                                if self.metadata.is_none() {
                                    let source_filename: Utf8PathBuf = metadata.source_filename;
                                    let destination_filename: Utf8PathBuf =
                                        metadata.destination_filename;

                                    self.send_indication(Indication::MetadataRecv(
                                        MetadataRecvIndication {
                                            id: self.id(),
                                            source_filename: source_filename.clone(),
                                            destination_filename: destination_filename.clone(),
                                            file_size: metadata.file_size,
                                            transmission_mode: self.config.transmission_mode,
                                        },
                                    ));

                                    self.metadata = Some(Metadata {
                                        source_filename,
                                        destination_filename,
                                        file_size: metadata.file_size,
                                        checksum_type: metadata.checksum_type,
                                    });
                                }
                                Ok(())
                            }
                            Operations::Nak(_nak) => Err(TransactionError::UnexpectedPDU(
                                self.config.sequence_number,
                                self.config.transmission_mode,
                                "NAK PDU".to_owned(),
                            )),
                        }
                    }
                }
            }
        }
    }

    // go to the Finished state if the file is complete (used only in acknowledged mode)
    fn check_finished(&mut self) -> TransactionResult<()> {
        if self.metadata.is_some()
            && self.eof_received()
            && !(self.is_file_transfer() && self.has_naks())
        {
            self.finalize_receive()?;
            self.recv_state = RecvState::Finished;
            self.prepare_empty_nak();
            self.timer.nak.pause();
        }
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
    use std::{fs::OpenOptions, io::Read};

    use crate::assert_err;

    use crate::{
        filestore::{ChecksumType, NativeFileStore},
        pdu::{CRCFlag, EndOfFile, FileSizeFlag, MetadataPDU, SegmentedData, UnsegmentedFileData},
    };

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
    fn header(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (indication_tx, _) = channel(1);
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Deferred(Duration::ZERO),
            filestore,
            indication_tx,
        );
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
    fn test_if_file_transfer(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (indication_tx, _) = channel(10);
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Deferred(Duration::ZERO),
            filestore,
            indication_tx,
        );
        assert_eq!(
            TransactionStatus::Undefined,
            transaction.get_status().clone()
        );
        let mut path = Utf8PathBuf::new();
        path.push("a");

        transaction.metadata = Some(Metadata {
            file_size: 600_u64,
            source_filename: path.clone(),
            destination_filename: path.clone(),
            checksum_type: ChecksumType::Modular,
        });

        assert!(transaction.is_file_transfer())
    }

    #[rstest]
    fn store_filedata(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (indication_tx, _) = channel(10);
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Deferred(Duration::ZERO),
            filestore,
            indication_tx,
        );

        let input = vec![0, 5, 255, 99];
        let data = FileDataPDU(UnsegmentedFileData {
            offset: 6,
            file_data: input.clone(),
        });
        let (offset, length) = transaction
            .store_file_data(data)
            .expect("Error saving file data");

        assert_eq!(6, offset);
        assert_eq!(4, length);

        let handle = transaction.get_handle().unwrap();
        handle.seek(SeekFrom::Start(6)).unwrap();
        let mut buff = vec![0; 4];
        handle.read_exact(&mut buff).unwrap();

        assert_eq!(input, buff)
    }

    #[rstest]
    fn finalize_file(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (indication_tx, _) = channel(10);
        let config = default_config.clone();
        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));

        let input = "This is test data\n!";
        let output_file = {
            let mut temp_buff = Utf8PathBuf::new();
            temp_buff.push("finalize_test.txt");
            temp_buff
        };

        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Deferred(Duration::ZERO),
            filestore.clone(),
            indication_tx,
        );
        transaction.metadata = Some(Metadata {
            file_size: input.len() as u64,
            source_filename: output_file.clone(),
            destination_filename: output_file.clone(),
            checksum_type: ChecksumType::Modular,
        });

        let data = FileDataPDU(UnsegmentedFileData {
            offset: 0,
            file_data: input.as_bytes().to_vec(),
        });
        let (offset, length) = transaction
            .store_file_data(data)
            .expect("Error saving file data");

        assert_eq!(0, offset);
        assert_eq!(input.len(), length);

        let result = transaction
            .finalize_file()
            .expect("Error writing to finalize file.");
        assert_eq!(FileStatusCode::Retained, result);

        let mut out_string = String::new();
        let contents = {
            filestore
                .open(output_file, OpenOptions::new().read(true))
                .expect("Cannot open finalized file.")
                .read_to_string(&mut out_string)
                .expect("Cannot read finalized file.")
        };
        assert_eq!(input.len(), contents);
        assert_eq!(input, out_string)
    }

    #[rstest]
    #[tokio::test]
    async fn test_naks(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(FileSizeFlag::Small, FileSizeFlag::Large)] file_size_flag: FileSizeFlag,
    ) {
        let (transport_tx, mut transport_rx) = channel(10);
        let (indication_tx, _) = channel(10);
        let mut config = default_config.clone();
        config.file_size_flag = file_size_flag;
        let file_size = match &file_size_flag {
            FileSizeFlag::Small => 20,
            FileSizeFlag::Large => u32::MAX as u64 + 100_u64,
        };

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Deferred(Duration::ZERO),
            filestore,
            indication_tx,
        );

        let input = vec![0, 5, 255, 99];
        let data = FileDataPDU(UnsegmentedFileData {
            offset: 6,
            file_data: input,
        });
        let (offset, length) = transaction
            .store_file_data(data)
            .expect("Error saving file data");

        assert_eq!(6, offset);
        assert_eq!(4, length);

        transaction.file_size = Some(file_size);
        transaction.received_file_size = file_size;

        let expected: VecDeque<SegmentRequestForm> = vec![
            SegmentRequestForm::from((0_u64, 0_u64)),
            SegmentRequestForm::from((0_u64, 6_u64)),
            SegmentRequestForm::from((10_u64, file_size)),
        ]
        .into();

        assert_eq!(expected, transaction.get_all_naks());

        let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
            start_of_scope: 0,
            end_of_scope: file_size,
            segment_requests: expected.into(),
        }));

        let payload_len = payload.encoded_len(transaction.config.file_size_flag);
        let header = transaction.get_header(
            Direction::ToSender,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );

        let pdu = PDU { header, payload };

        transaction.naks = transaction.get_all_naks();
        transaction
            .send_naks(transport_tx.reserve().await.unwrap())
            .unwrap();

        let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
        let expected_id = default_config.source_entity_id;

        assert_eq!(expected_id, destination_id);
        assert_eq!(pdu, received_pdu);
    }

    #[rstest]
    fn pdu_error_unack(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(
            Operations::Nak(NegativeAcknowledgmentPDU{
                start_of_scope: 0_u64,
                end_of_scope: 1022_u64,
                segment_requests: vec![SegmentRequestForm::from((0_u32, 0_u32)), SegmentRequestForm::from((0_u32, 1022_u32))],
            })
        )]
        operation: Operations,
    ) {
        let (indication_tx, _) = channel(1);
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Unacknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Deferred(Duration::ZERO),
            filestore,
            indication_tx,
        );

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            file_size: 600,
            source_filename: path.clone(),
            destination_filename: path,
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);

        let header = transaction.get_header(
            Direction::ToReceiver,
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
    fn pdu_error_ack(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(
            Operations::Nak(NegativeAcknowledgmentPDU{
                start_of_scope: 0_u64,
                end_of_scope: 1022_u64,
                segment_requests: vec![SegmentRequestForm::from((0_u32, 0_u32)), SegmentRequestForm::from((0_u32, 1022_u32))],
            })
        )]
        operation: Operations,
    ) {
        let (indication_tx, _) = channel(1);
        let mut config = default_config.clone();
        config.transmission_mode = TransmissionMode::Acknowledged;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Deferred(Duration::ZERO),
            filestore,
            indication_tx,
        );

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            file_size: 600,
            source_filename: path.clone(),
            destination_filename: path,
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(operation);
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);

        let header = transaction.get_header(
            Direction::ToReceiver,
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
    async fn recv_store_data(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (indication_tx, _indication_rx) = channel(1);
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Deferred(Duration::ZERO),
            filestore,
            indication_tx,
        );

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            file_size: 600,
            source_filename: path.clone(),
            destination_filename: path,
            checksum_type: ChecksumType::Modular,
        });

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

        transaction.process_pdu(pdu).unwrap();
        let contents: Vec<u8> = {
            let mut buf = Vec::new();
            let handle = transaction.get_handle().unwrap();
            handle.rewind().unwrap();
            handle.read_to_end(&mut buf).unwrap();
            buf
        };
        let expected = {
            let mut buf = vec![0_u8; 12];
            buf.extend(0..12_u8);
            buf
        };
        assert_eq!(expected, contents);
    }

    #[rstest]
    #[tokio::test]
    async fn recv_store_metadata(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (indication_tx, _) = channel(1);
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Deferred(Duration::ZERO),
            filestore,
            indication_tx,
        );

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        let expected = Some(Metadata {
            file_size: 600,
            source_filename: path.clone(),
            destination_filename: path,
            checksum_type: ChecksumType::Modular,
        });

        let payload = PDUPayload::Directive(Operations::Metadata(MetadataPDU {
            checksum_type: ChecksumType::Modular,
            file_size: 600,
            source_filename: "Test_file.txt".into(),
            destination_filename: "Test_file.txt".into(),
        }));
        let payload_len = payload.encoded_len(transaction.config.file_size_flag);

        let header = transaction.get_header(
            Direction::ToReceiver,
            PDUType::FileDirective,
            payload_len,
            SegmentationControl::NotPreserved,
        );
        let pdu = PDU { header, payload };

        transaction.process_pdu(pdu).unwrap();
        assert_eq!(expected, transaction.metadata);
    }

    #[rstest]
    #[tokio::test]
    async fn recv_eof_all_data(
        default_config: &TransactionConfig,
        tempdir_fixture: &TempDir,
        #[values(TransmissionMode::Unacknowledged, TransmissionMode::Acknowledged)]
        transmission_mode: TransmissionMode,
    ) {
        let (transport_tx, mut transport_rx) = channel(10);
        let (indication_tx, _indication_rx) = channel(1);
        let mut config = default_config.clone();
        config.transmission_mode = transmission_mode;

        let expected_id = config.source_entity_id;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Deferred(Duration::ZERO),
            filestore,
            indication_tx,
        );

        let path = {
            let mut path = Utf8PathBuf::new();
            path.push("Test_file.txt");
            path
        };

        transaction.metadata = Some(Metadata {
            file_size: 600,
            source_filename: path.clone(),
            destination_filename: path,
            checksum_type: ChecksumType::Modular,
        });

        let input_data = "Some_test words!\nHello\nWorld!";

        let (checksum, _overflow) =
            input_data
                .as_bytes()
                .chunks(4)
                .fold((0_u32, false), |accum: (u32, bool), chunk| {
                    let mut vec = vec![0_u8; 4];
                    vec[..chunk.len()].copy_from_slice(chunk);
                    accum
                        .0
                        .overflowing_add(u32::from_be_bytes(vec.try_into().unwrap()))
                });

        let file_pdu = {
            let payload = PDUPayload::FileData(FileDataPDU(UnsegmentedFileData {
                offset: 0,
                file_data: input_data.as_bytes().to_vec(),
            }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileData,
                payload_len,
                SegmentationControl::NotPreserved,
            );
            PDU { header, payload }
        };

        let input_pdu = {
            let payload = PDUPayload::Directive(Operations::EoF(EndOfFile {
                condition: Condition::NoError,
                checksum,
                file_size: input_data.len() as u64,
            }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileDirective,
                payload_len,
                SegmentationControl::NotPreserved,
            );
            PDU { header, payload }
        };

        let expected_pdu = {
            let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
                start_of_scope: 0,
                end_of_scope: input_data.len() as u64,
                segment_requests: vec![],
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

        {
            // this is the whole contents of the file
            transaction.process_pdu(file_pdu).unwrap();

            transaction.process_pdu(input_pdu).unwrap();
            transaction
                .send_pdu(transport_tx.reserve().await.unwrap())
                .unwrap();

            if transaction.config.transmission_mode == TransmissionMode::Acknowledged {
                transaction
                    .send_pdu(transport_tx.reserve().await.unwrap())
                    .unwrap();

                let eof = {
                    let payload = PDUPayload::Directive(Operations::EoF(EndOfFile {
                        condition: Condition::NoError,
                        checksum,
                        file_size: input_data.len() as u64,
                    }));

                    let payload_len = payload.encoded_len(transaction.config.file_size_flag);

                    let header = transaction.get_header(
                        Direction::ToReceiver,
                        PDUType::FileDirective,
                        payload_len,
                        SegmentationControl::NotPreserved,
                    );

                    PDU { header, payload }
                };
                transaction.process_pdu(eof).unwrap();

                assert!(!transaction.timer.nak.is_ticking());
            }
        }

        // need to get the EoF from Acknowledged mode too.
        if transmission_mode == TransmissionMode::Acknowledged {
            let expected_pdu = {
                let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
                    start_of_scope: 0,
                    end_of_scope: input_data.len() as u64,
                    segment_requests: vec![],
                }));

                let payload_len = payload.encoded_len(expected_pdu.header.large_file_flag);

                let header = {
                    let mut head = expected_pdu.header.clone();
                    head.pdu_data_field_length = payload_len;
                    head
                };

                PDU { header, payload }
            };
            let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
            assert_eq!(expected_id, destination_id);
            assert_eq!(expected_pdu, received_pdu)
        }

        // Unacknowledged mode does not wait for an empty NAK
        if transmission_mode == TransmissionMode::Acknowledged {
            let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
            assert_eq!(expected_id, destination_id);
            assert_eq!(expected_pdu, received_pdu)
        }
    }

    #[rstest]
    #[tokio::test]
    async fn nak_split(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, mut transport_rx) = channel(10);
        let (indication_tx, _indication_rx) = channel(10);
        let mut config = default_config.clone();
        config.file_size_segment = 16;
        config.transmission_mode = TransmissionMode::Acknowledged;

        let expected_id = config.source_entity_id;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Immediate(Duration::ZERO),
            filestore,
            indication_tx,
        );

        let file_pdu1 = {
            let payload = PDUPayload::FileData(FileDataPDU(UnsegmentedFileData {
                offset: 16,
                file_data: vec![0; 16],
            }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileData,
                payload_len,
                SegmentationControl::NotPreserved,
            );
            PDU { header, payload }
        };

        let file_pdu2 = {
            let payload = PDUPayload::FileData(FileDataPDU(UnsegmentedFileData {
                offset: 48,
                file_data: vec![0; 16],
            }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileData,
                payload_len,
                SegmentationControl::NotPreserved,
            );
            PDU { header, payload }
        };

        let expected_nak1 = {
            let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
                start_of_scope: 0,
                end_of_scope: 16,
                segment_requests: vec![SegmentRequestForm {
                    start_offset: 0,
                    end_offset: 16,
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

        let expected_nak2 = {
            let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
                start_of_scope: 32,
                end_of_scope: 48,
                segment_requests: vec![SegmentRequestForm {
                    start_offset: 32,
                    end_offset: 48,
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

        transaction.process_pdu(file_pdu1).unwrap();
        transaction.process_pdu(file_pdu2).unwrap();

        assert!(transaction.has_pdu_to_send());
        transaction
            .send_pdu(transport_tx.reserve().await.unwrap())
            .unwrap();
        assert!(transaction.has_pdu_to_send());
        transaction
            .send_pdu(transport_tx.reserve().await.unwrap())
            .unwrap();

        let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
        assert_eq!(expected_id, destination_id);
        assert_eq!(expected_nak1, received_pdu);
        let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
        assert_eq!(expected_id, destination_id);
        assert_eq!(expected_nak2, received_pdu)
    }

    #[rstest]
    #[tokio::test]
    async fn delayed_nak(default_config: &TransactionConfig, tempdir_fixture: &TempDir) {
        let (transport_tx, mut transport_rx) = channel(10);
        let (indication_tx, _indication_rx) = channel(10);
        let mut config = default_config.clone();
        config.file_size_segment = 16;
        config.transmission_mode = TransmissionMode::Acknowledged;

        let expected_id = config.source_entity_id;

        let filestore = Arc::new(NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        ));
        let mut transaction = RecvTransaction::new(
            config,
            NakProcedure::Immediate(Duration::from_millis(100)),
            filestore,
            indication_tx,
        );

        let file_pdu1 = {
            let payload = PDUPayload::FileData(FileDataPDU(UnsegmentedFileData {
                offset: 16,
                file_data: vec![0; 16],
            }));
            let payload_len = payload.encoded_len(transaction.config.file_size_flag);

            let header = transaction.get_header(
                Direction::ToReceiver,
                PDUType::FileData,
                payload_len,
                SegmentationControl::NotPreserved,
            );
            PDU { header, payload }
        };

        let expected_nak1 = {
            let payload = PDUPayload::Directive(Operations::Nak(NegativeAcknowledgmentPDU {
                start_of_scope: 0,
                end_of_scope: 16,
                segment_requests: vec![SegmentRequestForm {
                    start_offset: 0,
                    end_offset: 16,
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

        transaction.process_pdu(file_pdu1).unwrap();
        assert!(!transaction.has_pdu_to_send());

        let d = transaction.until_timeout().as_millis();
        assert!(d <= 100);

        tokio::time::sleep(Duration::from_millis(150)).await;

        transaction.handle_timeout().unwrap();

        assert!(transaction.has_pdu_to_send());

        transaction
            .send_pdu(transport_tx.reserve().await.unwrap())
            .unwrap();
        assert!(!transaction.has_pdu_to_send());

        let (destination_id, received_pdu) = transport_rx.recv().await.unwrap();
        assert_eq!(expected_id, destination_id);
        assert_eq!(expected_nak1, received_pdu);
    }
}
