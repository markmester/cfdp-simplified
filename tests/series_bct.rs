use std::{thread, time::Duration};

use camino::Utf8PathBuf;
use cfdp_simplified::{
    daemon::{PutRequest, Report},
    filestore::FileStore,
    pdu::{Condition, EntityID, PDUDirective, TransactionSeqNum, TransmissionMode},
    transaction::TransactionID,
};
use rstest::{fixture, rstest};

mod common;
use common::*;

#[rstest]
#[timeout(Duration::from_secs(10))]
// BCT Series
// Test goal:
//  - Confirm Empty Nak is received
//  - Confirm bytes sent >= file size
//  - Confirm file is transferred
// Configuration:
//  - Acknowledged
//  - File Size: Medium
fn bcts01(get_filestore: &UsersAndFilestore) {
    initialize();
    let (local_user, _remote_user, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/medium_bcts01.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }

    let tid = TransactionID::from(EntityID::from(0_u16), TransactionSeqNum::from(0_u32));
    let local_user = local_user.report(tid).unwrap().unwrap();

    assert!(local_user.empty_nak_received);
    assert!(local_user.file_bytes_sent.unwrap_or(0_u64) >= local_user.file_size);
    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_bcts02(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        None,
        Some(TransportIssue::All(vec![PDUDirective::Nak])),
        [Some(10), Some(1), Some(1), Some(1)],
        None,
    )
}

// BCT Series
// Test goal:
//  - Empty Nack is not sent by receiver
// Configuration:
//  - Acknowledged
//  - Drop empty nack
//  - File Size: Medium
#[rstest]
#[timeout(Duration::from_secs(10))]
fn bcts02(fixture_bcts02: &'static EntityConstructorReturn) {
    initialize();
    let (local_user, remote_user, _, _local, _remote) = fixture_bcts02;

    let out_file: Utf8PathBuf = "remote/medium_bcts02.txt".into();

    let id = local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
        })
        .expect("unable to send put request.");

    // wait long enough for the nak limit to be reached
    while remote_user
        .report(id)
        .expect("Unable to send Report Request.")
        .is_none()
    {
        thread::sleep(Duration::from_millis(100))
    }

    let mut report = remote_user
        .report(id)
        .expect("Unable to send Report Request.")
        .unwrap();

    while report.condition != Condition::NakLimitReached {
        thread::sleep(Duration::from_millis(100));
        report = remote_user
            .report(id)
            .expect("Unable to send Report Request.")
            .unwrap();
    }

    assert_eq!(report.condition, Condition::NakLimitReached)
}

#[fixture]
#[once]
fn fixture_bcts03(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        None,
        None,
        [Some(10), Some(1), Some(1), Some(1)],
        None,
    )
}

// BCT Series
// Test goal:
//  - Retrieve transaction history
// Configuration:
//  - Acknowledged
//  - File Size: Small, Medium(2)
#[rstest]
#[timeout(Duration::from_secs(10))]
fn bcts03(fixture_bcts03: &'static EntityConstructorReturn) {
    initialize();
    let (local_user, remote_user, filestore, _local, _remote) = fixture_bcts03;

    let out_file_1: Utf8PathBuf = "remote/medium_bcts03-0.txt".into();
    local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file_1,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Unacknowledged,
        })
        .expect("unable to send put request.");

    let out_file_2: Utf8PathBuf = "remote/medium_bcts03-1.txt".into();
    local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file_2,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Unacknowledged,
        })
        .expect("unable to send put request.");

    let out_file_3: Utf8PathBuf = "remote/small_bcts03-2.txt".into();
    let path_to_out = filestore.get_native_path(&out_file_3);
    local_user
        .put(PutRequest {
            source_filename: "local/small.txt".into(),
            destination_filename: out_file_3,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Unacknowledged,
        })
        .expect("unable to send put request.");

    // Wait for 2nd transaction to finish
    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }

    let remote_history: Vec<Report> = remote_user.history().unwrap();

    assert_eq!(remote_history.len(), 3);
    assert!(path_to_out.exists())
}
