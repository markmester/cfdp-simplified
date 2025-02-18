use std::{thread, time::Duration};

use camino::Utf8PathBuf;
use cfdp_simplified::{
    daemon::PutRequest,
    filestore::FileStore,
    pdu::{EntityID, TransmissionMode},
};
use rstest::{fixture, rstest};

mod common;
use common::*;

#[rstest]
#[timeout(Duration::from_secs(200))]
// Series F1
// Sequence 1 Test
// Test goal:
//  - Establish one-way connectivity
// Configuration:
//  - Unacknowledged
//  - File Size: Small (file fits in single pdu)
fn f1s01(get_filestore: &UsersAndFilestore) {
    initialize();
    let (local_user, _remote_user, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/small_f1s01.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    local_user
        .put(PutRequest {
            source_filename: "local/small.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Unacknowledged,
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }
    assert!(path_to_out.exists())
}

#[rstest]
#[timeout(Duration::from_secs(5))]
// Series F1
// Sequence 2 Test
// Test goal:
//  - Execute Multiple File Data PDUs
// Configuration:
//  - Unacknowledged
//  - File Size: Medium
fn f1s02(get_filestore: &UsersAndFilestore) {
    initialize();
    let (local_user, _remote_user, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/medium_f1s02.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Unacknowledged,
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }
    assert!(path_to_out.exists())
}

#[rstest]
#[timeout(Duration::from_secs(10))]
// Series F1
// Sequence 3 Test
// Test goal:
//  - Execute Two way communication
// Configuration:
//  - Acknowledged
//  - File Size: Medium
fn f1s03(get_filestore: &UsersAndFilestore) {
    initialize();
    let (local_user, _remote_user, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/medium_f1s03.txt".into();
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

    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_f1s04(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::Rate(13)),
        None,
        [None; 4],
        None,
    )
}

#[rstest]
#[timeout(Duration::from_secs(30))]
// Series F1
// Sequence 4 Test
// Test goal:
//  - Recovery of Lost data
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - ~1% data lost in transport
fn f1s04(fixture_f1s04: &'static EntityConstructorReturn) {
    initialize();
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f1s04;
    let out_file: Utf8PathBuf = "remote/medium_f1s04.txt".into();
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

    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_f1s05(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::Duplicate(13)),
        None,
        [None; 4],
        None,
    )
}

#[rstest]
#[timeout(Duration::from_secs(30))]
// Series F1
// Sequence 5 Test
// Test goal:
//  - Ignoring duplicated data
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - ~1% data duplicated in transport
fn f1s05(fixture_f1s05: &'static EntityConstructorReturn) {
    initialize();
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f1s05;

    let out_file: Utf8PathBuf = "remote/medium_f1s05.txt".into();
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

    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_f1s06(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::Reorder(13)),
        None,
        [None; 4],
        None,
    )
}

#[rstest]
#[timeout(Duration::from_secs(5))]
// Series F1
// Sequence 6 Test
// Test goal:
//  - Reorder Data test
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - ~1% data re-ordered in transport
fn f1s06(fixture_f1s06: &'static EntityConstructorReturn) {
    initialize();
    // let mut user = User::new(Some(_local_path))
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f1s06;

    let out_file: Utf8PathBuf = "remote/medium_f1s06.txt".into();
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

    assert!(path_to_out.exists())
}
