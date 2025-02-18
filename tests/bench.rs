use std::{
    cmp,
    collections::HashMap,
    fs::OpenOptions,
    io::{BufWriter, Write},
    thread,
    time::{Duration, Instant},
};

use cfdp::{
    daemon::{EntityConfig, NakProcedure, PutRequest},
    filestore::{ChecksumType, FileStore},
    pdu::{CRCFlag, Condition, EntityID, PDUDirective, TransmissionMode},
    transaction::FaultHandlerAction,
};
use log::info;
use rstest::{fixture, rstest};

mod common;
use common::*;

#[fixture]
#[once]
fn fixture_bench01(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        None,
        Some(TransportIssue::All(vec![PDUDirective::Nak])),
        [Some(10), Some(1), Some(1), Some(1)],
        Some(EntityConfig {
            fault_handler_override: HashMap::from([(
                Condition::PositiveLimitReached,
                FaultHandlerAction::Abandon,
            )]),
            file_size_segment: 4096,
            default_transaction_max_count: 2,
            inactivity_timeout: 1,
            eof_timeout: 1,
            nak_timeout: 1,
            crc_flag: CRCFlag::NotPresent,
            checksum_type: ChecksumType::Null,
            nak_procedure: NakProcedure::Deferred(Duration::ZERO),
            local_entity_id: 0_u16,
            remote_entity_id: 1_u16,
            local_server_addr: "127.0.0.1:0",
            remote_server_addr: "127.0.0.1:0",
            progress_report_interval_secs: 1,
        }),
    )
}

#[cfg_attr(not(feature = "benches"), ignore)]
#[rstest]
#[timeout(Duration::from_secs(120))]
// Bench Series
// Test goal:
//  - Benchmark the transfer of a 1GB file.
//  - File sent in chunk sizes of 4096
//  - Should process at ~800mbps (1600mps release mode)
fn bench01(fixture_bench01: &'static EntityConstructorReturn) {
    initialize();

    let in_file = "local/smdp_bench01.txt";
    let out_file = "remote/smdp_bench01.txt";
    let file_size = 1024 * 1024 * 1024; // 1GB

    let (local_user, _remote_user, filestore, _local, _remote) = fixture_bench01;

    filestore.create_file(in_file).unwrap();
    let f = filestore
        .open(in_file, &mut OpenOptions::new().write(true))
        .unwrap();
    let mut writer = BufWriter::new(f);
    let mut buffer = [0u8; 1024];
    let mut file_bytes_to_write = file_size;

    while file_bytes_to_write > 0 {
        let to_write = cmp::min(file_bytes_to_write, buffer.len());
        let buffer = &mut buffer[..to_write];
        writer.write_all(buffer).unwrap();
        file_bytes_to_write -= to_write;
    }

    let start_time = Instant::now();

    local_user
        .put(PutRequest {
            source_filename: in_file.into(),
            destination_filename: out_file.into(),
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
        })
        .expect("unable to send put request.");

    while !filestore.get_native_path(&out_file).exists() {
        thread::sleep(Duration::from_millis(10))
    }

    let end_time = start_time.elapsed().as_millis();
    let mbps = file_size as f64 * 8000. / end_time as f64 / 1_000_000.0;

    info!(
        "1GB file transfer took {:2} seconds to complete at a rate of {:5}mbps",
        end_time / 1000,
        mbps
    );

    assert!(mbps > 750.);

    filestore.delete_file(in_file).unwrap();
    filestore.delete_file(out_file).unwrap();
}

#[fixture]
#[once]
fn fixture_bench02(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        None,
        Some(TransportIssue::All(vec![PDUDirective::Nak])),
        [Some(10), Some(1), Some(1), Some(1)],
        Some(EntityConfig {
            fault_handler_override: HashMap::from([(
                Condition::PositiveLimitReached,
                FaultHandlerAction::Abandon,
            )]),
            file_size_segment: 1024,
            default_transaction_max_count: 2,
            inactivity_timeout: 1,
            eof_timeout: 1,
            nak_timeout: 1,
            crc_flag: CRCFlag::NotPresent,
            checksum_type: ChecksumType::Null,
            nak_procedure: NakProcedure::Deferred(Duration::ZERO),
            local_entity_id: 0_u16,
            remote_entity_id: 1_u16,
            local_server_addr: "127.0.0.1:0",
            remote_server_addr: "127.0.0.1:0",
            progress_report_interval_secs: 1,
        }),
    )
}

#[cfg_attr(not(feature = "benches"), ignore)]
#[rstest]
#[timeout(Duration::from_secs(120))]
// Bench Series
// Test goal:
//  - Benchmark the transfer of a 1GB file.
//  - File sent in chunk sizes of 1024
//  - Should process at ~200mbps (~450mbps release mode)
fn bench02(fixture_bench02: &'static EntityConstructorReturn) {
    initialize();

    let in_file = "local/smdp_bench02.txt";
    let out_file = "remote/smdp_bench02.txt";
    let file_size = 1024 * 1024 * 1024; // 1GB

    let (local_user, _remote_user, filestore, _local, _remote) = fixture_bench02;

    filestore.create_file(in_file).unwrap();
    let f = filestore
        .open(in_file, &mut OpenOptions::new().write(true))
        .unwrap();
    let mut writer = BufWriter::new(f);
    let mut buffer = [0u8; 1024];
    let mut file_bytes_to_write = file_size;

    while file_bytes_to_write > 0 {
        let to_write = cmp::min(file_bytes_to_write, buffer.len());
        let buffer = &mut buffer[..to_write];
        writer.write_all(buffer).unwrap();
        file_bytes_to_write -= to_write;
    }

    let start_time = Instant::now();

    local_user
        .put(PutRequest {
            source_filename: in_file.into(),
            destination_filename: out_file.into(),
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
        })
        .expect("unable to send put request.");

    while !filestore.get_native_path(&out_file).exists() {
        thread::sleep(Duration::from_millis(10))
    }

    let end_time = start_time.elapsed().as_millis();
    let mbps = file_size as f64 * 8000. / end_time as f64 / 1_000_000.0;

    info!(
        "1GB file transfer took {:2} seconds to complete at a rate of {:5}mbps",
        end_time / 1000,
        mbps
    );

    assert!(mbps > 200.);

    filestore.delete_file(in_file).unwrap();
    filestore.delete_file(out_file).unwrap();
}
