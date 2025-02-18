use ::tracing::warn;
use anyhow::Result;
use bit_struct::*;

pub mod constants {
    use bit_struct::u3;

    pub const CCSDS_HEADER_LENGTH: usize = 6;
    pub const CCSDS_SECONDARY_HEADER_LENGTH: usize = 6;
    pub const CCSDS_VERSION: u3 = u3!(0);
    pub const OUTGOING_CCSDS_APID_ENV_VAR: &str = "CCSDS_APID_TX";
}
pub use constants::*;

enums! {
    pub PacketType {Telemetry, Telecommand}
    pub SecondaryHeaderFlag {NoSecondaryHeader, SecondaryHeader}
}

pub fn encode_space_packet(data: &[u8], apid: u11, secondary_hdr_flag: u8) -> Result<Vec<u8>> {
    let has_secondary_header = secondary_hdr_flag == 1;
    let secondary_header_flag = if has_secondary_header {
        SecondaryHeaderFlag::SecondaryHeader
    } else {
        SecondaryHeaderFlag::NoSecondaryHeader
    };

    let packet_data_length = if has_secondary_header {
        (data.len() + CCSDS_SECONDARY_HEADER_LENGTH - 1) as u16
    } else {
        data.len() as u16
    };
    let header = CcsdsHeader::new(
        CCSDS_VERSION,
        PacketType::Telemetry,
        secondary_header_flag,
        apid,
        u2!(3),
        u14!(1),
        packet_data_length,
    );

    let packet_size = if has_secondary_header {
        CCSDS_HEADER_LENGTH + CCSDS_SECONDARY_HEADER_LENGTH + data.len()
    } else {
        CCSDS_HEADER_LENGTH + data.len()
    };

    let mut packet = Vec::with_capacity(packet_size);
    packet.extend(&u48::to_be_bytes(header.raw()));
    if has_secondary_header {
        let secondary_header = CcsdsSecondaryHeader::new(0, 0, 0);
        packet.extend(&u48::to_be_bytes(secondary_header.raw()));
    }
    packet.extend(data);

    Ok(packet)
}

pub fn decode_ccsds_header(data: &[u8]) -> Result<(CcsdsHeader, CcsdsSecondaryHeader)> {
    if data.len() < CCSDS_HEADER_LENGTH + CCSDS_SECONDARY_HEADER_LENGTH {
        warn!("Insufficient data length for CCSDS headers");
        return Err(anyhow::anyhow!(
            "Insufficient data length for CCSDS headers"
        ));
    }

    let header = u48::from_be_bytes(data[0..6].try_into().unwrap_or_else(|_| {
        warn!("Failed to convert header bytes");
        [0; 6]
    }));

    let secondary_header = u48::from_be_bytes(data[6..12].try_into().unwrap_or_else(|_| {
        warn!("Failed to convert secondary header bytes");
        [0; 6]
    }));

    Ok((
        CcsdsHeader::exact_from(header),
        CcsdsSecondaryHeader::exact_from(secondary_header),
    ))
}

bit_struct! {
    struct CcsdsHeader(u48) {
        /// CCSDS version number
        ccsds_version: u3,

        /// Packet Type (0 for telemetry, 1 for telecommand)
        packet_type: PacketType,

        /// Secondary Header Flag (0 for no secondary header, 1 for secondary header)
        secondary_header_flag: SecondaryHeaderFlag,

        /// Application Process ID (APID)
        application_process_id: u11,

        /// Sequence flags (3 for none)
        sequence_flags: u2,

        /// Packet Sequence Count (incremented for each packet)
        packet_sequence_count: u14,

        /// Packet Data Length (in bytes)
        packet_data_length: u16,
    }

    struct CcsdsSecondaryHeader(u48) {
        time_stamp_seconds: u32,

        time_stamp_subseconds: u8,

        reserved: u8,
    }
}
