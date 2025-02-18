# Simplified CFDP

A simplified CFDP implementation based off [cfdp-rs](https://github.com/ASU-cubesat/cfdp-rs).


Deviates from the [CFDP Spec](https://public.ccsds.org/Pubs/727x0b5e1.pdf) in the following ways:

    - Uses fixed length entity IDs and transaction sequence numbers in the CFDP header
      - source_entity_id: u16
      - destination_entity_id: u16
      - transaction_sequence_number: u32
    - CCSDS version in CFDP header is always 0
    - Utilizes NAKs instead of finished and Ack PDUs
      - When receiver receives an EOF PDU, rather than sending an Ack(EOF) followed by a Nak(missing-data-segments), the receiver only sends a Nak(missing-data-segments)
      - When the receiver has received all data PDUs, instead of sending a Finished PDU and the sender following with an Ack PDU, the receiver sends an empty Nak PDU