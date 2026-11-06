use crate::cdc::_helpers::pgoutput::decode_message;
use crate::cdc::models::{PgOutputMessage, Relation, RelationColumn, TupleValue};
use crate::cdc::tests::helpers::{NULL_PART, UNCHANGED_PART, cstring, text, text_part, tuple};
use crate::cdc::tests::test_types::DecodeTestCase;

#[test]
fn given_pgoutput_bytes_when_decoding_then_each_message_keeps_its_values_and_parts() {
    let relation_bytes = [
        vec![b'R'],
        16_401_u32.to_be_bytes().to_vec(),
        cstring("public"),
        cstring("orders"),
        vec![b'd'],
        2_i16.to_be_bytes().to_vec(),
        vec![1],
        cstring("id"),
        20_u32.to_be_bytes().to_vec(),
        (-1_i32).to_be_bytes().to_vec(),
        vec![0],
        cstring("amount"),
        1700_u32.to_be_bytes().to_vec(),
        786_438_i32.to_be_bytes().to_vec(),
    ]
    .concat();
    let test_cases = [
        DecodeTestCase {
            description: "a relation names its key columns, types and modifiers",
            bytes: relation_bytes,
            expected_message: Ok(PgOutputMessage::Relation(Relation {
                oid: 16_401,
                schema: "public".to_owned(),
                table: "orders".to_owned(),
                replica_identity: 'd',
                columns: vec![
                    RelationColumn {
                        name: "id".to_owned(),
                        key: true,
                        type_oid: 20,
                        type_modifier: -1,
                    },
                    RelationColumn {
                        name: "amount".to_owned(),
                        key: false,
                        type_oid: 1700,
                        type_modifier: 786_438,
                    },
                ],
            })),
        },
        DecodeTestCase {
            description: "an insert keeps NULLs and multi-byte text",
            bytes: [
                vec![b'I'],
                7_u32.to_be_bytes().to_vec(),
                vec![b'N'],
                tuple(&[&text_part("1"), NULL_PART, &text_part("Zoë")]),
            ]
            .concat(),
            expected_message: Ok(PgOutputMessage::Insert {
                relation: 7,
                new: vec![text("1"), TupleValue::Null, text("Zoë")],
            }),
        },
        DecodeTestCase {
            description: "an update without old values marks an untouched TOAST column unchanged",
            bytes: [
                vec![b'U'],
                7_u32.to_be_bytes().to_vec(),
                vec![b'N'],
                tuple(&[&text_part("1"), UNCHANGED_PART]),
            ]
            .concat(),
            expected_message: Ok(PgOutputMessage::Update {
                relation: 7,
                old: None,
                new: vec![text("1"), TupleValue::Unchanged],
            }),
        },
        DecodeTestCase {
            description: "an update that changed the key sends the old key first",
            bytes: [
                vec![b'U'],
                7_u32.to_be_bytes().to_vec(),
                vec![b'K'],
                tuple(&[&text_part("1"), NULL_PART]),
                vec![b'N'],
                tuple(&[&text_part("2"), &text_part("x")]),
            ]
            .concat(),
            expected_message: Ok(PgOutputMessage::Update {
                relation: 7,
                old: Some(vec![text("1"), TupleValue::Null]),
                new: vec![text("2"), text("x")],
            }),
        },
        DecodeTestCase {
            description: "a delete under replica identity full sends the whole old row",
            bytes: [
                vec![b'D'],
                7_u32.to_be_bytes().to_vec(),
                vec![b'O'],
                tuple(&[&text_part("3"), &text_part("y")]),
            ]
            .concat(),
            expected_message: Ok(PgOutputMessage::Delete {
                relation: 7,
                old: vec![text("3"), text("y")],
            }),
        },
        DecodeTestCase {
            description: "a truncate lists its tables and reads both option bits",
            bytes: [
                vec![b'T'],
                2_i32.to_be_bytes().to_vec(),
                vec![3],
                7_u32.to_be_bytes().to_vec(),
                9_u32.to_be_bytes().to_vec(),
            ]
            .concat(),
            expected_message: Ok(PgOutputMessage::Truncate {
                relations: vec![7, 9],
                cascade: true,
                restart_identity: true,
            }),
        },
        DecodeTestCase {
            description: "a type message is passed over",
            bytes: [vec![b'Y'], 5_u32.to_be_bytes().to_vec()].concat(),
            expected_message: Ok(PgOutputMessage::Other('Y')),
        },
        DecodeTestCase {
            description: "a text value cut short is an error, not a shorter value",
            bytes: [
                vec![b'I', 0, 0, 0, 7, b'N', 0, 1, b't'],
                10_i32.to_be_bytes().to_vec(),
                b"abc".to_vec(),
            ]
            .concat(),
            expected_message: Err(
                "the pgoutput message ends after 16 bytes, inside a field at byte 13",
            ),
        },
        DecodeTestCase {
            description: "a binary value is refused, since only text output is asked for",
            bytes: vec![b'I', 0, 0, 0, 7, b'N', 0, 1, b'b', 0, 0, 0, 0],
            expected_message: Err(
                "a value has kind 'b'; SQLLoad asks pgoutput for text values only",
            ),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            decode_message(&test_case.bytes),
            test_case.expected_message.map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
