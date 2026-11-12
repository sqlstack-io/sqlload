use crate::files_source::models::{SourceFormat, XlsxOptions, XmlOptions};
use crate::files_source::tests::helpers::{block_on, container_encoding, read_rows, xml_at};
use crate::files_source::tests::test_types::ReadTextFilesTestCase;

const ORDERS_XML: &str = "<?xml version=\"1.0\"?>\n<orders>\n  <order id=\"1\">\n    <name>A &amp; B</name>\n    <tag>x</tag>\n    <tag>y</tag>\n    <ship><city>Oslo</city></ship>\n  </order>\n  <order id=\"2\"><name><![CDATA[<raw>]]></name><tag>z</tag></order>\n  <note>not a row</note>\n</orders>\n";

#[test]
fn given_xlsx_xml_and_zip_files_when_reading_then_cells_load_typed_and_errors_name_the_place()
-> Result<(), String> {
    let sheet = |sheet: Option<&str>, header: bool, skip_rows: u32| XlsxOptions {
        sheet: sheet.map(str::to_owned),
        header,
        skip_rows,
    };
    let xlsx = |options: XlsxOptions, members: Option<&'static str>| {
        container_encoding(
            SourceFormat::Xlsx,
            (options, XmlOptions::default()),
            members,
        )
    };
    let xml = |options: XmlOptions, members: Option<&'static str>| {
        container_encoding(
            SourceFormat::Xml,
            (XlsxOptions::default(), options),
            members,
        )
    };
    let csv_zip = |members: Option<&'static str>| {
        container_encoding(SourceFormat::Csv, Default::default(), members)
    };
    let test_cases = [
        ReadTextFilesTestCase {
            description: "xlsx: numbers, text, booleans, dates, timestamps and times keep their cell types; an empty cell is NULL; a whole number is an int",
            encoding: xlsx(sheet(None, true, 0), None),
            files: &[(
                "a.xlsx",
                "[Orders]\nid|name|amount|paid|day|at|t\nn:1|Caf\u{e9}|n:2.5|b:true|d:2026-11-12|dt:2026-11-12T10:30:00|tm:10:30:00\nn:2||n:3|b:false|d:2026-11-13|dt:2026-11-13T00:00:00|tm:23:59:59\n",
            )],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok(
                "1|Caf\u{e9}|2.5|true|2026-11-12|2026-11-12T10:30:00|10:30:00\n2|NULL|3.0|false|2026-11-13|2026-11-13T00:00:00|23:59:59",
            ),
        },
        ReadTextFilesTestCase {
            description: "xlsx: the named sheet, its title rows skipped, the header after them; empty rows are left out",
            encoding: xlsx(sheet(Some("Data"), true, 2), None),
            files: &[(
                "a.xlsx",
                "[Cover]\nn:99\n[Data]\nMonthly report|\n\nid|v\nn:7|x\n\nn:8|y\n",
            )],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("7|x\n8|y"),
        },
        ReadTextFilesTestCase {
            description: "xlsx without a header: columns are numbered by their place in the sheet",
            encoding: xlsx(sheet(None, false, 0), None),
            files: &[("a.xlsx", "[S]\nn:1|a\nn:2|b\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1|a\n2|b"),
        },
        ReadTextFilesTestCase {
            description: "xlsx: a sheet the workbook lacks fails, naming its sheets",
            encoding: xlsx(sheet(Some("Nope"), true, 0), None),
            files: &[("a.xlsx", "[Orders]\nid\nn:1\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("the workbook has no sheet \"Nope\"; its sheets are [\"Orders\"]"),
        },
        ReadTextFilesTestCase {
            description: "xlsx: a header naming a column twice fails with its row",
            encoding: xlsx(sheet(None, true, 0), None),
            files: &[("a.xlsx", "[S]\nid|id\nn:1|n:2\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("row 1: column id appears twice in the header"),
        },
        ReadTextFilesTestCase {
            description: "xlsx: a value later rows hold that the sampled type does not fit fails with its row",
            encoding: xlsx(sheet(None, true, 0), None),
            files: &[("a.xlsx", "[S]\nid\nn:1\n")],
            unsampled: &[("b.xlsx", "[S]\nid\nn:2\nlate text\n")],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("b.xlsx: line 3: id: late text does not fit int"),
        },
        ReadTextFilesTestCase {
            description: "xml: attributes and child elements by name, a repeated or listed child as a JSON array, a nested element as a JSON object, entities and CDATA decoded, elements off the path ignored",
            encoding: xml(xml_at("orders/order", &["tag"]), None),
            files: &[("a.xml", ORDERS_XML)],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1|A & B|[\"x\",\"y\"]|{\"city\":\"Oslo\"}\n2|<raw>|[\"z\"]|NULL"),
        },
        ReadTextFilesTestCase {
            description: "xml: names match without their namespace prefix",
            encoding: xml(xml_at("/orders/order/", &[]), None),
            files: &[(
                "a.xml",
                "<ns:orders xmlns:ns=\"urn:x\"><ns:order ns:id=\"5\"><ns:qty>2</ns:qty></ns:order></ns:orders>",
            )],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("5|2"),
        },
        ReadTextFilesTestCase {
            description: "xml: a malformed document fails with its line",
            encoding: xml(xml_at("orders/order", &[]), None),
            files: &[("a.xml", "<orders>\n<order>\n<id>1</order>\n</orders>")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.xml: line 3: "),
        },
        ReadTextFilesTestCase {
            description: "xlsx: a header may use a numbered column's name when no headerless column takes it",
            encoding: xlsx(sheet(None, true, 0), None),
            files: &[("a.xlsx", "[S]\ncolumn_2|column_1\nn:1|n:2\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1|2"),
        },
        ReadTextFilesTestCase {
            description: "xlsx: a headerless column whose numbered name the header already uses fails with its row",
            encoding: xlsx(sheet(None, true, 0), None),
            files: &[("a.xlsx", "[S]\ncolumn_2|\nn:1|n:2\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err(
                "row 2: the column without a header in place 2 would be named column_2, which the header already uses",
            ),
        },
        ReadTextFilesTestCase {
            description: "xml: a document that ends inside an element fails rather than loading the records before the cut",
            encoding: xml(xml_at("orders/order", &[]), None),
            files: &[(
                "a.xml",
                "<orders>\n<order id=\"1\"/>\n<order id=\"2\"><x>5</x>\n",
            )],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.xml: line 4: the document ends inside <order>"),
        },
        ReadTextFilesTestCase {
            description: "xml: a UTF-16 document fails, naming its encoding",
            encoding: xml(xml_at("orders/order", &[]), None),
            files: &[("a.utf16.xml", "<orders><order id=\"1\"/></orders>")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.utf16.xml: the document is UTF-16"),
        },
        ReadTextFilesTestCase {
            description: "xml: namespace declarations and xsi attributes are not columns; xsi:nil is NULL; record_path may name prefixes",
            encoding: xml(xml_at("p:orders/p:order", &[]), None),
            files: &[(
                "a.xml",
                "<p:orders xmlns:p=\"urn:p\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><p:order xmlns=\"urn:d\" id=\"1\" xsi:type=\"T\"><price xsi:nil=\"true\"/><qty>3</qty></p:order><p:order id=\"2\"><price>4.5</price><qty>1</qty></p:order></p:orders>",
            )],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1|3|NULL\n2|1|4.5"),
        },
        ReadTextFilesTestCase {
            description: "xml: two attributes with one local name fail rather than one overwriting the other",
            encoding: xml(xml_at("orders/order", &[]), None),
            files: &[(
                "a.xml",
                "<orders xmlns:p=\"urn:p\" xmlns:q=\"urn:q\"><order p:id=\"1\" q:id=\"2\"/></orders>",
            )],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.xml: <order> has two attributes named id"),
        },
        ReadTextFilesTestCase {
            description: "xml: a record_path whose parent element is there but holds no such element loads no rows",
            encoding: xml(xml_at("orders/ordr", &[]), None),
            files: &[("a.xml", ORDERS_XML)],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok(""),
        },
        ReadTextFilesTestCase {
            description: "xml: a record_path matching no element fails",
            encoding: xml(xml_at("order/item", &[]), None),
            files: &[("a.xml", ORDERS_XML)],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.xml: no element at record_path \"order/item\""),
        },
        ReadTextFilesTestCase {
            description: "zip: each CSV member is a file of its own, other members and folders left out",
            encoding: csv_zip(Some("")),
            files: &[
                (
                    "a.zip",
                    "orders/\u{1e}orders/a.csv\u{1f}id,name\n1,a\n\u{1e}readme.txt\u{1f}hello\n\u{1e}b.csv\u{1f}id,name\n2,b\n",
                ),
                ("c.csv", "id,name\n3,loose\n"),
            ],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1|a\n2|b"),
        },
        ReadTextFilesTestCase {
            description: "zip: members picks the members by pattern",
            encoding: csv_zip(Some("orders/*.csv")),
            files: &[(
                "a.zip",
                "orders/a.csv\u{1f}id,name\n1,a\n\u{1e}b.csv\u{1f}id,name\n2,b\n",
            )],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1|a"),
        },
        ReadTextFilesTestCase {
            description: "zip: an xlsx workbook in a ZIP archive",
            encoding: xlsx(sheet(None, true, 0), Some("")),
            files: &[("a.zip", "book.xlsx\u{1f}[S]\nid|name\nn:5|e\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("5|e"),
        },
        ReadTextFilesTestCase {
            description: "zip: XML documents in a ZIP archive",
            encoding: xml(xml_at("orders/order", &["tag"]), Some("*.xml")),
            files: &[(
                "a.zip",
                "one.xml\u{1f}<orders><order id=\"1\"/></orders>\u{1e}two.xml\u{1f}<orders><order id=\"2\"/></orders>",
            )],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1\n2"),
        },
        ReadTextFilesTestCase {
            description: "zip: a ZIP archive without a member of the format fails as an empty source",
            encoding: csv_zip(Some("")),
            files: &[("a.zip", "readme.txt\u{1f}hello\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("no .csv members of .zip files found"),
        },
    ];
    for test_case in test_cases {
        let rows = block_on(async { Ok(read_rows(&test_case).await) })?;
        assert_eq!(
            rows.as_deref().ok(),
            test_case.expected_rows.ok(),
            "{}: {rows:?}",
            test_case.description
        );
        assert!(
            rows.as_ref().err().is_none_or(
                |error| error.contains(test_case.expected_rows.err().unwrap_or_default())
            ),
            "{}: {rows:?}",
            test_case.description
        );
    }
    Ok(())
}
