use crate::files_source::_helpers::plan::encoding::{file_codec, file_names};
use crate::files_source::models::{Codec, Compression, FilesEncoding, SourceFormat};
use crate::files_source::tests::test_types::{FileNamesTestCase, FileSelectionTestCase};

const KEYS: &[&str] = &[
    "a.parquet",
    "b.ndjson",
    "c.JSONL",
    "d.json.gz",
    "e.json.zst",
    "f.csv",
    "g.CSV.GZ",
    "h.csv.zst",
    "i.txt",
    "j.parquet.gz",
];

#[test]
fn given_listed_keys_when_selecting_by_format_and_compression_then_suffixes_decide() {
    let test_cases = [
        FileSelectionTestCase {
            description: "parquet reads only .parquet files, never a compressed suffix",
            keys: KEYS,
            format: SourceFormat::Parquet,
            compression: Compression::Auto,
            expected_files: &[("a.parquet", Codec::Plain)],
        },
        FileSelectionTestCase {
            description: "ndjson reads .ndjson, .jsonl and .json, decompressed by suffix, in any case",
            keys: KEYS,
            format: SourceFormat::Ndjson,
            compression: Compression::Auto,
            expected_files: &[
                ("b.ndjson", Codec::Plain),
                ("c.JSONL", Codec::Plain),
                ("d.json.gz", Codec::Gzip),
                ("e.json.zst", Codec::Zstd),
            ],
        },
        FileSelectionTestCase {
            description: "json reads the same names as ndjson",
            keys: &["x.json", "y.jsonl.gz"],
            format: SourceFormat::Json,
            compression: Compression::Auto,
            expected_files: &[("x.json", Codec::Plain), ("y.jsonl.gz", Codec::Gzip)],
        },
        FileSelectionTestCase {
            description: "csv reads .csv with any compression suffix",
            keys: KEYS,
            format: SourceFormat::Csv,
            compression: Compression::Auto,
            expected_files: &[
                ("f.csv", Codec::Plain),
                ("g.CSV.GZ", Codec::Gzip),
                ("h.csv.zst", Codec::Zstd),
            ],
        },
        FileSelectionTestCase {
            description: "compression none skips compressed names",
            keys: KEYS,
            format: SourceFormat::Csv,
            compression: Compression::None,
            expected_files: &[("f.csv", Codec::Plain)],
        },
        FileSelectionTestCase {
            description: "compression gzip reads unsuffixed names as gzip and skips zstd ones",
            keys: KEYS,
            format: SourceFormat::Csv,
            compression: Compression::Gzip,
            expected_files: &[("f.csv", Codec::Gzip), ("g.CSV.GZ", Codec::Gzip)],
        },
        FileSelectionTestCase {
            description: "compression zstd reads unsuffixed names as zstd and skips gzip ones",
            keys: KEYS,
            format: SourceFormat::Ndjson,
            compression: Compression::Zstd,
            expected_files: &[
                ("b.ndjson", Codec::Zstd),
                ("c.JSONL", Codec::Zstd),
                ("e.json.zst", Codec::Zstd),
            ],
        },
    ];
    for test_case in test_cases {
        let encoding = FilesEncoding {
            format: test_case.format,
            compression: test_case.compression,
            ..FilesEncoding::default()
        };
        let files: Vec<(&str, Codec)> = test_case
            .keys
            .iter()
            .filter_map(|key| file_codec(key, &encoding).map(|codec| (*key, codec)))
            .collect();
        assert_eq!(files, test_case.expected_files, "{}", test_case.description);
    }
}

#[test]
fn given_an_encoding_when_naming_its_files_then_every_suffix_is_listed() {
    let test_cases = [
        FileNamesTestCase {
            description: "parquet has one suffix",
            format: SourceFormat::Parquet,
            compression: Compression::Auto,
            expected_names: ".parquet",
        },
        FileNamesTestCase {
            description: "auto compression adds both compressed suffixes",
            format: SourceFormat::Csv,
            compression: Compression::Auto,
            expected_names: ".csv, .csv.gz, .csv.zst",
        },
        FileNamesTestCase {
            description: "no compression adds none",
            format: SourceFormat::Csv,
            compression: Compression::None,
            expected_names: ".csv",
        },
        FileNamesTestCase {
            description: "gzip adds .gz to every JSON suffix",
            format: SourceFormat::Ndjson,
            compression: Compression::Gzip,
            expected_names: ".ndjson, .ndjson.gz, .jsonl, .jsonl.gz, .json, .json.gz",
        },
    ];
    for test_case in test_cases {
        let encoding = FilesEncoding {
            format: test_case.format,
            compression: test_case.compression,
            ..FilesEncoding::default()
        };
        assert_eq!(
            file_names(&encoding),
            test_case.expected_names,
            "{}",
            test_case.description
        );
    }
}
