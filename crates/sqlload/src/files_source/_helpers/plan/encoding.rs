use globset::Glob;

use crate::files_source::constants::{
    CSV_SUFFIX, GZIP_SUFFIX, JSON_SUFFIXES, PARQUET_SUFFIX, XLSX_SUFFIX, XML_SUFFIX, ZIP_SUFFIX,
    ZSTD_SUFFIX,
};
use crate::files_source::models::{Codec, Compression, FilesEncoding, SourceFormat};

/// The file name suffixes of `format`'s data files.
fn format_suffixes(format: SourceFormat) -> &'static [&'static str] {
    match format {
        SourceFormat::Parquet => std::slice::from_ref(&PARQUET_SUFFIX),
        SourceFormat::Ndjson | SourceFormat::Json => JSON_SUFFIXES,
        SourceFormat::Csv => std::slice::from_ref(&CSV_SUFFIX),
        SourceFormat::Xlsx => std::slice::from_ref(&XLSX_SUFFIX),
        SourceFormat::Xml => std::slice::from_ref(&XML_SUFFIX),
    }
}

/// The decompression `key` is read with, or `None` when its name does not belong to `encoding` (case-insensitive); with `compression = "zip"`, `Plain` for a ZIP archive, whose members [`member_fits`] picks.
pub(crate) fn file_codec(key: &str, encoding: &FilesEncoding) -> Option<Codec> {
    let lower = key.to_ascii_lowercase();
    let (stem, suffix_codec) = match (
        lower.strip_suffix(GZIP_SUFFIX),
        lower.strip_suffix(ZSTD_SUFFIX),
    ) {
        (Some(stem), _) => (stem, Some(Codec::Gzip)),
        (None, Some(stem)) => (stem, Some(Codec::Zstd)),
        (None, None) => (lower.as_str(), None),
    };
    let codec = match (encoding.format, encoding.compression, suffix_codec) {
        (_, Compression::Zip, _) => return lower.ends_with(ZIP_SUFFIX).then_some(Codec::Plain),
        (SourceFormat::Parquet, _, Some(_)) => return None,
        (SourceFormat::Parquet, _, None) => Codec::Plain,
        (_, Compression::Auto, suffix) => suffix.unwrap_or(Codec::Plain),
        (_, Compression::None, None) => Codec::Plain,
        (_, Compression::Gzip, None | Some(Codec::Gzip)) => Codec::Gzip,
        (_, Compression::Zstd, None | Some(Codec::Zstd)) => Codec::Zstd,
        (_, Compression::None | Compression::Gzip | Compression::Zstd, Some(_)) => return None,
    };
    format_suffixes(encoding.format)
        .iter()
        .any(|suffix| stem.ends_with(suffix))
        .then_some(codec)
}

/// Whether a ZIP archive's member `name` is one of `encoding`'s files: its name ends with the format's suffix (case-insensitive) and matches `members` when set.
pub(crate) fn member_fits(name: &str, encoding: &FilesEncoding) -> Result<bool, String> {
    let lower = name.to_ascii_lowercase();
    let suffix = format_suffixes(encoding.format)
        .iter()
        .any(|suffix| lower.ends_with(suffix));
    let pattern = match &encoding.members {
        Some(pattern) => Glob::new(pattern)
            .map_err(|error| format!("members {pattern:?}: {error}"))?
            .compile_matcher()
            .is_match(name),
        None => true,
    };
    Ok(!name.ends_with('/') && suffix && pattern)
}

/// The file names `encoding` reads, for messages: `.csv`, `.csv.gz`, ...
pub(crate) fn file_names(encoding: &FilesEncoding) -> String {
    let compressed: &[&str] = match (encoding.format, encoding.compression) {
        (format, Compression::Zip) => {
            return format!(
                "{} members of {ZIP_SUFFIX}",
                format_suffixes(format).join(", ")
            );
        }
        (SourceFormat::Parquet, _) | (_, Compression::None) => &[""],
        (_, Compression::Auto) => &["", GZIP_SUFFIX, ZSTD_SUFFIX],
        (_, Compression::Gzip) => &["", GZIP_SUFFIX],
        (_, Compression::Zstd) => &["", ZSTD_SUFFIX],
    };
    let mut names: Vec<String> = Vec::new();
    for suffix in format_suffixes(encoding.format) {
        for compression in compressed {
            names.push(format!("{suffix}{compression}"));
        }
    }
    names.join(", ")
}
