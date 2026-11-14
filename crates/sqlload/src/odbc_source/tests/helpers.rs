use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{PgType, SourceColumn, SourceTable};

/// A source table of text `columns`.
pub(crate) fn table(schema: &str, name: &str, columns: &[&str]) -> SourceTable {
    SourceTable {
        schema: schema.to_owned(),
        name: name.to_owned(),
        columns: columns
            .iter()
            .map(|column| SourceColumn {
                name: (*column).to_owned(),
                postgres_type: "Utf8".to_owned(),
                pg_type: PgType::Other,
                column_type: Ok(ColumnType::Text),
                nullable: true,
                source_spelling: None,
            })
            .collect(),
        primary_key: Vec::new(),
        indexed_columns: Vec::new(),
        estimated_rows: -1,
        estimated_bytes: -1,
        comment: None,
        listed_keys: Vec::new(),
    }
}

/// A 64-bit little-endian ELF file holding just one program header, of its dynamic section, which sets DT_FLAGS to `flags`: no loader accepts it, and it reads like a driver built with those flags.
pub(crate) fn elf_with_flags(flags: u64) -> Vec<u8> {
    let mut elf = vec![0_u8; 152];
    elf[..6].copy_from_slice(b"\x7fELF\x02\x01");
    elf[0x20..0x28].copy_from_slice(&64_u64.to_le_bytes());
    elf[0x36..0x38].copy_from_slice(&56_u16.to_le_bytes());
    elf[0x38..0x3a].copy_from_slice(&1_u16.to_le_bytes());
    elf[64..68].copy_from_slice(&2_u32.to_le_bytes());
    elf[72..80].copy_from_slice(&120_u64.to_le_bytes());
    elf[96..104].copy_from_slice(&32_u64.to_le_bytes());
    elf[120..128].copy_from_slice(&30_u64.to_le_bytes());
    elf[128..136].copy_from_slice(&flags.to_le_bytes());
    elf
}
