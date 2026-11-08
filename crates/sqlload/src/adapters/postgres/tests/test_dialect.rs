use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::postgres::models::PostgresSql;
use crate::adapters::postgres::tests::helpers::{desired, orders};
use crate::adapters::postgres::tests::test_types::RenderedTestCase;
use crate::column_types::models::ColumnType;

#[test]
fn given_postgres_sql_when_rendering_writes_then_tables_are_kept_and_each_write_is_one_transaction()
{
    let adapter = PostgresSql;
    let (staging, create) = adapter.render_create_staging_table(
        &orders(),
        &[
            desired("ID", ColumnType::Int),
            desired("doc", ColumnType::Json),
            desired("code", ColumnType::Varchar(5)),
        ],
        "20261108000000_ab12",
    );
    let columns = [
        desired("id", ColumnType::Int),
        desired("doc", ColumnType::Json),
        desired("note", ColumnType::Text),
    ];
    let table = orders();
    let request = |mode| WriteStatementsRequest {
        table: &table,
        columns: &columns,
        mode,
        hashed: &[],
        built: None,
    };
    let merge = WriteMode::Merge {
        unique_key: vec!["id".to_owned()],
    };
    let key_only = [desired("id", ColumnType::Int)];
    let test_cases = [
        RenderedTestCase {
            description: "the staging table is temporary, named in pg_temp, and holds JSON and bounded text as text; an upper-case plain name folds to lower case",
            rendered: vec![create],
            expected_statements: &[
                "CREATE TEMPORARY TABLE \"pg_temp\".\"orders__sqlload_staging_20261108000000_ab12\" (\"id\" bigint, \"doc\" text, \"code\" text)",
            ],
        },
        RenderedTestCase {
            description: "replace empties and refills the table in one transaction, never dropping it",
            rendered: adapter.render_write(&request(&WriteMode::Replace), &staging),
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"app\".\"orders\"",
                "INSERT INTO \"app\".\"orders\" (\"id\", \"doc\", \"note\") SELECT \"id\", CAST(\"doc\" AS jsonb), \"note\" FROM \"pg_temp\".\"orders__sqlload_staging_20261108000000_ab12\"",
                "COMMIT",
            ],
        },
        RenderedTestCase {
            description: "merge upserts on the key with ON CONFLICT, updating every other column",
            rendered: adapter.render_write(&request(&merge), &staging),
            expected_statements: &[
                "INSERT INTO \"app\".\"orders\" (\"id\", \"doc\", \"note\") SELECT \"id\", CAST(\"doc\" AS jsonb), \"note\" FROM \"pg_temp\".\"orders__sqlload_staging_20261108000000_ab12\" ON CONFLICT (\"id\") DO UPDATE SET \"doc\" = EXCLUDED.\"doc\", \"note\" = EXCLUDED.\"note\"",
            ],
        },
        RenderedTestCase {
            description: "merge of a table holding only its key inserts what is new",
            rendered: vec![adapter.render_merge(
                &WriteStatementsRequest {
                    table: &table,
                    columns: &key_only,
                    mode: &merge,
                    hashed: &[],
                    built: None,
                },
                &["id".to_owned()],
                "s",
                &["\"id\"".to_owned()],
            )],
            expected_statements: &[
                "INSERT INTO \"app\".\"orders\" (\"id\") SELECT \"id\" FROM s ON CONFLICT (\"id\") DO NOTHING",
            ],
        },
        RenderedTestCase {
            description: "the commit wraps a lone merge with the emptying of the staging table in one transaction",
            rendered: adapter.render_commit(&request(&merge), &staging, (None, false)),
            expected_statements: &[
                "BEGIN",
                "INSERT INTO \"app\".\"orders\" (\"id\", \"doc\", \"note\") SELECT \"id\", CAST(\"doc\" AS jsonb), \"note\" FROM \"pg_temp\".\"orders__sqlload_staging_20261108000000_ab12\" ON CONFLICT (\"id\") DO UPDATE SET \"doc\" = EXCLUDED.\"doc\", \"note\" = EXCLUDED.\"note\"",
                "TRUNCATE \"pg_temp\".\"orders__sqlload_staging_20261108000000_ab12\"",
                "COMMIT",
            ],
        },
        RenderedTestCase {
            description: "a table SQLLoad creates for merge gets a unique index on the key",
            rendered: adapter
                .render_key_index(&orders(), &["id".to_owned()])
                .into_iter()
                .collect(),
            expected_statements: &[
                "CREATE UNIQUE INDEX IF NOT EXISTS \"orders_sqlload_key\" ON \"app\".\"orders\" (\"id\")",
            ],
        },
        RenderedTestCase {
            description: "the schema is created in the session's database, which is never named",
            rendered: vec![adapter.render_create_schema(&TableName {
                database: Some("mart".to_owned()),
                ..orders()
            })],
            expected_statements: &["CREATE SCHEMA IF NOT EXISTS \"app\""],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            test_case.rendered, test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}
