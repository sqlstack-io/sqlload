use std::time::Duration;

use crate::postgres_helpers::silent_server;
use crate::test_types::{PhaseTimesTestCase, PlanProgressTestCase};

#[test]
fn given_plan_piped_when_connecting_and_discovering_then_timed_phase_lines_and_running_rows_show_on_stderr()
-> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let root = directory.path();
    std::fs::create_dir_all(root.join("drop/orders")).map_err(|error| error.to_string())?;
    std::fs::write(root.join("drop/orders/a.csv"), "id,qty\n1,2\n")
        .map_err(|error| error.to_string())?;
    let port = silent_server(Duration::from_millis(1200))?;
    let target = format!(
        "[connections.files]\nkind = \"filesystem\"\nroot = \"{root}\"\n\n[connections.local]\nkind = \"duckdb\"\ndatabase = \"{root}/out.duckdb\"\n\n[targets.dev]\nconnection = \"local\"\nloader_schema = \"raw\"\n\n",
        root = root.display()
    );
    let files = "[sources.orders]\nkind = \"files\"\nconnection = \"files\"\npath = \"drop/orders/\"\nformat = \"csv\"\nwrite_strategy = \"append\"\n";
    let slow = format!(
        "[connections.pg]\nkind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {port}\nuser = \"sqlload\"\npassword = \"secret\"\ndatabase = \"app\"\n\n[sources.erp]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"public.*\"]\nwrite_strategy = \"replace\"\n"
    );
    let test_cases = [
        PlanProgressTestCase {
            description: "a fast plan: its connections and discoveries collapse into two summary lines, no RUNNING row, no spinner",
            config: format!("{target}{files}"),
            args: &["plan", "--progress", "always"],
            expected_stderr: &[
                "✓ Connected   dev DuckDB   0.",
                "✓ Discovered  1 table in 1 source   0.",
            ],
            unexpected_stderr: &[
                "Target connected",
                "orders  1 table",
                "RUNNING",
                "Planning",
                "⠋",
                "\u{1b}",
            ],
            least_running_rows: 0,
        },
        PlanProgressTestCase {
            description: "-v: a line per connection and discovery, and no summary",
            config: format!("{target}{files}"),
            args: &["plan", "-v"],
            expected_stderr: &[
                "✓ Target connected  dev  DUCKDB  (",
                "✓ Discovered  orders  1 table  (",
            ],
            unexpected_stderr: &["✓ Connected", "in 1 source"],
            least_running_rows: 0,
        },
        PlanProgressTestCase {
            description: "a source slow to connect: RUNNING rows as it waits, then the failure with its time, and the target in the summary",
            config: format!("{target}{slow}"),
            args: &["plan"],
            expected_stderr: &[
                "✓ Connected   dev DuckDB   0.",
                "  erp  connecting  RUNNING 0.",
                "✗ Source connection failed  erp  POSTGRES  (after ",
            ],
            unexpected_stderr: &["Discovered", "Discovery failed", "Target connected"],
            least_running_rows: 2,
        },
    ];
    let config = root.join("sqlload.toml");
    for test_case in test_cases {
        std::fs::write(&config, &test_case.config).map_err(|error| error.to_string())?;
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_sqlload"))
            .arg("--config")
            .arg(&config)
            .args(test_case.args)
            .env_remove("NO_COLOR")
            .env("SQLLOAD_RUNNING_EVERY_MS", "300")
            .output()
            .map_err(|error| error.to_string())?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        let rows = stderr.matches("RUNNING").count();
        assert!(
            test_case
                .expected_stderr
                .iter()
                .all(|expected| stderr.contains(expected))
                && !test_case
                    .unexpected_stderr
                    .iter()
                    .any(|unexpected| stderr.contains(unexpected))
                && rows >= test_case.least_running_rows,
            "{}: {stderr:?}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_plan_or_load_json_when_run_then_the_report_has_each_connection_and_discovery_time()
-> Result<(), String> {
    let expected = vec![
        (
            "target".to_owned(),
            "dev".to_owned(),
            "connect".to_owned(),
            true,
            true,
        ),
        (
            "source".to_owned(),
            "orders".to_owned(),
            "discover".to_owned(),
            true,
            true,
        ),
    ];
    let test_cases = [
        PhaseTimesTestCase {
            description: "plan --json has them at the top",
            args: &["plan", "--json"],
            pointer: "/phases",
            expected_phases: expected.clone(),
        },
        PhaseTimesTestCase {
            description: "load --json has them in its plan",
            args: &["load", "--json"],
            pointer: "/plan/phases",
            expected_phases: expected,
        },
    ];
    for test_case in test_cases {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let root = directory.path();
        std::fs::create_dir_all(root.join("drop/orders")).map_err(|error| error.to_string())?;
        std::fs::write(root.join("drop/orders/a.csv"), "id,qty\n1,2\n")
            .map_err(|error| error.to_string())?;
        let config = root.join("sqlload.toml");
        std::fs::write(
            &config,
            format!(
                "[connections.files]\nkind = \"filesystem\"\nroot = \"{root}\"\n\n[connections.local]\nkind = \"duckdb\"\ndatabase = \"{root}/out.duckdb\"\n\n[targets.dev]\nconnection = \"local\"\nloader_schema = \"raw\"\n\n[sources.orders]\nkind = \"files\"\nconnection = \"files\"\npath = \"drop/orders/\"\nformat = \"csv\"\nwrite_strategy = \"append\"\n",
                root = root.display()
            ),
        )
        .map_err(|error| error.to_string())?;
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_sqlload"))
            .arg("--config")
            .arg(&config)
            .args(test_case.args)
            .output()
            .map_err(|error| error.to_string())?;
        let report: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|error| format!("{}: {error}", test_case.description))?;
        let phases: Vec<(String, String, String, bool, bool)> = report
            .pointer(test_case.pointer)
            .and_then(serde_json::Value::as_array)
            .map(|phases| {
                phases
                    .iter()
                    .map(|phase| {
                        (
                            phase["role"].as_str().unwrap_or_default().to_owned(),
                            phase["name"].as_str().unwrap_or_default().to_owned(),
                            phase["phase"].as_str().unwrap_or_default().to_owned(),
                            phase["ok"].as_bool().unwrap_or_default(),
                            phase["elapsed_ms"].is_u64(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        assert_eq!(
            phases, test_case.expected_phases,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
