use crate::memory_budget::_helpers::limit::{cgroup_limit, lower_limit};
use crate::memory_budget::models::{LimitOrigin, MemoryLimit};
use crate::memory_budget::tests::test_types::{CgroupLimitTestCase, LowerLimitTestCase};

#[test]
fn given_cgroup_v2_folders_when_reading_the_memory_limit_then_the_lowest_memory_max_from_the_process_group_up_is_taken()
-> Result<(), String> {
    let test_cases = [
        CgroupLimitTestCase {
            description: "a container's own group sets memory.max",
            groups: "0::/\n",
            limits: &[("", "2147483648\n")],
            expected_limit: Some(2_147_483_648),
        },
        CgroupLimitTestCase {
            description: "the process's group is unlimited, a parent slice is not",
            groups: "0::/app.slice/run.scope\n",
            limits: &[
                ("app.slice/run.scope", "max\n"),
                ("app.slice", "6442450944\n"),
            ],
            expected_limit: Some(6_442_450_944),
        },
        CgroupLimitTestCase {
            description: "the lowest of several limits",
            groups: "0::/a/b\n",
            limits: &[("a/b", "8589934592\n"), ("a", "1073741824\n")],
            expected_limit: Some(1_073_741_824),
        },
        CgroupLimitTestCase {
            description: "no limit anywhere",
            groups: "0::/a\n",
            limits: &[("a", "max\n")],
            expected_limit: None,
        },
        CgroupLimitTestCase {
            description: "cgroup v1 only: no v2 line",
            groups: "4:memory:/docker/abc\n",
            limits: &[],
            expected_limit: None,
        },
    ];
    for test_case in test_cases {
        let root = tempfile::tempdir().map_err(|error| error.to_string())?;
        for (folder, limit) in test_case.limits {
            let folder = root.path().join(folder);
            std::fs::create_dir_all(&folder).map_err(|error| error.to_string())?;
            std::fs::write(folder.join("memory.max"), limit).map_err(|error| error.to_string())?;
        }
        assert_eq!(
            cgroup_limit(test_case.groups, root.path()),
            Ok(test_case.expected_limit),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_container_or_job_limit_and_the_machines_memory_when_choosing_the_limit_then_the_lower_one_is_taken_with_its_origin()
 {
    let test_cases = [
        LowerLimitTestCase {
            description: "a container below the machine",
            limit: Some((2_000, LimitOrigin::Container)),
            physical: Some(8_000),
            expected_limit: Some((2_000, LimitOrigin::Container)),
        },
        LowerLimitTestCase {
            description: "a job object above the machine: the machine's memory is the limit",
            limit: Some((16_000, LimitOrigin::JobObject)),
            physical: Some(8_000),
            expected_limit: Some((8_000, LimitOrigin::Machine)),
        },
        LowerLimitTestCase {
            description: "a job object without the machine's memory",
            limit: Some((3_000, LimitOrigin::JobObject)),
            physical: None,
            expected_limit: Some((3_000, LimitOrigin::JobObject)),
        },
        LowerLimitTestCase {
            description: "neither",
            limit: None,
            physical: None,
            expected_limit: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            lower_limit(test_case.limit, test_case.physical),
            test_case
                .expected_limit
                .map(|(bytes, origin)| MemoryLimit { bytes, origin }),
            "{}",
            test_case.description
        );
    }
}
