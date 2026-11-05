/// The partitions whose open files to write out, largest first, until at most `max_files` stay open holding at most `max_bytes`.
pub(crate) fn spill_order(
    sizes: &[(String, usize)],
    max_files: usize,
    max_bytes: usize,
) -> Vec<String> {
    let mut largest_first: Vec<&(String, usize)> = sizes.iter().collect();
    largest_first.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let mut open = sizes.len();
    let mut bytes: usize = sizes.iter().map(|(_, size)| size).sum();
    let mut spilled: Vec<String> = Vec::new();
    for (value, size) in largest_first {
        if open <= max_files && bytes <= max_bytes {
            break;
        }
        spilled.push(value.clone());
        open -= 1;
        bytes -= size;
    }
    spilled
}
