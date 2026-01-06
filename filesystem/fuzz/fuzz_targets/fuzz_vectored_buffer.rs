#![no_main]

use arbitrary::Arbitrary;
use falcon_filesystem::VectoredBuffer;
use libfuzzer_sys::fuzz_target;

/// 简化版的 VectoredBuffer 模糊测试
/// 专注于基本操作的正确性
#[derive(Debug, Arbitrary)]
struct FuzzCase {
    /// 多个写入操作
    writes: Vec<WriteOp>,
}

#[derive(Debug, Arbitrary, Clone)]
struct WriteOp {
    #[arbitrary(with = |u: &mut arbitrary::Unstructured| u.int_in_range(0..=16384))]
    pos: usize,
    data: Vec<u8>,
}

fuzz_target!(|case: FuzzCase| {
    let mut vbuf = VectoredBuffer::new();

    // 执行所有写入操作
    for write_op in &case.writes {
        if write_op.data.len() > 4096 {
            continue; // 限制数据大小
        }

        let written = vbuf.read_from_at(&write_op.data, write_op.pos);
        assert!(written <= write_op.data.len());
    }

    // 验证基本不变式
    let total_len = vbuf.total_remaining_len();

    // 计算预期的总长度（不考虑重叠）
    let expected_total: usize = case.writes.iter().map(|w| w.data.len()).filter(|&len| len <= 4096).sum();

    // 总长度不应超过所有写入数据的总和
    assert!(total_len <= expected_total);

    // 验证 buffer 数量合理
    let buffer_count = vbuf.buffer_count();
    assert!(buffer_count <= case.writes.len() * 2); // 每个write最多产生2个buffer

    // 执行一些内存管理操作
    vbuf.compact_all();
    vbuf.release_done();

    // 验证长度不变（release_done可能会减少）
    assert!(vbuf.total_remaining_len() <= total_len);
});
