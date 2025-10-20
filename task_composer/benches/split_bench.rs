use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::{hint::black_box, ops::RangeInclusive};
// 假设您的库名为 task_composer
use task_composer::FileMultiRange;

// 定义常量以提高可读性
const KB: usize = 1024;
const MB: usize = 1024 * KB;
const GB: usize = 1024 * MB;

const FILE_SIZE: usize = 16 * GB;
const BLOCK_SIZE: usize = 4 * MB;

// --- 1. 性能基线：修正后的手动分割器 ---
/// 这是一个简单的结构体，用于对 `RangeInclusive` 进行块分割。
/// 它使用 Option 来明确地管理状态，这是更符合 Rust 惯例的做法。
struct ManualRangeChunks {
    // 使用 Option<T> 来表示可能存在或不存在的范围
    remaining: Option<RangeInclusive<usize>>,
    chunk_size: usize,
}

impl Iterator for ManualRangeChunks {
    type Item = RangeInclusive<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        // 使用 .take() 方法取出 Some 中的值，并将 self.remaining 置为 None。
        // 如果 self.remaining 本来就是 None，则直接返回 None，迭代结束。
        let mut current_range = self.remaining.take()?;

        let start = *current_range.start();
        let end = *current_range.end();

        // 计算当前块的结束位置，使用 saturating_add 防止溢出
        let chunk_end = start.saturating_add(self.chunk_size - 1);

        if chunk_end >= end {
            // 这是最后一个块了。
            // 因为我们已经调用了 .take()，self.remaining 已经是 None，
            // 所以下一次调用 next() 会自然结束。
            Some(start..=end)
        } else {
            // 这不是最后一个块。
            let chunk = start..=chunk_end;
            // 将剩余的部分放回到 self.remaining 中，以供下一次迭代使用。
            self.remaining = Some((chunk_end + 1)..=end);
            Some(chunk)
        }
    }
}

// --- 2. 辅助函数：创建一个高度碎片化的 FileMultiRange ---
/// 创建一个包含大量小的、不连续区间的 FileMultiRange，用于模拟下载不连续文件块的场景。
fn create_fragmented_range(total_size: usize, num_fragments: usize, fragment_size: usize) -> FileMultiRange {
    let mut range_set = FileMultiRange::default();
    let gap = (total_size - num_fragments * fragment_size) / (num_fragments - 1);

    for i in 0..num_fragments {
        let start = i * (fragment_size + gap);
        let end = start + fragment_size - 1;
        range_set.insert_range((start..=end).into());
    }
    range_set
}

// --- 3. 扩展后的 Benchmark 主函数 ---
fn bench_splitting_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("Range Splitting Analysis");

    // --- 场景 A: 单一连续的 16GB 区间 ---

    // 测试 A.1: 性能基线 (手动分割)
    group.bench_function("A.1: Baseline (Manual split on 16GB)", |b| {
        b.iter(|| {
            let range = 0..=FILE_SIZE - 1;
            let chunks = ManualRangeChunks {
                // 将初始范围包装在 Some() 中
                remaining: Some(range),
                chunk_size: black_box(BLOCK_SIZE),
            };
            // 完全消耗迭代器
            for chunk in chunks {
                black_box(chunk);
            }
        })
    });

    // 测试 A.2: RangeSetBlaze (您的原始测试)
    let continuous_range = FileMultiRange::from(0..=FILE_SIZE - 1);
    group.bench_function("A.2: RangeSetBlaze (on single 16GB Range)", |b| {
        b.iter(|| {
            let mut cloned_range = continuous_range.clone();
            let chunks = cloned_range.as_block_chunks(black_box(BLOCK_SIZE)).unwrap();
            for chunk in chunks {
                black_box(chunk);
            }
        })
    });

    // --- 场景 B: 高度碎片化的区间 ---
    let num_fragments = 20_000;
    let fragment_size = 100 * KB;
    let fragmented_range = create_fragmented_range(FILE_SIZE, num_fragments, fragment_size);

    group.bench_with_input(
        BenchmarkId::new("B.1: RangeSetBlaze (on Fragmented Range)", num_fragments),
        &fragmented_range,
        |b, range| {
            b.iter(|| {
                let mut cloned_range = range.clone();
                let chunks = cloned_range.as_block_chunks(black_box(BLOCK_SIZE)).unwrap();
                for chunk in chunks {
                    black_box(chunk);
                }
            })
        },
    );

    group.finish();
}

criterion_group!(benches, bench_splitting_performance);
criterion_main!(benches);
