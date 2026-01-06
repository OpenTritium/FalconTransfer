// Buffer sizing benchmark for SeqBufFile
// Simulates HTTP download scenarios with varying buffer sizes

use compio::io::{AsyncWrite, AsyncWriteExt};
use criterion::{Criterion, Throughput, async_executor::AsyncExecutor, criterion_group, criterion_main};
use falcon_filesystem::SeqBufFile;
use tempfile::NamedTempFile;

fn create_temp_file() -> NamedTempFile { NamedTempFile::new().unwrap() }

struct CompioExecutor;

impl AsyncExecutor for CompioExecutor {
    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        compio::runtime::Runtime::new().unwrap().block_on(future)
    }
}

fn bench_buffer_sizing(c: &mut Criterion) {
    // Simulate HTTP download: 100MB file with varying chunk sizes
    let total_size = 100 * 1024 * 1024; // 100MB

    // Test different buffer configurations
    let buffer_configs = vec![
        (16 * 1024, 64 * 1024),    // 16KB base, 64KB max
        (32 * 1024, 128 * 1024),   // 32KB base, 128KB max
        (64 * 1024, 256 * 1024),   // 64KB base, 256KB max
        (128 * 1024, 512 * 1024),  // 128KB base, 512KB max
        (256 * 1024, 1024 * 1024), // 256KB base, 1MB max
    ];

    for (base, max) in buffer_configs {
        let group_name = format!("buffer_{}KB_{}KB", base / 1024, max / 1024);
        let mut group = c.benchmark_group(&group_name);
        group.throughput(Throughput::Bytes(total_size));
        group.sample_size(10);

        // SeqBufFile with specific buffer size
        group.bench_function("seqbuf_http_sim", |b| {
            // Pre-allocate static data to simulate HTTP chunks
            let chunk_4kb: Box<[u8; 4096]> = Box::new([0x01; 4096]);
            let chunk_4kb: &'static [u8; 4096] = Box::leak(chunk_4kb);

            let chunk_8kb: Box<[u8; 8192]> = Box::new([0x02; 8192]);
            let chunk_8kb: &'static [u8; 8192] = Box::leak(chunk_8kb);

            let chunk_16kb: Box<[u8; 16384]> = Box::new([0x03; 16384]);
            let chunk_16kb: &'static [u8; 16384] = Box::leak(chunk_16kb);

            let chunk_32kb: Box<[u8; 32768]> = Box::new([0x04; 32768]);
            let chunk_32kb: &'static [u8; 32768] = Box::leak(chunk_32kb);

            b.to_async(CompioExecutor).iter(|| async {
                let temp_file = create_temp_file();
                let file =
                    compio::fs::OpenOptions::new().write(true).create(true).open(temp_file.path()).await.unwrap();

                let mut seq_file = SeqBufFile::with_posistion(
                    file,
                    0,
                    std::num::NonZeroUsize::new(base).unwrap(),
                    std::num::NonZeroUsize::new(max).unwrap(),
                );

                // Simulate HTTP download with varying chunk sizes
                // Pattern: 50% 4KB, 25% 8KB, 15% 16KB, 7% 32KB, 3% larger
                let iterations = total_size / 4096 / 2; // approx

                for i in 0..iterations {
                    // Mix different chunk sizes
                    match i % 10 {
                        0..=5 => {
                            seq_file.write_all(chunk_4kb).await.unwrap();
                        }
                        6..=7 => {
                            seq_file.write_all(chunk_8kb).await.unwrap();
                        }
                        8 => {
                            seq_file.write_all(chunk_16kb).await.unwrap();
                        }
                        9 => {
                            seq_file.write_all(chunk_32kb).await.unwrap();
                        }
                        _ => unreachable!(),
                    }
                }

                seq_file.flush().await.unwrap();
            });
        });

        group.finish();
    }
}

criterion_group!(benches, bench_buffer_sizing);
criterion_main!(benches);
