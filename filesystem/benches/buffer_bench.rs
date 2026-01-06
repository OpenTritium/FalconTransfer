// Buffer performance benchmarks comparing compio, tokio and std
//
// These benchmarks compare SeqBufFile and RandBufFile performance
// against tokio's file operations and std's synchronous file I/O.
//
// Uses Criterion's native async benchmarking support with proper executors.

use compio::io::{AsyncWrite, AsyncWriteAt, AsyncWriteExt};
use criterion::{Criterion, Throughput, async_executor::AsyncExecutor, criterion_group, criterion_main};
use falcon_filesystem::{RandBufFile, SeqBufFile};
use std::{io::Write, path::Path};
use tempfile::NamedTempFile;

// ==================== Async Executors ====================

/// Compio executor wrapper for Criterion async benchmarks
struct CompioExecutor;

impl AsyncExecutor for CompioExecutor {
    fn block_on<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        // Use compio's current-thread runtime for benchmarking
        compio::runtime::Runtime::new().unwrap().block_on(future)
    }
}

// ==================== Helpers ====================

fn create_temp_file() -> NamedTempFile { NamedTempFile::new().unwrap() }

async fn open_compio_file(path: &Path) -> compio::fs::File {
    compio::fs::OpenOptions::new().read(true).write(true).truncate(true).create(true).open(path).await.unwrap()
}

async fn open_tokio_file(path: &Path) -> tokio::fs::File {
    tokio::fs::OpenOptions::new().read(true).write(true).truncate(true).create(true).open(path).await.unwrap()
}

fn open_std_file(path: &Path) -> std::fs::File {
    std::fs::OpenOptions::new().read(true).write(true).truncate(true).create(true).open(path).unwrap()
}

// ==================== Sequential Write Benchmarks ====================
// Test: Write 10MB sequentially with 8KB chunks
// Comparison:
//   - Compio SeqBufFile (our high-level buffered file)
//   - Tokio direct writes
//   - Tokio BufWriter
//   - Std direct writes
//   - Std BufWriter

fn bench_sequential_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_write");
    group.throughput(Throughput::Bytes(10 * 1024 * 1024));

    // Compio: Direct writes (no buffering)
    group.bench_function("compio", |b| {
        let chunk: Box<[u8; 8 * 1024]> = Box::new([b'X'; 8 * 1024]);
        let chunk: &'static [u8; 8 * 1024] = Box::leak(chunk);

        b.to_async(CompioExecutor).iter(|| async {
            let temp_file = create_temp_file();
            let file = open_compio_file(temp_file.path()).await;
            let mut file = std::io::Cursor::new(file);

            for _ in 0..1280 {
                file.write_all(chunk).await.unwrap();
            }
            file.flush().await.unwrap();
        });
    });

    // Compio: SeqBufFile (high-level sequential buffered file)
    // buffer_base=64KB (doubled) to reduce flush frequency, buffer_max=128KB for burst handling
    group.bench_function("compio_seqbuf", |b| {
        // Pre-allocate static data to satisfy 'static requirement
        let chunk: Box<[u8; 8 * 1024]> = Box::new([b'X'; 8 * 1024]);
        let chunk: &'static [u8; 8 * 1024] = Box::leak(chunk);

        b.to_async(CompioExecutor).iter(|| async {
            let temp_file = create_temp_file();
            let file = open_compio_file(temp_file.path()).await;
            let mut seq_file = SeqBufFile::with_posistion(
                file,
                0,
                std::num::NonZeroUsize::new(64 * 1024).unwrap(), // 64KB base (doubled)
                std::num::NonZeroUsize::new(128 * 1024).unwrap(), // 128KB max for burst
            );

            for _ in 0..1280 {
                use compio::io::AsyncWriteExt;
                seq_file.write_all(chunk).await.unwrap();
            }
            seq_file.flush().await.unwrap();
        });
    });

    // Compio: BufWriter (compio::io::BufWriter)
    group.bench_function("compio_bufwriter", |b| {
        let chunk: Box<[u8; 8 * 1024]> = Box::new([b'X'; 8 * 1024]);
        let chunk: &'static [u8; 8 * 1024] = Box::leak(chunk);

        b.to_async(CompioExecutor).iter(|| async {
            let temp_file = create_temp_file();
            let path = temp_file.path().to_path_buf();
            let file = open_compio_file(&path).await;
            let file = compio::fs::AsyncFd::new(file).unwrap();
            let mut writer = compio::io::BufWriter::with_capacity(64 * 1024, file);

            for _ in 0..1280 {
                writer.write_all(chunk).await.unwrap();
            }
            writer.flush().await.unwrap();
        });
    });

    // Tokio: Direct writes
    let rt = tokio::runtime::Runtime::new().unwrap();
    group.bench_function("tokio", |b| {
        b.to_async(&rt).iter(|| async {
            use tokio::io::AsyncWriteExt;
            let temp_file = create_temp_file();
            let mut file = open_tokio_file(temp_file.path()).await;

            let chunk = vec![b'T'; 8 * 1024];
            for _ in 0..1280 {
                file.write_all(&chunk).await.unwrap();
            }
            file.flush().await.unwrap();
        });
    });

    // Tokio: BufWriter (64KB buffer to match others)
    let rt2 = tokio::runtime::Runtime::new().unwrap();
    group.bench_function("tokio_bufwriter", |b| {
        b.to_async(&rt2).iter(|| async {
            use tokio::io::AsyncWriteExt;
            let temp_file = create_temp_file();
            let file = open_tokio_file(temp_file.path()).await;
            let mut writer = tokio::io::BufWriter::with_capacity(64 * 1024, file);

            let chunk = vec![b'T'; 8 * 1024];
            for _ in 0..1280 {
                writer.write_all(&chunk).await.unwrap();
            }
            writer.flush().await.unwrap();
        });
    });

    // Std: Direct writes
    group.bench_function("std", |b| {
        b.iter(|| {
            let temp_file = create_temp_file();
            let mut file = open_std_file(temp_file.path());

            let chunk = vec![b'S'; 8 * 1024];
            for _ in 0..1280 {
                file.write_all(&chunk).unwrap();
            }
            file.flush().unwrap();
        });
    });

    // Std: BufWriter (64KB buffer to match others)
    group.bench_function("std_bufwriter", |b| {
        b.iter(|| {
            let temp_file = create_temp_file();
            let file = open_std_file(temp_file.path());
            let mut writer = std::io::BufWriter::with_capacity(64 * 1024, file);

            let chunk = vec![b'S'; 8 * 1024];
            for _ in 0..1280 {
                writer.write_all(&chunk).unwrap();
            }
            writer.flush().unwrap();
        });
    });

    group.finish();
}

// ==================== Random Write Benchmarks ====================
// Test: 10000 random writes of 1KB each at different offsets
// Comparison:
//   - Compio RandBufFile (high-level random-access buffered file)
//   - Tokio direct random writes (seek + write)
//   - Std direct random writes (seek + write)

fn bench_random_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_write");
    group.throughput(Throughput::Bytes(10 * 1024 * 1024));

    // Compio: Direct random writes (no buffering)
    group.bench_function("compio", |b| {
        let data: Box<[u8; 1024]> = Box::new([b'X'; 1024]);
        let data: &'static [u8; 1024] = Box::leak(data);

        b.to_async(CompioExecutor).iter(|| async {
            let temp_file = create_temp_file();
            let mut file = open_compio_file(temp_file.path()).await;

            for i in 0..10000 {
                file.write_at(data, i * 1024).await.unwrap();
            }
        });
    });

    // Compio: RandBufFile (high-level random-access buffered file)
    // Using 64KB buffer size
    group.bench_function("compio_randbuf", |b| {
        // Pre-allocate static data to satisfy 'static requirement
        let data: Box<[u8; 1024]> = Box::new([b'X'; 1024]);
        let data: &'static [u8; 1024] = Box::leak(data);

        b.to_async(CompioExecutor).iter(|| async {
            let temp_file = create_temp_file();
            let file = open_compio_file(temp_file.path()).await;
            // Use explicit buffer size: 64KB
            let mut rand_file = RandBufFile::new(file, std::num::NonZeroUsize::new(64 * 1024).unwrap());

            for i in 0..10000 {
                rand_file.write_at(data, i * 1024).await.unwrap();
            }
            rand_file.shutdown().await.unwrap();
        });
    });

    // Tokio: Direct random writes
    let rt = tokio::runtime::Runtime::new().unwrap();
    group.bench_function("tokio", |b| {
        b.to_async(&rt).iter(|| async {
            let temp_file = create_temp_file();
            let mut file = open_tokio_file(temp_file.path()).await;
            use tokio::io::AsyncWriteExt;
            let chunk = vec![b'T'; 1024];
            for i in 0..10000 {
                use tokio::io::AsyncSeekExt;
                file.seek(std::io::SeekFrom::Start(i * 1024)).await.unwrap();
                file.write_all(&chunk).await.unwrap();
            }
            file.flush().await.unwrap();
        });
    });

    // Std: Direct random writes
    group.bench_function("std", |b| {
        b.iter(|| {
            let temp_file = create_temp_file();
            let mut file = open_std_file(temp_file.path());

            let chunk = vec![b'S'; 1024];
            for i in 0..10000 {
                use std::io::Seek;
                file.seek(std::io::SeekFrom::Start(i * 1024)).unwrap();
                file.write_all(&chunk).unwrap();
            }
            file.flush().unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_sequential_writes, bench_random_writes);
criterion_main!(benches);
