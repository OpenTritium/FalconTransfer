//! 示例程序：展示 TaskStatus 的格式化输出
//!
//! 运行方式：
//! ```bash
//! cargo run --example status_display
//! ```

use falcon_identity::task::TaskId;
use falcon_task_composer::{TaskStatus, WorkerError};
use sparse_ranges::RangeSet;
use url::Url;

fn main() {
    println!("\n╔═══════════════════════════════════════════════════════════════════════════════╗");
    println!("║               TaskStatus Display Format Examples                             ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════╝\n");

    // 示例 1: 运行中的任务，有进度和缓冲
    println!("【示例 1】运行中的任务 - 有进度和缓冲数据");
    println!("─────────────────────────────────────────────────────────");
    let mut status = TaskStatus::default_with(
        TaskId::new(),
        "ubuntu-24.04.3-desktop-amd64.iso",
        Url::parse("https://releases.ubuntu.com/24.04/ubuntu-24.04.3-desktop-amd64.iso").unwrap(),
        Some("/downloads/ubuntu.iso".into()),
        Some(6_000_000_000), // 6 GB
    );
    status.buffered.insert_n_at(1_500_000_001, 0); // 1.5 GB buffered
    status.flushed.insert_n_at(1_000_000_001, 0); // 1 GB flushed
    status.state.set_running();
    println!("{}\n", status);

    // 示例 2: 完成的任务
    println!("【示例 2】已完成的任务");
    println!("─────────────────────────────────────────────────────────");
    let mut status_completed = TaskStatus::default_with(
        TaskId::new(),
        "rust-1.75.0-x86_64-pc-windows-msvc.msi",
        Url::parse("https://static.rust-lang.org/dist/rust-1.75.0-x86_64-pc-windows-msvc.msi").unwrap(),
        Some("C:\\Downloads\\rust-installer.msi".into()),
        Some(300_000_000), // 300 MB
    );
    status_completed.buffered.insert_n_at(300_000_000, 0);
    status_completed.flushed.insert_n_at(300_000_000, 0);
    status_completed.state.set_completed();
    println!("{}\n", status_completed);

    // 示例 3: 失败的任务带错误信息
    println!("【示例 3】失败的任务 - 带错误信息");
    println!("─────────────────────────────────────────────────────────");
    let mut status_failed = TaskStatus::default_with(
        TaskId::new(),
        "large-dataset.tar.gz",
        Url::parse("https://example.com/data/large-dataset.tar.gz").unwrap(),
        Some("/tmp/dataset.tar.gz".into()),
        Some(10_000_000_000), // 10 GB
    );
    status_failed.buffered.insert_n_at(2_500_000_000, 0);
    status_failed.flushed.insert_n_at(2_000_000_000, 0);
    status_failed.state.set_failed();
    status_failed.set_err(WorkerError::ParitalDownloaded(RangeSet::new()));
    println!("{}\n", status_failed);

    // 示例 4: 没有总大小的流式下载
    println!("【示例 4】流式下载 - 无总大小");
    println!("─────────────────────────────────────────────────────────");
    let mut status_streaming = TaskStatus::default_with(
        TaskId::new(),
        "live-stream.mp4",
        Url::parse("https://streaming.example.com/live").unwrap(),
        None,
        None,
    );
    status_streaming.buffered.insert_n_at(50_000_000, 0); // 50 MB
    status_streaming.flushed.insert_n_at(45_000_000, 0); // 45 MB
    status_streaming.state.set_running();
    println!("{}\n", status_streaming);

    // 示例 5: 暂停的任务
    println!("【示例 5】已暂停的任务");
    println!("─────────────────────────────────────────────────────────");
    let mut status_paused = TaskStatus::default_with(
        TaskId::new(),
        "debian-12.5.0-amd64-DVD-1.iso",
        Url::parse("https://cdimage.debian.org/debian-cd/current/amd64/iso-dvd/debian-12.5.0-amd64-DVD-1.iso").unwrap(),
        Some("/home/user/downloads/debian.iso".into()),
        Some(4_000_000_000), // 4 GB
    );
    status_paused.buffered.insert_n_at(800_000_000, 0);
    status_paused.flushed.insert_n_at(800_000_000, 0);
    status_paused.state.set_paused();
    println!("{}\n", status_paused);

    // 示例 6: 空闲的任务（刚创建）
    println!("【示例 6】空闲任务 - 刚创建还未开始");
    println!("─────────────────────────────────────────────────────────");
    let status_idle = TaskStatus::default_with(
        TaskId::new(),
        "nodejs-v20.11.0-win-x64.zip",
        Url::parse("https://nodejs.org/dist/v20.11.0/node-v20.11.0-win-x64.zip").unwrap(),
        Some("C:\\Temp\\nodejs.zip".into()),
        Some(35_000_000), // 35 MB
    );
    println!("{}\n", status_idle);

    // 示例 7: 取消的任务
    println!("【示例 7】已取消的任务");
    println!("─────────────────────────────────────────────────────────");
    let mut status_cancelled = TaskStatus::default_with(
        TaskId::new(),
        "cancelled-download.bin",
        Url::parse("https://example.com/file.bin").unwrap(),
        Some("/tmp/file.bin".into()),
        Some(1_000_000_000),
    );
    status_cancelled.buffered.insert_n_at(150_000_000, 0);
    status_cancelled.flushed.insert_n_at(150_000_000, 0);
    status_cancelled.state.set_cancelled();
    println!("{}\n", status_cancelled);

    // 状态图例
    println!("╔═══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                           State Legend                                        ║");
    println!("╠═══════════════════════════════════════════════════════════════════════════════╣");
    println!("║  🔵 Idle       - 任务已创建但未开始                                           ║");
    println!("║  🟢 Running    - 任务正在下载中                                               ║");
    println!("║  🟡 Paused     - 任务已暂停                                                    ║");
    println!("║  ✅ Completed  - 任务成功完成                                                  ║");
    println!("║  ⚪ Cancelled  - 任务被用户取消                                                ║");
    println!("║  ❌ Failed     - 任务失败（超过重试次数）                                      ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════╝\n");
}
