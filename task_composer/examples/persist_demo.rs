use compio::{fs::create_dir_all, time::sleep};
use falcon_persist::{StatePersistPoller, store::PersistStore};
use falcon_task_composer::{TaskCommand, TaskDispatcher, TaskStatus, fetch_meta};
use flume as mpmc;
use see::sync as watch;
use std::time::Duration;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use url::Url;

const FOLDER_NAME_TEST: &str = "FalconTransferTests";

// todo 任务恢复后想办法激发一下第一次下载
#[compio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::ERROR) // 捕获 DEBUG 及更高级别的日志
            .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // 1. 创建和任务调度器沟通的通道
    let (cmd_tx, cmd_rx) = mpmc::unbounded();

    // 2. 创建持久化存储
    let path = std::env::current_dir().unwrap().join(FOLDER_NAME_TEST).join("states.redb");
    create_dir_all(&path.parent().unwrap()).await.unwrap();
    let store = PersistStore::create(path).unwrap();

    // 3. 从持久化存储中恢复任务状态
    let dispatcher = TaskDispatcher::from_store_cmd(&store, &cmd_rx)
        .await
        .unwrap_or_else(|_| TaskDispatcher::builder().cmd(cmd_rx).build());

    // 4. 将持久化存储转换到状态轮询器
    let mut poller = StatePersistPoller::new(store, Duration::from_secs(1));

    // 5. 注册组件的广播器
    poller.register(dispatcher.subscribe_pendings());
    poller.register(dispatcher.subscribe_persisted_tasks());

    // 6. 启动任务调度器
    dispatcher.spawn().detach();

    // 7. 启动状态轮询器
    poller.watch().detach();

    // 8. 创建下载任务
    let url = Url::parse("https://releases.ubuntu.com/24.04/ubuntu-24.04.3-desktop-amd64.iso").unwrap();
    // let url = Url::parse("https://repo.df.qq.com/repo/launcher/deltaforceminiloader0.0.7.38.10430644.exe").unwrap();
    let meta = Box::new(fetch_meta(&url).await.unwrap());
    let status = TaskStatus::builder()
        .name(meta.name())
        .url(meta.url().clone())
        .total(meta.full_content_range().and_then(|rng| rng.last().map(|n| n + 1)))
        .build();
    let (status_tx, mut status_rx) = watch::channel(status);
    let cmd = TaskCommand::Create { meta, watch: status_tx.into() };
    cmd_tx.send_async(cmd).await.unwrap();

    // 9. 显示任务状态
    loop {
        status_rx.changed().await.unwrap();
        sleep(Duration::from_secs(1)).await;
        // 清屏，让显示更清晰
        print!("\x1B[2J\x1B[1;1H");
        // 使用新的 Display 格式显示任务状态
        println!("{}", status_rx.borrow_and_update().as_ref());
    }
}
