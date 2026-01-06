#![no_main]

use arbitrary::Arbitrary;
use compio::{
    fs::OpenOptions,
    io::{AsyncReadAtExt, AsyncWrite},
};
use falcon_filesystem::SeqBufFile;
use libfuzzer_sys::fuzz_target;
use std::cell::RefCell;
use tempfile::TempDir;

thread_local! {
    static RUNTIME: RefCell<compio::runtime::Runtime> =
        RefCell::new(compio::runtime::Runtime::new().unwrap());
}

#[derive(Debug, Arbitrary)]
enum SeqAction {
    /// 写入随机字节
    Write(Vec<u8>),
    /// 写入矢量字节（测试 write_vectored）
    WriteVectored(Vec<Vec<u8>>),
    /// 手动 Flush
    Flush,
}

#[derive(Debug, Arbitrary)]
struct FuzzCase {
    actions: Vec<SeqAction>,
}

// 模拟模型：Ground Truth
struct Model {
    data: Vec<u8>,
}

impl Model {
    fn new() -> Self { Self { data: Vec::new() } }

    fn write(&mut self, data: &[u8]) { self.data.extend_from_slice(data); }
}

fuzz_target!(|case: FuzzCase| {
    // 如果没有任何操作，跳过这个测试用例
    if case.actions.is_empty() {
        return;
    }

    RUNTIME.with(|runtime| {
        runtime.borrow_mut().block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let file_path = temp_dir.path().join("fuzz_seq.bin");

            // 1. 创建被测对象
            let file = OpenOptions::new().create(true).write(true).read(true).open(&file_path).await.unwrap();
            let mut seq_file = SeqBufFile::from(file);

            // 2. 创建模型
            let mut model = Model::new();

            // 3. 执行动作
            for action in case.actions {
                match action {
                    SeqAction::Write(data) => {
                        model.write(&data);
                        let _ = seq_file.write(data).await.unwrap();
                    }
                    SeqAction::WriteVectored(data_vecs) => {
                        // 构造 vectored引用
                        let mut slices = Vec::new();
                        for v in data_vecs {
                            model.write(&v);
                            slices.push(v);
                        }
                        if slices.is_empty() {
                            continue;
                        }
                        let _ = seq_file.write_vectored(slices).await.unwrap();
                    }
                    SeqAction::Flush => {
                        seq_file.flush().await.unwrap();
                        // Flush 后，可以额外验证一下 Buffered range 和 Flushed range 的一致性
                        // 但最核心的验证是读取文件内容
                    }
                }
            }

            // 4. 结束并验证
            // shutdown 保证最后的数据落盘
            seq_file.shutdown().await.unwrap();

            // 读取实际文件内容
            let file = OpenOptions::new().read(true).open(&file_path).await.unwrap();
            let (n, buf) = file.read_to_end_at(Vec::new(), 0).await.unwrap();

            // 5. 对比
            assert_eq!(n, model.data.len(), "File size mismatch");
            if buf != model.data {
                // 找出不匹配的地方方便调试
                let len = std::cmp::min(buf.len(), model.data.len());
                for i in 0..len {
                    if buf[i] != model.data[i] {
                        panic!("Mismatch at index {}: expected {}, got {}", i, model.data[i], buf[i]);
                    }
                }
            }
        })
    })
});
