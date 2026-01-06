#![no_main]
use arbitrary::Arbitrary;
use compio::{
    fs::OpenOptions,
    io::{AsyncReadAtExt, AsyncWriteAt},
};
use falcon_filesystem::RandBufFile;
use libfuzzer_sys::fuzz_target;
use std::cell::RefCell;
use tempfile::TempDir;

thread_local! {
    static RUNTIME: RefCell<compio::runtime::Runtime> =
        RefCell::new(compio::runtime::Runtime::new().unwrap());
}

const MAX_FILE_SIZE: u64 = 128 * 1024 * 1024; // 限制文件大小为 128 MB

#[derive(Debug, Arbitrary)]
enum RandAction {
    /// 在指定偏移写入数据
    WriteAt {
        #[arbitrary(with = |u: &mut arbitrary::Unstructured| u.int_in_range(0..=MAX_FILE_SIZE))]
        offset: u64,
        data: Vec<u8>,
    },
    /// 在指定偏移写入矢量数据
    WriteVectoredAt {
        #[arbitrary(with = |u: &mut arbitrary::Unstructured| u.int_in_range(0..=MAX_FILE_SIZE))]
        offset: u64,
        data: Vec<Vec<u8>>,
    },
    /// 手动 Flush
    Flush,
}

#[derive(Debug, Arbitrary)]
struct FuzzCase {
    actions: Vec<RandAction>,
}

// 模拟模型：Ground Truth (使用 Vec<u8> 模拟文件，空洞填0)
struct Model {
    data: Vec<u8>,
}

impl Model {
    fn new() -> Self { Self { data: Vec::new() } }

    fn write_at(&mut self, offset: u64, src: &[u8]) {
        if src.is_empty() {
            return;
        }
        let offset = offset as usize;
        let end = offset + src.len();

        // 如果写入位置超过当前长度，需要填充 0
        if end > self.data.len() {
            self.data.resize(end, 0);
        }

        // 覆盖写入
        self.data[offset..end].copy_from_slice(src);
    }
}

fuzz_target!(|case: FuzzCase| {
    // 如果没有任何操作，跳过这个测试用例
    if case.actions.is_empty() {
        return;
    }

    RUNTIME.with(|runtime| {
        runtime.borrow_mut().block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let file_path = temp_dir.path().join("fuzz_rand.bin");

            let file = OpenOptions::new().create(true).write(true).read(true).open(&file_path).await.unwrap();
            let mut rand_file = RandBufFile::from(file);

            let mut model = Model::new();

            for action in case.actions {
                match action {
                    RandAction::WriteAt { offset, data } => {
                        // 限制单次写入大小，提高 fuzz 效率
                        if data.len() > 1024 * 64 {
                            continue;
                        }

                        model.write_at(offset, &data);
                        let _ = rand_file.write_at(data, offset).await.unwrap();
                    }
                    RandAction::WriteVectoredAt { offset, data } => {
                        let mut slices = Vec::new();
                        let mut current_offset = offset;
                        for v in data {
                            if v.len() > 1024 * 64 {
                                continue;
                            }
                            model.write_at(current_offset, &v);
                            current_offset += v.len() as u64;
                            slices.push(v);
                        }
                        if slices.is_empty() {
                            continue;
                        }
                        let _ = rand_file.write_vectored_at(slices, offset).await.unwrap();
                    }
                    RandAction::Flush => {
                        rand_file.flush().await.unwrap();
                    }
                }
            }

            rand_file.shutdown().await.unwrap();

            // 验证
            let file = OpenOptions::new().read(true).open(&file_path).await.unwrap();

            // 读取整个文件
            let (n, buf) = file.read_to_end_at(Vec::new(), 0).await.unwrap();

            // Model 的数据末尾可能包含 0，而文件如果后面没写过，大小由最大的 offset 决定
            // 但我们在 model write_at 里做了 resize，所以 model.data.len() 应该等于 max_written_pos
            // 实际上文件系统的文件大小也应该是一致的（sparse file 空洞也是大小的一部分）

            // 验证大小
            assert_eq!(n, model.data.len(), "File size mismatch");

            // 验证内容
            if buf != model.data {
                // 简单的 diff 打印
                let common_len = std::cmp::min(buf.len(), model.data.len());
                for i in 0..common_len {
                    if buf[i] != model.data[i] {
                        panic!(
                            "Mismatch at offset {}: expected byte {}, got {}. \nContext: Model vs File",
                            i, model.data[i], buf[i]
                        );
                    }
                }
            }
        })
    })
});
