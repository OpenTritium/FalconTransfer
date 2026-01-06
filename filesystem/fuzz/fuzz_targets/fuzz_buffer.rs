#![no_main]

use arbitrary::Arbitrary;
use falcon_filesystem::Buffer;
use libfuzzer_sys::fuzz_target;
use std::cell::RefCell;

thread_local! {
    static RUNTIME: RefCell<compio::runtime::Runtime> =
        RefCell::new(compio::runtime::Runtime::new().unwrap());
}

/// 简化版的 Buffer 模糊测试
/// 专注于公共 API 的基本正确性
#[derive(Debug, Arbitrary)]
struct FuzzCase {
    /// 输入数据
    data: Vec<u8>,
    /// 操作序列
    operations: Vec<BufferOp>,
}

#[derive(Debug, Arbitrary)]
enum BufferOp {
    Advance {
        #[arbitrary(with = |u: &mut arbitrary::Unstructured| u.int_in_range(0..=4096))]
        amount: usize,
    },
    SetPosition {
        #[arbitrary(with = |u: &mut arbitrary::Unstructured| u.int_in_range(0..=4096))]
        pos: usize,
    },
    Compact,
    Reset,
}

fuzz_target!(|case: FuzzCase| {
    // 如果没有数据也没有操作，跳过这个测试用例
    if case.data.is_empty() && case.operations.is_empty() {
        return;
    }

    RUNTIME.with(|runtime| {
        runtime.borrow_mut().block_on(async {
            // 创建 buffer
            let mut buffer = Buffer::with_capacity(1024);

            // 写入数据
            let written = buffer.read_from(&case.data);
            assert!(written <= case.data.len());

            // 执行操作序列
            for op in case.operations {
                match op {
                    BufferOp::Advance { amount } => {
                        let max_advance = buffer.remaining_len().min(amount);
                        buffer.advance(max_advance);

                        // 验证不变式
                        assert!(buffer.remaining_len() <= case.data.len());
                    }
                    BufferOp::SetPosition { pos } => {
                        let valid_pos = pos.min(buffer.remaining_len() + buffer.get_position());
                        buffer.set_position(valid_pos);
                    }
                    BufferOp::Compact => {
                        buffer.compact();

                        // 验证数据长度不变
                        assert!(buffer.remaining_len() <= case.data.len());
                    }
                    BufferOp::Reset => {
                        buffer.reset();
                        assert!(buffer.is_empty());
                    }
                }
            }

            // 最终验证
            assert!(buffer.remaining_len() <= case.data.len());
        })
    })
});
