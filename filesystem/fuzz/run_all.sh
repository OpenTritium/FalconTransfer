#!/bin/bash
# 并行运行所有模糊测试（前台运行，实时输出）

cd /home/tritium/FalconTransfer/filesystem/fuzz

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 检测CPU核心数
CPU_CORES=$(nproc)
# 计算每个目标的worker数（14核心，留2个给系统，12/4=3个worker）
# 每个fuzz目标独立运行，4个目标 x 3核心 = 12核心
WORKERS=$(( (CPU_CORES - 2) / 4 ))
if [ $WORKERS -lt 1 ]; then
    WORKERS=1
fi

echo -e "${GREEN}=== 启动所有模糊测试 (前台并行运行) ===${NC}"
echo -e "${BLUE}CPU核心数: ${CPU_CORES}, 每个目标 ${WORKERS} 个worker, 总计 $((WORKERS * 4)) 个并行进程${NC}"
echo ""
echo "测试目标:"
echo "  1. fuzz_buffer       - Buffer 结构体"
echo "  2. fuzz_vectored_buffer - VectoredBuffer 结构体"
echo "  3. fuzz_seqfile      - SeqBufFile 顺序文件"
echo "  4. fuzz_randfile     - RandBufFile 随机访问文件"
echo ""
echo -e "${YELLOW}按 Ctrl+C 停止所有测试${NC}"
echo ""

# 创建日志目录
mkdir -p logs

# 启动所有模糊测试（每个目标独立运行，不使用 -jobs）
# 使用 -max_len 和 -timeout 优化性能
# 4个目标 x ${WORKERS} worker = 总共使用 $((WORKERS * 4)) 个CPU核心
cargo fuzz run fuzz_buffer -- -max_len=4096 -timeout=10 2>&1 | tee logs/fuzz_buffer.log | grep -E "(NEW|cov:|exec/s|crash|ERROR|initiated)" &
PID1=$!
echo -e "${GREEN}[PID $PID1][fuzz_buffer]       已启动${NC}"

cargo fuzz run fuzz_vectored_buffer -- -max_len=4096 -timeout=10 2>&1 | tee logs/fuzz_vectored_buffer.log | grep -E "(NEW|cov:|exec/s|crash|ERROR|initiated)" &
PID2=$!
echo -e "${GREEN}[PID $PID2][fuzz_vectored_buffer] 已启动${NC}"

cargo fuzz run fuzz_seqfile -- -max_len=4096 -timeout=10 2>&1 | tee logs/fuzz_seqfile.log | grep -E "(NEW|cov:|exec/s|crash|ERROR|initiated)" &
PID3=$!
echo -e "${GREEN}[PID $PID3][fuzz_seqfile]      已启动${NC}"

cargo fuzz run fuzz_randfile -- -max_len=4096 -timeout=10 2>&1 | tee logs/fuzz_randfile.log | grep -E "(NEW|cov:|exec/s|crash|ERROR|initiated)" &
PID4=$!
echo -e "${GREEN}[PID $PID4][fuzz_randfile]     已启动${NC}"

echo ""
echo -e "${GREEN}所有模糊测试已启动，正在前台运行...${NC}"
echo -e "${BLUE}重要输出将实时显示在终端上${NC}"
echo -e "${YELLOW}完整日志保存在 logs/ 目录${NC}"
echo ""
echo -e "${BLUE}提示: 在另一个终端查看日志: tail -f logs/*.log${NC}"
echo ""

# 保存 PID 到文件
echo "$PID1 $PID2 $PID3 $PID4" > .fuzz_pids

# 设置清理函数
cleanup() {
    echo ""
    echo -e "${YELLOW}正在停止所有模糊测试...${NC}"
    for pid in $PID1 $PID2 $PID3 $PID4; do
        if kill -0 $pid 2>/dev/null; then
            kill $pid 2>/dev/null
            echo -e "  ${GREEN}已停止 PID $pid${NC}"
        fi
    done
    # 确保所有子进程都被清理
    pkill -P $$ 2>/dev/null
    rm -f .fuzz_pids
    echo -e "${GREEN}所有测试已停止${NC}"
    exit 0
}

# 捕获 Ctrl+C
trap cleanup INT TERM

# 等待所有后台进程
wait
