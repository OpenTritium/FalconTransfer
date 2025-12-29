# FalconTransfer 日志开发规范

本文档定义了 FalconTransfer 项目中日志记录和错误处理的规范标准。

## 1. 日志等级决策树 (Log Level Decision Logic)

在决定使用哪个日志等级时，请严格遵循以下逻辑判断流程：

* **Rule-L1**: 是否是系统崩溃、数据丢失或无法恢复的 IO 错误？ -> **ERROR**
* **Rule-L2**: 是否是配置缺失、API 废弃或自动降级（不影响主流程）？ -> **WARN**
* **Rule-L3**: 是否是正常的业务里程碑（如登录成功、传输开始/结束）？ -> **INFO**
* **Rule-L4**: 是否用于开发阶段排查逻辑分支或变量状态？ -> **DEBUG**
* **Rule-L5**: 是否包含极高频的细节（如循环内的每一步、锁竞争）？ -> **TRACE**

**注意**：生产环境默认过滤掉 `DEBUG` 和 `TRACE`。

---

## 2. 编码规范与反模式 (Code Rules & Anti-Patterns)

### [Rule-01] 格式化语法强制规范

**约束**：**禁止**使用 `format!` 宏拼接字符串。**必须**使用 `tracing` crate 的结构化字段捕获语法。

| 场景           | ❌ 错误写法 (Anti-Pattern)               | ✅ 正确写法 (Best Practice)       | 原因                               |
| -------------- | --------------------------------------- | -------------------------------- | ---------------------------------- |
| **基础变量**   | `info!("User: " + user);`               | `info!(user, "User logged in");` | 避免额外内存分配，支持结构化索引。 |
| **复杂拼接**   | `debug!("Table " + t + " hash: " + h);` | `debug!("Table {t} hash: {h}");` | 保持代码简洁，提升解析效率。       |
| **标识符引用** | `info!("Processing " + path);`          | `info!("Processing `{path}`");`  | 使用反引号包裹标识符，日志更清晰。 |

### [Rule-02] 上下文注入与 Span 命名 (Context Injection & Naming)

**约束**：异步任务入口**必须**使用 `#[instrument]` 宏，且必须明确记录关键 ID（Trace ID）。

**约束扩展**：对于泛型函数或逻辑复杂的任务，**建议显式指定 `name`**，以便在追踪系统（如 Jaeger/Grafana）中更好地区分和聚合。

* **❌ 错误示范 (上下文丢失/命名模糊)**
```rust
// 默认 Span 名称为 "process"，难以区分是处理什么
#[instrument(skip(data))] 
pub async fn process<T>(id: Uuid, data: T) { ... }
```

有一个需要注意的例子是针对错误的捕获，详情请查看 `[Rule-04]`。

* **✅ 正确示范 (全链路追踪)**
```rust
// 1. skip(data) 避免打印大对象
// 2. fields 显式记录 id
// 3. name 显式指定业务语义，而非简单的函数名
#[instrument(name = "transfer.worker", fields(transfer_id = %id), skip(data))]
pub async fn start_transfer(id: Uuid, data: &[u8]) {
    info!("Transfer started"); 
    // 后续调用会自动携带 transfer_id
}
```

### [Rule-03] 错误处理原则 (No Log-and-Throw)

**约束**：**禁止**在中间层既打印 `error!` 又返回 `Err`。错误日志只能在最顶层（Top-level）记录一次。

* **❌ 错误示范 (日志噪声与重复)**
```rust
fn parse_config() -> Result<()> {
    if let Err(e) = File::open("cfg") {
        error!(error = %e, "File error: {}"); // 违规：中间层记录了 Error
        return Err(e);               // 违规：同时向上抛出
    }
    Ok(())
}
```

* **✅ 正确示范 (上下文传递)**
```rust
fn parse_config() -> Result<()> {
    File::open("cfg").map_err(|e| {
        // 仅附加上下文，不打印日志
        anyhow::Error::new(e).context("failed to load system config")
    })
}
// 注意：最终的 error! 日志应仅在 main.rs 或 task root 处打印
```

### [Rule-04] 结构化字段与命名约定 (Structured Fields & Naming)

**约束**：数值型指标（耗时、大小、计数）**必须**作为独立字段记录，禁止拼接到 message 字符串中。
**约束**：记录错误对象时，**必须统一使用字段名 `error`**，以便后端日志系统自动识别异常堆栈。

* **字段命名规则**：`field_name = %variable` 会将变量值绑定到 `field_name` 字段
  * `error = %e` → 字段名是 `error`（✅ 正确，用 `e` 作为错误变量名）
  * `%error` → 字段名是 `error`（✅ 正确，前提是变量名本身就是 `error`）
  * `user_id = %id` → 字段名是 `user_id`（✅ 可读性更好）

* **❌ 错误示范 (字段混乱)**
```rust
info!("Upload finished in {}ms", cost); // 违规：拼接到消息
error!(%e, "Crash");                    // 违规：字段名是 e，不是 error
```

* **✅ 正确示范 (标准统一)**
```rust
// message 仅描述事件，数据放入键值对
info!(duration_ms = cost, size_bytes = size, "Upload finished");

// 错误对象统一命名为 error
error!(%error, "System crashed");
```

### [Rule-05] 字段修饰符 `%` vs `?`

**约束**：必须根据类型特征选择正确的修饰符，以避免编译错误或性能损耗。

* **规则**：
    * `%` : 用于实现了 `Display` 的类型
    * `?` : 用于实现了 `Debug` 的类型，当一个类型没有实现 `Display` 或者 `Display` 展示的内部细节不利于调试时可以 Fallback 到此符号 
    * **简写**：`%` 实际上是默认行为，可以省略不写


### [Rule-06] 避免为日志引入额外变量（No Extra Variables for Logging）

**约束**：**禁止**为了记录日志而引入非业务逻辑需要的变量或计算。日志应跟随业务逻辑，而不是主导业务逻辑。

* **规则细节**：
    1. **禁止引入仅用于日志的计数器** - 如果业务逻辑不需要计数，不要为了日志而添加
    2. **禁止为日志进行额外计算** - 避免遍历集合、求和等仅为了日志的操作
    3. **禁止为日志克隆大对象** - 避免为了在 `await` 后使用而克隆引用类型
    4. **优先使用已有变量** - 日志应记录业务逻辑中自然产生的数据

* **❌ 错误示范（为日志引入额外变量）**
```rust
// 原本不需要这个 count 变量，只是为了日志
let mut count = 0;
for item in items {
    process(item);
    count += 1;  // 仅为了日志
}
info!(processed_count = count, "Processing complete");
```

* **❌ 错误示范（为日志进行额外计算）**
```rust
// 原本不需要计算 total_size，只是为了日志
let total_size: usize = items.iter().map(|i| i.size()).sum();  // 仅为了日志
info!(total_size = total_size, "Items ready");
process_items(items);
```

* **❌ 错误示范（克隆引用类型仅为了日志）**
```rust
// 原本不需要克隆，只是为了在日志中使用
let path_clone = path.to_path_buf();  // 仅为了跨 .await 使用
some_async_work().await;
info!(path = %path_clone.display(), "Work completed");  // 违规：引入了不必要的克隆
```

* **✅ 正确示范（仅使用已有变量）**
```rust
// 使用业务逻辑已有的计数器
let mut processed = 0;
for item in items {
    process(item);
    processed += 1;  // 业务逻辑需要
}
info!(processed_count = processed, "Processing complete");
```

* **✅ 正确示范（在业务逻辑中自然计算）**
```rust
// 如果确实需要指标，在业务逻辑中自然计算
let items = process_items(items);
let total_size = items.iter().map(|i| i.size()).sum();  // 业务逻辑可能需要这个值
info!(total_size = total_size, "Items processed");
```

* **✅ 正确示范（避免克隆，使用 skip 或在 await 前记录）**
```rust
// 方案 1: 在 await 前记录（如果不需要异步结果）
info!(path = %path.display(), "Starting work");
some_async_work().await;

// 方案 2: 使用 instrument 自动注入上下文
#[instrument(skip(data), fields(path = %path.display()))]
async fn work(data: LargeData) {
    some_async_work(data).await;
    // path 通过 instrument 自动注入 span，无需克隆
}
```

* **✅ 正确示范（使用 Span 上下文而非显式传递）**
```rust
// 利用 tracing 的 Span 机制自动携带上下文
#[instrument(fields(request_id = %id))]
async fn handle_request(id: Uuid, data: &[u8]) {
    process(data).await;
    // 这里无需手动传递 id，日志会自动包含 request_id
    info!("Request processed");
}
```


### [Rule-07] 严禁在日志中捕获待处理 Buffer

**约束**：在使用 `compio`/`tokio-uring` 等异步 I/O 库时，在 `await` 完成前，**禁止**在日志中通过引用捕获被传递的 Buffer，以防 Use-after-free 或数据竞争。

* **✅ 正确示范**：
```rust
let (res, buf) = file.read_at(buf, 0).await; // 必须在 await 完成后记录日志
info!(read_bytes = %res.unwrap(), "Read completed");
```

### [Rule-08] 泛型类型感知日志 (Generic Type Awareness)

**约束**：在编写 Rust 泛型函数或结构体时，**必须**在日志上下文中包含泛型类型信息。

* **✅ 正确示范**
```rust
#[instrument(skip(payload), fields(data_type = %std::any::type_name::<T>()))]
pub async fn dispatch<T>(payload: T) {
    info!("Dispatching payload"); // 日志将自动携带数据类型上下文
}
```

### [Rule-09] 错误消息描述规范 (Error Message Semantics)

**约束**：日志消息（Message）部分必须清晰描述**“发生了什么事件”**或**“正在尝试什么操作”**，而不是重复错误内容或使用模糊词汇。

* **规则细节**：
    1. **禁止模糊词**：严禁使用 `Error`、`Failed`、`Something wrong` 作为唯一的日志消息。
    2. **描述意图**：Message 应描述代码的**高层意图**（Intent），而 `error` 字段描述**底层原因**（Root Cause）。
    3. **静态字面量**：Message 应当是静态字符串（Static String Literal），方便日志系统（如 ELK）进行模板聚合。

* **❌ 错误示范 (重复与模糊)**
```rust
// 错误原因：消息就是 error 本身，导致无法区分上下文
error!(%error, "{}", error); 

// 错误原因：信息量为零，查找不到具体位置
error!(%error, "Error occurred"); 

// 错误原因："Database error" 太宽泛
error!(%error, "Database error"); 
```

* **✅ 正确示范 (意图清晰)**
```rust
// Message: "Failed to initialize storage engine" (意图/事件)
// Field error: "No space left on device" (底层原因)
error!(%error, "Failed to initialize storage engine");

// Context: 描述正在做什么
anyhow::Context::context(err, "failed to parse remote configuration");
```

---

## 3. 错误架构与断言 (Error Architecture & Assertions)

### [Rule-10] 错误上下文分层策略 (Context Strategy)

**约束**：根据代码所处的层级，严格区分 `thiserror` 和 `anyhow` 的使用方式。

#### 核心层/库代码 (Core/Library) -> `thiserror`
*   **原则**：定义强类型的、可枚举的错误。不要在逻辑中拼接字符串，上下文应作为**结构体字段**存在。
*   **✅ 正确示范**：
    ```rust
    #[derive(thiserror::Error, Debug)]
    pub enum TransferError {
        #[error("checksum mismatch: expected {expected}, found {found}")]
        ChecksumError { expected: String, found: String }, // 上下文作为字段
    }
    ```

#### 应用层/业务胶水 (Application/Service) -> `anyhow`
*   **原则**：使用 `.context()` 说明“正在做什么”，使用 `.with_context()` 处理昂贵的上下文构建。
*   **✅ 正确示范**：
    ```rust
    // 动态添加业务意图
    db.get(id).with_context(|| format!("failed to fetch metadata for id: {}", id))?;
    ```

### [Rule-11] Debug 断言与防御性编程 (Assertions)

**约束**：清晰区分“运行时错误”与“逻辑缺陷（Bug）”。

*   **Rule-11.1**: **运行时错误**（I/O、网络、脏数据） -> 使用 `Result<T, E>` + `warn!/error!`。**禁止 Panic**。
*   **Rule-11.2**: **逻辑不变量**（代码逻辑保证绝对不应该发生的情况） -> 使用 `debug_assert!`。
*   **Rule-11.3**: **无副作用** -> 严禁在断言中包含副作用代码（如 `debug_assert!(file.write(..).is_ok())`）。

*   **✅ 正确示范**
    ```rust
    // 内部函数，调用者必须保证 idx 有效
    fn unsafe_access(data: &[u8], idx: usize) -> u8 {
        debug_assert!(idx < data.len(), "Index out of bounds: idx={}, len={}", idx, data.len());
        data[idx]
    }
    ```

### [Rule-12] 日志分层策略 (Logging Layering Strategy)

**约束**：严格区分底层库代码与业务代码的日志责任，避免日志依赖污染。

#### 核心层/库代码 (Core/Library) -> 禁止引入日志框架
*   **原则**：底层库不引入 `tracing`、`log` 等日志框架依赖，所有错误和异常通过返回值传递。
*   **原因**：
    - 保持库的可复用性和独立性（用户可能使用不同的日志系统）
    - 降低编译依赖和二进制体积
    - 让调用方决定如何处理错误和记录日志
*   **✅ 正确示范**：
    ```rust
    // 核心库：定义错误类型，不引入日志
    #[derive(thiserror::Error, Debug)]
    pub enum StorageError {
        #[error("I/O failed: {0}")]
        Io(#[from] std::io::Error),
        #[error("checksum mismatch")]
        ChecksumFailed,
    }
    
    pub fn read_block(path: &Path) -> Result<Vec<u8>, StorageError> {
        std::fs::read(path).map_err(Into::into)
    }
    ```

#### 应用层/业务胶水 (Application/Service) -> 使用 tracing 记录日志
*   **原则**：业务层在调用底层库时，捕获错误并使用 `tracing` 记录日志。
*   **原因**：业务层了解上下文和业务意图，适合决定日志等级和内容。
*   **✅ 正确示范**：
    ```rust
    // 业务层：引入 tracing，记录业务上下文
    use tracing::{error, info, instrument};
    
    #[instrument(fields(path = %path))]
    pub async fn load_config(path: &Path) -> Result<Config, AppError> {
        match storage::read_block(path) {
            Ok(data) => {
                info!(size = data.len(), "Config loaded successfully");
                parse_config(&data)
            }
            Err(e) => {
                error!(error = %e, "Failed to load config file");
                Err(AppError::ConfigLoad(e))
            }
        }
    }
    ```

*   **❌ 错误示范**：
    ```rust
    // 错误：底层库引入了 tracing
    use tracing::error;  // ← 库代码不应该这样做
    
    pub fn read_block(path: &Path) -> Result<Vec<u8>, StorageError> {
        let data = std::fs::read(path)?;
        error!("Read {} bytes", data.len());  // ← 违反规则
        Ok(data)
    }
    ```


