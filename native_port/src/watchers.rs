use futures_util::{
    Stream, StreamExt,
    future::{BoxFuture, FutureExt},
    stream::{AbortHandle, Abortable, FuturesUnordered},
};
use std::{
    collections::HashMap,
    hash::Hash,
    pin::Pin,
    task::{Context, Poll},
};
use tokio_stream::StreamMap;

// 1. 定义内部具体的流类型
// 关键点：必须是 Pin<Box<...>>，这样它才实现了 Stream trait
type PinnedStream<V> = Pin<Box<dyn Stream<Item = V> + Send>>;

// 2. 定义我们在任务间传递的流包装器
// Abortable<T> 本身也是 Unpin 的（只要 T 是 Unpin 的），所以不需要再包一层 Pin<Box>
type RecyclableStream<V> = Abortable<PinnedStream<V>>;

// 3. Future 的返回结果：Key, Value, 和归还的流
type RecycleResult<K, V> = (K, Option<V>, RecyclableStream<V>);

pub struct WatchGroup<K, V> {
    // 存放 "等待下一个值" 的 Future
    tasks: FuturesUnordered<BoxFuture<'static, RecycleResult<K, V>>>,

    // 仅用于触发 Abort，不持有流的所有权
    handles: HashMap<K, AbortHandle>,
}

impl<K, V> WatchGroup<K, V>
where
    K: Hash + Eq + Clone + Send + 'static,
    V: Send + 'static,
{
    pub fn new() -> Self { Self { tasks: FuturesUnordered::new(), handles: HashMap::new() } }

    pub fn push_stream<S>(&mut self, id: K, stream: S)
    where
        S: Stream<Item = V> + Send + 'static,
    {
        // 1. 清理旧流
        self.remove(&id);

        // 2. 创建 Abort 控制器
        let (handle, reg) = AbortHandle::new_pair();

        // 3. 构造流对象
        // 关键修复：使用 Box::pin 而不是 Box::new
        // 这样 PinnedStream 实现了 Stream，Abortable 也就自动实现了 Stream
        let pinned_stream: PinnedStream<V> = Box::pin(stream);
        let abortable_stream = Abortable::new(pinned_stream, reg);

        self.handles.insert(id.clone(), handle);

        // 4. 启动第一个任务
        self.tasks.push(Self::make_future(id, abortable_stream));
    }

    pub fn remove(&mut self, id: &K) {
        if let Some(handle) = self.handles.remove(id) {
            handle.abort();
        }
    }

    #[inline]
    fn make_future(key: K, mut stream: RecyclableStream<V>) -> BoxFuture<'static, RecycleResult<K, V>> {
        // Box::pin 也就是 boxed()，生成 BoxFuture
        async move {
            // 这里能调用 next() 是因为：
            // 1. RecyclableStream 是 Abortable<Pin<Box<...>>>
            // 2. Pin<Box<...>> 实现了 Stream 且是 Unpin
            // 3. 所以 Abortable 实现了 Stream 且是 Unpin
            // 4. StreamExt::next() 需要 &mut Self (where Self: Unpin)，满足条件
            let val = stream.next().await;
            (key, val, stream)
        }
        .boxed()
    }
}

impl<K, V> Stream for WatchGroup<K, V>
where
    K: Hash + Eq + Clone + Send + 'static,
    V: Send + 'static,
    Self: Unpin,
{
    type Item = (K, V);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.tasks.is_empty() {
                return Poll::Pending;
            }

            match self.tasks.poll_next_unpin(cx) {
                Poll::Ready(Some((key, val, stream))) => {
                    match val {
                        Some(v) => {
                            // 流还有数据，制造下一个 Future 放回队列
                            self.tasks.push(Self::make_future(key.clone(), stream));
                            return Poll::Ready(Some((key, v)));
                        }
                        None => {
                            // 流结束或被 Abort，清理 handle 并继续轮询其他流
                            self.handles.remove(&key);
                            continue;
                        }
                    }
                }
                Poll::Ready(None) => return Poll::Pending,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
// ----------------------
// Tests
// ----------------------
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    #[tokio::test]
    async fn test_high_perf_streams() {
        let mut wg = WatchGroup::<String, i32>::new();

        let (tx, rx) = mpsc::unbounded_channel();
        wg.push_stream("k1".to_string(), UnboundedReceiverStream::new(rx));

        tx.send(1).unwrap();
        let (k, v) = wg.next().await.unwrap();
        assert_eq!(k, "k1");
        assert_eq!(v, 1);

        // 测试 Abort
        wg.remove(&"k1".to_string());
        tx.send(2).unwrap();

        // 应该拿不到数据了
        let res = tokio::time::timeout(std::time::Duration::from_millis(10), wg.next()).await;
        assert!(res.is_err());
    }
}
