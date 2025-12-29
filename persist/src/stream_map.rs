// This file originally derives from the `tokio-stream` crate's `StreamMap` implementation.
// Source: https://github.com/tokio-rs/tokio/tree/master/tokio-stream
// The code has been adapted for use in this project.
use futures_util::Stream;
use std::{
    borrow::Borrow,
    future::poll_fn,
    hash::Hash,
    pin::Pin,
    task::{Context, Poll, ready},
};

#[derive(Debug)]
pub struct StreamMap<K, V> {
    entries: Vec<(K, V)>,
}

impl<K, V> StreamMap<K, V> {
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &(K, V)> { self.entries.iter() }

    #[inline]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut (K, V)> { self.entries.iter_mut() }

    #[inline]
    pub fn new() -> StreamMap<K, V> { StreamMap { entries: vec![] } }

    #[inline]
    pub fn with_capacity(capacity: usize) -> StreamMap<K, V> { StreamMap { entries: Vec::with_capacity(capacity) } }

    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = &K> { self.iter().map(|(k, _)| k) }

    #[inline]
    pub fn values(&self) -> impl Iterator<Item = &V> { self.iter().map(|(_, v)| v) }

    #[inline]
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> { self.iter_mut().map(|(_, v)| v) }

    #[inline]
    pub fn capacity(&self) -> usize { self.entries.capacity() }

    #[inline]
    pub fn len(&self) -> usize { self.entries.len() }

    #[inline]
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    #[inline]
    pub fn clear(&mut self) { self.entries.clear(); }

    // insert 需要确保 key 不重复以及数组间没有间隙，就只能这样实现
    #[inline]
    pub fn insert(&mut self, k: K, stream: V) -> Option<V>
    where
        K: Hash + Eq,
    {
        let ret = self.remove(&k);
        self.entries.push((k, stream));

        ret
    }

    #[inline]
    pub fn remove<Q>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        for i in 0..self.entries.len() {
            if self.entries[i].0.borrow() == k {
                return Some(self.entries.swap_remove(i).1);
            }
        }

        None
    }

    #[inline]
    pub fn contains_key<Q>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.keys().any(|ck| ck.borrow() == k)
    }
}

impl<K, V> StreamMap<K, V>
where
    K: Unpin,
    V: Stream + Unpin,
{
    fn poll_next_entry(&mut self, cx: &mut Context<'_>) -> Poll<Option<(usize, V::Item)>> {
        use Poll::*;
        let start = fastrand::usize(..self.entries.len());
        let mut idx = start;

        for _ in 0..self.entries.len() {
            let (_, stream) = &mut self.entries[idx];
            match Pin::new(stream).poll_next(cx) {
                Ready(Some(val)) => return Poll::Ready(Some((idx, val))),
                Ready(None) => {
                    // Remove the entry
                    self.entries.swap_remove(idx);
                    // Check if this was the last entry, if so the cursor needs
                    // to wrap
                    if idx == self.entries.len() {
                        idx = 0;
                    } else if idx < start && start <= self.entries.len() {
                        // The stream being swapped into the current index has
                        // already been polled, so skip it.
                        idx = idx.wrapping_add(1) % self.entries.len();
                    }
                }
                Pending => {
                    idx = idx.wrapping_add(1) % self.entries.len();
                }
            }
        }
        // If the map is empty, then the stream is complete.
        if self.entries.is_empty() { Ready(None) } else { Pending }
    }
}

impl<K, V> Default for StreamMap<K, V> {
    #[inline]
    fn default() -> Self { Self::new() }
}

impl<K, V> StreamMap<K, V>
where
    K: Clone + Unpin,
    V: Stream + Unpin,
{
    pub async fn next_many(&mut self, buffer: &mut Vec<(K, V::Item)>, limit: usize) -> usize {
        poll_fn(|cx| self.poll_next_many(cx, buffer, limit)).await
    }

    pub fn poll_next_many(
        &mut self, cx: &mut Context<'_>, buffer: &mut Vec<(K, V::Item)>, limit: usize,
    ) -> Poll<usize> {
        use Poll::*;
        if limit == 0 || self.entries.is_empty() {
            return Ready(0);
        }
        let mut added = 0;
        let start = fastrand::usize(..self.entries.len());
        let mut idx = start;

        while added < limit {
            // Indicates whether at least one stream returned a value when polled or not
            let mut should_loop = false;
            for _ in 0..self.entries.len() {
                let (_, stream) = &mut self.entries[idx];
                match Pin::new(stream).poll_next(cx) {
                    Ready(Some(val)) => {
                        added += 1;
                        let key = self.entries[idx].0.clone();
                        buffer.push((key, val));
                        should_loop = true;
                        idx = idx.wrapping_add(1) % self.entries.len();
                    }
                    Ready(None) => {
                        // Remove the entry
                        self.entries.swap_remove(idx);
                        // Check if this was the last entry, if so the cursor needs
                        // to wrap
                        if idx == self.entries.len() {
                            idx = 0;
                        } else if idx < start && start <= self.entries.len() {
                            // The stream being swapped into the current index has
                            // already been polled, so skip it.
                            idx = idx.wrapping_add(1) % self.entries.len();
                        }
                    }
                    Pending => {
                        idx = idx.wrapping_add(1) % self.entries.len();
                    }
                }
            }
            if !should_loop {
                break;
            }
        }
        if added > 0 {
            Ready(added)
        } else if self.entries.is_empty() {
            Ready(0)
        } else {
            Pending
        }
    }
}

impl<K, V> Stream for StreamMap<K, V>
where
    K: Clone + Unpin,
    V: Stream + Unpin,
{
    type Item = (K, V::Item);

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;
        if let Some((idx, val)) = ready!(self.poll_next_entry(cx)) {
            let key = self.entries[idx].0.clone();
            Ready(Some((key, val)))
        } else {
            Ready(None)
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let mut ret = (0, Some(0));
        for (_, stream) in &self.entries {
            let hint = stream.size_hint();
            ret.0 += hint.0;
            match (ret.1, hint.1) {
                (Some(a), Some(b)) => ret.1 = Some(a + b),
                (Some(_), None) => ret.1 = None,
                _ => {}
            }
        }
        ret
    }
}

impl<K, V> FromIterator<(K, V)> for StreamMap<K, V>
where
    K: Hash + Eq,
{
    #[inline]
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let iterator = iter.into_iter();
        let (lower_bound, _) = iterator.size_hint();
        let mut stream_map = Self::with_capacity(lower_bound);
        for (key, value) in iterator {
            stream_map.insert(key, value);
        }
        stream_map
    }
}

impl<K, V> Extend<(K, V)> for StreamMap<K, V> {
    #[inline]
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (K, V)>,
    {
        self.entries.extend(iter);
    }
}
