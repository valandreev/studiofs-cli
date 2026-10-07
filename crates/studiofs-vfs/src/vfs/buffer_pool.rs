use bytes::BytesMut;
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(super) struct BufferPool {
    inner: Arc<Mutex<Vec<BytesMut>>>,
    chunk_size: usize,
    max_buffers: usize,
}

impl BufferPool {
    pub(super) fn new(chunk_size: usize, max_buffers: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::with_capacity(max_buffers))),
            chunk_size: chunk_size.max(1),
            max_buffers,
        }
    }

    pub(super) fn acquire(&self) -> BytesMut {
        let mut buffers = self.inner.lock();
        if let Some(mut buffer) = buffers.pop() {
            buffer.clear();
            return buffer;
        }
        BytesMut::with_capacity(self.chunk_size)
    }

    pub(super) fn acquire_with_bytes(&self, data: &[u8]) -> BytesMut {
        let mut buffer = self.acquire();
        buffer.extend_from_slice(data);
        buffer
    }

    pub(super) fn acquire_read_window(&self, len: usize) -> ReadWindowBuffer {
        let mut buffer = self
            .inner
            .lock()
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(self.chunk_size));
        // Keep initialized bytes on reuse; DataReader overwrites the returned
        // range, including holes, before it is exposed as a cached window.
        if len > buffer.len() {
            buffer.resize(len, 0);
        }
        ReadWindowBuffer {
            pool: self.clone(),
            buffer,
            visible_len: len,
        }
    }

    pub(super) fn release(&self, buffer: BytesMut) {
        if self.max_buffers == 0 {
            return;
        }
        // Write acquisition clears the length; read acquisition preserves it
        // so a warmed window does not pay another full-buffer zero-fill.
        let mut buffers = self.inner.lock();
        if buffers.len() < self.max_buffers {
            buffers.push(buffer);
        }
    }
}

pub(super) struct ReadWindowBuffer {
    pool: BufferPool,
    buffer: BytesMut,
    visible_len: usize,
}

impl ReadWindowBuffer {
    pub(super) fn into_bytes(mut self, written: usize) -> bytes::Bytes {
        self.visible_len = written.min(self.visible_len);
        bytes::Bytes::from_owner(self)
    }
}

impl AsMut<[u8]> for ReadWindowBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buffer[..self.visible_len]
    }
}

impl AsRef<[u8]> for ReadWindowBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.buffer[..self.visible_len]
    }
}

impl Drop for ReadWindowBuffer {
    fn drop(&mut self) {
        if self.buffer.capacity() <= self.pool.chunk_size {
            self.pool.release(std::mem::take(&mut self.buffer));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BufferPool;
    use bytes::Bytes;

    #[test]
    fn buffer_pool_reuses_cleared_buffers() {
        let pool = BufferPool::new(4096, 1);
        let mut first = pool.acquire();
        first.extend_from_slice(b"hello");
        let initial_capacity = first.capacity();
        pool.release(first);

        let second = pool.acquire();
        assert_eq!(second.len(), 0);
        assert_eq!(second.capacity(), initial_capacity);
    }

    #[test]
    fn read_window_reuses_initialized_storage_without_clearing() {
        let pool = BufferPool::new(8, 1);
        let mut first = pool.acquire_read_window(8);
        first.as_mut().copy_from_slice(b"abcdefgh");
        let pointer = first.as_mut().as_ptr();
        drop(first);

        let mut second = pool.acquire_read_window(8);
        assert_eq!(second.as_mut().as_ptr(), pointer);
        assert_eq!(second.as_mut(), b"abcdefgh");
    }

    #[test]
    fn bytes_owner_returns_buffer_after_its_last_clone_is_dropped() {
        let pool = BufferPool::new(8, 1);
        let mut window = pool.acquire_read_window(8);
        window.as_mut().copy_from_slice(b"abcdefgh");
        let bytes = window.into_bytes(8);
        let clone = bytes.clone();

        drop(bytes);
        assert!(pool.inner.lock().is_empty());

        drop(clone);
        assert_eq!(pool.inner.lock().len(), 1);
    }

    #[test]
    fn read_window_bytes_expose_only_the_written_length() {
        let pool = BufferPool::new(8, 1);
        let mut window = pool.acquire_read_window(8);
        window.as_mut().copy_from_slice(b"abcdefgh");

        let bytes = window.into_bytes(3);

        assert_eq!(bytes, Bytes::from_static(b"abc"));
    }

    #[test]
    fn dropping_read_window_returns_buffer_for_cancelled_or_failed_read() {
        let pool = BufferPool::new(8, 1);
        let window = pool.acquire_read_window(8);

        drop(window);

        assert_eq!(pool.inner.lock().len(), 1);
    }

    #[test]
    fn read_window_pool_caps_count_and_rejects_oversized_buffers() {
        let pool = BufferPool::new(8, 1);
        let first = pool.acquire_read_window(8);
        let second = pool.acquire_read_window(8);
        drop(first);
        drop(second);
        assert_eq!(pool.inner.lock().len(), 1);

        let oversized_pool = BufferPool::new(8, 1);
        let oversized = oversized_pool.acquire_read_window(9);
        drop(oversized);
        assert!(oversized_pool.inner.lock().is_empty());
    }

    #[test]
    #[ignore = "manual release microbenchmark; run with --release --ignored --exact"]
    fn read_window_storage_microbenchmark() {
        use std::{hint::black_box, time::Instant};

        const PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
        const ITERATIONS: usize = 256;
        const WARMUP_LEASES: usize = 2;

        let mut baseline_cache = Bytes::new();
        for iteration in 0..WARMUP_LEASES {
            let mut buffer = vec![0; PAYLOAD_BYTES];
            black_box(&buffer[..]);
            buffer.fill(iteration as u8);
            baseline_cache = Bytes::from(buffer);
            black_box(baseline_cache.as_ref());
        }
        let started = Instant::now();
        for iteration in 0..ITERATIONS {
            let mut buffer = vec![0; PAYLOAD_BYTES];
            black_box(&buffer[..]);
            buffer.fill(iteration as u8);
            baseline_cache = Bytes::from(buffer);
            black_box(baseline_cache.as_ref());
        }
        let baseline_duration_ns = started.elapsed().as_nanos();
        drop(baseline_cache);

        let pool = BufferPool::new(PAYLOAD_BYTES, 1);
        let mut pooled_cache = None;
        for iteration in 0..WARMUP_LEASES {
            let mut window = pool.acquire_read_window(PAYLOAD_BYTES);
            black_box(window.as_ref());
            window.as_mut().fill(iteration as u8);
            pooled_cache = Some(window.into_bytes(PAYLOAD_BYTES));
            black_box(pooled_cache.as_ref().unwrap().as_ref());
        }
        let started = Instant::now();
        for iteration in 0..ITERATIONS {
            let mut window = pool.acquire_read_window(PAYLOAD_BYTES);
            black_box(window.as_ref());
            window.as_mut().fill(iteration as u8);
            let previous = pooled_cache.replace(window.into_bytes(PAYLOAD_BYTES));
            drop(previous);
            black_box(pooled_cache.as_ref().unwrap().as_ref());
        }
        let pooled_duration_ns = started.elapsed().as_nanos();
        drop(pooled_cache);

        eprintln!(
            "SFS-780 read-window microbenchmark: iterations={ITERATIONS} payload_bytes={PAYLOAD_BYTES} baseline_duration_ns={baseline_duration_ns} baseline_logical_bytes_written={} pooled_duration_ns={pooled_duration_ns} pooled_logical_bytes_written={} warmup_leases={WARMUP_LEASES}",
            ITERATIONS * PAYLOAD_BYTES * 2,
            ITERATIONS * PAYLOAD_BYTES,
        );
    }
}
