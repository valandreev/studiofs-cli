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

    // Unlike write acquisition, read-window acquisition preserves initialized
    // bytes to avoid another full-buffer zero-fill. DataReader overwrites the
    // returned range, including holes, before it is exposed as a cached window.
    pub(super) fn acquire_read_window(&self, len: usize) -> ReadWindowBuffer {
        let mut buffer = self
            .inner
            .lock()
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(self.chunk_size));
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
        if written == 0 {
            return bytes::Bytes::new();
        }
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
        self.pool.release(std::mem::take(&mut self.buffer));
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
    fn zero_length_read_window_returns_buffer_to_pool_immediately() {
        let pool = BufferPool::new(8, 1);
        let window = pool.acquire_read_window(8);

        let bytes = window.into_bytes(0);

        assert!(bytes.is_empty());
        assert_eq!(pool.inner.lock().len(), 1);
    }

    #[test]
    fn dropping_read_window_returns_buffer_for_cancelled_or_failed_read() {
        let pool = BufferPool::new(8, 1);
        let window = pool.acquire_read_window(8);

        drop(window);

        assert_eq!(pool.inner.lock().len(), 1);
    }

    #[test]
    fn read_window_pool_caps_count() {
        let pool = BufferPool::new(8, 1);
        let first = pool.acquire_read_window(8);
        let second = pool.acquire_read_window(8);
        drop(first);
        drop(second);
        assert_eq!(pool.inner.lock().len(), 1);
    }
}
