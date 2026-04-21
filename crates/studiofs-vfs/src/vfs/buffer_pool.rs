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

    pub(super) fn release(&self, mut buffer: BytesMut) {
        if self.max_buffers == 0 {
            return;
        }
        buffer.clear();
        let mut buffers = self.inner.lock();
        if buffers.len() < self.max_buffers {
            buffers.push(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BufferPool;

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
}
