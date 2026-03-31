//! ArcSwap-based mmap reader with lock-free remap on file growth.

use std::fs::File;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use bytes::Bytes;
use memmap2::{Mmap, MmapOptions};
use parking_lot::Mutex;

use crate::v2::platform;

struct MmapGeneration {
    #[cfg(test)]
    id: u64,
    mapped_len: u64,
    mmap: Mmap,
}

pub struct MmapReader {
    file: File,
    current: ArcSwap<MmapGeneration>,
    remap_lock: Mutex<()>,
    next_generation: AtomicU64,
}

impl MmapReader {
    pub fn new(file: &File, mapped_len: u64) -> io::Result<Self> {
        let file = file.try_clone()?;
        let initial = Arc::new(Self::map_generation(&file, mapped_len, 0)?);
        Ok(Self {
            file,
            current: ArcSwap::from(initial),
            remap_lock: Mutex::new(()),
            next_generation: AtomicU64::new(1),
        })
    }

    pub fn read(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        let len_u64 = u64::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "requested mmap read exceeds u64::MAX bytes",
            )
        })?;
        let end = offset.checked_add(len_u64).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "requested mmap read overflows file offset space",
            )
        })?;

        let generation = self.current.load_full();
        if end <= generation.mapped_len {
            let start = usize::try_from(offset).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "requested mmap read offset exceeds addressable space",
                )
            })?;
            let end = usize::try_from(end).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "requested mmap read end exceeds addressable space",
                )
            })?;
            // Cache v2 intentionally stays at one-copy reads for this phase.
            // A future zero-copy path can bind Bytes to the generation Arc if
            // profiling shows the copy is material on large blocks.
            return Ok(Bytes::copy_from_slice(&generation.mmap[start..end]));
        }

        let mut buf = vec![0u8; len];
        platform::pread(&self.file, &mut buf, offset)?;
        let _ = self.remap_if_needed(end);
        Ok(Bytes::from(buf))
    }

    pub fn remap_if_needed(&self, required_len: u64) -> io::Result<()> {
        if required_len <= self.current.load_full().mapped_len {
            return Ok(());
        }

        let _guard = self.remap_lock.lock();
        if required_len <= self.current.load_full().mapped_len {
            return Ok(());
        }

        let file_len = self.file.metadata()?.len();
        if file_len == 0 || file_len < required_len {
            return Ok(());
        }

        let generation_id = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let generation = Arc::new(Self::map_generation(&self.file, file_len, generation_id)?);
        self.current.store(generation);
        Ok(())
    }

    #[cfg(test)]
    fn remap_to_len(&self, mapped_len: u64) -> io::Result<()> {
        let _guard = self.remap_lock.lock();
        let generation_id = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let generation = Arc::new(Self::map_generation(&self.file, mapped_len, generation_id)?);
        self.current.store(generation);
        Ok(())
    }

    fn map_generation(file: &File, mapped_len: u64, _id: u64) -> io::Result<MmapGeneration> {
        let mapped_len_usize = usize::try_from(mapped_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "requested mmap length exceeds addressable space",
            )
        })?;
        let mmap = unsafe { MmapOptions::new().len(mapped_len_usize).map(file)? };
        // The Cache v2 design currently prefers a sequential hint on the whole
        // mapping, matching the original spec for OS page-cache driven reads.
        // If benchmarks show this is counterproductive for random-heavy access
        // patterns, revisit this advisory choice rather than changing reader
        // semantics.
        let _ = platform::advise_sequential(mmap.as_ref());
        Ok(MmapGeneration {
            #[cfg(test)]
            id: _id,
            mapped_len,
            mmap,
        })
    }

    #[cfg(test)]
    pub(crate) fn force_mapped_len_for_test(&self, mapped_len: u64) -> io::Result<()> {
        self.remap_to_len(mapped_len.min(self.file.metadata()?.len()))
    }

    #[cfg(test)]
    pub(crate) fn generation_for_test(&self) -> u64 {
        self.current.load_full().id
    }

    #[cfg(test)]
    pub(crate) fn mapped_len_for_test(&self) -> u64 {
        self.current.load_full().mapped_len
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use tempfile::TempDir;

    use super::*;
    use crate::v2::format::PAGE_SIZE;
    use crate::v2::platform;

    fn test_file() -> (TempDir, File) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("blocks.data");
        let file = File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        (dir, file)
    }

    #[test]
    fn fallback_reads_newly_grown_region_and_remaps() {
        let (_dir, file) = test_file();
        file.set_len(3 * PAGE_SIZE).unwrap();

        let payload = b"freshly-grown-payload";
        let offset = 2 * PAGE_SIZE + 128;
        platform::pwrite(&file, payload, offset).unwrap();

        let reader = MmapReader::new(&file, PAGE_SIZE).unwrap();
        assert_eq!(reader.mapped_len_for_test(), PAGE_SIZE);

        let got = reader.read(offset, payload.len()).unwrap();
        assert_eq!(&got[..], payload);
        assert_eq!(reader.mapped_len_for_test(), 3 * PAGE_SIZE);
        assert!(reader.generation_for_test() >= 1);
    }

    #[test]
    fn concurrent_readers_survive_growth_remaps() {
        let (_dir, file) = test_file();
        file.set_len(PAGE_SIZE).unwrap();

        let base_payload = vec![0xAB; 512];
        let base_offset = 64;
        platform::pwrite(&file, &base_payload, base_offset).unwrap();

        let reader = Arc::new(MmapReader::new(&file, PAGE_SIZE).unwrap());
        let mut handles = Vec::new();
        for _ in 0..4 {
            let reader = Arc::clone(&reader);
            let expected = base_payload.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..200 {
                    let got = reader.read(base_offset, expected.len()).unwrap();
                    assert_eq!(&got[..], &expected[..]);
                }
            }));
        }

        for page in 2..=16 {
            let page = page as u64;
            file.set_len(page * PAGE_SIZE).unwrap();
            let marker = [page as u8; 64];
            platform::pwrite(&file, &marker, (page - 1) * PAGE_SIZE + 32).unwrap();
            reader.remap_if_needed(page * PAGE_SIZE).unwrap();
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
