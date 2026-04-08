# Parallel Raw Block File Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the `RawBlockFileBackend` to use `Arc<File>`, an `RwLock` for state, and an `AtomicU64` for space reservation to enable lock-free parallel I/O and remove redundant syncs.

**Architecture:** We will replace the `Mutex<File>` and `Mutex<RawBlockFileState>` in `RawBlockFileBackend` with `Arc<File>`, `RwLock<RawBlockFileState>`, and `AtomicU64` for tracking `reserved_len`. `put` will reserve space atomically, perform parallel `pwrite`, then acquire the write lock to update the index. Reads will acquire a read lock, locate the block, drop the lock, and perform parallel `pread`. `fdatasync` will be removed from `append_record` and `put`, relying instead on the batch `sync_data`.

**Tech Stack:** Rust, `std::sync::{Arc, RwLock, atomic::AtomicU64}`, `tokio` (for testing context).

> **Audit update (2026-04-08):** The refactor shipped in `crates/studiofs-cache/src/v2/raw_block_file_backend.rs` (see commits `013e0dd` and `2110d07`). Tasks 1, 2, the core of Task 3, the corrected design in Task 4 Step 2, and Task 5 are present in the code. Two plan items did not ship exactly as originally written:
> - The preallocation sketch in Task 3 Step 1 was removed in `2110d07` after a concurrent append race was found, so the final write path uses atomic reservation + direct `pwrite` without `platform::preallocate`.
> - Task 4 Step 1 is a superseded first draft of compaction. The code implements the corrected lock/file-handle strategy from Task 4 Step 2 instead.

---

### Task 1: Update State Structures and Locks

**Files:**
- Modify: `crates/studiofs-cache/src/v2/raw_block_file_backend.rs`

- [x] **Step 1: Write failing compilation test (Mental check)**
The goal is to change the core struct definitions. If we change them, existing code using `file.lock()` or `state.lock()` will fail to compile. This is a structural refactoring step.

- [x] **Step 2: Update struct definitions**
Change `RawBlockFileBackend` fields to use `Arc`, `RwLock`, and `AtomicU64`.

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Debug)]
pub struct RawBlockFileBackend {
    path: PathBuf,
    volume_uuid: [u8; 16],
    file: Arc<File>,
    reserved_len: AtomicU64,
    state: RwLock<RawBlockFileState>,
}
```

- [x] **Step 3: Update `open` method**
Initialize the new fields in `open`.

```rust
        if file_len == 0 {
            write_file_header(&file, config.volume_uuid)?;
            platform::fdatasync(&file)?;
            let state = RawBlockFileState::new(FILE_HEADER_SIZE as u64);
            return Ok(Self {
                path,
                volume_uuid: config.volume_uuid,
                file: Arc::new(file),
                reserved_len: AtomicU64::new(FILE_HEADER_SIZE as u64),
                state: RwLock::new(state),
            });
        }

        let volume_uuid = read_file_header(&file, file_len)?;
        if volume_uuid != config.volume_uuid {
            return Err(RawBlockFileBackendError::VolumeUuidMismatch);
        }

        let state = load_state(&file, file_len)?;
        let reserved_len = AtomicU64::new(state.file_len);
        Ok(Self {
            path,
            volume_uuid: config.volume_uuid,
            file: Arc::new(file),
            reserved_len,
            state: RwLock::new(state),
        })
```

- [x] **Step 4: Fix compilation errors for read-only methods**
Update methods like `contains`, `lifecycle_state`, `is_evictable`, and `dirty_object_ids` to use `state.read()`.

```rust
    fn contains(&self, object_id: u64) -> bool {
        self.state.read().entries.contains_key(&object_id)
    }

    fn dirty_object_ids(&self) -> Vec<u64> {
        let state = self.state.read();
        let mut dirty_ids = Vec::new();
        for object_id in &state.dirty_order {
            if matches!(
                state.entries.get(object_id).map(|entry| entry.lifecycle),
                Some(LocalBlockLifecycleState::Dirty | LocalBlockLifecycleState::Uploading)
            ) {
                dirty_ids.push(*object_id);
            }
        }
        dirty_ids
    }

    fn lifecycle_state(&self, object_id: u64) -> LocalBlockLifecycleState {
        self.state
            .read()
            .entries
            .get(&object_id)
            .map(|entry| entry.lifecycle)
            .unwrap_or(LocalBlockLifecycleState::Missing)
    }

    fn is_evictable(&self, object_id: u64) -> bool {
        self.state
            .read()
            .entries
            .get(&object_id)
            .is_some_and(|entry| {
                entry.lifecycle == LocalBlockLifecycleState::Uploaded && entry.evictable
            })
    }
```

- [x] **Step 5: Verify compilation (partial)**
Run `cargo check -p studiofs-cache` to ensure the struct and read-only methods compile (errors in `put`, `get`, etc., are expected and will be fixed in subsequent tasks).

- [x] **Step 6: Commit**
```bash
git add crates/studiofs-cache/src/v2/raw_block_file_backend.rs
git commit -m "refactor(cache): change RawBlockFileBackend locks to RwLock and Arc"
```

---

### Task 2: Refactor Read Path (Lock-Free I/O)

**Files:**
- Modify: `crates/studiofs-cache/src/v2/raw_block_file_backend.rs`

- [x] **Step 1: Update `get` and `read_payload`**
Take a read lock to get the entry, then drop it and read from the `Arc<File>`.

```rust
    fn read_payload(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
        let entry = {
            let state = self.state.read();
            *state.entries.get(&object_id)?
        };
        let start = u64::try_from(data_offset).ok()?;
        if start >= entry.data_len {
            return Some(0);
        }
        let available = entry.data_len - start;
        let to_read = available.min(out.len() as u64) as usize;
        if to_read == 0 {
            return Some(0);
        }
        let offset = entry.data_offset + start;
        if let Err(error) = platform::pread(&self.file, &mut out[..to_read], offset) {
            warn!(
                object_id,
                data_offset,
                error = ?error,
                "raw backend read failed"
            );
            return None;
        }
        Some(to_read)
    }

    fn get(&self, object_id: u64) -> Option<Bytes> {
        let entry = {
            let state = self.state.read();
            *state.entries.get(&object_id)?
        };
        let len = usize::try_from(entry.data_len).ok()?;
        let mut buf = vec![0_u8; len];
        if len > 0 {
            if let Err(error) = platform::pread(&self.file, &mut buf, entry.data_offset) {
                warn!(object_id, error = ?error, "raw backend get failed");
                return None;
            }
        }
        Some(Bytes::from(buf))
    }
```

- [x] **Step 2: Run `cargo check`**
Verify the read path compiles.

- [x] **Step 3: Commit**
```bash
git add crates/studiofs-cache/src/v2/raw_block_file_backend.rs
git commit -m "refactor(cache): make raw backend read path lock-free"
```

---

### Task 3: Refactor Write Path (Parallel Allocation)

**Files:**
- Modify: `crates/studiofs-cache/src/v2/raw_block_file_backend.rs`

- [x] **Step 1: Rewrite `put` for parallel coordination**
Reserve space using `AtomicU64`, perform the write without locks, then take the write lock to update the index.

Final code note: the `platform::preallocate` sketch shown below was intentionally removed after it raced under concurrent append; the shipped path still reserves offsets atomically and writes in parallel.

```rust
    fn put(
        &self,
        object_id: u64,
        data: &[u8],
        evictable: bool,
    ) -> Result<(), LocalBlockBackendError> {
        let data_len = u64::try_from(data.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "block too large for raw backend format",
            )
        })?;
        
        let lifecycle = if evictable {
            LocalBlockLifecycleState::Uploaded
        } else {
            LocalBlockLifecycleState::Dirty
        };
        
        let record_len = RECORD_HEADER_SIZE as u64 + data_len;
        let offset = self.reserved_len.fetch_add(record_len, Ordering::SeqCst);
        let data_offset = offset + RECORD_HEADER_SIZE as u64;

        // Perform parallel preallocation in large chunks if needed (e.g., 256MB bounds).
        // Since many threads might race, best-effort preallocate using the new reserved end.
        let end_offset = offset + record_len;
        // Basic preallocation strategy (platform handles ignoring if already sized)
        let _ = platform::preallocate(&self.file, 0, end_offset);
        
        let header =
            encode_record_header(RECORD_KIND_PUT, lifecycle, evictable, object_id, data_len);
        
        // Write header and data (could be combined into one pwrite buffer, but doing two is fine if safe)
        platform::pwrite(&self.file, &header, offset)?;
        if !data.is_empty() {
            platform::pwrite(&self.file, data, data_offset)?;
        }

        let entry = RawBlockEntry {
            data_offset,
            data_len,
            lifecycle,
            evictable,
        };
        
        let mut state = self.state.write();
        state.entries.insert(object_id, entry);
        match lifecycle {
            LocalBlockLifecycleState::Dirty => state.enter_dirty(object_id),
            LocalBlockLifecycleState::Uploading => state.enter_uploading(object_id),
            LocalBlockLifecycleState::Uploaded => state.enter_uploaded(object_id),
            LocalBlockLifecycleState::Missing => {}
        }
        // Advance state file length only if we pushed the high water mark
        if end_offset > state.file_len {
             state.file_len = end_offset;
        }
        Ok(())
    }
```

- [x] **Step 2: Update `append_record` and state mutators**
Update `mark_uploaded`, `remove`, `remove_batch`, etc. to use `state.write()` and write directly via `Arc<File>`. `append_record` should reserve space, write, and not call `fdatasync`.

```rust
    fn append_record(
        &self,
        header: [u8; RECORD_HEADER_SIZE],
    ) -> Result<u64, LocalBlockBackendError> {
        let offset = self.reserved_len.fetch_add(RECORD_HEADER_SIZE as u64, Ordering::SeqCst);
        platform::pwrite(&self.file, &header, offset)?;
        Ok(offset)
    }

    fn mark_uploading(&self, object_id: u64) {
        let mut state = self.state.write();
        let should_mark = state
            .entries
            .get(&object_id)
            .is_some_and(|entry| entry.lifecycle == LocalBlockLifecycleState::Dirty);
        if should_mark {
            state.enter_uploading(object_id);
        }
    }

    fn mark_upload_failed(&self, object_id: u64) {
        let mut state = self.state.write();
        let should_requeue = {
            let Some(entry) = state.entries.get_mut(&object_id) else {
                return;
            };
            if entry.lifecycle == LocalBlockLifecycleState::Uploading {
                entry.lifecycle = LocalBlockLifecycleState::Dirty;
                entry.evictable = false;
                true
            } else {
                false
            }
        };
        if should_requeue {
            state.enter_dirty(object_id);
        }
    }

    fn mark_uploaded(&self, object_id: u64) {
        let header = encode_record_header(
            RECORD_KIND_STATE,
            LocalBlockLifecycleState::Uploaded,
            true,
            object_id,
            0,
        );
        let offset_res = self.append_record(header);

        let mut state = self.state.write();
        if !state.entries.contains_key(&object_id) {
            return;
        }
        if let Err(error) = offset_res {
            warn!(object_id, error = ?error, "raw backend mark_uploaded write failed");
            return;
        }
        let offset = offset_res.unwrap();
        let end_offset = offset + RECORD_HEADER_SIZE as u64;
        if end_offset > state.file_len {
            state.file_len = end_offset;
        }
        state.enter_uploaded(object_id);
    }

    fn remove(&self, object_id: u64) {
        let header = encode_record_header(
            RECORD_KIND_REMOVE,
            LocalBlockLifecycleState::Missing,
            false,
            object_id,
            0,
        );
        let offset_res = self.append_record(header);

        let mut state = self.state.write();
        if !state.entries.contains_key(&object_id) {
            return;
        }
        if let Err(error) = offset_res {
            warn!(object_id, error = ?error, "raw backend remove write failed");
            return;
        }
        let offset = offset_res.unwrap();
        let end_offset = offset + RECORD_HEADER_SIZE as u64;
        if end_offset > state.file_len {
            state.file_len = end_offset;
        }
        state.remove_entry(object_id);
    }

    fn remove_batch(&self, object_ids: &[u64]) {
        for object_id in object_ids {
            self.remove(*object_id);
        }
    }
```

- [x] **Step 3: Update `sync_data`**
```rust
    fn sync_data(&self) -> Result<(), LocalBlockBackendError> {
        platform::fdatasync(&self.file)?;
        Ok(())
    }
```

- [x] **Step 4: Fix `Drop` implementation**
```rust
impl Drop for RawBlockFileBackend {
    fn drop(&mut self) {
        if let Err(error) = platform::fdatasync(&self.file) {
            warn!(error = ?error, "raw backend drop: fdatasync failed");
        }
    }
}
```

- [x] **Step 5: Ensure compilation**
Run `cargo test -p studiofs-cache` to verify contract tests pass.

- [x] **Step 6: Commit**
```bash
git add crates/studiofs-cache/src/v2/raw_block_file_backend.rs
git commit -m "feat(cache): parallelize raw backend put operations"
```

---

### Task 4: Fix Compaction Logic

**Files:**
- Modify: `crates/studiofs-cache/src/v2/raw_block_file_backend.rs`

- [ ] **Step 1: Update `compact_locked` and `run_pressure_maintenance`**
Compaction needs exclusive access. Since it writes a new file and swaps it, we need to block other operations during the swap.

Audit note: this exact draft was not shipped. It exposed the file-handle swap problem and was replaced by the corrected design in Step 2.

```rust
    fn run_pressure_maintenance(&self) -> Result<(), LocalBlockBackendError> {
        let should_compact = {
             let state = self.state.read();
             state.should_compact()
        };
        if should_compact {
            // Need write lock to perform compaction to prevent concurrent writes
            // during the file swap.
            let mut state = self.state.write();
            // Re-check inside write lock
            if state.should_compact() {
                self.compact_locked(&mut state)?;
            } else {
                self.prune_dirty_order(&mut state);
            }
        } else {
            let mut state = self.state.write();
            self.prune_dirty_order(&mut state);
        }
        Ok(())
    }
```
Update `compact_locked` to use `&self.file` instead of `self.file.lock()`. Update the state update to ensure `reserved_len` is synchronized.

```rust
    fn compact_locked(&self, state: &mut RawBlockFileState) -> Result<(), LocalBlockBackendError> {
        self.prune_dirty_order(state);

        let mut ordered_object_ids = Vec::with_capacity(state.entries.len());
        let mut seen = HashSet::new();
        for object_id in &state.dirty_order {
            if state.entries.contains_key(object_id) && seen.insert(*object_id) {
                ordered_object_ids.push(*object_id);
            }
        }

        let mut remaining: Vec<u64> = state
            .entries
            .keys()
            .copied()
            .filter(|object_id| seen.insert(*object_id))
            .collect();
        remaining.sort_unstable();
        ordered_object_ids.extend(remaining);

        let temp_path = self.compact_path();
        let temp_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;
        write_file_header(&temp_file, self.volume_uuid)
            .map_err(|error| LocalBlockBackendError::Other(Box::new(error)))?;

        let mut new_state = RawBlockFileState::new(FILE_HEADER_SIZE as u64);
        for object_id in ordered_object_ids {
            let entry = *state.entries.get(&object_id).ok_or_else(|| {
                LocalBlockBackendError::Io(io::Error::new(
                    io::ErrorKind::NotFound,
                    "entry disappeared during raw backend compaction",
                ))
            })?;
            let payload_len = usize::try_from(entry.data_len).map_err(|_| {
                LocalBlockBackendError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raw backend entry too large to compact on this platform",
                ))
            })?;
            let mut payload = vec![0_u8; payload_len];
            if !payload.is_empty() {
                platform::pread(&self.file, &mut payload, entry.data_offset)?;
            }

            let offset = new_state.file_len;
            let data_offset = offset + RECORD_HEADER_SIZE as u64;
            let header = encode_record_header(
                RECORD_KIND_PUT,
                entry.lifecycle,
                entry.evictable,
                object_id,
                entry.data_len,
            );
            if !payload.is_empty() {
                platform::pwrite(&temp_file, &payload, data_offset)?;
            }
            platform::pwrite(&temp_file, &header, offset)?;
            new_state.file_len = data_offset + entry.data_len;
            new_state.entries.insert(
                object_id,
                RawBlockEntry {
                    data_offset,
                    data_len: entry.data_len,
                    lifecycle: entry.lifecycle,
                    evictable: entry.evictable,
                },
            );
            if matches!(
                entry.lifecycle,
                LocalBlockLifecycleState::Dirty | LocalBlockLifecycleState::Uploading
            ) {
                new_state.dirty_order.push_back(object_id);
            }
        }

        // Must sync new file before swapping
        platform::fdatasync(&temp_file)?;

        // Atomic swap of the file handle
        // Wait, `self.file` is an Arc. We cannot mutate it behind an immutable reference `&self`.
        // This is a bug in my earlier design!
        // To fix this, `file` must be protected, or we need to recreate the Backend.
        // Let's use an RwLock for the file handle as well, or store it inside the state.
        // Putting the `Arc<File>` inside the `RawBlockFileState` is the safest way to ensure
        // readers use the correct file handle without acquiring multiple locks.
```

- [x] **Step 2: Rethink Lock Strategy (Correction)**
Let's modify `RawBlockFileState` to hold the `Arc<File>` and `reserved_len` so they are swapped together during compaction.

Modify `RawBlockFileBackend` definition:
```rust
#[derive(Debug)]
struct RawBlockFileState {
    file: Arc<File>,
    reserved_len: Arc<AtomicU64>,
    file_len: u64,
    entries: HashMap<u64, RawBlockEntry>,
    dirty_order: VecDeque<u64>,
}

#[derive(Debug)]
pub struct RawBlockFileBackend {
    path: PathBuf,
    volume_uuid: [u8; 16],
    state: RwLock<RawBlockFileState>,
}
```

Update `open`:
```rust
        let state = RawBlockFileState {
            file: Arc::new(file),
            reserved_len: Arc::new(AtomicU64::new(file_len)),
            file_len,
            entries: HashMap::new(), // Populate via load_state
            dirty_order: VecDeque::new(), // Populate via load_state
        };
        // (Modify load_state to take and return entries/dirty_order instead of full state)
```

Update `put` and `read`:
```rust
    fn get(&self, object_id: u64) -> Option<Bytes> {
        let (entry, file) = {
            let state = self.state.read();
            (*state.entries.get(&object_id)?, state.file.clone())
        };
        // use `file`
    }
    
    fn put(...) {
        let (file, reserved_len) = {
             let state = self.state.read();
             (state.file.clone(), state.reserved_len.clone())
        };
        // reserve and write using `file` and `reserved_len`
        // then acquire state.write() to update entries.
    }
```

- [x] **Step 3: Apply the corrected state struct and run tests**
Refactor the file to match this corrected architecture, ensuring all tests pass (`cargo test -p studiofs-cache`).

- [x] **Step 4: Commit**
```bash
git add crates/studiofs-cache/src/v2/raw_block_file_backend.rs
git commit -m "fix(cache): place file handle in RwLock state for safe compaction swap"
```

---

### Task 5: Add Concurrency Test

**Files:**
- Modify: `crates/studiofs-cache/src/v2/raw_block_file_backend.rs`

- [x] **Step 1: Write concurrent test**
Add a test in the `tests` module to verify parallel puts.

```rust
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_puts() {
        use std::sync::Arc;
        let dir = TempDir::new().unwrap();
        let config = test_config(&dir);
        let backend = Arc::new(RawBlockFileBackend::open(config).unwrap());
        
        let mut handles = vec![];
        for i in 0..100 {
            let b = backend.clone();
            handles.push(tokio::spawn(async move {
                let data = format!("data-{}", i).into_bytes();
                b.put(i as u64, &data, false).unwrap();
            }));
        }
        
        for h in handles {
            h.await.unwrap();
        }
        
        backend.sync_data().unwrap();
        
        for i in 0..100 {
            let data = backend.get(i as u64).unwrap();
            assert_eq!(data.as_ref(), format!("data-{}", i).as_bytes());
        }
    }
```

- [x] **Step 2: Run the test**
Run `cargo test -p studiofs-cache concurrent_puts`

- [x] **Step 3: Commit**
```bash
git add crates/studiofs-cache/src/v2/raw_block_file_backend.rs
git commit -m "test(cache): add concurrency test for raw backend"
```
