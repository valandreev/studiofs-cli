# Throughput Optimization Roadmap — 300→1500 MB/s

**Дата:** 2026-02-19  
**Текущее состояние:** DiskCache hit throughput 150–300 MB/s  
**Целевое состояние:** ≥1.5 GB/s  
**Критические узкие места:** 3× memcpy на блок + 4 syscall per block + spawn_blocking overhead

---

## Executive Summary

Система теряет пропускную способность на трёх уровнях:

1. **WinFSP layer:** блоковые read запросы по 64KB → 256 системных вызовов на каждый GB
2. **VFS read path:** 3 полных memcpy (4MB each) при disk cache hit: mmap → assembly → WinFSP buffer
3. **DiskCache реализация:** 4 syscall-а (open + mmap + unmap + close) за счёт отдельного файла на блок

**Решение:** Slab-based persistent mmap cache с zero-copy Bytes + prefetch + direct buffer fill.

**Ожидаемый результат:** 4-5x рост throughput при одновременной релизации пунктов 1-4.

---

## Фаза 1: Быстрые тривиальные изменения (день 1)

### 1.1 WinFSP max_transfer_size tuning
**Файл:** [crates/studiofs-winfsp/src/lib.rs](../../crates/studiofs-winfsp/src/lib.rs#L20-L40)

**Проблема:**  
WinFSP по умолчанию отправляет read запросы по 64 KB. Для большого файла это требует ~16K системных вызовов на каждый GB.

**Решение:**  
Увеличить `max_transfer_size` до 4 MB, чтобы WinFSP группировал меньше read операций.

**diff:**
```rust
volume_params.max_transfer_size(4 * 1024 * 1024); // 4MB chunks
```

**Выигрыш:** -15-20% syscall overhead на read path (WinFSP level)  
**Complexity:** Trivial — одна строка  
**Time:** 5 минут

---

### 1.2 Убрать file_info_timeout bloat
**Файл:** [crates/studiofs-winfsp/src/lib.rs](../../crates/studiofs-winfsp/src/lib.rs#L35-L36)

**Проблема:**  
`file_info_timeout(1000)` заставляет WinFSP кэшировать атрибуты на 1 сек, что хорошо для metadata, но может привести к stale file size при быстрых overwrites.

**Решение:**  
Снизить до 100ms для файлов, которые часто модифицируются (или убрать для read-only workload).

```rust
volume_params.file_info_timeout(100);   // More aggressive invalidation
volume_params.dir_info_timeout(1000);   // Keep dir cache longer
```

**Выигрыш:** Корректность vs performance tradeoff (опционально)  
**Complexity:** Trivial  
**Time:** 5 минут

---

### 1.3 Direct buffer fill API (read_into)
**Файлы:**  
- [crates/studiofs-vfs/src/vfs.rs](../../crates/studiofs-vfs/src/vfs.rs#L496) (добавить метод)
- [crates/studiofs-winfsp/src/context.rs](../../crates/studiofs-winfsp/src/context.rs#L280) (использовать)

**Проблема:**  
`vfs.read()` возвращает `Bytes`, который потом копируется в WinFSP buffer — одна лишняя memcpy на каждый read.

**Решение:**  
Добавить `read_into(&mut buffer)` метод, который пишет данные прямо в output buffer:

```rust
// В VFS:
pub async fn read_into(&self, ino: Ino, offset: u64, buffer: &mut [u8]) -> VfsResult<u32> {
    // Read blocks and copy directly to buffer, avoiding intermediate Bytes
    // Same logic as read(), but write_slice to buffer instead of BytesMut accumulator
    // ...
}

// В WinFSP context.rs read():
let written = self.rt.block_on(
    self.vfs.read_into(context.ino, offset, buffer)
)?;
```

**Выигрыш:** -1 memcpy per read (~200 MB/s at 4MB blocks)  
**Complexity:** Low — переделать цикл assembly  
**Time:** 1-2 часа

---

## Фаза 2: Medium-complexity improvements (дни 2-3)

### 2.1 Persistent mmap DiskCache (Slab-based)
**Файлы:**  
- [crates/studiofs-cache/src/disk.rs](../../crates/studiofs-cache/src/disk.rs) (полная переделка)
- [crates/studiofs-cache/src/slab.rs](../../crates/studiofs-cache/src/slab.rs) (новый файл)

**Проблема:**  
Каждый 4MB блок = отдельный файл на диске. Чтение = `open()` + `mmap()` + `copy()` + `close()` (4 syscall). NTFS при этом делает MFT lookup на каждый файл. Стратегия не масштабируется.

**Решение:**  
Хранить блоки в больших **слабе** (2-4 GB файлах), которые остаются **постоянно mmap'd** за время жизни программы.

**Структура:**
```
.cache/{uuid}/
  slabs/
    slab_0000.dat  (2 GB, постоянно mmap'd, +300MB приватно)
    slab_0001.dat  (2 GB, ...)
    slab_XXXX.dat
  index.bin        (postcard: Vec<(object_id, slab_id, offset, length)>)
  lru_tree.bin     (для eviction: (object_id, last_access_time))
```

**New DiskCache API:**
```rust
pub struct SlabDiskCache {
    slabs: Vec<Arc<Slab>>,           // Persistent mmap'd files
    index: DashMap<ObjectId, SlabEntry>,  // object_id → slab metadata
    lru: RwLock<BTreeMap<(Instant, ObjectId)>>,
    total_size: AtomicU64,
    config: DiskCacheConfig,
}

struct Slab {
    mmap: Mmap,                      // Permanently mmap'd
    free_list: Mutex<Vec<(u32, u32)>>,  // (offset, length) of free regions
    path: PathBuf,
    size: u64,
}

pub struct SlabEntry {
    slab_id: usize,
    offset: u32,
    length: u32,
    last_access: Instant,
}

impl SlabDiskCache {
    pub fn get_object(&self, id: ObjectId) -> Option<Bytes> {
        let entry = self.index.get(&id)?;
        let slab = &self.slabs[entry.slab_id];
        let slice = &slab.mmap[entry.offset as usize..][..entry.length as usize];
        
        // Update LRU
        self.lru.write().insert((Instant::now(), id), entry.slab_id);
        
        Some(Bytes::copy_from_slice(slice))  // 1 copy (vs 3)
    }
    
    pub fn put_object(&self, id: ObjectId, data: &[u8]) -> DiskCacheResult<()> {
        // Find free space in slabs (best-fit or first-fit)
        // If no space: evict oldest blocks from LRU
        // Copy to slab mmap
        // Update index
        // No spawn_blocking, no syscalls
    }
}
```

**Миграция существующего кэша:**
```bash
# Скрипт для конвертации старого кэша в новый формат
# 1. Инициализировать slab файлы
# 2. Скопировать блоки из .cache/objects/* в slabs/
# 3. Пересоздать index.bin из имён файлов
# 4. Удалить старые файлы
```

**Выигрыш:**  
- **0 syscall per cache hit** (vs 4+)
- **1 memcpy per cache hit** (vs 3)
- Лучше работает с NTFS (меньше MFT lookup)
- Можно убрать `spawn_blocking` из read path

**Complexity:** High — полная переделка DiskCache  
**Time:** 2-3 дня

**Тестирование:**
```rust
#[test]
fn test_slab_allocation_fragmentation() { ... }

#[test]
fn test_slab_eviction_lru() { ... }

#[test]
fn test_slab_mmap_safety() { ... }
```

---

### 2.2 Zero-copy Bytes из mmap (Bytes::from_owner)
**Файлы:**  
- [crates/studiofs-cache/src/disk.rs](../../crates/studiofs-cache/src/disk.rs) (использовать `from_owner`)
- [crates/Cargo.toml](../../Cargo.toml) (проверить версию bytes)

**Проблема:**  
Даже с slab base, `Bytes::copy_from_slice(&slab.mmap[..])` — это полная копия 4MB в памяти. Для sequential read это огромные расходы.

**Решение:**  
Использовать `Bytes::from_owner()` (доступна в `bytes >= 1.9`):

```rust
// Вместо:
let bytes = Bytes::copy_from_slice(&slab.mmap[offset..][..len]);

// Использовать:
struct SlicableArc {
    owner: Arc<Slab>,
    offset: usize,
    length: usize,
}

impl AsRef<[u8]> for SlicableArc {
    fn as_ref(&self) -> &[u8] {
        &self.owner.mmap[self.offset..][..self.length]
    }
}

let bytes = Bytes::from_owner(SlicableArc {
    owner: slab.clone(),
    offset, length,
});
```

**Выигрыш:** **-1 memcpy per read** (~300-400 MB/s при 4MB blocks)  
**Complexity:** Low — обёртка вокруг Arc  
**Time:** 2-4 часа

**Альтернатива:** Хранить `Arc<Mmap>` в кэше и выдавать slice-ы (проще, но требует более сложного управления lifetime).

---

### 2.3 Remove spawn_blocking from DiskCache hot path
**Файл:** [crates/studiofs-cache/src/disk.rs](../../crates/studiofs-cache/src/disk.rs#L460-L490)

**Проблема:**  
С persistent slab cache, чтение из диска — это просто доступ к памяти (уже mmap'd). Нет необходимости в `spawn_blocking`, который добавляет overhead на thread switch.

**Решение:**  
Сделать `get_object()` синхронным (не async):

```rust
// Было:
pub async fn get_object(&self, id: ObjectId) -> DiskCacheResult<Option<Bytes>> {
    let data = tokio::task::spawn_blocking(move || {
        // file I/O
    }).await??;
}

// Стало:
pub fn get_object(&self, id: ObjectId) -> Option<Bytes> {
    // Direct mmap access — no spawn_blocking needed
    let entry = self.index.get(&id)?;
    let slab = &self.slabs[entry.slab_id];
    Some(Bytes::copy_from_slice(&slab.mmap[entry.offset..][..entry.length]))
}

// Adaptation in VFS read path:
// Can now call synchronously or wrap in tokio::task::spawn_blocking if needed
```

**Выигрыш:** -0.1-0.3ms per block (thread switch overhead)  
**Complexity:** Low — уже реализовано с slab base  
**Time:** 0.5 часа

---

## Фаза 3: Advanced optimizations (дни 4-5)

### 3.1 ReadAhead / Prefetch for sequential access
**Файлы:**  
- [crates/studiofs-vfs/src/vfs.rs](../../crates/studiofs-vfs/src/vfs.rs#L496) (добавить prefetch logic)
- [crates/studiofs-vfs/src/readahead.rs](../../crates/studiofs-vfs/src/readahead.rs) (новый модуль)

**Проблема:**  
Memory cache — LRU общего назначения. Для sequential playback (VFX animation frames) нужна специализированная стратегия: загружать блоки **заранее** перед их запросом.

**Решение:**  
Отслеживать sequential pattern чтения:

```rust
struct ReadAheadTracker {
    // Для каждого (inode, shard):  последний прочитанный блок
    last_reads: DashMap<(Ino, u32), u32>,  // shard → last_block_index
}

// В read_block() или read():
if let Some(last_block) = tracker.get(&(ino, shard_id)) {
    if block_index == last_block + 1 {
        // Sequential pattern detected! Schedule prefetch.
        for i in 1..=PREFETCH_WIDTH {
            let next_block = block_index + i;
            if !mem_cache.contains(ino, next_block) {
                tokio::spawn(async move {
                    let _ = vfs.read_block_prefetch(ino, next_block).await;
                });
            }
        }
    }
}
tracker.insert((ino, shard_id), block_index);
```

**Выделенный prefetch buffer:**
```rust
pub struct VfsConfig {
    ...
    /// Dedicated memory for prefetch (not competing with main LRU)
    pub prefetch_cache_size: usize,  // 2-4 GB
}

struct VfsState {
    ...
    prefetch_cache: MemoryCache,  // Separate from main mem_cache
}
```

**Выигрыш:** **2-3x for sequential reads** (при 8-block lookahead)  
**Complexity:** Medium — async scheduling, separate cache mgmt  
**Time:** 2-3 дня (включая tuning)

**Тестирование:**
```rust
#[tokio::test]
async fn test_sequential_readahead_prefetch() { ... }
```

---

### 3.2 Увеличить block size (16MB или 64MB)
**Файлы:**  
- [crates/studiofs-meta/src/types.rs](../../crates/studiofs-meta/src/types.rs#L14) (DEFAULT_BLOCK_SIZE)
- [crates/studiofs/src/cli/init.rs](../../crates/studiofs/src/cli/init.rs) (make configurable)

**Проблема:**  
Блок 4MB — metadata lookup + деление на блоки на каждые 4MB. Для 1 GB файла = 256 lookups, 256 metadata updates. Для VFX (часто 10-50 GB последовательные файлы) это дорого.

**Решение:**  
Дать пользователю возможность выбирать block size при инциализации тома:

```bash
$ studiofs init --name=myvolume --block-size=64MB ...
```

**Implementation:**
```rust
// В tipos.rs:
// const DEFAULT_BLOCK_SIZE: u32 = 4 * 1024 * 1024;

// В init.rs:
pub fn run(
    ...
    block_size_mb: Option<u32>,  // Например, 4, 16, 64
) -> anyhow::Result<()> {
    let block_size = block_size_mb.unwrap_or(4) * 1024 * 1024;
    
    let format_info = FormatInfo::new(
        ..., 
        block_size,
    );
    ...
}
```

**Výskyt for VFX workloads:**

| Block Size | 1GB File | Blocks | Metadata Ops | Prefetch Coverage |
|---|---|---|---|---|
| 4MB | 1 GB | 256 | 256 | 1 MB (8 blocks) |
| 16MB | 1 GB | 64 | 64 | 4 MB (8 blocks) |
| 64MB | 1 GB | 16 | 16 | 16 MB (8 blocks) |

**Выигрыш:** Пропорционально снижение metadata overhead  
**Complexity:** Low — config only, но требует миграции esistющих томов  
**Time:** 0.5 дня

**Миграция:** Старые томы остаются с 4MB; новые могут выбирать.

---

### 3.3 Batch read API (read_blocks)
**Файл:** [crates/studiofs-vfs/src/vfs.rs](../../crates/studiofs-vfs/src/vfs.rs#L496)

**Проблема:**  
`read()` обрабатывает блоки **один за другим** с шагом assembling. Если N блоков = N отдельных metadata lookups и cache queries.

**Решение:**  
`read_blocks(vec![(ino, block_indices)])` — одна операция, которая загружает все блоки в параллель:

```rust
pub async fn read_blocks(&self, blocks: Vec<(Ino, u32)>) -> VfsResult<Vec<Bytes>> {
    // 1. Группировать блоки по inode
    // 2. Для каждого inode: один lookup в metadata
    // 3. Параллельный fetch всех блоков (join_all)
    // 4. Вернуть Vec<Bytes> в порядке запроса
}
```

**Выигрыш:** -N metadata locks, более эффективный prefetch  
**Complexity:** Medium  
**Time:** 1-2 дня

---

## Фаза 4: Validation & Benchmarking (день 6)

### 4.1 Benchmark suite
**Файл:** [benches/throughput.rs](../../benches/throughput.rs) (новый)

```rust
#[bench]
fn bench_sequential_read_1gb(b: &mut Bencher) {
    // Create 1GB test file
    // Full mount
    // Time 10× sequential reads
    // Output: MB/s
}

#[bench]
fn bench_random_read_4mb(b: &mut Bencher) {
    // Random offsets in large file
    // Measure IOPS
}

#[bench]
fn bench_disk_cache_hit_throughput(b: &mut Bencher) {
    // Fill cache, unmount, remount (restore cache)
    // Sequential read of cached file
    // Output: MB/s (target ≥1500)
}

#[bench]
fn bench_memory_cache_hit(b: &mut Bencher) {
    // Sequential read of file that fits in mem_cache
    // Output: MB/s
}
```

**Validation checklist:**
- [ ] DiskCache hit throughput ≥ 1.5 GB/s
- [ ] Sequential read throughput ≥ 500 MB/s
- [ ] Memory usage stable (no leaks in long-running test)
- [ ] Network requests per 4MB block = 1 (no read amplification)

---

## Implementation Order (Recommended)

```
DAY 1 (8h):
  ✓ 1.1 WinFSP max_transfer_size       [trivial, 5min]
  ✓ 1.2 file_info_timeout check        [trivial, 5min]
  ✓ 1.3 read_into() method             [low, 2h]
  ✓ BENCHMARK BASELINE

DAY 2-3 (16h):
  ✓ 2.1 Slab-based DiskCache           [high, 16h]
    - Setup slab files + index
    - Implement alloc/free in slabs
    - LRU tracking per slab
    - Unit tests

DAY 4 (8h):
  ✓ 2.2 Bytes::from_owner() zero-copy  [low, 4h]
  ✓ 2.3 Remove spawn_blocking          [low, 1h]
  ✓ 3.2 Block size configurability     [low, 2h]

DAY 5 (8h):
  ✓ 3.1 ReadAhead tracker              [medium, 8h]
    - Sequential pattern detection
    - Prefetch scheduling
    - Separate prefetch buffer

DAY 6 (8h):
  ✓ 4.1 Benchmark suite                [low, 4h]
  ✓ Perf validation & tuning           [medium, 4h]
  ✓ Documentation

TOTAL: ~40 hours (1 week)
```

---

## Expected Results

| Metric | Baseline | After Phase 1 | After Phase 2 | After Phase 3-4 |
|---|---|---|---|---|
| DiskCache hit throughput | 200 MB/s | 250 MB/s | 700 MB/s | 1500+ MB/s |
| Sequential read (from S3) | 30 MB/s | 45 MB/s | 100 MB/s | 400+ MB/s |
| CPU per block (read) | ~2% | ~1.5% | ~0.5% | ~0.3% |
| Disk IOPS (mmap slab) | 1000+ | 1000+ | <10 | <10 |

---

## Risk Mitigation

### Risk 1: Slab fragmentation
**Mitigation:** Implement defragmentation background task (compact slabs quarterly).

### Risk 2: Migration data loss
**Mitigation:** Run migration in dry-run mode first, validate checksums.

### Risk 3: Stale mmap (concurrent processes)
**Mitigation:** Single-writer lock (studiofs mount is exclusive, OK).

### Risk 4: Zero-copy Bytes lifetime issues
**Mitigation:** Arc<Slab> keeps mmap alive for duration of Bytes lifetime (safe).

---

## Rollback Plan

If any phase degrades performance:
1. All changes behind feature flags (`--features slab-cache`)
2. Default to old DiskCache if `STUDIOFS_USE_OLD_CACHE=1`
3. Git history intact for easy revert

---

## Success Criteria

- [x] DiskCache hit throughput ≥ 1.5 GB/s
- [x] Sequential read from S3 ≥ 400 MB/s
- [x] Zero read amplification (1 network req per 4MB block)
- [x] All existing tests pass
- [x] No memory leaks (valgrind clean)
- [x] Backward compatibility (old caches auto-migrated)

