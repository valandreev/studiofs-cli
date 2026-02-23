# StudioFS Cache v3 — Design Document

## Status: DRAFT
## Date: 2026-02-23

---

## 1. Мотивация: что не так с текущим кешем

### 1.1 Проблемы текущей архитектуры

**Slab store (2 GB mmap-файлы) — основной источник проблем:**

| Проблема | Влияние |
|----------|---------|
| `Slab::open_or_create()` вызывает `file.set_len(2GB)` сразу | Нарушает эластичность (req #1). При первом запуске создаётся файл 2 GB, даже если кеш пуст |
| `MmapMut` на 2 GB файл → hard page faults при холодном чтении | Зависания при чтении не из кеша. OS отвечает за page-in с диска, tokio runtime заблокирован |
| Единый `RwLock<HashMap>` для index + `Mutex<BTreeMap>` для LRU | Контеншн на hot read path. Каждый cache hit берёт read lock на index + потенциально write lock на LRU |
| `first-fit` аллокатор с `Vec<FreeRegion>` — O(n) поиск | Фрагментация растёт, аллокация замедляется |
| Сжатые данные для аплоада хранятся в `pending_upload_data` (HashMap в RAM) | Удвоение потребления RAM: decompressed в slab + compressed в heap |
| `index.bin` — кастомный бинарный формат без чексумм | Потеря индекса = потеря всего кеша |
| `background_slab_flusher` — однопоточный, последовательный, 50ms poll | Bottleneck: compress + slab alloc + slab write + meta update — всё в одном потоке |

**Writeback manager:**
- journal files — по одному файлу на pending блок → тысячи мелких файлов в `pending/`
- без fsync → на crash часть журналов теряется (это ОК по дизайну, но грязно)

**MemoryCache:**
- Дизайн c 64 шардами по `Mutex<LruCache>` — адекватный для L1
- Проблема: `evict_to_fit()` итерирует round-robin по шардам — не true global LRU
- При 2-4 GB бюджете работает, но нужен weighter

### 1.2 Почему foyer не подходит

Foyer v0.22 — лучший hybrid cache в экосистеме Rust, но:
- `FsDeviceBuilder::with_capacity(N)` сразу аллоцирует N байт на диске (нарушает req #1 — эластичность)
- «blob engine» записывает данные в большие файлы фиксированного размера — те же проблемы что и наши slabs
- Нет writeback семантики — foyer это read cache, не write-back cache
- Нет понятия «pinned» блоков (pending upload) — eviction может вытеснить блок до завершения аплоада
- Code/serde overhead на деcериализацию — мы работаем с raw `Bytes`, нам это не нужно

### 1.3 Почему писать самостоятельно

Ни один существующий crate не даёт одновременно:
1. Эластичный рост от 0 до N
2. Writeback с pin/unpin
3. Two-level (RAM + NVMe) с единым LRU
4. Production-ready crash recovery

JuiceFS пишет свой кеш, ZeroFS пишет свой (foyer через SlateDB). Наш use-case достаточно специфичен.

---

## 2. Требования (формализация)

| # | Требование | Метрика |
|---|-----------|---------|
| R1 | Эластичность | Кеш при старте = 0 байт на диске. Растёт по мере записи. При уменьшении `--cache-size` фоновое вытеснение до нового лимита |
| R2 | Always writeback | Данные всегда проходят через кеш; upload в S3 asynchronous |
| R3 | Двухуровневость | L1 = RAM (2-4 GB), L2 = NVMe. Единый LRU |
| R4 | NVMe + journaling FS | Можем полагаться на: fsync гарантирует durability, rename атомарен, `O_DIRECT` доступен |
| R5 | Persist after upload | Блок остаётся в кеше после успешного аплоада → повторные чтения не идут в S3 |
| R6 | LRU eviction + disk space guard | Вытеснение по LRU. При <10% free space на диске — lazy background eviction |
| R7 | Cascade cleanup | Удаление файла → очистка кеша + remote storage + metadata |
| R8 | Near-NVMe I/O | Цель: seq write ≥3 GB/s, seq read ≥3.5 GB/s, random 4K read ≥200K IOPS (на кешированных данных) |
| R9 | Не жертвовать upload speed чрезмерно | Upload bandwidth ≥600 Mbit/s при 1 Gbit канале |
| R10 | Configurable block size | Задаётся при `init`, default 1 MiB |

---

## 3. Архитектура: File-per-block на NVMe

### 3.1 Ключевое решение: отказ от slab, переход к file-per-block

**Почему file-per-block вместо slab на NVMe:**

| Аспект | Slab (текущий) | File-per-block (новый) |
|--------|---------------|----------------------|
| Эластичность | ❌ 2GB файл сразу | ✅ 0 файлов при старте |
| Фрагментация | ❌ Внутренняя фрагментация, аллокатор | ✅ Нет — FS управляет |
| Eviction | Freelist bookkeeping | `unlink()` — мгновенно |
| Recovery | Кастомный index.bin | FS — файлы есть или нет |
| Page faults | ❌ 2GB mmap, cold = blocking | ✅ `pread()` — предсказуемо |
| Concurrent I/O | 1 mmap, OS решает | ✅ io_uring / overlapped I/O per file |
| Max IOPS (NVMe 4K random) | mmap OK для cached, плохо для cold | `O_DIRECT` + io_uring: 500K+ IOPS |
| Write amplification | Перезапись внутри mmap | Один write per file, NTFS copy-on-write |

**Почему это не страшно:**
- NVMe поддерживает **500K+ IOPS** random 4K — open/read/close одного файла ≈ 3 IOPS = ничтожно
- NTFS metadata overhead на файл ≈ 1 KB MFT entry — для 1 MiB блоков это 0.1%
- 50 GB кеш / 1 MiB = 50,000 файлов — NTFS легко держит миллионы
- Гарантирует req #1: при 0 блоков в кеше = 0 байт на диске

### 3.2 Структура директорий

```
{cache_dir}/
├── blocks/
│   ├── 00/              ← первые 2 hex-символа object_id (sharding 256 dirs)
│   │   ├── 0000000000000042.block   ← decompressed block data
│   │   ├── 00000000000000ff.block
│   │   └── ...
│   ├── 01/
│   │   └── ...
│   └── ff/
├── upload/
│   ├── 0000000000000042.pending     ← журнал pending upload (заменяет journal files)
│   └── ...
└── cache.meta                       ← LRU index, checkpoint каждые 60s
```

**Sharding:** `{object_id >> 8 & 0xFF}` → 256 поддиректорий. Не даёт NTFS провисать при >10K файлов в одной папке. При 50K блоков = ~195 файлов на директорию.

### 3.3 Block file layout

Каждый `.block` файл — просто raw decompressed данные блока (req: всегда decompressed в кеше).

- Нет header, нет metadata внутри файла
- Размер файла = размеру данных (≤ block_size, last block может быть меньше)
- При аплоаде данные перечитываются из файла и сжимаются (LZ4) → отправляются в S3

### 3.4 Upload journal

Заменяем текущие `*.journal` файлы (по одному на pending op) на **один файл на pending block** в `upload/`:

```
upload/{object_id:016x}.pending
```

Содержимое (48 bytes, fixed):
```
magic:    "STFSUP" (6 bytes)
version:  u16 LE
ino:      u64 LE
offset:   u64 LE
object_id: u64 LE
status:   u8 (0=pending, 1=uploading)
timestamp: i64 LE
_pad:     1 byte
```

- Файл создаётся при submit, удаляется при complete
- `rename(tmp, final)` для атомарности — NTFS/APFS гарантируют
- При crash recovery: scan `upload/*.pending` → re-queue

---

## 4. LRU Tracker: единый для L1 + L2

### 4.1 Дизайн

Вместо трёх отдельных LRU (MemoryCache, SlabStore BTreeMap, DiskCache) — **один глобальный LRU tracker**:

```rust
/// Global LRU tracker for the unified two-level cache.
///
/// Uses a doubly-linked intrusive list protected by a sharded lock.
/// Access time updated lazily (debounced 10s) to avoid contention.
pub struct LruTracker {
    /// Sharded LRU lists — shard = object_id % SHARD_COUNT
    shards: Vec<Mutex<LruShard>>,
    /// Total tracked bytes (lock-free)
    total_bytes: AtomicU64,
}

struct LruShard {
    /// Doubly-linked list: head = most recent, tail = oldest
    entries: LinkedHashMap<ObjectId, LruEntry>,
}

struct LruEntry {
    /// Size of the cached data in bytes
    size: u32,
    /// Where the block currently lives
    location: BlockLocation,
    /// Whether this block is pinned (pending upload)
    pinned: bool,
}

enum BlockLocation {
    /// In RAM (L1 memory cache) + on disk (L2 NVMe)
    MemoryAndDisk,
    /// On disk only (L2 NVMe)
    DiskOnly,
}
```

**Shard count:** 64 shards (same as MemoryCache). `object_id % 64` → shard.

**Debounced LRU update:** Только если последний access >10 секунд назад. Уменьшает contention на hot path.

### 4.2 Eviction flow

```
evict_lru():
  for каждый шард (round-robin):
    entry = oldest non-pinned entry
    if entry.location == MemoryAndDisk:
      → demote: удалить из L1 (RAM), оставить в L2 (NVMe)
      → entry.location = DiskOnly
    elif entry.location == DiskOnly:
      → полное вытеснение: unlink block file с диска
      → удалить из LRU
```

**Двухфазное вытеснение:** сначала из RAM → потом с диска. Это позволяет L1 оставаться маленьким (2-4 GB), а L2 (NVMe) хранить всё остальное.

### 4.3 Promotion

При cache hit на L2 (DiskOnly) — **conditional promote** в L1:
- Если L1 заполнен < 90% → promote (копировать `Bytes` в RAM, обновить location)
- Если L1 > 90% — evict oldest L1 entry первым, затем promote

---

## 5. Компоненты

### 5.1 `BlockCache` — единый API

```rust
pub struct BlockCache {
    /// L1: in-memory (sharded HashMap<ObjectId, Bytes>)
    l1: L1MemCache,
    /// L2: file-per-block on NVMe
    l2: L2DiskCache,
    /// Unified LRU tracker
    lru: LruTracker,
    /// Upload queue (writeback)
    upload_queue: UploadQueue,
    /// Configuration
    config: CacheConfig,
    /// Background eviction signal
    evict_notify: tokio::sync::Notify,
}

pub struct CacheConfig {
    /// L1 RAM budget (bytes) — default 2 GiB
    pub l1_size: u64,
    /// L2 NVMe budget (bytes) — default from --cache-size
    pub l2_size: u64,
    /// Cache directory on NVMe
    pub cache_dir: PathBuf,
    /// Block size (from FormatInfo)
    pub block_size: u32,
    /// Target free disk space ratio (trigger lazy eviction)
    pub disk_free_ratio: f64,
}
```

### 5.2 API

```rust
impl BlockCache {
    /// Create a new cache. Scans existing block files for recovery.
    pub async fn open(config: CacheConfig) -> Result<Arc<Self>>;

    /// Read a block. Returns None on miss.
    /// Lookup order: L1 (RAM) → L2 (NVMe file) → None
    pub async fn get(&self, id: ObjectId) -> Option<Bytes>;

    /// Write a block to cache (L1 + L2).
    /// Block is written to NVMe file and kept in RAM.
    /// If block has upload pending, it's pinned.
    pub async fn put(&self, id: ObjectId, data: Bytes) -> Result<()>;

    /// Remove block from cache entirely.
    pub async fn remove(&self, id: ObjectId);

    /// Check if block exists in cache (any level).
    pub fn contains(&self, id: ObjectId) -> bool;

    /// Pin a block (prevent eviction until upload completes).
    pub fn pin(&self, id: ObjectId);

    /// Unpin a block (allow eviction).
    pub fn unpin(&self, id: ObjectId);

    /// Submit block for upload. Creates pending journal.
    pub fn submit_upload(&self, ino: Ino, offset: u64, object_id: ObjectId);

    /// Mark upload as complete. Removes pending journal. Block stays in cache.
    pub fn complete_upload(&self, object_id: ObjectId);

    /// Resize L2 limit (for --cache-size changes between mounts).
    pub async fn resize(&self, new_l2_size: u64);

    /// Graceful shutdown: persist LRU index, wait for pending uploads.
    pub async fn shutdown(&self, timeout: Duration) -> bool;
}
```

### 5.3 `L1MemCache` — RAM level

Аналогичен текущему `MemoryCache`, но ключ — `ObjectId` вместо `(Ino, offset)`:

```rust
struct L1MemCache {
    shards: Vec<Mutex<HashMap<ObjectId, Bytes>>>,
    total_bytes: AtomicU64,
    max_bytes: u64,
}
```

- 64 шарда, `object_id % 64`
- Нет своего LRU — полагается на глобальный `LruTracker`
- `get()` → HashMap lookup, O(1)
- `put()` → insert, если RAM > budget → сигнал eviction

### 5.4 `L2DiskCache` — NVMe level

```rust
struct L2DiskCache {
    cache_dir: PathBuf,
    total_bytes: AtomicU64,
    max_bytes: AtomicU64,       // мутируется при resize()
    /// io_uring / overlapped I/O pool
    io_pool: IoPool,
}

impl L2DiskCache {
    fn block_path(&self, id: ObjectId) -> PathBuf {
        let hex = format!("{:016x}", id);
        let shard = &hex[..2];   // first 2 hex chars = 256 dirs
        self.cache_dir.join("blocks").join(shard).join(format!("{hex}.block"))
    }

    /// Read block from NVMe. Uses io_uring on Linux, Overlapped I/O on Windows.
    async fn read(&self, id: ObjectId) -> Option<Bytes>;

    /// Write block to NVMe.
    async fn write(&self, id: ObjectId, data: &[u8]) -> io::Result<()>;

    /// Delete block file.
    async fn remove(&self, id: ObjectId);
}
```

**I/O стратегия:**
- **Windows:** `tokio::fs` (thread-pool backed) — NTFS не поддерживает io_uring. `FILE_FLAG_SEQUENTIAL_SCAN` для seq reads, `FILE_FLAG_NO_BUFFERING + FILE_FLAG_WRITE_THROUGH` не нужен (NTFS journaling достаточно)
- **Linux (future):** `tokio-uring` или `glommio` для io_uring native async I/O
- **Оба:** `spawn_blocking` fallback для портабельности

**Write path:**
```
write(id, data):
  path = block_path(id)
  ensure_parent_dir(path)
  tmp = path.with_extension("tmp")
  write_file(tmp, data)        // full write
  rename(tmp, path)            // atomic
  total_bytes += data.len()
```

**Read path:**
```
read(id):
  path = block_path(id)
  if !path.exists() → None
  data = read_file(path)
  Some(Bytes::from(data))
```

### 5.5 `UploadQueue` — writeback journal

Заменяет текущий `WritebackManager`:

```rust
struct UploadQueue {
    upload_dir: PathBuf,
    sender: mpsc::UnboundedSender<UploadOp>,
    pending_count: AtomicUsize,
    idle_notify: Notify,
}

struct UploadOp {
    ino: Ino,
    offset: u64,
    object_id: ObjectId,
    journal_path: PathBuf,
}
```

**Submit (hot path, non-blocking):**
1. Создать `upload/{object_id:016x}.pending` (tmp + rename)
2. Pin объект в LRU
3. Отправить `UploadOp` в канал

**Upload worker:**
1. Получить `UploadOp` из канала
2. Прочитать decompressed блок из L2 disk cache → LZ4 compress → S3 PUT
3. При успехе: unpin, удалить `.pending` файл
4. При ошибке: requeue с backoff

**Recovery (startup):**
- Scan `upload/*.pending` → для каждого: pin, re-queue

---

## 6. Data Flow (новый)

### 6.1 Write path

```
Application write(ino, offset, data)
  │
  ├─ Buffer в pending_blocks (BytesMut, in-memory per-block)
  │
  ├─ [блок полон или flush()]
  │   │
  │   ├─ dirty_mem → (Ino, BlockIdx, Bytes)
  │   │
  │   └─ background_flusher (N worker tasks, не 1):
  │       │
  │       ├─ object_id = meta.next_block_id()
  │       ├─ cache.put(object_id, data)          // L1 + L2
  │       │   ├─ L1: HashMap insert (RAM)
  │       │   └─ L2: write tmp → rename (NVMe)
  │       │
  │       ├─ cache.submit_upload(ino, offset, object_id)
  │       │   ├─ write .pending journal
  │       │   └─ pin(object_id)
  │       │
  │       ├─ meta.set_file_block(ino, idx, block_ref)
  │       └─ done → remove from dirty_mem
  │
  └─ [upload worker, async]:
      ├─ data = cache.get(object_id) from L2   // read from NVMe
      ├─ compressed = lz4_compress(data)        // compress for upload
      ├─ S3 PUT compressed
      ├─ cache.complete_upload(object_id)       // unpin, remove .pending
      └─ блок ОСТАЁТСЯ в кеше (req R5)
```

### 6.2 Read path

```
Application read(ino, offset, len)
  │
  ├─ pending_blocks? → return (write buffer hit)
  ├─ dirty_mem? → return (not yet flushed)
  │
  ├─ block_ref = meta.get_file_block(ino, block_idx)
  │
  ├─ L1 cache.get(object_id)?
  │   └─ HashMap hit → return Bytes     (0 syscalls, ~10ns)
  │
  ├─ L2 cache.get(object_id)?
  │   ├─ read file from NVMe            (~50-100μs)
  │   ├─ promote to L1 (if budget allows)
  │   └─ return Bytes
  │
  ├─ S3 GET (with inflight coalescing)
  │   ├─ download compressed
  │   ├─ decompress LZ4
  │   ├─ cache.put(object_id, data)   // populate L1 + L2
  │   └─ return Bytes
```

### 6.3 Delete path

```
Application unlink(ino)
  │
  ├─ blocks = meta.get_file_blocks(ino)
  ├─ for each block:
  │   ├─ cancel pending upload if any
  │   ├─ cache.remove(object_id)          // L1 + L2 + LRU
  │   ├─ meta.add_pending_deletion(object_id)  // janitor удалит из S3
  │   └─ meta.remove_file_block(ino, idx)
  │
  └─ meta.remove_inode(ino)
```

---

## 7. Background flusher: параллельный

### Текущая проблема

`drain_dirty_mem()` — однопоточный цикл, обрабатывает блоки последовательно. При 1 MiB блоке и NVMe write ~100μs + LZ4 compress ~500μs + meta update ~100μs = ~700μs на блок = **~1.4 GB/s** max throughput. Для 3.5 GB/s NVMe это bottleneck.

### Решение: N flusher workers

```rust
async fn background_flusher_pool(self: Arc<Self>, mut rx: UnboundedReceiver<()>) {
    let concurrency = num_cpus::get().min(8).max(2);
    let sem = Arc::new(Semaphore::new(concurrency));

    let mut interval = tokio::time::interval(Duration::from_millis(20));
    loop {
        tokio::select! {
            biased;
            msg = rx.recv() => { if msg.is_none() { break; } }
            _ = interval.tick() => {}
        }

        // Grab all dirty blocks under one lock
        let batch: Vec<_> = {
            let mut dm = self.state.dirty_mem.lock();
            let keys: Vec<_> = dm.keys().cloned().collect();
            keys.into_iter()
                .filter_map(|k| dm.remove(&k).map(|d| (k, d)))
                .collect()
        };

        for ((ino, block_idx), data) in batch {
            let permit = sem.clone().acquire_owned().await.unwrap();
            let vfs = self.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let len = data.len();
                if let Err(e) = vfs.flush_block(ino, block_idx, data).await {
                    error!("flusher: flush_block({ino}, {block_idx}) failed: {e}");
                    // TODO: put back into dirty_mem for retry
                }
                vfs.state.dirty_bytes.fetch_sub(len, Ordering::Relaxed);
            });
        }
    }
}
```

**Ожидаемый throughput:** 4 workers × 1.4 GB/s = **~5.6 GB/s** theoretical → NVMe bottleneck ~3.5 GB/s → OK.

---

## 8. Eviction Engine

### 8.1 Background evictor

```rust
async fn background_evictor(cache: Arc<BlockCache>) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;

        let l2_used = cache.l2.total_bytes.load(Relaxed);
        let l2_max = cache.l2.max_bytes.load(Relaxed);
        let l1_used = cache.l1.total_bytes.load(Relaxed);
        let l1_max = cache.config.l1_size;

        // Phase 1: L1 overflow → demote to L2 only (free RAM)
        if l1_used > l1_max {
            cache.evict_l1(l1_used - l1_max);
        }

        // Phase 2: L2 overflow → full eviction (delete files)
        if l2_used > l2_max {
            cache.evict_l2(l2_used - l2_max);
        }

        // Phase 3: Disk space guard (<10% free → emergency eviction)
        if let Some((free, total)) = get_free_disk_space(&cache.config.cache_dir) {
            let ratio = free as f64 / total as f64;
            if ratio < cache.config.disk_free_ratio {
                let target_free = (total as f64 * cache.config.disk_free_ratio) as u64;
                let need_to_free = target_free.saturating_sub(free);
                cache.evict_l2(need_to_free);
            }
        }
    }
}
```

### 8.2 Resize (hot)

```rust
impl BlockCache {
    pub async fn resize(&self, new_l2_size: u64) {
        let old = self.l2.max_bytes.swap(new_l2_size, Ordering::Relaxed);
        if new_l2_size < old {
            // Trigger immediate background eviction
            self.evict_notify.notify_one();
        }
        info!("cache resized: {} → {} GB", old / GiB, new_l2_size / GiB);
    }
}
```

---

## 9. LRU persistence (crash safe)

### 9.1 `cache.meta`

LRU ordering и block registry нужно восстанавливать после restart. Опции:

**Option A: scan filesystem (no index file)**
- При старте: scan `blocks/**/*.block` → rebuild LRU (all entries = "now", no ordering)
- Pro: простейшая реализация, нет проблем с corrupt index
- Con: теряем LRU ordering при restart → cold start eviction будет неоптимальным

**Option B: periodic checkpoint (recommended)**
- Каждые 60 секунд: записать `cache.meta` с LRU order
- При старте: загрузить `cache.meta`, проверить наличие файлов, rebuild LRU
- При повреждении / отсутствии: fallback на Option A (scan)

**Формат `cache.meta`:**
```
Header:
  magic:   "STFSCM" (6 bytes)
  version: u16 LE
  count:   u32 LE
  checksum: u32 LE (CRC32 of entries)

Per entry (18 bytes):
  object_id: u64 LE
  size:      u32 LE
  location:  u8  (0=DiskOnly, 1=MemoryAndDisk)
  pinned:    u8
  _pad:      4 bytes
```

- Entries записаны в LRU order (oldest first)
- tmp + rename → atomic write
- CRC32 checksum → detect corruption

---

## 10. Производительность: ожидания

### 10.1 Benchmarks (цели)

| Операция | Текущий (slab) | Цель (file-per-block) | Bottleneck |
|---------|---------------|----------------------|-----------|
| L1 cache hit read | ~10 ns | ~10 ns | HashMap lookup |
| L2 cache hit read (warm) | ~1-5 μs (mmap, cached page) | ~50-100 μs (pread) | File open + read syscall |
| L2 cache hit read (cold) | **~1-50 ms** (page fault, blocking!) | ~100-200 μs (async I/O) | NVMe latency |
| Write to cache | ~5 μs (memcpy into mmap) | ~100-200 μs (file write) | NVMe write |
| Sequential read (cached) | ~2 GB/s | ~3+ GB/s | Readahead + mem bandwidth |
| Sequential write | ~1.4 GB/s (single flusher) | ~3+ GB/s (parallel flusher) | NVMe + CPU (LZ4) |

**Ключевое улучшение:** холодные чтения (L2 miss в page cache) падают с 1-50 ms (mmap page fault, blocking tokio) до 100-200 μs (async file read, non-blocking).

### 10.2 Почему L2 cold read быстрее

Текущий slab:
1. `read_slice()` → pointer dereference в mmap'd region
2. Если page не в OS page cache → **hard page fault**
3. Kernel обрабатывает page fault **синхронно в том же потоке**
4. Tokio worker заблокирован на 1-50 ms
5. Другие задачи на этом worker голодают

File-per-block:
1. `tokio::fs::read()` → `spawn_blocking` → `std::fs::read()`
2. Non-blocking для tokio runtime
3. NVMe latency ~10-20 μs для small random read
4. Overhead: thread pool scheduling ~5-20 μs
5. Total: ~30-100 μs, **never blocks the runtime**

### 10.3 Readahead optimization для seq read

Для последовательного чтения (видео) текущий ReadAheadTracker уже запускает prefetch на следующие блоки. С file-per-block кешем prefetch эффективнее:

- Каждый prefetched блок — отдельный file read → параллельные I/O operations
- NVMe command queue depth 32-64 → можно параллелить 8-16 reads
- OS-level readahead на каждый файл не мешает (файл = один блок)

---

## 11. Миграция

### 11.1 Совместимость

Кеш v3 **несовместим** с slab-form кешем v2. Миграция:
1. При первом запуске с новым кодом: детектировать наличие `slabs/` dir
2. Опции:
   - **Просто удалить** старый кеш (рекомендуется — кеш не содержит данных, которых нет в S3)
   - Миграция: прочитать index.bin → для каждого entry → записать file → удалить слаб

### 11.2 Этапы имплементации

```
Phase 1: BlockCache core (L2 file-per-block + LruTracker)
  ├─ L2DiskCache: put/get/remove/contains
  ├─ LruTracker: insert/touch/evict/pin/unpin
  ├─ Background evictor
  ├─ cache.meta checkpoint/recovery
  └─ Unit tests

Phase 2: L1 MemCache integration
  ├─ L1MemCache с ключом ObjectId
  ├─ Promotion/demotion logic
  ├─ Unified get() → L1 → L2 → None
  └─ Unit tests

Phase 3: UploadQueue (replace WritebackManager)
  ├─ .pending journal files
  ├─ submit/complete/recovery
  ├─ upload worker (re-compress from L2)
  └─ Unit tests

Phase 4: VFS integration
  ├─ Заменить DiskCache + MemoryCache + WritebackManager на BlockCache
  ├─ Параллельный background_flusher
  ├─ Remove dirty_mem pending_upload_data
  └─ Integration tests

Phase 5: Cleanup migration logic
  ├─ Detect old slabs/, delete
  ├─ Remove old slab.rs, disk.rs, writeback.rs
  └─ End-to-end benchmarks
```

---

## 12. Открытые вопросы

1. **Windows I/O strategy:** `tokio::fs` (spawn_blocking + regular I/O) vs `FILE_FLAG_NO_BUFFERING` (direct I/O) для L2 writes. Direct I/O позволяет обойти OS page cache (экономит RAM), но требует aligned buffers и усложняет код.

2. **Block dedup:** если два файла имеют одинаковый контент одного блока, они получают разные `object_id` и хранятся дважды в кеше. Сейчас это не проблема (каждый файл независим), но при переполненном кеше это неэффективно.

3. **Warm restart:** при перемонтировании того же volume с тем же `--cache-dir`, блоки в кеше уже валидны. Нужно проверять, что `cache.meta` соответствует format_info (volume name, block_size) — иначе инвалидировать.

4. **Large file optimization:** для файлов >10 GB seq write лучше bypass L1 (не загрязнять RAM). Пока не критично.
