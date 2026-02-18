# План реализации оптимизаций производительности чтения StudioFS

**Дата:** 2026-02-18  
**На основе:** `docs/read-performance-audit.md`  
**Цель:** устранить read amplification, достичь ~1.5 ГБ/с при чтении из кэша

---

## Этап 0 — Подготовка и бенчмарки (baseline)

> Перед любыми изменениями необходимо зафиксировать текущие показатели, чтобы измерять прогресс.

### Задачи

- [ ] **0.1** Написать бенчмарк-скрипт последовательного чтения: читать файлы размером 1 ГиБ и 10 ГиБ, фиксировать `user throughput` (МБ/с), `network bandwidth`, `CPU usage`.
- [ ] **0.2** Написать бенчмарк cache-hit read: заполнить DiskCache, перезапустить без сети, измерить устойчивый throughput (цель: 1.5 ГБ/с).
- [ ] **0.3** Добавить счётчики в `tracing`: логировать число обращений к `read_from_storage`, `get_object_with_cache`, количество network requests — для измерения read amplification.
- [ ] **0.4** Зафиксировать baseline-метрики в таблице:

| Метрика | Baseline (до) | Цель (после) |
|---|---|---|
| User sequential read throughput | ~30 МБ/с | ≥500 МБ/с |
| DiskCache hit throughput | 150–300 МБ/с | ≥1.5 ГБ/с |
| Network requests per 4 МБ read | ≥2 | 1 |
| CPU per block (compression path) | - | замерить |

---

## Этап 1 — Быстрые изменения (1-2 дня, нулевая архитектурная сложность)

### P4 — Убрать `sync_all()` в DiskCache при записи

**Файл:** `crates/studiofs-cache/src/disk.rs` → метод `put_object()`

**Почему:** `file.sync_all()` — это fsync; для кэша, данные которого можно всегда перечитать из S3, это бессмысленная задержка 50–500 мкс на блок.

**Что сделать:**
- [x] **1.1** Удалить строку `file.sync_all()?;` из `put_object()` (блок `spawn_blocking`).
- [x] **1.2** Убедиться, что `sync_all()` **остаётся** в `writeback.rs` → `submit()` (журнал — критические данные, fsync обязателен).
- [x] **1.3** Добавить комментарий: `// sync_all omitted: cache data is re-fetchable from S3`.
- [x] **1.4** Прогнать тест `cargo test -p studiofs-cache`.
- [x] **1.5** Замерить throughput записи в DiskCache (ожидается рост в 5–10×).

**Diff-пример:**
```rust
// БЫЛО:
file.write_all(&data_vec)?;
file.sync_all()?;

// СТАЛО:
file.write_all(&data_vec)?;
// sync_all() omitted: cache is re-fetchable from S3, fsync not required here
```

---

### P6 — Предаллокация буферов при чтении из файла

**Файл:** `crates/studiofs-cache/src/disk.rs` → `get_object()` (внутри `spawn_blocking`)

**Почему:** `read_to_end` с пустым `Vec` для 4 МБ блока делает 2–3 реаллокации; `with_capacity` устраняет их.

**Что сделать:**
- [x] **1.6** Заменить `let mut data = Vec::new();` на:
  ```rust
  let file_len = file.metadata().map(|m| m.len() as usize).unwrap_or(0);
  let mut data = Vec::with_capacity(file_len);
  ```
- [x] **1.7** Убедиться, что `file.metadata()` вызывается на уже открытом файловом дескрипторе (без доп. syscall на открытие).
- [x] **1.8** Прогнать `cargo test -p studiofs-cache`.

---

### P9-a — Включить кэширование FileInfo в WinFSP

**Файл:** `crates/studiofs-winfsp/src/lib.rs` → `mount_and_serve()`

**Почему:** `file_info_timeout` позволяет WinFSP кэшировать атрибуты файлов, сокращая число вызовов `GetFileInfo`/`getattr` → меньше redb транзакций.

**Что сделать:**
- [x] **1.9** После `volume_params.sector_size(512);` добавить:
  ```rust
  volume_params.file_info_timeout(1000); // cache file info for 1 second
  volume_params.dir_info_timeout(1000);  // cache dir info for 1 second
  ```
- [x] **1.10** Проверить корректность работы `overwrite`/`set_file_size` — при кэшированных атрибутах необходимо сбрасывать кэш на модификацию (WinFSP делает это автоматически на write-path операциях).

---

## Этап 2 — Оптимизация DiskCache (3–5 дней, средняя сложность)

### P2 — Хранить в DiskCache распакованные данные

**Файлы:** `crates/studiofs-cache/src/disk.rs`, `crates/studiofs-vfs/src/vfs_v2.rs`

**Почему:** Сейчас DiskCache хранит STFS-объекты (header 32B + LZ4). При каждом cache-hit выполняется LZ4 декомпрессия. Хранение распакованных данных исключает decompress из hot path чтения.

**Компромисс:** Дисковое пространство увеличивается пропорционально ratio сжатия (~1.5–3×); для медиаданных (VFX) сжимаемость низка, поэтому потери небольшие.

**Что сделать:**
- [x] **2.1** Добавить флаг в `DiskCacheConfig`:
  ```rust
  pub struct DiskCacheConfig {
      ...
      /// Store decompressed data in disk cache (faster reads, more disk space).
      pub store_decompressed: bool,
  }
  ```
- [x] **2.2** В `vfs_v2.rs` → `read_block()`: при **промахе кэша** после fetch из S3:
  - Декомпрессировать `read_object(&data)` → `decompressed`
  - Сохранить в DiskCache **декомпрессированное**: `disk_cache.put_object(id, &decompressed)`
  - Вернуть `Bytes::from(decompressed)` (данные уже готовы)
- [x] **2.3** В `vfs_v2.rs` → `read_block()`: при **попадании кэша**:
  ```rust
  // Если store_decompressed=true — данные уже decompressed, возвращаем напрямую
  if let Some(data) = self.disk_cache.get_object(block_ref.object_id).await? {
      // No decompression needed if store_decompressed is enabled
      return Ok(Bytes::from(data.to_vec()));
  }
  ```
- [x] **2.4** При записи блоков (`flush_block()`): данные сохраняются в DiskCache **до** загрузки на S3 — сохранять тоже в декомпрессированном виде (объект для S3 создаётся отдельно через `create_object()`).
- [x] **2.5** Убедиться, что writeback queue (`writeback.submit()`) получает ссылку на сжатый объект для S3, а не на данные DiskCache.
- [ ] **2.6** Добавить тест: запись 4 МБ блока → DiskCache hit → данные совпадают с оригиналом.
- [ ] **2.7** Замерить DiskCache hit throughput (ожидается +2–3×).

---

### P3 — mmap для чтения из DiskCache

**Файл:** `crates/studiofs-cache/src/disk.rs`, `Cargo.toml` кракта

**Почему:** `spawn_blocking` + `read_to_end` — это thread context switch + vector grow. mmap позволяет ядру OS управлять page cache и даёт zero-copy semantics.

**Что сделать:**
- [x] **3.1** Добавить зависимость в `crates/studiofs-cache/Cargo.toml`:
  ```toml
  memmap2 = "0.9"
  ```
- [x] **3.2** Реализовать `get_object_mmap()` как альтернативу `get_object()`:
  ```rust
  pub fn get_object_sync(&self, id: ObjectId) -> DiskCacheResult<Option<Bytes>> {
      let path = self.object_path(id);
      let file = match std::fs::File::open(&path) {
          Ok(f) => f,
          Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
          Err(e) => return Err(DiskCacheError::Io(e)),
      };
      let metadata = file.metadata()?;
      let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
      // Update LRU tracking
      self.touch_lru(id, metadata.len());
      Ok(Some(Bytes::copy_from_slice(&mmap)))
  }
  ```
- [x] **3.3** Обновить `get_object()` — использовать sync mmap (без `spawn_blocking`) или вызывать `get_object_sync()` из `spawn_blocking`.
- [ ] **3.4** Оценить: при `store_decompressed=true` + mmap — прямой возврат slice файла без аллокаций (максимальная эффективность).
- [ ] **3.5** Тест: корректность данных при mmap чтении.
- [ ] **3.6** Бенчмарк: ожидается +50–100% vs `read_to_end`.

---

## Этап 3 — Шардирование кэшей (2–3 дня, средняя сложность)

### P5 — Шардированный MemoryCache

**Файл:** `crates/studiofs-cache/src/memory.rs`

**Почему:** Единый `Mutex<LruCache>` — точка contention при ≥4 параллельных читателях. При шардировании по `(ino XOR (offset >> 22)) % N` блокировки не пересекаются.

**Что сделать:**
- [ ] **4.1** Добавить в `Cargo.toml` studiofs-cache: уже есть `parking_lot` — достаточно.
- [ ] **4.2** Заменить структуру `MemoryCache`:
  ```rust
  const SHARD_COUNT: usize = 64;
  
  pub struct MemoryCache {
      shards: Vec<Mutex<LruCache<CacheKey, Bytes>>>,
      shard_max_bytes: usize,
      total_bytes: std::sync::atomic::AtomicUsize, // atomic для избежания lock
      max_bytes: usize,
  }
  ```
- [ ] **4.3** Реализовать `fn shard_idx(key: &CacheKey) -> usize`:
  ```rust
  fn shard_idx(key: &CacheKey) -> usize {
      ((key.ino ^ (key.offset >> 22)) as usize) % SHARD_COUNT
  }
  ```
- [ ] **4.4** Обновить `get()`, `put()`, `remove()`, `invalidate_inode()` для работы с шардами.
- [ ] **4.5** `current_bytes()` — читать из `AtomicUsize` (lock-free).
- [ ] **4.6** `invalidate_inode()` — итерировать по всем шардам; взять lock на каждый последовательно (acceptable, редкая операция).
- [ ] **4.7** Прогнать все существующие тесты `cargo test -p studiofs-cache`.

---

### P5-b — Шардированный LRU в DiskCache

**Файл:** `crates/studiofs-cache/src/disk.rs`

**Что сделать:**
- [ ] **4.8** Шардировать `entries: Mutex<BTreeMap<ObjectId, CacheEntry>>` аналогично: `entries: Vec<Mutex<BTreeMap<ObjectId, CacheEntry>>>` с `SHARD_COUNT = 256`.
- [ ] **4.9** `current_size` → `AtomicU64` (lock-free).
- [ ] **4.10** `find_lru_candidate()` — итерировать по всем шардам, возвращать глобальный LRU (допустима небольшая неточность — approximate LRU).
- [ ] **4.11** Обновить тесты.

---

## Этап 4 — Параллельное чтение блоков в VFS v2 (1–2 дня, низкая сложность)

### P8 — Batch/parallel block fetch

**Файл:** `crates/studiofs-vfs/src/vfs_v2.rs` → метод `read()`

**Почему:** Текущий `while remaining > 0` читает блоки **последовательно**. Запрос на 16 МБ = 4 последовательных чтения по 4 МБ. Параллельный fetch даёт линейный прирост до насыщения диска/сети.

**Что сделать:**
- [ ] **5.1** Добавить в `Cargo.toml` studiofs-vfs: `futures = "0.3"` (уже есть).
- [ ] **5.2** Вычислить список всех нужных блоков до начала чтения:
  ```rust
  let mut blocks_needed: Vec<(u32, usize, usize)> = Vec::new(); // (block_index, offset_in_block, to_copy)
  let mut cur = offset;
  while cur < end {
      let idx = block_index_from_offset(cur, self.block_size);
      let off_in = offset_within_block(cur, self.block_size) as usize;
      let space = self.block_size as usize - off_in;
      let to_copy = (end - cur).min(space as u64) as usize;
      blocks_needed.push((idx, off_in, to_copy));
      cur += to_copy as u64;
  }
  ```
- [ ] **5.3** Параллельный fetch через `futures::future::join_all`:
  ```rust
  let fetched: Vec<VfsResult<Bytes>> = futures::future::join_all(
      blocks_needed.iter().map(|(idx, _, _)| {
          // check pending_blocks first, then read_block
          async move { self.read_block_or_pending(ino, *idx).await }
      })
  ).await;
  ```
- [ ] **5.4** Собрать результаты в порядке запроса (порядок гарантирован `join_all`).
- [ ] **5.5** Ограничить параллелизм: использовать `futures::stream::FuturesOrdered` + `buffer_unordered(N)` с `N = 16` чтобы не перегружать S3 при промахах кэша.
- [ ] **5.6** Тест: чтение 16 МБ файла — сравнить с seq чтением по корректности и скорости.
- [ ] **5.7** Замерить throughput (ожидается +2–4× для больших sequential reads при cache-miss).

---

## Этап 5 — Переход на VFS v2 в продакшне (3–7 дней, высокая важность)

### P1 — Миграция mount.rs и context.rs на VFS v2

**Файлы:** `crates/studiofs/src/cli/mount.rs`, `crates/studiofs-winfsp/src/context.rs`, `crates/studiofs-fuse/src/fs.rs`

**Почему:** VFS v1 содержит всю legacy архитектуру (ObjectPayload, root-object, splinter). VFS v2 использует `FILE_BLOCKS` таблицу redb — метаданные блоков хранятся локально, данные в S3 — простые compressed объекты.

#### 5a — Аудит совместимости API

- [ ] **6.1** Сравнить публичные методы `StudioVfs` (v1) и `vfs_v2::StudioVfs`: `read`, `write`, `flush`, `getattr`, `setattr`, `lookup`, `readdir`, `create`, `mkdir`, `unlink`, `rmdir`, `rename`.
- [ ] **6.2** Найти методы, присутствующие в v1 но отсутствующие в v2 (например, `volume_name()`, `uuid()`, `spawn_background_gc()`).
- [ ] **6.3** Добавить недостающие методы к `vfs_v2::StudioVfs`.

#### 5b — Адаптация context.rs (WinFSP)

- [ ] **6.4** Изменить import в `context.rs`:
  ```rust
  // БЫЛО:
  use studiofs_vfs::StudioVfs;
  // СТАЛО:
  use studiofs_vfs::vfs_v2::StudioVfs;
  ```
- [ ] **6.5** Проверить сигнатуры — `read(ino, offset, size: u32)` должна совпадать.
- [ ] **6.6** `VfsError` — убедиться, что новый `VfsError` из v2 маппится в WinFSP NTSTATUS корректно (обновить `vfs_to_winfsp_err()` при необходимости).

#### 5c — Адаптация mount.rs

- [ ] **6.7** Изменить import в `mount.rs`:
  ```rust
  use studiofs_vfs::vfs_v2::StudioVfs;
  ```
- [ ] **6.8** VFS v2 не принимает `mem_cache` как параметр (его нет в `StudioVfs::new`). Убрать `MemoryCache` из `mount.rs` для v2. Или добавить MemoryCache в v2 (рекомендуется — см. ниже).
- [ ] **6.9** Добавить MemoryCache в `vfs_v2::StudioVfs` для L1 кэша (сейчас его нет!):
  ```rust
  pub struct StudioVfs {
      ...
      mem_cache: Option<Arc<MemoryCache>>,  // Optional L1 memory cache
  }
  ```
- [ ] **6.10** В `read_block()` добавить проверку mem_cache до DiskCache:
  ```rust
  // 1. Check L1 memory cache
  let cache_key = CacheKey::new(ino, block_index as u64 * self.block_size as u64);
  if let Some(ref mc) = self.mem_cache {
      if let Some(data) = mc.get(&cache_key) {
          return Ok(data);
      }
  }
  // 2. Check disk cache
  // ...
  // 3. Populate memory cache on hit
  if let Some(ref mc) = self.mem_cache {
      mc.put(cache_key, result.clone());
  }
  ```
- [ ] **6.11** `spawn_background_gc()` — перенести логику в v2 или вызывать из `StudioVfs::new()`.

#### 5d — Адаптация FUSE

- [ ] **6.12** Аналогично 6.4–6.6 для `crates/studiofs-fuse/src/fs.rs`.

#### 5e — Тестирование и миграция данных

- [ ] **6.13** Убедиться, что тесты `vfs_v2_test.rs` проходят.
- [ ] **6.14** Написать интеграционный тест: создать файл через v2, прочитать его, убедиться в корректности.
- [ ] **6.15** Данные, записанные VFS v1 (DATA_ROOTS_TABLE, chunks-таблица), **несовместимы** с v2 (FILE_BLOCKS). Нужны варианты:
  - **A (рекомендуется):** При upgrade `studiofs mount` — детектировать старый формат и завершить с ошибкой `"старый формат, выполните studiofs migrate"`.
  - **B:** Написать `studiofs migrate` — читать DATA_ROOTS/chunks, конвертировать в FILE_BLOCKS.
- [ ] **6.16** Добавить версию формата в `FormatInfo` (`format_version: u8`) и проверять при монтировании.
- [ ] **6.17** Прогнать полный `cargo test`.

---

## Этап 6 — Кэш атрибутов на пути чтения (1 день)

### P7 — Read-through AttrCache с TTL

**Файл:** `crates/studiofs-vfs/src/vfs_v2.rs`

**Почему:** `read()` вызывает `getattr()` → `meta.get_attr()` → redb read transaction на каждый вызов. Для последовательного чтения одного файла каждый вызов повторяет открытие транзакции.

**Что сделать:**
- [ ] **7.1** Добавить `attr_cache` в `VfsState`:
  ```rust
  struct VfsState {
      ...
      attr_read_cache: Mutex<HashMap<Ino, (InodeAttr, std::time::Instant)>>,
  }
  const ATTR_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(1);
  ```
- [ ] **7.2** Обновить `getattr()`:
  ```rust
  pub fn getattr(&self, ino: Ino) -> VfsResult<InodeAttr> {
      // 1. Check dirty_attrs (write path)
      if let Some(a) = self.state.dirty_attrs.lock().get(&ino) { return Ok(a.clone()); }
      // 2. Check read-through cache
      {
          let cache = self.state.attr_read_cache.lock();
          if let Some((attr, ts)) = cache.get(&ino) {
              if ts.elapsed() < ATTR_CACHE_TTL { return Ok(attr.clone()); }
          }
      }
      // 3. Fetch from redb
      let attr = self.meta.get_attr(ino)?;
      self.state.attr_read_cache.lock().insert(ino, (attr.clone(), std::time::Instant::now()));
      Ok(attr)
  }
  ```
- [ ] **7.3** При `setattr()` — инвалидировать `attr_read_cache` для inode.
- [ ] **7.4** При `unlink()` — инвалидировать `attr_read_cache`.
- [ ] **7.5** Тест: при быстрых повторных вызовах getattr — должен возвращать кэшированный результат.

---

## Этап 7 — Финальное тестирование и замеры (1–2 дня)

- [ ] **8.1** Запустить полный `cargo test --workspace`.
- [ ] **8.2** Повторить все бенчмарки из Этапа 0 (baseline).
- [ ] **8.3** Заполнить таблицу результатов:

| Метрика | Baseline | После Этапа 1 | После Этапа 2 | После Этапа 5 | Цель |
|---|---|---|---|---|---|
| Sequential read (user) | ~30 МБ/с | - | - | - | ≥500 МБ/с |
| DiskCache hit throughput | 150–300 МБ/с | - | - | - | ≥1.5 ГБ/с |
| Network requests / 4 МБ | ≥2 | - | - | - | 1 |

- [ ] **8.4** При недостижении целей — профилировать через `cargo flamegraph` или `tokio-console`.
- [ ] **8.5** Обновить `docs/read-performance-audit.md` с финальными результатами.

---

## Зависимости между этапами

```
Этап 0 (baseline)
  └─► Этап 1 (sync_all, prealloc, winfsp tuning)  ← независимые изменения
        └─► Этап 2 (DiskCache: decomp + mmap)       ← зависит от понимания кэша
              └─► Этап 3 (shard caches)              ← можно параллельно
  └─► Этап 4 (parallel block fetch в VFS v2)        ← независим от 1–3
  └─► Этап 5 (migrate to VFS v2)                   ← главный,  завершить последним
        └─► Этап 6 (attr cache)                     ← только после v2
              └─► Этап 7 (benchmarks + cleanup)
```

---

## Зависимости в Cargo.toml

| Крейт | Новая зависимость | Где |
|---|---|---|
| `studiofs-cache` | `memmap2 = "0.9"` | P3 |
| — | (всё остальное уже есть) | — |

---

## Файлы, которые будут изменены

| Файл | Этап | Тип изменения |
|---|---|---|
| `crates/studiofs-cache/src/disk.rs` | 1,2,3,4 | Убрать sync_all, prealloc, mmap, shard |
| `crates/studiofs-cache/src/memory.rs` | 3 | Shard LruCache |
| `crates/studiofs-cache/Cargo.toml` | 3 | +memmap2 |
| `crates/studiofs-vfs/src/vfs_v2.rs` | 2,4,5,6 | store_decompressed, parallel fetch, mem_cache, attr_cache |
| `crates/studiofs-winfsp/src/lib.rs` | 1 | file_info_timeout |
| `crates/studiofs-winfsp/src/context.rs` | 5 | import vfs_v2 |
| `crates/studiofs-fuse/src/fs.rs` | 5 | import vfs_v2 |
| `crates/studiofs/src/cli/mount.rs` | 5 | import vfs_v2 |
| `crates/studiofs-meta/src/types.rs` | 5e | +format_version |

---

## Критерии готовности (Definition of Done)

- [ ] `cargo test --workspace` — все тесты проходят
- [ ] `cargo clippy --workspace -- -D warnings` — нет предупреждений
- [ ] DiskCache hit throughput ≥ 1.5 ГБ/с (замерено на диске 3.5+ ГБ/с)
- [ ] Sequential read throughput ≥ 500 МБ/с (при наличии кэша)
- [ ] Network requests per 4 МБ block = 1 (no read amplification)
- [ ] Существующие файлы на VFS v1 — детектируются, выдаётся понятная ошибка миграции
