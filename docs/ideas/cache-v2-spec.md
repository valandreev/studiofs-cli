# StudioFS Cache v2 — Спецификация и план реализации

**Дата:** 2026-03-31  
**Статус:** Draft  
**Автор:** AI-assisted design

---

## TL;DR

Полная замена текущей двухуровневой системы кеширования (L1 memory + L2 disk + AIMD + spill WAL) на **единый файловый кеш** с управлением RAM через OS page cache. Один файл данных, внутренний аллокатор, mmap для чтения, pwrite для записи. Конфигурация для пользователя: **размер + директория**. Всё остальное — автоматика.

Одновременно упрощается write pipeline: текущие 8 стадий (write_buffer → dirty_mem → spill → materializer → L1 → L2 commit → upload → S3) сокращаются до 4 (WriteBuffer → BlockStore.put → UploadQueue → S3).

---

## 1. Цели и принципы

### 1.1 Цели

| # | Цель | Метрика |
|---|------|---------|
| G1 | Максимальная скорость последовательного чтения | ≥80% пропускной способности накопителя |
| G2 | Единая логика для Linux / macOS / Windows | Один code path, платформенные оптимизации через conditional compilation |
| G3 | Эффективное использование места | Кеш ≈ реальный объём кешированных данных (±5%) |
| G4 | Простая конфигурация | 2 параметра: `cache_size`, `cache_dir` |
| G5 | Нет занятия места при пустом кеше | Файл растёт по мере наполнения |
| G6 | Корректное вытеснение | LRU, без TTL, удалённые файлы — lazy eviction |
| G7 | Архитектура поддерживает пиннинг | Без перепроектирования, реализация позже |

### 1.2 Принципы

1. **OS page cache = L1**. Отказ от явного in-memory тира. ОС управляет тем, какие страницы кеш-файла находятся в RAM.
2. **Один файл данных**. Без миллионов мелких файлов, без inode overhead. Внутренний аллокатор управляет пространством.
3. **mmap для чтения**. Минимум копирований: mmap → FUSE reply buffer (одна копия вместо двух).
4. **pwrite для записи**. Контроль над тем, когда данные попадают на диск. Совместимость с mmap (POSIX coherency).
5. **Скорость чтения > скорости записи**. Медиапроизводство: файлы от десятков МБ до сотен ТБ, последовательное чтение — основной сценарий.

### 1.3 Что уходит

| Компонент | Причина удаления |
|-----------|-----------------|
| L1MemCache (64 shards HashMap) | Заменяется OS page cache |
| L2DiskCache (file-per-block) | Заменяется единым файлом данных |
| AIMD controller | Не нужен без явного L1 |
| LruTracker (текущий, sharded Vec) | Заменяется новым LRU в составе BlockStore |
| Ingress WAL / spill | Dirty данные идут напрямую в кеш-файл |
| Materializer | Логика упрощена: flush = put в кеш + submit upload |
| DirtyMemIndex | Заменяется write buffer → cache put |
| cache.meta checkpoint | Заменяется snapshot + WAL индекса |
| L2 profiles (SSD/HDD) | Один режим, автоматика |

---

## 2. Архитектура

### 2.1 Обзор компонентов

```
┌─────────────────────────────────────────────────────────────────┐
│                          VFS                                     │
│  ┌─────────────┐  ┌────────────┐  ┌───────────┐  ┌───────────┐ │
│  │ WriteBuffer  │  │  ReadAhead  │  │ UploadMgr │  │ ExtentMap │ │
│  │ (per-file,   │  │ (sequential │  │ (bounded  │  │ (ino+blk  │ │
│  │  in-memory)  │  │  detection) │  │  channel) │  │  →obj_id) │ │
│  └──────┬───────┘  └─────┬──────┘  └─────┬─────┘  └───────────┘ │
│         │ put()          │ get()         │ get()                  │
│         │                │ contains()    │                        │
└─────────┼────────────────┼───────────────┼───────────────────────┘
          ▼                ▼               ▼
┌─────────────────────────────────────────────────────────────────┐
│                     BlockStore (новый кеш)                       │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │  MmapReader   │  │    Index      │  │     Allocator          │ │
│  │  (ArcSwap<    │  │  (HashMap     │  │  (FreeList: BTreeMap   │ │
│  │   Mmap>)      │  │   + LRU list) │  │   best-fit + coalesce) │ │
│  └──────┬────────┘  └──────┬───────┘  └──────────┬─────────────┘ │
│         │                  │                      │               │
│         └──────────────────┴──────────────────────┘               │
│                            │                                      │
│                     ┌──────┴──────┐                               │
│                     │  Data File   │                               │
│                     │ (blocks.data)│                               │
│                     └─────────────┘                               │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 OS Page Cache как L1

```
Приложение (read) → FUSE → VFS → BlockStore.get()
                                    │
                                    ▼
                              ┌──────────┐
                              │   mmap    │──── page fault ────→ [NVMe read]
                              │  (виртуальная память)         │
                              └──────────┘                    │
                                    │                         ▼
                              ┌──────────┐           ┌──────────────┐
                              │ Page Cache│◄──────────│ blocks.data  │
                              │ (RAM, OS) │           │  (NVMe/USB)  │
                              └──────────┘           └──────────────┘
```

- **Горячие блоки** попадают в page cache ОС автоматически после первого чтения
- **Вытеснение из RAM** управляется ОС (LRU page replacement)
- **Prefetch из page cache** управляется ОС (`readahead`, `madvise`)
- **Нет AIMD**, нет ручного управления RAM-бюджетом
- На 16 ГБ машине ОС отдаст под page cache 8–12 ГБ (всё что не занято процессами)

---

## 3. Формат хранения

### 3.1 Файловая структура кеш-директории

```
{cache_dir}/
├── blocks.data           # Единый файл данных (все блоки)
├── blocks.idx            # Снапшот индекса (бинарный)
├── blocks.wal            # WAL индекса (append-only, с последнего снапшота)
└── pins.json             # Конфигурация пиннинга (будущее)
```

4 файла. Никаких поддиректорий, никаких тысяч мелких файлов.

### 3.2 Файл данных (blocks.data)

**Superblock** (первые 4096 байт):

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | `magic` | `b"STFSCA\x01\x00"` |
| 8 | 4 | `version` | `1u32` (little-endian) |
| 12 | 4 | `page_size` | `4096u32` (alignment unit) |
| 16 | 8 | `max_size` | Максимальный размер кеша (байт) |
| 24 | 8 | `created_at` | Unix timestamp создания |
| 32 | 16 | `volume_uuid` | UUID тома |
| 48 | 4048 | `_reserved` | Zero-filled |

**Data region** (offset 4096+):

Блоки хранятся последовательно, каждый с компактным заголовком:

```
┌──────────────────────────────────────────────────┐
│ Block Header (32 bytes)                           │
│  magic: [u8; 4]     = b"SBLK"                   │
│  flags: u8           (DIRTY=0x01, PINNED=0x02)   │
│  _reserved: [u8; 3]                              │
│  object_id: u64                                   │
│  data_size: u32      (actual payload bytes)       │
│  checksum: u32       (xxhash32 of payload)        │
│  _padding: [u8; 8]                               │
├──────────────────────────────────────────────────┤
│ Payload (data_size bytes)                         │
├──────────────────────────────────────────────────┤
│ Padding (to next 4KB boundary)                    │
└──────────────────────────────────────────────────┘
```

**Размер аллокации** = `ceil((32 + data_size) / 4096) * 4096`

Примеры:

| Блок | data_size | Аллокация | Waste | Waste % |
|------|-----------|-----------|-------|---------|
| 1 KB файл | 1024 | 4 KB | 3 KB | 74% (3 KB абс.) |
| 100 KB файл | 102400 | 104 KB | ~1.6 KB | 1.5% |
| 1 MB файл | 1048576 | 1048 KB + 4 KB | ~3.5 KB | 0.3% |
| 16 MB блок (основной) | 16777216 | 16 MB + 4 KB | 4 KB | 0.024% |

Для доминантного сценария (большие медиафайлы, блоки 16 МБ) — overhead 4 КБ на блок (0.024%).

Для "10 000 файлов по 1 КБ" — 40 МБ кеша (вместо 40 ГБ при фиксированных 4 МБ слотах).

### 3.3 Зачем заголовки в файле данных

1. **Восстановление индекса** — сканируя файл, можно восстановить полный индекс
2. **Обнаружение повреждений** — magic + checksum
3. **Отладка** — можно hex-dump и увидеть содержимое кеша
4. **Стоимость** — 32 байта на блок. Для 10 ТБ кеша (625K блоков): 20 МБ overhead

### 3.4 Индекс

**In-memory структура:**

```rust
pub struct BlockIndex {
    /// ObjectId → запись о блоке
    entries: HashMap<u64, BlockEntry>,
    
    /// Doubly-linked list: head = oldest (кандидат на вытеснение)
    lru: LruList,                       
    
    /// Свободные регионы в файле данных
    allocator: FreeList,                
    
    /// Грязные (не загруженные в S3) блоки — не вытесняются
    dirty_count: u32,
}

pub struct BlockEntry {
    offset: u64,          // смещение в blocks.data
    data_size: u32,       // реальный размер данных
    alloc_size: u32,      // аллоцированный размер (4KB-aligned)
    flags: BlockFlags,    // DIRTY | PINNED | UPLOADING
    lru_handle: LruNode,  // позиция в LRU-списке
}

bitflags! {
    pub struct BlockFlags: u8 {
        const DIRTY     = 0x01;  // не загружен в S3
        const PINNED    = 0x02;  // закреплён пользователем (будущее)
        const UPLOADING = 0x04;  // идёт загрузка в S3
    }
}
```

**Размер в памяти:** ~80 байт на запись. Для 10 ТБ кеша (625K блоков): ~50 МБ RAM.  
Для 100 ГБ кеша (~6K блоков): ~0.5 МБ RAM.

**Персистентность (Snapshot + WAL):**

**Snapshot** (`blocks.idx`) — полный дамп всех записей:

```
[8 bytes: magic b"STFSI\x01\x00\x00"]
[4 bytes: entry_count]
[4 bytes: checksum (xxhash32 of all entries)]
For each entry:
  [8 bytes: object_id]
  [8 bytes: offset]
  [4 bytes: data_size]
  [4 bytes: alloc_size]
  [1 byte: flags]
  [3 bytes: padding]
= 28 bytes per entry
```

Для 625K записей: ~17 МБ. Запись/чтение < 100 мс на NVMe.

**WAL** (`blocks.wal`) — append-only лог операций с последнего snapshot:

```
Per entry:
  [1 byte: op (PUT=1, REMOVE=2, UPDATE_FLAGS=3)]
  [3 bytes: padding]
  [8 bytes: object_id]
  [8 bytes: offset]        // для PUT
  [4 bytes: alloc_size]    // для PUT
  [4 bytes: data_size]     // для PUT
= 28 bytes per WAL entry
```

**Стратегия персистентности:**

| Событие | Действие |
|---------|----------|
| `put()` / `remove()` | Append WAL entry (быстро, sequential write) |
| Каждые 30 секунд | Записать полный snapshot, truncate WAL |
| Clean shutdown | Записать snapshot, удалить WAL |
| Crash recovery | Загрузить snapshot + replay WAL |
| Corrupt snapshot + WAL | Пересканировать blocks.data по заголовкам (медленно, fallback) |

**WAL не требует fdatasync:** потеря WAL при крэше ≈ потеря записей после последнего snapshot ≈ несколько "orphaned" блоков в файле данных. Они будут перезаписаны при следующей аллокации или обнаружены при полном сканировании. Это допустимо (Variant A).

---

## 4. Внутренние компоненты

### 4.1 Аллокатор свободного пространства

**Тип:** Best-Fit с коалесценцией смежных свободных регионов.

```rust
pub struct FreeList {
    /// offset → size: для коалесценции смежных регионов
    by_offset: BTreeMap<u64, u64>,
    
    /// (size, offset): для best-fit поиска
    by_size: BTreeMap<(u64, u64), ()>,
    
    /// Текущий логический конец файла данных
    file_end: u64,
    
    /// Лимит (задан пользователем)
    max_size: u64,
    
    /// Физически занято (allocated - freed, без учёта hole-punch)
    allocated_bytes: u64,
}
```

**`allocate(size: u32) → Result<u64>`:**

1. `alloc_size = ceil((32 + size) / 4096) * 4096`
2. Ищем в `by_size` наименьший регион `≥ alloc_size` (best-fit)
3. **Найден:** разрезаем регион, возвращаем аллокацию, остаток — назад в free list
4. **Не найден:** проверяем `file_end + alloc_size ≤ max_size`
   - Да: расширяем файл на `max(alloc_size, GROWTH_CHUNK)`, добавляем излишек в free list
   - Нет: вызываем eviction, повторяем
5. Возвращаем offset в файле данных

**`free(offset: u64, alloc_size: u32)`:**

1. Добавляем регион (offset, alloc_size) в free list
2. **Коалесценция:** проверяем соседей в `by_offset`:
   - Предыдущий регион заканчивается на `offset`? → объединяем
   - Следующий регион начинается с `offset + alloc_size`? → объединяем
3. **Hole punch** (платформо-зависимо, опционально):
   - Linux: `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)`
   - macOS: `fcntl(F_PUNCHHOLE)`
   - Windows: `FSCTL_SET_ZERO_DATA` (sparse file)
4. **Truncate tail:** если освобождённый регион — хвост файла, вызываем `ftruncate`

**Константы:**

| Параметр | Значение | Обоснование |
|----------|----------|-------------|
| `GROWTH_CHUNK` | 256 МБ | Уменьшает количество расширений файла |
| `PAGE_SIZE` | 4 КБ | Alignment unit, совпадает с FS page size |

### 4.2 LRU-трекер

**Тип:** Intrusive doubly-linked list.

```rust
pub struct LruList {
    /// Указатель на самый старый элемент (кандидат на вытеснение)
    head: Option<NonNull<LruNode>>,
    
    /// Указатель на самый свежий элемент
    tail: Option<NonNull<LruNode>>,
    
    /// Количество элементов
    len: usize,
}

pub struct LruNode {
    object_id: u64,
    prev: Option<NonNull<LruNode>>,
    next: Option<NonNull<LruNode>>,
}
```

**Операции:**

| Операция | Когда | Сложность |
|----------|-------|-----------|
| `touch(node)` | Каждый `get()` — перемещение в tail | O(1) |
| `push_back(node)` | Каждый `put()` — вставка в tail | O(1) |
| `remove(node)` | `remove()`, eviction | O(1) |
| `pop_front_evictable()` | Eviction — пропускает DIRTY/PINNED/UPLOADING | O(k), k = кол-во пропущенных |

**Debouncing:** `touch()` игнорирует вызовы если с последнего touch прошло < 1 секунды. Для последовательного чтения (блоки 0,1,2,3...) каждый блок читается один раз — debouncing практически не срабатывает. Для повторных чтений горячих блоков — снижает overhead.

**Почему НЕ CLOCK/CLOCK-2:**
При максимуме 2.5M блоков (10 ТБ / 4 КБ min alloc — крайний случай) и 16 МБ типичных блоках (625K записей), LRU linked list работает эффективно. CLOCK экономит память (нет prev/next указателей) но даёт менее точное вытеснение. Для медиа-нагрузки с чётким sequential паттерном, точный LRU лучше.

### 4.3 MmapReader

**Проблема:** файл данных растёт. mmap создан для размера N, но данные записаны за пределами N.

**Решение:** ArcSwap-based remapping.

```rust
pub struct MmapReader {
    /// Текущий mmap. Атомарно заменяется при расширении файла.
    /// Читатели клонируют Arc — старый mmap живёт пока есть читатели.
    current: arc_swap::ArcSwap<MmapGeneration>,
    
    /// Файловый дескриптор (для pread fallback)
    fd: RawFd,
}

struct MmapGeneration {
    mmap: memmap2::Mmap,
    size: u64,
}
```

**Читатель:**

```rust
fn read(&self, offset: u64, size: u32) -> Bytes {
    let gen = self.current.load();  // Arc clone, lock-free
    let slice = &gen.mmap[offset as usize..(offset + size as u64) as usize];
    Bytes::copy_from_slice(slice)   // одна копия: page cache → heap
}
```

**Рост файла:**

```rust
fn remap(&self, new_size: u64) {
    // Создаём новый mmap для нового размера
    let new_mmap = unsafe { Mmap::map(&self.file)? };
    let new_gen = Arc::new(MmapGeneration { mmap: new_mmap, size: new_size });
    self.current.store(new_gen);
    // Старый mmap автоматически освобождается когда последний читатель отпустит Arc
}
```

**Частота remapping:** при `GROWTH_CHUNK = 256 МБ`, для заполнения 10 ТБ кеша потребуется ~40K remappings. При скорости записи 2 ГБ/с — один remap каждые 128 мс. Стоимость remap: ~1 мс (munmap + mmap syscall). Допустимо.

**Fallback для чтения за пределами текущего mmap:**
Если блок записан в область файла, ещё не покрытую mmap (race между pwrite и remap), используем `pread()`. Это редкий случай (только для самых свежих блоков до следующего remap).

---

## 5. API BlockStore

### 5.1 Публичный интерфейс

```rust
/// Единый дисковый кеш блоков.
/// Потокобезопасен. Один экземпляр на mount.
pub struct BlockStore { ... }

impl BlockStore {
    /// Создать или открыть кеш.
    /// Загружает индекс (snapshot + WAL replay).
    pub fn open(config: BlockStoreConfig) -> Result<Self>;
    
    /// Прочитать блок из кеша.
    /// Возвращает None при cache miss.
    /// Обновляет LRU позицию (touch).
    pub fn get(&self, object_id: u64) -> Option<Bytes>;
    
    /// Проверить наличие блока без чтения данных.
    /// Не обновляет LRU (чтение метаданных дешевле).
    pub fn contains(&self, object_id: u64) -> bool;
    
    /// Записать блок в кеш.
    /// evictable=false для dirty блоков (ещё не в S3).
    /// При нехватке места — вытесняет LRU блоки.
    /// Возвращает ошибку если кеш переполнен и вытеснять нечего.
    pub fn put(&self, object_id: u64, data: &[u8], evictable: bool) -> Result<()>;
    
    /// Пометить блок как evictable (после успешной загрузки в S3).
    pub fn mark_evictable(&self, object_id: u64);
    
    /// Удалить блок явно (файл удалён из VFS).
    pub fn remove(&self, object_id: u64);
    
    /// Удалить несколько блоков (batch, для удаления файла).
    pub fn remove_batch(&self, object_ids: &[u64]);
    
    /// Вызвать fdatasync на файле данных.
    /// VFS вызывает после записи dirty блоков.
    pub fn sync_data(&self) -> Result<()>;
    
    /// Graceful shutdown: записать snapshot, закрыть файлы.
    pub fn close(self) -> Result<()>;
    
    // --- Метрики ---
    
    /// Текущий размер кеша (аллоцировано)
    pub fn allocated_bytes(&self) -> u64;
    
    /// Количество блоков в кеше
    pub fn block_count(&self) -> u64;
    
    /// Количество dirty (не-evictable) блоков
    pub fn dirty_count(&self) -> u32;
    
    /// Hit/miss статистика
    pub fn hit_count(&self) -> u64;
    pub fn miss_count(&self) -> u64;

    // --- Будущее: Пиннинг ---
    
    /// Пометить блок как pinned (не вытесняется).
    pub fn pin(&self, object_id: u64);
    
    /// Снять pin.
    pub fn unpin(&self, object_id: u64);
    
    /// Общий размер pinned данных.
    pub fn pinned_bytes(&self) -> u64;
}
```

### 5.2 Конфигурация

```rust
pub struct BlockStoreConfig {
    /// Директория для файлов кеша
    pub cache_dir: PathBuf,
    
    /// Максимальный размер кеша (байт). Единственный user-facing параметр.
    pub max_size: u64,
    
    /// UUID тома (для валидации при открытии)
    pub volume_uuid: [u8; 16],
}
```

Всё остальное — внутренние константы с разумными дефолтами:

| Константа | Значение | Описание |
|-----------|----------|----------|
| `PAGE_SIZE` | 4096 | Alignment unit |
| `GROWTH_CHUNK` | 256 МБ | Шаг роста файла данных |
| `SNAPSHOT_INTERVAL` | 30 сек | Периодичность полного snapshot индекса |
| `EVICTION_HIGH_WM` | 0.95 | Порог начала вытеснения |
| `EVICTION_LOW_WM` | 0.85 | Порог остановки вытеснения |
| `LRU_TOUCH_DEBOUNCE` | 1 сек | Минимальный интервал между touch для одного блока |
| `PIN_MAX_RATIO` | 0.80 | Максимальная доля кеша под pinned данные |
| `REMAP_THRESHOLD` | 64 МБ | Если записано > N за пределами mmap → remap |

---

## 6. Путь чтения

### 6.1 Общий поток

```
App read(fd, offset, size)
│
▼
FUSE/WinFSP callback
│
▼
VFS do_read(ino, offset, size)
│
├─ 1. Overlay check: WriteBuffer (unflushed data in memory)
│     └─ Есть? → return данные из памяти
│
├─ 2. Determine block range: offset → [(block_index, offset_in_block, len), ...]
│
├─ 3. For each block, resolve ObjectId from ExtentMap
│
├─ 4. For each block, parallel:
│     │
│     ├─ block_store.get(object_id)
│     │   ├─ HIT: return Bytes (mmap → copy → Bytes, одна копия)
│     │   │        OS page cache обслужит горячие блоки из RAM
│     │   │
│     │   └─ MISS: return None
│     │         │
│     │         ▼
│     │     fetch_from_s3(object_id)
│     │         │
│     │         ▼
│     │     block_store.put(object_id, data, evictable=true)
│     │         │
│     │         ▼
│     │     return data
│     │
│     └─ Submit readahead hints for upcoming blocks
│
▼
Assemble response, copy to FUSE reply buffer
```

### 6.2 Двухуровневый readahead

**Уровень 1: VFS ReadAhead (S3 → Cache)**
- Существующий `readahead.rs` модуль (sequential detection, velocity EWMA)
- Для блоков **не в кеше**: асинхронный fetch из S3 → `block_store.put()`
- Таргетирует блоки, которых ещё нет в кеше

**Уровень 2: OS ReadAhead (NVMe → RAM)**
- Автоматический: kernel видит sequential page faults на mmap'd файле
- Усиливается через hints:
  - Linux: `madvise(MADV_SEQUENTIAL)` на весь mmap
  - Linux: `posix_fadvise(FADV_WILLNEED, offset, size)` для конкретных блоков
  - macOS: `fcntl(F_RDADVISE, &radvisory)` для конкретных регионов
  - Windows: `FILE_FLAG_SEQUENTIAL_SCAN` при открытии

**Интеграция:**

```
VFS ReadAhead обнаруживает seq. паттерн
│
├── Для следующих N блоков:
│   ├── block_store.contains(object_id)?
│   │   ├── Да: platform_fadvise(offset, size)  ← подсказка OS: загрузи в RAM
│   │   └── Нет: spawn S3 fetch → block_store.put()
│   └── ...
```

Два уровня readahead **дополняют друг друга**:
- VFS readahead заполняет кеш с данными, которых на диске нет
- OS readahead загружает данные с диска в RAM до того, как приложение их запросит

### 6.3 Производительность чтения

**Оценка для sequential read (горячие данные в кеше):**

| Стадия | Операция | Стоимость |
|--------|----------|-----------|
| Index lookup | HashMap get | ~50 нс |
| ArcSwap load | Arc clone | ~20 нс |
| mmap read | Если в page cache: memcpy 16МБ | ~2 мс |
| mmap read | Если не в page cache: page fault + NVMe read | ~3-5 мс (NVMe), ~15 мс (USB NVMe) |
| FUSE copy | memcpy to reply buffer | ~2 мс |
| **Итого (hot)** | | **~4 мс / 16МБ = ~4 ГБ/с** |
| **Итого (cold NVMe)** | | **~7 мс / 16МБ = ~2.3 ГБ/с** |
| **Итого (cold USB)** | | **~19 мс / 16МБ = ~840 МБ/с** |

С prefetch (OS readahead + VFS readahead), cold → warm за 1-2 блока вперёд. Effective throughput ≈ скорость NVMe.

---

## 7. Новый Write Pipeline

### 7.1 Сравнение с текущим

**Текущий (8+ стадий):**
```
write → write_buffer → dirty_mem → spill (WAL) → materializer → L1 stage
→ L2 commit (async) → upload queue → S3 → epoch finalization
```

**Новый (4 стадии):**
```
write → WriteBuffer → BlockStore.put() + fdatasync → upload queue → S3
```

| Аспект | Текущий | Новый |
|--------|---------|-------|
| In-memory буферизация | write_buffer + dirty_mem | WriteBuffer только |
| Промежуточное хранилище | Spill WAL (отдельные файлы) | Нет, данные сразу в кеш-файл |
| Staging | L1 + async L2 commit | Один put() |
| Durability dirty data | Spill WAL (durable mode) | fdatasync кеш-файла |
| Overlay стадий для read | 4 (write_buffer, dirty_mem, live_extents, spill) | 2 (write_buffer, cache) |
| Materializer | Отдельный background task | Логика inline в flush |

### 7.2 Поток записи

```
App write(fd, buf, size)
│
▼
FUSE/WinFSP callback
│
▼
VFS write(ino, offset, buf)
│
├─ Разбиение на блоки: offset → [(block_index, offset_in_block, len), ...]
│
├─ Для каждого блока:
│   │
│   ├─ WriteBuffer.write(ino, block_index, offset_in_block, data)
│   │   └─ Накапливает в BytesMut (per-file, per-block_index)
│   │
│   ├─ Если блок полностью заполнен (block_size байт накоплено):
│   │   └─ WriteBuffer.flush_block(ino, block_index)  ← eager flush полного блока
│   │
│   └─ Backpressure: если WriteBuffer.total_bytes > max_write_buffer
│       └─ flush_oldest_file()  ← освобождаем память
│
▼
return bytes_written
```

### 7.3 Flush / Fsync

```
VFS flush(ino) / fsync(ino)
│
├─ blocks = WriteBuffer.drain(ino)  // все pending блоки этого файла
│
├─ Batch processing:
│   │
│   ├─ object_ids = MetaEngine.allocate_block_ids(blocks.len())
│   │
│   ├─ For each (object_id, block_data):
│   │   ├─ BlockStore.put(object_id, block_data, evictable=false)
│   │   └─ ExtentMap.update(ino, block_index, object_id)
│   │
│   ├─ MetaEngine.flush_extent_map(ino)  // persist to redb
│   │
│   ├─ BlockStore.sync_data()  // fdatasync — dirty data теперь on-disk
│   │
│   └─ For each object_id:
│       └─ UploadQueue.submit(ino, block_index, object_id)
│
▼
return Ok(())  // данные durable локально, upload async
```

**Порядок операций для crash safety:**

1. `pwrite()` блоков в кеш-файл (данные на диске, но не guaranteed durable)
2. `fdatasync()` кеш-файла → **данные durable**
3. `redb commit` → **метаданные durable**, ссылаются на durable данные
4. Submit uploads → async

Crash в любой точке:
- До шага 2: данные могут не быть на диске → потеря записи (приложение не получило OK)
- Между 2 и 3: данные на диске, но metadata не обновлена → orphaned блоки (безопасно)
- После 3: всё durable → upload подхватится при рестарте

### 7.4 Read-Write overlap

**Чтение блока, который сейчас в WriteBuffer:**
```
VFS read(ino, offset, size)
│
├─ 1. WriteBuffer.get(ino, block_index)?
│     └─ Есть? → return данные из памяти (самые свежие)
│
├─ 2. BlockStore.get(object_id)?
│     └─ Есть? → return из кеша (committed данные)
│
└─ 3. S3 fetch → return
```

Только два уровня overlay вместо четырёх. Проще, быстрее, меньше шансов на баг.

### 7.5 WriteBuffer

```rust
pub struct WriteBuffer {
    /// Per-file буферы. Ключ: ino
    files: HashMap<u64, FileBuffer>,
    
    /// Общий размер данных в буферах
    total_bytes: u64,
    
    /// Лимит (default: min(512 МБ, 10% RAM))
    max_bytes: u64,
}

struct FileBuffer {
    /// Per-block accumulation. Ключ: block_index
    blocks: HashMap<u32, BytesMut>,
    
    /// Размер данных этого файла
    file_bytes: u64,
}
```

**Backpressure стратегия:**
- `total_bytes > max_bytes`: flush файл с наибольшим file_bytes
- Если после flush всё ещё > max_bytes: flush следующий файл
- Если буфер не может drain (BlockStore.put fails): EAGAIN / ENOSPC к приложению

---

## 8. Вытеснение (Eviction)

### 8.1 Алгоритм

**Background evictor thread:**

```
loop {
    wait_for_notification_or_timeout(10 sec)
    
    if allocated_bytes() < max_size * EVICTION_HIGH_WM {
        continue  // ещё есть место
    }
    
    while allocated_bytes() > max_size * EVICTION_LOW_WM {
        match lru.pop_front_evictable() {
            Some(entry) => {
                // 1. Удаляем данные из файла
                allocator.free(entry.offset, entry.alloc_size)
                
                // 2. Hole punch (опционально, platform-dependent)
                platform_hole_punch(fd, entry.offset, entry.alloc_size)
                
                // 3. Удаляем из индекса
                index.remove(entry.object_id)
                
                // 4. WAL append
                wal.append(REMOVE, entry.object_id)
            }
            None => {
                // Все блоки dirty/pinned/uploading — нечего вытеснять
                warn!("cache full, all blocks non-evictable");
                break
            }
        }
    }
}
```

**Триггеры пробуждения evictor:**
1. `put()` обнаружил что `allocated_bytes > high_watermark`
2. Таймер 10 секунд (периодическая проверка)
3. `disk_free_check()` обнаружил мало свободного места на разделе

### 8.2 Lazy eviction при удалении файлов

**Локальное удаление:**
```
VFS unlink(ino)
│
├─ Получаем все object_ids для этого ino из ExtentMap
├─ BlockStore.remove_batch(object_ids)
│   └─ Для каждого: free space, hole punch, remove from index
└─ ExtentMap.clear(ino)
```

**Удаление другим пользователем (remote):**
```
MetadataSync обнаруживает: ino удалён
│
├─ VFS invalidate_inode(ino)
│   └─ Kernel cache invalidation (FUSE notify_inval_inode)
│
├─ ExtentMap: получаем object_ids
├─ BlockStore.remove_batch(object_ids)
└─ ExtentMap.clear(ino)
```

Это **lazy**: удалённые блоки вытесняются при следующем sync с PostgreSQL. Между sync-ами они занимают место в кеше, но это допустимо (секунды).

### 8.3 Emergency eviction

Если свободное место на разделе < 5%:
- Evictor переключается в aggressive mode
- Eviction до `EVICTION_LOW_WM * 0.5` (42.5%)
- Логирование: warn level

---

## 9. Управление загрузкой на S3

### 9.1 UploadQueue

Upload queue концептуально остаётся, но упрощается:

```rust
pub struct UploadQueue {
    /// Bounded channel для pending uploads
    tx: tokio::sync::mpsc::Sender<UploadJob>,
    rx: tokio::sync::mpsc::Receiver<UploadJob>,
    
    /// Текущие inflight uploads
    inflight: DashMap<u64, UploadJob>,
}

struct UploadJob {
    ino: u64,
    block_index: u32,
    object_id: u64,
    content_epoch: u64,
}
```

**Upload worker:**
```
loop {
    job = rx.recv()
    
    // Читаем данные из кеша (тот же файл что и для read)
    data = block_store.get(job.object_id)
        .expect("dirty block must be in cache")
    
    // Загружаем в S3
    match s3_upload(job, data).await {
        Ok(()) => {
            block_store.mark_evictable(job.object_id)
            // Epoch finalization...
        }
        Err(e) if e.is_transient() => {
            // Retry с backoff
            requeue(job, backoff)
        }
        Err(e) => {
            // Permanent error → report to VFS
            error!("upload failed permanently: {}", e)
        }
    }
}
```

### 9.2 Нет конфликтов read vs upload

**Текущая проблема:** Upload worker читает из L2 (отдельные файлы), конкурируя с foreground reads за файловые дескрипторы и I/O bandwidth.

**Решение в v2:** Upload worker и foreground reads оба используют `BlockStore.get()`:
- mmap даёт обоим доступ к одному и тому же page cache
- Нет отдельных fd для upload vs read
- Upload reads естественно low-priority: OS scheduler отдаёт приоритет foreground

**Дополнительный throttling:**
- `posix_fadvise(FADV_DONTNEED)` после upload read → подсказка OS не кешировать upload-данные
- Rate limiting: если foreground P90 latency > target → замедляем uploads

### 9.3 Persistence upload journal

Для crash recovery: какие блоки dirty и нуждаются в upload?

**Текущий подход:** `.pending` файлы. Уходит.

**Новый подход:** dirty flag в `BlockEntry.flags`. На restart:
1. Загрузить index (snapshot + WAL)
2. Найти все записи с `flags & DIRTY != 0`
3. Re-submit в UploadQueue

Данные этих блоков уже в кеш-файле (fdatasync'd перед redb commit). Просто re-upload.

---

## 10. Пиннинг (архитектура для будущего)

### 10.1 Дизайн (не реализуется сейчас, но архитектура поддерживает)

**Pin Registry:**
```rust
struct PinRegistry {
    /// Паттерн пути → конфигурация
    rules: Vec<PinRule>,
}

struct PinRule {
    /// Glob-паттерн: "/projects/current/**", "/media/assets/logo.png"
    pattern: String,
    /// Рекурсивно для директорий
    recursive: bool,
}
```

**Как работает:**
1. Пользователь добавляет pin rule через CLI/API
2. При `BlockStore.put()` — VFS проверяет: файл этого блока матчится на pin rule?
   - Да → `block_store.pin(object_id)`
   - Нет → обычный блок
3. При изменении файла другим пользователем:
   - MetadataSync обнаруживает новую версию
   - VFS fetch новой версии → `block_store.put(new_id, ...)` + pin
   - `block_store.remove(old_id)`
4. Pinned блоки не вытесняются (LRU evictor пропускает)

**Ограничение 80%:**
```
if pinned_bytes() + new_block_size > max_size * PIN_MAX_RATIO {
    // Не пиним этот блок — он evictable
    warn!("pin budget exceeded, block not pinned");
}
```

**Persistence:** `pins.json` хранит PinRule-ы. pin/unpin flags в BlockEntry сохраняются через snapshot/WAL.

### 10.2 Что нужно в v2 для поддержки пиннинга

Ничего специального. `BlockFlags::PINNED` уже предусмотрен в BlockEntry. `pin()` / `unpin()` методы в API. Evictor уже пропускает PINNED блоки. Достаточно добавить PinRegistry и CLI-команды позже.

---

## 11. Платформенные оптимизации

### 11.1 Общая логика (все платформы)

Один и тот же код для:
- Аллокатора (FreeList)
- Индекса (HashMap + LRU)
- BlockStore API
- Write pipeline
- Upload queue

### 11.2 Platform-specific оптимизации

| Операция | Linux | macOS | Windows |
|----------|-------|-------|---------|
| **Hole punch** | `fallocate(PUNCH_HOLE)` | `fcntl(F_PUNCHHOLE)` | `FSCTL_SET_ZERO_DATA` |
| **Preallocation** | `fallocate(0, offset, len)` | `fcntl(F_PREALLOCATE)` | `SetFileValidData()` |
| **Read ahead hint** | `posix_fadvise(WILLNEED)` | `fcntl(F_RDADVISE)` | N/A (OS auto) |
| **Sequential hint** | `madvise(SEQUENTIAL)` | `madvise(SEQUENTIAL)` | `FILE_FLAG_SEQUENTIAL_SCAN` |
| **Don't need hint** | `posix_fadvise(DONTNEED)` | `madvise(FREE)` | N/A |
| **mmap grow** | `mremap()` (fast) | munmap+mmap (safe) | UnmapView+MapView |

**Реализация через trait:**

```rust
trait PlatformOps {
    fn hole_punch(fd: RawFd, offset: u64, len: u64) -> io::Result<()>;
    fn preallocate(fd: RawFd, offset: u64, len: u64) -> io::Result<()>;
    fn advise_willneed(fd: RawFd, offset: u64, len: u64) -> io::Result<()>;
    fn advise_sequential(addr: *const u8, len: usize) -> io::Result<()>;
    fn advise_dontneed(fd: RawFd, offset: u64, len: u64) -> io::Result<()>;
}

#[cfg(target_os = "linux")]
struct LinuxOps;

#[cfg(target_os = "macos")]
struct MacOps;

#[cfg(target_os = "windows")]
struct WindowsOps;
```

Все вызовы — best-effort. Если hole_punch не поддерживается (старая FS) — фоллбэк: ничего не делаем, пространство остаётся в free list но не в OS free space.

---

## 12. Конфигурация

### 12.1 User-facing (2 параметра)

```toml
[cache]
size = "500GB"    # Максимальный размер кеша
dir = "/data/studiofs-cache"  # Директория
```

Или через CLI:
```bash
studiofs mount --cache-size 500GB --cache-dir /data/studiofs-cache ...
```

Или через env:
```
STUDIOFS_CACHE_SIZE=500000000000
STUDIOFS_CACHE_DIR=/data/studiofs-cache
```

**Всё.** Никаких профилей, L2-режимов, AIMD-параметров, breaker-таймаутов.

### 12.2 Internal defaults

| Параметр | Default | Обоснование |
|----------|---------|-------------|
| `max_write_buffer` | `min(512MB, 10% RAM)` | Баланс: достаточно для burst writes, не забирает всю RAM |
| `upload_workers` | 4 | Достаточно для saturate типичного S3 connection |
| `upload_channel_capacity` | 4096 | Pending uploads buffer |
| `snapshot_interval` | 30 sec | Index persistence frequency |
| `eviction_high_wm` | 0.95 | Начало eviction |
| `eviction_low_wm` | 0.85 | Остановка eviction |
| `growth_chunk` | 256 MB | Шаг роста файла |
| `disk_free_emergency` | 5% | Emergency eviction при мало места на разделе |
| `lru_touch_debounce` | 1 sec | Минимальный интервал touch |
| `fadvise_willneed` | true | Включён на платформах с поддержкой |

---

## 13. Целостность данных

### 13.1 Checksums

**XXH3-32** (xxhash, 32-bit) хранится в заголовке каждого блока.

| Операция | Стоимость | Обоснование |
|----------|-----------|-------------|
| Compute on write | ~0.3 мс / 16 МБ | XXH3: >50 ГБ/с, пренебрежимо |
| Verify on read | ~0.3 мс / 16 МБ | Опционально, по умолчанию включено |

**При обнаружении checksum mismatch:**
1. Логируем error с object_id и offset
2. Удаляем блок из кеша (remove)
3. Возвращаем cache miss → VFS re-fetch из S3
4. Инкрементируем `checksum_errors` счётчик

Это защищает от bit rot на диске, ошибок в I/O стеке, и повреждений кеш-файла.

### 13.2 Index integrity

Snapshot файл включает xxhash32 всех записей. Если checksum не совпадает при загрузке → пересканирование data file по заголовкам (fallback recovery).

---

## 14. Конкурентность и потокобезопасность

### 14.1 Lock structure

```
BlockStore
├── index: RwLock<BlockIndex>         ← HashMap + LRU
│   ├── Read lock: get(), contains()  ← параллельные чтения
│   └── Write lock: put(), remove(), evict()
│
├── mmap_reader: MmapReader           ← lock-free (ArcSwap)
│   └── get(): load Arc, read slice   ← параллельные чтения, zero lock
│
├── allocator: Mutex<FreeList>        ← только при allocate/free
│   └── Short critical section (~μs)
│
├── data_fd: RawFd                    ← pwrite() thread-safe
│   └── Concurrent pwrite to different offsets: safe (POSIX guarantee)
│
├── wal_fd: Mutex<File>               ← append-only, serialized
│   └── Short critical section (write ~28 bytes)
│
└── evictor: background thread        ← takes write lock on index
```

### 14.2 Типичный concurrency flow

**N параллельных чтений (FUSE threads):**
```
Thread 1: index.read() → mmap.load() → return Bytes     ← no locks held during I/O
Thread 2: index.read() → mmap.load() → return Bytes
Thread N: index.read() → mmap.load() → return Bytes
```

Read lock на index удерживается ~50нс (только HashMap lookup + LRU touch). mmap read — lock-free.

**Запись + параллельные чтения:**
```
Write thread: index.write() → allocator.lock() → pwrite() → wal.lock()
Read thread:  index.read()  → mmap.load() → return        ← не блокируется pwrite
```

Write lock на index удерживается только для insert (~100нс). pwrite выполняется без lock-а index. Читатели не блокируются записью.

### 14.3 Deadlock prevention

Строгий порядок захвата lock-ов:

```
1. index (RwLock)
2. allocator (Mutex)  
3. wal (Mutex)
```

Никогда не захватываем allocator перед index, или wal перед allocator.

---

## 15. Изменения в других компонентах

### 15.1 VFS (studiofs-vfs)

| Что меняется | Действие |
|-------------|----------|
| `BlockCache` usage | Заменить на `BlockStore` |
| `do_read()` overlay | 4 стадии → 2 стадии (WriteBuffer, BlockStore) |
| `flush_ordered()` → dirty_mem | Убрать. flush = drain WriteBuffer → BlockStore.put() |
| DirtyMemIndex | Убрать полностью |
| Spill/Ingress WAL | Убрать полностью |
| Materializer | Убрать. Логика inline в flush |
| memory_envelope.rs (AIMD) | Убрать |
| IngressState, DirtyBufferState | Убрать из VfsState decomposition |
| ReadAhead | Сохранить, адаптировать API (contains + fadvise hints) |
| WriteBufferState | Rework: simpler per-file accumulation |

**Ожидаемое сокращение:** ~3000-5000 строк VFS-кода (dirty_mem, spill, materializer, memory_envelope).

### 15.2 FUSE bridge (studiofs-fuse)

| Параметр | Текущее | Новое | Обоснование |
|----------|---------|-------|-------------|
| `max_read` | 1 МБ | 16 МБ | Один FUSE request = один полный блок |
| `max_write` | (текущее) | 16 МБ | Аналогично |

### 15.3 WinFSP bridge

Аналогичные изменения в buffer sizes. BlockStore API одинаков для всех платформ.

### 15.4 studiofs-cache crate

**Полностью переписывается.** Текущее содержимое:

```
block_cache.rs    → Уходит
l1_mem.rs         → Уходит
l2_disk.rs        → Уходит
lru_tracker.rs    → Уходит (заменяется новым)
upload_queue.rs   → Рефакторится (упрощается)
metrics.rs        → Адаптируется
runtime.rs        → Уходит
platform_direct_io.rs → Уходит (не нужен: не используем O_DIRECT)
```

Новое содержимое:

```
lib.rs            → Re-export
block_store.rs    → BlockStore struct + impl
allocator.rs      → FreeList
index.rs          → BlockIndex + BlockEntry + persistence (snapshot/WAL)
lru.rs            → LruList + LruNode
mmap_reader.rs    → MmapReader (ArcSwap + Mmap)
platform.rs       → PlatformOps trait + conditional impls
write_buffer.rs   → WriteBuffer (может жить в VFS или cache)
evictor.rs        → Background eviction thread
metrics.rs        → Counters, histograms
```

---

## 16. Фазы реализации

### Phase 1: Core BlockStore (foundation)

**Scope:**
- `BlockStore::open()` / `close()`
- `put()` / `get()` / `remove()` — с pread/pwrite (без mmap)
- FreeList allocator
- BlockIndex (HashMap, in-memory only, без persistence)
- LRU eviction (basic)
- Файл данных с superblock + block headers
- Unit tests

**Проверка:** standalone тесты — put/get/remove/evict циклы, concurrent access.

**Зависимости:** нет внешних, новый код в studiofs-cache.

### Phase 2: Index Persistence + mmap reads

**Scope:**
- Snapshot (blocks.idx) write/load
- WAL (blocks.wal) append/replay
- Startup recovery (load snapshot + replay WAL)
- MmapReader (ArcSwap, remap on growth)
- Замена pread → mmap для get()
- Platform hole punch (Linux/macOS/Windows)

**Проверка:** crash simulation (kill process, restart, verify index consistency), mmap correctness.

### Phase 3: VFS Integration — Read Path

**Scope:**
- Заменить `BlockCache.get()` → `BlockStore.get()` в VFS do_read
- Заменить `BlockCache.get_sync()` → `BlockStore.get()`
- Адаптировать ReadAhead: `contains()` + fadvise hints
- Убрать L1MemCache usage из VFS
- Убрать AIMD controller (memory_envelope.rs)
- Увеличить FUSE max_read до 16 МБ

**Проверка:** end-to-end read test (mount → read file → verify data). Performance benchmark.

### Phase 4: VFS Integration — Write Path

**Scope:**
- Новый WriteBuffer (упрощённый)
- Flush: WriteBuffer → BlockStore.put() → UploadQueue
- Убрать dirty_mem, spill/WAL, materializer
- Убрать DirtyBufferState, IngressState из VfsState
- Упростить read overlay (2 стадии вместо 4)
- Адаптировать upload queue

**Проверка:** end-to-end write test (mount → write → read back → verify). Upload correctness test. Crash-during-write test.

### Phase 5: Performance & Polish

**Scope:**
- Performance profiling + optimization
- Platform-specific fadvise/madvise hints
- Metrics (hit rate, eviction rate, write throughput, read latency)
- Emergency disk free eviction
- Checksum verification toggle
- Cleanup: удалить весь старый код (L1, L2, AIMD, spill, materializer)

**Проверка:** benchmark suite — sequential read, random read, write throughput, mixed workload.

### Phase 6 (будущее): Pinning

**Scope:**
- PinRegistry + CLI commands
- Pin/unpin logic in VFS
- "Подгружай при изменении" — integration с metadata sync

---

## 17. Риски и open questions

### 17.1 Риски

| Риск | Impact | Mitigation |
|------|--------|------------|
| mmap remap latency spike | Кратковременная пауза при remap (1-2 мс) | ArcSwap обеспечивает lock-free read; pread fallback для in-flight блоков |
| Free list fragmentation (10 ТБ кеш, mixed sizes) | Невозможность аллоцировать большой блок при наличии мелких дыр | Periodic compaction: читаем блок, освобождаем, пишем в другое место |
| Page cache pressure на 16 ГБ машине | OS вытесняет кеш-страницы, каждый read = disk I/O | Это inherent limit; текущее решение с L1 не лучше (AIMD борется за ту же RAM) |
| Hole punch не поддержан на старой FS | Файл данных не shrinks физически | Fallback: только free list tracking, truncate хвоста |
| Single data file corruption | Потеря всего кеша | Variant A: допустимо. Rebuild from S3. |

### 17.2 Open questions

1. **Нужно ли сжатие блоков в кеше?** Сейчас блоки хранятся без сжатия. LZ4 даст ~2x для некоторых данных, но медиафайлы (видео, аудио) уже сжаты. Вероятно не стоит — overhead decompression при чтении снижает скорость.

2. **Фрагмент vs. блок в кеше.** Сейчас кеш хранит целые 16 МБ блоки. Альтернатива: хранить 4 МБ фрагменты (как в S3). Плюс: более гранулярное вытеснение, меньше fetch при обновлении части блока. Минус: 4x больше записей в индексе, 4x больше metadata per file block. **Рекомендация:** оставить 16 МБ как unit для простоты, пересмотреть если появятся use cases для 4 МБ гранулярности.

3. **Zero-copy Bytes из mmap.** Текущий дизайн: `Bytes::copy_from_slice()` — одна копия из mmap. Можно сделать true zero-copy через custom Bytes vtable (держит `Arc<MmapGeneration>`). Стоит реализовать, когда profiling покажет что copy — bottleneck.

4. **Direct I/O для upload reads.** Upload worker может использовать O_DIRECT чтобы не "загрязнять" page cache upload-данными. Но O_DIRECT не работает с mmap. Решение: отдельный fd с O_DIRECT только для upload reads (Linux only). Оптимизация для Phase 5.

---

## Appendix A: Сравнение с текущей архитектурой

| Аспект | Cache v1 (текущий) | Cache v2 (новый) |
|--------|-------------------|------------------|
| Файлы на диске | ~625K файлов (для 10 ТБ) | 4 файла |
| RAM management | Явный L1 + AIMD | OS page cache (автоматика) |
| Чтение: copy count | 2 (L2→buffer, buffer→FUSE) | 1 (mmap→FUSE) |
| Write pipeline стадии | 8 | 4 |
| VFS overlay стадии (read) | 4 | 2 |
| Конфигурация | 10+ параметров (L1 size, L2 size, profiles, breaker, timeouts) | 2 (size, dir) |
| Space efficiency | 1 KB файл = 1 KB на диске (OK), но 1 inode | 1 KB файл = 4 KB в data file |
| Lock contention | 64 shards + AIMD adjustments | RwLock + lock-free mmap reads |
| Startup time | Scan 256 dirs + reconcile | Load snapshot (~100 мс) |
| Crash recovery | cache.meta + FS reconcile | Snapshot + WAL replay |
| Cross-platform | O_DIRECT Linux only, regular I/O macOS | Единый код + platform traits |
| Code complexity | ~3500 строк (cache) + ~5000 (VFS pipeline) | ~2000 строк (cache) + ~2000 (VFS pipeline) |

## Appendix B: Зависимости (crates)

| Crate | Зачем |
|-------|-------|
| `memmap2` | mmap кросс-платформенно |
| `arc-swap` | Lock-free atomic Arc swap для mmap remapping |
| `xxhash-rust` | XXH3 checksums (быстрый, portable) |
| `bytes` | Bytes type для zero-copy sharing |
| `parking_lot` | Быстрые Mutex/RwLock |
| `bitflags` | BlockFlags |
| `tracing` | Логирование (уже используется) |

Никаких новых heavyweight зависимостей. `memmap2` и `arc-swap` — единственные новые.
