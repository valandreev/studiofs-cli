# StudioFS I/O Layer Redesign — Elastic Fetch/Upload Pipeline

## Status: PROPOSAL
## Date: 2026-02-24

---

## 1. Проблемы текущей архитектуры

### 1.1 Текущее состояние

Текущий I/O pipeline строит всё вокруг двух фиксированных OpenDAL `Operator`:

```
data_read  (IoProfile::Read):  io_timeout=20s, concurrent_limit=16, retries=3
data_write (IoProfile::Write): io_timeout=30s, concurrent_limit=16, retries=3
```

Проблемы:

| # | Проблема | Следствие |
|---|---------|-----------|
| 1 | **Cold-start stampede** | При первом чтении файла (десятки блоков) reqwest одновременно открывает 16+ TCP соединений. MinIO/S3 получает burst TLS-хендшейков → часть отклоняется → "error sending request" → OpenDAL ретраит быстро (200ms-3s) → все ретраи тоже fail → **файл не открывается** |
| 2 | **Фиксированный параллелизм** | `read_concurrency=16` всегда, независимо от состояния хранилища. Нет способа уменьшить когда S3 перегружен, нет способа увеличить когда он свободен |
| 3 | **Нет stream-first приоритизации** | Все блоки файла равноприоритетны. Для открытия файла (первый read) нужно дождаться любого блока, но мы стартуем fetch всех параллельно, засоряя пул |
| 4 | **Read amplification** | Каждый блок (1 MiB) скачивается целиком даже при small read. Нет HTTP Range оптимизации |
| 5 | **Нет backpressure от хранилища** | Upload семафор фиксирован (4). Нет адаптации к скорости S3 |
| 6 | **Костыльные обходные пути** | `FetchThrottle`, VFS-retry с фиксированными задержками — костыли поверх жёсткой архитектуры |

### 1.2 Как делает JuiceFS

JuiceFS подход (Go, pkg/chunk + pkg/vfs):

- **Fixed concurrency** (MaxDownload=200, MaxUpload=20) — но с **rate limiter** (token bucket)
- **Singleflight** — request dedup для одного блока (аналог наших inflight_fetches, но чище)
- **Adaptive readahead** — экспоненциальный рост (1×block → 2× → 4× → max), уменьшение при memory pressure
- **Range reads** для маленьких read (< blockSize/4) при некомпрессированных данных
- **Writeback staging** — запись сначала на NVMe, upload асинхронно с задержкой
- **Per-session tracking** — 2 сессии на файл, независимый readahead для каждого потока

**Что JuiceFS НЕ делает** (и мы можем лучше):
- Нет AIMD/slow-start — фиксированные лимиты, не адаптируется к нагрузке
- Нет приоритизации первого блока
- Нет эластичной конкурентности

---

## 2. Цели редизайна

### 2.1 Must-have (P0)

1. **Fast first-byte** — при открытии файла первый блок данных должен быть доставлен <500ms на warm connection, <2s на cold
2. **Elastic fetch concurrency** — AIMD-подобная адаптация: увеличиваем параллелизм при успешных fetch, уменьшаем при таймаутах/ошибках
3. **Elastic upload concurrency** — аналогичная адаптация для загрузки
4. **Zero-hang guarantee** — никогда не висеть бесконечно, всегда вернуть ошибку через конечное время
5. **Graceful degradation** — если S3 медленный/недоступен, работать с кешированными данными, чётко сообщать ошибки на uncached reads

### 2.2 Should-have (P1)

6. **HTTP Range reads** — для small reads (<256 KiB) из некомпрессированного блока, скачивать только нужный range
7. **Prioritized fetch queue** — первый блок файла имеет приоритет выше, чем readahead
8. **Streaming prefetch** — при стриминговом чтении подкачивать блоки ahead of cursor, но не впереди на много

### 2.3 Nice-to-have (P2)

9. **Multi-session readahead** — отдельный трекинг для 2+ независимых потоков чтения одного файла (как JuiceFS)
10. **Connection pool pre-warming** — фоновые keep-alive ping'и на idle соединения

---

## 3. Архитектура: Transfer Engine

### 3.1 Компоненты

```text
                          ┌──────────────────────────────────┐
                          │         Transfer Engine          │
                          │                                  │
                          │  ┌────────────┐  ┌────────────┐  │
       VFS read_block() ──┼─▶│  Fetch     │  │  Upload    │◀─┼── VFS drain_dirty_mem()
                          │  │  Scheduler │  │  Scheduler │  │
                          │  └──────┬─────┘  └──────┬─────┘  │
                          │         │               │        │
                          │  ┌──────▼───────────────▼─────┐  │
                          │  │     AIMD Controller        │  │
                          │  │  (shared or per-direction) │  │
                          │  └──────┬───────────────┬─────┘  │
                          │         │               │        │
                          │  ┌──────▼─────┐  ┌──────▼─────┐  │
                          │  │ HTTP Pool  │  │ HTTP Pool  │  │
                          │  │  (reads)   │  │ (writes)   │  │
                          │  └────────────┘  └────────────┘  │
                          └──────────────────────────────────┘
```

### 3.2 AIMD Controller

Ключевая идея — TCP-congestion-control–inspired adaptive concurrency:

```rust
/// Adaptive concurrency controller (AIMD — Additive Increase, Multiplicative Decrease).
///
/// Models the S3 endpoint as a "pipe" with unknown bandwidth. The window starts
/// small and grows on success, shrinks on failure — exactly like TCP cwnd.
pub struct AimdController {
    /// Current concurrency window (float for smooth growth).
    window: f64,
    /// Minimum window (never go below this).
    min_window: f64,
    /// Maximum window (never exceed this).
    max_window: f64,
    /// Additive increase per successful request.
    ///   window += increase_step  (default: 1.0)
    increase_step: f64,
    /// Multiplicative decrease factor on failure.
    ///   window *= decrease_factor  (default: 0.5)
    decrease_factor: f64,
    /// Semaphore that tracks the real permits.
    semaphore: Arc<Semaphore>,
    /// Permits currently issued to semaphore (to know when to add_permits).
    issued_permits: usize,
    /// Success/failure counters for observability.
    successes: u64,
    failures: u64,
    /// Cooldown: after a decrease, don't decrease again for this duration.
    last_decrease: Instant,
    decrease_cooldown: Duration,
}
```

**Алгоритм:**

```
on_success():
    window = min(window + increase_step, max_window)
    if floor(window) > issued_permits:
        semaphore.add_permits(floor(window) - issued_permits)
        issued_permits = floor(window)

on_failure():
    if now - last_decrease > decrease_cooldown:
        window = max(window * decrease_factor, min_window)
        // permits уменьшаются пассивно — не отбираем, просто не добавляем
        // когда текущие завершатся, issued_permits > floor(window),
        // новые acquire() будут ждать
        last_decrease = now
```

**Параметры по умолчанию:**

| Параметр | Fetch | Upload |
|----------|-------|--------|
| min_window | 1 | 1 |
| max_window | 32 | 8 |
| initial_window | 2 | 2 |
| increase_step | 1.0 | 0.5 |
| decrease_factor | 0.5 | 0.5 |
| decrease_cooldown | 5s | 5s |

### 3.3 Fetch Scheduler

```rust
pub struct FetchScheduler {
    /// AIMD controller for adaptive concurrency.
    aimd: Mutex<AimdController>,
    /// Priority queue: user-initiated reads > readahead.
    queue: PriorityQueue<FetchRequest>,
    /// Singleflight: dedup concurrent reads for same block.
    inflight: DashMap<ObjectId, Arc<Notify>>,
    /// HTTP client pool (dedicated for reads).
    operator: Operator,
    /// Metrics.
    metrics: FetchMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FetchPriority {
    /// User is actively waiting for this block (interactive read).
    /// Gets dispatched immediately, even if AIMD window is exhausted
    /// (up to min_window emergency slots).
    Interactive = 0,
    /// Sequential readahead — high priority but not urgent.
    Readahead = 1,
    /// Background prefetch (directory-level, speculative).
    Speculative = 2,
}

pub struct FetchRequest {
    pub object_id: ObjectId,
    pub priority: FetchPriority,
    /// Optional: if set, only download this byte range (HTTP Range).
    pub range: Option<(u64, u64)>,
    /// Oneshot channel to deliver the result back to the caller.
    pub result_tx: oneshot::Sender<Result<Bytes, FetchError>>,
}
```

**Поток:**

1. VFS `read_block()` создаёт `FetchRequest { priority: Interactive }` и ждёт на `result_rx`
2. Readahead создаёт `FetchRequest { priority: Readahead }`
3. FetchScheduler выбирает запрос с наивысшим приоритетом
4. Делает `aimd.acquire()` (ждёт permit)
5. Проверяет singleflight — если уже идёт fetch для этого object_id, подключается к ожиданию
6. Выполняет HTTP GET (или Range GET если `range` задан)
7. При успехе: `aimd.on_success()`, кладёт в кеш, отвечает через `result_tx`
8. При ошибке: `aimd.on_failure()`, ретраит (экспоненциальный backoff) или отвечает ошибкой

**Singleflight (request coalescing):**

```rust
// Вместо текущего broadcast::channel + parking_lot::Mutex + InFlightGuard
// используем DashMap + tokio::sync::watch или broadcast:

async fn fetch_or_join(&self, req: FetchRequest) -> Result<Bytes, FetchError> {
    // Fast path: check cache
    if let Some(data) = self.cache.get(req.object_id).await {
        return Ok(data);
    }

    // Singleflight
    loop {
        match self.inflight.entry(req.object_id) {
            Entry::Occupied(e) => {
                // Someone is already fetching — wait for them
                let notify = e.get().clone();
                drop(e);
                notify.notified().await;
                // Re-check cache
                if let Some(data) = self.cache.get(req.object_id).await {
                    return Ok(data);
                }
                // Fetcher failed, we become the fetcher
                continue;
            }
            Entry::Vacant(e) => {
                e.insert(Arc::new(Notify::new()));
                break;
            }
        }
    }

    // We are the fetcher
    let result = self.do_fetch(req.object_id, req.range).await;

    // Cleanup + notify
    if let Some((_, notify)) = self.inflight.remove(&req.object_id) {
        notify.notify_waiters();
    }

    result
}
```

### 3.4 Upload Scheduler

```rust
pub struct UploadScheduler {
    /// AIMD controller for upload concurrency.
    aimd: Mutex<AimdController>,
    /// Pending uploads (FIFO, no priority needed).
    queue_rx: mpsc::Receiver<PendingUpload>,
    /// HTTP client pool (dedicated for writes).
    operator: Operator,
    /// Retry queue: failed uploads go here with exponential backoff.
    retry_queue: DelayQueue<PendingUpload>,
}
```

**Поток:**

1. `drain_dirty_mem()` отправляет `PendingUpload` в канал
2. UploadScheduler берёт из канала, делает `aimd.acquire()`
3. Читает блок из кеша, сжимает, загружает в S3
4. При успехе: `aimd.on_success()`, `complete_upload()`
5. При ошибке: `aimd.on_failure()`, кладёт в `retry_queue` с delay = `2^attempt * 1s`
6. `retry_queue` автоматически переподаёт после задержки

### 3.5 HTTP Connection Pool Management

Вместо OpenDAL'овых `ConcurrentLimitLayer` (фиксированный лимит), управление полностью через AIMD:

```rust
// Конфигурация OpenDAL Operator без ConcurrentLimitLayer:
let op = Operator::new(builder)?
    .layer(
        TimeoutLayer::new()
            // Таймаут на установление соединения (TCP + TLS handshake):
            .with_timeout(Duration::from_secs(10))
            // Inactivity timeout на body read/write:
            .with_io_timeout(Duration::from_secs(30)),
    )
    .layer(
        RetryLayer::new()
            .with_max_times(2)           // меньше ретраев на этом уровне
            .with_min_delay(Duration::from_millis(500))
            .with_max_delay(Duration::from_secs(2))
            .with_jitter(),
    )
    // НЕТ ConcurrentLimitLayer — AIMD управляет конкурентностью
    .finish();
```

**Connection pool pre-warming:**

```rust
impl TransferEngine {
    /// Warm up the HTTP connection pool by issuing a HEAD request.
    /// Called synchronously during mount() — blocks until complete or timeout.
    async fn warmup(&self) -> Result<()> {
        // Issue a small HEAD request to establish at least one TCP connection
        // in each pool (read + write).
        let timeout = Duration::from_secs(10);
        tokio::time::timeout(timeout, async {
            let _ = self.fetch_operator.check().await;
            let _ = self.upload_operator.check().await;
        }).await?;
        Ok(())
    }
}
```

---

## 4. Read Path (новый дизайн)

### 4.1 Общий поток чтения

```text
read_into(ino, offset, buf)
    │
    ▼
Compute block list: [(block_idx, off_in_block, len)]
    │
    ▼
For each block:
    ├─ pending_blocks? → return data (write buffer)
    ├─ dirty_mem? → return data (dirty buffer)
    ├─ L1 cache? → return data (instant)
    ├─ L2 cache? → return data, promote to L1
    └─ Schedule FetchRequest(Interactive)
          │
          ▼
    FetchScheduler.fetch_or_join()
          │
          ├─ Singleflight check → join existing fetch
          │
          ├─ AIMD.acquire() → wait for permit
          │
          ├─ HTTP GET (full block or range)
          │
          ├─ Decompress + verify checksum
          │
          ├─ Cache in L1 + L2
          │
          └─ Return data to caller
    │
    ▼
Assemble buffer, trigger readahead
```

### 4.2 First-byte optimization

**Стратегия: first block = highest priority, rest = readahead**

```rust
async fn read_into(&self, ino: Ino, offset: u64, buffer: &mut [u8]) -> VfsResult<u32> {
    let blocks_needed = compute_blocks(offset, buffer.len(), self.block_size);

    if blocks_needed.is_empty() {
        return Ok(0);
    }

    // First block: Interactive priority, fetched inline
    let first = &blocks_needed[0];
    let first_data = self.fetch_block(ino, first.block_index, FetchPriority::Interactive).await?;
    copy_block_to_buffer(&first_data, first, buffer);

    // Remaining blocks: fetch in parallel as Readahead priority
    if blocks_needed.len() > 1 {
        let futures: Vec<_> = blocks_needed[1..].iter().map(|b| {
            self.fetch_block(ino, b.block_index, FetchPriority::Readahead)
        }).collect();

        let results = futures::future::join_all(futures).await;
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(data) => copy_block_to_buffer(&data, &blocks_needed[i + 1], buffer),
                Err(VfsError::BlockNotFound(..)) => fill_zeros(&blocks_needed[i + 1], buffer),
                Err(e) => return Err(e),
            }
        }
    }

    // Trigger readahead for blocks beyond the current read
    self.schedule_readahead(ino, &blocks_needed);

    Ok(written)
}
```

### 4.3 HTTP Range optimization

Для маленьких read (< block_size / 4) из некомпрессированных блоков:

```rust
async fn fetch_block_or_range(
    &self,
    ino: Ino,
    block_index: u32,
    read_offset: u64,
    read_len: usize,
    priority: FetchPriority,
) -> VfsResult<Bytes> {
    // Check if we should do a range read
    let use_range = read_len < (self.block_size as usize / 4)
        && !self.compression_enabled  // можно только без сжатия
        && read_offset > 0;          // первый блок всегда целиком (для кеша)

    if use_range {
        // Try range read first — lower latency, but doesn't populate cache
        let range = (read_offset, read_len as u64);
        if let Ok(data) = self.fetch_scheduler.fetch_range(object_id, range).await {
            // Trigger background full-block fetch for cache warming
            self.fetch_scheduler.schedule(FetchRequest {
                object_id,
                priority: FetchPriority::Speculative,
                range: None,  // full block
                ..
            });
            return Ok(data);
        }
        // Fall through to full fetch on failure
    }

    // Full block fetch (goes through singleflight + cache)
    self.fetch_scheduler.fetch_full(object_id, priority).await
}
```

### 4.4 Adaptive Readahead (улучшенный)

```rust
/// Per-file readahead state, inspired by JuiceFS's per-session tracking.
struct ReadaheadState {
    /// Last block index read.
    last_block: u32,
    /// Current readahead window (in blocks).
    window: u32,
    /// Consecutive sequential reads counter.
    sequential_count: u32,
}

impl ReadaheadState {
    fn record_read(&mut self, first_block: u32, last_block: u32) -> Vec<u32> {
        let is_sequential = first_block == self.last_block + 1
            || first_block == self.last_block;

        if is_sequential {
            self.sequential_count += 1;
            // Exponential growth: 1 → 2 → 4 → 8 → max
            if self.sequential_count >= self.window {
                self.window = (self.window * 2).min(MAX_READAHEAD_BLOCKS);
            }
        } else {
            // Random read — shrink window
            self.sequential_count = 0;
            self.window = (self.window / 2).max(1);
        }

        self.last_block = last_block;

        // Return blocks to prefetch
        let prefetch_start = last_block + 1;
        let prefetch_end = prefetch_start + self.window;
        (prefetch_start..prefetch_end).collect()
    }
}
```

---

## 5. Write/Upload Path (новый дизайн)

### 5.1 Общий поток записи (без изменений)

Текущий write path работает хорошо:

```text
write(ino, offset, data)
  → buffer into PendingBlock (BytesMut)
  → full block? → move to dirty_mem
  → signal background flusher

flush(ino)
  → move remaining PendingBlocks → dirty_mem
  → commit attrs to metadata

background_slab_flusher
  → drain_dirty_mem()
    → alloc block IDs (batch)
    → commit metadata (1 fsync)
    → stage L1 (instant)
    → submit upload journal
    → spawn commit L2 (NVMe)
```

### 5.2 Upload path: AIMD вместо фиксированного семафора

Заменяем `upload_semaphore: Semaphore::new(4)` на `UploadScheduler` с AIMD:

```rust
// Было:
let _permit = self.upload_semaphore.clone().acquire_owned().await.unwrap();
self.process_upload(op, _permit).await;

// Стало:
let _permit = self.upload_scheduler.aimd.acquire().await;
match self.process_upload(op).await {
    Ok(()) => self.upload_scheduler.aimd.on_success(),
    Err(e) => {
        self.upload_scheduler.aimd.on_failure();
        self.upload_scheduler.retry(op, attempt + 1);
    }
}
```

### 5.3 Retry с экспоненциальным backoff

```rust
impl UploadScheduler {
    fn retry(&self, upload: PendingUpload, attempt: u32) {
        if attempt >= MAX_UPLOAD_RETRIES {
            error!(object_id = upload.object_id, "upload permanently failed after {attempt} retries");
            return;
        }
        let delay = Duration::from_secs(2u64.pow(attempt).min(60));
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            self.queue_tx.send(upload).ok();
        });
    }
}
```

---

## 6. Что убрать / рефакторинг

### 6.1 Удалить

| Компонент | Причина |
|-----------|---------|
| `FetchThrottle` | Заменяется AIMD controller |
| `get_remote_object_with_retry()` VFS-level retry loop | Ретраи теперь внутри FetchScheduler |
| `upload_semaphore: Semaphore::new(4)` | Заменяется UploadScheduler с AIMD |
| `InFlightGuard` + `inflight_fetches` HashMap | Заменяется singleflight в FetchScheduler |
| `background_remote_warmup()` | Warmup теперь синхронный, внутри TransferEngine |
| `IoProfile` enum (Read/Write/Default) | Два Operator'а (fetch/upload) с одинаковой конфигурацией, различие только в AIMD параметрах |

### 6.2 Рефакторинг

| Компонент | Изменение |
|-----------|-----------|
| `DataStore` | Упростить до одного профиля. AIMD управляет конкурентностью вместо ConcurrentLimitLayer |
| `read_block()` | Делегирует в `FetchScheduler.fetch_or_join()` |
| `read_into()` | Первый блок — Interactive, остальные — Readahead |
| `ReadAheadTracker` | Экспоненциальный window рост/уменьшение вместо линейного score |
| `background_uploader()` | Заменяется UploadScheduler.run() |

---

## 7. Фазы реализации

### Phase 1: AIMD Controller + FetchScheduler (критический путь)

**Цель: убрать зависания при холодном чтении**

1. Написать `AimdController` (отдельный модуль `studiofs-transfer` или внутри `studiofs-vfs`)
2. Написать `FetchScheduler` с singleflight + priority queue
3. Заменить `read_block()` → `fetch_scheduler.fetch_or_join()`
4. Удалить `FetchThrottle`, `InFlightGuard`, VFS-retry loop
5. Синхронный warmup при mount

**Ориентировочно: 2-3 дня**

### Phase 2: Upload AIMD

**Цель: эластичный upload**

1. Заменить `upload_semaphore` на AIMD-based UploadScheduler
2. Retry queue с exponential backoff
3. Удалить фиксированный `Semaphore::new(4)`

**Ориентировочно: 1 день**

### Phase 3: Range reads + Readahead improvements

**Цель: уменьшить read amplification, улучшить streaming**

1. Определять может ли блок быть скачан range-запросом (нет сжатия, или новый формат)
2. HTTP Range для small reads
3. Экспоненциальный readahead window
4. Per-file readahead state вместо global score

**Ориентировочно: 2 дня**

---

## 8. Конфигурация

### Environment variables

```bash
# AIMD Fetch
STUDIOFS_FETCH_MIN_WINDOW=1         # Минимальный параллелизм скачивания
STUDIOFS_FETCH_MAX_WINDOW=32        # Максимальный параллелизм скачивания
STUDIOFS_FETCH_INITIAL_WINDOW=2     # Стартовый параллелизм
STUDIOFS_FETCH_DECREASE_FACTOR=0.5  # Множитель уменьшения при ошибке

# AIMD Upload
STUDIOFS_UPLOAD_MIN_WINDOW=1
STUDIOFS_UPLOAD_MAX_WINDOW=8
STUDIOFS_UPLOAD_INITIAL_WINDOW=2

# Readahead
STUDIOFS_READAHEAD_MAX_BLOCKS=32    # Максимальное окно readahead (в блоках)

# Timeouts (по-прежнему в OpenDAL)
STUDIOFS_IO_TIMEOUT=30              # Inactivity timeout для body I/O
STUDIOFS_CONNECT_TIMEOUT=10         # TCP+TLS establishment timeout
```

---

## 9. Метрики / observability

```rust
pub struct TransferMetrics {
    // Fetch
    pub fetch_total: AtomicU64,
    pub fetch_cache_hits: AtomicU64,
    pub fetch_s3_hits: AtomicU64,
    pub fetch_errors: AtomicU64,
    pub fetch_singleflight_joins: AtomicU64,
    pub fetch_aimd_window: AtomicU64,  // current window (for logging)
    pub fetch_bytes: AtomicU64,

    // Upload
    pub upload_total: AtomicU64,
    pub upload_errors: AtomicU64,
    pub upload_retries: AtomicU64,
    pub upload_aimd_window: AtomicU64,
    pub upload_bytes: AtomicU64,

    // Latency (histogram buckets or running avg)
    pub fetch_latency_p50_ms: AtomicU64,
    pub fetch_latency_p99_ms: AtomicU64,
}
```

Периодический лог (каждые 30s):

```
INFO fetch: window=12/32 ok=847 fail=3 singleflight=124 cache_hit=92%
     upload: window=4/8 ok=23 fail=0 retries=1 pending=7
```

---

## 10. Диаграмма: новый read path

```text
   User: read_into(ino=42, offset=0, len=5MB)
           │
           ▼
   Compute blocks: [0, 1, 2, 3, 4]  (1 MiB each)
           │
           ▼
   Block 0: INTERACTIVE priority ──────┐
   Block 1-4: READAHEAD priority ──────┤
                                       ▼
                              FetchScheduler
                              ┌────────────┐
                              │ Singleflight│
                              │ check       │
                              └──────┬─────┘
                                     │
                              ┌──────▼──────┐
                              │ AIMD.acquire │ ← window=2
                              │ (block 0+1   │   (только 2 одновременно)
                              │  get permits)│
                              └──────┬──────┘
                                     │
                    ┌────────────────┼────────────────┐
                    ▼                ▼                │
              S3 GET block 0   S3 GET block 1        │ (block 2-4 ждут пермит)
                    │                │                │
                    ▼                ▼                │
              ✓ success!        ✓ success!           │
              AIMD: window→3    AIMD: window→4       │
              cache + respond   cache                │
                                                     ▼
                              block 2,3,4 now get permits...
                              (window уже 4, все стартуют)
                                     │
                                     ▼
                              All done → assemble buffer
                              Schedule readahead blocks 5-8
```

---

## 11. Corner cases

### 11.1 Mount с холодным пулом

1. `TransferEngine::warmup()` блокирует mount, делает HEAD-запрос
2. AIMD начинает с window=2
3. Первый read: 2 блока параллельно, остальные ждут
4. Оба succeed → window=4 → следующие 4 блока → window=8 → ...
5. За 3-4 волны выходим на полную скорость

### 11.2 S3 начинает отказывать

1. `on_failure()` → window *= 0.5 (например 16 → 8)
2. Cooldown 5s — не уменьшаем повторно 5 секунд
3. Если продолжает отказывать: 8 → 4 → 2 → 1
4. На window=1 всегда работает один запрос
5. Когда S3 восстанавливается: каждый успех → window++ → быстро растём обратно

### 11.3 Файл частично в кеше

1. Блоки в кеше → instant return (не потребляют AIMD permit)
2. Uncached блоки → FetchScheduler → AIMD
3. Кеш-хиты не влияют на AIMD window (ни рост, ни уменьшение)

### 11.4 Конкурентные read одного блока (singleflight)

1. Task A: fetch block 42 → вставляет в inflight → делает HTTP GET
2. Task B: fetch block 42 → видит A в inflight → ждёт notify
3. Task C: fetch block 42 → видит A в inflight → ждёт notify
4. A завершает → кладёт в кеш → notify_waiters()
5. B и C просыпаются → читают из кеша → return

Если A провалился:
- B и C просыпаются → кеш пустой → один из них становится новым fetcher, другой снова ждёт

### 11.5 Размонтирование с очередью uploads

1. `TransferEngine::shutdown()` → не принимаем новые upload
2. Drain retry_queue — даём каждому ещё одну попытку
3. Wait pending uploads с таймаутом (30s)
4. Если uploads не завершились — они останутся в journal, подхватятся при следующем mount
