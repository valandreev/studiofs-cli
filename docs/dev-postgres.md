# studiofs-lobby — Design Document (v4 — WAN-first)

## Предыстория: почему отказ от Tonbo

Тест с 5 клиентами выявил фундаментальные проблемы serverless-подхода (Tonbo + S3 manifest + CAS):

| Проблема | Детали |
|----------|--------|
| **Потеря файлов** | Клиенты писали в разные директории, ~50% файлов потеряно после синхронизации |
| **Раздувание manifest** | 2 файла на диске → manifest.json уже 3.5 МБ (полный snapshot при каждом flush) |
| **Невидимость файлов** | Файлы появлялись с задержкой 20-60с (TTL), видны не всем клиентам одновременно |
| **3-way merge хрупок** | ~250 строк merge-логики с LWW-эвристиками — ненадёжно и не поддаётся отладке |
| **CAS contention** | 5 клиентов флашат одновременно → каскад 412 Conflict → retry storm |

**Вывод:** serverless metadata через S3 CAS не масштабируется на >2 клиентов. Нужен центральный арбитр.

---

## 1. Философия: PostgreSQL + WAN-first

используем **PostgreSQL**.

### Ключевое допущение: WAN-only

> **Все клиенты работают через WAN.** RTT до сервера PG: **40—300 мс** (типичные значения для студий в разных регионах). Архитектура оптимизирована исключительно под WAN — LAN-оптимизации не рассматриваются.

Это означает:
- Каждый PG round-trip стоит дорого → **минимизация количества round-trips** (batch mutations, range ID alloc, pipeline queries)
- Локальный redb на клиенте — **обязателен**, не опция. Все reads из redb (sub-µs), PG только для writes/sync
- Write-back batching агрессивный: до 100 мутаций или 2с, что раньше (а не 50/1с как в LAN-сценарии)
- ID allocation большими блоками: 8192 inodes + 131072 blocks (часы/дни offline работы)
- LISTEN/NOTIFY + periodic poll (каждые 30с) для sync — при WAN latency NOTIFY delivery может задерживаться

### Гонки — допустимы (модель LucidLink)

> Конкурентные изменения одного файла/директории — **race condition допустима** (Last-Writer-Wins). Это сознательное решение, аналогичное LucidLink. Для критических файлов есть advisory locks.

**Почему PostgreSQL:**
- ACID транзакции из коробки (inode + dentry + counter — атомарно)
- `LISTEN/NOTIFY` + trigger = push-нотификации без NATS
- Advisory locks = файловые блокировки с автоматическим release при disconnect
- `SEQUENCE` / atomic counter = ID allocation без CAS
- Знакомый стек → доверие пользователей
- `docker-compose up` и готово

**Что убираем:** NATS (PG NOTIFY заменяет для метаданных).

**Что остаётся:**
- Локальный `redb` на клиенте как L1 кэш (sub-microsecond reads для VFS hot path)
- S3 для блоков данных (без изменений)
- `MetaEngine` трейт (новый `PgMetaEngine` вместо `TonboMetaEngine`)

---

## 2. Архитектура

```
Client A ──tokio-postgres──▸ PostgreSQL (source of truth)
Client B ──tokio-postgres──▸     │         ├── metadata tables
Client C ──tokio-postgres──▸     │         ├── event_log
                                 │         ├── advisory locks
                                 │         └── LISTEN/NOTIFY trigger
                                 │
                            pg_notify("studiofs_vol_1", seq_id)
                                 │
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
              Client A       Client B      Client C
              (local redb    (local redb   (local redb
               L1 cache)      L1 cache)    L1 cache)
```

**Клиент:**
- **Read path** (горячий): `lookup`, `getattr`, `readdir` → из локального `redb` (sub-µs)
- **Write path**: запись в локальный `redb` (мгновенно для приложения) → фоновый воркер батчит и коммитит в PostgreSQL
- **Sync path**: `LISTEN` на PG канале → при NOTIFY запрос event_log → apply дельта в локальный redb
- **Catch-up**: при reconnect → `SELECT * FROM event_log WHERE volume_id=$1 AND seq_id > $2`

### Зачем оставить redb локально (критично для WAN)

- VFS hot path (`lookup` на каждый компонент пути) требует sub-millisecond. **PG по WAN = 40-300мс/query** — неприемлемо для hot path
- Глубокий путь `/a/b/c/d/e/file.exr` = 6 lookup'ов. При WAN 100мс = 600мс задержки без redb. С redb = <1мс
- Offline режим: redb работает без сети, мутации копятся в локальной очереди
- Write-back: приложение не ждёт WAN round-trip — запись в redb мгновенна, sync в PG фоном
- Уже работает: 3050 строк `redb_engine.rs` без изменений

---

## 3. PostgreSQL Schema

### Docker Compose

```yaml
# docker-compose.yml
# Target: 2× ARM CPU (Ampere Altra) + 8 GB RAM
services:
  postgres:
    image: postgres:18-alpine    # PostgreSQL 18.3
    platform: linux/arm64
    restart: unless-stopped
    environment:
      POSTGRES_DB: studiofs
      POSTGRES_USER: studiofs
      POSTGRES_PASSWORD: studiofs  # override in production
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 8G
        reservations:
          cpus: "1.0"
          memory: 4G
    command: >
      postgres
        -c shared_buffers=2GB
        -c effective_cache_size=6GB
        -c work_mem=32MB
        -c maintenance_work_mem=256MB
        -c max_connections=200
        -c synchronous_commit=on
        -c wal_buffers=64MB
        -c checkpoint_completion_target=0.9
        -c checkpoint_timeout=10min
        -c max_wal_size=1GB
        -c min_wal_size=256MB
        -c random_page_cost=1.1
        -c effective_io_concurrency=200
        -c max_worker_processes=2
        -c max_parallel_workers_per_gather=1
        -c max_parallel_workers=2
        -c max_parallel_maintenance_workers=1
        -c huge_pages=try
        -c tcp_keepalives_idle=60
        -c tcp_keepalives_interval=10
        -c tcp_keepalives_count=6
        -c idle_in_transaction_session_timeout=120000
        -c statement_timeout=30000
        -c jit=off
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U studiofs"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  pgdata:
```

**Настройки оптимизированы под 2 ARM CPU + 8 GB RAM (Ampere):**

| Параметр | Значение | Обоснование |
|----------|----------|-------------|
| `shared_buffers` | 2 GB | 25% RAM — стандартная рекомендация |
| `effective_cache_size` | 6 GB | 75% RAM — подсказка планировщику |
| `work_mem` | 32 MB | Достаточно для сортировок event_log; 200 connections × 32MB = 6.4GB max (редко все одновременно) |
| `max_connections` | 200 | До 50 клиентов × 4 conn/pool |
| `max_worker_processes` | 2 | = количество CPU |
| `max_parallel_workers` | 2 | = количество CPU |
| `random_page_cost` | 1.1 | Предполагаем SSD/NVMe |
| `effective_io_concurrency` | 200 | SSD/NVMe |
| `tcp_keepalives_idle` | 60с | Быстрое обнаружение отвалившихся WAN-клиентов |
| `tcp_keepalives_interval` | 10с | Агрессивный keepalive для WAN |
| `tcp_keepalives_count` | 6 | Dead connection detected ≈ 60+60=120с worst case |
| `jit` | off | JIT overhead не окупается на простых metadata queries |
| `statement_timeout` | 30с | Защита от зависших WAN-клиентов |
| `idle_in_transaction_session_timeout` | 120с | Автоматический rollback забытых транзакций |

Запуск: `docker compose up -d`

### Локальный PostgreSQL для тестов (без Docker)

Для быстрых локальных тестов удобнее запустить PG напрямую, без контейнера.

#### macOS (Homebrew)

```bash
# Установка (один раз)
brew install postgresql@18

# Инициализация кластера (один раз)
initdb -D /tmp/studiofs-pg --auth=trust --username=studiofs --encoding=UTF8

# Запуск (параметры максимально лёгкие для разработки)
postgres -D /tmp/studiofs-pg \
  -p 5432 \
  -c shared_buffers=128MB \
  -c work_mem=8MB \
  -c max_connections=50 \
  -c synchronous_commit=off \
  -c fsync=off \
  -c full_page_writes=off \
  -c log_min_messages=warning \
  -c log_destination=stderr \
  -c logging_collector=off

# или фоном:
pg_ctl -D /tmp/studiofs-pg -l /tmp/studiofs-pg.log \
  -o "-p 5432 -c shared_buffers=128MB -c work_mem=8MB -c max_connections=50 -c synchronous_commit=off -c fsync=off -c full_page_writes=off" \
  start
```

> **Внимание:** `fsync=off` и `full_page_writes=off` — только для тестов. При крэше данные могут быть повреждены. Никогда не использовать в production.

#### Создание базы и пользователя

```bash
createdb -h localhost -p 5432 -U studiofs studiofs
# PG запущен с --auth=trust, пароль не нужен
```

#### Подключение для проверки

```bash
psql -h localhost -p 5432 -U studiofs -d studiofs -c '\dt'
```

#### Остановка

```bash
# Graceful shutdown (ждёт завершения транзакций)
pg_ctl -D /tmp/studiofs-pg stop -m fast

# Или если запущен на переднем плане: Ctrl+C
```

#### Сброс до чистого состояния

```bash
# Полная очистка — удалить весь кластер и пересоздать
pg_ctl -D /tmp/studiofs-pg stop -m immediate 2>/dev/null; \
rm -rf /tmp/studiofs-pg /tmp/studiofs-pg.log && \
initdb -D /tmp/studiofs-pg --auth=trust --username=studiofs --encoding=UTF8 && \
pg_ctl -D /tmp/studiofs-pg -l /tmp/studiofs-pg.log \
  -o "-p 5432 -c fsync=off -c full_page_writes=off -c synchronous_commit=off" start && \
sleep 1 && createdb -h localhost -p 5432 -U studiofs studiofs
```

#### Только сброс данных studiofs (без пересоздания кластера)

```bash
# Дропнуть и пересоздать базу — быстрее чем reinit кластера
dropdb -h localhost -p 5432 -U studiofs studiofs && \
createdb -h localhost -p 5432 -U studiofs studiofs
# studiofs init пересоздаст все таблицы автоматически
```

#### Linux (системный PostgreSQL 18)

```bash
# Ubuntu/Debian
sudo apt install postgresql-18

# Запуск от текущего пользователя (для тестов)
initdb -D /tmp/studiofs-pg --auth=trust --username=$USER --encoding=UTF8
postgres -D /tmp/studiofs-pg -p 5432 -c fsync=off -c synchronous_commit=off &

createdb -h localhost -p 5432 studiofs
```

#### Windows (PowerShell)

Установка через [EDB installer](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads) или winget:

```powershell
# Установка PostgreSQL 18 (один раз)
winget install PostgreSQL.PostgreSQL.18

# После установки pg_ctl/initdb находятся в:
# C:\Program Files\PostgreSQL\18\bin\
# Добавить в PATH (один раз):
$env:PATH += ";C:\Program Files\PostgreSQL\18\bin"
# Или прописать в системных переменных окружения постоянно.
```

**Инициализация кластера (один раз):**

```powershell
initdb -D C:\Temp\studiofs-pg --auth=trust --username=studiofs --encoding=UTF8
```

**Запуск:**

```powershell
# Фоном через pg_ctl (рекомендуется для тестов)
pg_ctl -D C:\Temp\studiofs-pg -l C:\Temp\studiofs-pg.log `
  -o "-p 5432 -c fsync=off -c full_page_writes=off -c synchronous_commit=off -c log_min_messages=warning" `
  start

# Проверить статус
pg_ctl -D C:\Temp\studiofs-pg status
```

> **Внимание:** `fsync=off` — только для тестов. Никогда не для production.

**Создание базы:**

```powershell
createdb -h localhost -p 5432 -U studiofs studiofs

# Проверка подключения
psql -h localhost -p 5432 -U studiofs -d studiofs -c "\dt"
```

**Остановка:**

```powershell
pg_ctl -D C:\Temp\studiofs-pg stop -m fast
```

**Полный сброс до чистого состояния:**

```powershell
# Остановить, удалить кластер, пересоздать
pg_ctl -D C:\Temp\studiofs-pg stop -m immediate 2>$null
Remove-Item -Recurse -Force C:\Temp\studiofs-pg, C:\Temp\studiofs-pg.log -ErrorAction SilentlyContinue

initdb -D C:\Temp\studiofs-pg --auth=trust --username=studiofs --encoding=UTF8

pg_ctl -D C:\Temp\studiofs-pg -l C:\Temp\studiofs-pg.log `
  -o "-p 5432 -c fsync=off -c full_page_writes=off -c synchronous_commit=off" `
  start

Start-Sleep -Seconds 1
createdb -h localhost -p 5432 -U studiofs studiofs
```

**Только сброс данных studiofs (без пересоздания кластера):**

```powershell
dropdb -h localhost -p 5432 -U studiofs studiofs
createdb -h localhost -p 5432 -U studiofs studiofs
# studiofs init пересоздаст все таблицы автоматически
```

**Альтернатива — WSL2:**  
Если на машине есть WSL2, удобнее использовать Linux-инструкцию выше внутри WSL и подключаться с Windows по `localhost:5432` — WSL2 пробрасывает порты автоматически.

### SQL Schema (автоматически создаётся при `studiofs init`)

```sql
-- ═══ Volumes ═══════════════════════════════════════════════════════════════

CREATE TABLE volumes (
    id          SERIAL PRIMARY KEY,
    name        TEXT UNIQUE NOT NULL,
    uuid        TEXT UNIQUE NOT NULL,
    block_size  INT NOT NULL DEFAULT 8388608,
    settings    JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ═══ Metadata Tables (per volume, composite PK) ══════════════════════════

CREATE TABLE inodes (
    vol   INT NOT NULL REFERENCES volumes(id),
    ino   BIGINT NOT NULL,
    kind  SMALLINT NOT NULL,        -- 1=file, 2=dir, 3=symlink
    mode  INT NOT NULL,
    uid   INT NOT NULL DEFAULT 0,
    gid   INT NOT NULL DEFAULT 0,
    size  BIGINT NOT NULL DEFAULT 0,
    nlink INT NOT NULL DEFAULT 1,
    mtime BIGINT NOT NULL,          -- nanoseconds since epoch
    ctime BIGINT NOT NULL,
    atime BIGINT NOT NULL,
    crtime BIGINT NOT NULL DEFAULT 0,
    parent BIGINT NOT NULL DEFAULT 0,
    win_attrs INT NOT NULL DEFAULT 0,
    PRIMARY KEY (vol, ino)
);

CREATE TABLE dentries (
    vol        INT NOT NULL REFERENCES volumes(id),
    parent_ino BIGINT NOT NULL,
    name       BYTEA NOT NULL,
    child_ino  BIGINT NOT NULL,
    child_kind SMALLINT NOT NULL,
    PRIMARY KEY (vol, parent_ino, name)
);
CREATE INDEX idx_dentries_parent ON dentries (vol, parent_ino);

CREATE TABLE file_blocks (
    vol         INT NOT NULL REFERENCES volumes(id),
    ino         BIGINT NOT NULL,
    block_index INT NOT NULL,
    object_id   BIGINT NOT NULL,
    blk_offset  BIGINT NOT NULL DEFAULT 0,
    length      INT NOT NULL,
    flags       INT NOT NULL DEFAULT 0,
    PRIMARY KEY (vol, ino, block_index)
);
CREATE INDEX idx_file_blocks_ino ON file_blocks (vol, ino);

CREATE TABLE counters (
    vol   INT NOT NULL REFERENCES volumes(id),
    name  TEXT NOT NULL,
    value BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (vol, name)
);

CREATE TABLE pending_deletions (
    vol        INT NOT NULL REFERENCES volumes(id),
    object_id  BIGINT NOT NULL,
    deleted_at BIGINT NOT NULL,
    PRIMARY KEY (vol, object_id)
);

CREATE TABLE symlinks (
    vol    INT NOT NULL REFERENCES volumes(id),
    ino    BIGINT NOT NULL,
    target BYTEA NOT NULL,
    PRIMARY KEY (vol, ino)
);

CREATE TABLE xattrs (
    vol   INT NOT NULL REFERENCES volumes(id),
    ino   BIGINT NOT NULL,
    name  BYTEA NOT NULL,
    value BYTEA NOT NULL,
    PRIMARY KEY (vol, ino, name)
);

-- ═══ Event Log ═══════════════════════════════════════════════════════════

CREATE TABLE event_log (
    vol                BIGINT NOT NULL,
    seq_id             BIGSERIAL,
    client_id          TEXT NOT NULL,
    client_timestamp   BIGINT NOT NULL,   -- ns since epoch
    event_type         TEXT NOT NULL,
    event_payload      BYTEA NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vol, seq_id)
);
CREATE INDEX idx_event_log_seq ON event_log (vol, seq_id);

-- ═══ File Locks ══════════════════════════════════════════════════════════

CREATE TABLE file_locks (
    vol        INT NOT NULL REFERENCES volumes(id),
    ino        BIGINT NOT NULL,
    client_id  TEXT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (vol, ino)
);
CREATE INDEX idx_file_locks_expires ON file_locks (expires_at);

-- ═══ Idempotency (optional, for retry safety) ═══════════════════════════

CREATE TABLE applied_ops (
    operation_id TEXT PRIMARY KEY,
    seq_id       BIGINT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Auto-cleanup: delete entries older than 1 hour (cron or pg_partman)

-- ═══ LISTEN/NOTIFY Trigger ══════════════════════════════════════════════

CREATE OR REPLACE FUNCTION notify_meta_event() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify(
        'studiofs_vol_' || NEW.vol,
        NEW.seq_id::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_event_log_notify
    AFTER INSERT ON event_log
    FOR EACH ROW
    EXECUTE FUNCTION notify_meta_event();
```

### Ключевые SQL-операции

**Atomic file create** (полная транзакция в одном SQL round-trip — критично для WAN):
```sql
BEGIN;
  -- Allocate inode
  UPDATE counters SET value = value + 1
    WHERE vol = $1 AND name = 'next_inode' RETURNING value AS ino;

  -- Insert inode
  INSERT INTO inodes (vol, ino, kind, mode, mtime, ctime, atime, parent)
    VALUES ($1, $ino, 1, $mode, $now, $now, $now, $parent);

  -- Insert dentry
  INSERT INTO dentries (vol, parent_ino, name, child_ino, child_kind)
    VALUES ($1, $parent, $name, $ino, 1);

  -- Append event
  INSERT INTO event_log (vol, client_id, client_timestamp, event_type, event_payload)
    VALUES ($1, $client, $ts, 'CreateFile', $payload);

  -- Update parent mtime
  UPDATE inodes SET mtime = $now, ctime = $now WHERE vol = $1 AND ino = $parent;
COMMIT;
```

**Range-based ID allocation** (один round-trip, $size=8192 для inodes, 131072 для blocks):
```sql
UPDATE counters SET value = value + $size
  WHERE vol = $1 AND name = 'next_inode'
  RETURNING value - $size AS range_start, value AS range_end;
```

**Advisory lock (session-scoped, auto-release on disconnect):**
```sql
-- Acquire (non-blocking): lock_key = volume_id * 1000000 + ino
SELECT pg_try_advisory_lock($lock_key);

-- Release explicit:
SELECT pg_advisory_unlock($lock_key);

-- Auto-release: happens when PG connection drops (client crash/disconnect)
```

**Catch-up после reconnect** (один round-trip, WAN-friendly):
```sql
SELECT seq_id, client_id, client_timestamp, event_type, event_payload
  FROM event_log
  WHERE vol = $1 AND seq_id > $last_known
  ORDER BY seq_id;
```

**Event log compaction (cron job или клиент-janitor):**
```sql
DELETE FROM event_log WHERE vol = $1 AND seq_id < $checkpoint_seq;
```

---

## 4. Разрешение конфликтов

### Принцип: PostgreSQL serializes (ACID)

PostgreSQL сам сериализует транзакции. Не нужен ни custom код, ни timestamp ordering:

1. Клиент A и Б одновременно делают `INSERT INTO dentries ... (name='comp_v01.drp')`
2. PostgreSQL: одна транзакция коммитится, вторая получает unique constraint violation
3. Проигравший клиент видит `EEXIST` → откатывает из локального redb

**Почему не timestamp ordering:**
- PG транзакции уже serializable
- Timestamp ordering добавляет сложность (clock skew, rollback winners)
- `client_timestamp` сохраняем в event_log для аудита, но **не** для conflict resolution

### Конфликтные сценарии (LucidLink-style: race OK)

> **Принцип:** гонки между клиентами — **допустимы**. Поведение аналогично LucidLink: кто последний записал — тот и прав (LWW). Explicit locking — только для критических workflow.

| Сценарий | PG behaviour | Клиент проигравшего |
|----------|-------------|-------------------|
| create same filename | UNIQUE violation | rollback dentry + inode из redb |
| rename к одной цели | UNIQUE violation или кто первый | rollback rename |
| delete + create в одной dir | Serialized: порядок определяет PG | может получить ENOENT |
| overwrite same file (LWW) | Оба UPDATE проходят, последний коммит остаётся | **норма: LWW — ожидаемое поведение** |
| concurrent write same blocks | Оба upload в S3, PG file_blocks LWW | последний коммит определяет содержимое |
| lock + modify | advisory lock блокирует второго | ждёт release или таймаут |

### Операции без конфликтов

Разные директории, разные файлы — **разные строки в PG** → никаких блокировок, полный параллелизм.

### Пессимистичная блокировка

Для критических файлов (проекты Nuke, DaVinci Resolve):

1. Клиент: `SELECT pg_try_advisory_lock(vol * 1000000 + ino)` (~40-300мс WAN round-trip)
2. Если `true` → лок получен. PG session держит его автоматически
3. Другой клиент: `pg_try_advisory_lock` → `false` → `ELOCKED`
4. При закрытии файла: `pg_advisory_unlock(...)` или автоматически при disconnect
5. **Нет TTL/heartbeat проблем:** PG сам обнаруживает disconnected sessions за ~60-120с (tcp_keepalives_idle=60, count=6, interval=10)

Для таблицы `file_locks` — backup для случаев когда нужна информация "кто держит лок":
```sql
INSERT INTO file_locks (vol, ino, client_id, expires_at)
  VALUES ($1, $2, $3, now() + interval '120 seconds')
  ON CONFLICT (vol, ino) DO NOTHING
  RETURNING *;
```

---

## 5. Event Log и Sync

### Write-back flow

1. Приложение вызывает `create_file()` → VFS → `PgMetaEngine`
2. `PgMetaEngine` пишет в **локальный redb** мгновенно (приложение не ждёт)
3. Мутация добавляется в `pending_queue` (in-memory + persisted в redb event_log)
4. Фоновый воркер (WAN-optimized batching):
   - Батчит мутации: до **100 штук** или **2с**, что раньше (увеличено для амортизации WAN latency)
   - Отправляет в PG одной транзакцией: `BEGIN; ... COMMIT;` (1 round-trip на весь batch)
   - При success: чистит pending_queue
   - При conflict (UNIQUE violation и т.п.): rollback несовместимых мутаций из redb
   - **Pipelining:** несколько независимых мутаций отправляются как pipelined statements (tokio-postgres pipeline), не дожидаясь ответа на каждый
5. NOTIFY стреляет на других клиентах → те обновляют свой redb

### LISTEN/NOTIFY sync (заменяет NATS)

```rust
// Клиент подключается и слушает
conn.execute("LISTEN studiofs_vol_42", &[]).await?;

loop {
    // Ждём NOTIFY (или таймаут для periodic poll)
    match tokio::time::timeout(Duration::from_secs(30), conn.notifications().next()).await {
        Ok(Some(notification)) => {
            let new_seq: i64 = notification.payload().parse()?;
            if new_seq > last_known_seq {
                let events = fetch_events_since(pool, vol, last_known_seq).await?;
                apply_to_local_redb(&events)?;
                invalidate_caches(&events)?;
                last_known_seq = new_seq;
            }
        }
        _ => {
            // Periodic poll fallback (in case NOTIFY was missed)
            let events = fetch_events_since(pool, vol, last_known_seq).await?;
            // ...
        }
    }
}
```

**Гарантии:**
- NOTIFY — best-effort hint (может пропасть при reconnect)
- Periodic poll (каждые 30с) — fallback для надёжности
- При reconnect: полный catch-up по event_log
- Нет отдельного NATS — один PG на всё

### Catch-up при reconnect

1. Клиент знает `last_known_seq = 5400`
2. `SELECT ... FROM event_log WHERE vol=$1 AND seq_id > 5400 ORDER BY seq_id`
3. Проигрывает дельту в локальный redb
4. Если event_log обрезан (seq 5400 уже нет): full re-sync из PG таблиц
   ```sql
   SELECT * FROM inodes WHERE vol = $1;
   SELECT * FROM dentries WHERE vol = $1;
   -- ... etc → rebuild local redb entirely
   ```

### Event log compaction

- Каждые 100K событий (или по cron): `DELETE FROM event_log WHERE seq_id < $cutoff`
- Или: использовать PG partitioning by range для эффективного DROP PARTITION

---

## 6. Deployment: `studiofs init`

### Команда

```bash
studiofs init my-disk \
  --endpoint s3://minio.local:9000/bucket \
  --access-key ... --secret-key ... \
  --postgres postgres://studiofs:studiofs@pg.local:5432/studiofs
```

### Что происходит при `init`:

1. Подключение к PostgreSQL, проверка доступности
2. Создание таблиц (idempotent `CREATE TABLE IF NOT EXISTS`)
3. `INSERT INTO volumes (name, uuid, block_size) VALUES (...)` → получаем `volume_id`
4. `INSERT INTO counters` для NEXT_INODE (=2), NEXT_BLOCK_ID (=1), USED_SPACE (=0)
5. `INSERT INTO inodes` — root inode (ino=1, kind=directory)
6. Сохранение `FormatInfo` в локальный redb с `metadata.backend = "Postgres"`
7. Вывод: `✓ Volume "my-disk" initialized (volume_id=1, postgres=pg.local:5432)`

**PG connection string сохраняется** в `FormatInfo.metadata.remote` (аналогично текущему S3 config).

### Mount

```bash
studiofs mount my-disk Z:
# или на Linux:
studiofs mount my-disk /mnt/studio
```

При mount клиент:
1. Читает `FormatInfo` из локального redb → видит `backend = "Postgres"`
2. Подключается к PG (`tokio-postgres` connection pool)
3. Делает catch-up: `SELECT ... FROM event_log WHERE vol=$vol_id AND seq_id > $last_seq`
4. Запускает LISTEN на `studiofs_vol_{vol_id}`
5. Аллоцирует ID диапазон: `UPDATE counters SET value = value + 8192 ... RETURNING ...` (большой блок для WAN)
6. Монтирует VFS (FUSE/WinFSP)

---

## 7. Что меняется на клиенте (`studiofs-meta`)

### Удаляется

| Файл / Модуль | Строк | Что делал |
|---|---|---|
| `tonbo_engine.rs` | ~3510 | S3 manifest, 3-way merge, CAS, NATS publish, range alloc |
| NATS логика в VFS | ~370 | `background_invalidation_listener` (заменяется PG LISTEN) |

### Остаётся без изменений

- `redb_engine.rs` (~3050 строк) — L1 кэш, все reads идут сюда
- `schema.rs` — типы
- `events.rs` — `MetaEvent` enum (используется для event_log payload)
- `engine.rs` — трейт `MetaEngine`

### Создаётся

| Файл | Назначение |
|------|-----------|
| `pg_engine.rs` | `PgMetaEngine`: impl `MetaEngine`, PG connection pool, write-back, LISTEN sync |

**`PgMetaEngine` логика:**
- **Read**: из локального redb (мгновенно)
- **Write**: в локальный redb + pending_queue → фоновый воркер → PG transaction
- **Sync**: `LISTEN studiofs_vol_N` → poll event_log → apply delta в redb
- **ID alloc**: `UPDATE counters ... RETURNING` (batch 8192/131072 — увеличено для WAN)
- **Lock**: `pg_try_advisory_lock(vol * 1_000_000 + ino)` (~40-300мс WAN)
- **Catch-up**: `SELECT FROM event_log WHERE seq > last_known`
- **Offline**: redb работает, мутации копятся, sync при reconnect

### Зависимости (добавляются)

```toml
[dependencies]
tokio-postgres = { version = "0.7", features = ["with-serde_json-1"] }
deadpool-postgres = "0.14"      # connection pool
postgres-types = "0.2"
```

---

## 8. Edge Cases и решения

### A. Конфликты метаданных

| Case | Решение |
|------|--------|
| **Create same filename** | PG UNIQUE → second insert fails → rollback из redb |
| **Rename к одной цели** | PG UNIQUE OR UPDATE timing → serialized |
| **Rename dir + create inside** | PG serialized: rename first → create gets ENOENT, или наоборот |
| **Overwrite same file** | LWW: оба UPDATE inodes проходят, последний коммит = финальный |
| **Hardlink + concurrent delete** | PG nlink-- атомарно. При nlink=0 → cleanup |

### B. Блоки и data path

| Case | Решение |
|------|--------|
| **File visible, blocks not in S3 yet** | Read returns 0/partial. FetchScheduler retry с backoff |
| **Partial write + crash** | size обновляется только для committed blocks. Uncommitted blocks = orphans → janitor GC |
| **GC race (block deleted but cached)** | grace_period >> sync delay. Janitor проверяет `file_blocks` перед delete |

### C. Locks

| Case | Решение |
|------|--------|
| **Lock + offline** | PG session advisory lock auto-released on disconnect (~60-120с, tcp_keepalives) |
| **Lock expiry during upload** | file_locks.expires_at + client auto-renew |
| **rmdir with locked file** | unlink(locked) → ELOCKED. Остальные файлы удаляются. rmdir → ENOTEMPTY |

### D. Multi-volume

| Case | Решение |
|------|--------|
| **Volume isolation** | Все таблицы keyed by `vol` (composite PK). Advisory lock = `vol * 1M + ino` |
| **Volume lifecycle** | `studiofs init` creates volume. `studiofs destroy` drops rows. PG schema shared |
| **LISTEN routing** | Channel per volume: `studiofs_vol_1`, `studiofs_vol_2` → клиент слушает только свой |

### E. Offline + reconnect

| Case | Решение |
|------|--------|
| **Accumulated offline mutations** | Pending queue в redb. На reconnect: batch → PG. Conflicts → per-op rollback |
| **ID exhaustion offline** | 8192 inodes + 131072 blocks = дни offline работы. При исчерпании: FS read-only until reconnect |
| **Split brain (оба offline)** | PG serializes на reconnect. Один из create same-name проигрывает |
| **Event log trimmed** | If `last_known_seq < min(event_log.seq)` → full rebuild from PG tables |

### F. Production concerns

| Case | Решение |
|------|--------|
| **PG down** | Клиент read-only из redb. Writes queue в pending. Reconnect + flush |
| **PG backup** | Standard: `pg_dump` / pg_basebackup / WAL archiving |
| **PG scale** | Для <50 клиентов: single PG instance достаточно. Read replicas при необходимости |
| **Case sensitivity** | Volume-level setting в `volumes.settings`. PG BYTEA dentries = case-sensitive by default |
| **Windows file handles** | WinFSP deferred delete (POSIX-style on top). Lobby знает что nlink=0 + open handles |

---

## 9. План реализации

### Фаза 0: Reference Baseline (текущая задача)

1. ✅ Тест-утилита `tools/fstest/fstest.py`
2. Запуск на LucidLink (2 машины) → JSON report = эталон поведения
3. Запуск на текущем StudioFS (Tonbo) → JSON report = текущие баги

### Фаза 1: PgMetaEngine MVP

4. Docker compose + SQL schema (auto-init)
5. `pg_engine.rs` — connection pool, write-through (без batch), LISTEN sync
6. `studiofs init --postgres` → auto-create tables + volume
7. Интеграционный тест: 2 клиента, разные директории, 0 потерь
8. `fstest.py` на StudioFS+PG → сравнение с LucidLink baseline

### Фаза 2: Write-back + Performance

9. Write-back batching (pending queue → PG tx, 100 ops / 2s WAN-optimized)
10. Advisory locks integration (flock → pg_try_advisory_lock)
11. ID range allocation (batch 8192/131072 — WAN-sized)
12. Event log compaction (janitor)

### Фаза 3: Stress + Production readiness

13. 5-client stress test: конкурентные записи в одну директорию
14. Offline/reconnect тест: kill network → queue → reconnect → verify
15. `fstest.py` full suite на StudioFS+PG → pass rate >= LucidLink