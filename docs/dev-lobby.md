# studiofs-lobby — Design Document (v3)

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

## 1. Философия: PostgreSQL как lobby

Вместо написания custom lobby — используем **PostgreSQL**.

**Почему PostgreSQL, а не custom Rust-сервис:**
- ACID транзакции из коробки (inode + dentry + counter — атомарно)
- `LISTEN/NOTIFY` + trigger = push-нотификации без NATS
- Advisory locks = файловые блокировки с автоматическим release при disconnect
- `SEQUENCE` / atomic counter = ID allocation без CAS
- Знакомый стек → доверие пользователей
- `docker-compose up` и готово

**Что убираем:** custom lobby binary, gRPC протокол, NATS (PG NOTIFY заменяет для метаданных).

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

### Зачем оставить redb локально

- VFS hot path (`lookup` на каждый компонент пути) требует sub-millisecond. PG по сети = 1-5мс/query
- Offline режим: redb работает без сети, мутации копятся в локальной очереди
- Уже работает: 3050 строк `redb_engine.rs` без изменений

---

## 3. PostgreSQL Schema

### Docker Compose

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:17-alpine
    restart: unless-stopped
    environment:
      POSTGRES_DB: studiofs
      POSTGRES_USER: studiofs
      POSTGRES_PASSWORD: studiofs  # override in production
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    command: >
      postgres
        -c shared_buffers=256MB
        -c work_mem=16MB
        -c max_connections=100
        -c synchronous_commit=on

volumes:
  pgdata:
```

Запуск: `docker compose up -d`

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

**Atomic file create** (полная транзакция в одном SQL round-trip):
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

**Range-based ID allocation** (один round-trip):
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

**Catch-up после reconnect:**
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

### Конфликтные сценарии

| Сценарий | PG behaviour | Клиент проигравшего |
|----------|-------------|-------------------|
| create same filename | UNIQUE violation | rollback dentry + inode из redb |
| rename к одной цели | UNIQUE violation или кто первый | rollback rename |
| delete + create в одной dir | Serialized: порядок определяет PG | может получить ENOENT |
| overwrite same file (LWW) | Оба UPDATE проходят, последний коммит остаётся | норма: LWW |
| lock + modify | advisory lock блокирует второго | ждёт release или таймаут |

### Операции без конфликтов

Разные директории, разные файлы — **разные строки в PG** → никаких блокировок, полный параллелизм.

### Пессимистичная блокировка

Для критических файлов (проекты Nuke, DaVinci Resolve):

1. Клиент: `SELECT pg_try_advisory_lock(vol * 1000000 + ino)` (~1-5мс LAN)
2. Если `true` → лок получен. PG session держит его автоматически
3. Другой клиент: `pg_try_advisory_lock` → `false` → `ELOCKED`
4. При закрытии файла: `pg_advisory_unlock(...)` или автоматически при disconnect
5. **Нет TTL/heartbeat проблем:** PG сам обнаруживает disconnected sessions за ~15-30с (tcp_keepalives)

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
4. Фоновый воркер:
   - Батчит мутации (до 50 штук или 1с, что раньше)
   - Отправляет в PG одной транзакцией: `BEGIN; ... COMMIT;`
   - При success: чистит pending_queue
   - При conflict (UNIQUE violation и т.п.): rollback несовместимых мутаций из redb
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
5. Аллоцирует ID диапазон: `UPDATE counters SET value = value + 4096 ... RETURNING ...`
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
- **ID alloc**: `UPDATE counters ... RETURNING` (batch 4096/65536)
- **Lock**: `pg_try_advisory_lock(vol * 1_000_000 + ino)`
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
| **Lock + offline** | PG session advisory lock auto-released on disconnect (~15-30с) |
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
| **ID exhaustion offline** | 4096 inodes + 65536 blocks = часы работы. При исчерпании: FS read-only until reconnect |
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

9. Write-back batching (pending queue → PG tx)
10. Advisory locks integration (flock → pg_try_advisory_lock)
11. ID range allocation (batch 4096/65536)
12. Event log compaction (janitor)

### Фаза 3: Stress + Production readiness

13. 5-client stress test: конкурентные записи в одну директорию
14. Offline/reconnect тест: kill network → queue → reconnect → verify
15. `fstest.py` full suite на StudioFS+PG → pass rate >= LucidLink