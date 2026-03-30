# 🔍 Аудит StudioFS-CLI по Rust Best Practices
# Дата: 2026-03-30

## КРИТИЧЕСКИЕ ПРОБЛЕМЫ

### 2. 🔴 ~1600 вызовов `unwrap()` / `expect()` в production-коде

Общая статистика:
- .unwrap() — 872 вызова в production-коде
- .expect()  — 714 вызовов в production-коде

Топ файлов-нарушителей:

| Файл                              | .unwrap() | .expect() | Итого |
|-----------------------------------|-----------|-----------|-------|
| studiofs-meta/src/redb_engine.rs  | 508       | 6         | 514   |
| studiofs-meta/src/pg_engine.rs    | 4         | 578       | 582   |
| studiofs-vfs/src/ingress_wal.rs   | 86        | —         | 86    |
| studiofs-vfs/src/vfs.rs           | 38        | 74        | 112   |
| studiofs-cache/src/block_cache.rs | 36        | 2         | 38    |
| studiofs-cache/src/l2_disk.rs     | 34        | 1         | 35    |
| studiofs/src/cli/status.rs        | 36        | —         | 36    |
| studiofs/src/main.rs              | 36        | —         | 36    |
| studiofs-cache/src/upload_queue.rs| 21        | 2         | 23    |

Особенно критичные паттерны:

a) Парсинг бинарных данных без валидации (redb_engine.rs, ingress_wal.rs):
   ```rust
   let parent = u64::from_le_bytes(key[..8].try_into().unwrap());
   ```
   Повреждение данных → panic вместо graceful error.

b) Весь PostgreSQL-движок (pg_engine.rs — 578 .expect()):
   Системная проблема — ошибки соединения/операций вызывают panic
   вместо возврата Result. Пример:
   ```rust
   .expect("spawn pg-meta-poll thread")
   .expect("connect")
   .expect("reopen pg engine")
   ```

c) VFS-операции с коллекциями (vfs.rs):
   ```rust
   pending.get_mut(&(ino, block_index)).unwrap();  // строка 10676
   pending.remove(&(ino, block_index)).unwrap();    // строка 10692
   components.last().unwrap().as_bytes().to_vec();  // строка 449
   ```

### 3. 🔴 Lock poisoning — 11 мест с `.lock().unwrap()`

Если какой-либо поток запаникует, держа мьютекс, все последующие
.lock().unwrap() тоже запаникуют каскадом:

- studiofs-winfsp/src/lib.rs:291         — SHUTDOWN_TX.lock().unwrap()
- studiofs-vfs/src/vfs.rs:5223          — kernel_invalidator.read().unwrap()
- studiofs-vfs/src/vfs.rs:7970          — kernel_invalidator.write().unwrap()
- studiofs-vfs/src/vfs.rs:7978          — kernel_invalidator.write().unwrap()
- studiofs-meta/src/pg_engine.rs:624    — poll_handle.lock().unwrap()
- studiofs/src/cli/umount.rs:170        — pid_state().lock().unwrap()
- studiofs/src/cli/umount.rs:178        — pid_state().lock().unwrap()
- studiofs/src/cli/umount.rs:206        — pid_state().lock().unwrap()
- studiofs/src/cli/umount.rs:223        — pid_state().lock().unwrap()

Рекомендация: заменить на
```rust
lock.lock().unwrap_or_else(|poisoned| {
    tracing::warn!("lock poisoned, recovering");
    poisoned.into_inner()
})
```

--------------------------------------------------------------------------------

## СЕРЬЁЗНЫЕ ПРОБЛЕМЫ

### 4. 🟠 `std::sync::Mutex` в async-контексте

studiofs-cache/src/block_cache.rs:223,225:
```rust
last_checkpoint_signature: std::sync::Mutex<Option<(u32, u32)>>,
l2_read_breaker_open_until: std::sync::Mutex<Option<Instant>>,
```
Если задача заблокируется на этом мьютексе, она заблокирует весь
executor-тред Tokio. Нужно tokio::sync::Mutex или Atomic.

### 5. 🟠 Нет `#![forbid(unsafe_code)]` ни в одном крейте

Ни один крейт (даже чисто-логические studiofs-meta, studiofs-data,
studiofs-cache) не запрещает unsafe. По best practices, крейты без
необходимости в unsafe должны иметь #![forbid(unsafe_code)].

Рекомендация: добавить в lib.rs следующих крейтов:
- studiofs-meta
- studiofs-data
- studiofs-cache
- studiofs-vfs

### 6. 🟠 `tokio = { features = ["full"] }` — избыточно

Workspace Cargo.toml:
```toml
tokio = { version = "1", features = ["full"] }
```

Фактически используются: rt-multi-thread, time, sync, macros, fs, io-util.
НЕ используются: signal, process, net.

Замена на точечные фичи ускорит компиляцию на ~15-20%:
```toml
tokio = { version = "1", features = ["rt-multi-thread", "time", "sync", "macros", "fs", "io-util"] }
```

### 7. 🟠 Нет `cargo audit` в CI

Workflow .github/workflows/rust.yml запускает fmt, clippy, test, doc —
но НЕ проверяет зависимости на уязвимости (cargo audit / cargo deny).
Это рекомендуемая best practice для production-проектов.

### 8. 🟠 CI только на Ubuntu

CI запускается только на ubuntu-latest, хотя проект поддерживает:
- Windows (крейт studiofs-winfsp)
- macOS (крейт studiofs-fuse)

Нет matrix-стратегии для кросс-платформенного тестирования.
Windows-специфичный код (winfsp) никогда не тестируется в CI.

--------------------------------------------------------------------------------

## УМЕРЕННЫЕ ПРОБЛЕМЫ

### 9. 🟡 Массовое отсутствие документации

30+ публичных функций без /// doc-комментариев (практически весь CLI-модуль):
- cli/destroy.rs:8       — pub fn run
- cli/discovery.rs:48    — pub fn run
- cli/dump_load.rs:130   — pub fn dump
- cli/dump_load.rs:272   — pub fn load
- cli/fsck.rs:493        — pub fn run
- cli/init.rs:316        — pub fn run
- cli/mount.rs:364       — pub fn run
- cli/settings.rs:82     — pub fn load_studiofs_config
- cli/settings.rs:130    — pub fn resolve_studiofs_config_for_mount
- cli/status.rs:167      — pub fn run
- cli/utils.rs:4         — pub fn get_default_meta_dir
- cli/utils.rs:109       — pub fn is_drive_root
  ...и ещё ~20

20+ публичных типов без документации:
- cli/settings.rs:13       — pub struct StudioFsConfig
- cli/mount.rs:34          — pub enum L2Profile
- cache/block_cache.rs:136 — pub enum CacheReadLevel
- cache/block_cache.rs:143 — pub struct CacheReadTrace
- cache/metrics.rs:30      — pub struct LatencyHistogramSnapshot
- cache/metrics.rs:36      — pub struct LatencyHistogram
- cache/metrics.rs:81      — pub struct IoStatsSnapshot
  ...и ещё ~15

Doc-тесты: только 2 примера на весь проект (aimd.rs и object.rs).

### 10. 🟡 Клонирование данных в горячих путях кэша

studiofs-cache/src/block_cache.rs — data.clone() клонирует потенциально
большие блоки данных (Vec<u8> / Bytes):
- строка 508:  data.clone() перед promote_to_l1()
- строка 645:  data.clone() перед put_bytes_with_priority()
- строка 1130: job.data.clone() для async write
- строка 343,350,355: self.clone() в tokio::spawn

Рекомендация: использовать Arc<[u8]> или Bytes для zero-copy sharing.

### 11. 🟡 Нет rustfmt.toml / clippy.toml

Проект использует дефолтные настройки форматирования и линтинга.
Рекомендуется зафиксировать конфигурацию явно для командной работы.

### 12. 🟡 `#[allow(dead_code)]` в production-коде

- studiofs-fuse/src/lib.rs:42        — #[allow(unused_variables)] foreground
- studiofs-meta/src/pg_engine.rs:633 — #[allow(dead_code)]
  (13 остальных — в тестовом коде vfs_test.rs, допустимо)

### 13. 🟡 unsafe без SAFETY-комментария

studiofs-winfsp/src/context.rs:88-90:
```rust
unsafe fn as_mut_slice<'a>(self) -> &'a mut [u8] {
    unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
}
```
Нет SAFETY-комментария с описанием инвариантов (ptr валиден, выровнен,
эксклюзивно владеется на время 'a).

--------------------------------------------------------------------------------

## ХОРОШИЕ ПРАКТИКИ ✅

| Что                                                           | Статус       |
|---------------------------------------------------------------|--------------|
| thiserror для доменных ошибок в библиотеках, anyhow в CLI     | ✅ Правильно |
| Lock ordering инфраструктура (lock_order.rs, 10 тиров)        | ✅ Отлично   |
| parking_lot::Mutex только в синхронных контекстах              | ✅ Правильно |
| tokio::task::block_in_place для блокирующих вызовов в async    | ✅ Правильно |
| Нет todo!() / unimplemented!() в production                   | ✅ Чисто     |
| .env в .gitignore, есть .env.example                          | ✅ Правильно |
| Clippy проходит чисто с -D warnings                           | ✅ Строго    |
| 522 unit теста + 6 файлов интеграционных тестов                | ✅ Покрытие  |
| Workspace с чётким разделением (7 крейтов)                    | ✅ Чисто     |
| unsafe в WinFSP обёрнут в safe абстракции с SAFETY-коммент.   | ✅ Правильно |
| Нет println!/dbg! в non-CLI коде (только user-facing output)  | ✅ Чисто     |

--------------------------------------------------------------------------------

## ПРИОРИТЕТЫ ИСПРАВЛЕНИЙ

Приоритет 1 (тривиально, высокий импакт):
  2. Добавить cargo audit в CI - done
  3. Добавить #![forbid(unsafe_code)] в крейты без unsafe
  4. Сузить фичи tokio

Приоритет 2 (средняя сложность):
  5. Заменить .lock().unwrap() на обработку poisoning (11 мест)
  6. Заменить std::sync::Mutex → Atomic/tokio::sync в async-коде (2 поля)
  7. Добавить SAFETY-комментарий к unsafe fn as_mut_slice
  8. Добавить CI matrix для Windows/macOS

Приоритет 3 (большой объём работы):
  9. Постепенно заменять .unwrap()/.expect() в redb_engine (508) и pg_engine (578)
  10. Добавить документацию к публичным API
  11. Рефакторинг клонирования в горячих путях кэша
  12. Добавить doc-тесты к публичным типам и функциям
