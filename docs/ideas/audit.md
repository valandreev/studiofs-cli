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

--------------------------------------------------------------------------------

## СЕРЬЁЗНЫЕ ПРОБЛЕМЫ

### 5. 🟠 CI только на Ubuntu

CI запускается только на ubuntu-latest, хотя проект поддерживает:
- Windows (крейт studiofs-winfsp)
- macOS (крейт studiofs-fuse)

Нет matrix-стратегии для кросс-платформенного тестирования.
Windows-специфичный код (winfsp) никогда не тестируется в CI.

--------------------------------------------------------------------------------

## УМЕРЕННЫЕ ПРОБЛЕМЫ

### 7. 🟡 Массовое отсутствие документации

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

### 8. 🟡 Клонирование данных в горячих путях кэша

studiofs-cache/src/block_cache.rs — data.clone() клонирует потенциально
большие блоки данных (Vec<u8> / Bytes):
- строка 508:  data.clone() перед promote_to_l1()
- строка 645:  data.clone() перед put_bytes_with_priority()
- строка 1130: job.data.clone() для async write
- строка 343,350,355: self.clone() в tokio::spawn

Рекомендация: использовать Arc<[u8]> или Bytes для zero-copy sharing.

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

Приоритет 1 (средняя сложность):
  5. Добавить CI matrix для Windows/macOS

Приоритет 2 (большой объём работы, требует итеративного подхода):
  2. Постепенно заменять .unwrap()/.expect() в redb_engine (508) и pg_engine (578)
  7. Добавить документацию к публичным API
  8. Рефакторинг клонирования в горячих путях кэша (block_cache.rs)
