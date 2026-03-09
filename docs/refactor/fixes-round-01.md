**PR-11: Pending Read Fast Path**
Цель: локальные чтения файла с `pending incarnation` не должны ждать `publish` в S3 и не должны проваливаться в медленный spill-path, если блок уже materialized локально.

Файлы:
- [vfs.rs](/E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs.rs:4268)
- [materialize.rs](/E:/dev/studiofs-cli/crates/studiofs-vfs/src/materialize.rs:23)
- [block_cache.rs](/E:/dev/studiofs-cli/crates/studiofs-cache/src/block_cache.rs:294)
- [vfs_test.rs](/E:/dev/studiofs-cli/crates/studiofs-vfs/tests/vfs_test.rs)

Scope:
- Изменить порядок в `read_block_or_pending()` так, чтобы для `pending incarnation` путь был: `pending_blocks -> dirty_mem -> live materialized extent/block_cache -> spill -> committed base`.
- Использовать уже существующий live extent state из PR-05/06 как источник `object_id` для локально materialized pending-блоков, не полагаясь на committed metadata.
- Оставить `spill` только fallback-путем для блоков, которые еще не materialized.
- Добавить короткую retention-политику для active foreground read по pending extents, чтобы во время последовательного чтения они не демотировались слишком агрессивно.
- Не трогать shutdown, WinFSP cleanup и формат INFO-логов.

Инвариант после PR:
- accepted local data читается быстро сразу после materialization, независимо от того, завершен upload/publish или нет.

Тесты:
- `local_reads_of_pending_incarnation_prefer_materialized_extent_over_spill`
- `large_uploading_file_remains_on_local_fast_read_path_before_publish`
- `partial_overwrite_reads_pending_and_committed_blocks_consistently`
- `pending_local_reads_survive_s3_offline_after_materialization`

**PR-12: Bounded Shutdown And Idempotent Cleanup**
Цель: `Ctrl+C` и unmount должны завершаться за ограниченное время, а cleanup/delete race не должен оставлять ghost entries после remount.

Файлы:
- [vfs.rs](/E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs.rs:4914)
- [vfs.rs](/E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs.rs:5315)
- [context.rs](/E:/dev/studiofs-cli/crates/studiofs-winfsp/src/context.rs:745)
- [vfs_test.rs](/E:/dev/studiofs-cli/crates/studiofs-vfs/tests/vfs_test.rs)
- [main.rs](/E:/dev/studiofs-cli/crates/studiofs/src/main.rs) при необходимости, если сигнал/shutdown orchestration живет там

Scope:
- Ввести единый shutdown token и явный join/await всех фоновых задач VFS.
- Перед `flush_metadata()` останавливать producers: stats loop, GC, janitor, prefetch/upload dispatch и любые фоновые циклы, которые еще могут делать новые мутации.
- Сделать shutdown двухфазным: `quiesce background work -> drain/persist -> final close`.
- В WinFSP cleanup path сделать delete/rmdir idempotent к stale race-состояниям.
- Ошибки вида `NotFound`, `IsADirectory`, `NotADirectory` в cleanup обрабатывать как benign race, если namespace уже ушел в новое состояние.
- По возможности опираться на handle/inode truth, а не на повторное path-based разрешение как единственный источник истины.
- Не трогать upload journal warnings и лог-косметику.

Инвариант после PR:
- после начала unmount не стартуют новые фоновые мутации, выход не виснет, stale cleanup не оживляет удаленные директории/файлы.

Тесты:
- `shutdown_with_pending_uploads_completes_within_bound`
- `ctrl_c_during_small_file_churn_does_not_hang`
- `cleanup_delete_race_directory_is_idempotent`
- `remount_after_delete_shutdown_does_not_resurrect_ghost_entry`
- `unmount_after_recursive_delete_preserves_consistent_namespace`

**PR-13: Pending Upload Index And Quiet Logs**
Цель: убрать false-positive `corrupt upload journal`, перестать сканировать `upload/` как runtime truth и сделать INFO-лог компактным.

Файлы:
- [upload_queue.rs](/E:/dev/studiofs-cli/crates/studiofs-cache/src/upload_queue.rs:235)
- [block_cache.rs](/E:/dev/studiofs-cli/crates/studiofs-cache/src/block_cache.rs:402)
- [commit.rs](/E:/dev/studiofs-cli/crates/studiofs-vfs/src/commit.rs:20)
- [vfs.rs](/E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs.rs:2103)
- [vfs.rs](/E:/dev/studiofs-cli/crates/studiofs-vfs/src/vfs.rs:4914)
- [upload.rs](/E:/dev/studiofs-cli/crates/studiofs-vfs/src/upload.rs:264)

Scope:
- Ввести in-memory index pending uploads, обновляемый на enqueue/complete/cancel/requeue.
- На startup один раз rebuild'ить этот index из journal, дальше не использовать `read_dir` как hot-path источник истины.
- Перевести `commit coordinator`, periodic stats и helper'ы с `snapshot_pending_uploads()` на этот index.
- `ENOENT` при scan/open race трактовать как нормальную concurrent completion race, а не как `corrupt upload journal`.
- WARN оставлять только для реально битых journal entries: parse/decode mismatch, malformed name, invalid payload.
- INFO-лог упростить: в idle-состоянии достаточно `pending_uploads=N`; полные counters перевести в DEBUG или печатать только при non-zero/error state.
- Не менять semantics upload/publish и shutdown lifecycle.

Инвариант после PR:
- ложные warning'и исчезают, periodic logging перестает засорять стандартный лог, runtime path перестает зависеть от сканирования `upload/`.

Тесты:
- `pending_upload_snapshot_ignores_concurrent_completion_enoent`
- `startup_rebuilds_pending_upload_index_from_journal`
- `corrupt_upload_journal_warns_only_on_invalid_entry`
- `idle_upload_info_log_is_compact`
- `commit_coordinator_uses_pending_index_without_directory_scan`

**Почему именно такой порядок**
1. `PR-11` бьет по главному пользовательскому симптому и имеет самый маленький blast radius.
2. `PR-12` закрывает correctness на shutdown/remount и убирает риск ghost-state.
3. `PR-13` после этого чистит observability и остаточный hot-path шум, не смешивая его с функциональными фикcами.
