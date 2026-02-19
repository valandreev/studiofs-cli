# StudioFS → LucidLink-like: Implementation Plan

> Задачи упорядочены по зависимостям: каждая фаза опирается на
> результаты предыдущей. Ничего нельзя переставить — лишь добавить
> параллельные задачи внутри фазы, если зависимостей нет.

---

## Фаза 0 — Удаление Legacy кода

**Сложность: Простой** | **Зависит от:** — | **Дней:** 1

> **Зачем первым:** старые таблицы CHUNKS / PENDING_SLICES / DELETED_SLICES
> конфликтуют с новой логикой `rename`, `unlink`, атомарных транзакций.
> Мусорный код затруднит отладку всего последующего. Обратная
> совместимость не нужна → чистим сейчас.

### 0.1 — Удалить deprecated таблицы из `redb_engine.rs`

**Файл:** `crates/studiofs-meta/src/redb_engine.rs`

Удалить объявления:
```rust
const CHUNKS_TABLE: TableDefinition<...>       // строки ~31
const PENDING_SLICES: TableDefinition<...>     // строки ~34
const DELETED_SLICES: TableDefinition<...>     // строки ~37
```

Из `RedbMetaEngine::open()` убрать:
```rust
let _ = txn.open_table(CHUNKS_TABLE);
let _ = txn.open_table(PENDING_SLICES);
let _ = txn.open_table(DELETED_SLICES);
```

Удалить реализации методов: `get_chunk_slices`, `append_chunk_slice`,
`set_chunk_slices`, `remove_file_chunks`, `next_slice_id`,
`add_pending_slice`, `remove_pending_slice`, `get_pending_slices`,
`add_deleted_slice`, `remove_deleted_slice`, `get_deleted_slices`,
`get_data_root`, `set_data_root`.

### 0.2 — Удалить deprecated типы из `types.rs`

**Файл:** `crates/studiofs-meta/src/types.rs`

Удалить: `Slice`, `ChunkKey`, `CHUNK_SIZE`, `DirEntry` (старый), константы
связанные со слайсами. Убрать `#[allow(deprecated)]` из всех файлов.

### 0.3 — Удалить deprecated методы из `engine.rs` trait

**Файл:** `crates/studiofs-meta/src/engine.rs`

Удалить из трейта: `get_chunk_slices`, `append_chunk_slice`,
`set_chunk_slices`, `remove_file_chunks`, `next_slice_id`,
`add_pending_slice`, `remove_pending_slice`, `get_pending_slices`,
`add_deleted_slice`, `remove_deleted_slice`, `get_deleted_slices`,
`get_data_root`, `set_data_root`.

В `unlink()` default impl заменить `self.remove_file_chunks(ino)` на
`self.remove_file_blocks(ino)` (уже должно быть в VFS слое — проверить).

**Итог фазы:** codebase уменьшается на ~400 строк, нет больше
`#[allow(deprecated)]`, таблицы в redb только актуальные.

---

## Фаза 1 — Правильный маппинг ошибок

**Сложность: Простой** | **Зависит от:** Phase 0 | **Дней:** 0.5

> **Зачем вторым:** самое дешёвое изменение с самым большим эффектом на
> отладку. Сейчас FUSE возвращает ENOENT/EIO для всего. После этого
> шага приложения получат понятные errno, и мы сами будем видеть
> реальные причины сбоев в логах.

### 1.1 — Маппинг `MetaError` → errno в FUSE

**Файл:** `crates/studiofs-fuse/src/fs.rs`

Добавить хелпер:
```rust
fn meta_err_to_errno(e: &VfsError) -> i32 {
    use studiofs_meta::MetaError;
    match e {
        VfsError::Meta(MetaError::InodeNotFound(_))       => libc::ENOENT,
        VfsError::Meta(MetaError::EntryNotFound { .. })   => libc::ENOENT,
        VfsError::Meta(MetaError::EntryExists { .. })     => libc::EEXIST,
        VfsError::Meta(MetaError::NotADirectory(_))       => libc::ENOTDIR,
        VfsError::Meta(MetaError::IsADirectory(_))        => libc::EISDIR,
        VfsError::Meta(MetaError::DirNotEmpty(_))         => libc::ENOTEMPTY,
        VfsError::Meta(MetaError::NotFormatted)           => libc::ENODEV,
        VfsError::BlockNotFound(_, _)                     => libc::EIO,
        _                                                 => libc::EIO,
    }
}
```

Заменить все `reply.error(libc::ENOENT)` и `reply.error(libc::EIO)` на
`reply.error(meta_err_to_errno(&e))` во всех FUSE callback-ах.

### 1.2 — Маппинг `MetaError` → NTSTATUS в WinFSP

**Файл:** `crates/studiofs-winfsp/src/context.rs`

Функция `vfs_to_winfsp_err()` уже имеет частичный маппинг. Расширить:
```rust
MetaError::IsADirectory(_)  => STATUS_FILE_IS_A_DIRECTORY,   // 0xC00000BA
MetaError::NotFormatted     => STATUS_UNRECOGNIZED_VOLUME,    // 0xC000014F
```

Добавить эти NTSTATUS константы в начало файла.

**Итог фазы:** все операции возвращают корректный errno. `cp`, `mv`,
`git`, `vim` показывают понятные ошибки.

---

## Фаза 2 — Атомарный Rename

**Сложность: Сложный** | **Зависит от:** Phase 0 | **Дней:** 1

> **Зачем третьим:** атомарный rename — основа crash-safe сохранения.
> DaVinci, Premiere, After Effects сохраняют через `write→tmp→rename`.
> Сейчас rename — 4 отдельные redb-транзакции; crash в середине = потеря файла.
> Требует тщательного обхода logic overwrite files/dirs, nlink management.

### 2.1 — Перенести `rename` из default trait impl в `redb_engine.rs`

**Файл:** `crates/studiofs-meta/src/engine.rs`

Убрать `rename()` из default impl трейта (или сделать его делегирующим
к абстрактному методу). В трейт добавить:
```rust
fn rename_atomic(
    &self,
    parent: Ino,
    name: &[u8],
    new_parent: Ino,
    new_name: &[u8],
) -> MetaResult<Option<Ino>>;  // returns displaced ino if any
```

### 2.2 — Реализовать атомарный rename в redb

**Файл:** `crates/studiofs-meta/src/redb_engine.rs`

```rust
fn rename_atomic(&self, parent: Ino, name: &[u8], new_parent: Ino, new_name: &[u8])
    -> MetaResult<Option<Ino>>
{
    let txn = self.db.begin_write()?;
    let displaced_ino: Option<Ino>;
    {
        let mut dentries = txn.open_table(DENTRIES_TABLE)?;
        let mut inodes   = txn.open_table(INODES_TABLE)?;

        // 1. Fetch and remove source entry
        let src_key = encode_dentry_key(parent, name);
        let src_val = dentries.remove(src_key.as_slice())?
            .ok_or(MetaError::EntryNotFound { parent, name: str::from_utf8(name).unwrap_or("?").to_string() })?;
        let src_entry: DirEntryValue = decode(src_val.value())?;

        // 2. Check destination
        let dst_key = encode_dentry_key(new_parent, new_name);
        displaced_ino = if let Some(dst_raw) = dentries.get(dst_key.as_slice())? {
            let dst_entry: DirEntryValue = decode(dst_raw.value())?;

            if dst_entry.kind == FileType::Directory {
                // Target must be empty dir
                let prefix = dentry_prefix(dst_entry.ino);
                let range = dentries.range(prefix.as_ref()..)?;
                if range.count() > 0 {
                    return Err(MetaError::DirNotEmpty(dst_entry.ino));
                }
                // Remove dir inode
                inodes.remove(&dst_entry.ino)?;
            } else {
                // Overwrite file: decrement nlink, mark for GC if 0
                let mut attr: InodeAttr = decode(
                    inodes.get(&dst_entry.ino)?.unwrap().value()
                )?;
                attr.nlink = attr.nlink.saturating_sub(1);
                if attr.nlink == 0 {
                    inodes.remove(&dst_entry.ino)?;
                } else {
                    inodes.insert(&dst_entry.ino, encode(&attr)?.as_slice())?;
                }
            }
            dentries.remove(dst_key.as_slice())?;
            Some(dst_entry.ino)
        } else {
            None
        };

        // 3. Add destination entry
        dentries.insert(dst_key.as_slice(), src_val.value())?;

        // 4. Update directory nlinks if moved between dirs
        if src_entry.kind == FileType::Directory && parent != new_parent {
            // moved dir: update its parent field + nlink counts
            if let Some(raw) = inodes.get(&src_entry.ino)? {
                let mut attr: InodeAttr = decode(raw.value())?;
                attr.parent = new_parent;
                inodes.insert(&src_entry.ino, encode(&attr)?.as_slice())?;
            }
            Self::adjust_nlink_in_txn(&mut inodes, parent, -1)?;
            Self::adjust_nlink_in_txn(&mut inodes, new_parent, 1)?;
        }
    }
    txn.commit()?;

    // GC blocks for displaced file (nlink=0) is scheduled by VFS after this returns
    Ok(displaced_ino)
}
```

Добавить private helper `adjust_nlink_in_txn`.

### 2.3 — Обновить VFS и FUSE/WinFSP

**Файл:** `crates/studiofs-vfs/src/vfs.rs`

```rust
pub fn rename(&self, parent: Ino, name: &[u8], new_parent: Ino, new_name: &[u8])
    -> VfsResult<()>
{
    if let Some(displaced_ino) = self.meta.rename_atomic(parent, name, new_parent, new_name)? {
        // Schedule GC for displaced ino blocks
        self.evict_inode_from_caches(displaced_ino);
        let block_ids = self.meta.remove_file_blocks(displaced_ino)?;
        for id in block_ids {
            let ts = now_secs();
            self.meta.add_pending_deletion(id, ts)?;
        }
    }
    Ok(())
}
```

**Итог фазы:** rename атомарен, crash-safe. NLE-приложения могут
безопасно сохранять файлы.

---

## Фаза 3 — Handle Tracking

**Сложность: Средний–Сложный** | **Зависит от:** Phase 1 | **Дней:** 5

> **Зачем четвёртым:** от этого зависят phases 4–6 (open-unlink, O_APPEND,
> fsync per-fd, flock). Это самая большая из foundational задач.
> Содержит как простые элементы (fsync, O_APPEND) так и сложные (open-unlink race conditions).

### 3.1 — `HandleTable` в VFS (Простой–Средний, 1.5 дня)

**Файл:** `crates/studiofs-vfs/src/vfs.rs`

Добавить структуры во `VfsState`:
```rust
#[derive(Clone, Debug)]
pub struct OpenFile {
    pub ino: Ino,
    pub flags: OpenFlags,       // O_RDONLY | O_WRONLY | O_RDWR | O_APPEND
}

bitflags::bitflags! {
    pub struct OpenFlags: u32 {
        const RDONLY = 0x01;
        const WRONLY = 0x02;
        const RDWR   = 0x04;
        const APPEND = 0x08;
    }
}

struct HandleTable {
    next_fh: std::sync::atomic::AtomicU64,
    handles: parking_lot::RwLock<HashMap<u64, OpenFile>>,
    /// ino → count of currently open fds
    open_counts: Mutex<HashMap<Ino, u32>>,
    /// inodes that are unlinked while still open (pending final delete)
    orphans: Mutex<HashMap<Ino, ()>>,
}
```

Добавить `handle_table: Arc<HandleTable>` в `VfsState`.

Новые публичные методы VFS:
```rust
/// Called by FUSE/WinFSP on open().
pub fn open_file(&self, ino: Ino, flags: OpenFlags) -> VfsResult<u64>;

/// Called by FUSE/WinFSP on release() / close().
pub fn release_fh(&self, fh: u64) -> VfsResult<()>;

/// Called by FUSE on flush() (per-fd close).
pub fn flush_fh(&self, fh: u64) -> VfsResult<()>;
```

### 3.2 — Open-unlink семантика (Сложный, 1 день)

Когда `vfs.unlink()` вызывается для открытого файла (`open_count > 0`):
1. Удалить dentry (файл уже не виден в readdir/lookup).
2. НЕ удалять inode и блоки — добавить ino в `orphans`.
3. Обновить `InodeAttr` в redb: добавить флаг `PENDING_DELETE` в поле
   `mode` (или отдельная таблица `ORPHAN_INODES`).

В `release_fh()`:
```rust
let count = {
    let mut counts = self.handle_table.open_counts.lock();
    let c = counts.get_mut(&ino).unwrap();
    *c -= 1;
    *c
};

if count == 0 {
    let mut orphans = self.handle_table.orphans.lock();
    if orphans.remove(&ino).is_some() {
        // Final cleanup: evict caches + remove blocks + GC
        self.evict_inode_from_caches(ino);
        let block_ids = self.meta.remove_file_blocks(ino)?;
        for id in block_ids { ... add_pending_deletion }
        self.meta.remove_attr(ino)?;
    }
}
```

**Crash recovery:** добавить таблицу `ORPHAN_INODES: u64→()` в redb.
При старте VFS скананировать таблицу и удалять осиротевшие inode.

### 3.3 — fsync (Простой, 0.5 дня)

**Файл:** `crates/studiofs-vfs/src/vfs.rs`

```rust
pub async fn fsync(&self, ino: Ino, datasync: bool) -> VfsResult<()> {
    // 1. Flush pending write-buffer to dirty_mem
    self.flush(ino).await?;

    // 2. If !datasync — also ensure dirty attrs are committed to redb
    if !datasync {
        if let Some(attr) = self.state.dirty_attrs.lock().remove(&ino) {
            self.meta.set_attr(&attr)?;
            self.state.attr_read_cache.lock().remove(&ino);
        }
    }
    // S3 upload remains async — same as LucidLink behaviour.
    Ok(())
}
```

Наш `flush()` уже делает `pending_blocks → dirty_mem → slab`. `fsync`
просто вызывает его и синхронно сбрасывает dirty attrs.

### 3.4 — O_APPEND / O_TRUNC в open (Простой, 0.5 дня)

**Файл:** `crates/studiofs-vfs/src/vfs.rs`

В `open_file()`:
```rust
if flags.contains(OpenFlags::APPEND) {
    // stored in OpenFile, applied in write()
}
if raw_flags & O_TRUNC != 0 && !is_new_file {
    self.setattr(ino, SetattrArgs { size: Some(0), ..Default::default() })?;
}
```

В методе `write()` — если handle имеет флаг APPEND:
```rust
if open_file.flags.contains(OpenFlags::APPEND) {
    let attr = self.getattr(ino)?;
    // Override offset → always write at end
    offset = attr.size;
}
```

### 3.5 — FUSE/WinFSP интеграция handle tracking (Простой, 1 день)

**Файл:** `crates/studiofs-fuse/src/fs.rs`

```rust
fn open(&mut self, _req, ino, flags, reply) {
    let open_flags = parse_open_flags(flags);
    match self.vfs.open_file(ino, open_flags) {
        Ok(fh)  => reply.opened(fh, 0),
        Err(e)  => reply.error(meta_err_to_errno(&e)),
    }
}

fn release(&mut self, _req, ino, fh, _flags, _lock_owner, _flush, reply) {
    let _ = self.rt.block_on(self.vfs.release_fh(fh));
    reply.ok();
}

fn flush(&mut self, _req, ino, fh, _lock_owner, reply) {
    match self.rt.block_on(self.vfs.flush_fh(fh)) {
        Ok(())  => reply.ok(),
        Err(e)  => reply.error(meta_err_to_errno(&e)),
    }
}

fn fsync(&mut self, _req, ino, fh, datasync, reply) {
    match self.rt.block_on(self.vfs.fsync(ino, datasync)) {
        Ok(())  => reply.ok(),
        Err(e)  => reply.error(meta_err_to_errno(&e)),
    }
}
```

**Файл:** `crates/studiofs-winfsp/src/context.rs`

В `StudioFileHandle` добавить `fh: u64`.
В `open()` → `vfs.open_file(ino, OpenFlags::RDWR)`, сохранить fh в handle.
В `close()` → `vfs.release_fh(context.fh)`.

**Итог фазы:** полноценный open/close lifecycle. Open-unlink работает.
fsync не возвращает ENOSYS. O_APPEND корректен.

---

## Фаза 4 — setattr Timestamps + Symlinks + Open Flags

**Сложность: Простой–Средний** | **Зависит от:** Phase 0, Phase 1 | **Дней:** 3.5

> **Зачем пятым:** timestamps нужны для auto-reload в NLE.
> Symlinks нужны для VFX pipelines. Обе задачи независимы от Phase 3,
> но Phase 3 должна быть готова чтобы интеграционные тесты работали.

### 4.1 — `SetattrArgs` + расширенный `vfs.setattr` (Простой, 0.5 дня)

**Файл:** `crates/studiofs-vfs/src/vfs.rs`

```rust
#[derive(Debug, Default, Clone)]
pub struct SetattrArgs {
    pub size:  Option<u64>,
    pub mode:  Option<u16>,
    pub uid:   Option<u32>,
    pub gid:   Option<u32>,
    pub atime: Option<TimeSpec>,
    pub mtime: Option<TimeSpec>,
}

#[derive(Debug, Clone, Copy)]
pub enum TimeSpec {
    Now,
    Set(i64),  // nanoseconds since epoch
    Omit,
}
```

Метод `setattr(ino: Ino, args: SetattrArgs)`:
- `size` → truncate/extend (уже есть)
- `mode` → `attr.mode = mode; attr.touch_ctime()`
- `uid/gid` → обновить (права не проверяем — мы single-user root)
- `mtime` → `attr.mtime = resolve_timespec(mtime)`
- `atime` → `attr.atime = resolve_timespec(atime)` (храним, но читать не нужно)
- `ctime` → автоматически обновляется при любом изменении attr

### 4.2 — FUSE setattr передаёт все поля (Простой, 0.5 дня)

**Файл:** `crates/studiofs-fuse/src/fs.rs`

```rust
fn setattr(&mut self, _req, ino, mode, uid, gid, size, atime, mtime, ..., reply) {
    let args = SetattrArgs {
        size,
        mode: mode.map(|m| m as u16),
        uid,
        gid,
        atime: atime.map(fuser_time_to_timespec),
        mtime: mtime.map(fuser_time_to_timespec),
    };
    match self.vfs.setattr(ino, args) {
        Ok(attr) => reply.attr(&TTL, &to_fuse_attr(&attr, ...)),
        Err(e)   => reply.error(meta_err_to_errno(&e)),
    }
}
```

Добавить хелпер:
```rust
fn fuser_time_to_timespec(t: fuser::TimeOrNow) -> TimeSpec {
    match t {
        fuser::TimeOrNow::SpecificTime(st) => TimeSpec::Set(
            st.duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64
        ),
        fuser::TimeOrNow::Now => TimeSpec::Now,
    }
}
```

### 4.3 — WinFSP `set_basic_info` применяет timestamps (Простой, 0.5 дня)

**Файл:** `crates/studiofs-winfsp/src/context.rs`

```rust
fn set_basic_info(&self, context, _attrs, creation, last_access, last_write, change, out) {
    let args = SetattrArgs {
        atime: filetime_to_timespec(last_access),
        mtime: filetime_to_timespec(last_write),
        ..Default::default()
    };
    let attr = self.vfs.setattr(context.ino, args).map_err(vfs_to_winfsp_err)?;
    *out = attr_to_file_info(&attr);
    Ok(())
}

fn filetime_to_timespec(ft: u64) -> Option<TimeSpec> {
    if ft == 0 { return None; }
    const EPOCH_DIFF_NS: i64 = 116_444_736_000_000_000 * 100; // 100ns → ns
    let ns = ft as i64 * 100 - EPOCH_DIFF_NS;
    Some(TimeSpec::Set(ns))
}
```

### 4.4 — Symlinks: новая таблица в redb (Средний, 1 день)

**Файл:** `crates/studiofs-meta/src/redb_engine.rs`

```rust
/// Symlink targets: ino (u64) -> target path (&[u8])
const SYMLINKS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("symlinks");
```

Добавить таблицу в `open()` инициализацию.

Добавить в `MetaEngine` trait (`engine.rs`):
```rust
fn set_symlink_target(&self, ino: Ino, target: &[u8]) -> MetaResult<()>;
fn get_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>>;
```

В `unlink()` / `remove_attr()` — каскадно удалять запись из SYMLINKS_TABLE.

### 4.5 — Symlinks: VFS (Средний, 0.5 дня)

**Файл:** `crates/studiofs-vfs/src/vfs.rs`

```rust
pub fn symlink(&self, parent: Ino, name: &[u8], target: &[u8])
    -> VfsResult<InodeAttr>
{
    let attr = self.meta.create_symlink(parent, name, target)?;
    Ok(attr)
}

pub fn readlink(&self, ino: Ino) -> VfsResult<Vec<u8>> {
    let target = self.meta.get_symlink_target(ino)?;
    Ok(target)
}
```

В `create_symlink()` в MetaEngine default impl:
- Создать inode с `kind = FileType::Symlink`, `mode = 0o777`
- `attr.size = target.len() as u64` (POSIX: stat.st_size = len(target))
- Вызвать `set_symlink_target(ino, target)`

### 4.6 — Symlinks: FUSE (Простой, 0.5 дня)

**Файл:** `crates/studiofs-fuse/src/fs.rs`

```rust
fn symlink(&mut self, _req, parent, link_name, target, reply) {
    let name = link_name.to_string_lossy().as_bytes().to_vec();
    let tgt  = target.to_string_lossy().as_bytes().to_vec();
    match self.vfs.symlink(parent, &name, &tgt) {
        Ok(attr) => reply.entry(&TTL, &to_fuse_attr(&attr, ...), 0),
        Err(e)   => reply.error(meta_err_to_errno(&e)),
    }
}

fn readlink(&mut self, _req, ino, reply) {
    match self.vfs.readlink(ino) {
        Ok(target) => reply.data(&target),
        Err(e)     => reply.error(meta_err_to_errno(&e)),
    }
}
```

### 4.7 — Symlinks: WinFSP через reparse points (Средний, 0.5 дня)

**Файл:** `crates/studiofs-winfsp/src/context.rs`

В `VolumeParams`: включить `reparse_points(true)`.

Добавить callbacks `get_reparse_point` и `set_reparse_point`:
```rust
fn get_reparse_point(&self, context, file_name, buffer, size) -> winfsp::Result<u64> {
    let target = self.vfs.readlink(context.ino).map_err(vfs_to_winfsp_err)?;
    // Encode as IO_REPARSE_TAG_SYMLINK buffer
    let encoded = encode_symlink_reparse(&target);
    let len = encoded.len().min(*size as usize);
    buffer[..len].copy_from_slice(&encoded[..len]);
    Ok(len as u64)
}
```

Добавить хелпер `encode_symlink_reparse(target: &[u8]) -> Vec<u8>` —
формат буфера REPARSE_DATA_BUFFER для IO_REPARSE_TAG_SYMLINK.

**Итог фазы:** `utimensat`, `touch`, `cp -p` работают корректно.
VFX pipeline symlinks работают на Linux/macOS/Windows.

---

## Фаза 5 — File Locking (flock) + Hardlinks

**Сложность: Средний** | **Зависит от:** Phase 2, Phase 3 | **Дней:** 2.5

> **Зачем шестым:** flock зависит от handle tracking (Phase 3).
> Hardlinks зависят от чистой атомарной rename инфраструктуры (Phase 2).
> Эти две задачи можно делать параллельно.

### 5.1 — File Locking (flock, file-level) (Средний, 1.5 дня)

**Зачем:** SQLite (project DBs в DaVinci Resolve), Git, vim.
LucidLink использует global file locks. Для single-node нам достаточно
in-memory flock — семантика идентична.

**Файл:** `crates/studiofs-vfs/src/vfs.rs`

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum LockKind { Shared, Exclusive, Unlock }

struct FileLock {
    kind:  LockKind,
    owner: u64,          // lock_owner from FUSE (pid-based)
    count: u32,          // shared lock count
}

// В VfsState добавить:
file_locks: Mutex<HashMap<Ino, FileLock>>,
lock_notify: tokio::sync::Notify,  // для blocking flock(LOCK_EX)
```

```rust
pub async fn flock(&self, ino: Ino, op: LockKind, owner: u64, blocking: bool)
    -> VfsResult<()>
{
    loop {
        let mut locks = self.state.file_locks.lock();
        let current = locks.entry(ino).or_insert(FileLock {
            kind: LockKind::Unlock, owner: 0, count: 0
        });

        match &op {
            LockKind::Shared => {
                if current.kind == LockKind::Exclusive && current.owner != owner {
                    // Wait for exclusive lock to release
                    drop(locks);
                    if !blocking { return Err(VfsError::Other("EAGAIN".into())); }
                    self.state.lock_notify.notified().await;
                    continue;
                }
                current.kind = LockKind::Shared;
                current.count += 1;
                return Ok(());
            }
            LockKind::Exclusive => {
                if current.kind != LockKind::Unlock && current.owner != owner {
                    drop(locks);
                    if !blocking { return Err(VfsError::Other("EAGAIN".into())); }
                    self.state.lock_notify.notified().await;
                    continue;
                }
                *current = FileLock { kind: LockKind::Exclusive, owner, count: 1 };
                return Ok(());
            }
            LockKind::Unlock => {
                if current.owner == owner { locks.remove(&ino); }
                self.state.lock_notify.notify_waiters();
                return Ok(());
            }
        }
    }
}
```

**Файл:** `crates/studiofs-fuse/src/fs.rs`

```rust
fn setlk(&mut self, _req, ino, fh, lock_owner, start, end, typ, pid, sleep, reply) {
    // We implement file-level semantics only (ignore byte ranges)
    let op = match typ {
        libc::F_RDLCK => LockKind::Shared,
        libc::F_WRLCK => LockKind::Exclusive,
        libc::F_UNLCK => LockKind::Unlock,
        _ => { reply.error(libc::EINVAL); return; }
    };
    match self.rt.block_on(self.vfs.flock(ino, op, lock_owner, sleep)) {
        Ok(())                              => reply.ok(),
        Err(e) if e is EAGAIN               => reply.error(libc::EAGAIN),
        Err(e)                              => reply.error(meta_err_to_errno(&e)),
    }
}

fn getlk(&mut self, _req, ino, fh, lock_owner, start, end, typ, pid, reply) {
    // Report no conflicting locks (simplification for single-node)
    reply.locked(start, end, libc::F_UNLCK, 0);
}
```

### 5.2 — Hardlinks (Средний, 1 день)

**Файл:** `crates/studiofs-meta/src/engine.rs`

Добавить метод в trait:
```rust
fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr>;
```

Default impl в trait (компилируется через `redb_engine.rs`):
```rust
fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
    let attr = self.get_attr(ino)?;
    if attr.kind == FileType::Directory {
        return Err(MetaError::IsADirectory(ino)); // EPERM для dirs
    }
    self.link_atomic(ino, new_parent, new_name)
}
```

**Файл:** `crates/studiofs-meta/src/redb_engine.rs`

```rust
fn link_atomic(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
    let txn = self.db.begin_write()?;
    let result;
    {
        let mut dentries = txn.open_table(DENTRIES_TABLE)?;
        let mut inodes   = txn.open_table(INODES_TABLE)?;

        // Check new_name doesn't exist
        let dst_key = encode_dentry_key(new_parent, new_name);
        if dentries.get(dst_key.as_slice())?.is_some() {
            return Err(MetaError::EntryExists {
                parent: new_parent,
                name: String::from_utf8_lossy(new_name).to_string(),
            });
        }

        // Increment nlink + update ctime
        let raw = inodes.get(&ino)?.ok_or(MetaError::InodeNotFound(ino))?;
        let mut attr: InodeAttr = decode(raw.value())?;
        attr.nlink += 1;
        attr.touch_ctime();
        inodes.insert(&ino, encode(&attr)?.as_slice())?;

        // Add new dentry
        let entry_val = encode(&DirEntryValue { ino, kind: FileType::RegularFile })?;
        dentries.insert(dst_key.as_slice(), entry_val.as_slice())?;

        result = attr;
    }
    txn.commit()?;
    Ok(result)
}
```

**Файл:** `crates/studiofs-vfs/src/vfs.rs`

```rust
pub fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> VfsResult<InodeAttr> {
    let attr = self.meta.link(ino, new_parent, new_name)?;
    // Invalidate attr cache (nlink changed)
    self.state.attr_read_cache.lock().remove(&ino);
    self.state.dirty_attrs.lock().remove(&ino);
    Ok(attr)
}
```

**Файл:** `crates/studiofs-fuse/src/fs.rs`

```rust
fn link(&mut self, _req, ino, newparent, newname, reply) {
    let name = newname.to_string_lossy().as_bytes().to_vec();
    match self.vfs.link(ino, newparent, &name) {
        Ok(attr) => reply.entry(&TTL, &to_fuse_attr(&attr, ...), 0),
        Err(e)   => reply.error(meta_err_to_errno(&e)),
    }
}
```

**Итог фазы:** `flock()`, `git`, `sqlite` не зависают. Hardlinks:
`ln file1 file2` работает. `git` использует hardlinks для объектов.

---

## Фаза 6 — xattr + Directory Prefetch

**Сложность: Простой–Средний** | **Зависит от:** Phase 3 | **Дней:** 3.5

> **Зачем седьмым:** xattr нужны для macOS Finder и для `cp -a`.
> Directory prefetch — независимая задача, но требует стабильного
> handle tracking (Phase 3) чтобы видеть паттерны доступа.

### 6.1 — xattr: новая таблица в redb (Простой, 1 день)

**Файл:** `crates/studiofs-meta/src/redb_engine.rs`

```rust
/// Extended attributes: (ino(8) + name_bytes) -> value_bytes
const XATTRS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("xattrs");
```

Ключ: `ino (8 bytes LE) + name bytes`.

Хелперы:
```rust
fn encode_xattr_key(ino: Ino, name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + name.len());
    key.extend_from_slice(&ino.to_le_bytes());
    key.extend_from_slice(name);
    key
}
fn xattr_prefix(ino: Ino) -> [u8; 8] { ino.to_le_bytes() }
```

Добавить в `MetaEngine` trait:
```rust
fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>>;
fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()>;
fn remove_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()>;
fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>>;
```

Флаги: `XATTR_CREATE (1)` = fail if exists, `XATTR_REPLACE (2)` = fail if not exists.

В `unlink()` и `rmdir()` — каскадная очистка: сканировать xattr prefix + удалять все записи в той же транзакции.

### 6.2 — xattr: VFS хелперы (Простой, 0.5 дня)

**Файл:** `crates/studiofs-vfs/src/vfs.rs`

```rust
pub fn get_xattr(&self, ino: Ino, name: &[u8]) -> VfsResult<Option<Vec<u8>>> {
    Ok(self.meta.get_xattr(ino, name)?)
}
pub fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> VfsResult<()> {
    self.meta.set_xattr(ino, name, value, flags)?;
    Ok(())
}
pub fn remove_xattr(&self, ino: Ino, name: &[u8]) -> VfsResult<()> {
    self.meta.remove_xattr(ino, name)?;
    Ok(())
}
pub fn list_xattrs(&self, ino: Ino) -> VfsResult<Vec<Vec<u8>>> {
    Ok(self.meta.list_xattrs(ino)?)
}
```

### 6.3 — xattr: FUSE (Простой, 0.5 дня)

**Файл:** `crates/studiofs-fuse/src/fs.rs`

```rust
fn getxattr(&mut self, _req, ino, name, size, reply) {
    let key = name.to_string_lossy().as_bytes().to_vec();
    match self.vfs.get_xattr(ino, &key) {
        Ok(Some(val)) => {
            if size == 0 { reply.size(val.len() as u32); }
            else if val.len() <= size as usize { reply.data(&val); }
            else { reply.error(libc::ERANGE); }
        }
        Ok(None) => {
            #[cfg(target_os = "macos")] reply.error(libc::ENOATTR);
            #[cfg(not(target_os="macos"))] reply.error(libc::ENODATA);
        }
        Err(e) => reply.error(meta_err_to_errno(&e)),
    }
}

fn setxattr(&mut self, _req, ino, name, value, flags, _pos, reply) {
    let key = name.to_string_lossy().as_bytes().to_vec();
    match self.vfs.set_xattr(ino, &key, value, flags as u32) {
        Ok(()) => reply.ok(),
        Err(e) => reply.error(meta_err_to_errno(&e)),
    }
}

fn removexattr(&mut self, _req, ino, name, reply) {
    let key = name.to_string_lossy().as_bytes().to_vec();
    match self.vfs.remove_xattr(ino, &key) {
        Ok(()) => reply.ok(),
        Err(e) => reply.error(meta_err_to_errno(&e)),
    }
}

fn listxattr(&mut self, _req, ino, size, reply) {
    match self.vfs.list_xattrs(ino) {
        Ok(names) => {
            let data: Vec<u8> = names.iter()
                .flat_map(|n| n.iter().cloned().chain(std::iter::once(0u8)))
                .collect();
            if size == 0 { reply.size(data.len() as u32); }
            else if data.len() <= size as usize { reply.data(&data); }
            else { reply.error(libc::ERANGE); }
        }
        Err(e) => reply.error(meta_err_to_errno(&e)),
    }
}
```

### 6.4 — Directory-Level Prefetch (Средний, 1.5 дня)

> LucidLink: readdir() + несколько open/read в той же директории →
> автоматически prefetch первого блока остальных файлов.

**Файл:** `crates/studiofs-vfs/src/readahead.rs` (новый модуль или расширение)

```rust
pub struct DirAccessTracker {
    /// dir_ino → (Vec<child_ino>, read_count)
    active_dirs: Mutex<HashMap<Ino, DirPrefetchState>>,
}

struct DirPrefetchState {
    /// All child inodes from readdir (excluding dirs)
    children: Vec<Ino>,
    /// How many children we've already seen open+read
    seen: usize,
    /// Which children have been prefetched (first block)
    prefetched: HashSet<Ino>,
}

impl DirAccessTracker {
    /// Called after readdir(dir_ino) completes.
    pub fn on_readdir(&self, dir_ino: Ino, children: Vec<Ino>);

    /// Called after read(child_ino). Returns list of inos to prefetch.
    /// Triggers when >= 2 sequential children from same dir are read.
    pub fn on_read(&self, child_ino: Ino, dir_ino: Ino) -> Vec<Ino>;
}
```

**Интеграция в `vfs.rs`:**

- В `readdir()`: сообщить `dir_access_tracker.on_readdir(ino, child_inos)`.
- В `read_into()` / `read()`: если ino имеет known parent dir (из attr.parent):
  ```rust
  let to_prefetch = self.dir_access_tracker.on_read(ino, parent_ino);
  for child_ino in to_prefetch {
      // prefetch block 0 of each child
      let _ = self.prefetch_tx.send((child_ino, 0));
  }
  ```

**Параметры:** prefetch включается при `seen >= 2` последовательных файлов.
Prefetch только первый блок (block_index=0) — размер ≤ 4 MB.

**Итог фазы:** macOS Finder работает без ошибок xattr. Browse performance
для директорий с медиафайлами — как LucidLink.

---

## Фаза 7 — Полировка

**Сложность: Простой** | **Зависит от:** Phase 0–6 | **Дней:** 2

> Не блокирует функциональность, но важно для UX и инструментов.

### 7.1 — statfs: реальные данные (Простой, 0.5 дня)

**Файл:** `crates/studiofs-vfs/src/vfs.rs`

Счётчики в redb (уже есть `USED_SPACE`, `TOTAL_INODES`).
Обновлять `USED_SPACE`: `+= length` при flush_block, `-= length` при GC.

```rust
pub fn statfs(&self) -> VfsResult<StatFs> {
    let used_inodes = self.meta.get_counter(counters::TOTAL_INODES)?;
    let used_bytes  = self.meta.get_counter(counters::USED_SPACE)?;

    StatFs {
        blocks:  1024 * 1024 * 1024 * 1024 / self.block_size as u64, // 1 PB виртуально
        bfree:   (1024u64 * 1024 * 1024 * 1024 - used_bytes) / self.block_size as u64,
        bavail:  (1024u64 * 1024 * 1024 * 1024 - used_bytes) / self.block_size as u64,
        files:   used_inodes,
        ffree:   u64::MAX - used_inodes,
        bsize:   self.block_size,
        namelen: 255,
        frsize:  self.block_size,
    }
}
```

In FUSE `statfs()` и WinFSP `get_volume_info()` — использовать `vfs.statfs()`.

### 7.2 — readdirplus (Простой, 0.5 дня)

**Файл:** `crates/studiofs-fuse/src/fs.rs`

```rust
fn readdirplus(&mut self, _req, ino, fh, offset, mut reply: ReplyDirectoryPlus) {
    let entries = match self.vfs.readdir(ino) { ... };

    // "." and ".."
    // then entries with full FileAttr each
    for (i, entry) in entries.iter().enumerate() {
        let attr  = match self.vfs.getattr(entry.ino) { ... };
        let fattr = to_fuse_attr(&attr, self.mount_uid, self.mount_gid);
        if reply.add(entry.ino, (i + 3) as i64, entry_name, &TTL, &fattr, 0) { break; }
    }
    reply.ok();
}
```

### 7.3 — Filename validation (Простой, 0.5 дня)

**Файл:** `crates/studiofs-vfs/src/vfs.rs`

Добавить хелпер:
```rust
fn validate_name(name: &[u8]) -> VfsResult<()> {
    if name.is_empty() || name.len() > 255 {
        return Err(VfsError::Other("name too long".into()));
    }
    if name.contains(&b'/') || name == b"." || name == b".." {
        return Err(VfsError::Other("invalid name".into()));
    }
    Ok(())
}
```

Вызывать в `create`, `mkdir`, `symlink`, `link`, `rename` (new_name).

### 7.4 — fallocate: no-op (Простой, 0.5 дня)

**Файл:** `crates/studiofs-fuse/src/fs.rs`

```rust
fn fallocate(&mut self, _req, ino, fh, offset, length, mode, reply) {
    if mode == 0 {
        // FALLOC_FL_KEEP_SIZE not set → extend size if needed
        let attr = match self.vfs.getattr(ino) { Ok(a) => a, Err(e) => { ... } };
        let new_size = offset as u64 + length as u64;
        if new_size > attr.size {
            let args = SetattrArgs { size: Some(new_size), ..Default::default() };
            let _ = self.vfs.setattr(ino, args);
        }
    }
    // For cloud FS: preallocation = no-op (no local disk to reserve).
    reply.ok();
}
```

**Итог фазы:** `df`, Finder filesystem usage, `ls -la` производительность,
`cp -a` с xattr, edge cases с длинными именами — всё корректно.

---

## Сводный план с зависимостями

```
Phase 0: Cleanup legacy
    └── Phase 1: Errno mapping
            ├── Phase 2: Atomic rename
            │       └── Phase 5b: Hardlinks
            └── Phase 3: Handle tracking
                    ├── Phase 4a: setattr timestamps
                    │       └── Phase 4b: Symlinks
                    ├── Phase 5a: File locking (flock)
                    └── Phase 6a: xattr
                            └── Phase 6b: Dir prefetch
                                    └── Phase 7: Polish
```

| Фаза | Задача | Сложность | Зависит от | Дней |
|------|--------|-----------|------------|------|
| 0 | Удаление legacy кода | 🟢 Простой | — | 1 |
| 1 | Errno mapping | 🟢 Простой | 0 | 0.5 |
| 2 | Atomic rename (single redb txn) | 🔴 Сложный | 0 | 1 |
| 3.1 | HandleTable в VFS | 🟡 Средний | 1 | 1.5 |
| 3.2 | Open-unlink + ORPHAN_INODES | 🔴 Сложный | 3.1 | 1 |
| 3.3 | fsync (flush to local slab) | 🟢 Простой | 3.1 | 0.5 |
| 3.4 | O_APPEND / O_TRUNC | 🟢 Простой | 3.1 | 0.5 |
| 3.5 | FUSE/WinFSP: open→fh, release, flush, fsync | 🟢 Простой | 3.1–3.4 | 1 |
| 4.1 | SetattrArgs + VFS setattr | 🟢 Простой | 0 | 0.5 |
| 4.2 | FUSE/WinFSP setattr timestamps | 🟢 Простой | 4.1 | 0.5 |
| 4.3 | Symlinks: SYMLINKS_TABLE + MetaEngine | 🟡 Средний | 0 | 1 |
| 4.4 | Symlinks: VFS + FUSE + WinFSP reparse | 🟡 Средний | 4.3 | 1 |
| 5.1 | flock (file-level, in-memory) | 🟡 Средний | 3.1 | 1.5 |
| 5.2 | Hardlinks: link_atomic + VFS + FUSE | 🟡 Средний | 2 | 1 |
| 6.1 | xattr: XATTRS_TABLE + MetaEngine | 🟢 Простой | 0 | 1 |
| 6.2 | xattr: VFS + FUSE + WinFSP | 🟢 Простой | 6.1 | 0.5 |
| 6.3 | Directory-level prefetch tracker | 🟡 Средний | 3.1 | 1.5 |
| 7.1 | statfs реальные данные | 🟢 Простой | 0 | 0.5 |
| 7.2 | readdirplus | 🟢 Простой | 0 | 0.5 |
| 7.3 | Filename validation | 🟢 Простой | 0 | 0.5 |
| 7.4 | fallocate no-op | 🟢 Простой | 0 | 0.5 |
| **Итого** | | | | **~15.5 дней** |

---

## Порядок коммитов (рекомендуемый)

```
1. chore(meta): remove deprecated Chunks/PendingSlices/DeletedSlices tables
2. fix(fuse/winfsp): proper errno mapping for all VFS errors
3. feat(meta): atomic rename in single redb transaction
4. refactor(vfs): SetattrArgs — full setattr support (mode, uid, gid, timestamps)
5. fix(fuse/winfsp): wire all setattr parameters
6. feat(meta): SYMLINKS_TABLE + set/get symlink target
7. feat(vfs): symlink() and readlink()
8. feat(fuse): symlink + readlink callbacks
9. feat(winfsp): symlink via reparse points
10. feat(vfs): HandleTable + OpenFile + open_file() + release_fh()
11. feat(vfs): open-unlink semantics + ORPHAN_INODES crash recovery
12. feat(vfs): fsync() flushes to local slab (no S3 wait)
13. feat(vfs): O_APPEND and O_TRUNC handling
14. feat(fuse): real fh in open/release/flush/fsync
15. feat(winfsp): real fh in open/close/flush
16. feat(meta): link_atomic() + MetaEngine::link()
17. feat(vfs/fuse): link() hard link support
18. feat(vfs): file-level flock() locking
19. feat(fuse): setlk/getlk callbacks
20. feat(meta): XATTRS_TABLE + xattr CRUD
21. feat(vfs/fuse): xattr get/set/remove/list
22. feat(vfs): DirAccessTracker + directory-level prefetch
23. feat(fuse/vfs): statfs with real counters
24. feat(fuse): readdirplus
25. fix(vfs): filename validation (255 bytes, no slash)
26. feat(fuse): fallocate no-op
```
