use super::{
    AccessLevel, DirtyAttrPersistPolicy, ExplicitFlushOutcome, FileSessionOperation, FsyncOutcome,
    HandleReleaseContext, Ino, LastHandleReleaseAction, LocalSyncBarrierOutcome, LocalSyncPolicy,
    OpenFile, OpenFlags, ReleaseFhOutcome, StickyErrorPolicy, StudioVfs, VfsResult,
};
use std::sync::Arc;
use tracing::{debug, warn};

use crate::VfsMeta;
use crate::runtime::run_meta;

use std::time::Instant;

impl StudioVfs {
    #[cfg(test)]
    pub(super) fn arm_fail_next_inline_dirty_attr_persist_for_test(&self) {
        self.fail_next_inline_dirty_attr_persist
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    fn fail_inline_dirty_attr_persist_for_test(&self) -> VfsResult<()> {
        if self
            .fail_next_inline_dirty_attr_persist
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            Err(super::VfsError::Other(
                "simulated inline dirty attr persist failure".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    #[cfg(not(test))]
    fn fail_inline_dirty_attr_persist_for_test(&self) -> VfsResult<()> {
        Ok(())
    }

    /// Release a file handle.
    ///
    /// If the inode was unlinked while this handle was open (orphan) and this
    /// was the last open handle, perform the deferred deletion now.
    ///
    /// This is also the last-handle local-state drain point. If a writable
    /// handle reaches release with dirty blocks or attrs still pending,
    /// `release_fh` must push that state through the current local
    /// payload/metadata pipeline before the handle disappears so later opens,
    /// open-unlink cleanup, and sticky-error reporting all see a coherent
    /// inode. In durable mode this remains a strict local-durability barrier;
    /// in fast mode it only guarantees that local unpublished state is
    /// materialized, not that payload bytes have crossed a crash-safe sync
    /// boundary. Clean last-handle closes still short-circuit without
    /// inventing extra work on the foreground path.
    pub async fn release_fh(&self, fh: u64) -> VfsResult<()> {
        self.release_fh_with_outcome(fh).await.map(|_| ())
    }

    fn close_handle_for_release(&self, fh: u64) -> Option<HandleReleaseContext> {
        self.state.handle_writers.mark_closed(fh);
        let Some((open_file, remaining)) = self.handle_table.close(fh) else {
            debug!("release_fh: unknown fh={}", fh);
            return None;
        };
        let ino = open_file.ino;
        let stream_id = open_file.stream_id();
        self.state
            .read
            .decrement_active_access_class(open_file.current_access_class());
        debug!("release_fh fh={} ino={} remaining={}", fh, ino, remaining);
        self.state.file_session.note_close_handle(ino, remaining);
        self.clear_access_tracking_for_stream(ino, stream_id);
        self.debug_pending_inode_state(ino, "release_handle_closed");
        if remaining > 0 {
            return None;
        }
        self.debug_pending_inode_state(ino, "release_last_handle");
        Some(HandleReleaseContext { open_file, ino })
    }

    async fn cleanup_orphan_on_last_handle_close(&self, ino: Ino) -> VfsResult<ReleaseFhOutcome> {
        if self.release_ignored_entry(ino) {
            return Ok(ReleaseFhOutcome {
                closed_last_handle: true,
                flushed_local_dirty_state: false,
            });
        }
        // Last handle closed on an unlinked file — final cleanup
        debug!("orphan cleanup: ino={} (last fd closed)", ino);

        let cleanup = self.collect_orphan_deleted_inode_cleanup(ino).await?;
        let removed_blocks = cleanup.removed_ids.len();
        let freed_bytes = cleanup.freed_bytes;
        let deferred_local_blocks = self
            .finalize_deleted_inode_runtime_cleanup(ino, cleanup.removed_ids)
            .await;
        debug!(
            "orphan cleanup complete: ino={} removed_blocks={} deferred_local_blocks={} freed_bytes={}",
            ino, removed_blocks, deferred_local_blocks, freed_bytes
        );
        Ok(ReleaseFhOutcome {
            closed_last_handle: true,
            flushed_local_dirty_state: false,
        })
    }

    fn take_last_handle_release_action(
        &self,
        context: &HandleReleaseContext,
    ) -> LastHandleReleaseAction {
        let ino = context.ino;
        if self.release_ignored_entry(ino) {
            let _ = self.handle_table.take_orphan(ino);
            return LastHandleReleaseAction::IgnoredEntry;
        }

        if self.handle_table.take_orphan(ino) {
            return LastHandleReleaseAction::OrphanCleanup;
        }

        let sticky_error_policy = if context.open_file.flags.contains(OpenFlags::WRONLY)
            || context.open_file.flags.contains(OpenFlags::RDWR)
        {
            StickyErrorPolicy::Report
        } else {
            StickyErrorPolicy::Ignore
        };
        LastHandleReleaseAction::LocalSync {
            sticky_error_policy,
        }
    }

    async fn execute_last_handle_release_action(
        &self,
        ino: Ino,
        action: LastHandleReleaseAction,
    ) -> VfsResult<ReleaseFhOutcome> {
        match action {
            LastHandleReleaseAction::IgnoredEntry => Ok(ReleaseFhOutcome {
                closed_last_handle: true,
                flushed_local_dirty_state: false,
            }),
            LastHandleReleaseAction::OrphanCleanup => {
                self.cleanup_orphan_on_last_handle_close(ino).await
            }
            LastHandleReleaseAction::LocalSync {
                sticky_error_policy,
            } => {
                let outcome = self
                    .local_sync_barrier_with_outcome(
                        ino,
                        self.local_sync_policy_for_barrier(ino),
                        sticky_error_policy,
                        DirtyAttrPersistPolicy::Ignore,
                    )
                    .await?;
                Ok(ReleaseFhOutcome {
                    closed_last_handle: true,
                    flushed_local_dirty_state: outcome.had_local_dirty_state,
                })
            }
        }
    }

    async fn release_last_handle_with_outcome(
        &self,
        context: HandleReleaseContext,
    ) -> VfsResult<ReleaseFhOutcome> {
        let ino = context.ino;
        let action = self.take_last_handle_release_action(&context);
        self.execute_last_handle_release_action(ino, action).await
    }

    /// Release a handle and report whether this close owned the actual
    /// last-handle path, plus whether it had to drain local dirty state there.
    pub async fn release_fh_with_outcome(&self, fh: u64) -> VfsResult<ReleaseFhOutcome> {
        let Some(context) = self.close_handle_for_release(fh) else {
            // Non-final handle closes stop here and never enter the
            // last-handle drain path, so any error returned by this method
            // necessarily comes from the final release flow below.
            return Ok(ReleaseFhOutcome::default());
        };
        let ino = context.ino;

        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Release)
            .await;
        let result = self.release_last_handle_with_outcome(context).await;
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    async fn local_sync_barrier_with_outcome(
        &self,
        ino: Ino,
        local_sync_policy: LocalSyncPolicy,
        sticky_error_policy: StickyErrorPolicy,
        dirty_attr_persist_policy: DirtyAttrPersistPolicy,
    ) -> VfsResult<LocalSyncBarrierOutcome> {
        // Wait for any in-flight deferred materialization task to finish
        // before we run our own strict materialization pass.  Without this
        // the flush/close path and the background task would race on
        // HandleWriterRegistry snapshots, producing fragmented blocks.
        //
        // The parking_lot lock is scoped so the MutexGuard (which is !Send)
        // is dropped before the .await point.  We then acquire the
        // tokio::sync::Mutex which blocks until the background task releases it.
        let task_lock = {
            self.state
                .deferred_materialize_inflight
                .lock()
                .get(&ino)
                .cloned()
        };
        if let Some(task_lock) = task_lock {
            let _guard = task_lock.lock().await;
        }

        let materialized_handle_writers = self
            .materialize_handle_writers_for_inode(ino, local_sync_policy)
            .await?;
        let flushed_file_data =
            materialized_handle_writers || self.has_pending_local_dirty_blocks_for_inode(ino);
        let residual_pending_local_blocks = self.has_pending_local_dirty_blocks_for_inode(ino);
        let dirty_attr_present = self.state.namespace.dirty_attrs.lock().contains_key(&ino);
        let persist_dirty_attr_inline = !residual_pending_local_blocks
            && dirty_attr_present
            && matches!(
                dirty_attr_persist_policy,
                DirtyAttrPersistPolicy::PersistIfPresent
            );
        let residual_local_dirty_state =
            residual_pending_local_blocks || (dirty_attr_present && !persist_dirty_attr_inline);
        let had_local_dirty_state =
            materialized_handle_writers || residual_local_dirty_state || persist_dirty_attr_inline;
        if residual_local_dirty_state {
            self.flush_local_dirty_state(ino, local_sync_policy).await?;
        }
        if matches!(local_sync_policy, LocalSyncPolicy::Strict) {
            self.maybe_block_store_sync_data_async(0, LocalSyncPolicy::Strict)
                .await?;
        }

        let dirty_attr = match dirty_attr_persist_policy {
            DirtyAttrPersistPolicy::Ignore => None,
            DirtyAttrPersistPolicy::PersistIfPresent if !persist_dirty_attr_inline => None,
            DirtyAttrPersistPolicy::PersistIfPresent => {
                self.state.namespace.dirty_attrs.lock().get(&ino).cloned()
            }
        };
        let persisted_dirty_attrs = if let Some(attr) = dirty_attr {
            self.fail_inline_dirty_attr_persist_for_test()?;
            let meta = self.meta.clone();
            let attr_to_persist = attr.clone();
            run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.set_attr(&attr_to_persist)
            })
            .await?;
            self.clear_matching_dirty_attr_if_committed(ino, &attr);
            self.state.namespace.attr_read_cache.lock().remove(ino);
            true
        } else {
            false
        };

        if matches!(sticky_error_policy, StickyErrorPolicy::Report) {
            self.ensure_no_upload_sticky_error(ino)?;
        }

        Ok(LocalSyncBarrierOutcome {
            had_local_dirty_state,
            flushed_file_data,
            persisted_dirty_attrs,
        })
    }

    fn local_sync_policy_for_barrier(&self, ino: Ino) -> LocalSyncPolicy {
        let has_pending_local_state =
            self.inode_has_local_dirty_state(ino) || self.inode_has_dirty_handle_writers(ino);
        if has_pending_local_state {
            LocalSyncPolicy::Strict
        } else {
            LocalSyncPolicy::Opportunistic
        }
    }

    pub(super) async fn flush_inode_barrier_with_outcome(
        &self,
        ino: Ino,
        sticky_error_policy: StickyErrorPolicy,
    ) -> VfsResult<ExplicitFlushOutcome> {
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Flush)
            .await;
        let result: VfsResult<ExplicitFlushOutcome> = async {
            let outcome = self
                .local_sync_barrier_with_outcome(
                    ino,
                    self.local_sync_policy_for_barrier(ino),
                    sticky_error_policy,
                    DirtyAttrPersistPolicy::Ignore,
                )
                .await?;
            Ok(ExplicitFlushOutcome {
                flushed_local_dirty_state: outcome.had_local_dirty_state,
            })
        }
        .await;
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// Flush data for a file handle (per-fd close in POSIX).
    ///
    /// Called by FUSE `flush()`. Flushes pending writes for the inode.
    ///
    /// This is the explicit per-handle barrier used by FUSE and by WinFSP
    /// callback paths that decide to make dirty state explicit while the handle
    /// is still valid. Clean handles should return quickly; dirty handles must
    /// persist current local state before success or error is reported. Under
    /// the single-mode Fast Local Authority contract this is a strict local-
    /// durability barrier whenever dirty unpublished state remains.
    pub async fn flush_fh_with_outcome(&self, fh: u64) -> VfsResult<ExplicitFlushOutcome> {
        let Some(open_file) = self.handle_table.get(fh) else {
            return Ok(ExplicitFlushOutcome::default());
        };
        let session = self
            .state
            .file_session
            .enter_op(open_file.ino, FileSessionOperation::Flush)
            .await;
        let result: VfsResult<ExplicitFlushOutcome> = async {
            let outcome = self
                .local_sync_barrier_with_outcome(
                    open_file.ino,
                    self.local_sync_policy_for_barrier(open_file.ino),
                    StickyErrorPolicy::Report,
                    DirtyAttrPersistPolicy::PersistIfPresent,
                )
                .await?;
            Ok(ExplicitFlushOutcome {
                flushed_local_dirty_state: outcome.had_local_dirty_state,
            })
        }
        .await;
        drop(session);
        self.state.file_session.gc_if_idle(open_file.ino);
        result
    }

    pub async fn flush_fh(&self, fh: u64) -> VfsResult<()> {
        self.flush_fh_with_outcome(fh).await.map(|_| ())
    }

    pub(super) async fn fsync_with_outcome(
        &self,
        ino: Ino,
        datasync: bool,
    ) -> VfsResult<FsyncOutcome> {
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Fsync)
            .await;
        let result: VfsResult<FsyncOutcome> = async {
            self.enforce_access(ino, AccessLevel::ReadOnly)?;
            if self.ignored_entry_attr(ino).is_some() {
                return Ok(FsyncOutcome::default());
            }

            let outcome = self
                .local_sync_barrier_with_outcome(
                    ino,
                    LocalSyncPolicy::Strict,
                    StickyErrorPolicy::Report,
                    if datasync {
                        DirtyAttrPersistPolicy::Ignore
                    } else {
                        DirtyAttrPersistPolicy::PersistIfPresent
                    },
                )
                .await?;
            Ok(FsyncOutcome {
                flushed_file_data: outcome.flushed_file_data,
                persisted_dirty_attrs: outcome.persisted_dirty_attrs,
            })
        }
        .await;
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// fsync — flush data (and optionally metadata) to durable storage.
    ///
    /// 1. Flushes pending local dirty state to the dirty-mem / slab pipeline.
    /// 2. If `!datasync`, also commits dirty attrs to redb.
    ///
    /// S3 upload remains async — same as LucidLink behaviour.
    pub async fn fsync(&self, ino: Ino, datasync: bool) -> VfsResult<()> {
        self.fsync_with_outcome(ino, datasync).await.map(|_| ())
    }

    /// Get information about an open file handle.
    pub fn get_open_file(&self, fh: u64) -> Option<OpenFile> {
        self.handle_table.get(fh)
    }

    /// Get the number of open handles for an inode.
    pub fn open_count(&self, ino: Ino) -> u32 {
        self.handle_table.open_count(ino)
    }

    /// Read file data into `buffer` using the buffered read path.
    pub async fn read_into_fh(
        &self,
        fh: u64,
        ino: Ino,
        offset: u64,
        buffer: &mut [u8],
    ) -> VfsResult<u32> {
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Read)
            .await;
        let result = self.read_into_impl(Some(fh), ino, offset, buffer).await;
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// Write file data using the buffered write path.
    pub async fn write_fh(&self, fh: u64, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        self.write_fh_with_local_sync_policy(fh, ino, offset, data, LocalSyncPolicy::Opportunistic)
            .await
    }

    pub async fn write_fh_with_local_sync_policy(
        &self,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: &[u8],
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<u32> {
        // `session` is wrapped in Option so we can release it early for opportunistic writes
        // before local-disk materialization. Strict writes hold the gate through all I/O to
        // maintain ordering guarantees with concurrent flushes on the same inode.
        let mut session = Some(
            self.state
                .file_session
                .enter_op(ino, FileSessionOperation::Write)
                .await,
        );
        let open_file = self.enforce_mutating_handle_access(fh, ino).ok();
        let _ = self.state.read.observe_write_access(
            open_file.as_ref(),
            ino,
            offset,
            data.len(),
            self.access_tracker,
        );
        let result = async {
            self.enforce_access(ino, AccessLevel::ReadWrite)?;
            self.enforce_mutating_handle_access(fh, ino)?;
            if data.is_empty() {
                return Ok(0);
            }
            if self.ignored_entry_attr(ino).is_some() {
                let size = offset.saturating_add(data.len() as u64);
                let _ = self.update_ignored_entry(ino, |attr| {
                    if size > attr.size {
                        attr.size = size;
                        attr.committed_size = size;
                    }
                    attr.touch_mtime();
                });
                return Ok(data.len() as u32);
            }
            self.note_vfs_write_call(data.len());

            let record_handle_writer_state_started = Instant::now();
            let written = self.record_handle_writer_state(fh, ino, offset, data).await;
            self.record_vfs_write_record_handle_writer_state(
                record_handle_writer_state_started.elapsed(),
            );
            let written = written?;

            // Release the per-inode write gate before local-disk materialization for
            // opportunistic writes. This lets concurrent writes to the same inode proceed
            // through buffering while this write's materialization is in-flight.
            //
            // Duplicate materialization is safe: the local block store uses last-writer-wins
            // for overlapping block ranges, and orphaned objects are reclaimed by the
            // background janitor. Strict writes keep the gate to serialize flush/fsync
            // callers with any concurrent materialization on the same inode.
            if matches!(local_sync_policy, LocalSyncPolicy::Opportunistic) {
                session = None;
            }

            if matches!(local_sync_policy, LocalSyncPolicy::Strict) {
                let materialize_strict_started = Instant::now();
                let strict_result = self
                    .materialize_handle_writers_for_inode(ino, local_sync_policy)
                    .await;
                self.record_vfs_write_materialize_strict(materialize_strict_started.elapsed());
                strict_result?;
            } else {
                let materialize_completed_started = Instant::now();
                let completed_result = self
                    .materialize_completed_handle_writer_blocks_for_inode(ino, local_sync_policy)
                    .await;
                self.record_vfs_write_materialize_completed(
                    materialize_completed_started.elapsed(),
                );
                completed_result?;
            }
            let materialize_for_limits_started = Instant::now();
            let limits_result = self
                .materialize_handle_writers_for_limits(ino, local_sync_policy)
                .await;
            self.record_vfs_write_materialize_for_limits(materialize_for_limits_started.elapsed());
            limits_result?;
            Ok(written)
        }
        .await;
        // For opportunistic writes `session` is already None (gate released above).
        // For strict writes `session` holds the gate until here.
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// Write with deferred materialization for high-throughput streaming writes.
    ///
    /// For `Opportunistic` writes the callback returns as soon as data is buffered
    /// in the `HandleWriterRegistry`.  NVMe materialization and budget enforcement
    /// are spawned as a fire-and-forget background task so the calling thread
    /// (e.g. WinFSP dispatcher) is never blocked on local I/O.
    ///
    /// For `Strict` writes, behaviour is identical to
    /// `write_fh_with_local_sync_policy` — materialization is awaited inline.
    ///
    /// # Safety of fire-and-forget materialization
    ///
    /// * Data is safely held in `HandleWriterRegistry` memory buffers.
    /// * Any materialization error will surface at the next `Strict` barrier
    ///   (flush / fsync / close) because those paths call
    ///   `materialize_handle_writers_for_inode` synchronously.
    /// * Duplicate materialization is safe: the block store uses last-writer-wins
    ///   for overlapping ranges, and orphaned objects are reclaimed by the janitor.
    /// * Budget enforcement runs in the background task: if dirty bytes exceed
    ///   `3 × max_dirty_bytes` the *next* write will detect it synchronously via
    ///   the hard-backpressure guard at the top and block until drain catches up.
    pub async fn write_fh_with_deferred_materialize(
        self: &Arc<Self>,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: &[u8],
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<u32> {
        // ── Hard backpressure safety valve ───────────────────────────────
        // If background materialization falls behind and dirty memory is
        // dangerously high, fall back to the fully-synchronous path so we
        // exert real TCP-style backpressure on the caller and avoid OOM.
        let hard_backpressure_threshold = self.max_dirty_bytes().saturating_mul(3);
        if self.state.handle_writers.total_dirty_bytes() > hard_backpressure_threshold {
            return self
                .write_fh_with_local_sync_policy(fh, ino, offset, data, local_sync_policy)
                .await;
        }

        // ── Acquire per-inode write gate ─────────────────────────────────
        let mut session = Some(
            self.state
                .file_session
                .enter_op(ino, FileSessionOperation::Write)
                .await,
        );
        let open_file = self.enforce_mutating_handle_access(fh, ino).ok();
        let _ = self.state.read.observe_write_access(
            open_file.as_ref(),
            ino,
            offset,
            data.len(),
            self.access_tracker,
        );

        // ── Buffer phase (fast: ~0.1 ms) ────────────────────────────────
        let result = async {
            self.enforce_access(ino, AccessLevel::ReadWrite)?;
            self.enforce_mutating_handle_access(fh, ino)?;
            if data.is_empty() {
                return Ok(0);
            }
            if self.ignored_entry_attr(ino).is_some() {
                let size = offset.saturating_add(data.len() as u64);
                let _ = self.update_ignored_entry(ino, |attr| {
                    if size > attr.size {
                        attr.size = size;
                        attr.committed_size = size;
                    }
                    attr.touch_mtime();
                });
                return Ok(data.len() as u32);
            }
            self.note_vfs_write_call(data.len());

            let record_handle_writer_state_started = Instant::now();
            let written = self.record_handle_writer_state(fh, ino, offset, data).await;
            self.record_vfs_write_record_handle_writer_state(
                record_handle_writer_state_started.elapsed(),
            );
            written
        }
        .await;

        // Release the gate immediately — data is safe in HandleWriterRegistry.
        if matches!(local_sync_policy, LocalSyncPolicy::Opportunistic) {
            session = None;
        }

        // ── Materialization phase ────────────────────────────────────────
        match (&result, local_sync_policy) {
            (Ok(_), LocalSyncPolicy::Opportunistic) => {
                // Coalesced fire-and-forget: spawn at most ONE materialization
                // task per inode.  If a task is already in-flight for this inode
                // it will naturally drain all buffered data accumulated since it
                // started — no need for another task.
                let already_inflight = {
                    let inflight = self.state.deferred_materialize_inflight.lock();
                    inflight.contains_key(&ino)
                };
                if !already_inflight {
                    // Create a tokio::sync::Mutex and pre-acquire it so the
                    // spawned task holds the lock for its entire lifetime.
                    // Flush/close will `.lock().await` on the same Mutex to
                    // wait for completion — no TOCTOU race possible.
                    let task_lock = Arc::new(tokio::sync::Mutex::new(()));
                    let guard = task_lock.clone().lock_owned().await;
                    self.state
                        .deferred_materialize_inflight
                        .lock()
                        .insert(ino, Arc::clone(&task_lock));
                    let vfs = Arc::clone(self);
                    tokio::spawn(async move {
                        let materialize_completed_started = Instant::now();
                        if let Err(e) = vfs
                            .materialize_completed_handle_writer_blocks_for_inode(
                                ino,
                                LocalSyncPolicy::Opportunistic,
                            )
                            .await
                        {
                            warn!(
                                ino,
                                error = %e,
                                "deferred materialization (completed blocks) failed; \
                                 will retry at next strict barrier"
                            );
                        }
                        vfs.record_vfs_write_materialize_completed(
                            materialize_completed_started.elapsed(),
                        );

                        let materialize_for_limits_started = Instant::now();
                        if let Err(e) = vfs
                            .materialize_handle_writers_for_limits(
                                ino,
                                LocalSyncPolicy::Opportunistic,
                            )
                            .await
                        {
                            warn!(
                                ino,
                                error = %e,
                                "deferred materialization (budget enforcement) failed; \
                                 will retry at next strict barrier"
                            );
                        }
                        vfs.record_vfs_write_materialize_for_limits(
                            materialize_for_limits_started.elapsed(),
                        );

                        // Release the lock so any waiting flush/close can proceed,
                        // then remove the map entry.
                        drop(guard);
                        vfs.state.deferred_materialize_inflight.lock().remove(&ino);
                    });
                }
            }
            (Ok(_), LocalSyncPolicy::Strict) => {
                // Strict: await materialization inline (same as original).
                let materialize_strict_started = Instant::now();
                if let Err(e) = self
                    .materialize_handle_writers_for_inode(ino, local_sync_policy)
                    .await
                {
                    drop(session);
                    self.state.file_session.gc_if_idle(ino);
                    return Err(e);
                }
                self.record_vfs_write_materialize_strict(materialize_strict_started.elapsed());

                let materialize_for_limits_started = Instant::now();
                if let Err(e) = self
                    .materialize_handle_writers_for_limits(ino, local_sync_policy)
                    .await
                {
                    drop(session);
                    self.state.file_session.gc_if_idle(ino);
                    return Err(e);
                }
                self.record_vfs_write_materialize_for_limits(
                    materialize_for_limits_started.elapsed(),
                );
            }
            _ => {
                // Write failed or unknown policy — nothing to materialize.
            }
        }

        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }
}
