//! Timer-gated physical filesystem reserve; foreground I/O never samples space.

use super::*;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(30);
const MAX_RECLAIM_BYTES: u64 = 64 * 1024 * 1024;

/// Cached physical filesystem pressure diagnostics. Reading this performs no I/O.
#[derive(Debug, Clone)]
pub struct PhysicalSpaceStatus {
    /// Age of the last successful sample, or `None` before one succeeds.
    pub sample_age: Option<Duration>,
    /// Available filesystem bytes (Windows additionally honors the caller's quota).
    pub available_bytes: Option<u64>,
    /// Total filesystem capacity, independent of cache budget and caller quota.
    pub total_bytes: Option<u64>,
    /// Best-effort 10% reserve, rounded up to a byte.
    pub reserve_bytes: Option<u64>,
    /// Logical payload bytes removed in the last pass, not physical space freed.
    pub reclaimed_payload_bytes: u64,
    /// Last maintenance outcome, including why reclamation could not proceed.
    pub outcome: &'static str,
}

#[derive(Debug)]
pub(super) struct DiskPressureState {
    last_attempt: Option<Instant>,
    last_sample: Option<(Instant, platform::FilesystemSpace)>,
    pressured: bool,
    cursor: Option<u64>,
    reclaimed_payload_bytes: u64,
    outcome: &'static str,
}

impl Default for DiskPressureState {
    fn default() -> Self {
        Self {
            last_attempt: None,
            last_sample: None,
            pressured: false,
            cursor: None,
            reclaimed_payload_bytes: 0,
            outcome: "not_sampled",
        }
    }
}

fn percent_ceil(total: u64, percent: u64) -> u64 {
    // Divide first so even a u64::MAX capacity cannot overflow.
    (total / 100) * percent + ((total % 100) * percent).div_ceil(100)
}

impl LocalPayloadBackendRuntime {
    /// Returns the cached physical-space diagnostics without querying the filesystem.
    pub fn physical_space_status(&self) -> PhysicalSpaceStatus {
        let state = self.disk_pressure.lock();
        PhysicalSpaceStatus {
            sample_age: state.last_sample.map(|(at, _)| at.elapsed()),
            available_bytes: state.last_sample.map(|(_, space)| space.available_bytes),
            total_bytes: state.last_sample.map(|(_, space)| space.total_bytes),
            reserve_bytes: state
                .last_sample
                .map(|(_, space)| percent_ceil(space.total_bytes, 10)),
            reclaimed_payload_bytes: state.reclaimed_payload_bytes,
            outcome: state.outcome,
        }
    }

    pub(super) fn run_disk_pressure_maintenance(
        &self,
        now: Instant,
        mut sampler: impl FnMut(&Path) -> io::Result<platform::FilesystemSpace>,
    ) -> Result<(), LocalPayloadBackendError> {
        // Use the budget eviction owner; never queue background work behind foreground admission.
        let Some(_owner) = self.eviction_pass_lock.try_lock() else {
            return Ok(());
        };
        let mut state = self.disk_pressure.lock();
        if state
            .last_attempt
            .is_some_and(|at| now.saturating_duration_since(at) < SAMPLE_INTERVAL)
        {
            return Ok(());
        }
        state.last_attempt = Some(now);
        state.reclaimed_payload_bytes = 0;
        let mut sample = match checked_sample(&mut sampler, &self.layout.runtime_root) {
            Ok(sample) => sample,
            Err(error) => {
                state.outcome = "sampling_failed";
                warn!(error = %error, sample_age_secs = ?state.last_sample.map(|(at, _)| now.saturating_duration_since(at).as_secs()), "physical cache space sampling failed; retaining cached diagnostics until next interval");
                return Ok(());
            }
        };
        state.last_sample = Some((now, sample));
        let reserve = percent_ceil(sample.total_bytes, 10);
        let recovery = percent_ceil(sample.total_bytes, 11);
        state.pressured = if state.pressured {
            sample.available_bytes < recovery
        } else {
            sample.available_bytes < reserve
        };
        if !state.pressured {
            state.cursor = None;
            state.outcome = "healthy";
            debug!(
                available_bytes = sample.available_bytes,
                total_bytes = sample.total_bytes,
                reserve_bytes = reserve,
                sample_age_secs = 0,
                "physical cache space reserve healthy"
            );
            return Ok(());
        }

        let reclaim = (|| -> Result<(), LocalPayloadBackendError> {
            let page = self
                .index
                .eviction_candidates_scan_page(state.cursor, EVICTION_CANDIDATE_PAGE_SIZE)?;
            state.cursor = if page.scanned_rows < EVICTION_CANDIDATE_PAGE_SIZE as u64 {
                None
            } else {
                page.next_cursor
            };
            let mut candidates = Vec::with_capacity(EVICTION_DELETE_BATCH_SIZE);
            let mut selected_bytes = 0_u64;
            let mut last_selected = None;
            let needed = recovery
                .saturating_sub(sample.available_bytes)
                .min(MAX_RECLAIM_BYTES);
            for candidate in page.entries {
                if self.is_runtime_pinned(candidate.object_id)
                    || !self
                        .index
                        .get_entry(candidate.object_id)?
                        .is_some_and(|entry| entry.origin == LocalPayloadOrigin::Downloaded)
                {
                    continue;
                }
                if !candidates.is_empty()
                    && candidate.payload_len > MAX_RECLAIM_BYTES.saturating_sub(selected_bytes)
                {
                    state.cursor = last_selected;
                    break;
                }
                candidates.push(candidate.object_id);
                last_selected = Some(candidate.access_order);
                selected_bytes = selected_bytes.saturating_add(candidate.payload_len);
                if candidates.len() >= EVICTION_DELETE_BATCH_SIZE || selected_bytes >= needed {
                    // Resume after the last selected row; do not skip unprocessed candidates.
                    state.cursor = Some(candidate.access_order);
                    break;
                }
            }
            state.reclaimed_payload_bytes = self.evict_batch_internal(&candidates, true)?;
            Ok(())
        })();
        if let Err(error) = reclaim {
            state.outcome = "eviction_failed";
            warn!(error = %error, "physical cache space reclamation failed; protected data retained");
            return Err(error);
        }
        if state.reclaimed_payload_bytes > 0 {
            // Unlink/index accounting is not evidence of released physical space.
            match checked_sample(&mut sampler, &self.layout.runtime_root) {
                Ok(after) => {
                    sample = after;
                    state.last_sample = Some((now, sample));
                    state.pressured = sample.available_bytes < percent_ceil(sample.total_bytes, 11);
                }
                Err(error) => {
                    state.outcome = "sampling_failed";
                    warn!(error = %error, reclaimed_payload_bytes = state.reclaimed_payload_bytes, "physical cache space resampling failed after reclamation");
                    return Ok(());
                }
            }
        }
        state.outcome = if !state.pressured {
            state.cursor = None;
            "healthy"
        } else if state.cursor.is_some() {
            "scan_limited"
        } else {
            "protected_or_exhausted"
        };
        let reserve = percent_ceil(sample.total_bytes, 10);
        if state.pressured {
            warn!(
                available_bytes = sample.available_bytes,
                total_bytes = sample.total_bytes,
                reserve_bytes = reserve,
                recovery_bytes = percent_ceil(sample.total_bytes, 11),
                reclaimed_payload_bytes = state.reclaimed_payload_bytes,
                reason = state.outcome,
                sample_age_secs = 0,
                "physical cache space pressure remains; next bounded pass waits for sampling interval"
            );
        } else {
            debug!(
                available_bytes = sample.available_bytes,
                total_bytes = sample.total_bytes,
                reserve_bytes = reserve,
                reclaimed_payload_bytes = state.reclaimed_payload_bytes,
                reason = state.outcome,
                sample_age_secs = 0,
                "physical cache space recovered after reclamation"
            );
        }
        Ok(())
    }
}

fn checked_sample(
    sampler: &mut impl FnMut(&Path) -> io::Result<platform::FilesystemSpace>,
    path: &Path,
) -> io::Result<platform::FilesystemSpace> {
    let sample = sampler(path)?;
    if sample.total_bytes == 0 || sample.available_bytes > sample.total_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid filesystem capacity sample",
        ));
    }
    Ok(sample)
}

#[cfg(test)]
#[path = "disk_pressure_tests.rs"]
mod tests;
