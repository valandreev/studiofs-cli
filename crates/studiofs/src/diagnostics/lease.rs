use super::*;

#[derive(Debug, Default)]
pub(super) struct ControllerLeaseManager {
    state: Mutex<Option<ActiveControllerLease>>,
}

#[derive(Debug, Clone)]
pub(super) struct ActiveControllerLease {
    lease: ControllerLease,
    expires_at: Instant,
}

impl ControllerLeaseManager {
    pub(super) fn snapshot(&self) -> Option<ControllerLease> {
        let now = Instant::now();
        let mut state = self.state.lock();
        Self::expire_if_needed(&mut state, now);
        state.as_ref().map(|lease| lease.lease.clone())
    }

    pub(super) fn acquire(
        &self,
        client_name: String,
        ttl_ms: Option<u64>,
    ) -> Result<ControllerLease> {
        let now = Instant::now();
        let mut state = self.state.lock();
        Self::expire_if_needed(&mut state, now);
        if let Some(active) = state.as_ref() {
            return Err(anyhow!(
                "controller lease already held by '{}' until {}",
                active.lease.client_name,
                active.lease.expires_unix_ms
            ));
        }
        let ttl_ms = ttl_ms
            .unwrap_or(DEFAULT_LEASE_TTL_MS)
            .clamp(1, MAX_LEASE_TTL_MS);
        let lease = ControllerLease {
            lease_id: Uuid::new_v4().to_string(),
            client_name,
            ttl_ms,
            expires_unix_ms: now_unix_millis().saturating_add(ttl_ms),
        };
        *state = Some(ActiveControllerLease {
            lease: lease.clone(),
            expires_at: now + Duration::from_millis(ttl_ms),
        });
        Ok(lease)
    }

    pub(super) fn renew(&self, lease_id: &str, ttl_ms: Option<u64>) -> Result<ControllerLease> {
        let now = Instant::now();
        let mut state = self.state.lock();
        Self::expire_if_needed(&mut state, now);
        let active = state
            .as_mut()
            .ok_or_else(|| anyhow!("controller lease is not currently held"))?;
        if active.lease.lease_id != lease_id {
            return Err(anyhow!("controller lease '{lease_id}' is not current"));
        }
        active.lease.ttl_ms = ttl_ms
            .unwrap_or(active.lease.ttl_ms)
            .clamp(1, MAX_LEASE_TTL_MS);
        active.expires_at = now + Duration::from_millis(active.lease.ttl_ms);
        active.lease.expires_unix_ms = now_unix_millis().saturating_add(active.lease.ttl_ms);
        Ok(active.lease.clone())
    }

    pub(super) fn require_current(&self, lease_id: &str) -> Result<ControllerLease> {
        let now = Instant::now();
        let mut state = self.state.lock();
        Self::expire_if_needed(&mut state, now);
        let active = state
            .as_ref()
            .ok_or_else(|| anyhow!("controller lease is not currently held"))?;
        if active.lease.lease_id != lease_id {
            return Err(anyhow!("controller lease '{lease_id}' is not current"));
        }
        Ok(active.lease.clone())
    }

    pub(super) fn expire_if_needed(state: &mut Option<ActiveControllerLease>, now: Instant) {
        if let Some(lease) = state.as_ref()
            && lease.expires_at <= now
        {
            *state = None;
        }
    }
}
