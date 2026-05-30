use super::*;

pub(crate) struct LocalDiagnosticsHost {
    #[cfg(unix)]
    endpoint_path: PathBuf,
    shutdown_tx: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl LocalDiagnosticsHost {
    pub(crate) fn start(
        runtime: &Handle,
        mount_point: &str,
        source: Arc<dyn DiagnosticsDataSource>,
        profiling_backend: Arc<dyn RuntimeProfilingBackend>,
    ) -> Result<Self> {
        #[cfg(test)]
        if let Some(message) = TEST_START_FAILURE
            .get_or_init(|| Mutex::new(None))
            .lock()
            .clone()
        {
            return Err(anyhow!(message));
        }

        let mounts_dir = utils::get_mounts_dir()?;
        let base_name = utils::sanitize_mount_point(mount_point);
        let endpoint = endpoint_for(&mounts_dir, &base_name, source.session_info().pid);
        #[cfg(unix)]
        let endpoint_path = bind_unix_listener_path(&endpoint)?;
        let discovery = discovery_from_source(source.session_info(), endpoint.clone());
        publish_discovery(mount_point, &discovery)?;

        let lease_manager = Arc::new(ControllerLeaseManager::default());
        let investigation_store = match InvestigationStore::default_store() {
            Ok(store) => Some(Arc::new(store)),
            Err(error) => {
                warn!(
                    mount_point,
                    error = %error,
                    "investigation store unavailable; run metadata will not persist"
                );
                None
            }
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let profiling = Arc::new(DiagnosticsProfilingCoordinator::new(Arc::new(
            CompositeRuntimeProfilingBackend::new(source.clone(), profiling_backend),
        )));
        let task = runtime.spawn(run_server(
            endpoint,
            discovery.auth_token.clone(),
            source,
            lease_manager,
            investigation_store,
            Some(profiling),
            shutdown_rx,
        ));

        Ok(Self {
            #[cfg(unix)]
            endpoint_path,
            shutdown_tx,
            task,
        })
    }

    pub(crate) fn start_best_effort(
        runtime: &Handle,
        mount_point: &str,
        source: Arc<dyn DiagnosticsDataSource>,
        profiling_backend: Arc<dyn RuntimeProfilingBackend>,
    ) -> Option<Self> {
        match Self::start(runtime, mount_point, source, profiling_backend) {
            Ok(host) => Some(host),
            Err(error) => {
                warn!(
                    mount_point,
                    error = %error,
                    "diagnostics host unavailable; continuing without live attach"
                );
                None
            }
        }
    }

    pub(crate) async fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        self.task.abort();
        if let Err(error) = self.task.await
            && !error.is_cancelled()
        {
            warn!(error = %error, "diagnostics host task join failed during shutdown");
        }
        #[cfg(unix)]
        let _ = fs::remove_file(self.endpoint_path);
    }
}
