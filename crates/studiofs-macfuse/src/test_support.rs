//! Shared setup for deterministic deferred namespace callback tests.

pub(crate) fn dormant_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build dormant runtime")
}

pub(crate) fn admit_parent_on_runtime(
    fixture: &studiofs_vfs::contract_harness::ContractVfsFixture,
    runtime: &tokio::runtime::Runtime,
    name: &[u8],
) -> studiofs_meta::InodeAttr {
    fixture.runtime().block_on(async {
        fixture
            .vfs()
            .mkdir_with_deferred_metadata_admission(
                studiofs_vfs::contract_harness::CONTRACT_ROOT_INO,
                name,
                501,
                20,
                0o755,
                runtime.handle(),
            )
            .expect("admit deferred parent directory")
    })
}

pub(crate) fn callback_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .max_blocking_threads(1)
        .build()
        .expect("build single-worker callback runtime")
}
