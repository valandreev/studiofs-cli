# Linux Bridge Uses A Controlled Multithread Loop

The Linux platform bridge will run a controlled multithreaded low-level libfuse3 loop instead of a single `fuse_session_loop`. libfuse3 remains responsible for mount/session transport, request decoding, and reply mechanics, while StudioFS owns bounded dispatch, callback concurrency, shutdown coordination, and backpressure around `StudioVfs` calls. This keeps large-file and Unity/Unreal-style small-file workloads from being serialized by one userspace loop without returning to a custom raw `/dev/fuse` transport.

With libfuse 3.12+, the callback worker limit defaults to
`min(max_background, 16)` independently of CPU count. Operators can override it
through `STUDIOFS_LINUX_FUSE_WORKER_THREADS` or the bridge connection request,
capped at `max_background` and 128. The conservative default follows mounted
cold/warm read A/B measurements: 32 workers improved one low-latency cold run,
but increased peak RSS and slowed the high-latency cold and both warm passes.
The 16-worker default favors memory and consistent behavior over that isolated
cold-read gain. It is not a universal optimum.

Older system libfuse remains supported with its previous CPU-based idle worker
retention. Its API cannot enforce total callback concurrency; explicit worker
overrides are rejected before mounting rather than silently becoming idle-only
limits. Diagnostics distinguish this legacy path. See
[platform bridge policy](../platform-bridges.md) and the
[opt-in worker measurement](../tests.md) for configuration and validation.
