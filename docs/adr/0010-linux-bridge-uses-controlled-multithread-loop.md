# Linux Bridge Uses A Controlled Multithread Loop

The Linux platform bridge will run a controlled multithreaded low-level libfuse3 loop instead of a single `fuse_session_loop`. libfuse3 remains responsible for mount/session transport, request decoding, and reply mechanics, while StudioFS owns bounded dispatch, callback concurrency, shutdown coordination, and backpressure around `StudioVfs` calls. This keeps large-file and Unity/Unreal-style small-file workloads from being serialized by one userspace loop without returning to a custom raw `/dev/fuse` transport.

With libfuse 3.12+, the callback worker limit defaults to
`min(max(16, 2 * available_parallelism), max_background, 128)`. The 16-worker
floor raises concurrency on small hosts whose callbacks block on local or
remote I/O; hosts with more than eight CPUs keep the previous CPU-derived
count, so no host loses workers relative to the earlier policy. Operators can
override it through `STUDIOFS_LINUX_FUSE_WORKER_THREADS` or the bridge
connection request, capped at `max_background` and 128.

This default is a conservative policy, not an evidence-selected optimum.
Exploratory single-repetition debug-build runs on one 8-CPU host (16 vs 32
workers, 0 and 200 ms injected GET latency) were inconclusive: 32 workers was
faster cold at 0 ms but slower cold at 200 ms, with 27–29% higher sampled RSS.
Those runs read the warm pass under the adaptive cache policy, so warm timings
likely reflected the kernel page cache rather than workers. Hosts with more than eight CPUs and worker counts
above 32 were not measured. Revisit the default with repeated release-build
runs of the opt-in worker measurement, which now forces direct I/O on the
observer and records peak RSS and retained worker counts.

Older system libfuse remains supported with its previous CPU-based idle worker
retention. Its API cannot enforce total callback concurrency; explicit worker
overrides are rejected before mounting rather than silently becoming idle-only
limits. Known gap: the required Rocky Linux 9.x release target ships libfuse
3.10, so the worker override is unavailable there until a 3.12+ libfuse is
packaged or bundled. Diagnostics distinguish this legacy path. See
[platform bridge policy](../platform-bridges.md) and the
[opt-in worker measurement](../tests.md) for configuration and validation.
