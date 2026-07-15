# Linux Mount Defaults To direct_io And Self-Tuning

StudioFS Linux mounts default to `direct_io` so media workloads do not fight an
extra FUSE kernel page-cache layer while StudioFS already owns local
writer/cache behavior. Direct mode requests FUSE `ASYNC_DIO` by default;
`STUDIOFS_FUSE_ASYNC_DIO=0` is the serial rollback control. The user-facing
mount UX stays simple: users may choose the cache location, cache size, and S3
endpoint, while Linux FUSE sizing, background queues, cache policy, and
workload adaptation remain internal self-tuning decisions.

Kernel writeback caching remains an internal experiment. It may be selected
automatically only after mounted promotion evidence shows that it helps
small-file workloads without stale-read, durability, drain, CPU, or memory
regressions. The current Direct decision and its measured limitation are
recorded in [Linux cache and CPU validation](../perf/linux-cache-performance-result.md).
