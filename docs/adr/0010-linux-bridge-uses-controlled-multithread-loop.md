# Linux Bridge Uses A Controlled Multithread Loop

The Linux platform bridge will run a controlled multithreaded low-level libfuse3 loop instead of a single `fuse_session_loop`. libfuse3 remains responsible for mount/session transport, request decoding, and reply mechanics, while StudioFS owns bounded dispatch, callback concurrency, shutdown coordination, and backpressure around `StudioVfs` calls. This keeps large-file and Unity/Unreal-style small-file workloads from being serialized by one userspace loop without returning to a custom raw `/dev/fuse` transport.
