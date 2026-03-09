# NATS Invalidation Bus (Deprecated)

NATS integration has been removed from StudioFS.

Current metadata synchronization uses PostgreSQL only:
- local `redb` for foreground operations,
- background flush/pull with PostgreSQL snapshots.

Use these docs instead:
- [Metadata Engine](metadata.md)
- [Architecture Overview](architecture.md)
- [Configuration Reference](configuration.md)
