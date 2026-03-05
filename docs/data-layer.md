# Data Layer

`studiofs-data` handles object I/O against S3-compatible storage and object encoding/decoding.

## Responsibilities

- PUT/GET/DELETE/HEAD object operations.
- Object key generation and sharding.
- Object payload validation (checksum).
- Read/write connection profile separation.

## Key Layout

```
<volume>/objects/<ab>/<cd>/<object_id>
```

`<ab>/<cd>` are first hex bytes of object id for distribution.

## Object Format

- Header + payload layout.
- Versioned format.
- xxh64 checksum validation on read.
- Immutable object semantics.

## I/O Profiles

Separate pools for reads and writes to avoid upload pressure impacting read latency.

Common tunables:
- request timeout
- retry count/backoff
- concurrent limit

## Note on Metadata

Metadata is not stored in S3 manifests anymore.
Current metadata authority is PostgreSQL (`studiofs-meta` / `PgMetaEngine`).
