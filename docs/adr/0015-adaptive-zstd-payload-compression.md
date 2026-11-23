# Remote Payload Fragments Use Adaptive Zstd Compression With A Chunk Seek Table

Status: proposed (SFS-1067). Implementation is tracked in the Linear project
"[0.2.4] Compression".

## Context

Much studio data compresses well: scanline or uncompressed OpenEXR, ASCII USD,
`.hip`, `.nk`, geometry caches, manifests, and logs. Video, JPEG/PNG previews,
and lossy EXR do not. StudioFS currently uploads every remote payload fragment
raw, so compressible assets pay full S3 storage and WAN transfer.

Every remote payload fragment is an STFS object (30-byte header plus up to
4 MiB of payload) sealed in an `SFEC` envelope with 1 MiB XChaCha20-Poly1305
frames ([encryption-at-rest contract](../encryption-at-rest-contract.md)).
There is no plaintext remote mode. Cold random reads of 256 KiB or less fetch
the SFEC prefix, frame 0 (which holds the STFS header), and only the frames
covering the requested range. That path maps payload offsets to SFEC plaintext
offsets one to one.

The STFS reader rejects `version > 2` and the legacy LZ4 flag, and parses flags
with `from_bits_truncate`, so a new flag bit alone would read as a size or
checksum failure on an older build.

## Decision

### Scope

- Only remote payload fragments (`prepare_payload` and `get_payload_*`) are
  compressed. Snapshot artifacts and other keys are not.
- Writer state, RAM buffers, upload stage files, and the local payload cache
  stay plaintext and uncompressed. Compression happens only between building
  the STFS object and SFEC encryption; decompression happens after SFEC
  decryption and before cache insertion.
- Existing objects are never rewritten. There is no backfill.

### Codec

Zstd is the only codec. The format carries a codec byte so another codec can be
added later without a new object version.

### Object format

Uncompressed fragments keep STFS v2 unchanged. Compressed fragments use STFS v3:

```text
+------------------+
| Magic "STFS"     |  4 bytes
| Version = 3      |  2 bytes
| Flags            |  2 bytes (no compression flag; version implies it)
| Original Size    |  4 bytes (logical payload bytes)
| Compressed Size  |  4 bytes (bytes after the header, seek table included)
| Checksum         |  8 bytes (XXH64 of the original payload)
| Codec            |  1 byte  (1 = zstd)
| Chunk log2       |  1 byte  (17 = 128 KiB)
| Chunk count      |  2 bytes
| Reserved         |  2 bytes (must be zero)
+------------------+
| Seek table       |  chunk count x u32 LE
|                  |  bit 31: chunk stored raw; bits 0-30: stored length
+------------------+
| Chunk payloads   |  in order
+------------------+
```

- The payload is split into independent 128 KiB chunks. Each compressed chunk
  is one zstd frame without the zstd content checksum.
- A chunk's original length is derived from `Original Size` and its index.
  Decompression writes into a buffer of exactly that length; any other output
  length is a typed error. This bounds memory against decompression bombs.
- The decoder validates the whole geometry (chunk count, stored lengths, their
  sum, raw lengths, fragment bound) before touching chunk data.
- Integrity comes from SFEC frame authentication. Complete reads also check
  XXH64 of the original payload, as today.
- `LEGACY_LZ4_COMPRESSED` stays rejected.

For a full 4 MiB fragment the seek table is 32 entries (128 bytes).

### Encryption framing

STFS v3 objects are sealed with 64 KiB SFEC frames; STFS v2 objects keep 1 MiB
frames. The SFEC header already records `frame_size` per object; the encrypt
path and the key-wrap associated data take it from the header instead of the
1 MiB constant. The bound value is unchanged for existing 1 MiB objects.

The order stays compress, then encrypt. The tag overhead of 64 KiB frames is
16 bytes per frame, about 0.02%.

### Probe and thresholds

On upload, with compression enabled:

1. Compress the first 64 KiB of the fragment with zstd level 1. If that saves
   less than 10%, write STFS v2.
2. Otherwise compress every chunk at the volume level. A chunk that does not
   shrink is stored raw.
3. If the whole object saves less than 10%, write STFS v2.
4. Otherwise write STFS v3.

There are no file-extension or magic-byte hints: the upload path works with
object and fragment identities and has no path. Data that becomes compressible
only after its first 64 KiB is stored raw; this is an accepted trade-off.

### CPU placement

Compression and decompression run on the existing blocking crypto admission,
with chunks processed in parallel like SFEC frames. Nothing is added to FUSE or
WinFSP callbacks, and `write`, `flush`, `fsync`, and handle release keep their
current local durability contracts.

### Read paths

- Complete fetch decrypts, decodes the seek table, decompresses every chunk,
  checks XXH64, and populates the local cache and singleflight with plaintext.
- Cold range reads start with the fixed SFEC prefix, then read the header tail
  and frame 0. For v3, frame 0 holds the STFS header and the whole seek table.
  After authenticating frame 0 and validating the table, the reader reuses any
  needed chunk bytes already in frame 0. A single additional read retrieves
  the remaining frames covering the requested chunks, including when the
  first of those frames is adjacent to frame 0. Only the requested compressed
  chunks are decompressed. For v2, the prefix and logical offsets still let
  the reader merge adjacent frames or fetch nonadjacent frames concurrently.
  Partial plaintext never populates the complete cache.

A small read of a compressed object therefore transfers the prefix, 64 KiB of
frame 0, and the frames covering one or two compressed chunks, and costs at
most three object-store reads.

### Policy

- The policy is a shared volume setting in `VolumeSettings`:
  `compression = off | zstd:<level>` with level 1 to 19. It replicates like the
  other shared settings and is changed with
  `studiofs compression <name> [off|zstd[:<level>]]`.
- New volumes default to `zstd:3`. There is no client-local override.
- The field is added without a global metadata format bump: settings events
  get a new wire tag, serving checkpoints a new format version, and the local
  and PostgreSQL records a default. Older settings decode as `off`.
- A writer reads the policy from its local metadata view when it prepares an
  upload. Fragments already queued use the policy current at preparation.

### Compatibility

Clients older than 0.2.5 are not supported. STFS v3, 64 KiB SFEC frames, and the
`compression` setting are part of the 0.2.5 compatibility baseline (SFS-1001,
SFS-1002), so they need no separate capability. A pre-0.2.5 reader rejects v3
through its version check.

### Accounting

Logical quota keeps counting logical bytes. Clients report logical and stored
upload bytes, the number of fragments written as v3 or stored raw by the probe
or the final threshold, and decompression time.

The client-lifetime upload counters advance after a successful fragment PUT.
Preparation and failed PUT attempts do not count; another successful PUT of the
same fragment counts again. Generic snapshot artifact uploads do not count.
`logical_bytes_uploaded` counts uncompressed payload bytes.
`stored_bytes_uploaded` counts the STFS body (including the v3 seek table),
excluding the STFS header and SFEC envelope, frame tags, and key wrapping.
Existing encrypted byte counters continue to describe network bytes.
The observed ratio is stored bytes divided by logical bytes; smaller is better,
and it is absent until at least one nonempty fragment has been uploaded.
`compression_policy` is the effective policy read from local metadata, and is
absent if policy lookup fails.

The upload stats and local diagnostics expose those counters, v3 fragment count,
probe and final-threshold raw fallback counts, and v3 `decompression_nanos`.
Decode time is wall time around complete and range v3 decoding, including
blocking admission and checksum work but excluding network I/O and decryption;
failed decode work also counts. Local cached reads do not decode again.
Read and write connection pools share the same client counters.
The runtime stats text surface exposes the same byte/count counters under
`object_io`, decode time as `decompression` in nanoseconds, effective policy as
`compression_policy_known`, `compression_enabled`, and `compression_level`
gauges, and the stored/logical ratio in millionths. Missing policy and ratio
values are omitted from their gauges; `compression_policy_known` is zero when
lookup fails. Quota continues to count logical inode sizes independently of
these transfer counters.

### Security

Compression makes each fragment's stored size depend on its content, and an
object-store observer can see that size. A CRIME/BREACH-style attack also needs
attacker-controlled plaintext mixed with a secret in the same fragment and a way
to trigger repeated uploads while watching sizes. That does not match the
threat model of the encryption-at-rest contract, which already leaves file
sizes unencrypted. No padding is added. Volumes that need to hide compressibility
use `off`.

## Consequences

Compressible assets use less S3 storage and WAN bandwidth, and range reads of
compressed objects transfer less than range reads of raw objects because their
frames are smaller. Incompressible data costs one 64 KiB level-1 probe per
fragment.

Local cache size and local read CPU do not change. Two object versions and two
SFEC frame sizes exist side by side, and every reader must handle both.

Validation needs a mounted `studiofs-e2e` scenario with compressible and
incompressible data, cold range reads, and a policy switch, on macFUSE and Linux
FUSE. Native WinFSP validation is a separate gate.
