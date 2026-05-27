# Transport Encryption Contract

## Status

This document defines the StudioFS transport encryption contract for SFS-448.
It is the policy vocabulary for SFS-449, SFS-450, and SFS-452.

This is transport-only work. It must not reopen at-rest SFEC implementation
decisions, `EncryptionConfig`, key IDs, `DataStoreCryptoConfig`, plaintext
local-cache behavior, or the at-rest threat model defined in
[encryption-at-rest-contract.md](./encryption-at-rest-contract.md).

StudioFS already encrypts remote payload objects at rest. Transport encryption
protects links to services that now carry sensitive data:

- S3-compatible object storage carries encrypted `SFEC` payload fragments.
- PostgreSQL carries metadata and active volume key material, including
  `volume_crypto_keys.key_material`.

## S3-Compatible Object Storage Transport

HTTPS is required for non-local S3-compatible object storage endpoints.

Plain HTTP is allowed only for explicit local/test loopback endpoints:

- `localhost`;
- IPv4 loopback in `127.0.0.0/8`;
- IPv6 loopback `::1`.

IPv4-mapped IPv6 loopback addresses must be normalized to their embedded IPv4
address before classification. For example, `::ffff:127.0.0.1` is a loopback
endpoint, while `::ffff:192.168.1.50` is not.

memory:// remains a test-only operator path and is not a production transport
exception. The init/configuration layer rejects memory:// for production
user-supplied endpoints; explicit test utilities may still construct an
in-memory operator directly.

Examples:

| Endpoint | Classification | Policy |
|---|---|---|
| `https://s3.example.com` | non-local HTTPS remote | accepted |
| `http://s3.example.com` | non-local plain HTTP remote | rejected |
| `http://localhost:9000` | local/test loopback HTTP | accepted |
| `http://127.0.0.1:9000` | local/test loopback HTTP | accepted |
| `http://127.42.0.7:9000` | local/test loopback HTTP | accepted |
| `http://[::1]:9000` | local/test loopback HTTP | accepted |
| `http://192.168.1.50:9000` | non-local plain HTTP remote | rejected |
| `memory://` | test-only in-memory operator | accepted only in tests or explicit test utilities |

Path-style endpoint helpers such as `is_ip_endpoint` are addressing decisions,
not the security policy. They must not be treated as proof that a transport is
safe.

## PostgreSQL Metadata Transport

TLS is required for non-local PostgreSQL DSNs. sslmode=disable is allowed only
for explicit local/test loopback DSNs.

Accepted local/test loopback DSNs are limited to hosts classified as:

- `localhost`;
- IPv4 loopback in `127.0.0.0/8`;
- IPv6 loopback `::1`;
- Unix domain sockets, such as keyword DSN `host=/var/run/postgresql` on Unix
  systems or an implicit socket-based default where the PostgreSQL driver
  resolves the connection locally.

IPv4-mapped IPv6 loopback addresses must be normalized to their embedded IPv4
address before classification for PostgreSQL DSNs as well.

There must be no silent downgrade for remote hosts. A PostgreSQL connection
layer must not fall back from TLS to cleartext for non-local DSNs if TLS setup
or validation fails.

Examples:

| DSN shape | Classification | Policy |
|---|---|---|
| `postgresql://user:pass@db.example.com/studiofs` with TLS enabled | non-local TLS remote | accepted |
| `postgresql://user:pass@db.example.com/studiofs?sslmode=disable` | non-local cleartext remote | rejected |
| `host=db.example.com user=studiofs sslmode=disable` | non-local cleartext remote | rejected |
| `postgresql://user:pass@localhost/studiofs?sslmode=disable` | local/test loopback cleartext | accepted |
| `host=127.0.0.1 user=studiofs sslmode=disable` | local/test loopback cleartext | accepted |
| `host=127.42.0.7 user=studiofs sslmode=disable` | local/test loopback cleartext | accepted |
| `host=::1 user=studiofs sslmode=disable` | local/test loopback cleartext | accepted |
| `host=/var/run/postgresql user=studiofs sslmode=disable` | local Unix domain socket | accepted on Unix |

PostgreSQL is sensitive even though StudioFS does not encrypt PostgreSQL
metadata at rest. PostgreSQL stores shared namespace metadata and active volume
key material, so non-local PostgreSQL links must be treated as carrying secrets.

## Error Behavior

StudioFS must reject insecure non-local transports early, before creating or
mounting a volume through that transport.

Actionable errors must identify:

- whether the rejected transport is S3-compatible object storage or PostgreSQL;
- the endpoint or DSN host;
- the rejected insecure mode, such as plain HTTP or `sslmode=disable`;
- the accepted local exception: `localhost`, `127.0.0.0/8`, `::1`, normalized
  IPv4-mapped IPv6 loopback addresses, or Unix domain sockets for PostgreSQL.

Errors must reject insecure links without logging credentials or key material.
They must not log DSN passwords, access keys, secret keys, volume keys, or raw
`volume_crypto_keys.key_material`.

## Local Development And CI

Existing local development and CI setups using loopback RustFS/PostgreSQL remain
supported. The allowed insecure cases are intentionally narrow so Windows,
Linux, and macOS local test environments can keep using loopback HTTP RustFS
and loopback PostgreSQL without weakening production defaults.

Non-loopback private networks are not local exceptions. Addresses such as
`10.0.0.5`, `172.16.0.5`, `192.168.1.50`, and non-loopback IPv6 addresses are
remote for this policy unless a future issue adds an explicit, reviewed
operator override.

## Follow-Up Boundary

SFS-449 implements the PostgreSQL connection layer and replaces production
`tokio_postgres::NoTls` call sites with TLS-aware behavior that follows this
contract.

SFS-450 enforces S3 endpoint transport policy in `DataStore`, `init`, and mount
endpoint overrides.

SFS-452 can reference this contract for operator-facing diagnostics and
documentation, but must keep the remaining work transport-only and must not
reopen at-rest SFEC implementation decisions.
