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
- PostgreSQL carries shared metadata and active volume key material, including
  `volume_crypto_keys.key_material`.

SFEC protects remote payload confidentiality and integrity at rest. TLS protects
credentials, metadata/key-material links, and network transport. TLS does not
replace SFEC and SFEC does not replace TLS.

## S3-Compatible Object Storage Transport

S3-compatible object storage endpoints honor the operator-selected URL scheme.
Use `https://` for TLS. Use `http://` only when the operator intentionally
selects plain HTTP transport for that endpoint.

Plain HTTP S3 is accepted when the endpoint URL explicitly uses the `http://`
scheme. Bare host-port S3 endpoint overrides without a scheme remain a
development/test convenience and are accepted only for explicit local/test
loopback endpoints:

- `localhost`;
- IPv4 loopback in `127.0.0.0/8`;
- IPv6 loopback `::1`.

IPv4-mapped IPv6 loopback addresses and IPv4-compatible IPv6 loopback
addresses must be normalized to their embedded IPv4 address before
classification. For example, `::ffff:127.0.0.1` and `::127.0.0.1` are loopback
endpoints, while `::ffff:192.168.1.50` and `::192.168.1.50` are not.

memory:// remains a test-only operator path and is not a production transport
exception. The init/configuration layer rejects memory:// for production
user-supplied endpoints; explicit test utilities may still construct an
in-memory operator directly.

Examples:

| Endpoint | Classification | Policy |
|---|---|---|
| `https://s3.example.com` | operator-selected HTTPS remote | accepted |
| `http://s3.example.com` | operator-selected plain HTTP remote | accepted |
| `http://localhost:9000` | local/test loopback HTTP | accepted |
| `http://127.0.0.1:9000` | local/test loopback HTTP | accepted |
| `http://127.42.0.7:9000` | local/test loopback HTTP | accepted |
| `http://[::1]:9000` | local/test loopback HTTP | accepted |
| `http://192.168.1.50:9000` | operator-selected plain HTTP remote | accepted |
| `192.168.1.50:9000` | bare non-loopback host-port | rejected; use `http://192.168.1.50:9000` to select plain HTTP |
| `memory://` | test-only in-memory operator | accepted only in tests or explicit test utilities |

Path-style endpoint helpers such as `is_ip_endpoint` are addressing decisions,
not the security policy. They must not be treated as proof that a transport is
safe.

## PostgreSQL Metadata Transport

TLS is preferred for non-local PostgreSQL DSNs. If sslmode is omitted for a
non-local DSN, StudioFS uses the PostgreSQL driver's TLS-preferred behavior:
attempt TLS first, then fall back to cleartext when the server does not support
TLS. The fallback is allowed for operator compatibility and must be logged at
info level, not warning level.

Non-local PostgreSQL DSNs use TLS-preferred or TLS-required transport.
Operators who require strict transport encryption can set `sslmode=require`.
In that mode TLS setup failures remain fatal and there is no cleartext
fallback. `sslmode=disable` is accepted as an explicit cleartext operator
choice and must also be logged at info level for non-local hosts.

Accepted local/test loopback DSNs are limited to hosts classified as:

- `localhost`;
- IPv4 loopback in `127.0.0.0/8`;
- IPv6 loopback `::1`;
- Unix domain sockets, such as keyword DSN `host=/var/run/postgresql` on Unix
  systems or an implicit socket-based default where the PostgreSQL driver
  resolves the connection locally.

IPv4-mapped IPv6 loopback addresses and IPv4-compatible IPv6 loopback
addresses must be normalized to their embedded IPv4 address before
classification for PostgreSQL DSNs as well.

There must be no noisy downgrade warning for remote PostgreSQL hosts. A
PostgreSQL connection layer should make the default TLS attempt visible through
info-level diagnostics while keeping cleartext fallback non-fatal when the
server lacks TLS support.

Examples:

| DSN shape | Classification | Policy |
|---|---|---|
| `postgresql://user:pass@db.example.com/studiofs` | non-local TLS-preferred remote | accepted; fallback to cleartext if server lacks TLS |
| `postgresql://user:pass@db.example.com/studiofs?sslmode=require` | non-local TLS-required remote | accepted; TLS failure is fatal |
| `postgresql://user:pass@db.example.com/studiofs?sslmode=disable` | non-local explicit cleartext remote | accepted with info log |
| `host=db.example.com user=studiofs sslmode=disable` | non-local explicit cleartext remote | accepted with info log |
| `postgresql://user:pass@localhost/studiofs?sslmode=disable` | local/test loopback cleartext | accepted |
| `host=127.0.0.1 user=studiofs sslmode=disable` | local/test loopback cleartext | accepted |
| `host=127.42.0.7 user=studiofs sslmode=disable` | local/test loopback cleartext | accepted |
| `host=::1 user=studiofs sslmode=disable` | local/test loopback cleartext | accepted |
| `host=/var/run/postgresql user=studiofs sslmode=disable` | local Unix domain socket | accepted on Unix |

PostgreSQL is sensitive even though StudioFS does not encrypt PostgreSQL
metadata at rest. PostgreSQL stores shared namespace metadata and active volume
key material, so non-local PostgreSQL links must be treated as carrying secrets.

## Error Behavior

StudioFS must reject policy-invalid transports early, before creating or
mounting a volume through that transport. PostgreSQL cleartext fallback is not
policy-invalid when it follows the TLS-preferred compatibility behavior above.
For the S3 side, malformed endpoints, unsupported schemes, credentials embedded
in endpoint URLs, `memory://` runtime endpoints, and bare non-loopback host-port
values are rejected before init, persisted mount, or endpoint override.

Actionable errors must identify:

- whether the rejected transport is S3-compatible object storage or PostgreSQL;
- the endpoint or DSN host;
- the rejected mode, such as an unsupported scheme or an implicit bare
  non-loopback host-port value;
- the accepted local exception for implicit S3 HTTP and PostgreSQL cleartext:
  `localhost`, `127.0.0.0/8`, `::1`, normalized IPv4-mapped IPv6 loopback
  addresses, or Unix domain sockets for PostgreSQL.

Errors and info-level PostgreSQL transport diagnostics must not log credentials
or key material. They must sanitize DSNs and endpoints before formatting them
into errors or logs. Sanitization must strip or mask DSN passwords, access keys,
secret keys, secret-bearing query parameters, volume keys, and raw
`volume_crypto_keys.key_material`.

## Local Development And CI

Existing local development and CI setups using loopback RustFS/PostgreSQL remain
supported. Bare loopback S3 endpoints remain a narrow convenience so Windows,
Linux, and macOS local test environments can keep using loopback HTTP RustFS
without requiring a URL scheme.

Non-loopback private networks are not implicit local exceptions. Addresses such
as `10.0.0.5`, `172.16.0.5`, `192.168.1.50`, and non-loopback IPv6 addresses
require an explicit `http://` or `https://` scheme.

## Follow-Up Boundary

SFS-449 implements the PostgreSQL connection layer and replaces production
`tokio_postgres::NoTls` call sites with TLS-aware behavior that follows this
contract.
Production PostgreSQL call sites use `connect_studiofs_postgres` instead of
`tokio_postgres::NoTls`; remaining `tokio_postgres::NoTls` call sites are local
fixture setup.

SFS-450 enforces S3 endpoint transport policy in `DataStore`, `init`, and mount
endpoint overrides.

SFS-452 can reference this contract for operator-facing diagnostics and
documentation, but must keep the remaining work transport-only and must not
reopen at-rest SFEC implementation decisions.

## SFS-452 Final Acceptance Gate

SFS-452 is the final transport encryption acceptance gate. It freezes the
operator-facing relationship between S3 transport, PostgreSQL transport, and
SFEC at-rest protection:

- S3-compatible endpoints honor the operator-selected `https://` or `http://`
  scheme;
- Plain HTTP S3 is accepted when the endpoint explicitly uses `http://`, or as
  the existing bare-host loopback development/test convenience;
- non-local PostgreSQL DSNs use TLS-preferred or TLS-required transport by
  default, while `sslmode=disable` is accepted as an explicit cleartext operator
  choice and logged at info level;
- PostgreSQL carries shared metadata and active volume key material, so its
  transport remains security-sensitive even though PostgreSQL metadata is not
  encrypted at rest by SFEC;
- SFEC protects remote payload confidentiality and integrity at rest;
- TLS protects credentials, metadata/key-material links, and network transport;
- TLS does not replace SFEC and SFEC does not replace TLS;
- malformed, credential-bearing, `memory://`, unsupported-scheme, and bare
  non-loopback S3 endpoints are rejected before init, persisted mount, or
  endpoint override;
- production PostgreSQL call sites use `connect_studiofs_postgres` instead of
  `tokio_postgres::NoTls`;
- remaining `tokio_postgres::NoTls` call sites are local fixture setup.

Local loopback examples remain available as development/test examples. Non-local
S3-compatible examples should prefer HTTPS unless the operator intentionally
selects explicit HTTP. This final transport gate adds no user passphrases, KMS,
S3 SSE/KMS, AES/RSA modes, legacy plaintext fallback, or local-cache
encryption.
