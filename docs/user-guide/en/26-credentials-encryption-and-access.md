# 26. Credentials, Encryption and Access
> **Work in progress**

StudioFS protection consists of several components, each responsible for a different aspect.

## Which secrets need protecting?

A volume needs S3 credentials and a PostgreSQL connection string. If that connection string contains a password, the entire string is a secret. StudioFS also automatically creates an encryption key for the volume's contents.

These details do not stay only in memory during the first run. After `init`, the local metadata database `<volume-name>.db` contains the volume configuration, including S3 access keys, the PostgreSQL connection string, and the encryption key. The volume configuration is also sent to the shared PostgreSQL metadata, where the encryption key material is stored too. During `discover`, another computer receives the configuration and creates its own local database.

**Treat the local database, PostgreSQL, and their backups as stores of secrets.** Do not add them to a repository, send them to public chats, or attach them to support requests before inspecting the files and removing secrets. Hiding a password in diagnostic output does not mean it is encrypted in the database file.

## What is encrypted automatically?

In the current volume format, StudioFS encrypts contents before uploading them to S3-compatible storage. It uses XChaCha20-Poly1305 and a random 256-bit volume key. Each remote fragment is encrypted separately; the whole file is not encrypted as a single object.

When reading, the client authenticates and decrypts the encrypted fragments. A damaged or substituted object causes a verification error, rather than an attempt to read it as plaintext.

You do not need to enable this manually. StudioFS has no separate encryption flag, encryption password, or KMS setting. You do not need to add such options to `init`, `mount`, or `settings`. Compatibility with older volumes whose remote objects are unencrypted is not guaranteed by the current format.

This protection is useful if someone gains access to the objects in the bucket alone. But the volume key is stored in metadata. **If an attacker obtains both the encrypted objects and the metadata containing the key, this encryption no longer protects the contents.**

## What remains in plaintext?

Encryption of remote contents does not cover all StudioFS data:

| Location or data | Protection boundary |
|---|---|
| Contents of remote fragments in S3 | Encrypted by StudioFS before upload |
| Local cache and upload staging files | May contain plaintext contents; StudioFS does not encrypt them |
| Local metadata database and PostgreSQL | Not encrypted by this mechanism |
| File names, sizes, attributes, ACLs, extended attributes, and metadata snapshots | Not hidden by this mechanism |
| Process memory and file system buffers | Contain plaintext data during operation |

## Protecting connections

Encryption of S3 objects and encryption of a network connection serve different purposes. Use `https://` for a production S3 endpoint. StudioFS also accepts an explicitly specified `http://` address, but use it only for a connection on a trusted local network. If clients connect over the internet, configure TLS on a reverse proxy and use `https://`.

For PostgreSQL, pay attention to the `sslmode` parameter in the connection string:

- If it is not specified, the default mode first tries TLS, but allows an unencrypted connection when the server does not support TLS.
- `sslmode=require` requires TLS: if a secure connection cannot be established, the connection fails.
- `sslmode=disable` explicitly disables TLS. This is an administrator's deliberate choice, not a recommended way to resolve a certificate error.

The current PostgreSQL client uses Rustls and the `webpki-roots` root certificate set to validate the certificate and server name. Use a server name that matches the certificate. If you replace it with a VPN address while the certificate is issued only for a DNS name, validation may fail.

## Private networking and access permissions

As described in the [Tailscale chapter](08-tailscale-netbird-a-private-path-to-postgresql.md), restrict access to PostgreSQL to a private route. Tailscale (or NetBird) restricts the path to the server, but permissions must be configured at every boundary:

- In the VPN, grant designated users and devices access only to the PostgreSQL server and port.
- In the firewall and cloud provider rules, block public access to PostgreSQL.
- In PostgreSQL, configure authentication, allowed IP addresses, and role permissions.
- In S3, keep the bucket private and grant credentials access only to the required bucket or prefix and necessary operations.

Through an SNAT gateway, the database may see the gateway's address instead of the laptop's. Account for this in IP-based access rules, but do not open access to the entire internet for convenience.

---

[Contents](README.md) · [Українська](../uk/26-credentials-encryption-and-access.md) · [← Previous](25-migrating-studiofs-metadata-to-new-postgresql-server.md) · [Next →](27-maintenance-and-removing-a-volume.md)
