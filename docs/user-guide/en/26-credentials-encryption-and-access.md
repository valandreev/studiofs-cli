# 26. Credentials, Encryption and Access

Your files are now available from several computers. It is time to check who else might be able to reach them: through S3, the metadata database, or simply a lost laptop. Protection in StudioFS has several parts, and each has its own job.

## Which secrets need protecting?

A volume needs S3 credentials and a PostgreSQL connection string. If that string contains a password, the entire string is a secret. StudioFS also automatically creates a key to encrypt the volume's contents.

These details do not stay only in memory during the first run. After `init`, the local metadata database `<volume-name>.db` contains the volume configuration, including the S3 access keys, PostgreSQL connection string, and encryption key. The volume configuration is also sent to the shared PostgreSQL metadata, which stores the encryption key material too. During `discover`, another computer receives the configuration and creates its own local database.

**Treat the local database, PostgreSQL, and their backups as stores of secrets.** Do not add them to a repository, send them to a public chat, or attach them to a support request without inspecting and sanitizing them. Hiding a password in diagnostic output does not mean it is encrypted in the database file.

When entering secrets on the command line, consider shell history, terminal recordings, and whether other system users can see process arguments. Do not put real keys in screenshots or shared instructions. Keep the original credentials in a password manager accessible only to the people responsible for them.

## What is encrypted automatically?

In the current volume format, StudioFS encrypts contents before uploading them to S3-compatible storage. It uses XChaCha20-Poly1305 and a random 256-bit volume key. Individual remote fragments are encrypted independently; a whole file is not stored as one encrypted object.

When reading, the client authenticates and decrypts the encrypted fragments. A damaged or substituted object causes a verification error, rather than an attempt to read it as plaintext.

You do not need to enable this manually. StudioFS has no separate encryption flag, encryption password, or KMS setting. There is no need to add such options to `init`, `mount`, or `settings`. Compatibility between older volumes with unencrypted remote objects and the current format is not guaranteed.

This protection helps if an outsider gains access to the objects in the bucket alone. But the volume key is stored in metadata. **If an attacker obtains both the encrypted objects and the metadata containing the key, this encryption no longer protects the contents.**

## What remains in plaintext?

Remote content encryption does not cover all StudioFS data:

| Location or data | Protection boundary |
|---|---|
| Contents of remote fragments in S3 | Encrypted by StudioFS before upload |
| Local cache and upload staging files | May contain plaintext contents; StudioFS does not encrypt them |
| Local metadata database and PostgreSQL | Not encrypted by this mechanism |
| File names, sizes, attributes, ACLs, extended attributes, and metadata snapshots | Not hidden by this mechanism |
| Process memory and file system buffers | Contain plaintext data during operation |

On laptops and workstations, enable system disk encryption through your OS: BitLocker, FileVault, or the appropriate Linux mechanism. Use OS permissions to restrict access to the metadata directory, cache, and backups. Check the drives used for temporary data too. Disk encryption helps protect a device that is powered off or locked, but does not replace protection for an active user session.

## Protecting connections

Encrypting S3 objects and encrypting a network connection serve different purposes. Use `https://` for a production S3 endpoint. StudioFS also accepts an explicitly specified `http://`, so a successful connection alone does not prove the transport is protected by TLS. Encrypting object contents does not make a remote HTTP connection safe for credentials.

For PostgreSQL, pay attention to the `sslmode` parameter in the connection string:

- If it is omitted, the default mode tries TLS first, but allows an unencrypted connection when the server does not support TLS.
- `sslmode=require` requires TLS: if a secure connection cannot be established, the connection fails.
- `sslmode=disable` explicitly disables TLS. This is a deliberate administrator choice, rather than a recommended way to fix a certificate error.

The current PostgreSQL client validates the certificate and server name through Rustls using the bundled `webpki-roots` root certificates. Do not assume that a certificate added to your OS trust store automatically becomes trusted by StudioFS. A private or self-signed certificate requires a separate compatibility check; do not rely on unverified parameters such as `sslrootcert` as a ready-made way to add trust.

Use a server name that matches the certificate. If you replace it with a VPN address while the certificate covers only a DNS name, validation may fail. Work with your administrator to align the private route, DNS name, and certificate before connecting workstations.

## Private networking and access permissions

As described in the [Tailscale chapter](08-tailscale-netbird-a-private-path-to-postgresql.md), keep PostgreSQL accessible through a private route. Tailscale or NetBird restricts the path to the server, but permissions must be configured at each boundary:

- In the VPN, allow the required users and devices to reach only the required server and port.
- In the firewall and cloud provider rules, block public access to PostgreSQL.
- In PostgreSQL, configure authentication, allowed addresses, and role permissions.
- In S3, keep the bucket private and limit credential permissions to the required bucket or prefix and operations.

Through a gateway using SNAT, the database may see the gateway's address instead of the laptop's. Account for this in the allowed addresses, but do not open access to the entire internet for convenience.

A private route does not automatically enable PostgreSQL TLS. Similarly, VPN membership does not grant permissions in the database or S3. Do not expect a separate VPN account by itself to create individual permissions for StudioFS files: service access rules and file system permissions need to be considered separately.

## When someone leaves the team or a secret may have leaked

Revoking a VPN invitation is not enough if the person already has a copy of local metadata and credentials. Start by restricting the device's access through the VPN and to PostgreSQL and S3. If you suspect a leak, change the affected credentials at the provider and inspect the services' access logs.

Plan routine rotation in advance: identify the computers and processes using the old access, prepare new credentials, coordinate updates to the stored configuration, and verify client access. Revoke the old credentials after the transition. Simply creating a new S3 key in the provider's control panel does not update the configuration StudioFS has already stored.

The current public CLI has no dedicated command for rotating a volume's encryption key, or a general `settings` option for replacing secrets. Do not edit the metadata database manually or run `init` over a working volume to change a password. Changing stored configuration requires a verified procedure for your StudioFS version, agreed with your administrator or support.

Changing the PostgreSQL password or S3 access keys does not change the encryption key for contents already written. If the volume key itself has leaked, revoking network access will not secure copies of data already held by an outsider. Separately plan the transfer of data to a new volume with a new key and verification of the result; do not delete the old volume until the copy has been confirmed complete and recoverable.

## Before the next connection

Check four things: S3 uses HTTPS; PostgreSQL has the required TLS mode and a trusted certificate; the private route allows access only to the required devices; and local disks, metadata, and backups are protected against unauthorized reading. This protects both the remote contents and the keys that make them readable.

---

[Contents](README.md) · [Українська](../uk/26-credentials-encryption-and-access.md) · [← Previous](25-backups-even-time-machines-need-insurance.md) · [Next →](27-maintenance-and-removing-a-volume.md)

[Linear source](https://linear.app/valandreev/document/26-credentials-encryption-and-access-226f59cf2da6)
