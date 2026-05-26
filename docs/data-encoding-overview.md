# How StudioFS Encodes And Decodes File Data

This is a plain-language overview of how StudioFS turns file data into stored
objects and then turns those objects back into readable files. It does not
replace the technical encryption-at-rest contract or the data-layer document.
Think of it as the story of the data path: what happens on the local machine,
what is sent to object storage, and how StudioFS reads it back safely.

## The Short Version

When an application writes a file into a StudioFS mount, StudioFS accepts normal
file bytes and keeps them usable through the local filesystem path. The local
runtime and local cache work with plaintext because they are part of the
trusted local performance boundary.

Before payload data is uploaded to object storage, StudioFS packages it as a
StudioFS object and encrypts that object. Remote object storage receives
encrypted `SFEC` objects, not readable plaintext `STFS` objects.

When StudioFS later needs to read from remote storage, it downloads the
encrypted object, checks that it belongs to the expected volume and object key,
decrypts it, verifies the decoded payload, and only then gives the plaintext
back to the filesystem read path.

## Why We Do It This Way

The protection is designed to stay out of the user's way. StudioFS should feel
like a normal filesystem: applications open, save, rename, and read files
without needing a separate encryption workflow.

That means encryption is automatic at the remote payload boundary. StudioFS
owns the packaging, encryption, upload, download, decryption, and verification
steps internally. Users and applications keep using regular files.

This is a deliberate product choice. StudioFS protects remote payload objects
without asking ordinary users to manage cryptographic details as part of their
daily work.

## What Happens When We Write Data

A write starts as ordinary file data from an application.

StudioFS first accepts that data into local runtime state and local cache so
the mount can stay responsive. This is important for local-first behavior:
local reads and writes should not turn into a slow remote-storage workflow.

When a payload object is ready for remote storage, StudioFS builds the normal
inner StudioFS object. That inner object is still plaintext inside the trusted
local boundary.

At the object-storage edge, StudioFS encrypts the whole object and stores the
encrypted result as an `SFEC` object. The bucket should only receive
ciphertext for payload objects.

## What Happens When We Read Data

If the needed data is already in the local cache, StudioFS can read it from
that cache directly. This keeps warm local reads fast and avoids unnecessary
crypto work.

If StudioFS needs to fetch from remote object storage, it downloads the
encrypted `SFEC` object. Then it authenticates and decrypts the object using
the expected StudioFS identity for that volume and object key.

If the encrypted bytes were corrupted, tampered with, or copied to the wrong
object key, decryption fails before plaintext reaches the normal object
decoder.

After successful decryption, StudioFS decodes the normal inner object, checks
the payload, and returns bytes to the mounted filesystem.

## What Is Protected

This protection is focused on remote payload storage.

It protects file payload confidentiality when someone can inspect the
object-storage bucket directly. It also detects remote payload tampering and
object swaps before the decrypted bytes reach the normal file read path.

In plain terms, the bucket should not contain readable file payloads.

## What Is Not Protected

This feature does not mean every copy of every byte is encrypted everywhere.

The local cache remains plaintext by design. Local runtime memory, local
metadata, filenames, file sizes, inode metadata, ACLs, xattrs, snapshots, and
PostgreSQL metadata are outside this remote-payload encryption boundary.

The design also does not protect against an attacker who can read both the
encrypted remote payloads and the metadata or key material needed to decrypt
them.

That boundary is intentional. StudioFS chooses transparent remote payload
protection instead of a user-managed secret system.

## How To Think About The Boundary

StudioFS treats the local machine as the fast working area and remote object
storage as the place where payloads must be protected.

Locally, StudioFS keeps the filesystem responsive and compatible with normal
applications. Remotely, StudioFS stores encrypted payload objects and refuses
to decode remote bytes that do not authenticate as the expected object.

The result is meant to be simple for users and strict at the storage boundary:
ordinary file workflows on the mount, encrypted payload objects in the bucket.
