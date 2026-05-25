# Encryption-At-Rest Contract

## Status

This document is the implementation contract for transparent StudioFS
encryption-at-rest of remote payload objects. It defines the required behavior
for the encryption work tracked by SFS-346 and its implementation follow-ups;
it is not a claim that older builds already encrypted remote objects.

The mounted IO contract in
[juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md)
remains the source of truth for foreground writeback behavior. Encryption must
fit that shape instead of changing foreground visibility, flush, fetch,
publication, or peer-convergence semantics.

## Data Flow

StudioFS encryption-at-rest is a remote object-store boundary feature:

1. Foreground writes accept plaintext into writer state, RAM buffers, upload hot
   buffers, slice-writer pages, upload stage files, and the local payload cache.
2. The cached-store upload path builds the existing StudioFS object payload for
   a single remote payload fragment.
3. Before S3-compatible object storage upload, StudioFS encrypts the complete
   StudioFS remote object for that fragment.
4. Object storage stores only ciphertext.
5. On remote fetch, StudioFS decrypts the object before passing plaintext bytes
   to the existing object decoder and checksum validator.
6. VFS, FUSE, WinFSP, writer stack, local cache, fetch scheduler, upload
   scheduler, and file streaming semantics remain transparent to callers.

The encryption unit is the current StudioFS remote payload fragment, not a
whole user file. Current remote storage uses STFS fragment objects under
`<volume>/objects/<ab>/<cd>/<object_id>/<block_index>` where each fragment is
typically up to 4 MiB of raw payload plus the StudioFS object header. Each
remote payload fragment is encrypted independently.

## Plaintext And Ciphertext Boundaries

Plaintext remains inside trusted local runtime boundaries:

- mounted VFS read and write buffers;
- FUSE and WinFSP callback buffers;
- writer dirty state and slice-writer pages;
- upload hot buffers and upload helpers;
- local upload stage files;
- local payload cache;
- object decoder output and checksum validation input.

The local payload cache stays plaintext. StudioFS does not encrypt local cache
files, local metadata files, local upload-stage files, process memory, or bridge
buffers as part of this contract.

Ciphertext begins only at the remote object-store edge. The bytes submitted to
S3-compatible storage for encrypted payload fragments are ciphertext, and
object storage stores only ciphertext.

## Algorithm Contract

XChaCha20-Poly1305 is the only supported algorithm for the initial
implementation. The implementation must not add algorithm negotiation or user
selection.

Out of scope:

- AES-256-GCM;
- ChaCha20-Poly1305 without extended nonce;
- RSA wrapping;
- SM2 or SM4;
- configurable algorithm selection.

## Volume Compatibility

For this implementation contract, there is no legacy plaintext volume mode.
Existing legacy plaintext StudioFS
volumes do not need to remain readable through the encrypted-object
implementation, and implementation work must not add plaintext fallback
complexity for old StudioFS object layouts.

The implementation target is that all current and newly formatted volumes are
encrypted at rest by default. There are no user-facing encryption flags, no
prompts, no passphrase prompts, no environment variables, no KMS configuration,
and no additional runtime status signals required to enable encryption.

The volume key is generated and stored automatically in StudioFS metadata so
encryption remains transparent to users. That storage choice is part of the
security boundary, not a hidden user-controlled secret feature.

## Security Boundary

This contract protects against object storage and bucket compromise. It protects
remote payload object confidentiality and integrity for encrypted payload
fragments.

AEAD associated data must bind the ciphertext to the StudioFS remote-object
identity. At minimum, associated data must cover the volume identity, remote
object key, object ID, fragment index, object-format version, and encryption
format version. This detects ciphertext tampering and object-key swapping: a
ciphertext copied to a different remote object key or fragment identity must
fail authentication before plaintext reaches the object decoder.

The boundary does not include every StudioFS datum. StudioFS explicitly does
not encrypt filenames, does not encrypt file sizes, does not encrypt inode
metadata, does not encrypt ACL metadata, does not encrypt PostgreSQL metadata,
and does not encrypt client process memory.

StudioFS also does not protect against an attacker who can read PostgreSQL or
local metadata where the automatically generated volume key is stored. This
tradeoff is intentional because encryption must be completely transparent to
users and must not introduce passphrases, KMS setup, or operator-managed key
material.

## Non-Goals

This contract does not require:

- encrypting the local payload cache;
- encrypting local metadata, PostgreSQL metadata, filenames, sizes, inode
  attributes, ACLs, xattrs, snapshots, or process memory;
- encrypting whole user files as a single object;
- supporting old plaintext remote object layouts;
- supporting mixed encrypted/plaintext volumes;
- adding user-facing options, prompts, environment variables, KMS
  configuration, or passphrase flows;
- adding AES, RSA, SM2/SM4, or configurable crypto agility;
- changing foreground writeback, flush, fsync, close, publication, fetch, or
  peer-convergence semantics.

## Implementation Guardrails

Encryption belongs at the remote object-store edge in `studiofs-data` and the
upload/fetch paths that already own StudioFS object encode/decode boundaries.
It must not create a second local payload authority or bypass `CachedStore`.

Upload order:

1. Build the STFS remote payload object for one fragment.
2. Encrypt the full object bytes with XChaCha20-Poly1305.
3. PUT ciphertext to object storage.
4. Preserve the existing upload-completion and metadata-publication boundary.

Fetch order:

1. GET ciphertext from object storage.
2. Authenticate and decrypt with XChaCha20-Poly1305 and associated data.
3. Pass plaintext to the existing STFS object decoder.
4. Run the existing checksum validator on decoded plaintext payload bytes.

Authentication failure is a remote payload integrity failure. It must not fall
back to plaintext decode or silently treat ciphertext as legacy object bytes.
