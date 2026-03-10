# Task: Test `destroy` Command

The `studiofs destroy` command performs irreversible operations (deleting S3 objects, dropping PostgreSQL metadata databases, and removing local files). There is currently no integration test covering this flow, making it a high risk for accidental bugs.

## Requirements

1. **Safety mechanisms validation:** Write an integration test to ensure `destroy` fails cleanly when incorrect permissions or invalid names are provided.
2. **Local-only destroy test:** Validate that `destroy --local` only removes local cache and redb files without touching remote S3 or PostgreSQL state.
3. **Full destroy test:** Validate that a full `destroy` correctly cascades to remote S3 buckets and PostgreSQL, and verify that the target environment is left clean.
