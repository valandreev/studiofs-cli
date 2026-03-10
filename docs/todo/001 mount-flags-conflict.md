# Task: Resolve Mount `--foreground` and `--detach` Flag Conflict

The CLI currently parses both `--foreground` and `--detach` (`-d`) simultaneously in the `mount` command test, but there is ambiguity on the intended behavior when both are provided or when neither is provided.

## Requirements

1. **Define the intended behavior:**
   - If nothing is specified: default to `--foreground`.
   - If `-d` or `--detach` is specified: run as a background daemon.
   - If both are specified simultaneously: The CLI parser should explicitly reject it and return a conflict error.
2. **Update clap parameters:** Use `clap`'s `conflicts_with` macro on the `detach` and `foreground` args in `crates/studiofs/src/main.rs`.
3. **Update documentation:** Ensure `docs/configuration.md` correctly outlines this specific mutual exclusivity.
