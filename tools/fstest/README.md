# studiofs-fstest — Multi-Client Filesystem Behavior Test Suite

Cross-platform test utility that records filesystem behavior on shared/network volumes.
Use it to capture a reference profile (e.g. LucidLink, SMB) and later verify StudioFS matches.

## Quick Start

```bash
# Machine A:
python fstest.py /mnt/shared --role A --peers 2

# Machine B (simultaneously):
python fstest.py /mnt/shared --role B --peers 2
```

Results are written to `<mount>/.fstest/results_<role>.json`.

## Requirements

- Python 3.9+
- No external packages (stdlib only)
- Works on Windows, Linux, macOS

## Test Categories

| Category | Tests | Multi-client |
|----------|-------|---|
| Basic CRUD | create, read, write, delete, mkdir, rmdir, rename | No |
| Cross-directory visibility | A writes dir1, B writes dir2, both verify | Yes |
| Same-directory writes | A and B create different files in same dir | Yes |
| Same-file overwrite | A and B write to same file (LWW) | Yes |
| Concurrent create (same name) | A and B create file with identical name | Yes |
| Delete while reading | A reads, B deletes | Yes |
| Directory delete with open file | A opens file, B rm -rf dir | Yes |
| Concurrent rename | A and B rename same file to different targets | Yes |
| Large directory | readdir 1000+ files, concurrent modifications | Yes |
| File visibility timing | Measure delay until file appears on other client | Yes |
| Lock semantics | flock/LockFile behavior across clients | Yes |
| Partial file reads | File visible before all data flushed | Yes |
| Hardlinks | Create, verify nlink across clients | Yes |
| Symlinks | Create, resolve across clients | Yes |
| Xattrs | Set, get, list across clients | Yes |

## Output

JSON report with per-test results:
```json
{
  "filesystem": "LucidLink",
  "role": "A",
  "peers": 2,
  "platform": "Windows",
  "tests": {
    "cross_dir_visibility": {
      "passed": true,
      "visibility_delay_ms": 1200,
      "notes": "file appeared after 1.2s"
    }
  }
}
```
