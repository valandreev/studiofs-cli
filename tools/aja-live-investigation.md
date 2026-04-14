# AJA Live Investigation

This helper drives a marker-based StudioFS live investigation for a manual AJA run.

## One-time setup

1. Mount the disk the usual way:

```powershell
./studiofs mount Z:
```

2. Prepare the investigation run:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\aja-live-investigation.ps1 -Action prepare -MountPoint Z:
```

When `prepare` completes, the helper has already:

- attached to the live diagnostics session
- acquired a controller lease
- created an investigation case and run

At that point the script prints the `run_id` and `state file`.

## Manual AJA phase

1. Start AJA System Test against `Z:`.
2. Let the write pass finish and allow the read pass to run.
3. Do not stop AJA as soon as the read phase starts.
4. Once the read phase is clearly active and uploads have had time to overlap with it, mark the read window and start profiling:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\aja-live-investigation.ps1 -Action begin-read -MountPoint Z:
```

5. Keep AJA reading for at least 30 seconds after `begin-read`.
6. Do not unmount yet.

If the AJA run lasts a long time and you want to refresh the controller lease before finishing, run:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\aja-live-investigation.ps1 -Action renew -MountPoint Z:
```

## Finish and read the report

When you are ready to stop AJA:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\aja-live-investigation.ps1 -Action finish -MountPoint Z: -PrintReport
```

Useful follow-up commands:

```powershell
powershell -ExecutionPolicy Bypass -File .\tools\aja-live-investigation.ps1 -Action status -MountPoint Z:
powershell -ExecutionPolicy Bypass -File .\tools\aja-live-investigation.ps1 -Action report -MountPoint Z:
```

## Notes

- The helper stores run state in a temp JSON file so `begin-read` and `finish` can pick up the same `run_id`.
- Investigation metadata persists under `%USERPROFILE%\.studiofs\investigations`.
- Profiling artifacts are written under the mount cache path.
