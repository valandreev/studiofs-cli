# SFS-301 Mounted IO Parity Validation

Date: 2026-04-27
Host: Windows 11, same machine for StudioFS and JuiceFS probes
StudioFS volume: `sfs301`, mounted at `Z:`
StudioFS cache root: `D:\cache\sfs301`
JuiceFS reference mount: `W:` (`\\juicefs\sqlite`)
Reference source checkout: `E:\dev\studiofs-project\juicefs`

## Summary

SFS-301 is not complete against the parity gate.

The clean 8 GiB / 4 MiB mounted sequential probe shows StudioFS write
throughput at 90.3% of the comparable JuiceFS result, which meets the 90% write
gate. The same probe shows StudioFS read throughput at 82.0% of JuiceFS and
higher CPU per phase, so the read/CPU parity gate needs follow-up work before
the issue can be closed.

The StudioFS-native `test-cache` report produced valid during-upload and
post-upload read phases after increasing the upload-idle wait. It also exposed a
high peak RSS sample during the 8 GiB writeback run, so memory behavior needs a
follow-up gate before completion.

## Raw Artifacts

- `D:\reports\sfs-301-aja-studiofs-z-8g-4m-final.json`
- `D:\reports\sfs-301-aja-juicefs-w-8g-4m-final.json`
- `D:\reports\sfs-301-studiofs-test-cache-8g-4m-final.json`
- Earlier exploratory smoke: `D:\reports\sfs-301-studiofs-test-smoke.json`

## Comparable 8 GiB / 4 MiB Sequential Probe

The probe wrote and read one 8 GiB file using 4 MiB IO buffers on each mounted
filesystem. CPU and RSS were sampled from WMI process counters.

| Metric | StudioFS `Z:` | JuiceFS `W:` | StudioFS / JuiceFS |
|---|---:|---:|---:|
| Sequential write | 477.65 MiB/s | 528.85 MiB/s | 90.3% |
| Sequential read | 394.52 MiB/s | 480.94 MiB/s | 82.0% |
| Write CPU, one-core basis | 198.1% | 114.7% | 172.7% |
| Read CPU, one-core basis | 120.9% | 70.1% | 172.5% |
| Write sampled peak RSS | 2.32 GiB | 2.50 GiB | 92.8% |
| Read sampled peak RSS | 1.64 GiB | 2.50 GiB | 65.6% |

StudioFS upload queue was idle before the run, then had 1,521 queued upload
files after write and 901 after read. The queue drained to zero after the run.

## StudioFS Native Mounted Report

Command shape:

```powershell
.\target\release\studiofs.exe test-cache Z:\ `
  --block-size 4MB `
  --read-size 4MB `
  --sizes 8GB `
  --concurrency 1 `
  --io-depth 32 `
  --warmup-runs 0 `
  --access-pattern sequential `
  --during-upload-timeout-ms 240000 `
  --json-output D:\reports\sfs-301-studiofs-test-cache-8g-4m-final.json
```

Report `eb5493ad-b02f-4785-bcac-ec2a8a0a27e6`:

| Metric | Result |
|---|---:|
| Write bytes | 8.0 GiB |
| Write throughput | 227.33 MiB/s |
| During-upload read | valid, 415.42 MiB/s |
| Post-upload read | valid, 743.08 MiB/s |
| Post-upload idle wait | 2.61 s |
| Peak RSS | 7.4 GiB |
| Steady RSS | 3.9 GiB |
| Peak dirty bytes | 2.3 GiB |
| Dirty limit | 1.2 GiB |

Note: with `--sizes 8GB --read-size 4MB`, the current `test-cache` read phase
reads one 4 MiB operation from the 8 GiB file. The external sequential probe
above is therefore the full-file 8 GiB read comparison for the AJA-style gate.

## Follow-Up Required

- Read throughput is below the 90% parity gate: 82.0% of JuiceFS in the clean
  8 GiB / 4 MiB probe.
- CPU usage is materially higher than JuiceFS for both write and read phases.
- The StudioFS-native mounted report showed a 7.4 GiB peak RSS sample during
  writeback, above the intended memory budget for this gate.
- The `test-cache --sizes 8GB --read-size 4MB` surface does not perform a full
  8 GiB read, so full-file parity should continue to use an explicit mounted
  sequential probe until the report tool grows a full-file read mode.
