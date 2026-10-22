# 29. Logs, Diagnostics and Asking for Help
> **Work in progress**

“It worked yesterday, but today it hangs” is a perfectly understandable way to start a conversation. Finding the cause takes a few more details: what you were doing, when it happened, and what StudioFS saw at the time. Let’s gather that information without sending your entire working project along with its passwords.

If you suspect changes have not yet reached storage, save your work first and stop making new writes. Do not delete the cache or try repairs at random just to get a clean report: you could lose both data and evidence of the problem. The [previous chapter](28-troubleshooting-when-the-cat-stops-purring.md) covers practical first checks.

## Start with the version and a quick status snapshot

These commands help gather the basics for a report:

```bash
studiofs --version
studiofs --help
studiofs status myvolume --verbose
```

Replace `myvolume` with your volume’s name. `status` takes a **volume name**, rather than a drive letter or mount directory. It shows the metadata binding, local counters, cache state, and available write metrics. If the mount’s cache is in a separate directory, specify the same base directory with `--cache-dir`:

```powershell
studiofs status myvolume --verbose --cache-dir "D:\StudioFS-cache"
```

A missing metric or a warning about stale data is useful information too. It does not prove that the corresponding queue is empty or that everything has synchronized. Save the error text as well if the `status` command itself fails.

Include your OS version and build, and the filesystem driver version: WinFSP on Windows, macFUSE on macOS, or FUSE on Linux. For a problem involving two computers, include both client versions. Also specify the S3 storage type and PostgreSQL version, if known; passwords are not needed for this.

You can open help for an individual command:

```bash
studiofs status --help
studiofs perf --help
studiofs mount --help
```

Use the help for your **installed version**: available options can differ between releases.

## If the problem appears under load

`perf` observes a drive that is already mounted. Unlike `status`, it takes a **mount point**:

```powershell
studiofs perf Z: --interval 1000 --count 10 --json-lines > perf.jsonl
```

On macOS or Linux, replace `Z:` with your path, such as `/mnt/studio`. The interval is in milliseconds; `--count 10` limits collection to ten interval samples. The `perf.jsonl` file will be created in the current directory. Choose a local folder outside the StudioFS drive so writing the report does not add load to the volume being measured.

Start collection in a second terminal and repeat a safe action that triggers the problem. Note when you started the action. `perf` shows counters, rates, and latencies; it does not replace an error log. If the mount is no longer working, the command may be unable to retrieve statistics—include its message in the report.

Without `--count`, observation continues until you stop it. For a short report, setting a limit up front is more convenient.

## Where to find logs

The location depends on how StudioFS was started:

| How StudioFS was started | Where to look for messages |
| --- | --- |
| `mount` in an open terminal | In that terminal; a persistent log file is not created automatically |
| `mount` with `-d` or `--detach` | In the `logs` subdirectory of the client root directory |

The default log directory is `~/.studiofs/logs/` on macOS and Linux. On Windows, it is `.studiofs\logs` in the user’s home directory, usually `%USERPROFILE%\.studiofs\logs`. If `STUDIOFS_CLIENT_ROOT` is set for this client, look in its `logs` subdirectory. This is separate from the cache directory: changing `--cache-dir` does not move logs there.

Filenames follow the pattern `<volume-name>-<UUID>.log.<YYYY-MM-DD>`. Characters in the volume name that are unsuitable for a filename are replaced with `_`. Background process logs rotate daily using UTC; at startup, the process attempts to delete its own files older than 30 days. Save the relevant excerpt promptly. If the event happened near midnight UTC, check both adjacent files.

A few minutes of logs before and after the event are usually enough for a report. Keep timestamps and surrounding messages: the last error line is often a consequence, while the cause appeared earlier.

## When you need more detailed logging

The default log level is `info`. The global `--log-level` option accepts `trace`, `debug`, `info`, `warn`, or `error`. For a short reproduction, you can start a mount with `debug` logging:

```bash
studiofs --log-level debug mount myvolume /mnt/studio
```

On Windows, use your drive letter, such as `Z:`, instead of `/mnt/studio`. Do this after cleanly unmounting the previous mount, when it is safe for your work; [chapter 17](17-unplug-safely-studiofs-umount.md) explains the procedure. Do not restart a failing client just to get more detailed logs before preserving the evidence already available.

The option applies to the process you start. Running `studiofs --log-level debug status myvolume` does not change logging for an existing mount process.

In an open terminal, you can save both output streams to a local file:

```bash
studiofs --log-level debug mount myvolume /mnt/studio > mount-debug.log 2>&1
```

The command occupies the terminal while the mount is running, and messages go to the file. For a background launch, add `-d`: then look for the daily log in the `logs` directory rather than relying on terminal redirection.

Detailed logging produces more data and can affect performance measurements. After the short reproduction you need, return to a normal launch with `info`. Use `trace` for a specific developer request rather than leaving it enabled permanently.

## What to remove before sending

Review a **copy** of every file you plan to send, including JSON, command output, and screenshots. Keep the originals locally. Do not assume the application has reliably hidden every sensitive detail for you.

Remove or replace with consistent placeholders:

- S3 access keys, secret keys, passwords, tokens, authorization headers, and signed URLs;
- full PostgreSQL DSNs and connection strings, even if their passwords are already masked;
- private server addresses, bucket names, user and volume names, identifiers, and local paths if they reveal your infrastructure;
- customer, project, and file names, document contents, and any other data you are not allowed to publish.

For example, label the same computer consistently as `CLIENT_A`, another as `CLIENT_B`, and a private path as `<PROJECT_PATH>`. Preserve the event structure, error codes, timestamps, and order of actions. If you changed names or timestamps, explain that in the description so developers do not end up investigating an artifact of your edits.

Do not attach an entire `.env` file, configuration containing credentials, client directory, or cache “just in case.” If a secret has already appeared in a public issue, deleting the text does not make it secret again: revoke or replace it through the relevant service.

## A dump is not a ready-made support bundle

`studiofs dump` exports a metadata manifest from PostgreSQL. It is not a universal diagnostics archive or a complete backup: it does not contain all file contents from S3, local changes that have not yet been published, or the complete state of every internal table. In particular, it does not replace backups of the snapshot catalog and object reachability accounting state.

At the same time, metadata can reveal project structure, filenames, and other private information. Some state is stored as serialized data: a quick look through the JSON is not enough to declare a dump safe. Do not attach it to a public issue by default. If developers request a dump, agree on the scope and a private transfer method; preferably reproduce the problem on a separate test volume with made-up data.

Likewise, the hidden `studiofs internal diagnostics-*` commands, profiling, and workflows in `docs/diagnostics-workflows.md` are intended for guided investigations with developers. They are not a required step for an ordinary report. You do not need to obtain diagnostics permissions or run internal scenarios on your own to report a bug.

## Writing a useful report

First, look for a similar problem in [StudioFS GitHub Issues](https://github.com/valandreev/studiofs-cli/issues). If an existing issue describes the same behavior, adding your environment and reproduction there is more useful. For a new problem, choose a short title, such as “Windows: reopening a file hangs after a network outage.”

You can start your description with this template. For international discussion, fill it in in English while preserving the exact error text:

```text
Summary:

Environment:
- StudioFS: studiofs --version output from each client involved
- OS and build; filesystem driver and version
- S3 storage type; PostgreSQL version, if known
- Number of clients; network type; local or external SSD for the cache

Prerequisites:
- Mount command with secrets removed; cache size and location
- New or existing volume; approximate size and number of test files
- What changed before the problem appeared

Steps to reproduce:
1. ...
2. ...
3. ...

Expected result:
Actual result and exact error text:
Frequency: every time / sometimes / once; number of attempts
Event time and time zone:

Attachments:
- Redacted status --verbose output
- Log excerpt covering the specified time
- Short perf.jsonl if the problem concerns performance

What you have already checked and the result:
What has been hidden or replaced in the attachments:
```

For a problem between clients, describe their actions separately: “CLIENT_A wrote the file,” “CLIENT_B opened it after ….” For poor performance, include file sizes, sequential or random reads, cache state, and measured time. “Slow” is hard to compare; “a 2 GB file takes 40 seconds to open after the first mount” already gives a reference point.

A reproduction using a small test file without customer materials is particularly valuable. But if repeating the problem puts working data at risk, say so and send what you have already collected safely. A good report does not require losing your work again to make a convincing case.

---

[Contents](README.md) · [Українська](../uk/29-logs-diagnostics-and-asking-for-help.md) · [← Previous](28-troubleshooting-when-the-cat-stops-purring.md) · [Next →](30-glossary-command-cheat-sheet-and-faq.md)

[Linear source](https://linear.app/valandreev/document/29-logs-diagnostics-and-asking-for-help-44b8d69f824f)
