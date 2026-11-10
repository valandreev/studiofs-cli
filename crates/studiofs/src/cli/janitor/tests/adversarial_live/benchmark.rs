//! SFS-979 live benchmark: the janitor CLI binary against a disposable RustFS bucket
//! and a CPU/memory-limited PostgreSQL reached through a latency-injecting proxy.
//!
//! Knobs are `STUDIOFS_JANITOR_BENCH_*` variables; see
//! `docs/perf/sfs-979-janitor-benchmark.md` for the recorded matrix.

use super::*;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn knob<T: std::str::FromStr>(key: &str, default: T) -> anyhow::Result<T> {
    match std::env::var(key) {
        Ok(value) if !value.trim().is_empty() => value
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("{key} has an invalid value")),
        _ => Ok(default),
    }
}

/// Object `index` is referenced by live metadata unless it falls in the orphan share.
/// The multiplier spreads orphans across the key space instead of clustering them.
fn is_live(index: u64, orphan_percent: u64) -> bool {
    index * 37 % 100 >= orphan_percent
}

/// Every third object uses the legacy key, the rest the fragment key, so both layouts are
/// listed; the modulus is coprime to the orphan pattern so orphans get both layouts too.
fn bench_key(prefix: &str, id: u64, index: u64) -> String {
    if index.is_multiple_of(3) {
        studiofs_data::object::format_object_key(prefix, id)
    } else {
        studiofs_data::object::format_object_fragment_key(prefix, id, 0)
    }
}

/// Loopback TCP proxy that delays each direction by `one_way` and counts open connections.
struct DelayProxy {
    port: u16,
    peak_open: Arc<AtomicU64>,
    accepted: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for DelayProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn delayed_pipe(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    one_way: Duration,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
    let writer = tokio::spawn(async move {
        while let Some((due, bytes)) = rx.recv().await {
            tokio::time::sleep_until(due).await;
            if to.write_all(&bytes).await.is_err() {
                break;
            }
        }
        let _ = to.shutdown().await;
    });
    let mut buffer = vec![0_u8; 64 * 1024];
    while let Ok(read @ 1..) = from.read(&mut buffer).await {
        if tx
            .send((
                tokio::time::Instant::now() + one_way,
                buffer[..read].to_vec(),
            ))
            .is_err()
        {
            break;
        }
    }
    drop(tx);
    let _ = writer.await;
}

async fn start_proxy(upstream_port: u16, one_way: Duration) -> anyhow::Result<DelayProxy> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let open = Arc::new(AtomicU64::new(0));
    let peak_open = Arc::new(AtomicU64::new(0));
    let accepted = Arc::new(AtomicU64::new(0));
    let (task_open, task_peak, task_accepted) = (open, peak_open.clone(), accepted.clone());
    let task = tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let (open, peak, accepted) =
                (task_open.clone(), task_peak.clone(), task_accepted.clone());
            tokio::spawn(async move {
                let Ok(server) = tokio::net::TcpStream::connect(("127.0.0.1", upstream_port)).await
                else {
                    return;
                };
                let _ = (client.set_nodelay(true), server.set_nodelay(true));
                accepted.fetch_add(1, Ordering::Relaxed);
                peak.fetch_max(open.fetch_add(1, Ordering::Relaxed) + 1, Ordering::Relaxed);
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                tokio::join!(
                    delayed_pipe(client_read, server_write, one_way),
                    delayed_pipe(server_read, client_write, one_way),
                );
                open.fetch_sub(1, Ordering::Relaxed);
            });
        }
    });
    Ok(DelayProxy {
        port,
        peak_open,
        accepted,
        task,
    })
}

fn postgres_container(stack: &DisposableStack) -> anyhow::Result<String> {
    let output = Command::new("docker")
        .args([
            "ps",
            "-q",
            "--filter",
            &format!(
                "label=com.docker.compose.project={}",
                stack.compose.project_name()
            ),
            "--filter",
            "label=com.docker.compose.service=postgres",
        ])
        .output()?;
    let id = String::from_utf8(output.stdout)?.trim().to_string();
    anyhow::ensure!(!id.is_empty(), "PostgreSQL container not found");
    Ok(id)
}

/// Peak resource use observed while one CLI run was in flight.
#[derive(Default)]
struct Samples {
    peak_rss: u64,
    peak_pg_cpu_percent: f64,
    peak_pg_memory_mib: f64,
}

fn parse_mib(value: &str) -> Option<f64> {
    let value = value.trim();
    let split = value.find(|c: char| c.is_ascii_alphabetic())?;
    let number: f64 = value[..split].parse().ok()?;
    Some(match &value[split..] {
        "GiB" | "GB" => number * 1024.0,
        "KiB" | "kB" => number / 1024.0,
        "B" => number / (1024.0 * 1024.0),
        _ => number,
    })
}

/// Runs a CLI child to completion while sampling its RSS and the PostgreSQL container.
fn run_measured(
    command: Command,
    timeout: Duration,
    stdout: &Path,
    stderr: &Path,
    pg_container: &str,
) -> anyhow::Result<(Output, Duration, Samples)> {
    let stop = Arc::new(AtomicBool::new(false));
    let stats_stop = stop.clone();
    let container = pg_container.to_string();
    // `docker stats` blocks for about a second per sample, so it runs beside the RSS loop.
    let docker = std::thread::spawn(move || {
        let (mut cpu, mut memory) = (0.0_f64, 0.0_f64);
        while !stats_stop.load(Ordering::Relaxed) {
            let Ok(output) = Command::new("docker")
                .args([
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.CPUPerc}}|{{.MemUsage}}",
                    &container,
                ])
                .output()
            else {
                break;
            };
            let line = String::from_utf8_lossy(&output.stdout);
            if let Some((cpu_text, memory_text)) = line.trim().split_once('|') {
                if let Ok(value) = cpu_text.trim_end_matches('%').parse::<f64>() {
                    cpu = cpu.max(value);
                }
                if let Some(value) = memory_text.split('/').next().and_then(parse_mib) {
                    memory = memory.max(value);
                }
            }
        }
        (cpu, memory)
    });

    let started = std::time::Instant::now();
    let result = run_child_sampled(command, timeout, stdout, stderr);
    let elapsed = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    let (peak_pg_cpu_percent, peak_pg_memory_mib) = docker
        .join()
        .map_err(|_| anyhow::anyhow!("docker stats sampler panicked"))?;
    let (output, peak_rss) = result?;
    Ok((
        output,
        elapsed,
        Samples {
            peak_rss,
            peak_pg_cpu_percent,
            peak_pg_memory_mib,
        },
    ))
}

fn run_child_sampled(
    mut command: Command,
    timeout: Duration,
    stdout: &Path,
    stderr: &Path,
) -> anyhow::Result<(Output, u64)> {
    command.stdout(std::fs::File::create(stdout)?);
    command.stderr(std::fs::File::create(stderr)?);
    let mut child = OwnedCli(command.spawn()?);
    let pid = sysinfo::Pid::from_u32(child.0.id());
    let mut system = sysinfo::System::new();
    let mut peak_rss = 0;
    let started = std::time::Instant::now();
    let status = loop {
        system.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[pid]),
            true,
            sysinfo::ProcessRefreshKind::nothing().with_memory(),
        );
        peak_rss = peak_rss.max(system.process(pid).map_or(0, sysinfo::Process::memory));
        if let Some(status) = child.0.try_wait()? {
            break status;
        }
        anyhow::ensure!(
            started.elapsed() < timeout,
            "janitor CLI exceeded {timeout:?}; artifacts at {}",
            stdout.display()
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    Ok((
        Output {
            status,
            stdout: std::fs::read(stdout)?,
            stderr: std::fs::read(stderr)?,
        },
        peak_rss,
    ))
}

/// Concurrent foreground load: young S3 objects plus PostgreSQL reference inserts, paced
/// at one operation pair per `interval`; returns per-operation latencies in milliseconds.
async fn foreground_load(
    writer: Operator,
    postgres_url: String,
    prefix: String,
    volume_id: i64,
    first_id: u64,
    interval: Duration,
    stop: Arc<AtomicBool>,
) -> anyhow::Result<(Vec<u64>, Vec<u64>, Vec<u64>)> {
    let (client, connection) =
        tokio_postgres::connect(&postgres_url, tokio_postgres::NoTls).await?;
    let connection = tokio::spawn(connection);
    let (mut s3_ms, mut pg_ms, mut ids) = (Vec::new(), Vec::new(), Vec::new());
    let mut ticker = tokio::time::interval(interval);
    let mut id = first_id;
    while !stop.load(Ordering::Relaxed) {
        ticker.tick().await;
        let started = std::time::Instant::now();
        writer
            .write(&bench_key(&prefix, id, id), b"foreground".to_vec())
            .await?;
        s3_ms.push(u64::try_from(started.elapsed().as_millis())?);
        let started = std::time::Instant::now();
        client
            .execute(
                "INSERT INTO snapshot_current_references (vol, ino, object_id) VALUES ($1, $2, $2)",
                &[&volume_id, &i64::try_from(id)?],
            )
            .await?;
        pg_ms.push(u64::try_from(started.elapsed().as_millis())?);
        ids.push(id);
        id += 1;
    }
    connection.abort();
    Ok((s3_ms, pg_ms, ids))
}

fn percentile(values: &[u64], percent: usize) -> Option<u64> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted
        .get((sorted.len() * percent / 100).min(sorted.len().saturating_sub(1)))
        .copied()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "SFS-979 benchmark; requires Docker and a release CLI (see docs/perf/sfs-979-janitor-benchmark.md)"]
async fn live_cli_janitor_benchmark() -> anyhow::Result<()> {
    let objects: u64 = knob("STUDIOFS_JANITOR_BENCH_OBJECTS", 1_000)?;
    let orphan_percent: u64 = knob("STUDIOFS_JANITOR_BENCH_ORPHAN_PERCENT", 50)?;
    let pg_latency_ms: u64 = knob("STUDIOFS_JANITOR_BENCH_PG_LATENCY_MS", 100)?;
    let s3_latency_ms: u64 = knob("STUDIOFS_JANITOR_BENCH_S3_LATENCY_MS", 20)?;
    let pg_cpus: String = knob("STUDIOFS_JANITOR_BENCH_PG_CPUS", "1".to_string())?;
    let pg_memory: String = knob("STUDIOFS_JANITOR_BENCH_PG_MEMORY", "512m".to_string())?;
    let rps: u32 = knob("STUDIOFS_JANITOR_BENCH_RPS", 4)?;
    let max_deletes: u64 = knob("STUDIOFS_JANITOR_BENCH_MAX_DELETES", 500)?;
    let max_runs: u32 = knob("STUDIOFS_JANITOR_BENCH_MAX_RUNS", 20)?;
    let foreground_ms: u64 = knob("STUDIOFS_JANITOR_BENCH_FOREGROUND_MS", 250)?;
    let timeout = Duration::from_secs(knob("STUDIOFS_JANITOR_BENCH_TIMEOUT_SECS", 3_600)?);
    let baseline: Option<PathBuf> = std::env::var_os("STUDIOFS_JANITOR_BENCH_BASELINE_BINARY")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let label: String = knob(
        "STUDIOFS_JANITOR_BENCH_LABEL",
        if baseline.is_some() {
            "baseline"
        } else {
            "candidate"
        }
        .to_string(),
    )?;

    let stack = DisposableStack::start().await?;
    let base = object_id(0)?;
    let prefix = format!("sfsbench/{base:016x}/objects/");
    let pg_port: u16 = stack
        .postgres_url
        .split("port=")
        .nth(1)
        .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL port missing from DSN"))?;
    let s3_port: u16 = stack
        .endpoint
        .rsplit(':')
        .next()
        .and_then(|port| port.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("S3 port missing from endpoint"))?;

    // Seed the bucket directly; seeding is not part of the measured window.
    let seed_started = std::time::Instant::now();
    let writer = stack.writer()?;
    let mut writes = tokio::task::JoinSet::new();
    for index in 0..objects {
        if writes.len() >= 64 {
            writes.join_next().await.expect("pending write")??;
        }
        let (writer, key) = (writer.clone(), bench_key(&prefix, base + index, index));
        writes.spawn(async move {
            writer
                .write(&key, b"janitor benchmark".to_vec())
                .await
                .map(drop)
        });
    }
    while let Some(write) = writes.join_next().await {
        write??;
    }
    let last_key = bench_key(&prefix, base + objects - 1, objects - 1);
    // The container clock can run ahead of the host; future-dated objects are skipped.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while writer
        .stat(&last_key)
        .await?
        .last_modified()
        .map(std::time::SystemTime::from)
        .is_none_or(|modified| modified > std::time::SystemTime::now())
    {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "seeded objects stayed future-dated"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let seed_ms = seed_started.elapsed().as_millis();

    let pg_proxy = start_proxy(pg_port, Duration::from_millis(pg_latency_ms / 2)).await?;
    let s3_proxy = start_proxy(s3_port, Duration::from_millis(s3_latency_ms / 2)).await?;
    let proxied_pg = format!(
        "host=127.0.0.1 port={} user=studiofs password=studiofs dbname=studiofs",
        pg_proxy.port
    );
    let proxied_s3 = format!("http://127.0.0.1:{}", s3_proxy.port);
    let s3 = LiveS3Config {
        endpoint: &proxied_s3,
        bucket: &stack.bucket,
        access_key: ACCESS_KEY,
        secret_key: SECRET_KEY,
        region: REGION,
        prefix: &prefix,
    };
    let client_root = stack.root.join("benchmark");
    let (volume_id, _, _) = create_live_postgres_janitor_fixture(
        &proxied_pg,
        &s3,
        &client_root.join("meta/janitor-live"),
        base,
    )
    .await?;
    let (client, task) = connection(&stack).await?;
    let live_refs = client
        .execute(
            "INSERT INTO snapshot_current_references (vol, ino, object_id) SELECT $1, i + 1, $2 + i FROM generate_series(0::bigint, $3::bigint - 1) AS i WHERE i * 37 % 100 >= $4::bigint",
            &[&volume_id, &i64::try_from(base)?, &i64::try_from(objects)?, &i64::try_from(orphan_percent)?],
        )
        .await?;
    client.execute("ANALYZE", &[]).await?;
    task.abort();

    let pg_container = postgres_container(&stack)?;
    let update = Command::new("docker")
        .args([
            "update",
            "--cpus",
            &pg_cpus,
            "--memory",
            &pg_memory,
            "--memory-swap",
            &pg_memory,
            &pg_container,
        ])
        .output()?;
    anyhow::ensure!(
        update.status.success(),
        "docker update failed: {}",
        String::from_utf8_lossy(&update.stderr)
    );

    let stop_foreground = Arc::new(AtomicBool::new(false));
    let foreground = (foreground_ms > 0).then(|| {
        tokio::spawn(foreground_load(
            stack.writer().expect("writer"),
            stack.postgres_url.clone(),
            prefix.clone(),
            volume_id,
            base + objects + 1_000,
            Duration::from_millis(foreground_ms),
            stop_foreground.clone(),
        ))
    });

    let mut runs = Vec::new();
    let mut complete = false;
    let (mut total_ms, mut peak_rss, mut peak_cpu, mut peak_memory) =
        (0_u128, 0_u64, 0.0_f64, 0.0_f64);
    let mut first_candidate_ms = None;
    for run in 0..max_runs {
        let mut args = vec![
            "janitor".to_string(),
            "janitor-live".to_string(),
            "--skip-newer-minutes".to_string(),
            "0".to_string(),
            "--max-deletes".to_string(),
            max_deletes.to_string(),
        ];
        let command = match &baseline {
            Some(binary) => DisposableStack::command_for(binary, &args, &client_root),
            None => {
                args.extend([
                    "--output".into(),
                    "jsonl".into(),
                    "--s3-requests-per-second".into(),
                    rps.to_string(),
                ]);
                if run == 0 {
                    args.push("--full-scan".into());
                }
                stack.command(&args, &client_root)?
            }
        };
        let stdout = stack.artifact_root.join(format!("bench-{run}.stdout"));
        let stderr = stack.artifact_root.join(format!("bench-{run}.stderr"));
        let (output, elapsed, samples) = tokio::task::block_in_place(|| {
            run_measured(command, timeout, &stdout, &stderr, &pg_container)
        })?;
        total_ms += elapsed.as_millis();
        peak_rss = peak_rss.max(samples.peak_rss);
        peak_cpu = peak_cpu.max(samples.peak_pg_cpu_percent);
        peak_memory = peak_memory.max(samples.peak_pg_memory_mib);
        let cycle = if baseline.is_none() {
            parse_events(&output)?
                .into_iter()
                .find(|event| event["event"] == "cycle")
        } else {
            None
        };
        if first_candidate_ms.is_none() {
            first_candidate_ms = cycle
                .as_ref()
                .and_then(|cycle| cycle["first_candidate_ms"].as_u64())
                .map(|ms| u128::from(ms) + total_ms - elapsed.as_millis());
        }
        runs.push(serde_json::json!({
            "run": run,
            "exit_code": output.status.code(),
            "wall_ms": elapsed.as_millis(),
            "peak_rss_mib": samples.peak_rss / (1 << 20),
            "peak_pg_cpu_percent": samples.peak_pg_cpu_percent,
            "peak_pg_memory_mib": samples.peak_pg_memory_mib,
            "cycle": cycle,
        }));
        eprintln!(
            "janitor benchmark run {run}: exit={:?} wall_ms={}",
            output.status.code(),
            elapsed.as_millis()
        );
        // The baseline binary has no partial/complete contract: one run is its whole sweep.
        if baseline.is_some() || output.status.success() {
            complete = output.status.success();
            break;
        }
        anyhow::ensure!(
            output.status.code() == Some(2),
            "janitor run {run} failed with {:?}; artifacts at {}",
            output.status.code(),
            stack.artifact_root.display()
        );
    }

    stop_foreground.store(true, Ordering::Relaxed);
    let (fg_s3, fg_pg, fg_ids) = match foreground {
        Some(task) => task.await??,
        None => Default::default(),
    };

    // Zero data loss: every referenced object (seeded or foreground) must still be listed.
    let remaining: HashSet<String> = writer
        .list_with(&prefix)
        .recursive(true)
        .await?
        .into_iter()
        .map(|entry| entry.path().to_string())
        .collect();
    let (mut live_missing, mut orphans_remaining, mut orphans) = (0_u64, 0_u64, 0_u64);
    for index in 0..objects {
        let present = remaining.contains(&bench_key(&prefix, base + index, index));
        if is_live(index, orphan_percent) {
            live_missing += u64::from(!present);
        } else {
            orphans += 1;
            orphans_remaining += u64::from(present);
        }
    }
    let foreground_missing = fg_ids
        .iter()
        .filter(|id| !remaining.contains(&bench_key(&prefix, **id, **id)))
        .count();
    let deleted = orphans - orphans_remaining;

    let report = serde_json::json!({
        "label": label,
        "objects": objects,
        "orphan_percent": orphan_percent,
        "orphans": orphans,
        "live_references": live_refs,
        "pg_latency_ms": pg_latency_ms,
        "s3_latency_ms": s3_latency_ms,
        "pg_cpus": pg_cpus,
        "pg_memory": pg_memory,
        "s3_requests_per_second": if baseline.is_some() { None } else { Some(rps) },
        "max_deletes": max_deletes,
        "seed_ms": seed_ms,
        "runs": runs.len(),
        "complete": complete,
        "total_ms": total_ms,
        "first_candidate_ms": first_candidate_ms,
        "deleted": deleted,
        "delete_rate_per_second": deleted as f64 * 1000.0 / total_ms.max(1) as f64,
        "orphans_remaining": orphans_remaining,
        "live_missing": live_missing,
        "foreground_ops": fg_ids.len(),
        "foreground_missing": foreground_missing,
        "foreground_s3_p50_ms": percentile(&fg_s3, 50),
        "foreground_s3_p99_ms": percentile(&fg_s3, 99),
        "foreground_pg_p50_ms": percentile(&fg_pg, 50),
        "foreground_pg_p99_ms": percentile(&fg_pg, 99),
        "foreground_pg_max_ms": fg_pg.iter().max(),
        "peak_rss_mib": peak_rss / (1 << 20),
        "peak_pg_cpu_percent": peak_cpu,
        "peak_pg_memory_mib": peak_memory,
        "peak_pg_connections": pg_proxy.peak_open.load(Ordering::Relaxed),
        "pg_connections_opened": pg_proxy.accepted.load(Ordering::Relaxed),
        "peak_s3_connections": s3_proxy.peak_open.load(Ordering::Relaxed),
        "s3_connections_opened": s3_proxy.accepted.load(Ordering::Relaxed),
        "run_details": runs,
    });
    let dir = studiofs_test_support::workspace_root().join("target/janitor-benchmark");
    std::fs::create_dir_all(&dir)?;
    std::fs::write(
        dir.join(format!(
            "live-{label}-{objects}-o{orphan_percent}-pg{pg_latency_ms}ms-rps{rps}.json"
        )),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("janitor live benchmark: {report}");

    // The pre-redesign baseline is measured, not gated: its losses are the "before" result.
    if baseline.is_none() {
        anyhow::ensure!(
            live_missing == 0 && foreground_missing == 0,
            "referenced objects were deleted"
        );
        anyhow::ensure!(
            complete && orphans_remaining == 0,
            "candidate did not finish the sweep"
        );
    }
    Ok(())
}
