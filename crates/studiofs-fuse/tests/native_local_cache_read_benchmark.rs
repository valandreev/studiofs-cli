//! SFS-529 benchmark evidence for native local-cache read strategies.

use std::fs;
use std::io::{Cursor, Seek, Write};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use studiofs_fuse::native::protocol::{FuseLocalCacheReadRange, FuseReply};
use studiofs_fuse::native::transport::{
    ClassicReadWriteTransport, FuseTransport, LocalCacheReplyMetricsSnapshot,
    splice_local_cache_range_to_fd,
};

const DATA_LEN: usize = 16 * 1024 * 1024;
const CHUNK_LEN: usize = 1024 * 1024;
const ITERATIONS: usize = 16;

#[test]
#[ignore = "SFS-529 benchmark; writes JSON evidence under target/"]
fn sfs529_native_local_cache_read_strategy_benchmark() {
    let artifact_dir = benchmark_artifact_dir();
    fs::create_dir_all(&artifact_dir).expect("create benchmark artifact dir");

    let mut cache_file = tempfile::tempfile().expect("create local cache file");
    let data = deterministic_data(DATA_LEN);
    cache_file.write_all(&data).expect("write local cache data");
    cache_file.rewind().expect("rewind local cache file");

    let bytes = run_bytes_baseline(&data);
    let pread = run_pread_transport(cache_file.as_raw_fd());
    let splice = run_splice_helper(cache_file.as_raw_fd());

    let report = format!(
        concat!(
            "{{\n",
            "  \"issue\": \"SFS-529\",\n",
            "  \"artifact\": \"LinuxNativeBridge local-cache read benchmark\",\n",
            "  \"data_len_bytes\": {data_len},\n",
            "  \"chunk_len_bytes\": {chunk_len},\n",
            "  \"iterations\": {iterations},\n",
            "  \"strategies\": [\n",
            "{bytes},\n",
            "{pread},\n",
            "{splice}\n",
            "  ]\n",
            "}}\n"
        ),
        data_len = DATA_LEN,
        chunk_len = CHUNK_LEN,
        iterations = ITERATIONS,
        bytes = bytes.to_json(),
        pread = pread.to_json(),
        splice = splice.to_json()
    );
    fs::write(artifact_dir.join("sfs-529-local-cache-read.json"), report)
        .expect("write benchmark report");
}

#[derive(Debug)]
struct StrategyMeasurement {
    name: &'static str,
    throughput_mib_s: f64,
    p95_latency_us: u128,
    p99_latency_us: u128,
    copy_count: u64,
    local_cache_hits: u64,
    local_cache_misses: u64,
    materialization_wait_us: u64,
    read_amplification_bytes: u64,
    io_uring_used: bool,
    io_uring_fallback: &'static str,
}

impl StrategyMeasurement {
    fn to_json(&self) -> String {
        format!(
            concat!(
                "    {{",
                "\"name\":\"{}\",",
                "\"throughput_mib_s\":{:.2},",
                "\"cpu\":\"not captured by harness; use perf stat command from docs\",",
                "\"selected_reply_strategy\":\"{}\",",
                "\"p95_latency_us\":{},",
                "\"p99_latency_us\":{},",
                "\"copy_count\":{},",
                "\"local_cache_hits\":{},",
                "\"local_cache_misses\":{},",
                "\"local_cache_reply_bytes\":{},",
                "\"fallback_reasons\":[],",
                "\"materialization_wait_us\":{},",
                "\"read_amplification_bytes\":{},",
                "\"io_uring_used\":{},",
                "\"io_uring_fallback\":\"{}\"",
                "}}"
            ),
            self.name,
            self.throughput_mib_s,
            self.selected_reply_strategy(),
            self.p95_latency_us,
            self.p99_latency_us,
            self.copy_count,
            self.local_cache_hits,
            self.local_cache_misses,
            self.local_cache_reply_bytes(),
            self.materialization_wait_us,
            self.read_amplification_bytes,
            self.io_uring_used,
            self.io_uring_fallback
        )
    }

    fn selected_reply_strategy(&self) -> &'static str {
        if self.name.starts_with("splice(") {
            "splice"
        } else if self.name.starts_with("pread ") {
            "pread"
        } else {
            "bytes"
        }
    }

    fn local_cache_reply_bytes(&self) -> u64 {
        self.local_cache_hits.saturating_mul(CHUNK_LEN as u64)
    }
}

#[test]
fn strategy_measurement_json_preserves_sfs535_local_cache_reply_evidence() {
    let measurement = StrategyMeasurement {
        name: "splice(file_fd -> pipe -> fuse_fd)",
        throughput_mib_s: 1234.5,
        p95_latency_us: 95,
        p99_latency_us: 99,
        copy_count: 0,
        local_cache_hits: 16,
        local_cache_misses: 0,
        materialization_wait_us: 0,
        read_amplification_bytes: 0,
        io_uring_used: false,
        io_uring_fallback: "classic transport selected",
    };

    let json = measurement.to_json();

    assert!(json.contains("\"selected_reply_strategy\":\"splice\""));
    assert!(json.contains("\"local_cache_reply_bytes\":16777216"));
    assert!(json.contains("\"fallback_reasons\":[]"));
    assert!(json.contains("\"cpu\":\"not captured by harness; use perf stat command from docs\""));
}

fn run_bytes_baseline(data: &[u8]) -> StrategyMeasurement {
    let mut latencies = Vec::with_capacity(ITERATIONS);
    let started = Instant::now();
    let mut sink = Vec::with_capacity(CHUNK_LEN * ITERATIONS);
    for _ in 0..ITERATIONS {
        let op_started = Instant::now();
        sink.extend_from_slice(&data[..CHUNK_LEN]);
        latencies.push(op_started.elapsed());
    }
    measurement("bytes baseline", started.elapsed(), latencies, 1, 0, 0)
}

fn run_pread_transport(fd: i32) -> StrategyMeasurement {
    let mut latencies = Vec::with_capacity(ITERATIONS);
    let started = Instant::now();
    let mut transport = ClassicReadWriteTransport::new(Cursor::new(Vec::new()));
    for iteration in 0..ITERATIONS {
        let op_started = Instant::now();
        let reply = FuseReply::local_cache_range_data(
            iteration as u64 + 1,
            FuseLocalCacheReadRange::borrowed(
                fd,
                ((iteration * CHUNK_LEN) % DATA_LEN) as u64,
                CHUNK_LEN as u32,
                1,
            ),
        )
        .expect("build pread local cache reply");
        transport
            .send_reply(reply)
            .expect("send pread local cache reply");
        latencies.push(op_started.elapsed());
    }
    let metrics = transport.local_cache_reply_metrics_snapshot();
    measurement_from_transport(
        "pread local-cache range",
        started.elapsed(),
        latencies,
        metrics,
    )
}

fn run_splice_helper(fd: i32) -> StrategyMeasurement {
    let mut latencies = Vec::with_capacity(ITERATIONS);
    let started = Instant::now();
    let output = tempfile::tempfile().expect("create splice output");
    for iteration in 0..ITERATIONS {
        let op_started = Instant::now();
        splice_local_cache_range_to_fd(
            FuseLocalCacheReadRange::borrowed(
                fd,
                ((iteration * CHUNK_LEN) % DATA_LEN) as u64,
                CHUNK_LEN as u32,
                1,
            ),
            output.as_raw_fd(),
        )
        .expect("splice local cache range");
        latencies.push(op_started.elapsed());
    }
    measurement(
        "splice(file_fd -> pipe -> fuse_fd)",
        started.elapsed(),
        latencies,
        0,
        ITERATIONS as u64,
        0,
    )
}

fn measurement(
    name: &'static str,
    elapsed: Duration,
    mut latencies: Vec<Duration>,
    copy_count: u64,
    local_cache_hits: u64,
    local_cache_misses: u64,
) -> StrategyMeasurement {
    latencies.sort_unstable();
    let total_mib = (CHUNK_LEN * ITERATIONS) as f64 / (1024.0 * 1024.0);
    StrategyMeasurement {
        name,
        throughput_mib_s: total_mib / elapsed.as_secs_f64(),
        p95_latency_us: percentile(&latencies, 95),
        p99_latency_us: percentile(&latencies, 99),
        copy_count,
        local_cache_hits,
        local_cache_misses,
        materialization_wait_us: 0,
        read_amplification_bytes: 0,
        io_uring_used: false,
        io_uring_fallback: "not implemented for SFS-529 local-file benchmark yet",
    }
}

fn measurement_from_transport(
    name: &'static str,
    elapsed: Duration,
    latencies: Vec<Duration>,
    metrics: LocalCacheReplyMetricsSnapshot,
) -> StrategyMeasurement {
    let mut measurement = measurement(
        name,
        elapsed,
        latencies,
        metrics.pread_local_cache_replies,
        metrics.pread_local_cache_replies + metrics.splice_replies,
        0,
    );
    measurement.read_amplification_bytes = metrics
        .payload_bytes
        .saturating_sub((CHUNK_LEN * ITERATIONS) as u64);
    measurement
}

fn percentile(latencies: &[Duration], percentile: usize) -> u128 {
    let index = ((latencies.len().saturating_sub(1)) * percentile) / 100;
    latencies[index].as_micros()
}

fn deterministic_data(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

fn benchmark_artifact_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/sfs-529-local-cache-read-benchmark")
}
