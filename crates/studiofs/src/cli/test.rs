//! `studiofs test` — validate S3-compatible storage for StudioFS compatibility.
//! Internal S3 compatibility probes used by `studiofs init`.

use anyhow::{Context, anyhow};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use opendal::{
    Operator,
    layers::{RetryLayer, TimeoutLayer},
    options::WriteOptions,
    services::S3,
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

// ── Payload sizes ─────────────────────────────────────────────────────────────

/// 1 KB — typical metadata object (manifest counter, inode counter).
const SMALL_SIZE: usize = 1024;
/// 4 MB — large data object probe for throughput checks.
const LARGE_SIZE: usize = 4 * 1024 * 1024;

// ── Result model ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum Category {
    Connectivity,
    Data,
    Metadata,
}

impl Category {
    fn label(&self) -> &'static str {
        match self {
            Category::Connectivity => "connectivity",
            Category::Data => "data",
            Category::Metadata => "metadata",
        }
    }
}

#[derive(Debug, Clone)]
enum Status {
    Pass,
    Fail(String),
}

impl Status {
    fn is_pass(&self) -> bool {
        matches!(self, Status::Pass)
    }
    fn symbol(&self) -> &'static str {
        match self {
            Status::Pass => "✓",
            Status::Fail(_) => "✗",
        }
    }
}

#[derive(Debug)]
struct TestCase {
    category: Category,
    name: String,
    status: Status,
    duration: Duration,
}

// ── Programmatic compatibility check API (used by `studiofs init`) ───────────

/// Result of a programmatic compatibility pre-flight run.
///
/// Each category reports whether ALL tests in that group passed.
/// `suitable_for_data()` and `suitable_for_metadata()` encode the specific
/// requirements for each use-case.
pub(crate) struct CompatResult {
    pub connectivity_ok: bool,
    pub data_ok: bool,
    pub meta_ok: bool,
    cases: Vec<TestCase>,
}

impl CompatResult {
    /// All tests passed.
    #[allow(dead_code)]
    pub fn all_ok(&self) -> bool {
        self.connectivity_ok && self.data_ok && self.meta_ok
    }

    /// The bucket can serve as the **data** store (block I/O).
    pub fn suitable_for_data(&self) -> bool {
        self.connectivity_ok && self.data_ok
    }

    /// One-line summaries of every failed test, suitable for error output.
    pub fn failure_summary(&self) -> Vec<String> {
        self.cases
            .iter()
            .filter_map(|tc| match &tc.status {
                Status::Fail(msg) => {
                    Some(format!("[{}] {}: {}", tc.category.label(), tc.name, msg))
                }
                Status::Pass => None,
            })
            .collect()
    }

    /// Print the human-readable tabular report.
    pub fn print_report(&self) {
        print_report(&self.cases);
    }
}

/// Run a full compatibility test against an **already-accessible** bucket.
///
/// Temporary objects are written under a random prefix and cleaned up before
/// returning.  The bucket itself is never deleted.
///
/// Returns `Err` only for infrastructure failures (cannot build operator, etc.).
/// Test failures are encoded in the returned `CompatResult`.
pub(crate) fn run_compat_on_bucket(
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
    rt: &tokio::runtime::Runtime,
) -> anyhow::Result<CompatResult> {
    let uid = Uuid::new_v4().to_string();
    let prefix = format!("studiofs-compat-{}", &uid[..8]);

    let op = build_op(endpoint, bucket, access_key, secret_key, region)?;
    let mut cases: Vec<TestCase> = Vec::new();

    rt.block_on(run_connectivity(&op, &prefix, &mut cases));
    rt.block_on(run_data_tests(&op, &prefix, &mut cases));
    rt.block_on(run_metadata_tests(&op, &prefix, &mut cases));

    // Clean up temp objects regardless of result.
    rt.block_on(async {
        remove_prefix(&op, &prefix).await.ok();
    });

    Ok(compat_result_from_cases(cases))
}

fn compat_result_from_cases(cases: Vec<TestCase>) -> CompatResult {
    let all_pass = |cat: &str| -> bool {
        let matching: Vec<_> = cases
            .iter()
            .filter(|tc| tc.category.label() == cat)
            .collect();
        !matching.is_empty() && matching.iter().all(|tc| tc.status.is_pass())
    };
    CompatResult {
        connectivity_ok: all_pass("connectivity"),
        data_ok: all_pass("data"),
        meta_ok: all_pass("metadata"),
        cases,
    }
}

async fn run_connectivity(op: &Operator, prefix: &str, out: &mut Vec<TestCase>) {
    println!("  ── Connectivity ──────────────────────────────────────────────────────────");

    timed_test(
        out,
        Category::Connectivity,
        "bucket reachable (stat non-existent key)",
        None,
        async {
            let probe = format!("{prefix}/__probe__");
            match op.stat(&probe).await {
                Ok(_) => Ok(()),
                Err(e) if is_not_found(&e) => Ok(()), // 404 is fine — we can talk to the bucket
                Err(e) => Err(anyhow!("{e}")),
            }
        },
    )
    .await;
}

// ── Data suite ────────────────────────────────────────────────────────────────

async fn run_data_tests(op: &Operator, prefix: &str, out: &mut Vec<TestCase>) {
    println!("  ── Data ──────────────────────────────────────────────────────────────────");

    let small_key = format!("{prefix}/data/small");
    let large_key = format!("{prefix}/data/large");

    // PUT small
    let small_data = make_payload(SMALL_SIZE, 0xAA);
    let small_hash = sha256(&small_data);
    {
        let k = small_key.clone();
        let d = small_data.clone();
        let op2 = op.clone();
        timed_test(
            out,
            Category::Data,
            "PUT 1 KB (small object)",
            None,
            async move {
                op2.write(&k, d)
                    .await
                    .map(|_| ())
                    .map_err(|e| anyhow!("{e}"))
            },
        )
        .await;
    }

    // GET small + verify
    {
        let k = small_key.clone();
        let expected = small_hash.clone();
        let op2 = op.clone();
        timed_test(
            out,
            Category::Data,
            "GET 1 KB + integrity check (SHA-256)",
            None,
            async move {
                let got = op2.read(&k).await.map_err(|e| anyhow!("{e}"))?.to_bytes();
                let got_hash = sha256(&got);
                if got_hash == expected {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "hash mismatch: expected {expected}, got {got_hash}"
                    ))
                }
            },
        )
        .await;
    }

    // HEAD / stat
    {
        let k = small_key.clone();
        let op2 = op.clone();
        timed_test(
            out,
            Category::Data,
            "HEAD (stat metadata)",
            None,
            async move {
                let meta = op2.stat(&k).await.map_err(|e| anyhow!("{e}"))?;
                if meta.content_length() == SMALL_SIZE as u64 {
                    Ok(())
                } else {
                    Err(anyhow!(
                        "wrong content-length: expected {}, got {}",
                        SMALL_SIZE,
                        meta.content_length()
                    ))
                }
            },
        )
        .await;
    }

    // Range GET
    {
        let k = small_key.clone();
        let op2 = op.clone();
        timed_test(
            out,
            Category::Data,
            "Range GET (byte range 256..512)",
            Some("used for partial block reads in VFS"),
            async move {
                let slice = op2
                    .read_with(&k)
                    .range(256..512)
                    .await
                    .map_err(|e| anyhow!("{e}"))?
                    .to_bytes();
                if slice.len() == 256 {
                    Ok(())
                } else {
                    Err(anyhow!("expected 256 bytes, got {}", slice.len()))
                }
            },
        )
        .await;
    }

    // PUT large (4 MB — full block size)
    {
        let k = large_key.clone();
        let d = make_payload(LARGE_SIZE, 0xBB);
        let op2 = op.clone();
        timed_test(
            out,
            Category::Data,
            "PUT 4 MB (full block write)",
            Some("throughput test — expected: ≥10 MB/s"),
            async move {
                op2.write(&k, d)
                    .await
                    .map(|_| ())
                    .map_err(|e| anyhow!("{e}"))
            },
        )
        .await;
    }

    // GET large + verify
    let large_data = make_payload(LARGE_SIZE, 0xBB);
    let large_hash = sha256(&large_data);
    {
        let k = large_key.clone();
        let expected = large_hash;
        let op2 = op.clone();
        timed_test(
            out,
            Category::Data,
            "GET 4 MB + integrity check (SHA-256)",
            None,
            async move {
                let got = op2.read(&k).await.map_err(|e| anyhow!("{e}"))?.to_bytes();
                let got_hash = sha256(&got);
                if got_hash == expected {
                    Ok(())
                } else {
                    Err(anyhow!("hash mismatch"))
                }
            },
        )
        .await;
    }

    // DELETE small + large
    {
        let sk = small_key.clone();
        let lk = large_key.clone();
        let op2 = op.clone();
        timed_test(out, Category::Data, "DELETE objects", None, async move {
            op2.delete(&sk)
                .await
                .map_err(|e| anyhow!("delete small: {e}"))?;
            op2.delete(&lk)
                .await
                .map_err(|e| anyhow!("delete large: {e}"))?;
            Ok(())
        })
        .await;
    }
}

// ── Metadata suite ────────────────────────────────────────────────────────────

async fn run_metadata_tests(op: &Operator, prefix: &str, out: &mut Vec<TestCase>) {
    println!("  ── Metadata (conditional writes / CAS) ──────────────────────────────────");

    let manifest_key = format!("{prefix}/meta/manifest.json");
    let counter_key = format!("{prefix}/meta/counter");

    // --- Test 1: basic PUT of a metadata object ---
    {
        let k = manifest_key.clone();
        let op2 = op.clone();
        timed_test(
            out,
            Category::Metadata,
            "PUT metadata object",
            Some("basic write — manifest, counter, etc."),
            async move {
                let payload = Bytes::from(b"{\"version\":1,\"seq\":0}\n".to_vec());
                op2.write(&k, payload)
                    .await
                    .map(|_| ())
                    .map_err(|e| anyhow!("{e}"))
            },
        )
        .await;
    }

    // --- Test 2: GET + ETag availability ---
    let etag_for_cas: std::sync::Arc<tokio::sync::Mutex<Option<String>>> =
        std::sync::Arc::new(tokio::sync::Mutex::new(None));
    {
        let k = manifest_key.clone();
        let op2 = op.clone();
        let etag_store = etag_for_cas.clone();
        timed_test(
            out,
            Category::Metadata,
            "GET + ETag (backend returns ETag)",
            Some("required for CAS/conditional writes"),
            async move {
                let meta = op2.stat(&k).await.map_err(|e| anyhow!("{e}"))?;
                match meta.etag() {
                    Some(etag) if !etag.is_empty() => {
                        *etag_store.lock().await = Some(etag.to_string());
                        Ok(())
                    }
                    Some(_) | None => Err(anyhow!(
                        "ETag not returned — backend does not support ETags."
                    )),
                }
            },
        )
        .await;
    }

    // --- Test 3: conditional PUT with correct If-Match (CAS update) ---
    let etag_after_cas: std::sync::Arc<tokio::sync::Mutex<Option<String>>> =
        std::sync::Arc::new(tokio::sync::Mutex::new(None));
    {
        let k = manifest_key.clone();
        let op2 = op.clone();
        let etag_in = etag_for_cas.clone();
        let etag_out = etag_after_cas.clone();
        timed_test(
            out,
            Category::Metadata,
            "conditional PUT If-Match: <correct-etag> (CAS update)",
            Some("conditional metadata update safety check"),
            async move {
                let etag = {
                    let g = etag_in.lock().await;
                    g.clone()
                };
                let etag = etag.ok_or_else(|| {
                    anyhow!("skipped — preceding ETag test failed; cannot test CAS update")
                })?;

                let opts = WriteOptions {
                    if_match: Some(etag),
                    ..WriteOptions::default()
                };

                let new_payload = Bytes::from(b"{\"version\":1,\"seq\":1}\n".to_vec());
                op2.write_options(&k, new_payload, opts)
                    .await
                    .map(|_| ())
                    .map_err(|e| anyhow!("conditional PUT failed: {e}"))?;

                // Record new ETag for the next test
                let new_meta = op2.stat(&k).await.map_err(|e| anyhow!("{e}"))?;
                *etag_out.lock().await = new_meta.etag().map(|s| s.to_string());

                Ok(())
            },
        )
        .await;
    }

    // --- Test 4: conditional PUT with WRONG If-Match must be REJECTED ---
    {
        let k = manifest_key.clone();
        let op2 = op.clone();
        timed_test(
            out,
            Category::Metadata,
            "conditional PUT If-Match: <wrong-etag> → must reject (412)",
            Some("prevents lost-update races; must return 412 PreconditionFailed"),
            async move {
                let opts = WriteOptions {
                    if_match: Some("wrong-etag-aabbccdd".to_string()),
                    ..WriteOptions::default()
                };

                let payload = Bytes::from(b"{\"version\":1,\"seq\":99}\n".to_vec());
                match op2.write_options(&k, payload, opts).await {
                    Err(e) if is_condition_error(&e) => Ok(()), // correct rejection
                    Err(e) => Err(anyhow!(
                        "unexpected error (expected 412 ConditionNotMatch): {e}"
                    )),
                    Ok(_) => Err(anyhow!(
                        "backend ACCEPTED a write with an invalid ETag — \
                         conditional writes are NOT enforced. \
                         The metadata engine cannot safely share state across clients."
                    )),
                }
            },
        )
        .await;
    }

    // --- Test 5: If-None-Match: * on non-existent key must SUCCEED (first-writer-wins) ---
    {
        let k = counter_key.clone();
        let op2 = op.clone();
        timed_test(
            out,
            Category::Metadata,
            "conditional PUT If-None-Match: * on new key → must succeed",
            Some("used when writing the inode/block counter for the first time"),
            async move {
                let opts = WriteOptions {
                    if_not_exists: true,
                    ..WriteOptions::default()
                };

                let payload = Bytes::from(b"1000\n".to_vec());
                op2.write_options(&k, payload, opts)
                    .await
                    .map(|_| ())
                    .map_err(|e| anyhow!("write_options if_not_exists on new key failed: {e}"))
            },
        )
        .await;
    }

    // --- Test 6: If-None-Match: * on EXISTING key must be REJECTED ---
    {
        let k = counter_key.clone();
        let op2 = op.clone();
        timed_test(
            out,
            Category::Metadata,
            "conditional PUT If-None-Match: * on existing key → must reject (412)",
            Some("first-writer-wins gate; must prevent double-initialisation"),
            async move {
                let opts = WriteOptions {
                    if_not_exists: true,
                    ..WriteOptions::default()
                };

                let payload = Bytes::from(b"9999\n".to_vec());
                match op2.write_options(&k, payload, opts).await {
                    Err(e) if is_condition_error(&e) => Ok(()), // correct rejection
                    Err(e) => Err(anyhow!("unexpected error (expected 412): {e}")),
                    Ok(_) => Err(anyhow!(
                        "backend ACCEPTED if_not_exists on an existing key — \
                         the first-writer-wins guarantee is broken."
                    )),
                }
            },
        )
        .await;
    }
}

// ── Report ────────────────────────────────────────────────────────────────────

fn print_report(cases: &[TestCase]) {
    println!();
    println!("  ── Report ────────────────────────────────────────────────────────────────");
    println!(
        "  {:<14} {:<3} {:<45} {:>8}",
        "Category", "", "Test", "Duration"
    );
    println!("  {}", "─".repeat(74));

    let mut prev_cat: Option<&Category> = None;
    for tc in cases {
        if prev_cat.map(|c| c != &tc.category).unwrap_or(true) {
            prev_cat = Some(&tc.category);
        }
        let status_str = match &tc.status {
            Status::Pass => "PASS".to_string(),
            Status::Fail(_) => "FAIL".to_string(),
        };
        let ms = tc.duration.as_millis();
        println!(
            "  {:14} {} {:<45} {:>6}ms  {}",
            tc.category.label(),
            tc.status.symbol(),
            tc.name,
            ms,
            status_str,
        );
        if let Status::Fail(msg) = &tc.status {
            let trimmed = if msg.len() > 120 { &msg[..120] } else { msg };
            println!("  {:14}   └─ {}", "", trimmed);
        }
    }

    // Verdict by category
    let mut by_cat: HashMap<String, (usize, usize)> = HashMap::new(); // (pass, total)
    for tc in cases {
        let e = by_cat.entry(tc.category.label().to_string()).or_default();
        e.1 += 1;
        if tc.status.is_pass() {
            e.0 += 1;
        }
    }

    let connectivity_ok = by_cat
        .get("connectivity")
        .map(|(p, t)| p == t)
        .unwrap_or(false);
    let data_ok = by_cat.get("data").map(|(p, t)| p == t).unwrap_or(false);
    let meta_ok = by_cat.get("metadata").map(|(p, t)| p == t).unwrap_or(false);

    println!();
    println!("  ── Verdict ───────────────────────────────────────────────────────────────");
    println!(
        "  Connectivity       : {}",
        verdict_str(connectivity_ok, "reachable", "cannot connect")
    );
    println!(
        "  Data (block I/O)   : {}",
        verdict_str(
            data_ok,
            "suitable for file data",
            "NOT suitable for file data — check failures above"
        )
    );
    println!(
        "  Metadata (CAS)     : {}",
        verdict_str(
            meta_ok,
            "suitable for metadata",
            "NOT suitable for metadata — conditional writes broken or unsupported"
        )
    );
    println!();

    let overall = connectivity_ok && data_ok && meta_ok;
    if overall {
        println!("  OVERALL VERDICT: ✓  This backend is suitable for StudioFS.");
    } else {
        println!("  OVERALL VERDICT: ✗  This backend is NOT fully compatible with StudioFS.");
        if !meta_ok {
            println!();
            println!("  IMPORTANT: Metadata writes rely on conditional");
            println!("  updates (If-Match / If-None-Match) for safe concurrent access. If your");
            println!("  backend does not support them, consider using a separate metadata");
            println!("  backend that does (e.g. MinIO ≥ RELEASE.2024-*, RustFS, AWS S3).");
        }
    }
    println!();
}

fn verdict_str(ok: bool, pass_msg: &'static str, fail_msg: &'static str) -> String {
    if ok {
        format!("✓  {pass_msg}")
    } else {
        format!("✗  {fail_msg}")
    }
}

// ── Internals ─────────────────────────────────────────────────────────────────

async fn timed_test<F>(
    out: &mut Vec<TestCase>,
    category: Category,
    name: &'static str,
    note: Option<&'static str>,
    fut: F,
) where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    let start = Instant::now();
    let status = match tokio::time::timeout(Duration::from_secs(30), fut).await {
        Ok(Ok(())) => Status::Pass,
        Ok(Err(e)) => Status::Fail(e.to_string()),
        Err(_) => Status::Fail("timeout (30 s)".to_string()),
    };
    let duration = start.elapsed();

    let note_str = note.map(|n| format!(" # {n}")).unwrap_or_default();
    println!(
        "    {} {:<50} {:>6}ms{}",
        status.symbol(),
        name,
        duration.as_millis(),
        note_str,
    );

    out.push(TestCase {
        category,
        name: name.to_string(),
        status,
        duration,
    });
}

fn make_payload(size: usize, byte: u8) -> Bytes {
    Bytes::from(vec![byte; size])
}

fn sha256(data: &Bytes) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn is_not_found(e: &opendal::Error) -> bool {
    e.kind() == opendal::ErrorKind::NotFound
}

fn is_condition_error(e: &opendal::Error) -> bool {
    let s = e.to_string();
    s.contains("ConditionNotMatch")
        || s.contains("412")
        || s.contains("PreconditionFailed")
        || s.contains("condition")
        || s.contains("Condition")
        || s.contains("If-Match")
        || s.contains("if_not_exists")
        || e.kind() == opendal::ErrorKind::ConditionNotMatch
}

fn build_op(
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> anyhow::Result<Operator> {
    let mut builder = S3::default()
        .endpoint(endpoint)
        .bucket(bucket)
        .access_key_id(access_key)
        .secret_access_key(secret_key)
        .disable_config_load()
        .disable_ec2_metadata();

    builder = if region.is_empty() {
        builder.region("us-east-1")
    } else {
        builder.region(region)
    };

    let op = Operator::new(builder)?
        .layer(
            TimeoutLayer::new()
                .with_timeout(Duration::from_secs(15))
                .with_io_timeout(Duration::from_secs(30)),
        )
        .layer(RetryLayer::new().with_max_times(2))
        .finish();

    Ok(op)
}

async fn remove_prefix(op: &Operator, prefix: &str) -> anyhow::Result<usize> {
    let path = format!("{prefix}/");
    let entries: Vec<_> = op
        .list_with(&path)
        .recursive(true)
        .await
        .context("list failed")?;

    let mut count = 0usize;
    for entry in &entries {
        if entry.metadata().is_file() {
            op.delete(entry.path()).await.ok();
            count += 1;
        }
    }
    Ok(count)
}

// ── AWS SigV4 helper for bucket create/delete ─────────────────────────────────
//
// Only two operations need signing at the bucket level that opendal does not
// expose: CreateBucket (PUT /{bucket}) and DeleteBucket (DELETE /{bucket}).

fn hmac_sha256_raw(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can accept any key length");
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex_str(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// Format a UNIX timestamp as `YYYYMMDDTHHmmssZ` (AWS SigV4 datetime format).
fn fmt_datetime_now() -> (String, String) {
    use chrono::{DateTime, Utc};
    let dt: DateTime<Utc> = Utc::now();
    let datetime = dt.format("%Y%m%dT%H%M%SZ").to_string();
    let date = dt.format("%Y%m%d").to_string();
    (datetime, date)
}

#[allow(clippy::too_many_arguments)]
fn build_signed_headers(
    method: &str,
    endpoint_url: &str,
    path: &str,  // e.g. "/mybucket"
    query: &str, // empty for create/delete
    body: &[u8],
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let url = reqwest::Url::parse(endpoint_url).context("invalid endpoint URL")?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("endpoint URL has no host"))?;
    let host_with_port = match url.port() {
        Some(p) => format!("{host}:{p}"),
        None => host.to_string(),
    };

    let (datetime, date) = fmt_datetime_now();
    let payload_hash = sha256_hex_str(body);
    let content_len = body.len().to_string();

    // Headers must be in lowercase, sorted alphabetically
    let content_type = if body.is_empty() {
        "".to_string()
    } else {
        "application/xml".to_string()
    };

    let mut canon_hdrs: Vec<(String, String)> = vec![
        ("host".to_string(), host_with_port.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), datetime.clone()),
    ];
    if !content_type.is_empty() {
        canon_hdrs.push(("content-type".to_string(), content_type.clone()));
    }
    canon_hdrs.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers_str: String = canon_hdrs
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();
    let signed_headers_str: String = canon_hdrs
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{method}\n{path}\n{query}\n{canonical_headers_str}\n{signed_headers_str}\n{payload_hash}"
    );
    let cr_hash = sha256_hex_str(canonical_request.as_bytes());

    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{cr_hash}");

    let k_date = hmac_sha256_raw(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256_raw(&k_date, region.as_bytes());
    let k_service = hmac_sha256_raw(&k_region, b"s3");
    let k_signing = hmac_sha256_raw(&k_service, b"aws4_request");
    let signature = hex::encode(hmac_sha256_raw(&k_signing, string_to_sign.as_bytes()));

    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope},\
         SignedHeaders={signed_headers_str},Signature={signature}"
    );

    // Build the final header list for the reqwest call (original casing)
    let mut out = vec![
        ("Host".to_string(), host_with_port),
        ("X-Amz-Date".to_string(), datetime),
        ("X-Amz-Content-Sha256".to_string(), payload_hash),
        ("Authorization".to_string(), auth),
        ("Content-Length".to_string(), content_len),
    ];
    if !content_type.is_empty() {
        out.push(("Content-Type".to_string(), content_type));
    }
    Ok(out)
}

pub(crate) async fn bucket_create(
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> anyhow::Result<()> {
    let path = format!("/{bucket}");

    // For us-east-1 the body must be empty; for other regions we send
    // LocationConstraint XML.
    let body: Vec<u8> = if region == "us-east-1" {
        vec![]
    } else {
        format!(
            "<CreateBucketConfiguration><LocationConstraint>{region}</LocationConstraint></CreateBucketConfiguration>"
        )
        .into_bytes()
    };

    let headers = build_signed_headers(
        "PUT", endpoint, &path, "", &body, access_key, secret_key, region,
    )?;

    let url = format!("{endpoint}{path}");
    let client = reqwest::Client::new();
    let mut rb = client.put(&url).body(body.clone());
    for (k, v) in &headers {
        rb = rb.header(k.as_str(), v.as_str());
    }

    let resp = rb.send().await.context("HTTP request failed")?;
    let status = resp.status();
    match status.as_u16() {
        200 | 204 => Ok(()),
        // 409 BucketAlreadyOwnedByYou or BucketAlreadyExists → acceptable
        409 => Ok(()),
        _ => {
            let body_text = resp.text().await.unwrap_or_default();
            Err(anyhow!("HTTP {status}: {body_text}"))
        }
    }
}

async fn bucket_delete(
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> anyhow::Result<()> {
    let path = format!("/{bucket}");
    let headers = build_signed_headers(
        "DELETE", endpoint, &path, "", b"", access_key, secret_key, region,
    )?;

    let url = format!("{endpoint}{path}");
    let client = reqwest::Client::new();
    let mut rb = client.delete(&url);
    for (k, v) in &headers {
        rb = rb.header(k.as_str(), v.as_str());
    }

    let resp = rb.send().await.context("HTTP request failed")?;
    let status = resp.status();
    match status.as_u16() {
        200 | 204 | 404 => Ok(()),
        _ => {
            let body_text = resp.text().await.unwrap_or_default();
            Err(anyhow!("HTTP {status}: {body_text}"))
        }
    }
}

/// Expose the SigV4 bucket deletion for use from init.rs cleanup path.
pub(crate) async fn bucket_delete_call(
    endpoint: &str,
    bucket: &str,
    access_key: &str,
    secret_key: &str,
    region: &str,
) -> anyhow::Result<()> {
    bucket_delete(endpoint, bucket, access_key, secret_key, region).await
}
