//! Real mounted compression, remote format inspection, and independent cold reads.

use super::*;
use crate::client::ClientControllerConfig;
use opendal::{Operator, services::S3};
use studiofs_data::{
    compression::decode_object,
    crypto::{ObjectCipher, VolumeMasterKey},
    envelope::SfecEnvelope,
    object::ObjectHeader,
};

const NAME: &str = "mounted-compression";
const SUMMARY: &str = "compression.json";
const LIMIT: Duration = Duration::from_secs(120);
const BYTES: usize = 2 * 1024 * 1024;
const RANGE_SIZES: [usize; 4] = [4096, 16384, 65536, 262144];
type Controller = MultiClientController<SystemClientProcessExecutor>;

pub(in crate::scenario) struct MountedCompression;

impl Scenario for MountedCompression {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: NAME,
            description: "adaptive zstd remote formats, independent cold ranges, and policy switch",
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        let config = context
            .client_controller_config
            .clone()
            .ok_or(ScenarioError::MissingComposeInfrastructure)?;
        let mut writer = Controller::new(config.clone(), SystemClientProcessExecutor);
        // Keep peer ownership here so every failure still tears down its mount.
        let mut peer = Controller::new(config.clone(), SystemClientProcessExecutor);
        let result = exercise(context, &config, &mut writer, &mut peer);
        let timed_out = matches!(
            result,
            Err(ScenarioError::MountedIoTimeout { .. } | ScenarioError::MountedIoWorkerLimit { .. })
        );
        let cleanup = if timed_out {
            peer.emergency_kill_all_no_wait("compression timeout")
                .and(writer.emergency_kill_all_no_wait("compression timeout"))
        } else {
            peer.unmount_all().and(writer.unmount_all())
        };
        match (result, cleanup) {
            (result, Ok(())) => result,
            (Ok(()), Err(error)) => Err(error.into()),
            (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
                context: NAME.into(),
                primary: Box::new(primary),
                cleanup: Box::new(cleanup.into()),
            }),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary: serde_json::Value =
            serde_json::from_slice(&fs::read(context.scenario_dir.join(SUMMARY))?)?;
        let rows = |key: &str| summary[key].as_array().map_or(0, Vec::len);
        // A retried upload may leave a second matching object, so require at least one per fixture.
        if rows("cold_ranges") != RANGE_SIZES.len()
            || rows("objects") < 3
            || rows("off_objects") < 1
            || summary["full_peer_verified"] != true
            || summary["off_peer_verified_old_and_new"] != true
        {
            return Err(io::Error::other("incomplete mounted compression evidence").into());
        }
        Ok(vec![format!("artifacts/scenarios/{NAME}/{SUMMARY}")])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

fn fixtures() -> Vec<(String, Vec<u8>)> {
    let usd = b"#usda 1.0\ndef Xform \"Scene\" {\n def Mesh \"Repeated\" { float3[] points = [(0, 1, 2), (3, 4, 5)] }\n}\n";
    let scanline = (0_u32..1024)
        .flat_map(|pixel| ((pixel % 32) as f32 / 32.0).to_le_bytes())
        .collect::<Vec<_>>();
    let mut state = 0x1094_27ab_d319_5921_u64;
    let random: Vec<u8> = (0..BYTES / 8)
        .flat_map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()
        })
        .collect();
    let mut scanlines = scanline
        .iter()
        .copied()
        .cycle()
        .take(BYTES)
        .collect::<Vec<_>>();
    // Preserve repetitive rows while spreading 12.5% entropy across every chunk,
    // so compressed ranges also authenticate SFEC frames beyond frame zero.
    for (index, row) in scanlines.as_chunks_mut::<4096>().0.iter_mut().enumerate() {
        row[3584..].copy_from_slice(&random[index * 512..(index + 1) * 512]);
    }
    vec![
        (
            "scene.usda".into(),
            usd.iter().copied().cycle().take(BYTES).collect(),
        ),
        ("scanlines.float32".into(), scanlines),
        ("entropy.bin".into(), random),
    ]
}

fn policy(controller: &Controller, volume: &str, value: &str) -> Result<(), ScenarioError> {
    controller.run_cli(
        "client-a",
        "compression policy",
        vec!["compression".into(), volume.into(), value.into()],
    )?;
    Ok(())
}

fn write_files(mount: &Path, files: &[(String, Vec<u8>)]) -> Result<(), ScenarioError> {
    let mount = mount.to_owned();
    let files = files.to_vec();
    run_bounded_mounted_io("compression fixture writes", LIMIT, move || {
        for (name, bytes) in files {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(mount.join(name))?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        Ok(())
    })
}

fn wait_safe(
    context: &ScenarioContext,
    writer: &Controller,
    mode: &str,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + LIMIT;
    loop {
        let snapshot = writer.runtime_snapshot("client-a")?;
        save(context, &format!("{mode}-writer-runtime.json"), &snapshot)?;
        let upload = &snapshot["upload"];
        // "safe" alone can precede enqueue; also require drained upload queues.
        if snapshot["sync_safety"]["state"] == "safe"
            && upload["pending_uploads"] == 0
            && upload["active_uploads"] == 0
            && upload["compression_policy"]["mode"] == mode
            && (mode == "off" || upload["compression_policy"]["level"] == 3)
        {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(io::Error::other(format!(
                "{mode}: remote publication never became safe; see writer runtime"
            ))
            .into());
        }
        thread::sleep(Duration::from_millis(200).min(deadline - now));
    }
}

fn save(
    context: &ScenarioContext,
    name: &str,
    value: &serde_json::Value,
) -> Result<(), ScenarioError> {
    fs::write(
        context.scenario_dir.join(name),
        serde_json::to_vec_pretty(value)?,
    )?;
    Ok(())
}

fn exercise(
    context: &ScenarioContext,
    config: &ClientControllerConfig,
    writer: &mut Controller,
    peer: &mut Controller,
) -> Result<(), ScenarioError> {
    writer.bootstrap_volume(VolumeBootstrapOptions {
        client_names: vec!["client-a".into()],
        clean_existing_local_metadata: true,
    })?;
    let a = writer.client_at(0)?.clone();
    let format = studiofs_meta::open_meta_engine(&a.local_metadata_db)
        .and_then(|meta| meta.get_format_info())
        .map_err(io::Error::other)?;
    policy(writer, &a.volume_name, "zstd:3")?;
    writer.mount("client-a")?;
    let files = fixtures();
    write_files(&a.mount_point, &files)?;
    wait_safe(context, writer, "zstd")?;
    let objects = inspect_remote(context, &format, &files, false)?;
    writer.unmount_all()?;

    let mut cold_ranges = Vec::new();
    for size in RANGE_SIZES {
        *peer = fresh_peer(config, &format!("range-{size}"));
        let evidence = verify_peer(context, peer, &files, Some(size), &format!("range-{size}"))?;
        cold_ranges.push(evidence);
        peer.unmount_all()?;
    }
    *peer = fresh_peer(config, "full");
    verify_peer(context, peer, &files, None, "full")?;
    peer.unmount_all()?;

    // Change the replicated policy while unmounted; the next mount starts with off.
    policy(writer, &a.volume_name, "off")?;
    writer.mount("client-a")?;
    let off_body = b"#usda 1.0\n# compression disabled for this new object\n"
        .iter()
        .copied()
        .cycle()
        .take(BYTES)
        .collect();
    let off_files = vec![("after-off.usda".into(), off_body)];
    write_files(&a.mount_point, &off_files)?;
    wait_safe(context, writer, "off")?;
    let off_objects = inspect_remote(context, &format, &off_files, true)?;
    writer.unmount_all()?;
    let all_files = files.into_iter().chain(off_files).collect::<Vec<_>>();
    *peer = fresh_peer(config, "off-full");
    verify_peer(context, peer, &all_files, None, "off-full")?;
    save(
        context,
        SUMMARY,
        &json!({"policy": "zstd:3", "objects": objects,
        "cold_ranges": cold_ranges, "full_peer_verified": true, "off_objects": off_objects,
        "off_peer_verified_old_and_new": true}),
    )?;
    Ok(())
}

fn fresh_peer(config: &ClientControllerConfig, label: &str) -> Controller {
    let mut config = config.clone();
    config.sandbox = config.sandbox.join(format!("compression-{label}"));
    Controller::new(config, SystemClientProcessExecutor)
}

fn verify_peer(
    context: &ScenarioContext,
    peer: &mut Controller,
    files: &[(String, Vec<u8>)],
    range_size: Option<usize>,
    label: &str,
) -> Result<serde_json::Value, ScenarioError> {
    peer.create_clients(["client-b"])?;
    let b = peer.client_at(0)?.clone();
    if b.local_metadata_db.exists()
        || (b.cache_dir.exists() && fs::read_dir(&b.cache_dir)?.next().is_some())
    {
        return Err(io::Error::other(
            "compression peer must start with absent metadata and empty cache",
        )
        .into());
    }
    save(
        context,
        &format!("{label}-cold-start.json"),
        &json!({"metadata_absent": true, "cache_empty": true, "client": b}),
    )?;
    peer.discover("client-b")?;
    peer.mount("client-b")?;
    let before = peer.runtime_snapshot("client-b")?;
    save(context, &format!("{label}-runtime-before.json"), &before)?;
    // No payload reads or tree convergence snapshots precede these reads. Each
    // range size owns a different metadata/cache root and a fresh mount process.
    let mut reads = Vec::new();
    for (index, (name, expected)) in files.iter().enumerate() {
        let per_file_before = peer.runtime_snapshot("client-b")?;
        let mount = b.mount_point.clone();
        let read_name = name.clone();
        let expected = expected.clone();
        let mut evidence = run_bounded_mounted_io(
            "independent compression peer read",
            LIMIT,
            move || {
                let name = read_name;
                let mut file = fs::File::open(mount.join(&name))?;
                let (offset, length) = range_size.map_or((0, expected.len()), |length| {
                    // Unaligned, distant, deterministic ranges cross 128 KiB chunks.
                    let seed = (length as u64)
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(index as u64 + 1094);
                    let chunk = 1 + ((seed >> 32) as usize % 12);
                    let offset = chunk * 128 * 1024 - 137;
                    (offset, length)
                });
                file.seek(SeekFrom::Start(offset as u64))?;
                let mut actual = vec![0; length];
                file.read_exact(&mut actual)?;
                if actual != expected[offset..offset + length] {
                    return Err(io::Error::other(format!(
                        "{name}: byte mismatch at {offset}, length {length}"
                    )));
                }
                if range_size.is_none() && file.metadata()?.len() != expected.len() as u64 {
                    return Err(io::Error::other(format!(
                        "{name}: incorrect full file size"
                    )));
                }
                Ok(
                    json!({"file": name, "offset": offset, "length": length, "sha256": bytes_sha256(&actual)}),
                )
            },
        )?;
        let per_file_after = peer.runtime_snapshot("client-b")?;
        let counters = ColdReadCounters {
            mount_gets: metric(&before, "gets")?,
            gets_before: metric(&per_file_before, "gets")?,
            gets_after: metric(&per_file_after, "gets")?,
            mount_decode: metric(&before, "decode")?,
            decode_before: metric(&per_file_before, "decode")?,
            decode_after: metric(&per_file_after, "decode")?,
        };
        let (gets_before, gets_after) = (counters.gets_before, counters.gets_after);
        let (decode_before, decode_after) = (counters.decode_before, counters.decode_after);
        let puts_before = metric(&per_file_before, "puts")?;
        let puts_after = metric(&per_file_after, "puts")?;
        let compressed = name != "entropy.bin" && name != "after-off.usda";
        let fetched_before_read = cold_read_fetch_source(index, compressed, &counters)
            .map_err(|reason| io::Error::other(format!("{name}: {reason}")))?;
        evidence["fetched_before_read"] = json!(fetched_before_read);
        evidence["s3_gets_before"] = json!(gets_before);
        evidence["s3_gets_after"] = json!(gets_after);
        evidence["decompression_nanos_before"] = json!(decode_before);
        evidence["decompression_nanos_after"] = json!(decode_after);
        evidence["complete_cache_puts_before"] = json!(puts_before);
        evidence["complete_cache_puts_after"] = json!(puts_after);
        reads.push(evidence);
        save(context, &format!("{label}-reads.json"), &json!(reads))?;
    }
    let after = peer.runtime_snapshot("client-b")?;
    save(context, &format!("{label}-runtime-after.json"), &after)?;
    let (gets_before, gets_after) = (metric(&before, "gets")?, metric(&after, "gets")?);
    if gets_after <= gets_before {
        return Err(io::Error::other("cold peer reads did not increase S3 GET count").into());
    }
    let evidence = json!({"fresh_cache": true, "reads": reads, "s3_gets_before": gets_before, "s3_gets_after": gets_after});
    save(context, &format!("{label}-reads.json"), &evidence)?;
    Ok(evidence)
}

/// Fetch/decode counters around one cold peer read, plus their values at mount.
struct ColdReadCounters {
    mount_gets: u64,
    gets_before: u64,
    gets_after: u64,
    mount_decode: u64,
    decode_before: u64,
    decode_after: u64,
}

/// Proves that the `index`-th fixture read by a cold peer came from its remote payload.
///
/// The peer starts with absent metadata and an empty cache, and each fixture is a
/// distinct remote object. Normally the read's own window shows the S3 GET (and, for
/// compressed fixtures, the decode). Another opener on the fresh mount, such as macOS
/// Finder enumerating the new volume, can open a later fixture first; the VFS eager open
/// prefetch then fetches that object during an earlier window and the read is served
/// from the local payload. That fetch is accepted only when the GETs issued since mount
/// already cover every fixture read so far plus this one, and a compressed fixture's
/// decode advanced since mount. Returns whether the fetch preceded the read window.
fn cold_read_fetch_source(
    index: usize,
    compressed: bool,
    counters: &ColdReadCounters,
) -> Result<bool, String> {
    if counters.gets_after > counters.gets_before {
        if compressed && counters.decode_after <= counters.decode_before {
            return Err("cold read fetched but did not decode its remote payload".into());
        }
        return Ok(false);
    }
    let fetched_since_mount = counters.gets_before.saturating_sub(counters.mount_gets);
    let covers_this_read = usize::try_from(fetched_since_mount).is_ok_and(|n| n > index);
    if !covers_this_read || (compressed && counters.decode_before <= counters.mount_decode) {
        return Err(format!(
            "cold read did not fetch/decode its remote payload \
             ({fetched_since_mount} GETs since mount before fixture read {index})"
        ));
    }
    Ok(true)
}

fn metric(snapshot: &serde_json::Value, name: &str) -> io::Result<u64> {
    let value = match name {
        "gets" => &snapshot["write_path"]["fetch"]["s3_get_calls"],
        "puts" => &snapshot["write_path"]["fetch"]["local_cache_put_calls"],
        "decode" => &snapshot["upload"]["decompression_nanos"],
        _ => return Err(io::Error::other(format!("unknown cold-read metric {name}"))),
    };
    value
        .as_u64()
        .ok_or_else(|| io::Error::other(format!("missing cold-read metric {name}")))
}

fn inspect_remote(
    context: &ScenarioContext,
    format: &studiofs_meta::schema::FormatInfo,
    files: &[(String, Vec<u8>)],
    off: bool,
) -> Result<serde_json::Value, ScenarioError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        tokio::time::timeout(LIMIT, async {
            let backend = S3::default().endpoint(&format.endpoint).bucket(&format.bucket)
                .access_key_id(&format.access_key).secret_access_key(&format.secret_key)
                .region(&format.region).disable_config_load().disable_ec2_metadata();
            let operator = Operator::new(backend).map_err(io::Error::other)?.finish();
            let cipher = ObjectCipher::new(&VolumeMasterKey::from_bytes(format.encryption.volume_master_key), &format.uuid)
                .map_err(io::Error::other)?;
            let prefix = if format.data_prefix.is_empty() { format!("{}/objects/", format.name) } else { format.data_prefix.clone() };
            let entries = operator.list_with(&prefix).recursive(true).await.map_err(io::Error::other)?;
            let mut rows = Vec::new();
            let mut found = vec![false; files.len()];
            for entry in entries {
                if !entry.metadata().is_file() { continue; }
                let key = entry.path();
                let sealed = operator.read(key).await.map_err(io::Error::other)?;
                let stored_bytes = sealed.len();
                let envelope = SfecEnvelope::parse(sealed.clone()).map_err(io::Error::other)?;
                let frame_size = envelope.header().frame_size();
                let frame_count = envelope.header().frame_count();
                let stfs = cipher.decrypt_object(sealed, key, 1).map_err(io::Error::other)?;
                let header = ObjectHeader::from_bytes(&stfs).map_err(io::Error::other)?;
                let decoded = decode_object(stfs).await.map_err(io::Error::other)?;
                for (index, (name, expected)) in files.iter().enumerate() {
                    if decoded.as_ref() != expected { continue; }
                    let compressed = !off && name != "entropy.bin";
                    check_remote_format(header.version, frame_size, stored_bytes, expected.len(), compressed)?;
                    if name == "scanlines.float32" && frame_count < 2 {
                        return Err(io::Error::other("scanline fixture must exercise multiple SFEC frames"));
                    }
                    found[index] = true;
                    rows.push(json!({"file": name, "key": key, "envelope": "SFEC", "stfs_version": header.version,
                        "frame_size": frame_size, "frame_count": frame_count, "stored_bytes": stored_bytes, "logical_bytes": expected.len(),
                        "sha256": bytes_sha256(&decoded)}));
                }
            }
            save(context, if off { "off-objects.json" } else { "zstd-objects.json" }, &json!(rows)).map_err(io::Error::other)?;
            if found.iter().any(|found| !found) {
                return Err(io::Error::other(format!("missing complete fixture payload objects: {found:?}; see remote inspection evidence")));
            }
            Ok(json!(rows))
        }).await.map_err(|_| io::Error::other("remote compression inspection timed out"))?
    }).map_err(ScenarioError::from)
}

fn check_remote_format(
    version: u16,
    frame_size: u32,
    stored: usize,
    logical: usize,
    compressed: bool,
) -> io::Result<()> {
    let (expected_version, expected_frame) = if compressed { (3, 65536) } else { (2, 1048576) };
    if version != expected_version
        || frame_size != expected_frame
        || (compressed && stored > logical / 2)
    {
        return Err(io::Error::other(format!(
            "unexpected remote format: STFS v{version}, frame {frame_size}, stored {stored}, logical {logical}, compressed expected {compressed}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_data::compression::compress_fragment;
    use studiofs_meta::trash::CompressionPolicy;

    #[tokio::test]
    async fn compression_fixtures_exercise_v3_and_entropy_probe_fallback() {
        for (name, bytes) in fixtures() {
            let encoded = compress_fragment(bytes.into(), CompressionPolicy::zstd_default())
                .await
                .unwrap();
            assert_eq!(encoded.is_v3(), name != "entropy.bin", "{name}");
            if name != "entropy.bin" {
                assert!(encoded.stored_bytes <= BYTES / 2);
                if name == "scanlines.float32" {
                    assert!(encoded.stored_bytes > 65536);
                }
            } else {
                assert!(encoded.probe_skipped);
            }
        }
    }

    fn counters(mount: (u64, u64), before: (u64, u64), after: (u64, u64)) -> ColdReadCounters {
        ColdReadCounters {
            mount_gets: mount.0,
            gets_before: before.0,
            gets_after: after.0,
            mount_decode: mount.1,
            decode_before: before.1,
            decode_after: after.1,
        }
    }

    #[test]
    fn cold_read_accepts_fetch_in_own_window() {
        let window = counters((0, 0), (1, 10), (2, 20));
        assert_eq!(cold_read_fetch_source(1, true, &window), Ok(false));
        let uncompressed = counters((0, 0), (1, 10), (2, 10));
        assert_eq!(cold_read_fetch_source(1, false, &uncompressed), Ok(false));
        assert!(cold_read_fetch_source(1, true, &uncompressed).is_err());
    }

    #[test]
    fn cold_read_accepts_earlier_prefetch_only_when_gets_cover_it() {
        // Failure evidence from run 20261124T222616Z-ae4a22: after-off.usda (read 3)
        // was prefetched while scanlines.float32 was being read.
        let prefetched = counters((0, 0), (4, 690_626), (4, 690_626));
        assert_eq!(cold_read_fetch_source(3, false, &prefetched), Ok(true));
        // A compressed prefetched fixture also needs decode progress since mount.
        assert_eq!(cold_read_fetch_source(3, true, &prefetched), Ok(true));
        let no_decode = counters((0, 0), (4, 0), (4, 0));
        assert!(cold_read_fetch_source(3, true, &no_decode).is_err());
        // Three GETs cannot cover four distinct cold objects.
        let uncovered = counters((0, 0), (3, 10), (3, 10));
        assert!(cold_read_fetch_source(3, false, &uncovered).is_err());
        // GETs from before this mount's reads never count.
        let stale = counters((4, 0), (4, 10), (4, 10));
        assert!(cold_read_fetch_source(0, false, &stale).is_err());
    }

    #[test]
    fn compression_format_gate_rejects_wrong_version_frame_and_ratio() {
        assert!(check_remote_format(3, 65536, 100, 1000, true).is_ok());
        assert!(check_remote_format(2, 1048576, 1000, 1000, false).is_ok());
        for (version, frame, stored) in [(2, 65536, 100), (3, 1048576, 100), (3, 65536, 501)] {
            assert!(check_remote_format(version, frame, stored, 1000, true).is_err());
        }
        assert!(check_remote_format(3, 65536, 100, 1000, false).is_err());
    }
}
