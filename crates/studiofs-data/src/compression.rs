//! Adaptive Zstd chunk compression and entropy probing for STFS v3 objects (ADR 0015).
//!
//! Turns a plaintext fragment (up to [`CLOUD_FRAGMENT_SIZE`]) into either an STFS v3
//! object buffer or a "store raw" decision so the caller writes STFS v2, and decodes
//! STFS v3 objects back into their verified plaintext payload.
//!
//! Compression and decompression execute on the shared blocking crypto admission pool
//! (`CRYPTO_ADMISSION` via `run_crypto_blocking`) rather than on async worker
//! threads.

use crate::object::{
    CLOUD_FRAGMENT_SIZE, CompressionCodec, DEFAULT_V3_CHUNK_LOG2, DEFAULT_V3_CHUNK_SIZE,
    ObjectError, ObjectFlags, ObjectHeader, V3EncodedChunk, V3HeaderMeta, V3SeekTable,
    create_v3_object_buffer,
};
use crate::store::{CRYPTO_ADMISSION, DataError, DataResult, run_crypto_blocking};
use bytes::{Buf, Bytes};
use opendal::Buffer;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use studiofs_meta::trash::CompressionPolicy;
use tokio::sync::Semaphore;
use xxhash_rust::xxh64::xxh64;
use zstd::bulk::{Compressor, Decompressor};
use zstd::zstd_safe::zstd_sys::ZSTD_ErrorCode;

/// Prefix length sampled by the entropy probe (64 KiB, ADR 0015).
pub const PROBE_BYTES: usize = 64 * 1024;

/// Zstd compression level used by the entropy probe (level 1, ADR 0015).
pub const PROBE_LEVEL: i32 = 1;

/// Minimum savings percentage required by both the probe and final object check (10%).
pub const MIN_SAVINGS_PERCENT: u64 = 10;

/// Why a fragment was left uncompressed (STFS v2) instead of encoded as STFS v3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreRawReason {
    /// Volume compression policy is `off`.
    PolicyOff,
    /// The 64 KiB level-1 entropy probe saved less than 10% (or input was empty).
    ProbeSkipped,
    /// Full chunk compression saved less than 10% overall (including seek table).
    FinalThreshold,
}

/// Result of attempting to encode a plaintext fragment as STFS v3.
#[derive(Debug, Clone)]
pub enum EncodedFragment {
    /// Complete STFS v3 object buffer (30-byte header + seek table + chunk payloads).
    V3(Buffer),
    /// Caller should write the original plaintext as an STFS v2 object.
    StoreRaw {
        /// Original plaintext payload returned unchanged.
        payload: Bytes,
        /// Reason compression was not selected.
        reason: StoreRawReason,
    },
}

/// Encoded fragment outcome and byte metrics for upload accounting.
#[derive(Debug, Clone)]
pub struct FragmentCompressionOutput {
    /// Encoded STFS v3 object or raw fallback.
    pub fragment: EncodedFragment,
    /// Logical (uncompressed) payload byte length.
    pub logical_bytes: usize,
    /// Stored payload byte length (`compressed_size` for v3, `logical_bytes` for raw).
    pub stored_bytes: usize,
    /// `true` when the entropy probe skipped compression before compressing chunks.
    pub probe_skipped: bool,
}

impl FragmentCompressionOutput {
    fn raw(payload: Bytes, reason: StoreRawReason) -> Self {
        let len = payload.len();
        Self {
            fragment: EncodedFragment::StoreRaw { payload, reason },
            logical_bytes: len,
            stored_bytes: len,
            probe_skipped: matches!(reason, StoreRawReason::ProbeSkipped),
        }
    }

    /// Returns `true` when the fragment was encoded as STFS v3.
    pub fn is_v3(&self) -> bool {
        matches!(self.fragment, EncodedFragment::V3(_))
    }

    /// Returns the raw fallback reason when the fragment was not encoded as STFS v3.
    pub fn raw_reason(&self) -> Option<StoreRawReason> {
        match &self.fragment {
            EncodedFragment::V3(_) => None,
            EncodedFragment::StoreRaw { reason, .. } => Some(*reason),
        }
    }
}

/// Compress a plaintext fragment into an STFS v3 object or return a "store raw"
/// decision under `policy`.
///
/// Runs on the shared blocking crypto admission pool (`CRYPTO_ADMISSION`) with
/// per-chunk parallelism.
pub async fn compress_fragment(
    payload: Bytes,
    policy: CompressionPolicy,
) -> DataResult<FragmentCompressionOutput> {
    compress_fragment_with_admission(&CRYPTO_ADMISSION, payload, policy).await
}

pub(crate) async fn compress_fragment_with_admission(
    admission: &Arc<Semaphore>,
    payload: Bytes,
    policy: CompressionPolicy,
) -> DataResult<FragmentCompressionOutput> {
    if payload.len() > CLOUD_FRAGMENT_SIZE {
        return Err(ObjectError::ObjectTooLarge(payload.len() as u64).into());
    }
    if !policy.is_enabled() {
        return Ok(FragmentCompressionOutput::raw(
            payload,
            StoreRawReason::PolicyOff,
        ));
    }
    if payload.is_empty() {
        return Ok(FragmentCompressionOutput::raw(
            payload,
            StoreRawReason::ProbeSkipped,
        ));
    }

    let max_workers = payload.len().div_ceil(DEFAULT_V3_CHUNK_SIZE).max(1);
    run_crypto_blocking(admission, max_workers, move |workers| {
        compress_fragment_blocking(payload, policy, workers).map_err(DataError::from)
    })
    .await
}

/// Decode and verify a complete STFS v3 object into its original plaintext payload.
///
/// Validates header, codec, seek table, and total stored length before acquiring
/// blocking admission or allocating the output buffer. Chunk decompression runs in
/// parallel on `CRYPTO_ADMISSION`.
pub async fn decompress_v3_object(data: impl Into<Buffer>) -> DataResult<Bytes> {
    decompress_v3_object_with_checksum(data)
        .await
        .map(|(payload, _)| payload)
}

/// Decode and verify a complete STFS v3 object, returning both the original payload
/// bytes and the verified XXH64 checksum.
pub async fn decompress_v3_object_with_checksum(
    data: impl Into<Buffer>,
) -> DataResult<(Bytes, u64)> {
    decompress_v3_object_with_admission(&CRYPTO_ADMISSION, data).await
}

pub(crate) async fn decompress_v3_object_with_admission(
    admission: &Arc<Semaphore>,
    data: impl Into<Buffer>,
) -> DataResult<(Bytes, u64)> {
    let data: Buffer = data.into();
    let seek_table = parse_complete_v3_seek_table(&data)?;

    if seek_table.original_size() == 0 {
        let computed = xxh64(b"", 0);
        let expected = seek_table.header().checksum;
        if computed != expected {
            return Err(ObjectError::ChecksumMismatch { expected, computed }.into());
        }
        return Ok((Bytes::new(), computed));
    }

    let compressed_chunks = seek_table
        .entries()
        .iter()
        .filter(|entry| !entry.is_raw)
        .count();
    let max_workers = compressed_chunks.max(1);

    run_crypto_blocking(admission, max_workers, move |workers| {
        decompress_v3_parsed(&data, &seek_table, workers).map_err(DataError::from)
    })
    .await
}

/// Decompress one Zstd chunk into a caller-provided buffer of exactly its derived
/// original length.
///
/// Rejects frames that inflate past `dst.len()` without allocating beyond `dst`,
/// as well as frames that produce fewer than `dst.len()` bytes.
pub fn decompress_zstd_chunk(
    compressed: &[u8],
    dst: &mut [u8],
    chunk_index: usize,
) -> Result<(), ObjectError> {
    let mut decompressor = Decompressor::new().map_err(|err| ObjectError::ChunkDecompression {
        chunk_index,
        message: format!("failed to initialize zstd decompressor: {err}"),
    })?;
    decompress_zstd_chunk_with(&mut decompressor, compressed, dst, chunk_index)
}

fn decompress_zstd_chunk_with(
    decompressor: &mut Decompressor<'_>,
    compressed: &[u8],
    dst: &mut [u8],
    chunk_index: usize,
) -> Result<(), ObjectError> {
    let written = decompressor
        .decompress_to_buffer(compressed, dst)
        .map_err(|err| ObjectError::ChunkDecompression {
            chunk_index,
            message: err.to_string(),
        })?;
    if written != dst.len() {
        return Err(ObjectError::ChunkDecompression {
            chunk_index,
            message: format!(
                "decompressed length {written} != derived chunk length {}",
                dst.len()
            ),
        });
    }
    Ok(())
}

/// Synchronous fragment compression using up to `workers` threads.
///
/// Exposed for internal callers and benchmarks; production async paths should use
/// [`compress_fragment`] so work is admitted through `CRYPTO_ADMISSION`.
pub fn compress_fragment_blocking(
    payload: Bytes,
    policy: CompressionPolicy,
    workers: usize,
) -> Result<FragmentCompressionOutput, ObjectError> {
    if payload.len() > CLOUD_FRAGMENT_SIZE {
        return Err(ObjectError::ObjectTooLarge(payload.len() as u64));
    }
    if !policy.is_enabled() {
        return Ok(FragmentCompressionOutput::raw(
            payload,
            StoreRawReason::PolicyOff,
        ));
    }
    if payload.is_empty() {
        return Ok(FragmentCompressionOutput::raw(
            payload,
            StoreRawReason::ProbeSkipped,
        ));
    }

    let probe_len = payload.len().min(PROBE_BYTES);
    if !passes_entropy_probe(&payload[..probe_len])? {
        return Ok(FragmentCompressionOutput::raw(
            payload,
            StoreRawReason::ProbeSkipped,
        ));
    }

    let chunks = compress_chunks_parallel(&payload, i32::from(policy.level()), workers)?;

    let seek_table_len = chunks.len() * std::mem::size_of::<u32>();
    let stored_chunks_bytes: usize = chunks
        .iter()
        .map(|chunk| match chunk {
            V3EncodedChunk::Raw(bytes) | V3EncodedChunk::Compressed(bytes) => bytes.len(),
        })
        .sum();
    let compressed_size = seek_table_len
        .checked_add(stored_chunks_bytes)
        .ok_or_else(|| ObjectError::InvalidV3Format("v3 compressed_size overflow".into()))?;

    if !saves_at_least_min_percent(payload.len(), compressed_size) {
        return Ok(FragmentCompressionOutput::raw(
            payload,
            StoreRawReason::FinalThreshold,
        ));
    }

    let checksum = xxh64(payload.as_ref(), 0);
    let original_size = u32::try_from(payload.len())
        .map_err(|_| ObjectError::ObjectTooLarge(payload.len() as u64))?;
    let buffer = create_v3_object_buffer(
        original_size,
        checksum,
        CompressionCodec::Zstd,
        DEFAULT_V3_CHUNK_LOG2,
        ObjectFlags::empty(),
        chunks,
    )?;

    Ok(FragmentCompressionOutput {
        fragment: EncodedFragment::V3(buffer),
        logical_bytes: payload.len(),
        stored_bytes: compressed_size,
        probe_skipped: false,
    })
}

fn new_chunk_compressor(level: i32) -> Result<Compressor<'static>, ObjectError> {
    let mut compressor = Compressor::new(level)?;
    compressor.include_checksum(false)?;
    Ok(compressor)
}

fn is_dst_size_too_small(err: &io::Error) -> bool {
    err.get_ref()
        .and_then(|inner| inner.downcast_ref::<zstd::Error>())
        .is_some_and(|zerr| zerr.is(ZSTD_ErrorCode::ZSTD_error_dstSize_tooSmall))
}

/// Returns `true` iff `stored` saves at least [`MIN_SAVINGS_PERCENT`] (10%) relative
/// to `logical`.
fn saves_at_least_min_percent(logical: usize, stored: usize) -> bool {
    if logical == 0 {
        return false;
    }
    let lhs = u128::from(stored as u64) * 100;
    let rhs = u128::from(logical as u64) * u128::from(100 - MIN_SAVINGS_PERCENT);
    lhs <= rhs
}

/// Run the 64 KiB level-1 entropy probe. Returns `true` iff the probe saves at least 10%.
fn passes_entropy_probe(probe_slice: &[u8]) -> Result<bool, ObjectError> {
    if probe_slice.is_empty() {
        return Ok(false);
    }
    // Integer floor of `(len * 90) / 100` is the exact maximum compressed length
    // that satisfies `compressed * 100 <= len * 90`.
    let max_compressed = ((probe_slice.len() as u64 * (100 - MIN_SAVINGS_PERCENT)) / 100) as usize;
    if max_compressed == 0 {
        return Ok(false);
    }

    let mut compressor = new_chunk_compressor(PROBE_LEVEL)?;
    let mut dst = Vec::with_capacity(max_compressed);
    match compressor.compress_to_buffer(probe_slice, &mut dst) {
        Ok(written) => Ok(saves_at_least_min_percent(probe_slice.len(), written)),
        Err(err) if is_dst_size_too_small(&err) => Ok(false),
        Err(err) => Err(ObjectError::Io(err)),
    }
}

fn compress_chunks_parallel(
    payload: &Bytes,
    level: i32,
    workers: usize,
) -> Result<Vec<V3EncodedChunk>, ObjectError> {
    let chunk_count = payload.len().div_ceil(DEFAULT_V3_CHUNK_SIZE);
    let workers = workers.clamp(1, chunk_count.max(1));
    let mut slots: Vec<Option<V3EncodedChunk>> = vec![None; chunk_count];

    let queue = Mutex::new(slots.iter_mut().enumerate());
    let failed = AtomicBool::new(false);

    let run = || -> Result<(), ObjectError> {
        let mut compressor = new_chunk_compressor(level)?;
        while !failed.load(Ordering::Relaxed) {
            let next = queue.lock().unwrap_or_else(PoisonError::into_inner).next();
            let Some((idx, slot)) = next else {
                break;
            };
            let start = idx * DEFAULT_V3_CHUNK_SIZE;
            let end = (start + DEFAULT_V3_CHUNK_SIZE).min(payload.len());
            let chunk_slice = &payload[start..end];
            let encoded = compress_one_chunk(&mut compressor, payload, start, end, chunk_slice);
            match encoded {
                Ok(chunk) => *slot = Some(chunk),
                Err(err) => {
                    failed.store(true, Ordering::Relaxed);
                    return Err(err);
                }
            }
        }
        Ok(())
    };

    if workers == 1 {
        run()?;
    } else {
        thread::scope(|scope| -> Result<(), ObjectError> {
            let handles: Vec<_> = (1..workers)
                .filter_map(|_| thread::Builder::new().spawn_scoped(scope, run).ok())
                .collect();
            let mut result = run();
            for handle in handles {
                let joined = handle
                    .join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
                result = result.and(joined);
            }
            result
        })?;
    }

    slots
        .into_iter()
        .enumerate()
        .map(|(idx, slot)| {
            slot.ok_or_else(|| {
                ObjectError::InvalidV3Format(format!("missing compressed output for chunk {idx}"))
            })
        })
        .collect()
}

fn compress_one_chunk(
    compressor: &mut Compressor<'_>,
    payload: &Bytes,
    start: usize,
    end: usize,
    chunk_slice: &[u8],
) -> Result<V3EncodedChunk, ObjectError> {
    if chunk_slice.len() <= 1 {
        return Ok(V3EncodedChunk::Raw(payload.slice(start..end)));
    }
    // A compressed chunk must strictly shrink (`stored_len < original_len`).
    let max_compressed = chunk_slice.len() - 1;
    let mut dst = Vec::with_capacity(max_compressed);
    match compressor.compress_to_buffer(chunk_slice, &mut dst) {
        Ok(written) if written > 0 && written < chunk_slice.len() => {
            Ok(V3EncodedChunk::Compressed(Bytes::from(dst)))
        }
        Ok(_) => Ok(V3EncodedChunk::Raw(payload.slice(start..end))),
        Err(err) if is_dst_size_too_small(&err) => {
            Ok(V3EncodedChunk::Raw(payload.slice(start..end)))
        }
        Err(err) => Err(ObjectError::Io(err)),
    }
}

fn parse_complete_v3_seek_table(data: &Buffer) -> Result<V3SeekTable, ObjectError> {
    if data.len() < ObjectHeader::SIZE {
        return Err(
            io::Error::new(io::ErrorKind::UnexpectedEof, "file too short for header").into(),
        );
    }

    let mut header_buf = data.slice(..ObjectHeader::SIZE);
    let mut header_bytes = [0u8; ObjectHeader::SIZE];
    header_buf.copy_to_slice(&mut header_bytes);
    let header = ObjectHeader::from_bytes(&header_bytes)?;
    let meta = V3HeaderMeta::parse(&header)?;

    let payload = data.slice(ObjectHeader::SIZE..);
    let seek_table_len = meta.seek_table_len();
    if payload.len() < seek_table_len {
        return Err(ObjectError::InvalidV3Format(format!(
            "truncated seek table: need {seek_table_len} bytes, got {}",
            payload.len()
        )));
    }

    let mut seek_table_bytes = vec![0u8; seek_table_len];
    let mut seek_table_buf = payload.slice(..seek_table_len);
    seek_table_buf.copy_to_slice(&mut seek_table_bytes);
    let seek_table = V3SeekTable::parse_with_header(header, &seek_table_bytes)?;

    if payload.len() != header.compressed_size as usize {
        return Err(ObjectError::SizeMismatch {
            original_size: header.original_size,
            compressed_size: header.compressed_size,
            actual_size: payload.len(),
        });
    }

    Ok(seek_table)
}

fn decompress_v3_parsed(
    data: &Buffer,
    seek_table: &V3SeekTable,
    workers: usize,
) -> Result<(Bytes, u64), ObjectError> {
    let mut out = vec![0u8; seek_table.original_size()];
    let chunk_size = seek_table.chunk_size();
    let workers = workers.clamp(1, seek_table.entries().len().max(1));

    let queue = Mutex::new(seek_table.entries().iter().zip(out.chunks_mut(chunk_size)));
    let failed = AtomicBool::new(false);

    let run = || -> Result<(), ObjectError> {
        let mut decompressor: Option<Decompressor<'static>> = None;
        while !failed.load(Ordering::Relaxed) {
            let next = queue.lock().unwrap_or_else(PoisonError::into_inner).next();
            let Some((entry, dst)) = next else {
                break;
            };
            debug_assert_eq!(dst.len(), entry.original_len);
            let mut chunk_buf = data.slice(entry.plaintext_range());
            let step = if entry.is_raw {
                chunk_buf.copy_to_slice(dst);
                Ok(())
            } else {
                let stored = if Buffer::count(&chunk_buf) == 1 {
                    chunk_buf.current()
                } else {
                    chunk_buf.to_bytes()
                };
                match seek_table.codec() {
                    CompressionCodec::Zstd => {
                        let dctx = match decompressor.as_mut() {
                            Some(dctx) => dctx,
                            None => {
                                let created = Decompressor::new().map_err(|err| {
                                    ObjectError::ChunkDecompression {
                                        chunk_index: entry.index,
                                        message: format!(
                                            "failed to initialize zstd decompressor: {err}"
                                        ),
                                    }
                                })?;
                                decompressor.insert(created)
                            }
                        };
                        decompress_zstd_chunk_with(dctx, stored.as_ref(), dst, entry.index)
                    }
                }
            };
            if let Err(err) = step {
                failed.store(true, Ordering::Relaxed);
                return Err(err);
            }
        }
        Ok(())
    };

    if workers == 1 {
        run()?;
    } else {
        thread::scope(|scope| -> Result<(), ObjectError> {
            let handles: Vec<_> = (1..workers)
                .filter_map(|_| thread::Builder::new().spawn_scoped(scope, run).ok())
                .collect();
            let mut result = run();
            for handle in handles {
                let joined = handle
                    .join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
                result = result.and(joined);
            }
            result
        })?;
    }

    let computed_checksum = xxh64(&out, 0);
    let expected_checksum = seek_table.header().checksum;
    if computed_checksum != expected_checksum {
        return Err(ObjectError::ChecksumMismatch {
            expected: expected_checksum,
            computed: computed_checksum,
        });
    }

    Ok((Bytes::from(out), computed_checksum))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{V3SeekTable, read_header};
    use std::task::Poll;

    const ZSTD_FRAME_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
    const ZSTD_CONTENT_CHECKSUM_FLAG: u8 = 0x04;

    /// Deterministic high-entropy byte generator (splitmix64).
    fn pseudo_random_bytes(len: usize, mut seed: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = seed;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let remaining = len - out.len();
            let take = remaining.min(8);
            out.extend_from_slice(&z.to_le_bytes()[..take]);
        }
        out
    }

    /// Repeated ASCII text resembling USD/manifest/log content.
    fn compressible_text(len: usize) -> Bytes {
        let line = b"def Xform \"shot_010_hero_rig\" (kind = \"component\") { double3 xformOp:translate = (12.5, 0.0, -4.25) }\n";
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            let take = (len - out.len()).min(line.len());
            out.extend_from_slice(&line[..take]);
        }
        Bytes::from(out)
    }

    /// Repeated EXR-like scanlines: structured headers + correlated 16-bit channel runs.
    fn exr_like_scanlines(len: usize) -> Bytes {
        let mut out = Vec::with_capacity(len);
        let mut y: u32 = 0;
        while out.len() < len {
            // Scanline y-coordinate + pixel data size header (8 bytes)
            let scanline_pixels = 2048usize;
            for b in y.to_le_bytes() {
                if out.len() < len {
                    out.push(b);
                }
            }
            for b in (scanline_pixels as u32).to_le_bytes() {
                if out.len() < len {
                    out.push(b);
                }
            }
            // Repeated half-float-like smooth gradient across the scanline
            for x in 0..(scanline_pixels / 2) {
                let lo = ((x / 16) as u8).wrapping_add((y % 4) as u8);
                let hi = 0x3Cu8; // ~1.0 in IEEE 754 binary16
                if out.len() < len {
                    out.push(lo);
                }
                if out.len() < len {
                    out.push(hi);
                }
            }
            y += 1;
        }
        Bytes::from(out)
    }

    #[tokio::test]
    async fn compressible_text_and_exr_scanlines_produce_v3_and_round_trip() {
        let policy = CompressionPolicy::zstd_default();

        for (label, input) in [
            ("text-4mib", compressible_text(CLOUD_FRAGMENT_SIZE)),
            ("exr-4mib", exr_like_scanlines(CLOUD_FRAGMENT_SIZE)),
            ("text-300kib-unaligned", compressible_text(300 * 1024 + 17)),
            ("text-16kib-small", compressible_text(16 * 1024)),
        ] {
            let output = compress_fragment(input.clone(), policy).await.unwrap();
            assert!(output.is_v3(), "{label}: expected v3");
            assert!(!output.probe_skipped, "{label}: probe should not skip");
            assert_eq!(output.logical_bytes, input.len(), "{label}: logical_bytes");
            assert!(
                saves_at_least_min_percent(output.logical_bytes, output.stored_bytes),
                "{label}: stored_bytes {} did not save >= 10% of {}",
                output.stored_bytes,
                output.logical_bytes
            );

            let EncodedFragment::V3(v3_buf) = output.fragment else {
                unreachable!();
            };
            let v3_bytes = v3_buf.to_bytes();
            let seek_table = V3SeekTable::parse_from_plaintext(&v3_bytes).unwrap();
            assert_eq!(seek_table.codec(), CompressionCodec::Zstd);
            assert_eq!(seek_table.original_size(), input.len());
            assert_eq!(seek_table.compressed_size(), output.stored_bytes);

            // Verify every compressed chunk is a zstd frame with content checksum disabled.
            for entry in seek_table.entries() {
                if !entry.is_raw {
                    let chunk = &v3_bytes[entry.plaintext_range()];
                    assert!(chunk.len() >= 5, "{label}: chunk {} too short", entry.index);
                    assert_eq!(
                        &chunk[..4],
                        &ZSTD_FRAME_MAGIC,
                        "{label}: chunk {} missing zstd magic",
                        entry.index
                    );
                    assert_eq!(
                        chunk[4] & ZSTD_CONTENT_CHECKSUM_FLAG,
                        0,
                        "{label}: chunk {} must have content checksum disabled",
                        entry.index
                    );
                }
            }

            let (decoded, checksum) = decompress_v3_object_with_checksum(v3_bytes).await.unwrap();
            assert_eq!(decoded, input, "{label}: round-trip mismatch");
            assert_eq!(checksum, xxh64(input.as_ref(), 0), "{label}: checksum");
        }
    }

    #[tokio::test]
    async fn random_input_exits_at_probe() {
        let input = Bytes::from(pseudo_random_bytes(CLOUD_FRAGMENT_SIZE, 0xDEAD_BEEF_1234));
        let output = compress_fragment(input.clone(), CompressionPolicy::zstd_default())
            .await
            .unwrap();

        assert!(!output.is_v3());
        assert!(output.probe_skipped);
        assert_eq!(output.raw_reason(), Some(StoreRawReason::ProbeSkipped));
        assert_eq!(output.logical_bytes, input.len());
        assert_eq!(output.stored_bytes, input.len());
        let EncodedFragment::StoreRaw { payload, reason } = output.fragment else {
            panic!("expected StoreRaw");
        };
        assert_eq!(reason, StoreRawReason::ProbeSkipped);
        assert_eq!(payload, input);
    }

    #[tokio::test]
    async fn compressible_only_after_first_64kib_still_exits_at_probe() {
        // Documented ADR 0015 trade-off: first 64 KiB random, rest all zeros.
        let mut data = pseudo_random_bytes(PROBE_BYTES, 0xCAFE_BABE_0001);
        data.resize(CLOUD_FRAGMENT_SIZE, 0u8);
        let input = Bytes::from(data);

        let output = compress_fragment(input.clone(), CompressionPolicy::zstd_default())
            .await
            .unwrap();
        assert!(!output.is_v3());
        assert!(output.probe_skipped);
        assert_eq!(output.raw_reason(), Some(StoreRawReason::ProbeSkipped));
        assert_eq!(output.logical_bytes, input.len());
        assert_eq!(output.stored_bytes, input.len());
    }

    #[tokio::test]
    async fn passes_probe_but_fails_final_threshold_returns_store_raw() {
        // First 64 KiB is all zeros (passes probe easily), remaining ~3.94 MiB is random
        // so overall object saves far less than 10%.
        let mut data = vec![0u8; PROBE_BYTES];
        data.extend_from_slice(&pseudo_random_bytes(
            CLOUD_FRAGMENT_SIZE - PROBE_BYTES,
            0x1122_3344_5566,
        ));
        let input = Bytes::from(data);

        let output = compress_fragment(input.clone(), CompressionPolicy::zstd_default())
            .await
            .unwrap();
        assert!(!output.is_v3());
        assert!(!output.probe_skipped);
        assert_eq!(output.raw_reason(), Some(StoreRawReason::FinalThreshold));
        assert_eq!(output.logical_bytes, input.len());
        assert_eq!(output.stored_bytes, input.len());
    }

    #[tokio::test]
    async fn mixed_input_stores_incompressible_chunks_raw_and_round_trips() {
        // 8 chunks (1 MiB): even chunks compressible text, odd chunks random.
        // Overall savings ~50%, so v3 is selected and odd chunks must be stored raw.
        let num_chunks = 8usize;
        let mut data = Vec::with_capacity(num_chunks * DEFAULT_V3_CHUNK_SIZE);
        for chunk_idx in 0..num_chunks {
            if chunk_idx % 2 == 0 {
                data.extend_from_slice(&compressible_text(DEFAULT_V3_CHUNK_SIZE));
            } else {
                data.extend_from_slice(&pseudo_random_bytes(
                    DEFAULT_V3_CHUNK_SIZE,
                    0x5000 + chunk_idx as u64,
                ));
            }
        }
        let input = Bytes::from(data);

        let output = compress_fragment(input.clone(), CompressionPolicy::zstd_default())
            .await
            .unwrap();
        assert!(output.is_v3());
        assert!(!output.probe_skipped);

        let EncodedFragment::V3(v3_buf) = output.fragment else {
            unreachable!();
        };
        let v3_bytes = v3_buf.to_bytes();
        let seek_table = V3SeekTable::parse_from_plaintext(&v3_bytes).unwrap();
        assert_eq!(seek_table.entries().len(), num_chunks);

        for entry in seek_table.entries() {
            if entry.index % 2 == 0 {
                assert!(
                    !entry.is_raw,
                    "chunk {} should be stored compressed",
                    entry.index
                );
            } else {
                assert!(entry.is_raw, "chunk {} should be stored raw", entry.index);
                assert_eq!(entry.stored_len, DEFAULT_V3_CHUNK_SIZE);
            }
        }

        let decoded = decompress_v3_object(v3_bytes).await.unwrap();
        assert_eq!(decoded, input);
    }

    #[tokio::test]
    async fn malicious_decompression_bomb_chunk_is_rejected() {
        // Build a zstd frame that decompresses to 256 KiB (2x the 128 KiB chunk size)
        // yet occupies only a few dozen compressed bytes.
        let bomb_plaintext = vec![0xAAu8; DEFAULT_V3_CHUNK_SIZE * 2];
        let mut compressor = new_chunk_compressor(3).unwrap();
        let bomb_compressed = Bytes::from(compressor.compress(&bomb_plaintext).unwrap());
        assert!(bomb_compressed.len() < DEFAULT_V3_CHUNK_SIZE);

        let declared_original_size = DEFAULT_V3_CHUNK_SIZE as u32;
        let v3_buf = create_v3_object_buffer(
            declared_original_size,
            xxh64(&vec![0xAAu8; DEFAULT_V3_CHUNK_SIZE], 0),
            CompressionCodec::Zstd,
            DEFAULT_V3_CHUNK_LOG2,
            ObjectFlags::empty(),
            vec![V3EncodedChunk::Compressed(bomb_compressed)],
        )
        .unwrap();

        let err = decompress_v3_object(v3_buf).await.unwrap_err();
        assert!(
            matches!(
                err,
                DataError::Object(ObjectError::ChunkDecompression { chunk_index: 0, .. })
            ),
            "expected ChunkDecompression error for decompression bomb, got {err:?}"
        );
    }

    #[tokio::test]
    async fn short_decompressed_chunk_is_rejected() {
        // Compress only half a chunk and claim the chunk's original length is 128 KiB.
        let short_plaintext = vec![0x55u8; DEFAULT_V3_CHUNK_SIZE / 2];
        let mut compressor = new_chunk_compressor(3).unwrap();
        let short_compressed = Bytes::from(compressor.compress(&short_plaintext).unwrap());
        assert!(short_compressed.len() < DEFAULT_V3_CHUNK_SIZE);

        let v3_buf = create_v3_object_buffer(
            DEFAULT_V3_CHUNK_SIZE as u32,
            xxh64(&vec![0x55u8; DEFAULT_V3_CHUNK_SIZE], 0),
            CompressionCodec::Zstd,
            DEFAULT_V3_CHUNK_LOG2,
            ObjectFlags::empty(),
            vec![V3EncodedChunk::Compressed(short_compressed)],
        )
        .unwrap();

        let err = decompress_v3_object(v3_buf).await.unwrap_err();
        assert!(
            matches!(
                err,
                DataError::Object(ObjectError::ChunkDecompression { chunk_index: 0, .. })
            ),
            "expected ChunkDecompression error for short output, got {err:?}"
        );
    }

    #[tokio::test]
    async fn corrupted_zstd_chunk_and_bad_checksum_are_rejected() {
        let input = compressible_text(DEFAULT_V3_CHUNK_SIZE * 2);
        let output = compress_fragment(input.clone(), CompressionPolicy::zstd_default())
            .await
            .unwrap();
        let EncodedFragment::V3(v3_buf) = output.fragment else {
            panic!("expected v3");
        };
        let v3_bytes = v3_buf.to_bytes();
        let seek_table = V3SeekTable::parse_from_plaintext(&v3_bytes).unwrap();

        // 1. Corrupt compressed chunk bytes.
        let mut bad_chunk = v3_bytes.to_vec();
        let chunk0_offset = seek_table.entries()[0].plaintext_offset;
        bad_chunk[chunk0_offset] ^= 0xFF;
        let err = decompress_v3_object(Bytes::from(bad_chunk))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            DataError::Object(ObjectError::ChunkDecompression { chunk_index: 0, .. })
        ));

        // 2. Corrupt header checksum (bytes 16..24 of STFS header).
        let mut bad_checksum = v3_bytes.to_vec();
        bad_checksum[16] ^= 0x01;
        let err = decompress_v3_object(Bytes::from(bad_checksum))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            DataError::Object(ObjectError::ChecksumMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn policy_off_empty_and_oversized_inputs_handled() {
        let input = compressible_text(256 * 1024);
        let off = compress_fragment(input.clone(), CompressionPolicy::off())
            .await
            .unwrap();
        assert_eq!(off.raw_reason(), Some(StoreRawReason::PolicyOff));
        assert!(!off.probe_skipped);
        assert_eq!(off.logical_bytes, input.len());
        assert_eq!(off.stored_bytes, input.len());

        let empty = compress_fragment(Bytes::new(), CompressionPolicy::zstd_default())
            .await
            .unwrap();
        assert_eq!(empty.raw_reason(), Some(StoreRawReason::ProbeSkipped));
        assert!(empty.probe_skipped);
        assert_eq!(empty.logical_bytes, 0);
        assert_eq!(empty.stored_bytes, 0);

        let oversized = Bytes::from(vec![0u8; CLOUD_FRAGMENT_SIZE + 1]);
        let err = compress_fragment(oversized, CompressionPolicy::zstd_default())
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            DataError::Object(ObjectError::ObjectTooLarge(size))
                if size == (CLOUD_FRAGMENT_SIZE + 1) as u64
        ));
    }

    #[tokio::test]
    async fn compression_and_decompression_respect_crypto_admission_semaphore() {
        let admission = Arc::new(Semaphore::new(1));
        let held = admission.clone().acquire_owned().await.unwrap();

        let input = compressible_text(256 * 1024);
        let mut compress_fut = Box::pin(compress_fragment_with_admission(
            &admission,
            input.clone(),
            CompressionPolicy::zstd_default(),
        ));

        // While the sole permit is held, compression cannot start.
        let poll = futures::poll!(&mut compress_fut);
        assert!(matches!(poll, Poll::Pending));

        drop(held);
        let compressed = compress_fut.await.unwrap();
        let EncodedFragment::V3(v3_buf) = compressed.fragment else {
            panic!("expected v3");
        };
        let v3_bytes = v3_buf.to_bytes();
        assert_eq!(
            read_header(&v3_bytes).unwrap().version,
            crate::object::VERSION_V3
        );

        // Now hold the sole permit again and verify decompression also waits on admission.
        let held = admission.clone().acquire_owned().await.unwrap();
        let mut decompress_fut =
            Box::pin(decompress_v3_object_with_admission(&admission, v3_bytes));
        let poll = futures::poll!(&mut decompress_fut);
        assert!(matches!(poll, Poll::Pending));

        drop(held);
        let (decoded, _) = decompress_fut.await.unwrap();
        assert_eq!(decoded, input);
    }
}
