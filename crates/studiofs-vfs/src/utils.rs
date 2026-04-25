//! Utility functions for studiofs-vfs

use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IntervalRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

impl IntervalRange {
    pub(crate) fn new(start: u64, end: u64) -> Option<Self> {
        (start < end).then_some(Self { start, end })
    }

    pub(crate) fn intersection(self, other: Self) -> Option<Self> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        Self::new(start, end)
    }
}

pub(crate) fn subtract_claimed_range<I, R, C>(
    ranges: I,
    candidate: IntervalRange,
    remaining: &mut R,
    claimed: &mut C,
) where
    I: IntoIterator<Item = IntervalRange>,
    R: Extend<IntervalRange>,
    C: Extend<IntervalRange>,
{
    for range in ranges {
        let Some(overlap) = range.intersection(candidate) else {
            remaining.extend(std::iter::once(range));
            continue;
        };
        if range.start < overlap.start {
            remaining.extend(std::iter::once(IntervalRange {
                start: range.start,
                end: overlap.start,
            }));
        }
        claimed.extend(std::iter::once(overlap));
        if overlap.end < range.end {
            remaining.extend(std::iter::once(IntervalRange {
                start: overlap.end,
                end: range.end,
            }));
        }
    }
}

pub(crate) fn trim_range_to_available_len(
    range: IntervalRange,
    range_offset: u64,
    available_len: u64,
    max_end: u64,
) -> Option<IntervalRange> {
    if range_offset >= available_len {
        return None;
    }
    let allowed_len = available_len.checked_sub(range_offset)?;
    let new_end = range.start.checked_add(allowed_len)?;
    let end = range.end.min(new_end);
    if end > max_end {
        return None;
    }
    IntervalRange::new(range.start, end)
}

/// Parse a human-readable size string into bytes.
///
/// Supports plain bytes and `KB`, `MB`, `GB`, `TB`, `PB`, and `EB` suffixes.
/// Suffixes use binary multipliers (powers of 1024).
/// Fractional values are truncated toward zero after conversion to bytes.
pub fn parse_size_bytes(raw: &str) -> Option<u64> {
    let s = raw.trim().to_ascii_uppercase();
    if s.is_empty() {
        return None;
    }
    let (num_part, multiplier) = if let Some(v) = s.strip_suffix("EB") {
        (v, 1024u128 * 1024 * 1024 * 1024 * 1024 * 1024)
    } else if let Some(v) = s.strip_suffix("PB") {
        (v, 1024u128 * 1024 * 1024 * 1024 * 1024)
    } else if let Some(v) = s.strip_suffix("TB") {
        (v, 1024u128 * 1024 * 1024 * 1024)
    } else if let Some(v) = s.strip_suffix("GB") {
        (v, 1024u128 * 1024 * 1024)
    } else if let Some(v) = s.strip_suffix("MB") {
        (v, 1024u128 * 1024)
    } else if let Some(v) = s.strip_suffix("KB") {
        (v, 1024u128)
    } else if let Some(v) = s.strip_suffix("B") {
        (v, 1u128)
    } else {
        (s.as_str(), 1u128)
    };
    let num_part = num_part.trim();
    let (int_part, frac_part) = match num_part.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (num_part, ""),
    };

    let int_part = if int_part.is_empty() {
        0
    } else {
        int_part.trim().parse::<u128>().ok()?
    };

    if int_part == 0 && frac_part.is_empty() {
        return Some(0);
    }

    let frac_part = frac_part.trim();
    let (scaled_num, scale) = if frac_part.is_empty() {
        (int_part, 1u128)
    } else {
        if !frac_part.chars().all(|ch| ch.is_ascii_digit()) {
            return None;
        }
        let frac_value = frac_part.parse::<u128>().ok()?;
        let scale = 10u128.checked_pow(u32::try_from(frac_part.len()).ok()?)?;
        let scaled_int = int_part.checked_mul(scale)?;
        let scaled_num = scaled_int.checked_add(frac_value)?;
        (scaled_num, scale)
    };

    let scaled_bytes = scaled_num.checked_mul(multiplier)?;
    let bytes = scaled_bytes / scale;
    u64::try_from(bytes).ok()
}

pub(crate) fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub(crate) fn now_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_intersection_handles_empty_boundary_and_full_cover() {
        let range = IntervalRange::new(10, 20).unwrap();

        assert_eq!(
            range.intersection(IntervalRange::new(15, 25).unwrap()),
            Some(IntervalRange::new(15, 20).unwrap())
        );
        assert_eq!(
            range.intersection(IntervalRange::new(20, 30).unwrap()),
            None
        );
        assert_eq!(
            range.intersection(IntervalRange::new(0, 30).unwrap()),
            Some(range)
        );
        assert!(IntervalRange::new(7, 7).is_none());
        assert!(IntervalRange::new(8, 7).is_none());
    }

    #[test]
    fn subtract_claimed_range_preserves_order_and_fragments() {
        let source = [
            IntervalRange::new(0, 8).unwrap(),
            IntervalRange::new(16, 24).unwrap(),
            IntervalRange::new(30, 40).unwrap(),
        ];
        let mut remaining = Vec::new();
        let mut claimed = Vec::new();

        subtract_claimed_range(
            source,
            IntervalRange::new(4, 35).unwrap(),
            &mut remaining,
            &mut claimed,
        );

        assert_eq!(
            claimed,
            vec![
                IntervalRange::new(4, 8).unwrap(),
                IntervalRange::new(16, 24).unwrap(),
                IntervalRange::new(30, 35).unwrap(),
            ]
        );
        assert_eq!(
            remaining,
            vec![
                IntervalRange::new(0, 4).unwrap(),
                IntervalRange::new(35, 40).unwrap(),
            ]
        );
    }

    #[test]
    fn trim_range_to_available_len_keeps_prefix_and_trims_suffix() {
        let range = IntervalRange::new(20, 30).unwrap();

        assert_eq!(
            trim_range_to_available_len(range, 4, 20, 64),
            Some(IntervalRange::new(20, 30).unwrap())
        );
        assert_eq!(
            trim_range_to_available_len(range, 4, 10, 64),
            Some(IntervalRange::new(20, 26).unwrap())
        );
        assert_eq!(trim_range_to_available_len(range, 10, 10, 64), None);
    }

    #[test]
    fn trim_range_to_available_len_rejects_overflow_and_bounds() {
        assert_eq!(
            trim_range_to_available_len(
                IntervalRange::new(u64::MAX - 2, u64::MAX).unwrap(),
                0,
                8,
                u64::MAX
            ),
            None
        );
        assert_eq!(
            trim_range_to_available_len(IntervalRange::new(20, 30).unwrap(), 0, 20, 25),
            None
        );
    }

    #[test]
    fn parse_size_bytes_accepts_fractional_values() {
        assert_eq!(parse_size_bytes("1.5MB"), Some(1_572_864));
        assert_eq!(parse_size_bytes("2.25GB"), Some(2_415_919_104));
        assert_eq!(parse_size_bytes("0.5KB"), Some(512));
    }

    #[test]
    fn parse_size_bytes_accepts_pb_eb_suffixes() {
        assert_eq!(parse_size_bytes("1PB"), Some(1_125_899_906_842_624));
        assert_eq!(parse_size_bytes("1EB"), Some(1_152_921_504_606_846_976));
    }

    #[test]
    fn parse_size_bytes_accepts_whitespace_and_case_insensitive_suffixes() {
        assert_eq!(parse_size_bytes(" 1 kb "), Some(1024));
        assert_eq!(parse_size_bytes(" .5 MB "), Some(524_288));
    }

    #[test]
    fn parse_size_bytes_truncates_fractional_bytes() {
        assert_eq!(parse_size_bytes("1.999B"), Some(1));
        assert_eq!(parse_size_bytes("0.5B"), Some(0));
    }

    #[test]
    fn parse_size_bytes_rejects_malformed_values() {
        assert_eq!(parse_size_bytes(""), None);
        assert_eq!(parse_size_bytes("   "), None);
        assert_eq!(parse_size_bytes("1.2.3MB"), None);
        assert_eq!(parse_size_bytes("1..5MB"), None);
        assert_eq!(parse_size_bytes("1,5MB"), None);
        assert_eq!(parse_size_bytes("abc"), None);
    }

    #[test]
    fn parse_size_bytes_rejects_overflow() {
        let too_big = (u64::MAX as u128) + 1;
        assert_eq!(parse_size_bytes(&too_big.to_string()), None);
    }

    #[test]
    fn parse_size_bytes_rejects_fraction_scale_overflow() {
        let input = format!("1.{}KB", "0".repeat(50));
        assert_eq!(parse_size_bytes(&input), None);
    }
}
