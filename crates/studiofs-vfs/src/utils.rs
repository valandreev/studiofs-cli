//! Utility functions for studiofs-vfs

/// Parse a human-readable size string into bytes.
///
/// Supports plain bytes and `KB`, `MB`, `GB`, and `TB` suffixes.
pub fn parse_size_bytes(raw: &str) -> Option<usize> {
    let s = raw.trim().to_ascii_uppercase();
    if s.is_empty() {
        return None;
    }
    let (num_part, multiplier) = if let Some(v) = s.strip_suffix("TB") {
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
        let scale = 10u128.pow(frac_part.len() as u32);
        let scaled_int = int_part.checked_mul(scale)?;
        let scaled_num = scaled_int.checked_add(frac_value)?;
        (scaled_num, scale)
    };

    let scaled_bytes = scaled_num.checked_mul(multiplier)?;
    let bytes = scaled_bytes / scale;
    usize::try_from(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_size_bytes_accepts_fractional_values() {
        assert_eq!(parse_size_bytes("1.5MB"), Some(1_572_864));
        assert_eq!(parse_size_bytes("2.25GB"), Some(2_415_919_104));
        assert_eq!(parse_size_bytes("0.5KB"), Some(512));
    }
}
