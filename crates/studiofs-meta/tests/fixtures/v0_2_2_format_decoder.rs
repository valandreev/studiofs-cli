// Actual v0.2.2 decoder, commit 2680630adc8f61ce39ca1fd59bdd023e8977cb62.
// Function copied verbatim; surrounding imports and version pinned for this test.
use studiofs_meta::{FormatInfo, MetaError, MetaResult};
const CURRENT_FORMAT_VERSION: u16 = 5;
#[derive(serde::Deserialize)]
struct FormatVersionOnly {
    format_version: u16,
}
pub fn decode_format_info(bytes: &[u8]) -> MetaResult<FormatInfo> {
    let version: FormatVersionOnly =
        serde_json::from_slice(bytes).map_err(|e| MetaError::Serialization(e.to_string()))?;
    if version.format_version != CURRENT_FORMAT_VERSION {
        return Err(MetaError::UnsupportedFormatVersion {
            found: version.format_version,
            expected: CURRENT_FORMAT_VERSION,
        });
    }
    let info: FormatInfo =
        serde_json::from_slice(bytes).map_err(|e| MetaError::Serialization(e.to_string()))?;
    info.validate_format_contract()?;
    Ok(info)
}
