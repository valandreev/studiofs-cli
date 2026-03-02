//! Block operations — splitting data into blocks and reassembling.

use bytes::Bytes;
use studiofs_meta::DEFAULT_BLOCK_SIZE;

/// Split a data buffer into blocks of `DEFAULT_BLOCK_SIZE`.
///
/// Returns a list of (block_index, block_data) pairs.
pub fn split_into_blocks(data: &[u8], start_offset: u32) -> Vec<(u32, Bytes)> {
    let bs = DEFAULT_BLOCK_SIZE as usize;
    let mut blocks = Vec::new();

    // Calculate the first block index and offset within that block
    let first_block_idx = start_offset as usize / bs;
    let offset_in_first_block = start_offset as usize % bs;

    let mut remaining = data;
    let mut block_idx = first_block_idx;
    let mut offset_in_block = offset_in_first_block;

    while !remaining.is_empty() {
        let available_in_block = bs - offset_in_block;
        let take = remaining.len().min(available_in_block);

        blocks.push((block_idx as u32, Bytes::copy_from_slice(&remaining[..take])));

        remaining = &remaining[take..];
        block_idx += 1;
        offset_in_block = 0; // subsequent blocks start from 0
    }

    blocks
}

/// Calculate block index and offset within block for a given offset within a slice.
pub fn block_position(offset_in_slice: u32) -> (u32, u32) {
    let bs = DEFAULT_BLOCK_SIZE;
    let block_idx = offset_in_slice / bs;
    let offset_in_block = offset_in_slice % bs;
    (block_idx, offset_in_block)
}

/// Calculate how many blocks a slice of given size spans.
pub fn block_count(slice_size: u32) -> u32 {
    let bs = DEFAULT_BLOCK_SIZE;
    slice_size.div_ceil(bs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_aligned() {
        let bs = DEFAULT_BLOCK_SIZE as usize;
        let data = vec![0u8; 2 * bs]; // 2 blocks
        let blocks = split_into_blocks(&data, 0);
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].0, 0);
        assert_eq!(blocks[0].1.len(), bs);
        assert_eq!(blocks[1].0, 1);
        assert_eq!(blocks[1].1.len(), bs);
    }

    #[test]
    fn test_split_unaligned() {
        let bs = DEFAULT_BLOCK_SIZE as usize;
        let data = vec![0u8; bs + 512 * 1024]; // 1 full + 0.5 MiB partial
        let blocks = split_into_blocks(&data, 0);
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].1.len(), bs);
        assert_eq!(blocks[1].1.len(), 512 * 1024);
    }

    #[test]
    fn test_split_with_offset() {
        let bs = DEFAULT_BLOCK_SIZE as usize;
        let data = vec![0u8; bs / 2]; // 0.5 MiB
        let blocks = split_into_blocks(&data, (3 * bs) as u32); // starting at block 3
        // offset 3 MiB is in block 3, with the full block available
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].0, 3);
        assert_eq!(blocks[0].1.len(), bs / 2);
    }

    #[test]
    fn test_block_position() {
        assert_eq!(block_position(0), (0, 0));
        assert_eq!(block_position(DEFAULT_BLOCK_SIZE), (1, 0));
        assert_eq!(block_position(DEFAULT_BLOCK_SIZE + 1024), (1, 1024));
    }

    #[test]
    fn test_block_count() {
        assert_eq!(block_count(DEFAULT_BLOCK_SIZE), 1);
        assert_eq!(block_count(DEFAULT_BLOCK_SIZE + 1), 2);
        assert_eq!(block_count(2 * DEFAULT_BLOCK_SIZE), 2);
    }
}
