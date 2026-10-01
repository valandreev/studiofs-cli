use studiofs_cache::runtime::platform::{
    UnbufferedReadBuffer, open_read_only_unbuffered, read_unbuffered_with_buffer,
    unbuffered_read_alignment_for_path,
};

#[test]
fn public_unbuffered_read_api_roundtrips_payload() {
    let temp = studiofs_test_support::workspace_target_tempdir("test-");
    let path = temp.path().join("payload");
    let alignment = unbuffered_read_alignment_for_path(temp.path());
    let payload = vec![0xCD; alignment.max(4096)];
    std::fs::write(&path, &payload).unwrap();
    let file = open_read_only_unbuffered(&path).unwrap();
    let mut scratch = UnbufferedReadBuffer::new();
    let mut out = vec![0; payload.len()];

    assert_eq!(
        read_unbuffered_with_buffer(&file, &mut out, 0, &mut scratch, alignment).unwrap(),
        payload.len()
    );
    assert_eq!(out, payload);
}
