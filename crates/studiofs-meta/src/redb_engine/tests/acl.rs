#[test]
fn test_acl_rule_set_get_remove_roundtrip() {
    let engine = temp_engine();
    let secret = engine.create_dir(ROOT_INO, b"secret", 0, 0, 0o755).unwrap();

    assert_eq!(engine.get_acl_rule(secret.ino, "alice").unwrap(), None);
    engine
        .set_acl_rule(secret.ino, "alice", AccessLevel::ReadOnly)
        .unwrap();
    assert_eq!(
        engine.get_acl_rule(secret.ino, "alice").unwrap(),
        Some(AccessLevel::ReadOnly)
    );

    engine
        .set_acl_rule(secret.ino, "alice", AccessLevel::NoAccess)
        .unwrap();
    assert_eq!(
        engine.get_acl_rule(secret.ino, "alice").unwrap(),
        Some(AccessLevel::NoAccess)
    );

    engine.remove_acl_rule(secret.ino, "alice").unwrap();
    assert_eq!(engine.get_acl_rule(secret.ino, "alice").unwrap(), None);
}

#[test]
fn test_acl_rules_cascade_on_remove_attr() {
    let engine = temp_engine();
    let dir = engine.create_dir(ROOT_INO, b"secret", 0, 0, 0o755).unwrap();
    engine
        .set_acl_rule(dir.ino, "bob", AccessLevel::ReadWrite)
        .unwrap();
    assert_eq!(
        engine.get_acl_rule(dir.ino, "bob").unwrap(),
        Some(AccessLevel::ReadWrite)
    );

    engine.rmdir(ROOT_INO, b"secret").unwrap();
    assert_eq!(engine.get_acl_rule(dir.ino, "bob").unwrap(), None);
}
