//! Integration tests for `.studiofsignore` parsing and path classification,
//! including parity checks that require `git check-ignore`.

use std::fs;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::process::Command;

use ScratchClass::{Scratch, Tracked};
use studiofs_vfs::scratch_rules::{MAX_RULE_BYTES, RuleUpdateError, ScratchClass, ScratchRules};

const ROOT_RULES: &str = concat!(
    r#"# comment
*.tmp
!important.tmp
/root-only
a/**/output
**/deep-marker
tail/**
cache/
!cache/rescue.tmp
reopened/
!reopened/
!reopened/rescue.tmp
nested/*.log
\#literal
\!literal
"#,
    "literal-space\\ \ntrimmed   \nclass[0-9].bin\nCaseSensitive\n",
    "[[:ab]c\n[x[:y]z\n[[:]]w\n",
);

const NESTED_RULES: &str = "/local-only\n!important.log\nimportant.tmp\n";
const EXCLUDED_RULES: &str = "!rescue.tmp\n";

const CASES: &[(&str, bool, bool)] = &[
    ("ordinary.txt", false, false),
    ("# comment", false, false),
    ("nested/take.tmp", false, true),
    ("important.tmp", false, false),
    ("nested/important.tmp", false, true),
    ("root-only", false, true),
    ("nested/root-only", false, false),
    ("a/output", false, true),
    ("a/b/c/output", false, true),
    ("other/a/output", false, false),
    ("deep-marker", false, true),
    ("a/b/deep-marker", false, true),
    ("tail", true, false),
    ("tail/a/b", false, true),
    ("cache", true, true),
    ("other/cache", false, false),
    ("cache/rescue.tmp", false, true),
    ("cache/plain", false, true),
    ("reopened/rescue.tmp", false, false),
    ("reopened/other.tmp", false, true),
    ("nested/build.log", false, true),
    ("nested/important.log", false, false),
    ("nested/deeper/build.log", false, false),
    ("nested/local-only", false, true),
    ("nested/deeper/local-only", false, false),
    ("#literal", false, true),
    ("!literal", false, true),
    ("literal-space ", false, true),
    ("literal-space", false, false),
    ("trimmed", false, true),
    ("class4.bin", false, true),
    ("classx.bin", false, false),
    ("casesensitive", false, false),
    ("ac", false, true),
    ("[c", false, true),
    (":c", false, false),
    ("yz", false, true),
    ("[z", false, true),
    (":z", false, false),
    ("[]w", false, true),
    (":]w", false, false),
];

fn git(root: &Path) -> Command {
    let mut command = Command::new("git");
    for (key, _) in std::env::vars_os() {
        if key
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("GIT_")
        {
            command.env_remove(key);
        }
    }
    command
        .current_dir(root)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", root.join("empty-git-config"))
        .args([
            "-c",
            "core.ignoreCase=false",
            "-c",
            "core.excludesFile=empty-git-config",
        ]);
    command
}

fn git_repo() -> tempfile::TempDir {
    let repo = tempfile::tempdir().unwrap();
    let root = repo.path();
    fs::write(root.join("empty-git-config"), "").unwrap();
    fs::create_dir(root.join("empty-template")).unwrap();
    let init = git(root)
        .args(["init", "--quiet", "--template=empty-template"])
        .output()
        .expect("Git is required for the scratch rule parity test");
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    repo
}

fn git_ignores(root: &Path, path: &std::ffi::OsStr) -> bool {
    let result = git(root)
        .args(["check-ignore", "--no-index", "--quiet", "--"])
        .arg(path)
        .output()
        .unwrap();
    match result.status.code() {
        Some(0) => true,
        Some(1) => false,
        _ => panic!(
            "git check-ignore failed for {path:?}: {}",
            String::from_utf8_lossy(&result.stderr)
        ),
    }
}

#[test]
fn scratch_rules_match_git_check_ignore() {
    let repo = git_repo();
    let root = repo.path();

    fs::write(root.join(".gitignore"), ROOT_RULES).unwrap();
    for (directory, contents) in [("nested", NESTED_RULES), ("cache", EXCLUDED_RULES)] {
        fs::create_dir(root.join(directory)).unwrap();
        fs::write(root.join(directory).join(".gitignore"), contents).unwrap();
    }

    fs::create_dir(root.join("tail")).unwrap();
    let rules = fixture_rules();
    for &(path, is_dir, expected) in CASES {
        let ignored = git_ignores(root, path.as_ref());
        assert_eq!(ignored, expected, "Git fixture for {path}");
        assert_eq!(
            rules.classify(path.as_bytes(), is_dir) == Scratch,
            ignored,
            "{path}"
        );
    }
}

fn fixture_rules() -> ScratchRules {
    let mut rules = ScratchRules::new(false);
    rules.update(b"", ROOT_RULES.as_bytes()).unwrap();
    rules.update(b"nested", NESTED_RULES.as_bytes()).unwrap();
    rules.update(b"cache", EXCLUDED_RULES.as_bytes()).unwrap();
    rules
}

#[test]
fn scratch_rules_apply_git_syntax_and_precedence() {
    let rules = fixture_rules();
    for &(path, is_dir, ignored) in CASES {
        assert_eq!(
            rules.classify(path.as_bytes(), is_dir),
            if ignored { Scratch } else { Tracked },
            "StudioFS classification for {path}",
        );
    }
}

#[test]
fn scratch_rules_protect_rule_files_and_allow_appledouble_override() {
    let mut rules = ScratchRules::new(false);
    assert_eq!(rules.classify(b"nested/._shot", false), Scratch);
    let diagnostics = rules
        .update(
            b"",
            b"# line one\n.studiofsignore\n!._*\ncache/\n/a//.studiofsignore\n[//a]/.studiofsignore\n!*ignore\n",
        )
        .unwrap();
    let lines: Vec<_> = diagnostics
        .iter()
        .map(|diagnostic| diagnostic.line)
        .collect();
    assert_eq!(lines, [2, 6, 7]);
    assert_eq!(rules.classify(b".studiofsignore", false), Tracked);
    assert_eq!(rules.classify(b"nested/.studiofsignore", false), Tracked);
    assert_eq!(rules.classify(b"cache/.studiofsignore", false), Tracked);
    assert_eq!(rules.classify(b"nested/._shot", false), Tracked);
    rules.remove(b"");
    assert_eq!(rules.classify(b"nested/._shot", false), Scratch);
}

#[test]
fn scratch_rules_follow_volume_dentry_folding() {
    for (case_insensitive, expected) in [(false, Tracked), (true, Scratch)] {
        let mut rules = ScratchRules::new(case_insensitive);
        rules
            .update(b"", "ÉDIT.TMP\n�.raw\nİ.raw\n".as_bytes())
            .unwrap();
        rules.update("ÉTAGE".as_bytes(), b"/SAVE.tmp\n").unwrap();
        for path in [
            "édit.tmp".as_bytes(),
            b"\xff.raw",
            "i\u{307}.raw".as_bytes(),
            "étage/save.TMP".as_bytes(),
        ] {
            assert_eq!(rules.classify(path, false), expected, "{path:?}");
        }
        assert_eq!(rules.classify("i.raw".as_bytes(), false), Tracked);
    }
}

#[test]
fn scratch_rules_retain_last_good_update_on_invalid_payloads() {
    let mut rules = ScratchRules::new(false);
    assert_eq!(MAX_RULE_BYTES, 64 * 1024);
    let mut payload = b"old.tmp\n#".to_vec();
    payload.resize(MAX_RULE_BYTES, b'x');
    rules.update(b"nested", &payload).unwrap();
    payload.push(b'x');

    for (invalid, expected) in [
        (payload.as_slice(), RuleUpdateError::TooLarge),
        (b"new.tmp\n\0\n", RuleUpdateError::InvalidPayload),
        (
            b"new.tmp\n[unterminated\n",
            RuleUpdateError::InvalidPattern { line: 2 },
        ),
    ] {
        assert_eq!(rules.update(b"nested", invalid).unwrap_err(), expected);
        assert_eq!(rules.classify(b"nested/old.tmp", false), Scratch);
        assert_eq!(rules.classify(b"nested/new.tmp", false), Tracked);
    }

    rules.update(b"nested", b"new.tmp\n").unwrap();
    assert_eq!(rules.classify(b"nested/old.tmp", false), Tracked);
    assert_eq!(rules.classify(b"nested/new.tmp", false), Scratch);
}

#[cfg(unix)]
#[test]
fn scratch_rules_match_git_for_raw_byte_patterns() {
    let repo = git_repo();
    let root = repo.path();
    let payload = b"raw\xff\n[\xfe]class\nsub/\xfd*\n";
    fs::write(root.join(".gitignore"), payload).unwrap();
    let mut rules = ScratchRules::new(false);
    rules.update(b"", payload).unwrap();

    for (path, expected) in [
        (&b"raw\xff"[..], true),
        (b"nested/raw\xff", true),
        (b"raw\xfe", false),
        (b"\xfeclass", true),
        (b"\xffclass", false),
        (b"sub/\xfdname", true),
        (b"sub/\xfcname", false),
    ] {
        let ignored = git_ignores(root, std::ffi::OsStr::from_bytes(path));
        assert_eq!(ignored, expected, "Git fixture for {path:?}");
        assert_eq!(rules.classify(path, false) == Scratch, ignored, "{path:?}");
    }
}

#[test]
fn scratch_rules_fold_posix_classes_on_case_insensitive_volumes() {
    let mut rules = ScratchRules::new(true);
    rules.update(b"", b"[[:upper:]].tmp\n").unwrap();
    assert_eq!(rules.classify(b"A.tmp", false), Scratch);
    assert_eq!(rules.classify(b"a.tmp", false), Scratch);
    assert_eq!(rules.classify(b"1.tmp", false), Tracked);
}

#[test]
fn scratch_rules_pick_last_match_across_large_rule_sets() {
    let mut payload = String::new();
    for index in 0..3_000 {
        payload.push_str(&format!("l{index}\n*.e{index}\n"));
    }
    payload.push_str(concat!(
        "!l2999\n",
        "!*.e17\n",
        "deep/l5\n",
        "!keep\n",
        "keep/\n",
        "drop/\n",
        "!drop\n",
        "sub/late.tmp\n",
        "!late.tmp\n",
        "!early.tmp\n",
        "sub/early.tmp\n",
    ));
    assert!(payload.len() <= MAX_RULE_BYTES);
    let mut rules = ScratchRules::new(false);
    rules.update(b"", payload.as_bytes()).unwrap();
    rules.update(b"nested", b"!l7\n").unwrap();

    for (path, is_dir, expected) in [
        ("a/b/c/l0", false, Scratch),
        ("a/b/c/l2998", false, Scratch),
        ("a/b/c/l2999", false, Tracked),
        ("a/b/c/l3000", false, Tracked),
        ("a/b/c/name.e2999", false, Scratch),
        ("a/b/c/name.e17", false, Tracked),
        ("a/b/c/name.e3000", false, Tracked),
        ("deep/l5", false, Scratch),
        ("nested/l7", false, Tracked),
        ("nested/l8", false, Scratch),
        ("keep", true, Scratch),
        ("keep", false, Tracked),
        ("drop", true, Tracked),
        ("sub/late.tmp", false, Tracked),
        ("sub/early.tmp", false, Scratch),
        ("other/early.tmp", false, Tracked),
    ] {
        assert_eq!(rules.classify(path.as_bytes(), is_dir), expected, "{path}");
    }
    for index in 0..1_000 {
        let path = format!("d{index}/e/f/miss{index}.dat");
        assert_eq!(rules.classify(path.as_bytes(), false), Tracked, "{path}");
    }
}
