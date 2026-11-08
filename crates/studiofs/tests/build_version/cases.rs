use super::*;

fn temporary_directory() -> tempfile::TempDir {
    tempfile::tempdir_in(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target")).unwrap()
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn commit(repo: &Path, version: &str) {
    std::fs::write(
        repo.join("Cargo.toml"),
        format!("[workspace.package]\nversion = \"{version}\"\n"),
    )
    .unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "--allow-empty", "-m", "fixture"]);
}

fn repository() -> tempfile::TempDir {
    let repo = temporary_directory();
    git(repo.path(), &["init", "-b", "main"]);
    git(repo.path(), &["config", "user.name", "Version Test"]);
    git(
        repo.path(),
        &["config", "user.email", "version@example.invalid"],
    );
    git(repo.path(), &["config", "commit.gpgsign", "false"]);
    commit(repo.path(), "0.2.2");
    repo
}

fn expected(repo: &Path, base: &str, count: &str) -> String {
    let sha = git(repo, &["rev-parse", "--short=8", "HEAD"]);
    format!("{base}-dev.{count} ({sha})")
}

#[test]
fn version_counter_resets_and_ignores_unrelated_manifest_changes() {
    let repo = repository();
    let path = repo.path();
    assert_eq!(build_version(path, "0.2.2"), expected(path, "0.2.2", "0"));
    commit(path, "0.2.2");
    std::fs::write(
        path.join("Cargo.toml"),
        "[workspace.package]\nversion = \"0.2.2\"\n[workspace.metadata]\nnote = 'changed'\n",
    )
    .unwrap();
    git(path, &["commit", "-am", "unrelated manifest change"]);
    assert_eq!(build_version(path, "0.2.2"), expected(path, "0.2.2", "2"));
    commit(path, "0.2.3");
    assert_eq!(build_version(path, "0.2.3"), expected(path, "0.2.3", "0"));
}

#[test]
fn merge_counts_once_including_a_version_change_from_the_branch() {
    let repo = repository();
    let path = repo.path();
    git(path, &["checkout", "-b", "feature"]);
    commit(path, "0.2.2");
    commit(path, "0.2.2");
    git(path, &["checkout", "main"]);
    git(
        path,
        &["merge", "--no-ff", "feature", "-m", "merge feature"],
    );
    assert_eq!(build_version(path, "0.2.2"), expected(path, "0.2.2", "1"));
    git(path, &["checkout", "feature"]);
    commit(path, "0.2.3");
    git(path, &["checkout", "main"]);
    git(
        path,
        &["merge", "--no-ff", "feature", "-m", "merge version"],
    );
    assert_eq!(build_version(path, "0.2.3"), expected(path, "0.2.3", "0"));
}

#[test]
fn only_a_clean_matching_release_tag_hides_development_identity() {
    let repo = repository();
    let path = repo.path();
    git(path, &["tag", "v0.2.1"]);
    assert_eq!(build_version(path, "0.2.2"), expected(path, "0.2.2", "0"));
    git(path, &["tag", "-a", "v0.2.2", "-m", "release"]);
    assert_eq!(build_version(path, "0.2.2"), "0.2.2");
    std::fs::write(
        path.join("Cargo.toml"),
        "# local edit\n[workspace.package]\nversion = '0.2.2'\n",
    )
    .unwrap();
    let sha = git(path, &["rev-parse", "--short=8", "HEAD"]);
    assert_eq!(
        build_version(path, "0.2.2"),
        format!("0.2.2-dev.0-dirty ({sha})")
    );
}

#[test]
fn missing_or_shallow_history_does_not_invent_a_counter() {
    let no_git = temporary_directory();
    // Prevent Git from discovering the enclosing StudioFS checkout.
    std::fs::write(no_git.path().join(".git"), "gitdir: unavailable\n").unwrap();
    assert_eq!(
        build_version(no_git.path(), "0.2.2"),
        "0.2.2-dev.unknown (unknown)"
    );
    let source = repository();
    commit(source.path(), "0.2.2");
    let clone_parent = temporary_directory();
    let clone = clone_parent.path().join("clone");
    git(
        clone_parent.path(),
        &[
            "clone",
            "--depth=1",
            url::Url::from_directory_path(source.path())
                .unwrap()
                .as_str(),
            "clone",
        ],
    );
    assert_eq!(
        build_version(&clone, "0.2.2"),
        expected(&clone, "0.2.2", "unknown")
    );
}
