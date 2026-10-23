#!/usr/bin/env python3
"""Exercise the real cargo-deny policy against isolated approved/rejected crates."""

from pathlib import Path
import subprocess
import tempfile


def main() -> None:
    repo = Path(__file__).resolve().parents[2]
    version = subprocess.check_output(["cargo", "deny", "--version"], text=True).strip()
    if version != "cargo-deny 0.20.2":
        raise RuntimeError(f"expected cargo-deny 0.20.2, got {version}")
    target = repo / "target"
    target.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="license-policy-", dir=target) as directory:
        fixture = Path(directory)
        (fixture / "src").mkdir()
        (fixture / "src/lib.rs").write_text("", encoding="utf-8")
        for license_name, expected in [("MIT", True), ("AGPL-3.0-only", False), (None, False)]:
            manifest = '[package]\nname = "license-policy-probe"\nversion = "0.0.0"\n[workspace]\n'
            if license_name is not None:
                manifest = manifest.replace('[workspace]', f'license = "{license_name}"\n[workspace]')
            (fixture / "Cargo.toml").write_text(manifest, encoding="utf-8")
            subprocess.run(["cargo", "generate-lockfile", "--offline"], cwd=fixture, check=True,
                           capture_output=True)
            result = subprocess.run(
                ["cargo", "deny", "--locked", "--offline", "--config",
                 str(repo / "deny.toml"), "check", "licenses"], cwd=fixture, capture_output=True,
                text=True, encoding="utf-8", errors="replace",
            )
            if (result.returncode == 0) != expected:
                raise RuntimeError(f"unexpected policy result for {license_name}:\n{result.stdout}\n{result.stderr}")
            if not expected and "unlicensed" not in result.stderr and "error[rejected]" not in result.stderr:
                raise RuntimeError(f"probe failed for a reason other than license policy:\n{result.stderr}")
    print("license policy probes passed: MIT accepted, AGPL and missing license rejected")


if __name__ == "__main__":
    main()
