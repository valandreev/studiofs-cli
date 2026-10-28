#!/usr/bin/env bash
# Fetch and build the POSIX conformance suites used by docs/posix-conformance.md
# (SFS-906): pjdfstest (Rust rewrite) and fsx from xfstests.
#
# Everything lands under $POSIX_ROOT (default <repo>/target/posix-conformance/tools).
# Pass the recorded commits to rebuild the exact baseline versions.
set -euo pipefail

repo=$(cd "$(dirname "$0")/../.." && pwd)
root=${POSIX_ROOT:-$repo/target/posix-conformance}/tools
pjdfstest_rev=${PJDFSTEST_REV:-ce3ca5c10d1d29f25b2881c85c62be2c74c27bf8}
xfstests_rev=${XFSTESTS_REV:-22348afe338c0f6d540c0d7ef0db749eaa51b218}
mkdir -p "$root/bin" "$root/fsx-config"

fetch() { # <url> <dir> <rev>
  if [ ! -d "$root/$2/.git" ]; then git clone -q "$1" "$root/$2"; fi
  git -C "$root/$2" fetch -q --depth 1 origin "$3" 2>/dev/null || git -C "$root/$2" fetch -q origin
  git -C "$root/$2" checkout -q "$3"
}

fetch https://github.com/saidsay-so/pjdfstest.git pjdfstest-rs "$pjdfstest_rev"
fetch https://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git xfstests "$xfstests_rev"

# The checkout lives below this workspace; keep cargo from treating it as a member.
manifest=$root/pjdfstest-rs/rust/Cargo.toml
grep -q '^\[workspace\]' "$manifest" || printf '\n[workspace]\n' >>"$manifest"
cargo build --release --manifest-path "$manifest"

# fsx needs only a handful of autoconf results; generate them instead of running configure.
{
  for m in ASSERT_H DIRENT_H ERRNO_H ERR_H LIBGEN_H LINUX_FALLOC_H COPY_FILE_RANGE MALLOC_H \
    STDLIB_H STRINGS_H STRING_H SYS_FCNTL_H SYS_IOCTL_H SYS_MMAN_H SYS_PARAM_H SYS_STATVFS_H \
    SYS_STAT_H SYS_TIME_H SYS_TYPES_H SYS_WAIT_H TIME_H UNISTD_H; do
    echo "#define HAVE_$m 1"
  done
  echo '#include <getopt.h>'
} >"$root/fsx-config/config.h"
gcc -o "$root/bin/fsx" -O2 -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 \
  -I"$root/fsx-config" -I"$root/xfstests/include" -I"$root/xfstests/src" \
  "$root/xfstests/ltp/fsx.c"

echo "pjdfstest: $root/pjdfstest-rs/rust/target/release/pjdfstest ($pjdfstest_rev)"
echo "fsx:       $root/bin/fsx (xfstests $xfstests_rev)"
