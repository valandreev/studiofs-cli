#!/usr/bin/env python3
"""Classify run-pjdfstest-per-case.sh results into SFS-906 buckets and print a Markdown table.

Usage: classify-pjdfstest.py <run-dir>/pjdfstest-studiofs-per-case/results.txt
"""
import collections
import re
import sys

SPECIAL = re.compile(r"::(socket|char|block|fifo)$")
INODE_CTIME_FAILURES = {
    "rename::to_multiply_linked::regular",
    "rename::changed_ctime_success::symlink",
    "unlink::update_ctime_success::regular",
}


def bucket(name, verdict, detail):
    if verdict != "FAILED":
        return None
    group = name.split("::")[0]
    if "EOPNOTSUPP" in detail or SPECIAL.search(name) or (
        group in ("mkfifo", "mknod") and ("mknod(" in detail or "f(&path" in detail)
    ):
        return "B2 special files"
    if "uid_gid_eq_euid_egid" in name:
        return "B2 root owner"
    if name in INODE_CTIME_FAILURES:
        return "B1 SFS-923 inode ctime"
    if "ENAMETOOLONG" in detail:
        return "B1 SFS-912"
    if name in ("mkdir::changed_time_fields_success", "rename::updates_link_parent"):
        return "B1 SFS-913"
    if "ENOENT" in detail or "NotFound" in detail or "symlink(" in detail or "link(" in detail:
        return "B1 SFS-911"
    return "UNCLASSIFIED"


def main(path):
    rows = [line.rstrip("\n").split("\t") for line in open(path)]
    per_group = collections.OrderedDict()
    buckets = collections.Counter()
    unclassified = []
    degraded = []
    for name, verdict, _rc, deg, *rest in rows:
        detail = rest[0] if rest else ""
        group = name.split("::")[0]
        g = per_group.setdefault(group, collections.Counter())
        g[verdict] += 1
        b = bucket(name, verdict, detail)
        if b:
            g[b] += 1
            buckets[b] += 1
            if b == "UNCLASSIFIED":
                unclassified.append((name, detail))
        if deg == "degraded=1":
            degraded.append(name)
    cols = ["B1 SFS-911", "B1 SFS-912", "B1 SFS-913", "B1 SFS-923 inode ctime",
            "B2 special files", "B2 root owner"]
    print("| group | total | ok | skipped | failed | " + " | ".join(cols) + " |")
    print("|---|---:|---:|---:|---:|" + "---:|" * len(cols))
    tot = collections.Counter()
    for group, c in per_group.items():
        total = c["ok"] + c["skipped"] + c["FAILED"]
        tot.update(c)
        tot["total"] += total
        print(f"| `{group}` | {total} | {c['ok']} | {c['skipped']} | {c['FAILED']} | "
              + " | ".join(str(c[k]) for k in cols) + " |")
    print(f"| **all** | **{tot['total']}** | **{tot['ok']}** | **{tot['skipped']}** | **{tot['FAILED']}** | "
          + " | ".join(f"**{tot[k]}**" for k in cols) + " |")
    print()
    print("buckets:", dict(buckets))
    print("degraded:", degraded)
    for name, detail in unclassified:
        print("UNCLASSIFIED", name, detail)


if __name__ == "__main__":
    main(sys.argv[1])
