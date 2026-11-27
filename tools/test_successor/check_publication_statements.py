#!/usr/bin/env python3
"""Check the SQL statement count of a single file-payload publication batch.

Pass the ordinary compatibility matrix's Compose log with PostgreSQL
log_statement=all and bind parameter logging disabled. The comparison uses
the published v0.2.4 and adopted 0.2.5 volume before test-successor starts.
"""

import collections
import json
import re
import sys
from pathlib import Path

CONTAINER_LINE = re.compile(r"^postgres-1\s+\|\s+\S+ (.*)$")
POSTGRES_LINE = re.compile(r"^\S+ \S+ \S+ \[(\d+)\] ([A-Z]+):\s+(.*)$")
SQL = re.compile(r"^(?:statement:|execute [^:]+:)\s*(.*)$")
BUMP = "update metadata_state set version = version + 1"
RELEASES = ("0.2.4", "0.2.5")


def postgres_statements(lines):
    current = None
    for line in lines:
        container = CONTAINER_LINE.match(line)
        if not container:
            continue
        body = container[1]
        entry = POSTGRES_LINE.match(body)
        if entry:
            if current is not None:
                yield current
            statement = SQL.match(entry[3]) if entry[2] == "LOG" else None
            current = (entry[1], statement[1]) if statement else None
        elif current is not None and body.startswith((" ", "\t")):
            current = (current[0], f"{current[1]} {body.strip()}")
    if current is not None:
        yield current


def publication_statement_counts(lines):
    transactions = {}
    counts = {release: collections.Counter() for release in RELEASES}
    for pid, statement in postgres_statements(lines):
        sql = " ".join(statement.lower().split()).rstrip(";")
        if "create table if not exists studiofs_successor_probe" in sql:
            break
        if sql in ("begin", "start transaction"):
            transactions[pid] = ["begin"]
        elif sql == "rollback":
            transactions.pop(pid, None)
        elif pid in transactions:
            tx = transactions[pid]
            tx.append(sql)
            if sql != "commit":
                continue
            del transactions[pid]
            bumps = [
                row for row in tx
                if row.startswith(BUMP) or f", bumped as ({BUMP}" in row
            ]
            event_inserts = sum(row.startswith("insert into event_log") for row in tx)
            payload_publication = any("from snapshot_current_references" in row for row in tx)
            unrelated_writes = ("trash_entry_authority", "trash_publication_receipts", "acl_rules")
            if len(bumps) == event_inserts == 1 and payload_publication and not any(
                table in row for table in unrelated_writes for row in tx
            ):
                release = "0.2.5" if "publication_epoch" in bumps[0] else "0.2.4"
                counts[release][len(tx)] += 1
    return counts


def main():
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <compose.log>")
    with Path(sys.argv[1]).open(encoding="utf-8") as log:
        counts = publication_statement_counts(log)
    print(json.dumps(counts, indent=2))
    baseline, fenced = (counts[release] for release in RELEASES)
    if len(baseline) != 1 or len(fenced) != 1:
        sys.exit("expected one statement count per release for a comparable payload batch")
    extra = next(iter(fenced)) - next(iter(baseline))
    if extra > 0:
        sys.exit(f"publication adds {extra} SQL statement(s) per batch")


if __name__ == "__main__":
    main()
