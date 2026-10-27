"""Print the StudioFS E2E report summary and exit with the runner status."""

import json
import pathlib
import sys

artifact_dir = pathlib.Path(sys.argv[1])
status = int(sys.argv[2])
report_path = artifact_dir / "report.json"

print(f"artifact_path={artifact_dir}")
if not report_path.exists():
    print(f"report_missing={report_path}")
    sys.exit(status)

report = json.loads(report_path.read_text(encoding="utf-8"))
print(f"run_id={report.get('run_id')}")
print(f"status={report.get('status')}")
print(f"report={report_path}")

for scenario in report.get("scenarios", []):
    name = scenario.get("name")
    scenario_status = scenario.get("status")
    print(f"scenario={name} status={scenario_status}")
    error = scenario.get("error")
    if error:
        print(f"failed_invariant={error}")
    for artifact in scenario.get("artifacts", []):
        print(f"scenario_artifact={artifact}")

preflight = report.get("preflight", {})
if preflight.get("status") != "passed":
    for check in preflight.get("checks", []):
        if check.get("status") != "passed":
            print(f"preflight_failed={check.get('name')}: {check.get('detail')}")

sys.exit(status)
