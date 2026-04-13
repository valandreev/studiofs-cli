# Diagnostics Workflows

## `aja_hybrid_v1`

`aja_hybrid_v1` is the first built-in hybrid investigation workflow for live
StudioFS attach sessions. It is designed for workloads that combine bounded
automation with a manual operator-driven phase, such as AJA setup and execution.

### What the workflow does

The workflow currently advances through these phases:

1. Capture a bounded attach snapshot artifact.
2. Start a balanced wall-blocking profiling window.
3. Pause for the manual AJA/operator phase.
4. Resume after explicit operator acknowledgement.
5. Stop the active profiling window.
6. Materialize a single-run investigation report.

The manual phase records marker boundaries as `AJA workload start` and
`AJA workload done`, so later reports and comparisons can line up with the real
operator-controlled span.

### Safety model

The workflow engine does not accept arbitrary shell commands.

It only runs allowlisted tools that are registered in the diagnostics host. The
first workflow uses:

- a bounded internal attach-snapshot helper
- bounded diagnostics profiling control
- built-in report materialization

Workflow-managed artifacts are written under the mount cache path in
`workflow-artifacts/<workflow-run-id>/`.

### Hidden CLI helpers

The hidden internal CLI mirrors the live diagnostics workflow surface for local
validation and operator-driven use:

```text
studiofs internal diagnostics-list-workflows <mount-point>
studiofs internal diagnostics-start-workflow <mount-point> --lease-id <lease> --run-id <run> --workflow-id aja_hybrid_v1
studiofs internal diagnostics-get-workflow <mount-point> --lease-id <lease> --workflow-run-id <workflow-run>
studiofs internal diagnostics-advance-workflow <mount-point> --lease-id <lease> --workflow-run-id <workflow-run> --step-id <step>
studiofs internal diagnostics-ack-workflow-step <mount-point> --lease-id <lease> --workflow-run-id <workflow-run> --step-id run_manual_aja_phase --actor <operator>
studiofs internal diagnostics-abort-workflow <mount-point> --lease-id <lease> --workflow-run-id <workflow-run>
studiofs internal diagnostics-complete-workflow <mount-point> --lease-id <lease> --workflow-run-id <workflow-run>
```

### Recommended operator loop

1. Attach to the live mount and acquire a controller lease.
2. Create or reuse an investigation case and run.
3. Start `aja_hybrid_v1`.
4. Advance the automation steps until the workflow enters the manual operator
   phase.
5. Perform the AJA-controlled workload.
6. Acknowledge the manual step when the operator phase completes.
7. Advance the remaining profiling/report steps.
8. Review the generated report and workflow artifacts before deciding on
   comparison or hypothesis updates.

### Validation expectation

The scriptable portions of the workflow are covered by focused Rust tests. The
manual operator path still requires a human validation pass on a real workload
before the slice can be considered fully validated.
