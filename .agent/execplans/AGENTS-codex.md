# Prepare StudioFS instructions for Codex development

This ExecPlan follows `.agent/PLANS.md`. It is a living record of the repository-instruction
audit and must keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and
`Outcomes & Retrospective` current.

## Purpose / Big Picture

After this change, GPT-6 Astra and GPT-5.6 can enter the repository, identify the
correct source of truth, trace the real Rust/runtime path, choose focused validation,
and report native or infrastructure limits without overstating acceptance. The
project-level `AGENTS.md` stays short enough to load reliably, while scoped
instructions appear automatically for the VFS, metadata, WinFSP, E2E, and cache areas.

## Progress

- [x] (2026-09-06) Audited the root instructions, repository docs, workspace
  architecture, Git state, and Codebase Memory coverage.
- [x] (2026-09-06) Compared the design with official OpenAI guidance for `AGENTS.md`,
  instruction precedence, prompt size, and model-specific behavior.
- [x] (2026-09-06) Rewrote the root `AGENTS.md` as a concise execution contract.
- [x] (2026-09-06) Added scoped `AGENTS.md` files for the high-risk crates.
- [x] (2026-09-06) Added the Codex workflow and source-of-truth links to the detailed
  agent guide.
- [x] (2026-09-06) Validated links, instruction size, Markdown structure, and the
  architecture vocabulary contract test.
- [x] (2026-09-06) Commit all intended changes as `docs: optimize Codex repository instructions`.

## Surprises & Discoveries

- The Codebase Memory project is indexed as `E-dev-studiofs-cli` with 24,660 nodes
  and 155,688 edges. The old root example used a different project name and would
  mislead an agent.
- Codebase Memory reports six parse-partial files and intentionally excludes
  generated, ignored, and review-artifact paths. Graph results need a coverage
  caveat and a direct `rg`/PowerShell fallback.
- The repository had no nested `AGENTS.md`, although the VFS, metadata, cache,
  WinFSP, and mounted E2E areas have materially different contracts.

## Decision Log

- Decision: Keep the root file high-signal and move detailed domain guidance into
  linked documentation and scoped instruction files. Rationale: Codex merges
  root-to-current-directory instructions and stops at its configured project-document
  byte limit; repetition increases truncation and conflict risk. Date/Author:
  2026-09-06, Codex.
- Decision: Use the official project name `E-dev-studiofs-cli` in graph examples.
  Rationale: it is the exact indexed key returned by Codebase Memory. Date/Author:
  2026-09-06, Codex.
- Decision: Reserve full workspace test suites for an explicit user request.
  Rationale: this is an existing repository policy and focused commands provide the
  appropriate local signal for an instruction-document change. Date/Author:
  2026-09-06, Codex.

## Outcomes & Retrospective

The root contract is 6,562 bytes; the root plus all five scoped files are about
13 KiB, below Codex's default 32 KiB project-document limit. Local Markdown links
resolve, `git diff --check` passes, and the focused architecture vocabulary test
passes (`1 passed`). No Rust workspace suite was run because the repository reserves
full suites for an explicit user request. The instruction changes were committed
as one documentation commit.

## Context and Orientation

StudioFS is a Rust 2024 workspace. `studiofs-vfs` is the filesystem core; it serves
foreground metadata from local `redb` and coordinates writer, cache, upload, and
invalidation state. `studiofs-meta` owns the local metadata engine and PostgreSQL
bootstrap/replication path. `studiofs-cache` owns local staging and pending uploads.
`studiofs-winfsp`, `studiofs-linux-fuse`, and `studiofs-macfuse` translate the VFS
contract to native filesystem APIs. `studiofs-e2e` runs real multi-client mounted
scenarios.

The canonical local-first contract is `docs/juicefs-local-first-rust-contract.md`.
System orientation is in `docs/architecture.md`; developer commands are in
`docs/development.md`; validation boundaries are in `docs/tests.md` and
`docs/contributing.md`; detailed Rust rules are in `docs/agents-coding-guide.md`.

## Plan of Work

Rewrite `AGENTS.md` around instruction precedence, task execution, graph-first
discovery, architecture invariants, focused validation, native-test boundaries, and
delivery hygiene. Add small scoped files in the five crates named above so a model
working from one of those directories receives the relevant lifecycle and test
contract automatically. Add a short Codex-oriented section to
`docs/agents-coding-guide.md` where it clarifies links and evidence rules; keep Rust
implementation detail there rather than duplicating it in the root file.

## Concrete Steps

Run from `E:\dev\studiofs-cli`:

    rg --files -g 'AGENTS.md' -g 'AGENTS.override.md'
    git diff --check
    git status --short

The instruction files must be present, contain no stale graph project name, and fit
comfortably below Codex's default project-document limit when loaded from a scoped
crate directory.

## Validation and Acceptance

The acceptance proof is documentation-focused: all linked files exist, all scoped
files are discovered from their crate directories, graph examples use the exact
indexed project, and no full Rust suite is required. Run Markdown/link checks that
are available locally, `git diff --check`, and the focused architecture vocabulary
test if its current contract covers these instruction terms. Do not treat a command
that reports `running 0 tests` as validation.

## Idempotence and Recovery

The edits are additive or textual and can be repeated safely with `apply_patch`. Do
not reset or checkout the worktree. Preserve unrelated user changes. If a link or
instruction assertion fails, correct the document rather than weakening the check.

## Artifacts and Notes

The durable artifacts are the root and scoped `AGENTS.md` files, the detailed agent
guide update, and this plan. No generated logs, `.env` contents, or native E2E output
belongs in the commit.

## Interfaces and Dependencies

The instruction contract depends on the repository's existing files and tools:
Codebase Memory project `E-dev-studiofs-cli`, `rg`/PowerShell for literal and
non-indexed text, `cargo` for focused Rust validation, `gh` for GitHub operations,
the configured Linear integration for issue operations, and Context7 for current
third-party library documentation.
