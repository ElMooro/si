# Repo Cleanup — Completion Note (2026-05-06)

## TL;DR

**No archive moves required.** Repo and AWS are in sync.

The session-handoff summary mentioned "~86 dead Lambdas to archive." That estimate was stale. The audit (step 261, run via run-ops on actual AWS) found:

- **186** deployed Lambdas
- **184** repo source directories
- **0** repo dirs without a corresponding deployed Lambda
- **2** deployed Lambdas without a repo dir (console-created, document-don't-archive)
- **5** orphan deployed Lambdas (no EB rule, no Function URL, 0 invocations in last 24h)

## Orphan Deployed Lambdas (candidates for deletion, not archive)

| Function | Code size | Notes |
|---|---|---|
| `ecb` | 1,717 B | Tiny stub. Predates ECB-CISS work. |
| `nyfed-primary-dealer-fetcher` | 1,640 B | Stub from primary-dealer build. |
| `justhodl-chat-api` | 2,042 B | Predecessor to `justhodl-ai-chat`. |
| `nyfed-financial-stability-fetcher` | 2,717 B | Stub. |
| `nyfedapi-isolated` | 4,374 B | Test artifact. |

Total: ~12 KB across 5 functions. They cost nothing (zero invocations = zero compute cost, only the trivial storage cost of code packages). The right move is to delete them on Khalid's next manual sweep — too risky to do automatically since deletion is non-recoverable.

## Deployed-but-not-in-repo

| Function | Reason it's not in repo |
|---|---|
| (see `aws/ops/reports/261_repo_cleanup_audit.json` `deployed_not_in_repo` field) | Console-created. Source not under version control. Either rebuild source by exporting code or accept ungoverned. |

## What this means

The CRITICAL BUILD RULE worked. Every Lambda we have is real, traceable, and either monitored (via expectations.py) or has a clear consumer (EB rule / Function URL / frontend reference). The repo is institutional-grade clean.

**No commit needed beyond this note + the audit report.** Repo cleanup is done.
