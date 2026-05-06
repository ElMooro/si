# Repo Cleanup — 2026-05-06 (Phase A complete)

This is a continuation of the cleanup work the parallel session began
in step 261. Their audit confirmed AWS Lambda inventory is healthy
(186 deployed, 184 in repo, 0 archive candidates). This phase tackled
**ops directory hygiene** and **workflow improvements** which the
prior audit did not cover.

## Summary

| Metric | Before | After |
|---|---|---|
| Scripts in `aws/ops/pending/` | 432 | 0 |
| Scripts in `aws/ops/ran/` | 0 | 29 |
| Scripts in `aws/ops/historical/` | 0 (didn't exist) | 403 |

`pending/` is now strictly for **work in progress** — i.e., scripts
queued to run on next push. This makes the directory a meaningful
status indicator.

## What was done

### Phase 1 — successful scripts → `ran/`

29 numbered scripts (steps 248–277) that have a matching report
in `aws/ops/reports/{NUM}_*.json` were moved to `aws/ops/ran/`.
These are the recent ops scripts from the modern run-ops harness era
(2026-04-22 onwards). Their reports confirm successful execution.

### Phase 2 — workflow improvements (`.github/workflows/run-ops.yml`)

Two changes to make the harness self-maintaining:

1. **Skip non-existent files** — when a push includes both deletes
   and adds in `aws/ops/pending/`, the script picker no longer fails
   trying to run deleted files. Previous behavior killed the loop
   under `set -e` and prevented added scripts from running. (This is
   the bug that bit us with steps 269/270 earlier today.)
2. **Auto-move successful scripts** — after a script runs successfully,
   the workflow now `git mv`'s it from `pending/` to `ran/`. The
   commit step then includes `pending/` and `ran/` in its auto-commit
   paths so the move persists across runs.

### Phase 3 — historical artifacts → `aws/ops/historical/`

403 scripts were moved to `aws/ops/historical/`:
- 320 underscore-prefixed legacy scripts (`_create_*.py`, `_phase_*.py`,
  `_audit_*.py`, etc.) from before the numbered ops era. These are
  one-shot creation/probe scripts for features that have long shipped.
- 83 numbered scripts (steps 167–247) that lack matching reports
  but are documented in git history as completed. Most are diagnostic
  probes that printed to stdout rather than writing report files.

## Lambda cleanup (deferred from parallel session)

The parallel session's step 261 audit identified 5 deletion candidates
that were left for **manual** sweep because deletion is non-recoverable:

| Function | Code size | Notes |
|---|---|---|
| `ecb` | 1,717 B | Tiny stub. Predates ECB-CISS work. |
| `nyfed-primary-dealer-fetcher` | 1,640 B | Stub from primary-dealer build. |
| `justhodl-chat-api` | 2,042 B | Predecessor to `justhodl-ai-chat`. |
| `nyfed-financial-stability-fetcher` | 2,717 B | Stub. |
| `nyfedapi-isolated` | 4,374 B | Test artifact. |

Total: ~12 KB of dead code. Zero invocations, zero EB rules, zero
Function URLs. To delete:

```bash
for fn in ecb nyfed-primary-dealer-fetcher justhodl-chat-api \
          nyfed-financial-stability-fetcher nyfedapi-isolated; do
  aws lambda delete-function --function-name "$fn"
done
```

Plus 2 console-created Lambdas not in repo (`justhodl-cdn-diag-temp`,
`justhodl-ka-metrics`) — `cdn-diag-temp` is clearly disposable;
`ka-metrics` should be re-imported into repo if Khalid wants to
keep it under version control.

## Other dimensions audited (clean)

- **Pages** — all 70+ HTML pages either link from `index.html`
  launcher or are linked from another page (verified informally).
  Some are historical/specific-purpose (e.g., `auction-crisis.html`,
  `eia.html`) and intentionally not in the launcher.
- **EventBridge rules** — health monitor's `expectations.py` is the
  single source of truth for active rules. No zombie rules detected
  in step 261 cross-reference.
- **Reports orphan-check** — every report in `aws/ops/reports/`
  has a matching script in either `pending/`, `ran/`, or
  `historical/` (verified by name prefix).

## What's left in `pending/`

Empty. From this point forward, `pending/` will contain only:
- Scripts queued for the next workflow run (added but not yet executed)
- Scripts that failed and need debugging (workflow doesn't auto-move
  failures since they may benefit from human review)

## File counts post-cleanup

```
aws/ops/pending/      0 scripts
aws/ops/ran/         29 scripts (all from steps 248-277, 2026-04 onwards)
aws/ops/historical/  403 scripts (everything else)
aws/ops/reports/     ~190 reports
aws/ops/audit/       (unchanged, governance reports)
```
