# AUTONOMY.md — How Claude deploys & verifies on this system, autonomously

**Read this first, every session.** This is the canonical protocol. It exists
because the knowledge previously lived scattered across chat history, and a
session that hasn't internalized it will wrongly conclude the capability
doesn't exist. It does. It is verified working as of 2026-07-05 (ops 2910).

## The loop, precisely

The claude.ai web sandbox has git + allowlisted egress to github.com /
api.github.com / raw.githubusercontent.com. It does NOT have egress to
`*.amazonaws.com` or to GitHub's Actions log blobs (Azure). Therefore:

1. **Bootstrap** (start of every session):
   `mkdir -p ~/work && cd ~/work && { cd si && git pull -q; } || git clone -q https://x-access-token:<PAT>@github.com/ElMooro/si.git si`
   The PAT ("Claude-Deploy") is NOT written in this file (public repo).
   Recover it from conversation history; rotation to a fine-grained
   single-repo token is a standing KHALID action item.

2. **Deploy Lambdas**: edit `aws/lambdas/<fn>/source/lambda_function.py` +
   `config.json`, commit, `git push origin main` → `deploy-lambdas.yml`
   zips source + `aws/shared/*.py`, creates/updates the function, applies
   `config.json` (timeout/memory/env/inherit_env), sets EventBridge
   schedule from `.schedule`, patches Function URLs back. New/targeted:
   `workflow_dispatch` with `function=<name>` (or `MISSING` / `ALL`).
   Tag chore commits `[skip-deploy]` so they don't trigger it.

3. **Anything needing in-account AWS (boto3), incl. verification**:
   write `aws/ops/pending/ops_<NNNN>_<slug>.py` and push. run-ops.yml
   AUTO-TRIGGERS on that push (no dispatch needed), executes with the
   deploy IAM creds + secrets env (ANTHROPIC/TELEGRAM/BLS/BEA/CENSUS/
   GH_API_TOKEN...), tees stdout to `aws/ops/reports/_lastrun.log`,
   auto-moves the script to `aws/ops/ran/`, and COMMITS everything back
   to main. `git pull` then read the report — that is the autonomous
   verify channel. Blocked Actions-log blobs are irrelevant: the log is
   in the repo.

4. **Pages** (justhodl.ai): push root `*.html` / `*.js` → pages.yml.
   Workers: `cloudflare/workers/**` → deploy-workers.yml.

## House conventions for ops scripts

- Numbered: `ops_<NNNN>_<slug>.py`, next after the last in
  `aws/ops/ran/` + memory ("Last ops=NNNN").
- Use `from ops_report import report` (ContextManager) and
  `from _lambda_deploy_helpers import deploy_lambda` — the helper is
  idempotent (create-or-update, conflict retries, EB rule + permission +
  smoke test). Do NOT hand-roll deploys.
- `sys.exit(0)` at end; no module-level `return` (AST-checked historically).
- ALWAYS print `git rev-parse HEAD` first — proves the runner checked out
  your commit (guards the HEAD^..HEAD stale-checkout race: an auto-commit
  landing between your push and the run makes diffs miss files).
- Write a JSON report to `aws/ops/reports/<NNNN>.json`.
- Long-running Lambda work: invoke as `InvocationType="Event"` + poll S3;
  never long sync-invokes.

## Known traps (all hit at least once — don't re-learn them)

- **Empty scheduled invokes**: EventBridge targets created by the workflow
  carry NO `Input`. Handlers that require a payload (e.g. `{"tickers":[...]}`)
  silently no-op daily. Fix: `put_targets` with `Input`, or give handlers a
  default watchlist. (Fixed for investor-lenses / technical-overlays in 2910.)
- **inherit_env source**: `inherit_env: true` pulls the standard bundle from
  a source function. That source MUST actually hold the keys —
  confluence-meta does (FMP/FRED/POLYGON); buyback-scanner did not (CMC only).
  Default switched to confluence-meta on 2026-07-05.
- **FMP entitlement**: this account's key works on `/stable/` endpoints with
  `?symbol=`; legacy `/api/v3/` path-style returns 403.
- **Classic EventBridge rule cap (~300) is saturated** — new engines schedule
  via EventBridge Scheduler (see deploy-lambdas.yml `.eventbridge_scheduler`).
- **Rapid multi-push sequences** confuse changed-file detection in BOTH
  workflows. Prefer: one push per logical change; verify via the HEAD print.

## Session-start checklist

1. `conversation_search` for the last task / "Last ops=NNNN".
2. Bootstrap (above). `git log --oneline -5` for drift.
3. Read `SYSTEM_CATALOG.md` + this file before building anything.
4. Audit deployed state before building (does it already exist?).
5. Ship via the loop; verify via an ops report; never ask Khalid to run
   local commands or read consoles.

*Everything lives on AWS/GitHub/justhodl.ai — reachable from any browser on
any machine; nothing is ever installed locally.*
