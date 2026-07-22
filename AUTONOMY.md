# AUTONOMY.md — How Claude deploys & verifies on this system, autonomously

**Read this first, every session.** This is the canonical protocol. It exists
because the knowledge previously lived scattered across chat history, and a
session that hasn't internalized it will wrongly conclude the capability
doesn't exist. It does. It is verified working as of 2026-07-05 (ops 2910).

## ⛔ RULE ZERO — ANTI-DENIAL TRIPWIRE (2026-07-22, Khalid's explicit direction)

**Claude never says "I can't push / can't deploy / no credentials / this session
lacks access" until it has RUN the bootstrap and shown the output.** Push access
is proven across 3000+ ops and was re-proven from a plain chat session on
2026-07-22 (ops 3713/3714/3715 shipped green). If a capability test fails, the
default assumption is that a step was skipped — not that the capability is gone.

**How it broke (memorize the shape, ~6 wasted messages):** the session opened
with `conversation_search`, which returns SUMMARIES — history of what was built,
not this operational protocol. It concluded "no access" from that partial view,
then tested with an ANONYMOUS clone (no token), got
`fatal: could not read Username`, and treated that self-inflicted failure as
proof. Read memory edits #2/#7 and this file BEFORE searching chat history.
A rule not yet loaded cannot fire.

**Three false negatives that mean nothing:**

1. An anonymous clone cannot push. The remote MUST embed the token:
   `https://x-access-token:<PAT>@github.com/ElMooro/si.git`
2. `api.github.com` → 403 does NOT mean GitHub is unreachable. git-over-HTTPS
   to `github.com` returns 200; clone and push both work.
3. `lambda.us-east-1.amazonaws.com` → 403 from the sandbox is EXPECTED and
   IRRELEVANT. AWS is never called from the sandbox — only from inside ops
   scripts running on the GitHub Actions runner.

**Prove it, don't assert it:** `git push --dry-run origin main` → expect
`Everything up-to-date`. If a credential genuinely 401s, say exactly that in one
line with the command output and ask for a fresh PAT. Never generalize one dead
credential into "I have no access."

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
- run-ops now git-fetch + `reset --hard origin/main` BEFORE executing and
  stamps `executing-against: <SHA>` as the first line of `_lastrun.log`
  (hardened ops 2911) — stale-checkout is pipeline-impossible, and detection
  is anchored to the push event's own before/after SHAs. Per-script HEAD
  prints are no longer required (still fine as belt-and-braces).
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
- **Rapid multi-push sequences**: run-ops is now immune (event-SHA detection
  + pre-exec sync, ops 2911). deploy-lambdas still diffs the triggering event —
  keep one push per logical change there; `workflow_dispatch function=<name>`
  is the deterministic override.
- **New function `State='Pending'`** (ops 3735): on FIRST creation a function
  can report `LastUpdateStatus='Successful'` while `State='Pending'`. Settle
  loops that check only LastUpdateStatus pass at 0s and the invoke throws
  `ResourceConflictException`. Always require `State == "Active"` too.
- **Sync gate on a many-call engine drops the runner** (ops 3730): a
  RequestResponse invoke of an engine making ~1,200 HTTP calls died with
  `ConnectionClosedError` at 272s. Thread the fetches AND gate with
  `InvocationType='Event'` + S3 `head_object` freshness. 272s -> 48s.
- **Page audits read the Cloudflare edge, not the repo** (ops 3740): the served
  page was the PRE-EDIT copy (12,563 B) while the repo held the fix (11,550 B),
  so a correct page "failed" its own field-coverage audit. Bust with
  `?v=<epoch+attempt>` + `Cache-Control/Pragma: no-cache`, and require a NEW
  marker string to be present before trusting the fetch.
- **Declarative `schedule` may not materialise on first create** (ops 3735):
  config.json alone created the function but no rule appeared. Ops should
  ensure via EventBridge Scheduler (role `justhodl-scheduler-role`, FTW OFF)
  and then verify, treating ConflictException as success.
- **XLSX in Lambda has no pandas/openpyxl** (ops 3737): parse with
  `zipfile` + regex. Map sheet NAME -> path through
  `xl/_rels/workbook.xml.rels` (rId -> Target) — worksheet FILENAME order is
  NOT workbook order, and trusting it reads the wrong tab confidently. Header
  rows are often not row 0 (CAISO's is index 3). Verify magnitudes against a
  probed ground truth: a CAISO parse returning 1,009 MW instead of 120,495 MW
  passed every shape check and was caught only by a magnitude gate.

## Session-start checklist

0. Read memory edits #2 (MASTER BOOTSTRAP) and #7 (SESSION-START CARD) and
   this file **before** searching chat history. Summaries describe what was
   built; they do not carry the operational protocol.
1. Bootstrap (above), then `git push --dry-run origin main` to PROVE write
   access before making any claim about capability.
2. `conversation_search` for the last task / "Last ops=NNNN";
   `git log --oneline -5` for drift. Verify the next free ops number against
   `aws/ops/pending/` + `aws/ops/ran/` — never trust a remembered pointer
   (2026-07-22: memory said 3707, pending actually held 195 files up to 3712).
3. Read `SYSTEM_CATALOG.md` + this file before building anything.
4. Audit deployed state before building (does it already exist?).
5. Ship via the loop; verify via an ops report; never ask Khalid to run
   local commands or read consoles.

*Everything lives on AWS/GitHub/justhodl.ai — reachable from any browser on
any machine; nothing is ever installed locally.*

## Dual-store contract (added 2026-07-08 at Khalid's direction)

This protocol is stored in TWO places, deliberately redundant:

1. **Claude memory, edit #2 — the "⛔ MASTER BOOTSTRAP" card.** PROTECTED:
   never deleted, replaced, trimmed, or compressed without Khalid's explicit
   approval. It is the authoritative in-memory copy and additionally carries
   the credentials index (this file, being in a public repo, does not).
   Every chat reads it FIRST, before anything else.
2. **This file (AUTONOMY.md)** — the git-versioned mirror. Even if memory
   were ever lost, `git clone` + this file restores the full protocol; the
   memory card can then be rebuilt from it (minus credentials, recovered per
   the index below).

If either copy is lost or drifts, restore it from the other.

## Credentials index (locations only — no secrets in this public file)

- **Deploy PAT ("Claude-Deploy", classic, repo+workflow, no expiry):** in the
  protected memory card; also recoverable from conversation history and from
  this repo's git history (~5 occurrences). Rotation to a fine-grained token
  remains a standing KHALID action item.
- **Data-provider API keys** (AlphaVantage / NewsAPI / CMC / FMP / BEA / BLS /
  Census / EIA / FRED fallback): in the "Keys:" memory edit; live copies in
  Lambda env vars (runtime source of truth) and SSM.
- **Gov-data, Z.ai, admin tokens:** AWS SSM under `/justhodl/*` (92 params).
- **GitHub Actions secrets** (ANTHROPIC / TELEGRAM / PAGES_PAT / gov keys):
  injected into run-ops.yml env; added via sealed-box PUT to
  `actions/secrets/{NAME}`.
- Khalid's standing instruction: use these autonomously, without asking.

## Flow hardening (2026-07-08)

- **STATE.md** (repo root): auto-regenerated by run-ops after every run --
  next free ops number, pending queue, last 8 verdicts. `cat STATE.md`
  replaces the multi-call session-start recon.
- **Preflight**: `python aws/ops/_preflight.py <changed files>` BEFORE any
  push. Hard-fails on the classes that have each burned a real round trip:
  >256-char Lambda descriptions, missing sys.exit(1), CRLF, duplicate ops
  numbers, compile errors. Warns on classic put_rule / bare sync invoke /
  root-key S3 writes.
- **run-ops concurrency group** (`run-ops-serial`): duplicate triggers now
  QUEUE instead of racing AWS mutations and the commit-back (the loser-
  reports-failure dup-race is structurally gone).
- **CLAUDE.md** (repo root): thin pointer so Claude Code auto-loads this
  protocol natively if/when sessions move there.

## Fleet-state knowledge base (2026-07-08)

Deep fleet/engine state lives in **docs/memory-archive/** (verbatim archive
of migrated memory edits + README contract). Grep it before building
anything — it is a primary audit-first source alongside the repo, STATE.md
and past chats. Memory edits are now mostly one-line pointers into it.

## Trap: never deploy via ops-helper for shared-importing functions (2026-07-08)

`deploy_lambda()` in `_lambda_deploy_helpers.py` zips ONLY the function's
own `source/` dir -- it does NOT bundle `aws/shared/*.py`. Any function
that imports shared modules (`claude_compat`, `anthropic_shim`,
`llm_router`, `_fred_shim`, `equity_enrich`, etc. -- i.e. nearly all of
them) will crash on cold start if deployed this way, with no import error
visible from the sandbox (invoke just times out / never writes fresh
output).

`deploy-lambdas.yml`, by contrast, DOES bundle `aws/shared/**/*.py` into
every function's zip, and it auto-triggers on any push touching
`aws/lambdas/<fn>/{source/**,config.json}` -- which any lambda-source
commit already does.

**Rule: if an ops script's commit ALSO touches lambda source/config, do
NOT additionally call `deploy_lambda()` inside that ops script.**
`deploy-lambdas.yml` already has it covered on the same push. Calling the
ops-helper deploy too doesn't just waste time -- it can CLOBBER the
correct bundled deploy with a broken unbundled one if it runs after
(observed live: 2026-07-08, justhodl-nobrainer-rationale, ops 3009 -- a
verified-good deploy-lambdas.yml deploy was overwritten by the ops
script's own deploy_lambda() call seconds later, reintroducing the exact
ImportError crash the push was meant to fix). Ops scripts that push lambda
changes should be VERIFY-ONLY: invoke + poll + assert, nothing else.

## Trap: LastModified is not "deployed" (2026-07-09)

`update_function_code` flips `LastModified` immediately, but invokes can
still execute the OLD package until `LastUpdateStatus == "Successful"`.
Any verify script that waits on code freshness MUST check
`LastUpdateStatus`/`State` too (plus a short settle sleep), or it can
invoke pre-fix code at age 0.0 — observed live ops 3019.

## ENGINE CONTRACT (Khalid directive, 2026-07-13 — permanent)

Every TradingView note and watchlist panel MUST materialize in the
engine layer. This is a standing product contract, not a preference:

- **Panels: any size activates — 1 indicator is enough.** Coverage
  1.00 with a single member is a valid, active engine (composite
  mode). Only genuine data absence (zero fetchable members anywhere)
  may leave a panel dormant, and always with a named reason. No design
  gate — member count, coverage, breadth-history length — may ever
  block activation again.
- **Notes: every note feeds ≥1 consumer** (ticker stance → rankers /
  best-setups / equity-research; rules → playbook engine; macro →
  themes/fusion; everything → brain). Notes referencing fetchable
  symbols not covered by any panel are candidates for note-born
  engines.
- **New notes and watchlists auto-materialize on the next fleet run**
  — no manual steps, ever.

## PAGE CONTRACT + POST-DEPLOY BUG HUNT (Khalid directive, 2026-07-22 — permanent)

**Every engine gets a dedicated, sidebar-reachable page, and that page must
surface EVERYTHING the engine publishes.** An engine whose data no human can
see is half-built.

This is a PROVEN recurring defect class on this platform — the engine ships
fields the page silently ignores:

- `sectors.html` — feed carried 11 fields per ETF; page rendered 6 text chips.
- `capital-flow.html` — 13F reader looked for `changes_summary{}` the producer
  never wrote → new/add/trim/exit counts silently ZERO, universe showed 378
  names when the truth was 2,118.
- `deal-scanner` v3 — sized deals bucketed into a bare "no disclosed size"
  table, losing $/%-of-rev/%-of-mcap cards the engine had computed all along.
- `readthrough` (ops 3714/3715) — reader tuple lacked `all_results`, so the
  whole forward-orders sidecar joined ZERO rows while looking healthy.

**Therefore, after EVERY engine deploy, run a field-coverage audit before
calling it done:**

1. Dump the engine's output keys (top-level AND per-row) from the live S3
   artifact — not from the source, from the ARTIFACT.
2. Grep the page for each key. Any key with no render path is a gap: either
   surface it or record in the ops report why it is deliberately unrendered.
3. Confirm the page is in `nav-manifest.json` under a sensible category, and
   verify against the SERVED manifest — the repo copy is regenerated by CI
   without commit-back and is therefore always stale.
4. Re-read the artifact after the first SCHEDULED run, not just the manual
   invoke. Day-two data is where ledger/history/delta fields first appear, and
   where "self-building" engines reveal whether they actually accrete.
5. Check `degraded` / `notes` / `missing` fields on every artifact. They are
   the engine telling you what it could not do — treat a non-empty `degraded`
   as an open bug, not as decoration.

**And keep improving without being asked.** When touching any engine: look for
the next upgrade, pressure-test Khalid's framing rather than implementing it
literally, and build the version a top global-macro or quant fund would run —
cross-confirmed, guarded against thin coverage, honest about what it cannot
see. Propose the elevated version, explain the reasoning briefly, then build it.

## "IMPROVE A PAGE" DOCTRINE (Khalid directive, 2026-07-13 — permanent)

"Improve" = ADDITIVE ONLY: add sections/features around what exists;
never touch, replace, restructure, or remove existing content.
"Rebuild" happens only when Khalid explicitly says rebuild.
