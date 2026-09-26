# Deploy lane — how a change reaches AWS (v2, 2026-09-11)

Push to `main` **is** the deploy. There is no laptop-side AWS step. Everything AWS
happens on the GitHub Actions runner, which holds the credentials.

## What runs, by path

| You changed | Workflow | Result |
|---|---|---|
| `aws/lambdas/<fn>/**`, `aws/shared/*.py` | `deploy-lambdas.yml` | preflight → zip → update function → **CodeSha256 proof** → receipt |
| `aws/ops/pending/*.py` | `run-ops.yml` | script runs with AWS creds; ledger + report committed |
| `cloudflare/workers/**` | `deploy-workers.yml` | worker deploy |
| root `*.html` / `*.js`, `assets/`, `js/`, `css/` | `pages.yml` | site publish |
| `aws/ops/patchers/**`, `aws/ops/staged/grok_*.py` | `apply-staged-large-files.yml` | runner assembles/patches, pushes, **dispatches a pinned deploy** |

`[skip-deploy]` skips pages **and** lambdas — never use it on page-only commits.
`[skip-ops]` skips the ops runner. `[shrink-ok]` permits a deliberate >50% source shrink.

## Which lane are you?

| Lane | Has a shell + git? | Write path |
|---|---|---|
| Khalid (Git Bash), Claude (sandbox), Codex desktop with a shell | yes | one atomic git commit + push; engine files above 40 KB need no splitting |
| Grok with a write-capable Contents API connection | no — one file per write, large request bodies can truncate | **batch upload v4** for anything multi-file, **multipart v3** for one large file, a tiny patcher for a surgical edit |
| OpenAI's read-only GitHub app | read-only | inspect files only; use shell git for writes |

The staged-patcher path exists only for lanes without a shell. It is not the general push path.
For the current ChatGPT batch improvements, push to the review branch, not `main`:

```sh
git push origin HEAD:refs/heads/chatgpt/batch-improvements
```

Git Credential Manager supplies the login when configured. Otherwise the owner sets
`GITHUB_TOKEN` securely in the runtime. Never paste credentials in chat. A branch push makes
the changes available for review; production deploys after merge to `main`. Do not retry
writes through OpenAI's read-only GitHub app: splitting requests cannot make it writable.

## Lane authorization (Khalid directive, 2026-09-17)

The **Grok lane and the ChatGPT/Codex lane are authorized for full write on every deploy
path** — engines (`aws/lambdas/**`), shared modules, ops scripts (`aws/ops/pending/`),
workers, pages — Grok through the "Grok (by xAI)" GitHub App (code / actions / workflows
read-write; commits as `ElMooro`), Codex through the standing deploy PAT (`repo` + `workflow`;
commits as `Codex`) or authenticated git. This standing grant does not establish which credential
is available in a particular runtime, nor make OpenAI's read-only GitHub app writable. The latest
user instruction controls the destination branch; the current batch improvements use
`chatgpt/batch-improvements` for review. A merge or authorized push to `main` deploys through
the runner. "AWS access" for any lane means the runner: an ops script
in `aws/ops/pending/` runs with the runner's IAM. No lane ever holds AWS keys, and none are
issued (audit 2026-09-08, Release A) — a key in a chat window is a regression, not a grant.

Standing rules the grant does not waive:
- read `aws/ops/ran/`, `aws/ops/pending/` and `STATE.md` before choosing an ops number — the
  bands interleave now (Grok 5582/5583/5610, Claude 5584–5588); `_preflight.py` refuses duplicates
- a bundled policy JSON (`aws/lambdas/<fn>/source/*.json` with a `config/` twin) must be
  byte-identical to its twin **in the same commit** (`tests/deployment/test_bundled_config_identity.py`)
- never a placeholder or partial source on `main` — a body over ~40 KB goes through the
  multipart upload below
- `.github/workflows/*` edits need the `workflow` OAuth scope; without it the Contents API
  refuses the write — hand the change to a shell lane rather than retrying
- a deploy is proven by the receipt (`data/ops/releases/<fn>.json`, commit == yours), not by a
  green run; ops reports land in `aws/ops/reports/latest/`

## Verify a push from any lane (GitHub side, then AWS side)

```
python3 scripts/verify_push.py <sha> --wait 20            # every run the commit caused, job/step verdicts
python3 scripts/verify_release.py <fn> --commit <sha> --data data/<engine>.json   # AWS runs those bytes; data fresh
git pull --rebase origin main && cat aws/ops/reports/latest/<N>_<slug>.md          # an ops script's own report
```

`verify_push.py` reads a token from `GITHUB_TOKEN` / `GH_TOKEN` / `JH_PAT` or `~/.jh_pat` and never
prints it; job logs are not needed (they sit on a host most sandboxes cannot reach). A **red
deploy writes its own report to main**: `aws/ops/reports/deploy-failures/<sha7>-<run_id>.md` —
the failing step (preflight or deploy), the error-shaped lines first, then the last 150 lines,
redacted. Read it, fix the cause, push again. Anything that
needs boto3 eyes on AWS is a **read-only** ops script: `aws/ops/STAGED/ops_<N>_<slug>.py` dispatched
through `run-ops-direct.yml` (`script=STAGED/ops_<N>_<slug>.py`) — it runs at once, commits its
report to `main`, and never re-runs on the serial lane (ops 5587/5588 are the pattern).

## Proof, not green checks

A green workflow proves the runner finished, not that AWS runs your bytes. The
deploy transaction now compares the live `CodeSha256` to the zip it built and
fails hard on mismatch, then publishes a receipt any lane can read over HTTPS:

```
https://justhodl.ai/data/ops/releases/<function>.json
python3 scripts/verify_release.py <function> --commit <sha> --data data/<engine>.json
```

The receipt carries the commit, run id, `CodeSha256`, zip bytes and every source
file's sha256. Compare `commit` to what you pushed; that is the proof.

For a migrated research producer, require its expected public contract as well:

```sh
python3 scripts/verify_release.py justhodl-fifx-vol-migration --commit <sha> --data data/fifx-vol.json --data-contract fifx-vol-research.v1
python3 scripts/replay_fifx_research.py
```

A fresh predecessor packet is not proof that the new producer has published.
`--data-contract` rejects a missing or different contract, even if the code receipt
matches and the predecessor is less than 26 hours old. Receipt/freshness/contract
checks do not replace complete original-source and compiler replay. When output
is pending, preserve the normal schedule and report that state explicitly.

## Several related files at once — batch upload v4 (the default for any multi-file change)

One file per write means an engine can go live before its helper or its `config.json` — every
direct write under `aws/lambdas/**` deploys immediately. A **batch** stages everything and deploys
nothing until its manifest says complete; then all targets land in **one commit** and **one pinned
deploy** covers every function touched (shared-module importers included, via the release order).

```
aws/ops/patchers/batch/<batch-id>/files/aws/lambdas/justhodl-x/config.json              small files whole (≤ 10 KB)
aws/ops/patchers/batch/<batch-id>/files/aws/lambdas/justhodl-x/source/fmp_client.py
aws/ops/patchers/batch/<batch-id>/parts/aws/lambdas/justhodl-x/source/lambda_function.py/part-001   large files in v3 parts
aws/ops/patchers/batch/<batch-id>/parts/aws/lambdas/justhodl-x/source/lambda_function.py/part-002   (@@PART n/N@@ … @@END n@@, ≤ 12 KB)
aws/ops/patchers/batch/<batch-id>/manifest.json      written LAST:
    {"complete": true, "lane": "grok", "note": "stock-buying v1.6: engine + FMP client + config",
     "files": [{"target": "aws/lambdas/justhodl-x/source/lambda_function.py", "first_line": "import json", "last_line": "    return out"},
               {"target": "aws/lambdas/justhodl-x/source/fmp_client.py"},
               {"target": "aws/lambdas/justhodl-x/config.json"}]}
```

- `<batch-id>`: `[A-Za-z0-9._-]`, 8–80 chars — use `<lane>-<YYYYMMDDTHHMMSSZ>-<slug>`; an id that
  has a successful assembly receipt is permanently reserved; rejected batches can be repaired in place
- `files` in the manifest is the contract: a listed target that was not uploaded, or an upload that
  is not listed, rejects the whole batch (typo protection)
- **all-or-nothing**: every file is checked (markers, compile/parse/document, first/last line,
  shrink guard) before the first byte is written; one bad file = nothing lands, and `STATUS.json`
  lists **every** problem so the next push fixes them all
- targets allowed: `aws/lambdas/`, `aws/shared/`, `config/`, `schemas/`, `docs/`,
  `cloudflare/workers/`, `assets/`, `js/`, `css/`, root `*.html|*.js|*.css`, and
  `aws/ops/pending/` — an ops gate script in the batch is dispatched on run-ops right after the
  deploy dispatch (so "ship the engine, then run its gate" is one batch)
- receipt: `aws/ops/patchers/batch/_receipts/<batch-id>.json` — per file (bytes, sha256, check,
  whole/parts), `functions` to be deployed, `config`/`pages`/`workers`/`ops_scripts` touched, the
  apply-lane run id, and `verify` (where the proof of the deploy will appear). `assembled` means
  written and committed — it is never proof of a deploy; the release receipt is
- a **rejected** batch is repaired in place: fix the file(s) STATUS.json names, push them under the
  **same** id, done. Only an assembled id is retired
- optional per-file `base_sha`: the `sha` you got when you GET the file you edited (the git blob id).
  If main moved since, the batch is rejected as stale instead of overwriting another lane's work
- a `config/<name>.json` and its bundled `aws/lambdas/*/source/<name>.json` twins must be identical
  after the batch (same rule as the deploy gate) — put every copy in the batch
- two complete batches in one run that write the same file: both are rejected as an overlap
- a batch that touches `aws/shared/` redeploys every importer of that module, not just the engines in the batch
- to abandon a batch (or a v3 upload): write `manifest.json` as `{"cancel": true, "note": "..."}` — one write;
  the folder is removed, nothing lands, the id stays free. Never leave a half-uploaded folder behind
- **part size is yours to probe**: 12 KB is the safe default, the runner accepts parts up to 40 KB, and
  a part the connector truncated is always caught (its `@@END n@@` is missing) — so try 24–30 KB
  parts once; whatever lands intact is your real per-write cap, and a 57 KB engine is then 2–3 parts
- while a batch is in flight, never write any of its targets directly — that is the race the batch exists to end
- lanes with a shell don't need this: put the whole change set in **one commit** and push

## Large / complicated change without a shell — multipart upload v3 (any size, no hashes)

The connector truncates a single write around 40 KB, so a large file goes up as **parts**, one
Contents-API write each, and the runner reassembles it. v3 (2026-09-17) needs nothing a model
cannot produce: no sha256, no byte count.

```
aws/ops/patchers/parts/<upload-id>/part-001      <= 12 KB, whole lines, marker lines first and last:
    @@PART 1/4@@
    import json
    ...
    @@END 1@@
aws/ops/patchers/parts/<upload-id>/part-002 … part-004      same shape (2/4 … 4/4)
aws/ops/patchers/parts/<upload-id>/manifest.json            written LAST — this is the go signal:
    {"target": "aws/lambdas/justhodl-x/source/lambda_function.py",
     "complete": true,
     "note": "stock-buying v1.6.0 full source",
     "first_line": "import json",                # optional but recommended: first non-empty line of the file
     "last_line": "    return out"}               # optional but recommended: last non-empty line of the file
```

Rules the runner enforces before it writes a single byte: part numbers contiguous from 001; every
`@@PART n/N@@` matches its file and the total; every part ends with its `@@END n@@` (a missing END
= the write was cut off); parts ≤ 40 KB; `first_line`/`last_line` match; `.py` compiles, `.json`
parses, `.js` passes `node --check`, `.html` is a whole document (`"fragment": true` for a partial);
a result under 50% of the existing file needs `"shrink_ok": true`. CRLF becomes LF; each part joins
on a line boundary; the file ends with a newline.

Both markers are required for every text part, even when its content happens to compile. A manifest
with `"complete": false` stays inactive even if it also lists part names. Raw `"join": "bytes"`
uploads retain the v2 requirement for a sha256 or exact byte count; no-hash/no-count uploads use
the marked text format above. The shell splitter writes LF on every platform so its parts stay
within the requested byte limit.

What comes back, on `main`:
- **assembled** → target written, parts folder removed, receipt at
  `aws/ops/patchers/parts/_receipts/<upload-id>.json` (bytes, sha256, check), then the deploy is
  dispatched pinned to that commit and the release receipt follows as usual
- **rejected** → nothing written; `aws/ops/patchers/parts/<upload-id>/STATUS.json` (and the same
  receipt) says exactly which part and why; fix that one part (or the manifest) and push it — the
  lane re-runs by itself, and the run goes red so nobody mistakes it for a landing
- parts still arriving → nothing happens until `manifest.json` says `"complete": true`

An `assembled` receipt proves assembly, not completed deployment. If the target bytes are unchanged,
no deploy is needed or dispatched. For changed engines, verify the apply run's resulting commit
against `data/ops/releases/<function>.json`; the part-upload or manifest commit is not that result commit.

Targets allowed: `aws/lambdas/`, `aws/shared/`, `cloudflare/workers/`, root `*.html|*.js|*.css`,
`assets/`, `js/`, `css/`. Pages/workers get their own dispatch. A v2 manifest (`"parts": [...]`,
`sha256`, `bytes`, `"join": "bytes"`) still works for exact/binary uploads.

With a shell: `python3 scripts/split_parts.py <file> --target <repo path>` writes the whole folder
(markers, manifest, sha256) — or just `git push`, the parts lane is for lanes without one.

## Batch validation, retries and shell preparation

Use the v4 batch layout above for all related files. The apply runner tests the upload code
before assembly. It verifies bundled JSON against every existing `config/` twin before writing;
upload every affected copy byte-identically in the same batch. Storage failures abort the
workflow before a source commit can be pushed. Publication is one Git commit; individual AWS
function updates still run through the existing deployment transaction.

A rejected batch can be repaired under the **same ID**: read `STATUS.json`, fix the named files
or parts, and push again. Successfully assembled batch IDs remain permanently reserved, and
their original receipts are preserved. Concurrent complete batches targeting the same file
are rejected rather than overwriting one another. Do not mix single-file uploads and batches
for the same target while a release is in flight.

Optionally copy the destination's `sha` from a GitHub Contents GET into its file entry's
`base_sha` (`base_blob_sha` is accepted as an alias). You do not calculate it. The runner rejects a replacement when that file has
since changed; read main again and merge before retrying. Explicit `null` means create only;
leaving it out preserves the original contract. Retrying identical bytes is harmless. The
apply workflow also refuses to rebase changes to files it already validated.

With a shell, prepare a JSON list such as
`[{"source":"work/new-engine.py","target":"aws/lambdas/justhodl-x/source/lambda_function.py"}, ...]`
and run `python3 scripts/split_parts.py --batch release-files.json`. Sources are relative to the
repository (absolute paths also work); entries may include `base_sha`, `shrink_ok` or
`fragment`. This generates the **same v4 batch format**, with marker parts at or below 12 KB,
optional exactness values computed for you, and the root manifest written last. Generated
IDs include a random suffix and part numbers beyond 999 are supported.

## One-command sender: prepare, upload every part, resume, verify

`split_parts.py` only prepares files. **`publish_batch.py` also sends them** using small,
sequential GitHub Contents API requests. An authenticated shell lane can publish a large
engine and its related files with one command; the network still carries multiple small writes.
Normal atomic `git push` is also suitable for shell lanes and has no connector's 40 KB limit.

Prepare a JSON list with the complete source/target pairs:

```json
[
  {"source": "work/new-engine.py", "target": "aws/lambdas/justhodl-x/source/lambda_function.py"},
  {"source": "work/helper.py", "target": "aws/lambdas/justhodl-x/source/helper.py"},
  {"source": "work/config.json", "target": "aws/lambdas/justhodl-x/config.json"}
]
```

Then run:

```sh
python3 scripts/publish_batch.py --spec release-files.json --lane codex --wait 20 \
  --verify justhodl-x=data/x.json
```

- Files up to 10 KB are staged whole. Larger files use parts at or below 12 KB. The sender
  computes optional exactness values; a no-shell model still need not compute any hash/count.
- Every payload is read back and compared byte-for-byte, and the full inventory is checked
  again before `manifest.json` is written last. Missing, changed or unexpected files block it.
- A transport timeout is followed by a read before retrying, so a successful write with a
  lost response is not duplicated. Writes are sequential and paced to avoid bursts.
- The sender prints a stable `--resume <folder>` command before contacting GitHub. Re-running
  it skips matching files already present and uploads the rest. It never reports an incomplete
  six-part upload as a completed release.
- Existing differing payloads or a changed submitted manifest are not overwritten by this
  automated client. Inspect `STATUS.json` and reconcile explicitly, or prepare a new unique
  batch ID. Manual repair of a rejected batch remains supported by the receiver.
- `--prepare-only` validates/prepares without network access. `--wait` follows the assembly
  receipt and handoff. Repeated `--verify FUNCTION=data/file.json` options run `verify_push.py`
  and `verify_release.py` against the exact `result_sha` for each requested engine. Only
  `requested_checks_verified` confirms those checks passed. Without `--verify`, success means
  transport/assembly/dispatch only; it never claims a completed AWS deployment.
- GitHub authorization uses the protected token lookup already documented above. HTTP 401/403
  stops the sender immediately; splitting files cannot repair an integration's permission grant.

A lane **without a shell and with a write-capable connector** uses the same loop: GET the
existing payload, write any missing part, GET it back, continue until all payloads match, then
write the final manifest and follow the receipts. These are multiple tool requests, not one
oversized `push_files` call. Do not end the task after landing just one of six required parts.

## Follow assembly to its actual source commit

1. Read `parts/_receipts/<upload-id>.json` or `batch/_receipts/<batch-id>.json` on main. It includes `changed`, every batch member's
   SHA-256, and `apply_run_id`, `handoff_ref`, `handoff_path` when assembled on Actions.
2. Read `handoff_path` on `handoff_ref` (`ops-evidence`). Its `result_sha` is the commit that
   contains the assembled source. Its status is `not_required`, `dispatched_unverified` or
   `dispatch_failed`. A missing handoff means the apply run has not published that evidence yet;
   inspect the apply run instead of assuming success.
3. For each changed Lambda, run `verify_push.py <result_sha> --wait 20`, then
   `verify_release.py <fn> --commit <result_sha> --data data/<engine>.json`.

The handoff never claims completed deployment. A Lambda dispatch must have a successful step
and a workflow run matching both the source SHA and the caller's unique request ID. A batch
touching shared modules redeploys their importers as well as explicitly changed engines. New
files are staged before validation/target selection so newly added helpers and engines count.
Site/worker dispatches report their step outcome and still need their own workflow verification.

Repository code cannot grant a connector additional GitHub permissions. OpenAI's GitHub app
is read-only in this environment; an HTTP 403 through it is not fixed by retries or smaller files.
The shell lane uses its configured GitHub credential; a write-capable API lane needs Contents
write (plus Workflows write for workflow edits). Keep credentials in the integration or its
protected runtime, never in parts, manifests, committed code, or chat. AWS access stays on Actions.

## No-shell lane loop (write-capable API only) — read, write, verify

1. **Read first**: `STATE.md` (`next_free_ops_number`), `DEPLOY_LANE.md`, the file you will change
   (`GET /repos/ElMooro/si/contents/<path>?ref=main` → content + `sha`; an update PUT needs that sha).
2. **Write**: one file per PUT to `main`. A single small file (≤ 10 KB): write the target directly.
   A single large file: multipart v3 (parts ≤ 12 KB, `manifest.json` last). **Two or more related
   files, any size: a batch** — never write them one by one to their real paths. Surgical edits: a patcher.
   An ops script: `aws/ops/pending/ops_<next_free>_<slug>.py` (the runner refuses a taken number —
   STAGED and report-only numbers count as taken).
3. **Verify** by reading files back, never by trusting a green run:
   - your commit's runs: `GET /repos/ElMooro/si/actions/runs?head_sha=<sha>` (push/dispatch events)
   - a Lambda: `https://justhodl.ai/data/ops/releases/<fn>.json` → `commit` must equal yours
   - an ops script: `aws/ops/reports/latest/<N>_<slug>.md` (run-ops commits it to main)
   - a multipart upload: `aws/ops/patchers/parts/_receipts/<upload-id>.json` or `parts/<upload-id>/STATUS.json`
   - a batch: `aws/ops/patchers/batch/_receipts/<batch-id>.json` or `batch/<batch-id>/STATUS.json`
   - a red deploy: `aws/ops/reports/deploy-failures/<sha7>-<run_id>.md`
   - the `ops-evidence` branch (`?ref=ops-evidence`) holds apply-lane and audit receipts
4. A `409`/`422` on PUT means another lane changed the file: GET it again, re-apply your change to
   the new content, PUT with the new sha. Never overwrite from a stale copy.

## Small surgical change without a shell — patcher

Add `aws/ops/patchers/<slug>.py` (legacy `aws/ops/staged/grok_<slug>.py` still works):
`read_text` / `replace` / `write_text`, idempotent (`already clean` if the needle
is gone). Applied patchers move to `aws/ops/patchers/applied/`. A patcher that raises is
**quarantined** to `aws/ops/patchers/failed/<name>.py` with `<name>.md` holding its error, its
partial edits are reverted, and the rest of the run (uploads, batches, other patchers) still lands;
push a corrected patcher under a new name. A patcher that exits 0 but changes **nothing** is recorded as
`aws/ops/patchers/applied/<name>.NOOP.md` — read it before believing a green run; "needle not found"
and "already applied" look the same from the outside (ops 5617 did the rename, ops 5620 was a no-op). Match the engine's real text — e.g. carry-surface defines
`def lambda_handler(event=None, context=None):`, so a marker without the defaults never matches. Never put a
patcher in `aws/ops/pending/` (that queue is serial AWS work) and never put an
ops script in `aws/ops/patchers/`.

## Why the old apply lane never deployed

A push made with `GITHUB_TOKEN` does **not** fire push-triggered workflows
(GitHub rule). The 2026-09-11 apac-flows apply (`d02838a`) landed on `main`
and produced zero `deploy-lambdas` runs. The v2 lane dispatches the deploy
explicitly with `expected_sha=<result commit>` and fails if no run appears.

## Ops queue (run-ops.yml)

GitHub keeps one pending run per concurrency group; a third push used to cancel
the queued run and its scripts silently never ran. `scripts/ops_queue.py` now
runs the push range **plus** any never-recorded pending script (7-day window;
`[skip-ops]` pushes, held scripts and the rollout plan are excluded). Every
execution is recorded in `aws/ops/reports/_ops_ledger.json` with its content
hash; a failed script is not retried until its content changes. Legacy scripts
that sat in `pending/` before the ledger existed are frozen and never auto-run.

## Stub / truncation guard

`scripts/guard_stub_lambdas.py` fails the SHA if any `lambda_function.py` is
under 500 bytes **or** lost more than half its size versus the push base while
previously ≥ 2 KB. `[shrink-ok]` in the commit message overrides the second rule.

## Audit receipts

Release/audit receipts are published to the `ops-evidence` branch by
`scripts/push_evidence.py`, never to `main` (they were 117 bot commits/day).

## Local shell (Claude / Khalid, Git Bash)

```
cd ~/work/si
git pull --rebase origin main
# edit aws/lambdas/<fn>/source/lambda_function.py
python -m py_compile aws/lambdas/<fn>/source/lambda_function.py
python scripts/guard_stub_lambdas.py
git add -A && git commit -m "<engine>: <what and why>"
git pull --rebase origin main && git push origin main
# then, once the run finishes:
python scripts/verify_release.py <fn> --commit $(git rev-parse HEAD)
```
