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
| Khalid (Git Bash), Claude (sandbox) | yes | normal git push — any file size, nothing special |
| Grok / ChatGPT connector (Contents API) | no — one file per write, ~40 KB bodies truncate | **multipart upload v3** (parts ≤ 12 KB, no hashes) or a tiny patcher (below) |

The staged-patcher path exists only for lanes without a shell. It is not the general push path.

## Lane authorization (Khalid directive, 2026-09-17)

The **Grok lane and the ChatGPT/Codex lane are authorized for full write on every deploy
path** — engines (`aws/lambdas/**`), shared modules, ops scripts (`aws/ops/pending/`),
workers, pages — Grok through the "Grok (by xAI)" GitHub App (code / actions / workflows
read-write; commits as `ElMooro`), Codex through the standing deploy PAT (`repo` + `workflow`;
commits as `Codex`), both straight to `main` (unprotected). That is the same authority Claude
holds; every push deploys through the runner exactly as for any lane. "AWS access" for any lane means the runner: an ops script
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

What comes back, on `main`:
- **assembled** → target written, parts folder removed, receipt at
  `aws/ops/patchers/parts/_receipts/<upload-id>.json` (bytes, sha256, check), then the deploy is
  dispatched pinned to that commit and the release receipt follows as usual
- **rejected** → nothing written; `aws/ops/patchers/parts/<upload-id>/STATUS.json` (and the same
  receipt) says exactly which part and why; fix that one part (or the manifest) and push it — the
  lane re-runs by itself, and the run goes red so nobody mistakes it for a landing
- parts still arriving → nothing happens until `manifest.json` says `"complete": true`

Targets allowed: `aws/lambdas/`, `aws/shared/`, `cloudflare/workers/`, root `*.html|*.js|*.css`,
`assets/`, `js/`, `css/`. Pages/workers get their own dispatch. A v2 manifest (`"parts": [...]`,
`sha256`, `bytes`, `"join": "bytes"`) still works for exact/binary uploads.

With a shell: `python3 scripts/split_parts.py <file> --target <repo path>` writes the whole folder
(markers, manifest, sha256) — or just `git push`, the parts lane is for lanes without one.

## No-shell lane loop (Grok, ChatGPT) — read, write, verify, all through the Contents API

1. **Read first**: `STATE.md` (`next_free_ops_number`), `DEPLOY_LANE.md`, the file you will change
   (`GET /repos/ElMooro/si/contents/<path>?ref=main` → content + `sha`; an update PUT needs that sha).
2. **Write**: one file per PUT to `main`. Under ~10 KB: write the target directly. Larger: the
   multipart upload above (parts ≤ 12 KB, `manifest.json` last). Surgical edits: a patcher.
   An ops script: `aws/ops/pending/ops_<next_free>_<slug>.py` (the runner refuses a taken number —
   STAGED and report-only numbers count as taken).
3. **Verify** by reading files back, never by trusting a green run:
   - your commit's runs: `GET /repos/ElMooro/si/actions/runs?head_sha=<sha>` (push/dispatch events)
   - a Lambda: `https://justhodl.ai/data/ops/releases/<fn>.json` → `commit` must equal yours
   - an ops script: `aws/ops/reports/latest/<N>_<slug>.md` (run-ops commits it to main)
   - a multipart upload: `aws/ops/patchers/parts/_receipts/<upload-id>.json` or `parts/<upload-id>/STATUS.json`
   - a red deploy: `aws/ops/reports/deploy-failures/<sha7>-<run_id>.md`
   - the `ops-evidence` branch (`?ref=ops-evidence`) holds apply-lane and audit receipts
4. A `409`/`422` on PUT means another lane changed the file: GET it again, re-apply your change to
   the new content, PUT with the new sha. Never overwrite from a stale copy.

## Small surgical change without a shell — patcher

Add `aws/ops/patchers/<slug>.py` (legacy `aws/ops/staged/grok_<slug>.py` still works):
`read_text` / `replace` / `write_text`, idempotent (`already clean` if the needle
is gone). Applied patchers move to `aws/ops/patchers/applied/`. Never put a
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
