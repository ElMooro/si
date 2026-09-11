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
| Grok / connector `push_files` (Contents API) | no — one file per write, ~40 KB bodies truncate | **multipart upload** or a tiny patcher (below) |

The staged-patcher path exists only for lanes without a shell. It is not the general push path.

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

## Large / complicated change without a shell — multipart upload (any size)

Upload the file the way S3 does a multipart upload:

```
aws/ops/patchers/parts/<upload-id>/manifest.json
   {"target": "aws/lambdas/justhodl-stock-buying/source/lambda_function.py",
    "parts": ["part-001", "part-002", "part-003"],
    "sha256": "<hex sha256 of the whole file>",      # best; or at least
    "bytes": 40564,                                    # exact byte count
    "note": "stock-buying v1.5.2 full source"}
aws/ops/patchers/parts/<upload-id>/part-001 …        # raw bytes, each ≤ 16 KB
```

Push the folder (any order, several commits are fine — an incomplete upload is
skipped, never failed). When every listed part exists the runner concatenates,
verifies sha256/bytes, compiles it if it is Python, writes the target, guards it,
commits, pushes, and dispatches `deploy-lambdas.yml` pinned to that commit.
Nothing is written to the target unless the check passes.

Targets allowed: `aws/lambdas/`, `aws/shared/`, `cloudflare/workers/`, root
`*.html|*.js|*.css`, `assets/`, `js/`, `css/`. Pages/workers get their own dispatch.

With a shell: `python3 scripts/split_parts.py <file> --target <repo path>` writes the folder.

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
