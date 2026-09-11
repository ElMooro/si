# Deploy lane — how a change reaches AWS

Push to `main` **is** the deploy. There is no separate AWS step from a laptop.

## What runs, by path

| You changed | Workflow | Result |
|---|---|---|
| `aws/lambdas/<name>/**` | `deploy-lambdas.yml` | zip + update live function |
| `aws/ops/pending/*.py` | `run-ops.yml` | script runs with AWS creds; report committed |
| `cloudflare/workers/**` | `deploy-workers.yml` | worker deploy |
| root `*.html` / `*.js` | `pages.yml` | site publish |
| `aws/ops/staged/grok_*.py` | `apply-staged-large-files.yml` | runner patches the **large** source, then the lambda deploy fires |

One pending-ops push at a time. `[skip-deploy]` skips pages **and** lambdas — do not use it on page-only commits. `[skip-ops]` skips the ops runner.

## Why agent lanes struggle with big files

GitHub **Contents API** (what Grok/connector `push_files` uses) cannot carry ~40 KB bodies. Writes come back as 9-byte or 441-byte stubs. AWS is fine; the blob on `main` is not.

**Do not** PUT `justhodl-stock-buying` (~40 KB) or other 30 KB+ sources through Contents API.

## Large / complicated change (the path we use now)

1. Keep the real engine file untouched on `main`.
2. Add a **tiny** patcher: `aws/ops/staged/grok_<engine>_<slug>.py`.
   - It must `read_text` / `replace` / `write_text` the large file.
   - It must be idempotent (`already clean` if the needle is gone).
   - It must **not** live in `aws/ops/pending/` (that queue is serial AWS work).
3. Push the patcher. The apply workflow checks out `main` on the runner (real git), patches, `py_compile`s, stub-guards (≥500 bytes), commits `apply staged large-file patch from runner`.
4. `deploy-lambdas.yml` sees the large source diff and deploys.
5. Proof is the op report / fresh `data/<engine>.json`, not a green check by itself.

Manual replay: Actions → **Apply staged large-file patches** → Run workflow → optional patcher filename.

## Small change (≲ ~18 KB after GET size check)

Direct Contents write is OK **if** you immediately GET `size` and cancel `deploy-lambdas` when the blob is short.

## Local shell (Claude / Khalid laptop)

```
cd ~/work/si
git pull --rebase origin main
# edit aws/lambdas/<name>/source/lambda_function.py
python3 -m py_compile aws/lambdas/<name>/source/lambda_function.py
python3 aws/ops/_preflight.py aws/lambdas/<name>/source
git add -A && git commit -m "<engine>: <what and why>"
git pull --rebase origin main && git push origin main
```

## Stub rule

`scripts/guard_stub_lambdas.py` fails the SHA if any tracked `lambda_function.py` is under 500 bytes. Real small engines (~1 KB) are allowed. Keep-alive placeholders are not.
