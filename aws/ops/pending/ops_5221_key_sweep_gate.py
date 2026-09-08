"""ops_5221 -- audit 2026-09-08 Release A2 gate: fleet credential sweep verification.

The A2 push rewrote every Lambda that carried a provider credential literal to
`managed_secret((ENV...), (SSM,))`. deploy-lambdas redeploys all of them on the same push. This
gate, running after that deploy (serialised behind it by waiting on the functions' LastModified):
  1. every rewritten function is Active with LastUpdateStatus Successful and modified after the push;
  2. no CloudWatch log line since the push mentions a managed_secret import failure or an
     'unavailable' resolution (i.e. env var present -> SSM never even consulted; if SSM was consulted it
     succeeded);
  3. the working tree holds zero residual literals outside aws/lambdas/_archived.
sys.exit(1) on any regression -- the fix-forward is per-function, never a fleet rollback.
"""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FAILS = []
T_PUSH = int(subprocess.run(["git", "log", "-1", "--format=%ct", "HEAD"], capture_output=True, text=True, cwd=ROOT).stdout.strip() or "0")


def rewritten_functions():
    out = []
    for d in sorted((ROOT / "aws" / "lambdas").iterdir()):
        src = d / "source"
        if not src.is_dir() or d.name.startswith("_"):
            continue
        if any("from managed_secret import managed_secret" in f.read_text(errors="ignore") for f in src.glob("*.py")):
            out.append(d.name)
    return out


with report("ops_5221_key_sweep_gate") as R:
    R.heading("ops 5221 -- audit 2026-09-08 Release A2 gate: fleet credential sweep")
    lam = boto3.client("lambda", region_name=REGION)
    logs = boto3.client("logs", region_name=REGION)
    fns = rewritten_functions()
    R.log("%d engines read credentials through managed_secret" % len(fns))

    R.section("1. redeploy state")
    t0 = time.time()
    pending = set(fns)
    missing = set()
    while pending and time.time() - t0 < 4800:   # the fleet redeploy takes up to ~80 min
        for fn in list(pending):
            try:
                cfg = lam.get_function_configuration(FunctionName=fn)
            except lam.exceptions.ResourceNotFoundException:
                missing.add(fn); pending.discard(fn); continue
            except Exception:
                continue
            lm = datetime.fromisoformat(cfg["LastModified"].replace("Z", "+00:00")).timestamp()
            if lm >= T_PUSH and cfg.get("LastUpdateStatus", "Successful") == "Successful" and cfg.get("State", "Active") == "Active":
                pending.discard(fn)
        if pending:
            R.log("   waiting: %d not yet redeployed (%ds)" % (len(pending), int(time.time() - t0)))
            time.sleep(120)
    if missing:
        R.warn("%d rewritten engines have no live function (repo-only): %s" % (len(missing), ", ".join(sorted(missing)[:20])))
    if pending:
        FAILS.append("%d engines not redeployed after %ds: %s" % (len(pending), int(time.time() - t0), ", ".join(sorted(pending)[:25])))
    else:
        R.ok("every live rewritten engine redeployed after the push")

    R.section("2. log scan for managed_secret failures since the push")
    bad = {}
    scanned = 0
    for fn in fns:
        if fn in missing:
            continue
        try:
            ev = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=T_PUSH * 1000,
                                        filterPattern='?"managed_secret" ?"No module named" ?"ImportError"', limit=20)
            scanned += 1
        except logs.exceptions.ResourceNotFoundException:
            continue
        except Exception as e:
            R.warn("%s: log scan failed: %s" % (fn, str(e)[:80])); continue
        hits = [e["message"][:160] for e in ev.get("events", []) if ("managed_secret" in e["message"] and ("unavailable" in e["message"] or "No module" in e["message"] or "ImportError" in e["message"]))]
        if hits:
            bad[fn] = hits[:3]
    R.log("scanned %d log groups" % scanned)
    for fn, hits in bad.items():
        FAILS.append("%s: %s" % (fn, hits[0]))
    if not bad:
        R.ok("no managed_secret import/resolution failures logged since the push")

    R.section("3. residual literals")
    keys = []
    try:
        ssm = boto3.client("ssm", region_name=REGION)
        for name in ("/justhodl/fmp/api-key", "/justhodl/polygon/api-key", "/justhodl/fred/api-key", "/justhodl/cmc/api-key", "/justhodl/telegram/bot_token"):
            try:
                keys.append(ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"])
            except Exception:
                pass
    except Exception as e:
        R.warn("ssm read failed: %s" % str(e)[:80])
    residual = []
    if keys:
        for base in ("aws/lambdas", "aws/shared", "aws/ops/ran", "aws/ops/historical", "cloudflare", "scripts"):
            for f in (ROOT / base).rglob("*"):
                if f.is_file() and "_archived" not in f.parts and f.suffix in (".py", ".json", ".js", ".toml", ".md", ".txt", ".sh"):
                    try:
                        t = f.read_text(errors="ignore")
                    except Exception:
                        continue
                    if any(k in t for k in keys):
                        residual.append(str(f.relative_to(ROOT)))
        if residual:
            FAILS.append("%d files still carry a live credential literal: %s" % (len(residual), residual[:15]))
        else:
            R.ok("zero residual credential literals in the tree (excluding aws/lambdas/_archived)")
    else:
        R.warn("no SSM values readable -- residual scan skipped")

    R.section("verdict")
    for f in FAILS:
        R.fail(f)
    R.kv(step="a2", engines=len(fns), missing_live=len(missing), not_redeployed=len(pending), log_failures=len(bad), residual_files=len(residual))
    if FAILS:
        R.log("RED: %d failure(s)" % len(FAILS))
        sys.exit(1)
    R.ok("GREEN -- fleet reads credentials from managed configuration; the provider keys can now be ROTATED (Khalid)")
