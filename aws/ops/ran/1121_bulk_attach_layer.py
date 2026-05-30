"""ops 1121 — Bulk-attach the jhcore Lambda Layer (v1) to the entire fleet — PARALLEL.

Idempotent: re-running attaches only Lambdas missing the layer. Safe to retry.

NON-DESTRUCTIVE: attaching a layer adds modules to sys.path; no Lambda source is
modified. Existing duplicated helpers keep working; jhcore.* becomes importable.

Parallelism: 20 workers. Skips the post-update wait_active — AWS accepts the
update_function_configuration call when validated; layer attach propagates
within seconds. We re-poll Lambda state in the next-touched op (in run-ops auto-commit).

Safety rails:
  - Skip Lambdas with the layer already attached
  - Skip Lambdas at the 5-layer cap
  - Skip non-Python runtimes
  - Skip justhodl-scheduler itself
  - Per-Lambda error captured; doesn't abort batch
"""
import json, os, time, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"; ACCOUNT = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
LAYER_ARN = f"arn:aws:lambda:{REGION}:{ACCOUNT}:layer:justhodl-core:1"
TG_BOT = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TG_CHAT = "8678089260"
WORKERS = 20

_cfg = Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 3, "mode": "standard"})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)


def is_justhodl(name):
    return name.startswith("justhodl") or name.startswith("jhk")


def list_fleet():
    fns = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for f in page.get("Functions", []):
            n = f["FunctionName"]
            if not is_justhodl(n):
                continue
            cur_layers = [l.get("Arn") for l in (f.get("Layers") or [])]
            fns.append({"name": n, "layers": cur_layers, "runtime": f.get("Runtime")})
    return fns


def attempt_attach(f):
    """Returns (name, status_string, optional_err)."""
    name = f["name"]; layers = f["layers"] or []; runtime = f["runtime"] or ""
    if name == "justhodl-scheduler":
        return name, "SKIPPED_SELF", None
    if not runtime.startswith("python"):
        return name, "SKIPPED_NON_PYTHON", runtime
    if LAYER_ARN in layers:
        return name, "ALREADY", None
    if len(layers) >= 5:
        return name, "SKIPPED_CAP", str(len(layers))
    new_layers = layers + [LAYER_ARN]
    for attempt in range(3):
        try:
            lam.update_function_configuration(FunctionName=name, Layers=new_layers)
            return name, "ATTACHED", None
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "ResourceConflictException":
                # Lambda currently updating — back off and retry
                time.sleep(1.5 * (attempt + 1))
                continue
            return name, "ERR", str(e)[:160]
        except Exception as e:
            return name, "ERR", str(e)[:160]
    return name, "ERR", "ResourceConflict after retries"


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    counts = {"total": 0, "already_attached": 0, "attached": 0, "skipped_full": 0,
              "skipped_non_python": 0, "skipped_self": 0, "errors": 0}
    details = []

    try:
        fleet = list_fleet()
        counts["total"] = len(fleet)
        print(f"[1121] fleet size: {len(fleet)}, workers: {WORKERS}")

        # Map status string to counter key
        bucket = {
            "SKIPPED_SELF": "skipped_self",
            "SKIPPED_NON_PYTHON": "skipped_non_python",
            "ALREADY": "already_attached",
            "SKIPPED_CAP": "skipped_full",
            "ATTACHED": "attached",
            "ERR": "errors",
        }
        last_progress = 0
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futures = {ex.submit(attempt_attach, f): f["name"] for f in fleet}
            for i, fut in enumerate(as_completed(futures), 1):
                try:
                    name, status, extra = fut.result()
                except Exception as e:
                    name = futures[fut]; status = "ERR"; extra = str(e)[:150]
                k = bucket.get(status, "errors")
                counts[k] += 1
                details.append({"name": name, "status": status, "info": extra})
                if i - last_progress >= 50:
                    last_progress = i
                    print(f"[1121] {i}/{len(fleet)} processed (attached={counts['attached']}, already={counts['already_attached']}, errors={counts['errors']})")

    except Exception as e:
        rpt["fatal_err"] = str(e)[:400]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["counts"] = counts
    rpt["details"] = details
    rpt["layer_arn"] = LAYER_ARN
    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1121.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)

    # Telegram summary
    try:
        import urllib.request, urllib.parse
        msg = (
            f"<b>jhcore layer rollout</b>\n"
            f"Fleet: {counts['total']}\n"
            f"Newly attached: <b>{counts['attached']}</b>\n"
            f"Already had it: {counts['already_attached']}\n"
            f"Skipped (cap/non-py/self): {counts['skipped_full']+counts['skipped_non_python']+counts['skipped_self']}\n"
            f"Errors: {counts['errors']}"
        )
        urllib.request.urlopen(urllib.request.Request(
            f"https://api.telegram.org/bot{TG_BOT}/sendMessage",
            data=urllib.parse.urlencode({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode(),
            headers={"Content-Type": "application/x-www-form-urlencoded"}), timeout=8).read()
    except Exception: pass

    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
