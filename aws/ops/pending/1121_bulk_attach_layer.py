"""ops 1121 — Bulk-attach the jhcore Lambda Layer (v1) to the entire fleet.

NON-DESTRUCTIVE: attaching a layer adds modules to sys.path but does NOT modify
any Lambda's source code. Existing duplicated helpers keep working; jhcore.* becomes
importable in addition. Future per-Lambda touches can swap the duplicated code for
`from jhcore import fred, s3io, notify, claude, kb` over time.

Safety rails:
  - Skip Lambdas with the layer already attached
  - Skip Lambdas already at the 5-layer cap (rare in this fleet)
  - Update one Lambda at a time, wait for Active before moving on
  - Telegram an end-of-run summary

Output report: aws/ops/reports/1121.json with per-Lambda attach status.
"""
import io, json, os, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"; ACCOUNT = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
LAYER_ARN = f"arn:aws:lambda:{REGION}:{ACCOUNT}:layer:justhodl-core:1"
TG_BOT = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TG_CHAT = "8678089260"

_cfg = Config(connect_timeout=10, read_timeout=60, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)


def is_justhodl(name):
    return name.startswith("justhodl") or name.startswith("jhk")


def list_fleet():
    fns = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for f in page.get("Functions", []):
            n = f["FunctionName"]
            if not is_justhodl(n): continue
            cur_layers = [l.get("Arn") for l in (f.get("Layers") or [])]
            fns.append({"name": n, "layers": cur_layers, "runtime": f.get("Runtime")})
    return fns


def wait_active(name, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=name)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
                return True
            if c.get("LastUpdateStatus") == "Failed":
                return False
        except ClientError:
            return False
        time.sleep(1.5)
    return False


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    counts = {"total": 0, "already_attached": 0, "attached": 0, "skipped_full": 0,
              "skipped_non_python": 0, "skipped_self": 0, "errors": 0}
    details = []

    try:
        fleet = list_fleet()
        counts["total"] = len(fleet)
        print(f"[1121] fleet size: {len(fleet)}")

        for f in fleet:
            name = f["name"]; layers = f["layers"] or []; runtime = f["runtime"] or ""

            # Skip the scheduler itself + the smoke-test reference (would self-attach harmlessly but pointless)
            if name == "justhodl-scheduler":
                counts["skipped_self"] += 1
                details.append({"name": name, "status": "SKIPPED_SELF"})
                continue

            # Only Python runtimes can use a python-layout layer
            if not runtime.startswith("python"):
                counts["skipped_non_python"] += 1
                details.append({"name": name, "status": "SKIPPED_NON_PYTHON", "runtime": runtime})
                continue

            # Already attached?
            if LAYER_ARN in layers:
                counts["already_attached"] += 1
                details.append({"name": name, "status": "ALREADY"})
                continue

            # At the 5-layer cap?
            if len(layers) >= 5:
                counts["skipped_full"] += 1
                details.append({"name": name, "status": "SKIPPED_CAP", "layer_count": len(layers)})
                continue

            new_layers = layers + [LAYER_ARN]
            try:
                wait_active(name)
                lam.update_function_configuration(FunctionName=name, Layers=new_layers)
                wait_active(name)
                counts["attached"] += 1
                details.append({"name": name, "status": "ATTACHED", "now_layers": len(new_layers)})
                if counts["attached"] % 25 == 0:
                    print(f"[1121] progress: attached {counts['attached']}/{counts['total']}")
            except ClientError as e:
                counts["errors"] += 1
                details.append({"name": name, "status": "ERR", "err": str(e)[:160]})
            time.sleep(0.05)  # gentle throttle vs Lambda update API

    except Exception as e:
        rpt["fatal_err"] = str(e)[:400]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["counts"] = counts
    rpt["details"] = details
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
