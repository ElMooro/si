"""ops 3276 — client ground truth. Deploy the diag-capable API, verify
all served literals, then LISTEN: tail the API's CloudWatch logs for
[diag] beacons for ~4 minutes — if Khalid loads any page in that
window his browser's reality (SW controller, drawer version, favs
count, lists count, errors) lands verbatim in this report."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
FN = "justhodl-wl-series-api"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3276)"}


def get(u):
    return urllib.request.urlopen(
        urllib.request.Request(u, headers=UA), timeout=20).read()\
        .decode("utf-8", "replace")


with report("3276_client_truth") as rep:
    fails = []
    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json")
                     .read_text())
    env = (LAM.get_function_configuration(FunctionName=FN)
           .get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=env, eb_rule_name=None, eb_schedule=None,
                      timeout=cfg.get("timeout", 60),
                      memory=cfg.get("memory", 2048),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2,
                                           "MaxAttempts": 40})
    except Exception as e:
        fails.append(f"deploy: {str(e)[:80]}")

    rep.section("1. Served literals")
    checks = [
        ("https://justhodl.ai/service-worker.js", "v1.2.0-3276",
         "sw bumped"),
        ("https://justhodl.ai/jh-nav-drawer.js", "jh_diag_3276",
         "drawer nudge+beacon"),
        ("https://justhodl.ai/chart-pro.html", "ops 3276: client",
         "page diag footer"),
    ]
    for u, lit, label in checks:
        okx = False
        for i in range(20):
            try:
                if lit in get(f"{u}?t={int(time.time())}"):
                    okx = True
                    rep.ok(f"{label} live (~{(i + 1) * 15}s)")
                    break
            except Exception:
                pass
            time.sleep(15)
        if not okx:
            fails.append(f"{label} not live")

    rep.section("2. Listening for [diag] beacons (~4 min)")
    seen = []
    grp = f"/aws/lambda/{FN}"
    start = int((datetime.now(timezone.utc)
                 - timedelta(minutes=2)).timestamp() * 1000)
    for _ in range(16):
        time.sleep(15)
        try:
            evs = LOGS.filter_log_events(
                logGroupName=grp, startTime=start,
                filterPattern='"[diag]"', limit=20).get("events") or []
            for e in evs:
                m = e["message"].strip()[:220]
                if m not in seen:
                    seen.append(m)
                    rep.log("  " + m)
        except Exception:
            pass
    rep.kv(beacons=len(seen))
    if seen:
        rep.ok("client telemetry captured — his browser's reality is "
               "above")
    else:
        rep.log("  no beacons yet — they land in CloudWatch the "
                "moment any page is loaded; next ops reads them")

    rep.kv(n_fails=len(fails),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
