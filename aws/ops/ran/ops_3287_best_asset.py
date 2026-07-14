"""ops 3287 — BEST ASSET NOW (Khalid: the system must name the single
best asset to invest in, and cash/the dollar/a money-market fund is a
perfectly valid answer when everything is too risky). Extends
justhodl-master-allocator with a dual-momentum block (Antonacci): 12
asset classes + explicit CASH, every risk asset must beat the T-bill
(BIL) momentum hurdle or cash outranks it; risk override (crisis-
composite/GSI ≥70 or us10y-sentinel RED+) restricts the podium to
CASH/UUP/GLD/IEF. Compass v1.2 bridge already live — this closes the
"single accountable answer" layer on top. Hero live on
master-allocator.html. Truth bands: winner ∈ universe, ranked ≥ 10
rows, hurdle finite."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-master-allocator"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]
UNIV = {"SPY", "QQQ", "IWM", "EFA", "EEM", "TLT", "IEF", "GLD",
        "SLV", "DBC", "UUP", "BTC-USD", "CASH"}


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def get(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (jh-ops-3287)"})
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "ignore")


with report("3287_best_asset") as rep:
    fails = []
    live = LAM.get_function_configuration(FunctionName=FN)
    env = (live.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=max(600, int(live.get("Timeout") or 300)),
                  memory=int(live.get("MemorySize") or 1024),
                  description=str(live.get("Description") or "")[:250],
                  smoke=False)

    rep.section("2. Run + best_asset truth bands")
    mark = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    d = None
    for i in range(45):
        time.sleep(12)
        d = s3_json("data/master-allocation.json")
        ba = (d or {}).get("best_asset")
        if ba and str((d or {}).get("as_of", "")) >= mark[:10]:
            if ba.get("winner") or ba.get("error"):
                break
    ba = (d or {}).get("best_asset") or {}
    if ba.get("error"):
        fails.append("best_asset error: %s" % ba["error"])
    win = (ba.get("winner") or {})
    rep.kv(winner=win.get("asset"), score=win.get("score"),
           hurdle=ba.get("cash_hurdle_score"),
           override=(ba.get("risk_override") or {}).get("active"),
           reasons=(ba.get("risk_override") or {}).get("reasons"),
           ranked_n=len(ba.get("ranked") or []))
    if win.get("asset") not in UNIV:
        fails.append("winner outside universe: %s" % win.get("asset"))
    if len(ba.get("ranked") or []) < 10:
        fails.append("ranked table thin: %d"
                     % len(ba.get("ranked") or []))
    if not isinstance(ba.get("cash_hurdle_score"), (int, float)):
        fails.append("cash hurdle missing")
    if win.get("asset") and not win.get("why"):
        fails.append("winner has no why")

    rep.section("3. master-allocator.html hero live")
    ok = False
    for i in range(20):
        try:
            pg = get("https://justhodl.ai/master-allocator.html?cb=%d"
                     % time.time())
            if "jhk-best" in pg and "BEST ASSET NOW" in pg:
                ok = True
                break
        except Exception:
            pass
        time.sleep(18)
    if not ok:
        fails.append("hero not live on master-allocator.html")
    else:
        rep.log("  hero markers live")

    rep.kv(fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3287 PASS — the system now names its best asset, "
            "and cash is allowed to win.")
sys.exit(0)
