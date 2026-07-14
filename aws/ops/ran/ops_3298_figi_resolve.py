"""ops 3298 — fix-forward for 3297: the diagnosed unresolved giants
are megacaps with SEC-abbreviated names (OCCIDENTAL PETE, foreign
H-/N-prefix CUSIPs like Chubb/ASML, BRK.B share classes). Fixes
shipped: OpenFIGI CUSIP→ticker tier (authoritative, value-first, 400
cusips/run, keyless-rate-respecting), abbreviation-aware name norm
(PETE→PETROLEUM etc.), dotted-ticker acceptance (BRK.B). Truth bands:
UNRESOLVED drops ≥50% from the $200.2B baseline (or ≤$60B); the
diagnosed cusips 674599105/N07059210/H1467J104 resolve to real
tickers; zero nameless rows anywhere."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-13f-positions"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]
BASELINE_B = 200.2
PROBE = ("674599105", "N07059210", "H1467J104", "084670702",
         "038222105")


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


with report("3298_figi_resolve") as rep:
    fails, warns = [], []
    live = LAM.get_function_configuration(FunctionName=FN)
    env = (live.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=int(live.get("Timeout") or 900),
                  memory=int(live.get("MemorySize") or 2048),
                  description=str(live.get("Description") or "")[:250],
                  smoke=False)
    mark = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    d = None
    for i in range(75):
        time.sleep(15)
        d = s3_json("data/13f-positions.json")
        if d and d.get("generated_at", "") >= mark:
            break
    if not d or d.get("generated_at", "") < mark:
        fails.append("doc never freshened")
    else:
        AC = d.get("asset_classes") or {}
        un = ((AC.get("UNRESOLVED") or {}).get("total_usd") or 0) / 1e9
        rep.kv(unresolved_b=round(un, 2), baseline_b=BASELINE_B,
               debt_notes_b=round(((AC.get("DEBT_NOTES") or {})
                                   .get("total_usd") or 0) / 1e9, 2))
        if un > BASELINE_B * 0.5 and un > 60:
            fails.append("UNRESOLVED still %.1fB" % un)
        cmap = s3_json("data/13f-cusip-map.json") or {}
        got = {cu: (cmap.get(cu) or {}).get("ticker") for cu in PROBE}
        rep.kv(probe_cusips=got)
        if sum(1 for v in got.values() if v) < 4:
            fails.append("probe cusips unresolved: %s" % got)
        nameless = 0
        for k, v in AC.items():
            if k == "_note":
                continue
            for t in v.get("top") or []:
                if not (t.get("name") or "").strip():
                    nameless += 1
        df = d.get("dollar_flows") or {}
        for b in ("most_bought_usd", "most_sold_usd", "accumulating"):
            for r in df.get(b) or []:
                if not (r.get("name") or "").strip():
                    nameless += 1
        rep.kv(nameless_rows=nameless)
        if nameless:
            fails.append("%d nameless rows" % nameless)
        rows = [a for a in (d.get("aggregate_by_ticker") or {})
                .values() if not a.get("ticker")]
        rows.sort(key=lambda a: -(a.get("total_value") or 0))
        rep.kv(remaining_top=[((a.get("name") or "?")[:26],
                               round((a.get("total_value") or 0)
                                     / 1e9, 2)) for a in rows[:5]])

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3298 PASS — the whale megacaps have their tickers "
            "back.")
sys.exit(0)
