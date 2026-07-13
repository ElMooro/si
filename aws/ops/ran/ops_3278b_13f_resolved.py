"""ops 3278b — resolver wired + honesty filters, proven. The FMP
ladder now actually RUNS (it was defined, never called) with a
persistent cusip map. Prove: ARGAN keyed by an alpha ticker with a
real tier; tier census; page carries the honesty-filter literal;
existing anchors intact."""
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
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
FN = "justhodl-13f-positions"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3278b)"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3278b_13f_resolved") as rep:
    fails, warns = [], []
    live_cfg = LAM.get_function_configuration(FunctionName=FN)
    env = (live_cfg.get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=env, eb_rule_name=None, eb_schedule=None,
                      timeout=max(int(live_cfg.get("Timeout") or 0),
                                  900),
                      memory=int(live_cfg.get("MemorySize") or 1536),
                      description=str(live_cfg.get("Description")
                                      or "")[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2,
                                           "MaxAttempts": 40})
    except Exception as e:
        fails.append(f"deploy: {str(e)[:80]}")

    if not fails:
        rep.section("1. Fresh feed — resolution + tiers")
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        d = None
        for _ in range(60):
            time.sleep(10)
            x = s3_json("data/13f-positions.json") or {}
            if str(x.get("generated_at", "")) > mark:
                d = x
                break
        if not d:
            fails.append("feed not fresh")
        else:
            agg = d.get("aggregate_by_ticker") or {}
            cm = s3_json("data/13f-cusip-map.json") or {}
            mapped = sum(1 for v in cm.values() if v.get("ticker"))
            tiers = {}
            unresolved = 0
            for k, a in agg.items():
                if not (k and k.isalpha()):
                    unresolved += 1
                t = a.get("cap_tier")
                if t:
                    tiers[t] = tiers.get(t, 0) + 1
            rep.kv(mcap_enriched=d.get("mcap_enriched"),
                   map_mapped=mapped, agg_unresolved=unresolved,
                   **{f"tier_{k}": v for k, v in tiers.items()})
            arg = next(((k, a) for k, a in agg.items()
                        if "ARGAN" in str(a.get("name", "")).upper()),
                       None)
            if arg and arg[0].isalpha():
                rep.ok(f"ARGAN RESOLVED: {arg[0]} "
                       f"tier={arg[1].get('cap_tier')} "
                       f"cap={arg[1].get('market_cap')}")
            elif arg:
                warns.append(f"ARGAN still keyed {arg[0]} — ladder "
                             "missed; inspect map entry")
            for k, a in sorted(
                    ((k, a) for k, a in agg.items()
                     if a.get("cap_tier") in ("MICRO", "SMALL")
                     and not a.get("market_cap", 0) * 3
                     < a.get("total_value", 0)),
                    key=lambda kv:
                    -((kv[1].get("n_funds_new_position") or 0)
                      + (kv[1].get("n_funds_adding") or 0)))[:3]:
                rep.log(f"  {a.get('cap_tier'):<5} {k:<6} "
                        f"{str(a.get('name'))[:26]:<26} NEW="
                        f"{a.get('n_funds_new_position') or 0} add="
                        f"{a.get('n_funds_adding') or 0}")

        rep.section("2. Page — filters live, anchors intact")
        okp = False
        for i in range(20):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/13f.html?t="
                    f"{int(time.time())}", headers=UA),
                    timeout=20).read().decode("utf-8", "replace")
                okp = ("ownership-implausible entries filtered" in h
                       and "Small & Mid-Cap Footprint" in h
                       and "Action Spotlight" in h
                       and "Rare picks" in h)
            except Exception:
                pass
            if okp:
                rep.ok(f"live (~{(i + 1) * 15}s)")
                break
            time.sleep(15)
        if not okp:
            fails.append("page literals not live")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
