"""ops 3278 — Khalid's 13F visual directives, verified end-to-end:
name-first everywhere (no '?' or codes — 4 renderer cells), cap-tier
badges in the Action Spotlight, $-held on Most-held names, tiers on
Rare picks, and the new Small & Mid-Cap Footprint section. Engine
gains the cusip→name-search fallback ladder + live market-cap
enrichment. Deploy engine → invoke → prove: mcap_enriched > 0, the
ARGAN entry resolved with tier, small/mid census, then page literals
live with existing anchors intact."""
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
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3278)"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3278_13f_visuals") as rep:
    fails, warns = [], []
    cfg = {}
    pc = AWS_DIR / "lambdas" / FN / "config.json"
    if pc.exists():
        cfg = json.loads(pc.read_text())
    sch = cfg.get("schedule")
    rule, cron = (sch.get("rule_name"), sch.get("cron")) \
        if isinstance(sch, dict) else (None, None)
    live_cfg = LAM.get_function_configuration(FunctionName=FN)
    env = (live_cfg.get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=env, eb_rule_name=rule,
                      eb_schedule=cron,
                      timeout=max(int(live_cfg.get("Timeout") or 0),
                                  900),
                      memory=int(live_cfg.get("MemorySize") or 1536),
                      description=str(cfg.get("description")
                                      or live_cfg.get("Description")
                                      or "")[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2,
                                           "MaxAttempts": 40})
    except Exception as e:
        fails.append(f"deploy: {str(e)[:80]}")

    if not fails:
        rep.section("1. Fresh feed with enrichment")
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
            fails.append("feed not fresh in 10 min")
        else:
            agg = d.get("aggregate_by_ticker") or {}
            tiers = {}
            for a in agg.values():
                t = a.get("cap_tier")
                if t:
                    tiers[t] = tiers.get(t, 0) + 1
            rep.kv(mcap_enriched=d.get("mcap_enriched"),
                   tickers=len(agg), **{f"tier_{k}": v
                                        for k, v in tiers.items()})
            arg = next(((t, a) for t, a in agg.items()
                        if "ARGAN" in str(a.get("name", "")).upper()),
                       None)
            if arg:
                t, a = arg
                rep.ok(f"ARGAN resolved: ticker={t} "
                       f"tier={a.get('cap_tier')} "
                       f"cap={a.get('market_cap')}")
            else:
                warns.append("ARGAN not in aggregate this run")
            sm = sorted(((t, a) for t, a in agg.items()
                         if a.get("cap_tier") in ("MICRO", "SMALL",
                                                  "MID")),
                        key=lambda kv:
                        -((kv[1].get("n_funds_new_position") or 0)
                          + (kv[1].get("n_funds_adding") or 0)))[:3]
            for t, a in sm:
                rep.log(f"  {a.get('cap_tier'):<5} {t:<6} "
                        f"{str(a.get('name'))[:28]:<28} "
                        f"NEW={a.get('n_funds_new_position') or 0} "
                        f"add={a.get('n_funds_adding') or 0} "
                        f"held=${(a.get('total_value') or 0) / 1e6:.0f}M")
            if not d.get("mcap_enriched"):
                fails.append("mcap enrichment produced zero")

        rep.section("2. Page live, existing intact")
        okp = False
        for i in range(22):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/13f.html?t="
                    f"{int(time.time())}", headers=UA),
                    timeout=20).read().decode("utf-8", "replace")
                okp = ("Small & Mid-Cap Footprint" in h
                       and "capBadge" in h
                       and "Action Spotlight" in h
                       and "Rare picks" in h
                       and "|| '?')" not in h)
            except Exception:
                pass
            if okp:
                rep.ok(f"visual set live, '?' fallbacks gone "
                       f"(~{(i + 1) * 15}s)")
                break
            time.sleep(15)
        if not okp:
            fails.append("page literals not fully live")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
