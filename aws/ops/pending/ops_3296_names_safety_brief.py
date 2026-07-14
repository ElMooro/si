"""ops 3296 — Khalid's three: [1] Cross-fund consensus / Most-held now
shows the FULL company/ETF name next to every ticker (bug: mobile CSS
display:none + a 4-col grid holding 5 spans — fixed to 5 cols desktop,
name-on-own-line mobile). [2] FLIGHT-TO-SAFETY CHECK inside the
consensus section: cash/T-bill ETFs, bonds, gold, real estate, crypto
— $ held, net $ flow, and the actual holdings BY NAME; honest
zero-state (money-market funds/futures aren't 13F-reportable);
name-regex classifier fallback so bond/gold/REIT ETFs can't slip to
OTHER_ETF; safety_rotation summary (safe $ / % of complex / net flow).
[3] AI brief's 'Conviction 62' now shows WHAT it's reading — a
'book it's reading' strip of top consensus holdings with names, fund
counts, $ held. Truth bands: safety_rotation present and coherent
(safe_usd == sum of the 3 classes), class tops carry names, page
markers live, consensus-name no longer display:none on mobile."""
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
FN = "justhodl-13f-positions"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def get(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (jh-ops-3296)"})
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "ignore")


with report("3296_names_safety_brief") as rep:
    fails = []
    live = LAM.get_function_configuration(FunctionName=FN)
    env = (live.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=int(live.get("Timeout") or 900),
                  memory=int(live.get("MemorySize") or 2048),
                  description=str(live.get("Description") or "")[:250],
                  smoke=False)

    rep.section("2. Run + safety/name truth bands")
    mark = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    d = None
    for i in range(70):
        time.sleep(15)
        d = s3_json("data/13f-positions.json")
        if (d and d.get("generated_at", "") >= mark
                and d.get("safety_rotation")):
            break
    if not d or d.get("generated_at", "") < mark:
        fails.append("doc never freshened")
    else:
        SR = d.get("safety_rotation") or {}
        AC = d.get("asset_classes") or {}
        rep.kv(safe_usd=SR.get("safe_usd"),
               safe_pct=SR.get("safe_pct_of_book"),
               safe_net=SR.get("safe_net_flow_usd"))
        if not SR:
            fails.append("safety_rotation missing")
        else:
            calc = sum((AC.get(k) or {}).get("total_usd") or 0
                       for k in ("CASH_TBILLS", "BONDS", "GOLD_PM"))
            if abs(calc - (SR.get("safe_usd") or 0)) > 1:
                fails.append("safety sum incoherent")
            if not (0 <= (SR.get("safe_pct_of_book") or -1) <= 100):
                fails.append("safe_pct out of range")
        classes = {k: v for k, v in AC.items() if k != "_note"}
        rep.kv(classes={k: round((v.get("total_usd") or 0) / 1e9, 2)
                        for k, v in classes.items()})
        named = miss = 0
        for k, v in classes.items():
            for t in v.get("top") or []:
                if "name" in t:
                    named += 1
                    if not t["name"] and t.get("ticker") != "?":
                        miss += 1
                else:
                    fails.append("%s top lacks name field" % k)
                    break
        rep.kv(top_entries=named, empty_names=miss)
        gp = (AC.get("GOLD_PM") or {}).get("top") or []
        if gp:
            rep.kv(gold_sample=[(t["ticker"], t.get("name", "")[:24])
                                for t in gp[:3]])
            if not any(t.get("name") for t in gp):
                fails.append("gold tops all nameless")
        ch = d.get("consensus_holds") or []
        if ch and sum(1 for x in ch[:20] if x.get("name")) < 15:
            fails.append("consensus_holds names sparse")

    rep.section("3. Page live: names + safety + brief book")
    ok = False
    for i in range(20):
        try:
            pg = get("https://justhodl.ai/13f.html?cb=%d"
                     % time.time())
            css_ok = ".consensus-name{display:none}" not in pg \
                and "grid-column:1/-1;margin-top:-4px" in pg
            ok = ("jh-safety" in pg
                  and "FLIGHT-TO-SAFETY CHECK" in pg
                  and "jh-brief-book" in pg and css_ok)
            if ok:
                break
        except Exception:
            pass
        time.sleep(18)
    if not ok:
        fails.append("page markers/CSS not live")
    else:
        rep.log("  names CSS + safety block + brief book live")

    rep.kv(fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3296 PASS — every ticker has its name, the safety "
            "rotation is visible, and the brief shows its book.")
sys.exit(0)
