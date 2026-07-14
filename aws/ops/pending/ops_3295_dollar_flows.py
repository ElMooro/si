"""ops 3295 — DOLLAR-BASIS REVERT + FULL FLOW SPEC (Khalid: 'net =
bought minus sold' means DOLLAR VALUE per stock, not fund counts).
most_bought/most_sold now rank by dollars (counts kept as context);
per-ticker bought_usd/sold_usd/net_flow_usd accumulate inside the SAME
dv loop as the NET FLOW headline so every row reconciles to the
banner. NEW boards on 13f.html: MOST BOUGHT ($) / MOST SOLD ($) top-20
with $bought+$sold+net per stock and cap tiers; TOP 20 OWNED with
ACCUMULATING/DISTRIBUTING net-$ chips; ACCUMULATION LIST (net$>0 and
>=2 funds adding); HIGHEST CONVICTION (% of book, weighted by
clone-alpha skill); WHERE THE WHALE MONEY SITS (asset classes: US
stocks, equity ETFs, bonds, cash T-bills, gold, crypto, REITs,
commodities — $ held + net $ flow each). consensus bar repointed to
net_flow_usd_m. Truth bands: sum(per-ticker bought) reconciles to
flow_summary.total_buy_usd within 12%; boards dollar-sorted; >=6 asset
classes with US_EQUITY largest; conviction rows carry fund+weight."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-13f-positions"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=60,
                                 retries={"max_attempts": 0}))
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def get(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (jh-ops-3295)"})
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "ignore")


with report("3295_dollar_flows") as rep:
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

    rep.section("2. Full run + reconciliation truth bands")
    mark = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    d = None
    for i in range(70):
        time.sleep(15)
        d = s3_json("data/13f-positions.json")
        if (d and d.get("generated_at", "") >= mark
                and d.get("dollar_flows")):
            break
    if not d or d.get("generated_at", "") < mark:
        fails.append("positions doc never freshened")
    elif not d.get("dollar_flows"):
        fails.append("dollar_flows missing")
    else:
        df = d["dollar_flows"]
        mb, ms = df.get("most_bought_usd") or [], \
            df.get("most_sold_usd") or []
        agg = d.get("aggregate_by_ticker") or {}
        tot_b = sum((a.get("bought_usd") or 0) for a in agg.values())
        tot_s = sum((a.get("sold_usd") or 0) for a in agg.values())
        fs = d.get("flow_summary") or {}
        rep.kv(tickers=len(agg),
               per_ticker_bought=round(tot_b),
               headline_bought=fs.get("total_buy_usd"),
               per_ticker_sold=round(tot_s),
               headline_sold=fs.get("total_sell_usd"))
        hb, hs = fs.get("total_buy_usd") or 1, \
            fs.get("total_sell_usd") or 1
        if abs(tot_b - hb) / hb > 0.12:
            fails.append("bought reconciliation off %.1f%%"
                         % (100 * abs(tot_b - hb) / hb))
        if abs(tot_s - hs) / hs > 0.12:
            fails.append("sold reconciliation off %.1f%%"
                         % (100 * abs(tot_s - hs) / hs))
        if len(mb) < 15 or len(ms) < 15:
            fails.append("flow boards thin")
        if mb and any(mb[i]["bought_usd"] < mb[i + 1]["bought_usd"]
                      for i in range(len(mb) - 1)):
            fails.append("most_bought not dollar-sorted")
        if ms and any(ms[i]["sold_usd"] < ms[i + 1]["sold_usd"]
                      for i in range(len(ms) - 1)):
            fails.append("most_sold not dollar-sorted")
        rep.kv(top_bought=[(x["ticker"],
                            round((x["bought_usd"] or 0) / 1e9, 2))
                           for x in mb[:5]],
               top_sold=[(x["ticker"],
                          round((x["sold_usd"] or 0) / 1e9, 2))
                         for x in ms[:5]])
        to = d.get("top_owned") or []
        if len(to) != 20:
            fails.append("top_owned != 20: %d" % len(to))
        cv = d.get("conviction_top") or []
        rep.kv(conviction=[(x["ticker"], x["conviction_score"],
                            x["max_weight_fund"],
                            x["max_weight_pct"]) for x in cv[:5]])
        if len(cv) < 8:
            fails.append("conviction thin: %d" % len(cv))
        if cv and not all(x.get("max_weight_fund")
                          and x.get("max_weight_pct") for x in cv):
            fails.append("conviction rows missing fund/weight")
        AC = d.get("asset_classes") or {}
        classes = [k for k in AC if k != "_note"]
        rep.kv(classes={k: round((AC[k].get("total_usd") or 0) / 1e9,
                                 1) for k in classes})
        if len(classes) < 6:
            fails.append("asset classes < 6: %d" % len(classes))
        if classes and max(
                classes, key=lambda k: AC[k].get("total_usd") or 0) \
                != "US_EQUITY":
            fails.append("US_EQUITY not the largest class — "
                         "classification suspect")
        acc = df.get("accumulating") or []
        if acc and not all((a.get("net_flow_usd") or 0) > 0
                           for a in acc):
            fails.append("accumulating list has non-positive net")

    rep.section("3. 13f.html boards live")
    ok = False
    for i in range(20):
        try:
            pg = get("https://justhodl.ai/13f.html?cb=%d"
                     % time.time())
            if ("jh-dollar-flows" in pg
                    and "INSTITUTIONAL DOLLAR FLOWS" in pg
                    and "net_flow_usd_m" in pg):
                ok = True
                break
        except Exception:
            pass
        time.sleep(18)
    if not ok:
        fails.append("13f.html dollar boards not live")
    else:
        rep.log("  boards + repointed consensus bar live")

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3295 PASS — every buy/sell board now speaks in "
            "dollars and reconciles to the headline.")
sys.exit(0)
