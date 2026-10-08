"""ops 3297 — kill the '?' rows (Khalid: $203.7B 'Unresolved' with no
names; '?' tickers on the dollar-flow boards). Phase 1 DIAGNOSES the
live doc: prints the top-10 unresolved rows (cusip, filed issuer name,
title, share_type, $) so the cause is on the record. Phase 2 deploys
the fix: [a] resolver spends its budget BIGGEST-DOLLARS-FIRST (was
file order — whale cusips starved); [b] agg name/title/share_type
backfilled on every touch, and _slim/class tops can never emit a
nameless row (CUSIP-labelled fallback); [c] PRN (principal-amount) and
note/debenture titles classify as DEBT_NOTES — convertible/corporate
paper, not 'unresolved equities'; [d] name rules ungated so bond/gold
funds without 'ETF' in the name still classify; [e] page renders the
issuer NAME wherever a ticker is pending. Truth bands: zero nameless
rows anywhere; UNRESOLVED total shrinks vs the diagnosed baseline."""
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
        "User-Agent": "Mozilla/5.0 (jh-ops-3297)"})
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "ignore")


def unresolved_rows(d):
    agg = (d or {}).get("aggregate_by_ticker") or {}
    rows = [a for a in agg.values() if not a.get("ticker")]
    rows.sort(key=lambda a: -(a.get("total_value") or 0))
    return rows


with report("3297_unresolved_named") as rep:
    fails = []
    rep.section("1. DIAGNOSIS — what the $203B 'unresolved' really is")
    d0 = s3_json("data/13f-positions.json") or {}
    base = unresolved_rows(d0)
    base_total = sum(a.get("total_value") or 0 for a in base)
    rep.kv(unresolved_n=len(base),
           unresolved_usd_b=round(base_total / 1e9, 2))
    for a in base[:10]:
        rep.kv(**{("cu_" + (a.get("cusip") or "?")[:9]):
                  "%s | title=%s | type=%s | $%.2fB | funds=%d"
                  % ((a.get("name") or "(no name)")[:34],
                     (a.get("title") or "?")[:18],
                     a.get("share_type") or "?",
                     (a.get("total_value") or 0) / 1e9,
                     a.get("n_funds_holding") or 0)})

    rep.section("2. Deploy fix + rerun")
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
    for i in range(70):
        time.sleep(15)
        d = s3_json("data/13f-positions.json")
        if d and d.get("generated_at", "") >= mark:
            break
    if not d or d.get("generated_at", "") < mark:
        fails.append("doc never freshened")
    else:
        AC = d.get("asset_classes") or {}
        un = (AC.get("UNRESOLVED") or {}).get("total_usd") or 0
        dn = (AC.get("DEBT_NOTES") or {}).get("total_usd") or 0
        rep.kv(unresolved_after_b=round(un / 1e9, 2),
               debt_notes_b=round(dn / 1e9, 2),
               baseline_b=round(base_total / 1e9, 2))
        if base_total > 1e9 and un >= base_total * 0.9:
            fails.append("UNRESOLVED barely moved: %.1fB -> %.1fB"
                         % (base_total / 1e9, un / 1e9))
        nameless = 0
        for k, v in AC.items():
            if k == "_note":
                continue
            for t in v.get("top") or []:
                if not (t.get("name") or "").strip():
                    nameless += 1
        df = d.get("dollar_flows") or {}
        for board in ("most_bought_usd", "most_sold_usd",
                      "accumulating"):
            for r in df.get(board) or []:
                if not (r.get("name") or "").strip():
                    nameless += 1
        for r in (d.get("top_owned") or []):
            if not (r.get("name") or "").strip():
                nameless += 1
        rep.kv(nameless_rows=nameless)
        if nameless:
            fails.append("%d nameless rows remain" % nameless)
        top_un = unresolved_rows(d)[:3]
        rep.kv(remaining_unresolved=[
            ((a.get("name") or "?")[:28],
             round((a.get("total_value") or 0) / 1e9, 2))
            for a in top_un])

    rep.section("3. Page name-first rendering live")
    ok = False
    for i in range(20):
        try:
            pg = get("https://justhodl.ai/13f.html?cb=%d"
                     % time.time())
            if ("Convertible & Corp Notes" in pg
                    and "ticker pending" in pg
                    and "var tick=(a.ticker" in pg):
                ok = True
                break
        except Exception:
            pass
        time.sleep(18)
    if not ok:
        fails.append("page name-first render not live")
    else:
        rep.log("  name-first rendering live")

    rep.kv(fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3297 PASS — no more '?': every dollar has a name.")
sys.exit(0)
