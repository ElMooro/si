#!/usr/bin/env python3
"""ops 3123 -- WHY.HTML MOMENTUM + INDUSTRY TREND (Khalid: why.html
should also show stock momentum, industry momentum, industry growth
or shrinkage, where the industry stands vs its moving averages, and
whether a golden or death cross occurred). Two-sided build:
(1) ENGINE justhodl-industry-rotation v4.0 -> v4.1: ladder_row now
emits pct_vs_sma50 / pct_vs_sma200 and golden_cross_sessions_ago /
death_cross_sessions_ago (SMA50xSMA200 scan, 60-session lookback) --
the closes were already in hand, ~zero added cost.
(2) PAGE why.html: new renderMomentum block (1M/3M/6M/12M returns,
Jegadeesh-Titman 12-1 skip-month, price vs SMA50/200, stock
golden/death cross with sessions-ago -- all computed client-side from
the existing 504-session research series, zero engine cost) + the
industry fleet join gains momentum (3m raw, rel-vs-SPY pp, pctile,
12-1 skip), the 20/50/100/200 MA ladder with distances, and the
industry 50/200 cross row. Growth/shrink verdict from 3117 unchanged.
Deploy-lambdas runs in parallel with this job, so: Event-invoke the
engine, poll the S3 doc for version 4.1 (re-invoke periodically until
the new bundle is live -- Event+S3-age-poll doctrine, never long sync
invokes). Lessons applied: 3118 ASCII markers, 3116 marks[0] new to
this push."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3123", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def doc():
    try:
        return json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/industry-rotation.json"
        )["Body"].read())
    except Exception:
        return {}


def fire():
    try:
        LAM.invoke(FunctionName="justhodl-industry-rotation",
                   InvocationType="Event")
        return True
    except Exception:
        return False


def main():
    fails, warns = [], []
    with report("3123_why_momentum") as rep:
        rep.section("1. Engine v4.1 doc live (Event + S3 poll)")
        fire()
        d = {}
        for i in range(30):
            time.sleep(20)
            d = doc()
            if str(d.get("version")) == "4.1":
                break
            if i in (8, 16, 24):
                fire()
        rep.kv(doc_version=d.get("version"),
               generated_at=d.get("generated_at"))
        if str(d.get("version")) != "4.1":
            fails.append("doc still v%s after poll budget"
                         % d.get("version"))
        ladder = d.get("ladder") or []
        with_ma = [r for r in ladder if r.get("pct_vs_sma200")
                   is not None]
        gx = [r["etf"] for r in ladder
              if r.get("golden_cross_sessions_ago") is not None]
        dx = [r["etf"] for r in ladder
              if r.get("death_cross_sessions_ago") is not None]
        rep.kv(ladder_rows=len(ladder), rows_with_ma_dist=len(with_ma),
               golden=gx, death=dx)
        if ladder and len(with_ma) < max(5, len(ladder) // 2):
            fails.append("MA distances thin: %d of %d"
                         % (len(with_ma), len(ladder)))
        if not gx and not dx:
            warns.append("no 50/200 cross across %d ETFs in the "
                         "60-session window (legitimate -- crosses "
                         "are rare by construction)" % len(ladder))

        rep.section("2. why.html live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/why.html?cb=%d"
                         % time.time())
                if "renderMomentum" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(15)
        if not ok:
            fails.append("why.html renderMomentum not live")
        else:
            for m in ("jh-momentum", "12-1", "skip-month",
                      "GOLDEN CROSS", "DEATH CROSS",
                      "no 50/200 cross",
                      "Industry momentum",
                      "Industry vs moving averages",
                      "Industry 50/200 cross",
                      "golden_cross_sessions_ago",
                      "pct_vs_sma200"):
                if m not in pg:
                    fails.append("why.html marker missing: %s" % m)

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3123.json").write_text(json.dumps(
        {"ops": 3123, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
