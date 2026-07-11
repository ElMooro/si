#!/usr/bin/env python3
"""ops 3125 -- WHY.HTML TIER-1 ROADMAP (#1-#5) (Khalid: build every
single one, one by one; batched per-tier, verified per-feature).
Shipped page-side: (#1) Fleet Conviction Strip -- 10 live boards
(master-rank, accumulation, insider, share-flows, forensic, industry,
FINRA squeeze, AI-rerating, estimate revisions, best-setups) resolved
per-ticker with defensive multi-key lookups, verdict bar 'X of N
systems constructive', per-system chips + deep links; (#2) AI-rerating
mispricing chip (z vs modeled multiple / % to model); (#3) revision
velocity chip (30d up/down + EPS est %); (#4) RS vs own industry ETF
(stock/ETF ratio 3m + 1m + RS-new-high; engine IR v4.2 emits
closes_66 per ladder row to feed it); (#5) Risk & Tradability
(1y/2y max DD, 20d/1y realized vol, Sortino, ADV$, days-to-build at
10%% ADV). Verify: IR doc v4.2 with closes_66 breadth, donor boards
non-empty (fail-level where schema is confirmed, warn-level where the
page resolves defensively), page markers per feature. Lessons: 3118
ASCII markers, 3116 marks[0] new to this push, Event+S3-poll."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3125", "Cache-Control": "no-cache"}


def get(url, timeout=30):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=timeout).read().decode(
        "utf-8", "replace")


def s3j(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def fire():
    try:
        LAM.invoke(FunctionName="justhodl-industry-rotation",
                   InvocationType="Event")
    except Exception:
        pass


def main():
    fails, warns = [], []
    with report("3125_tier1_conviction") as rep:
        rep.section("1. IR v4.2 doc (Event + poll)")
        fire()
        d = {}
        for i in range(30):
            time.sleep(20)
            d = s3j("data/industry-rotation.json") or {}
            if str(d.get("version")) == "4.2":
                break
            if i in (8, 16, 24):
                fire()
        n66 = sum(1 for r in (d.get("ladder") or [])
                  if len(r.get("closes_66") or []) >= 40)
        rep.kv(ir_version=d.get("version"), rows_with_closes66=n66)
        if str(d.get("version")) != "4.2":
            fails.append("IR doc still v%s" % d.get("version"))
        if n66 < 30:
            fails.append("closes_66 thin: %d rows" % n66)

        rep.section("2. Strip donor boards")
        mr = s3j("data/master-ranker.json") or {}
        tt = mr.get("top_tickers") or mr.get("rows") or []
        rep.kv(master_rank_rows=len(tt))
        if len(tt) < 10:
            fails.append("master-ranker thin: %d" % len(tt))
        for key, name, floor in (
                ("data/finra-short.json", "finra", 20),
                ("data/estimate-revisions.json", "revisions", 20),
                ("data/ai-rerating-radar.json", "rerating", 20),
                ("data/best-setups.json", "best_setups", 1),
                ("data/accumulation-radar.json", "accumulation", 1),
                ("data/insider-radar.json", "insider", 1)):
            doc = s3j(key)
            n = 0
            if isinstance(doc, dict):
                for k, v in doc.items():
                    if isinstance(v, list):
                        n += len(v)
                    elif isinstance(v, dict) and k in ("tickers",
                                                       "by_ticker"):
                        n += len(v)
            rep.kv(**{name + "_items": n})
            if doc is None:
                warns.append("%s doc missing -- chip degrades to "
                             "'no data'" % name)
            elif n < floor:
                warns.append("%s sparse: %d items" % (name, n))

        rep.section("3. Page live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/why.html?cb=%d"
                         % time.time())
                if "jh-conviction" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(15)
        if not ok:
            fails.append("why.html jh-conviction not live")
        else:
            marks = {
                "#1 strip": ("Fleet Conviction",
                             "systems constructive",
                             "MASTER-RANK", "BEST-SETUPS",
                             "SHORT/SQUEEZE"),
                "#2 rerating": ("vs modeled multiple",
                                "ai-rerating.html"),
                "#3 revisions": ("revisions 30d",
                                 "estimate-revisions.html"),
                "#4 rs-pair": ("Relative Strength vs own industry",
                               "RS NEW HIGH", "closes_66"),
                "#5 risk": ("jh-risktrade", "Max drawdown",
                            "Sortino",
                            "Days to build 1% position"),
            }
            for feat, ms in marks.items():
                for m in ms:
                    if m not in pg:
                        fails.append("%s marker missing: %s"
                                     % (feat, m))

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3125.json").write_text(json.dumps(
        {"ops": 3125, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
