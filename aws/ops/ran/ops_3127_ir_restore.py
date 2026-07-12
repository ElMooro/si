#!/usr/bin/env python3
"""ops 3127 -- IR PAGE RESTORE + CAPEX BOARD FIX-PACK (Khalid: stuff
disappeared from industry-rotation; improve while keeping everything).
Git forensics: ops-3126 removed ZERO content lines (purely additive).
The disappearance was RUNTIME: v4.2 put a 66-float closes_66 array
inside every ladder row; the page's main render chain iterates row
fields and its catch ("feed unavailable -- engine may not have run
yet", line 347) fired, blanking Leaders/Soldiers downstream. Fixes:
(1) ENGINE v4.3 -- closes_66 removed from ladder rows (rows back to
v4.1 shape) and re-published as a top-level closes66 {ETF:[...]} map;
(2) why.html RS-pair reads IR.closes66[etf] (fallback kept);
(3) capex board: sector yoy now reads the engine's own yoy_pct (the
prior_proxy field is popped server-side -- my 3126 read a ghost),
hyperscaler tile reads total_ttm_b/yoy_pct (actual emit names),
FinViz sector vocabulary aliased into the beneficiary map (Consumer
Cyclical/Financial/Basic Materials/Consumer Defensive -- the
documented FinViz-to-GICS silent-no-op trap, hit again, now fixed),
top-3 spenders shown per sector, sort yoy-desc then ttm, ASCII
header. Verify at the DATA level that drove the blanks: ladder rows
must NOT contain closes_66, top-level closes66 must cover >=30 ETFs,
capex sectors must expose yoy_pct non-null on >=6, hyperscalers must
expose total_ttm_b. Lessons: 3118 ASCII, 3116 marks[0]-new; NEW
LESSON BANKED: source-marker verification passes while runtime
rendering fails -- when adding fields to a doc consumed by generic
row-iterating renderers, publish bulky arrays at top level, never
inside rows."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3127", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
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
    with report("3127_ir_restore") as rep:
        rep.section("1. IR v4.3 doc: rows clean + closes66 map")
        fire()
        d = {}
        for i in range(30):
            time.sleep(20)
            d = s3j("data/industry-rotation.json") or {}
            if str(d.get("version")) == "4.3":
                break
            if i in (8, 16, 24):
                fire()
        ladder = d.get("ladder") or []
        dirty = sum(1 for r in ladder if "closes_66" in r)
        c66 = d.get("closes66") or {}
        rep.kv(ir_version=d.get("version"), rows_dirty=dirty,
               closes66_etfs=len(c66))
        if str(d.get("version")) != "4.3":
            fails.append("doc still v%s" % d.get("version"))
        if dirty:
            fails.append("%d ladder rows still carry closes_66"
                         % dirty)
        if len(c66) < 30:
            fails.append("closes66 map thin: %d" % len(c66))

        rep.section("2. Capex doc fields the board now reads")
        cp = s3j("data/capex-pulse.json") or {}
        secs = cp.get("sectors") or {}
        n_yoy = sum(1 for v in secs.values()
                    if isinstance(v, dict)
                    and v.get("yoy_pct") is not None)
        hyp = cp.get("hyperscalers") or {}
        fv_named = sum(1 for k in secs
                       if k in ("Consumer Cyclical", "Financial",
                                "Basic Materials",
                                "Consumer Defensive"))
        rep.kv(sectors=len(secs), sectors_with_yoy=n_yoy,
               hyp_total_ttm_b=hyp.get("total_ttm_b"),
               finviz_named_sectors=fv_named)
        if n_yoy < 6:
            fails.append("sector yoy_pct thin: %d" % n_yoy)
        if hyp.get("total_ttm_b") is None:
            fails.append("hyperscalers.total_ttm_b missing")

        rep.section("3. Pages live (this-push)")
        checks = {
            "industry-rotation.html": (
                "Consumer Cyclical", "led by ", "total_ttm_b",
                "yoy_pct", "WHERE THE CAPEX IS GOING",
                "Leadership Ladder"),
            "why.html": ("IR.closes66", "closes_66"),
        }
        for pg_name, marks in checks.items():
            ok = False
            pg = ""
            for i in range(20):
                try:
                    pg = get("https://justhodl.ai/%s?cb=%d"
                             % (pg_name, time.time()))
                    if marks[0] in pg:
                        ok = True
                        break
                except Exception:
                    pass
                time.sleep(15)
            if not ok:
                fails.append("%s not live (missing %s)"
                             % (pg_name, marks[0]))
                continue
            for m in marks:
                if m not in pg:
                    fails.append("%s marker missing: %s"
                                 % (pg_name, m))

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3127.json").write_text(json.dumps(
        {"ops": 3127, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
