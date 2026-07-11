#!/usr/bin/env python3
"""ops 3096 -- SHARE-FLOWS fleet map (Khalid: track issuance,
management buying/selling + how much, share count shrink/grow,
dilution, buybacks -- on opportunities and everywhere it applies).
New composer justhodl-share-flows -> data/share-flows.json: per-name
sh_yoy/qoq, TTM buybacks + yield, issuance + %mcap, insider buy/sell
joins (from the existing insider desks, never re-fetched), read
classification + boards. Joined client-side on opportunities.html,
IR soldiers, accumulation reversal cards, chart-pro JHF badges.
3095 lessons: universe joined only 68 names (ranker/soldier doc
shapes) -> engine now unions the phase-detector ~700-name ring +
valuations heatmap; WHLR +74,502%% sh_yoy is REAL death-spiral
issuance, not bad data -> engine flags extreme=True +
EXTREME_DILUTION read, own board, main boards stay clean; verify
only fails on UNFLAGGED out-of-band values.
Sequential: create fn + schedule -> invoke -> map truth-bands ->
4 pages live."""
import json
import sys
import time
import urllib.request
import zipfile
import io
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

L = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
SCH = boto3.client("scheduler", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-share-flows"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3096",
      "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def ensure_function(rep, fails):
    src = (AWS_DIR / "lambdas" / FN / "source" /
           "lambda_function.py").read_text()
    cfg = json.loads((AWS_DIR / "lambdas" / FN /
                      "config.json").read_text())
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w",
                         zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
    donor = L.get_function_configuration(
        FunctionName="justhodl-confluence-meta")
    env = donor.get("Environment", {}).get("Variables", {}) or {}
    env = {k: v for k, v in env.items()
           if any(s in k for s in ("FMP", "FRED", "POLYGON"))}
    if "FMP_API_KEY" not in env and "FMP_KEY" in env:
        env["FMP_API_KEY"] = env["FMP_KEY"]
    def _wait_active():
        for _ in range(40):
            c = L.get_function_configuration(FunctionName=FN)
            if c.get("State") in ("Active", None) and \
                    c.get("LastUpdateStatus") in ("Successful",
                                                  None):
                return True
            time.sleep(8)
        return False

    try:
        L.get_function(FunctionName=FN)
        # deploy-lambdas creates this fn on the same push -- wait out
        # 'Creating' then update with conflict retry (3093 lesson)
        _wait_active()
        for att in range(6):
            try:
                L.update_function_code(FunctionName=FN,
                                       ZipFile=buf.getvalue())
                break
            except Exception as e:
                if "ResourceConflict" not in str(e) or att == 5:
                    raise
                time.sleep(15)
                _wait_active()
        for _ in range(20):
            st = L.get_function_configuration(
                FunctionName=FN).get("LastUpdateStatus")
            if st in ("Successful", None):
                break
            time.sleep(6)
        L.update_function_configuration(
            FunctionName=FN, Timeout=cfg["timeout"],
            MemorySize=cfg["memory"],
            Environment={"Variables": env})
        rep.kv(fn="updated")
    except L.exceptions.ResourceNotFoundException:
        L.create_function(
            FunctionName=FN, Runtime=cfg["runtime"],
            Role=cfg["role"], Handler=cfg["handler"],
            Code={"ZipFile": buf.getvalue()},
            Timeout=cfg["timeout"], MemorySize=cfg["memory"],
            Description=cfg["description"],
            Environment={"Variables": env})
        rep.kv(fn="created")
    for _ in range(20):
        st = L.get_function_configuration(
            FunctionName=FN).get("LastUpdateStatus")
        if st in ("Successful", None):
            return True
        time.sleep(6)
    fails.append("fn never settled")
    return False


def ensure_schedule(rep, warns):
    try:
        SCH.get_schedule(Name="justhodl-share-flows-daily",
                         GroupName="default")
        rep.kv(schedule="exists")
    except SCH.exceptions.ResourceNotFoundException:
        arn = L.get_function(FunctionName=FN)["Configuration"][
            "FunctionArn"]
        # 3094 lesson: don't guess the scheduler role -- copy the
        # RoleArn from an existing fleet schedule
        role = None
        try:
            for pg in SCH.get_paginator("list_schedules").paginate(
                    GroupName="default"):
                for it in pg.get("Schedules", []):
                    if it["Name"].startswith("justhodl"):
                        det = SCH.get_schedule(
                            Name=it["Name"], GroupName="default")
                        role = det["Target"]["RoleArn"]
                        break
                if role:
                    break
        except Exception as e:
            warns.append("role discovery: %s" % str(e)[:80])
        if not role:
            warns.append("no fleet schedule to copy role from -- "
                         "schedule skipped (engine still runs via "
                         "manual/ops invokes)")
            return
        SCH.create_schedule(
            Name="justhodl-share-flows-daily", GroupName="default",
            ScheduleExpression="cron(35 13 * * ? *)",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": arn, "RoleArn": role, "Input": "{}"})
        rep.kv(schedule="created 13:35 UTC daily")
    except Exception as e:
        warns.append("schedule: %s" % str(e)[:90])


def main():
    fails, warns = [], []
    with report("3096_share_flows") as rep:
        rep.section("1. Function + schedule")
        if not ensure_function(rep, fails):
            _fin(rep, fails, warns)
            sys.exit(1)
        ensure_schedule(rep, warns)

        rep.section("2. Invoke + map truth")
        L.invoke(FunctionName=FN, InvocationType="Event",
                 Payload=b"{}")
        d = None
        for _ in range(40):
            time.sleep(20)
            try:
                o = S3.get_object(Bucket=BUCKET,
                                  Key="data/share-flows.json")
                if (datetime.now(timezone.utc) - o["LastModified"]
                        ).total_seconds() < 1200:
                    d = json.loads(o["Body"].read())
                    break
            except Exception:
                pass
        if not d:
            fails.append("no fresh share-flows.json")
            _fin(rep, fails, warns)
            sys.exit(1)
        tk = d.get("tickers") or {}
        rep.kv(n_tickers=len(tk),
               fresh_fetched=d.get("fresh_fetched"),
               warns_engine=json.dumps(d.get("warns"))[:250])
        if len(tk) < 250:
            fails.append("map thin: %d names (<250)" % len(tk))
        bad = [t for t, v in tk.items()
               if ((v.get("buyback_yield_pct") or 0) > 30
                   or abs(v.get("sh_yoy_pct") or 0) > 80)
               and not v.get("extreme")]
        if bad:
            fails.append("unflagged out-of-band values: %s" % bad[:5])
        n_ext = sum(1 for v in tk.values() if v.get("extreme"))
        rep.kv(n_extreme_flagged=n_ext)
        uw = next((w for w in (d.get("warns") or [])
                   if w.startswith("universe:")), "")
        try:
            un = int(uw.split()[1])
        except Exception:
            un = 0
        rep.kv(universe_n=un)
        if un < 400:
            fails.append("universe still thin: %s (<400) -- "
                         "phase-ring join not effective" % un)
        aapl = tk.get("AAPL") or {}
        rep.kv(aapl=json.dumps(aapl)[:260],
               nvda=json.dumps(tk.get("NVDA") or {})[:200])
        if aapl:
            by = aapl.get("buyback_yield_pct")
            yo = aapl.get("sh_yoy_pct")
            if by is not None and not (1.0 <= by <= 5.0):
                fails.append("AAPL buyback yield %s outside 1-5%% "
                             "external truth band" % by)
            if yo is not None and yo > 0.5:
                fails.append("AAPL sh_yoy %s -- should be shrinking"
                             % yo)
        else:
            warns.append("AAPL not in universe today")
        bd = d.get("boards") or {}
        rep.kv(top_bb=json.dumps((bd.get("top_buybacks")
                                  or [])[:3])[:220],
               top_dil=json.dumps((bd.get("top_diluters")
                                   or [])[:3])[:220])
        if not bd.get("top_buybacks"):
            fails.append("top_buybacks board empty")

        rep.section("3. Four pages live (this-push markers)")
        pages = {
            "opportunities.html": ("sfChips", "SHARES ", "BUYBACK "),
            "industry-rotation.html": ("window.__sf", " BB '"),
            "accumulation.html": ("DILUTING", "window.__sf"),
            "chart-pro.html": ('g("share-flows.json")', '"DIL"'),
        }
        for pg_name, marks in pages.items():
            ok = False
            for i in range(20):
                try:
                    pg = get("https://justhodl.ai/%s?cb=%d"
                             % (pg_name, time.time()))
                    if marks[0] in pg:
                        ok = True
                        break
                except Exception:
                    pass
                time.sleep(18)
            if not ok:
                fails.append("%s join not live" % pg_name)
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
    (AWS_DIR / "ops" / "reports" / "3096.json").write_text(json.dumps(
        {"ops": 3096, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
