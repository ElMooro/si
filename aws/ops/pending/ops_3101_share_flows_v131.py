#!/usr/bin/env python3
"""ops 3101 -- SHARE-FLOWS v1.3.0 (Khalid: add P/E + search-bar
full profile -- market cap, shares outstanding, dilution, everything
important on any typed stock). Engine: /stable/ratios-ttm pe_ttm +
ps_ttm (quote has NO pe -- 3091 lesson), price / market_cap /
shares_outstanding stored from the quote payload already fetched
(zero extra names-budget), cache marker rolled to market_cap so v1.3
fields propagate inside the 420/day budget. Page: live typeahead
suggestions (prefix match, mcap+PE preview) + valuation header row
in the lookup card. Race-guard: ops updates fn code itself (wait
Active + conflict retry), invokes, version-gates on doc 1.3.0.
3100 lesson: /stable/quote has NO sharesOutstanding -> derived mcap/price; cache rows version-stamped (_v) so future field rolls are automatic. AAPL bands: mcap 2-6T, shares 13-17B, PE 20-55,
price 100-500."""
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
UA = {"User-Agent": "Mozilla/5.0 ops-3101",
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
    with report("3101_share_flows_v131") as rep:
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
                    cand = json.loads(o["Body"].read())
                    if cand.get("version") == "1.3.1":
                        d = cand
                        break
            except Exception:
                pass
        if not d:
            fails.append("no fresh v1.3.1 share-flows.json")
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
            mc = aapl.get("market_cap") or 0
            so = aapl.get("shares_outstanding") or 0
            pe = aapl.get("pe_ttm")
            pr = aapl.get("price")
            if not 2e12 <= mc <= 6e12:
                fails.append("AAPL market_cap %s outside 2-6T" % mc)
            if not 13e9 <= so <= 17e9:
                fails.append("AAPL shares_outstanding %s outside "
                             "13-17B" % so)
            if pe is None or not 20 <= pe <= 55:
                fails.append("AAPL pe_ttm %s outside 20-55" % pe)
            if pr is None or not 100 <= pr <= 500:
                fails.append("AAPL price %s outside 100-500" % pr)
            n_pe = sum(1 for v in tk.values()
                       if v.get("pe_ttm") is not None)
            n_mc = sum(1 for v in tk.values()
                       if v.get("market_cap"))
            rep.kv(n_pe=n_pe, n_mcap=n_mc)
            if n_mc < 100:
                fails.append("market_cap coverage thin: %d" % n_mc)
        else:
            warns.append("AAPL not in universe today")
        # v1.2 asserts (this push): net buyback / SBC / 3Y CAGR /
        # flags / threaded coverage
        v12 = [v for v in tk.values() if "buyback_net_ttm_usd" in v]
        rep.kv(n_v12_rows=len(v12),
               n_sbc=sum(1 for v in v12 if v.get("sbc_ttm_usd")),
               n_flagged=sum(1 for v in tk.values()
                             if v.get("flags")))
        if not v12:
            fails.append("no v1.2 rows (net buyback/SBC absent)")
        a2 = tk.get("AAPL") or {}
        if a2.get("buyback_net_yield_pct") is not None \
                and not (0.5 <= a2["buyback_net_yield_pct"] <= 5.0):
            fails.append("AAPL NET bb yield %s outside 0.5-5"
                         % a2["buyback_net_yield_pct"])
        if a2.get("total_shareholder_yield_pct") is not None \
                and not (0.5 <= a2["total_shareholder_yield_pct"]
                         <= 6.0):
            fails.append("AAPL total yield %s outside 0.5-6"
                         % a2["total_shareholder_yield_pct"])
        bd = d.get("boards") or {}
        rep.kv(top_bb=json.dumps((bd.get("top_buybacks")
                                  or [])[:3])[:220],
               top_dil=json.dumps((bd.get("top_diluters")
                                   or [])[:3])[:220])
        if not bd.get("top_buybacks"):
            fails.append("top_buybacks board empty")
        rep.kv(sbc_washers=len(bd.get("sbc_washers") or []),
               mgmt_selling=len(bd.get(
                   "mgmt_selling_into_buyback") or []))

        rep.section("3. Search-bar profile live (this-push)")
        pages = {"share-flows.html":
                 ("pe_ttm", "shares_outstanding", "sugg",
                  "Shares outstanding", "P/E (TTM)")}
        for pg_name, marks in pages.items():
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
                time.sleep(18)
            if not ok:
                fails.append("%s v1.3 not live" % pg_name)
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
    (AWS_DIR / "ops" / "reports" / "3101.json").write_text(json.dumps(
        {"ops": 3101, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
