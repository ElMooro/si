#!/usr/bin/env python3
"""ops 3106 -- FUNDAMENTALS EVERYWHERE (Khalid: mcap on opportunities
+ all pages where necessary; pe/peg/fcf/p-s/dilution/buyback in all
stock engines AND taken into account when finding opportunities).
(A) share-flows v1.4.0 = the canonical per-name fundamentals map:
+fcf_yield_pct (TTM OCF-capex/mcap), rev/eps growth YoY, PEG -- all
from statements already fetched, zero new budget. (B) opportunity
engine scores capital return: EXTREME -35q / heavy dilution -15 /
shrinking +8 / net-bb>=2% +6, death-spiral + dilution risk strings,
shrinking-float highlight; rows carry market_cap + capital_return
{pe,peg,ps,fcf,sh_yoy,net_bb,read}. (C) opportunities.html chips:
MCAP/PE/PEG/P-S/FCF. (D) master-rank cards: same fundamentals line.
3105 lesson (and 3097's before it): opportunities doc rows live under key 'all' -- gate now reads all/opportunities/rows; skip re-invoke if a joined doc <3h already exists. Verify: v1.4.0 gate, AAPL bands (fcf 1-6, peg 0.5-12), opp rows
joined, page markers both pages."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

L = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3106", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def deploy(fn):
    src = (AWS_DIR / "lambdas" / fn / "source" /
           "lambda_function.py").read_text()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
    for att in range(6):
        try:
            L.update_function_code(FunctionName=fn,
                                   ZipFile=buf.getvalue())
            break
        except Exception as e:
            if "ResourceConflict" not in str(e) or att == 5:
                raise
            time.sleep(15)
    for _ in range(20):
        if L.get_function_configuration(FunctionName=fn).get(
                "LastUpdateStatus") in ("Successful", None):
            return
        time.sleep(6)


def fresh(key, gate, tries=45, maxage=1500):
    for _ in range(tries):
        time.sleep(20)
        try:
            o = S3.get_object(Bucket=BUCKET, Key=key)
            if (datetime.now(timezone.utc) - o["LastModified"]
                    ).total_seconds() < maxage:
                d = json.loads(o["Body"].read())
                if gate(d):
                    return d
        except Exception:
            pass
    return None


def main():
    fails, warns = [], []
    with report("3106_fundamentals_everywhere") as rep:
        rep.section("1. share-flows v1.4.0")
        d = None
        try:
            o = S3.get_object(Bucket=BUCKET, Key="data/share-flows.json")
            cand = json.loads(o["Body"].read())
            if cand.get("version") == "1.4.0" and (
                    datetime.now(timezone.utc) - o["LastModified"]
                    ).total_seconds() < 21600:
                d = cand
                rep.kv(sf_source="existing v1.4.0 doc")
        except Exception:
            pass
        if not d:
            deploy("justhodl-share-flows")
            L.invoke(FunctionName="justhodl-share-flows",
                     InvocationType="Event", Payload=b"{}")
            d = fresh("data/share-flows.json",
                      lambda x: x.get("version") == "1.4.0")
        if not d:
            fails.append("no fresh v1.4.0 share-flows")
            _fin(rep, fails, warns); sys.exit(1)
        tk = d.get("tickers") or {}
        aapl = tk.get("AAPL") or {}
        rep.kv(aapl_fcf=aapl.get("fcf_yield_pct"),
               aapl_peg=aapl.get("peg"),
               aapl_rev_g=aapl.get("rev_growth_yoy_pct"))
        fcf = aapl.get("fcf_yield_pct")
        if fcf is None or not 1 <= fcf <= 6:
            fails.append("AAPL fcf_yield %s outside 1-6" % fcf)
        peg = aapl.get("peg")
        if peg is not None and not 0.5 <= peg <= 12:
            fails.append("AAPL peg %s outside 0.5-12" % peg)
        n_fcf = sum(1 for v in tk.values()
                    if v.get("fcf_yield_pct") is not None)
        n_peg = sum(1 for v in tk.values() if v.get("peg") is not None)
        rep.kv(n_fcf=n_fcf, n_peg=n_peg, n=len(tk))
        if n_fcf < 80:
            fails.append("fcf coverage thin: %d" % n_fcf)
        insane = [t for t, v in tk.items()
                  if v.get("fcf_yield_pct") is not None
                  and abs(v["fcf_yield_pct"]) > 150]
        if insane:
            fails.append("fcf out of band: %s" % insane[:5])

        rep.section("2. opportunity engine join")
        deploy("justhodl-opportunity-engine")
        def _rows(x):
            return (x.get("all") or x.get("opportunities")
                    or x.get("rows") or [])

        def _joined(x):
            return any((r.get("capital_return") or r.get("market_cap"))
                       for r in _rows(x)[:120])
        od = None
        try:
            o = S3.get_object(Bucket=BUCKET,
                              Key="data/opportunities.json")
            if (datetime.now(timezone.utc) - o["LastModified"]
                    ).total_seconds() < 10800:
                cand = json.loads(o["Body"].read())
                if _joined(cand):
                    od = cand
                    rep.kv(opp_source="existing joined doc (<3h)")
        except Exception:
            pass
        if not od:
            L.invoke(FunctionName="justhodl-opportunity-engine",
                     InvocationType="Event", Payload=b"{}")
            od = fresh("data/opportunities.json", _joined, tries=50)
        if not od:
            fails.append("no fresh joined opportunities.json")
        else:
            rws = _rows(od)
            n_mc = sum(1 for r in rws if r.get("market_cap"))
            n_cr = sum(1 for r in rws if r.get("capital_return"))
            rep.kv(opp_rows=len(rws), opp_mcap=n_mc, opp_capret=n_cr)
            if rws and n_mc < len(rws) * 0.5:
                fails.append("opp market_cap coverage %d/%d"
                             % (n_mc, len(rws)))
            if rws and n_cr < 20:
                fails.append("opp capital_return join thin: %d" % n_cr)

        rep.section("3. Pages live (this-push)")
        for pg_name, marks in {
                "opportunities.html": ("MCAP ", "PEG ", "fcf_yield_pct"),
                "master-rank.html": ("sfLine", "window.SFMAP",
                                     "NET BB")}.items():
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
                fails.append("%s not live" % pg_name)
                continue
            for m in marks:
                if m not in pg:
                    fails.append("%s marker missing: %s" % (pg_name, m))

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3106.json").write_text(json.dumps(
        {"ops": 3106, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
