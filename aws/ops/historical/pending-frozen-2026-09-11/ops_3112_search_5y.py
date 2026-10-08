#!/usr/bin/env python3
"""ops 3112 -- SEARCH = FULL 5-YEAR READ (Khalid: typing a stock must
pull latest financials, chart 5 years, compare P/E / P/S / PEG / FCF
to the industry). Engine v2.2: fetch_history (annual income+cashflow
limit=5, 2 extra threaded calls/name) -> per-name history [fy,
revenue, net_income, eps, gross/op margin, fcf]; pe/ps/peg/fcf/mcap
joined from share-flows onto every forensic row;
sector_valuation_medians (median pe/ps/peg/fcf per sector). Page
lookup: valuation chips vs sector median (green=better) + six 5-year
SVG bar charts (revenue, NI, FCF, gross margin, op margin, EPS) with
per-year tooltips. AAPL truth: history>=4 rows, latest revenue
350-500B, pe present, sector medians non-empty."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

L = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-forensic-screen"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3112", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3112_search_5y") as rep:
        rep.section("1. Deploy + invoke")
        src = (AWS_DIR / "lambdas" / FN / "source" /
               "lambda_function.py").read_text()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        for att in range(6):
            try:
                L.update_function_code(FunctionName=FN,
                                       ZipFile=buf.getvalue())
                break
            except Exception as e:
                if "ResourceConflict" not in str(e) or att == 5:
                    raise
                time.sleep(15)
        for _ in range(20):
            if L.get_function_configuration(FunctionName=FN).get(
                    "LastUpdateStatus") in ("Successful", None):
                break
            time.sleep(6)
        L.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")

        rep.section("2. Doc truth")
        d = None
        for _ in range(50):
            time.sleep(20)
            try:
                o = S3.get_object(Bucket=BUCKET,
                                  Key="data/forensic-screen.json")
                if (datetime.now(timezone.utc) - o["LastModified"]
                        ).total_seconds() < 1700:
                    cand = json.loads(o["Body"].read())
                    if cand.get("version") == "2.2.0":
                        d = cand
                        rep.kv(doc_mb=round(
                            len(o["Body"].read() or b"") / 1e6, 2)
                            if False else round(len(json.dumps(
                                cand)) / 1e6, 2))
                        break
            except Exception:
                pass
        if not d:
            fails.append("no fresh v2.2.0 doc")
            _fin(rep, fails, warns); sys.exit(1)
        allr = d.get("all_results") or []
        n_hist = sum(1 for r in allr
                     if len(r.get("history") or []) >= 4)
        n_pe = sum(1 for r in allr if r.get("pe_ttm") is not None)
        vm = d.get("sector_valuation_medians") or {}
        rep.kv(n=len(allr), n_hist=n_hist, n_pe=n_pe,
               n_sector_val_medians=len(vm))
        if n_hist < 400:
            fails.append("history coverage thin: %d" % n_hist)
        if n_pe < 350:
            fails.append("pe join thin: %d" % n_pe)
        if len(vm) < 5:
            fails.append("sector valuation medians thin: %d" % len(vm))
        aapl = next((r for r in allr if r.get("symbol") == "AAPL"), {})
        hist = aapl.get("history") or []
        rep.kv(aapl_hist_n=len(hist),
               aapl_latest=json.dumps(hist[-1] if hist else {})[:180],
               aapl_pe=aapl.get("pe_ttm"))
        if len(hist) < 4:
            fails.append("AAPL history %d rows (<4)" % len(hist))
        else:
            rev = (hist[-1] or {}).get("revenue") or 0
            if not 3.5e11 <= rev <= 5.5e11:
                fails.append("AAPL latest revenue %s outside "
                             "350-550B band" % rev)
        if aapl.get("pe_ttm") is None:
            fails.append("AAPL pe join missing")

        rep.section("3. Page live (this-push)")
        pg = ""
        ok = False
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/forensic.html?cb=%d"
                         % time.time())
                if "sparkbar" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(18)
        if not ok:
            fails.append("forensic.html 5y charts not live")
        else:
            for m in ("5 FISCAL YEARS", "vs sector",
                      "sector_valuation_medians", "FREE CASH FLOW",
                      "OPERATING MARGIN"):
                if m not in pg:
                    fails.append("page marker missing: %s" % m)

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3112.json").write_text(json.dumps(
        {"ops": 3112, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
