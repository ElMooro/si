#!/usr/bin/env python3
"""ops 3104 -- BUYBACK BOARD x3 (Khalid: build all): (1) blackout
join -- per-name next-earnings [T-30,T+2] street proxy from the
market-wide FMP calendar (chunked 7d, ~8 calls/run) + aggregate
market_blackout chip from the sibling engine's doc; PUMP names get
the 'corporate bid off' warning exactly when it matters; (2)
share-flows join -- pe_ttm + insider buy/sell $ composed onto every
card, two desks one read; (3) dual-class collapse -- FOX/FOXA class
double-count killed: larger class on boards, sibling chipped +
board_suppressed. All composed/calendar -- ~8 extra calls total.
Version-gated 1.1.0."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

L = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-buyback-engine"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3104", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3104_buyback_x3") as rep:
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

        rep.section("2. Doc truth: three joins")
        d = None
        for _ in range(45):
            time.sleep(20)
            try:
                o = S3.get_object(Bucket=BUCKET,
                                  Key="data/buyback-engine.json")
                if (datetime.now(timezone.utc) - o["LastModified"]
                        ).total_seconds() < 1500:
                    cand = json.loads(o["Body"].read())
                    if cand.get("version") == "1.1.0":
                        d = cand
                        break
            except Exception:
                pass
        if not d:
            fails.append("no fresh v1.1.0 doc")
            _fin(rep, fails, warns); sys.exit(1)
        tk = d.get("tickers") or {}
        rows = list(tk.values())
        n_bo = sum(1 for v in rows if v.get("in_blackout")
                   or v.get("days_to_blackout") is not None
                   or v.get("next_earnings"))
        n_pe = sum(1 for v in rows if v.get("pe_ttm") is not None)
        n_ins = sum(1 for v in rows if v.get("insider_buy_usd_90d")
                    or v.get("insider_sell_usd_recent"))
        mb = d.get("market_blackout") or {}
        rep.kv(n=len(rows), n_blackout_fields=n_bo, n_pe=n_pe,
               n_insider=n_ins, market_blackout=json.dumps(mb))
        if n_bo < 40:
            fails.append("blackout join thin: %d (<40)" % n_bo)
        if n_pe < 40:
            fails.append("pe join thin: %d (<40)" % n_pe)
        if mb.get("pct") is None:
            warns.append("aggregate market_blackout absent")
        # pump-in-blackout consistency: warning string present
        for t, v in tk.items():
            if v.get("high_conviction_pump") and v.get("in_blackout") \
                    and "blackout" not in (v.get("why") or ""):
                fails.append("pump %s in blackout w/o warning" % t)
        # dual-class: FOX/FOXA never both on one board
        boards = ["high_conviction_pumps", "fresh_authorizations",
                  "net_shrinkers", "high_shareholder_yield",
                  "cheap_repurchasers"]
        for b in boards:
            syms = [r.get("symbol") for r in (d.get(b) or [])]
            names = {}
            for r2 in (d.get(b) or []):
                nm = (r2.get("company_name") or "").strip().lower()
                if nm:
                    names.setdefault(nm, []).append(r2.get("symbol"))
            dups = {k: v for k, v in names.items() if len(v) > 1}
            if dups:
                fails.append("dual-class dup on %s: %s"
                             % (b, list(dups.values())[:2]))
        if "FOX" in tk and "FOXA" in tk:
            sup = sum(1 for t in ("FOX", "FOXA")
                      if tk[t].get("board_suppressed"))
            rep.kv(foxes_suppressed=sup)
            if sup != 1:
                fails.append("FOX/FOXA collapse wrong: %d suppressed"
                             % sup)

        rep.section("3. Page live (this-push)")
        pg = ""
        ok = False
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/buybacks.html?cb=%d"
                         % time.time())
                if "insNet" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(18)
        if not ok:
            fails.append("buybacks.html v1.1 not live")
        else:
            for m in ("in_blackout", "dual_class_with", "pe_ttm",
                      "market blackout", "corporate bid off"):
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
    (AWS_DIR / "ops" / "reports" / "3104.json").write_text(json.dumps(
        {"ops": 3104, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
