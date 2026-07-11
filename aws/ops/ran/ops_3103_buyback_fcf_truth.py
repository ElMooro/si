#!/usr/bin/env python3
"""ops 3103 -- BUYBACK-ENGINE FCF-yield truth (Khalid caught DCGO at
-32,762,564.1%). Root cause: FMP's derived freeCashFlowYield trusted
and x4'd. Fixes verified here: (1) FCF yield SELF-COMPUTED from the
cash-flow quarters already fetched (OCF - capex TTM / mcap), banded
to +/-150; (2) financials (banks/insurers/REITs/asset mgrs) carry
fcf_nm=True -- OCF there is float, not free cash (BAC 47% / MOH 62%
/ GS -50% class of nonsense); (3) |shares dYoY|>=80 -> extreme flag,
never surfaced on the fresh-authorizations board (TONX +56,978%);
(4) debt-funded buybacks take a -8 score haircut. 3102 lesson: MOH files as Healthcare/Healthcare Plans -- managed-care float is insurer economics, keyword added. Page shows n/m
(financial) + skull tag."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

L = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-buyback-engine"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3103", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3103_buyback_fcf_truth") as rep:
        rep.section("1. Deploy + invoke")
        import io, zipfile
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
        for _ in range(45):
            time.sleep(20)
            try:
                o = S3.get_object(Bucket=BUCKET,
                                  Key="data/buyback-engine.json")
                if (datetime.now(timezone.utc) - o["LastModified"]
                        ).total_seconds() < 1500:
                    cand = json.loads(o["Body"].read())
                    tt = cand.get("tickers") or {}
                    moh = tt.get("MOH") or {}
                    if tt and (moh.get("fcf_nm") or "MOH" not in tt):
                        d = cand
                        break
            except Exception:
                pass
        if not d:
            fails.append("no fresh post-fix buyback-engine.json")
            _fin(rep, fails, warns); sys.exit(1)
        tk = d.get("tickers") or {}
        rep.kv(n=len(tk))
        insane = [t for t, v in tk.items()
                  if v.get("fcf_yield_annualized") is not None
                  and abs(v["fcf_yield_annualized"]) > 150]
        if insane:
            fails.append("FCF yield out of band: %s" % insane[:5])
        dcgo = tk.get("DCGO") or {}
        rep.kv(dcgo_fcf=dcgo.get("fcf_yield_annualized"))
        if dcgo and dcgo.get("fcf_yield_annualized") is not None \
                and abs(dcgo["fcf_yield_annualized"]) > 150:
            fails.append("DCGO still insane")
        fins = [t for t in ("BAC", "GS", "C", "MOH", "AMP", "COF")
                if t in tk]
        bad_fin = [t for t in fins if not tk[t].get("fcf_nm")]
        rep.kv(fins_checked=fins, bad_fin=bad_fin)
        if fins and bad_fin:
            fails.append("financials without fcf_nm: %s" % bad_fin)
        n_ext = sum(1 for v in tk.values() if v.get("extreme"))
        fa = d.get("fresh_authorizations") or []
        ext_on_fa = [r.get("symbol") for r in fa if r.get("extreme")]
        rep.kv(n_extreme=n_ext, ext_on_fresh_auth=ext_on_fa)
        if ext_on_fa:
            fails.append("extreme names on fresh-auth board: %s"
                         % ext_on_fa)
        over = [t for t, v in tk.items() if v.get("debt_funded")
                and (v.get("buyback_score") or 0) > 92]
        if over:
            fails.append("debt-funded score >92 (penalty not "
                         "applied): %s" % over[:5])

        rep.section("3. Page live (this-push)")
        ok = False
        pg = ""
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/buybacks.html?cb=%d"
                         % time.time())
                if "n/m (financial)" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(18)
        if not ok:
            fails.append("buybacks.html n/m marker not live")
        elif "extreme dilution" not in pg:
            fails.append("skull tag marker missing")

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3103.json").write_text(json.dumps(
        {"ops": 3103, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
