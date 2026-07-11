#!/usr/bin/env python3
"""ops 3109 -- forensic desk refinement (Khalid): (1) financial-sector
Beneish/Sloan/WC legs ZEROED in concern + flags suppressed w/ honest
fin_suppressed_flags note (3108 problem board was bank-heavy on
spurious M-flags -- the model's own paper excludes financials);
financials now flag only via goodwill / dilution / weak strength.
(2) Search bar on forensic.html: typeahead over all 502 names, full
per-name read (grade + legs + industry %ile + sector median + every
forensic factor + plain-English why). (3) Historical-context block
removed. (4) A-grade / D-F strength KPIs added."""
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
UA = {"User-Agent": "Mozilla/5.0 ops-3109", "Cache-Control": "no-cache"}
FIN = {"Financial Services", "Financials", "Financial", "Insurance",
       "Real Estate", "Banks"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3109_forensic_fin_search") as rep:
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

        rep.section("2. Doc truth: financials clean")
        d = None
        for _ in range(50):
            time.sleep(20)
            try:
                o = S3.get_object(Bucket=BUCKET,
                                  Key="data/forensic-screen.json")
                if (datetime.now(timezone.utc) - o["LastModified"]
                        ).total_seconds() < 1700:
                    cand = json.loads(o["Body"].read())
                    allr = cand.get("all_results") or []
                    if cand.get("version") == "2.0.0" and any(
                            r.get("fin_suppressed_flags")
                            for r in allr):
                        d = cand
                        break
            except Exception:
                pass
        if not d:
            fails.append("no fresh fin-suppressed doc")
            _fin(rep, fails, warns); sys.exit(1)
        allr = d.get("all_results") or []
        pb = d.get("problem_financials") or []
        n_sup = sum(1 for r in allr if r.get("fin_suppressed_flags"))
        fin_pb = [r for r in pb if (r.get("sector") or "") in FIN]
        bad = [r.get("symbol") for r in fin_pb
               if not (r.get("goodwill_bloat_flag")
                       or r.get("dilution_flag")
                       or (r.get("strength_score") or 100) <= 35)]
        rep.kv(n_scored=d.get("n_scored_ok"), n_fin_suppressed=n_sup,
               n_problems=len(pb), n_fin_in_problems=len(fin_pb),
               problems_top=json.dumps(
                   [(r.get("symbol"), r.get("concern_score"),
                     r.get("strength_grade")) for r in pb[:5]]))
        if bad:
            fails.append("financials in problems without valid "
                         "gate: %s" % bad[:5])
        # spurious fin M-flags must be gone from headline flags
        fin_mflag = [r.get("symbol") for r in allr
                     if (r.get("sector") or "") in FIN
                     and r.get("m_flag")]
        if fin_mflag:
            fails.append("financials still m_flagged: %s"
                         % fin_mflag[:5])

        rep.section("3. Page live (this-push)")
        pg = ""
        ok = False
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/forensic.html?cb=%d"
                         % time.time())
                if "flookup" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(18)
        if not ok:
            fails.append("forensic.html search not live")
        else:
            for m in ("fsugg", "A-grade financials",
                      "fin_suppressed_flags", "SEARCH ANY"):
                if m not in pg:
                    fails.append("page marker missing: %s" % m)
            if "jhk-hist" in pg or "Historical Context" in pg:
                fails.append("historical-context block still present")

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3109.json").write_text(json.dumps(
        {"ops": 3109, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
