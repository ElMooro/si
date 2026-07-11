#!/usr/bin/env python3
"""ops 3108 -- FORENSIC + STRENGTH DESK v2.0 (Khalid: analyze stocks
the way hedge funds / accounting firms do -- fraud flags + financial
strength + industry comparison; dedicated page for best financials
and S&P 500 problem names). Extends the EXISTING forensic-screen
(Beneish M / Sloan / WC divergence / goodwill), never duplicates:
+three-statement strength composite (income 30 / balance 35 / cash
35, sector-aware for financials), financial-scores join (F+Z, one
call, 3092 precedent), extreme-dilution join from share-flows,
industry strength percentiles + sector medians, fortress +
problem_financials boards, full 503-name S&P via 10-worker threads
(840s/1024MB). forensic.html gains both boards. 3107 lesson: deployed env N_TICKERS=200 overrides the code default -- env now set to 503 explicitly."""
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
UA = {"User-Agent": "Mozilla/5.0 ops-3108", "Cache-Control": "no-cache"}


def get(url):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=30).read().decode(
        "utf-8", "replace")


def main():
    fails, warns = [], []
    with report("3108_forensic_strength") as rep:
        rep.section("1. Deploy + config + invoke")
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
        env = (L.get_function_configuration(FunctionName=FN)
               .get("Environment", {}).get("Variables", {}) or {})
        env["N_TICKERS"] = "503"
        L.update_function_configuration(FunctionName=FN, Timeout=840,
                                        MemorySize=1024,
                                        Environment={"Variables": env})
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
                    if cand.get("version") == "2.0.0" and \
                            (cand.get("n_universe_attempted")
                             or 0) > 400:
                        d = cand
                        break
            except Exception:
                pass
        if not d:
            fails.append("no fresh v2.0.0 forensic doc")
            _fin(rep, fails, warns); sys.exit(1)
        rep.kv(n_scored=d.get("n_scored_ok"),
               n_strength=d.get("n_strength"),
               dur=d.get("duration_s"))
        if (d.get("n_scored_ok") or 0) < 400:
            fails.append("S&P coverage thin: %s" % d.get("n_scored_ok"))
        if (d.get("n_strength") or 0) < 350:
            fails.append("strength coverage thin: %s"
                         % d.get("n_strength"))
        ft = d.get("fortress_financials") or []
        pb = d.get("problem_financials") or []
        rep.kv(n_fortress=len(ft), n_problems=len(pb),
               fortress_top=json.dumps([(r.get("symbol"),
                                         r.get("strength_grade"),
                                         r.get("strength_score"))
                                        for r in ft[:5]]),
               problems_top=json.dumps([(r.get("symbol"),
                                         r.get("concern_score"),
                                         r.get("strength_grade"))
                                        for r in pb[:5]]))
        if not ft:
            fails.append("fortress board empty")
        elif any((r.get("strength_score") or 0) < 70 for r in ft[:10]):
            fails.append("fortress top-10 has strength <70")
        if not pb:
            fails.append("problem board empty")
        bad_pb = [r.get("symbol") for r in pb
                  if (r.get("concern_score") or 0) < 40
                  and (r.get("strength_score") or 0) > 35]
        if bad_pb:
            fails.append("problem board members failing neither "
                         "gate: %s" % bad_pb[:5])
        allr = d.get("all_results") or ft + pb
        n_pct = sum(1 for r in allr if r.get("industry_pctile")
                    is not None)
        rep.kv(n_pctile=n_pct)
        if n_pct < 300:
            warns.append("industry percentile coverage %d" % n_pct)
        if not d.get("sector_strength_medians"):
            fails.append("sector medians missing")

        rep.section("3. Page live (this-push)")
        pg = ""
        ok = False
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/forensic.html?cb=%d"
                         % time.time())
                if "fortress-body" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(18)
        if not ok:
            fails.append("forensic.html v2 not live")
        else:
            for m in ("Best financials", "Problem financials",
                      "problems-body", "strength_grade",
                      "industry_pctile"):
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
    (AWS_DIR / "ops" / "reports" / "3108.json").write_text(json.dumps(
        {"ops": 3108, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
