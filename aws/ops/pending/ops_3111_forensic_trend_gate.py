#!/usr/bin/env python3
"""ops 3111 -- forensic desk x3 (Khalid, one by one): (1) sector
dropdown filtering all four boards; (2) M-SCORE TREND -- quarterly
TTM windows (limit=9, same 3 calls) give M(now) vs M(one quarter
ago); deterioration >= +0.15 toward the zone flags m_deteriorating,
+10 concern, DETERIORATING why-line + dM chip in the lookup; annual
fallback for short histories; (3) concern_score wired into the
opportunity engine as a QUALITY RISK GATE like dilution: >=60 -> -20
quality, >=40 -> -10 + plain-English forensic risk string; rows
carry forensic{concern,strength_grade,m_deteriorating}. 3110 lesson: dM computed across differing component sets is an artifact -- delta now requires matching components and |dM|<=3, else m_trend_suspect."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report

L = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3111", "Cache-Control": "no-cache"}


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


def main():
    fails, warns = [], []
    with report("3111_forensic_trend_gate") as rep:
        rep.section("1. Forensic trend")
        deploy("justhodl-forensic-screen")
        L.invoke(FunctionName="justhodl-forensic-screen",
                 InvocationType="Event", Payload=b"{}")
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
                    has_tr = any(r.get("m_score_delta_q") is not None
                                 for r in allr)
                    sane = all(abs(r.get("m_score_delta_q") or 0) <= 3
                               for r in allr)
                    if has_tr and sane:
                        d = cand
                        break
            except Exception:
                pass
        if not d:
            fails.append("no fresh trend doc")
            _fin(rep, fails, warns); sys.exit(1)
        allr = d.get("all_results") or []
        n_tr = sum(1 for r in allr
                   if r.get("m_score_delta_q") is not None)
        n_det = sum(1 for r in allr if r.get("m_deteriorating"))
        rep.kv(n_scored=d.get("n_scored_ok"), n_trend=n_tr,
               n_deteriorating=n_det)
        if n_tr < 250:
            fails.append("trend coverage thin: %d" % n_tr)
        big = [r["symbol"] for r in allr
               if abs(r.get("m_score_delta_q") or 0) > 3]
        if big:
            fails.append("unguarded dM values: %s" % big[:5])
        rep.kv(n_trend_suspect=sum(1 for r in allr
                                   if r.get("m_trend_suspect")))

        rep.section("2. Opportunity forensic gate")
        deploy("justhodl-opportunity-engine")

        def _rows(x):
            return (x.get("all") or x.get("opportunities")
                    or x.get("rows") or [])

        def _gated(x):
            return any(r.get("forensic") for r in _rows(x)[:200])
        od = None
        try:
            o = S3.get_object(Bucket=BUCKET,
                              Key="data/opportunities.json")
            if (datetime.now(timezone.utc) - o["LastModified"]
                    ).total_seconds() < 10800:
                cand = json.loads(o["Body"].read())
                if _gated(cand):
                    od = cand
        except Exception:
            pass
        if not od:
            L.invoke(FunctionName="justhodl-opportunity-engine",
                     InvocationType="Event", Payload=b"{}")
            for _ in range(50):
                time.sleep(20)
                try:
                    o = S3.get_object(Bucket=BUCKET,
                                      Key="data/opportunities.json")
                    if (datetime.now(timezone.utc) - o["LastModified"]
                            ).total_seconds() < 1700:
                        cand = json.loads(o["Body"].read())
                        if _gated(cand):
                            od = cand
                            break
                except Exception:
                    pass
        if not od:
            fails.append("no forensic-gated opportunities doc")
        else:
            rws = _rows(od)
            n_fo = sum(1 for r in rws if r.get("forensic"))
            n_risk = sum(1 for r in rws
                         if any("Forensic accounting red flags"
                                in (x or "")
                                for x in (r.get("risks") or [])))
            rep.kv(opp_rows=len(rws), opp_forensic=n_fo,
                   opp_forensic_risks=n_risk)
            if n_fo < 100:
                fails.append("opp forensic join thin: %d" % n_fo)

        rep.section("3. Page live (this-push)")
        pg = ""
        ok = False
        for i in range(20):
            try:
                pg = get("https://justhodl.ai/forensic.html?cb=%d"
                         % time.time())
                if "secf" in pg:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(18)
        if not ok:
            fails.append("forensic.html sector filter not live")
        else:
            for m in ("SECTOR FILTER", "m_score_delta_q",
                      "DETERIORATING", "All sectors"):
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
    (AWS_DIR / "ops" / "reports" / "3111.json").write_text(json.dumps(
        {"ops": 3111, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
