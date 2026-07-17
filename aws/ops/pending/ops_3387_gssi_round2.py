"""ops 3387 — round 2: SG/HK ladder retry + Turkey/Argentina (brain-mandated).

3386 shipped GSSI validated (8/14, Lehman +198d early) but SG/HK official-API
guesses 404'd. v2.3.0: SG ladder now covers MAS datastore_search (underscore
form) + legacy + apimg-gw; HK ladder covers 4 HKMA paths incl govt-bond-
benchmark-yield-daily. Brain scan (japan 1487 · china 828 · gold 813 all
covered; TURKEY 159 + ARGENTINA 31 uncovered) → both added to the WGB desk
(TUR/ARGT, region "EM Canaries"); Turkey 10Y also joins the GSSI spread block
(OECD, joins when its history begins). Page flag map extended.

Gates: G1 v2.3.0 settled · G2 probe report: SG/HK bp values or per-rung
errors visible; >=1 numeric required, both targeted · G3 turkey+argentina
scored on desk (Argentina CDS should light it up) · G4 GSSI regenerated
healthy with turkey present in components (alive or insufficient-history).
"""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3387)"}
FN = "justhodl-sovereign-stress"

def invoke_resilient(fn, tries=6):
    for k in range(tries):
        try:
            return LAM.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
        except Exception as e:
            if "TooManyRequests" in str(e) or "Rate Exceeded" in str(e):
                time.sleep(15 * (k + 1)); continue
            raise
    raise RuntimeError("throttled")

def zsrc(fn):
    info = LAM.get_function(FunctionName=fn)
    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
        return zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")

with report("3387_gssi_round2") as rep:
    rep.heading("ops 3387 — SG/HK retry + Turkey/Argentina")
    out = {"gates": {}}; fails = []
    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:360]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:300]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    ok1 = False
    dl = time.time() + 300
    while time.time() < dl:
        try:
            if LAM.get_function_configuration(FunctionName=FN).get("LastUpdateStatus") == "Successful":
                src = zsrc(FN)
                if 'VERSION = "2.3.0"' in src and "govt-bond-benchmark-yield-daily" in src and '"argentina"' in src:
                    ok1 = True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_engine_230_settled", ok1, "markers in zip")

    t_inv = datetime.now(timezone.utc).isoformat()
    invoke_resilient(FN)
    feed = None
    dl = time.time() + 540
    while time.time() < dl:
        try:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/sovereign-stress.json")["Body"].read())
            if j.get("version") == "2.3.0" and (j.get("generated_at") or "") > t_inv:
                feed = j; break
        except Exception: pass
        time.sleep(20)
    wg = (feed or {}).get("wgb_sovereigns") or {}
    sg_bp = (wg.get("singapore") or {}).get("yield_yoy_bp")
    hk_bp = (wg.get("hong_kong") or {}).get("yield_yoy_bp")
    errs = " | ".join(e for e in ((feed or {}).get("errors") or []) if "yoy/" in e)
    gate("G2_sg_hk", bool(feed) and (isinstance(sg_bp, (int, float)) or isinstance(hk_bp, (int, float))),
         f"SG_bp={sg_bp} HK_bp={hk_bp} errs=[{errs[:180]}]")

    tr = (wg.get("turkey") or {}); ar = (wg.get("argentina") or {})
    gate("G3_tr_ar_on_desk", isinstance(tr.get("stress_0_100"), (int, float))
         and isinstance(ar.get("stress_0_100"), (int, float)),
         f"TR={tr.get('stress_0_100')} (CDS {tr.get('cds_bp')}bp) AR={ar.get('stress_0_100')} (CDS {ar.get('cds_bp')}bp)")
    out["desk"] = {"turkey": {"s": tr.get("stress_0_100"), "cds": tr.get("cds_bp")},
                   "argentina": {"s": ar.get("stress_0_100"), "cds": ar.get("cds_bp")},
                   "sg_bp": sg_bp, "hk_bp": hk_bp}

    g, ok4, det4 = None, False, "no gssi"
    try:
        g = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/sovereign-gssi.json")["Body"].read())
        names = {c.get("name") for c in (g.get("components") or [])}
        det = (g.get("detection") or {}).get("detected") or 0
        ok4 = ((g.get("generated_at") or "") > t_inv and "turkey" in names
               and len(g.get("series_weekly") or []) >= 1500 and det >= 8)
        det4 = f"gen_fresh={(g.get('generated_at') or '')>t_inv} turkey_in={('turkey' in names)} detected={det}/14 now={json.dumps(g.get('latest'))[:80]}"
    except Exception as e:
        det4 = str(e)[:120]
    gate("G4_gssi_regen", ok4, det4)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3387.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
