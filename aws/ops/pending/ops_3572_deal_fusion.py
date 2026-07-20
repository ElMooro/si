"""ops 3572 — deal-scanner FUSION: best-setups deal_context · master-ranker
deal_win overlay · morning-intelligence deal facts · alpha-families 7th card ·
why.html Deal Radar strip. Verification: zip markers on all 3 patched engines
(never sync-invoke heavy engines — 3308 doctrine), live master-ranker Event
run for behavior proof, served-page markers for both pages."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3572)"}
BUCKET = "justhodl-dashboard-live"

with report("3572_deal_fusion") as rep:
    rep.heading("ops 3572 — deal-win fusion across the fleet")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:400]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:360]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    def zip_marker(fn, markers, deadline):
        while time.time() < deadline:
            try:
                if LAM.get_function_configuration(FunctionName=fn).get("LastUpdateStatus") == "Successful":
                    info = LAM.get_function(FunctionName=fn)
                    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
                        src = zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
                    if all(m in src for m in markers):
                        return True
            except Exception:
                pass
            time.sleep(12)
        return False

    dl = time.time() + 660
    gate("G1_best_setups_ctx", zip_marker("justhodl-best-setups", ['_s["deal_context"]', "deal-scanner.json"], dl),
         "zip markers deal_context join")
    gate("G2_master_ranker_overlay", zip_marker("justhodl-master-ranker", ['t["deal_win"]', "deal_wins={n_deal}"], time.time() + 240),
         "zip markers deal_win overlay + fusion print")
    gate("G3_mi_facts", zip_marker("justhodl-morning-intelligence", ['"deal_scanner":"data/deal-scanner.json"', "fresh_deal_wins"], time.time() + 240),
         "zip markers feed + fresh_deal_wins fact")

    # G4 — master-ranker behavior: Event run regenerates its feed clean with the patch
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName="justhodl-master-ranker", InvocationType="Event", Payload=b"{}")
    fresh = False; n_overlay = 0; dl = time.time() + 480
    while time.time() < dl:
        try:
            h = S3C.head_object(Bucket=BUCKET, Key="data/master-ranker.json")
            if h["LastModified"].replace(tzinfo=timezone.utc) > t0:
                fresh = True
                j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/master-ranker.json")["Body"].read())
                rows = (j.get("top_tickers") or j.get("rows") or j.get("tickers") or [])
                if isinstance(rows, list):
                    n_overlay = sum(1 for r in rows if isinstance(r, dict) and r.get("deal_win"))
                break
        except Exception:
            pass
        time.sleep(15)
    gate("G4_ranker_behavior", fresh,
         f"feed regenerated post-patch · rows carrying deal_win overlay = {n_overlay} "
         "(count depends on a ranked name having a fresh <=72h deal — overlay attach is data-gated, presence of ANY is bonus proof)")

    # G5/G6 — served pages carry the fusion surfaces (pages.yml in parallel; bare URLs)
    def served(url, markers, deadline):
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30) as r:
                    html = r.read().decode("utf-8", "replace")
                if all(m in html for m in markers):
                    return True
            except Exception:
                pass
            time.sleep(15)
        return False

    gate("G5_alpha_families_card", served("https://justhodl.ai/alpha-families.html",
         ['id="c-deals"', "data/deal-scanner.json", "deal-win · [5,21,63] vs SPY"], time.time() + 330),
         "served markers: c-deals card + feed wire")
    gate("G6_why_deal_radar", served("https://justhodl.ai/why.html",
         ["jhDealRadar", "fillJHDealRadar"], time.time() + 240),
         "served markers: Deal Radar section + filler")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3572.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
