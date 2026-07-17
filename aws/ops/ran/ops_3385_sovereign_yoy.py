"""ops 3385 — Sovereign YoY chart: stress vs 12 months ago, all countries.

Khalid: visual chart of every country's stress as YoY % change from 12m ago.
The daily ledger is one day old — but the engine already holds full ECB
series in-hand each run, and FRED/OECD carries 12m of 10Y yields for three
bond-desk names. Shipped honestly:
  ENGINE v2.1.0 — CISS + SovCISS entries enriched in-loop with yoy_pct +
  level_12m (pct_change/val_days_ago on the series already fetched); WGB
  sovereigns get yield_yoy_bp via FRED OECD (NL/KR/CL) with an explicit
  yoy_note; the four without any 12m source carry the note "ledger
  accruing — true stress YoY unlocks automatically". Assembled payload
  yoy_chart, sorted by |move|.
  PAGE — 📊 tornado card, center-zero dual-direction bars, two labeled
  groups: ECB gauges (true 12m history, red=up/green=down, level_12m→level
  annotated) and bond-desk 10Y-yield Δ12m proxy (Δbp). 18-behavior harness
  PASS_ALL pre-push.

Gates:
  G1  engine 2.1.0 settled (yoy markers in zip)
  G2  fresh feed: yoy_chart with >=6 sovciss/ciss numeric yoy_pct entries,
      >=2 wgb_yield numeric Δbp, and accruing notes on the source-less
  G3  page live: yoy-chart + renderYoY + group-label markers
"""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3385)"}
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

with report("3385_sovereign_yoy") as rep:
    rep.heading("ops 3385 — Sovereign YoY chart")
    out = {"gates": {}}; fails = []
    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:330]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:280]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    ok1 = False
    dl = time.time() + 300
    while time.time() < dl:
        try:
            if LAM.get_function_configuration(FunctionName=FN).get("LastUpdateStatus") == "Successful":
                src = zsrc(FN)
                if 'VERSION = "2.1.0"' in src and "yoy_chart" in src and "WGB_YIELD_FRED" in src:
                    ok1 = True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_engine_211_settled", ok1, "markers in zip")

    t_inv = datetime.now(timezone.utc).isoformat()
    invoke_resilient(FN)
    feed = None
    dl = time.time() + 480
    while time.time() < dl:
        try:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/sovereign-stress.json")["Body"].read())
            if j.get("version") == "2.1.0" and (j.get("generated_at") or "") > t_inv:
                feed = j; break
        except Exception: pass
        time.sleep(20)
    yc = (feed or {}).get("yoy_chart") or []
    ecb = [r for r in yc if r.get("kind") in ("sovciss", "ciss") and isinstance(r.get("yoy_pct"), (int, float))]
    wy = [r for r in yc if r.get("kind") == "wgb_yield" and isinstance(r.get("yoy_bp"), (int, float))]
    accr = [r for r in yc if r.get("kind") == "wgb_yield" and r.get("yoy_bp") is None and "accru" in (r.get("note") or "")]
    gate("G2_yoy_in_feed", bool(feed) and len(ecb) >= 6 and len(wy) >= 2 and len(accr) >= 1,
         f"ecb_numeric={len(ecb)} yield_proxy={len(wy)} accruing={len(accr)} total={len(yc)}")
    out["sample"] = {"top_moves": [{r.get('name'): r.get('yoy_pct')} for r in ecb[:4]],
                     "yield_proxy": [{r.get('name'): r.get('yoy_bp')} for r in wy]}

    need = ["yoy-chart", "renderYoY", "true 12-month history", "Δ12m proxy", "Year over Year"]
    ok3, missing = False, need
    dl = time.time() + 240
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    f"https://justhodl.ai/sovereign-stress.html?t={int(time.time())}", headers=UA), timeout=25) as r:
                b = r.read().decode("utf-8", "replace")
            missing = [m for m in need if m not in b]
            if not missing: ok3 = True; break
        except Exception: pass
        time.sleep(12)
    gate("G3_page_live", ok3, f"missing={missing}")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"], "|", json.dumps(out.get("sample", {}))[:240])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3385.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
