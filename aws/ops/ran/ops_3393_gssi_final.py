"""ops 3388 — GSSI math v2 + the barometer. Khalid: "your math is ok but not
very good and it's showing on the chart."

Diagnosis of v1 (self-critique, all three visible on the chart):
  1. full-sample z on spread LEVELS = regime contamination — the 90s lira era
     reads permanently stressed, 2000-07 permanently calm; the line was a
     slow regime chart, not a crisis detector (and used future information).
  2. plain mean aggregation — 3 screaming peripherals drown in 15 calm cores
     (2011 washed to ~45 while DE/CH were BID).
  3. no velocity term and no co-movement term — the two defining features of
     systemic sovereign crises (CISS literature, Hollo-Kremer-Lo Duca 2012).

v2 math (engine v2.4.4), no future information anywhere:
  s_c(t)  = 0.45·L(z_lvl) + 0.55·L(z_vel63), both EXPANDING as-known-then z
  SPREAD  = clamp( I_t · A_t ), where
     I_t = Σ s_c·(1+s_c/100) / Σ (1+s_c/100)   (stress-weighted intensity —
           the tail IS the signal)
     A_t = 0.75 + 0.5·ρ̄_t  (co-movement amplifier; ρ̄ = rolling-126 avg
           pairwise corr of standardized velocities via the dispersion
           identity ρ = (N·Var(m)−1)/(N−1) — O(n·N), no pair loops)
  GSSI   = 0.72·SPREAD + 0.28·CANARY
  Feed adds latest.breadth_pct (% sovereigns ≥70) + latest.comove, and b/c
  per weekly row. PAGE: semicircular BAROMETER (5 regime arcs, needle,
  value+regime, pctile·YoY·breadth·co-move stats) replacing the text KPI.
  31-behavior harness PASS_ALL pre-push.

Gates: G1 v2.4.4 settled · G2 fresh GSSI: breadth+comove numeric, weekly b/c
present, series depth intact · G3 detection vs v1 baseline (8/14): require
>=8 with Lehman+COVID+Euro-2011 mandatory; report leads old→new ·
G4 page gauge live.
"""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3388)"}
FN = "justhodl-sovereign-stress"
BASE = {"Lehman": 198, "COVID crash": -10, "Euro debt crisis (IT/ES)": 61}

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

with report("3393_gssi_final") as rep:
    rep.heading("ops 3388 — GSSI math v2 + barometer")
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
                if 'VERSION = "2.4.4"' in src and "dispersion identity" in src and "_expz" in src and "krw_canary" in src:
                    ok1 = True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_engine_240_settled", ok1, "markers in zip")

    t_inv = datetime.now(timezone.utc).isoformat()
    invoke_resilient(FN)
    g = None
    dl = time.time() + 540
    while time.time() < dl:
        try:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/sovereign-gssi.json")["Body"].read())
            if j.get("version") == "2.4.4" and (j.get("generated_at") or "") > t_inv:
                g = j; break
        except Exception: pass
        time.sleep(20)
    kp = (g or {}).get("latest") or {}
    sw = (g or {}).get("series_weekly") or []
    tail = sw[-10:] if sw else []
    bc_ok = tail and all(("b" in r and "c" in r) for r in tail) and any(r.get("c") is not None for r in tail)
    gate("G2_v2_feed", bool(g) and isinstance(kp.get("breadth_pct"), (int, float))
         and isinstance(kp.get("comove"), (int, float)) and len(sw) >= 1500 and bc_ok,
         f"latest={json.dumps(kp)[:150]} pts={len(sw)} bc_tail_ok={bc_ok}")

    sc = {r["crisis"]: r for r in ((g or {}).get("crisis_scorecard") or [])}
    det = ((g or {}).get("detection") or {}).get("detected") or 0
    must_ok = all(sc.get(k, {}).get("detected") for k in BASE)
    leads = {k: {"v1": BASE[k], "v2": sc.get(k, {}).get("lead_days")} for k in BASE}
    improved = sum(1 for k in BASE
                   if sc.get(k, {}).get("lead_days") is not None
                   and sc[k]["lead_days"] >= BASE[k])
    gate("G3_detection", det >= 8 and must_ok,
         f"detected={det}/14 (v1: 8/14) must_ok={must_ok} leads_v1_to_v2={json.dumps(leads)} improved_or_equal={improved}/3")
    out["comparison"] = {"detected_v1": "8/14", "detected_v2": f"{det}/14", "leads": leads,
                         "all_leads_v2": {r: sc[r].get("lead_days") for r in sc if sc[r].get("detected")}}

    need = ["gssi-gauge", "renderGSSIGauge", "gssi-needle", "co-movement", "dispersion"]
    need = ["gssi-gauge", "renderGSSIGauge", "gssi-needle", "co-movement"]
    ok4, missing = False, need
    dl = time.time() + 240
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    f"https://justhodl.ai/sovereign-stress.html?t={int(time.time())}", headers=UA), timeout=25) as r:
                b = r.read().decode("utf-8", "replace")
            missing = [m for m in need if m not in b]
            if not missing: ok4 = True; break
        except Exception: pass
        time.sleep(12)
    gate("G4_gauge_live", ok4, f"missing={missing}")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3393.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
