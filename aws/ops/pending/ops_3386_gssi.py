"""ops 3386 — Global Sovereign Stress Index (1990→) + SG/HK sources + CH + brain scan.

Khalid: (1) fill SG/HK in the yield-Δ12m proxy; (2) build a NEW index from all
sovereigns on the page, line-chart its YoY back to 1990, validate against
actual crisis start dates; (3) add Switzerland (famous canary — incl Swiss
unemployment) to the system; (4) read the brain for more canary countries.

Shipped: engine v2.2.0 — Switzerland on the WGB desk (EWL) + FRED yield proxy;
SG via MAS SGS datastore + HK via HKMA API ladders; NEW build_gssi(): 18 10Y
spreads vs Bund/UST + canary block (CH unemployment 12m-change, CHF & JPY
safe-haven 63d inverted, gold), full-own-history z → logistic 0-100 →
70/30 weighted mean of present components, identical across eras; full-sample
percentile, YoY, Δ6m; crisis scorecard vs 14 dated events (rule: pctile≥85
crossing OR Δ6m≥+12 in [start−270d, start+60d]) with lead/lag days; writes
data/sovereign-gssi.json. Page: line chart (YoY default + Level toggle,
ranges, crisis vlines + amber warning dots, crosshair), scorecard table,
canary strip. 27-behavior harness PASS_ALL pre-push.

Gates: G0 brain canary scan · G1 deploy · G2 CH scored + SG/HK proxy ≥1
numeric · G3 GSSI feed depth/validation (≥8/14 detected, Lehman+COVID+
Euro-2011 mandatory) · G4 page live.
"""
import io, json, re, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3386)"}
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

with report("3386_gssi") as rep:
    rep.heading("ops 3386 — GSSI 1990-index + SG/HK + Switzerland + brain scan")
    out = {"gates": {}}; fails = []
    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:360]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:300]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    # ── G0: brain canary scan ──
    counts, scan_src = {}, None
    try:
        raw = None
        for key in ("data/brain.json", "data/tv-notes-mirror.json", "data/notes-intel.json"):
            try:
                raw = S3C.get_object(Bucket="justhodl-dashboard-live", Key=key)["Body"].read().decode("utf-8", "replace").lower()
                scan_src = key; break
            except Exception: continue
        KW = {"switzerland": r"switzerland|swiss|\bchf\b|franc", "japan": r"\bjapan\b|\byen\b|\bjpy\b|jgb",
              "sweden": r"sweden|riksbank|\bsek\b", "norway": r"norway|\bnok\b", "korea": r"korea|\bkrw\b",
              "turkey": r"turkey|\btry\b|lira", "mexico": r"mexico|\bmxn\b|peso", "argentina": r"argentin",
              "china": r"\bchina\b|\byuan\b|\bcny\b", "gold": r"\bgold\b", "canary": r"canar"}
        if raw:
            for k, pat in KW.items():
                counts[k] = len(re.findall(pat, raw))
        covered = {"switzerland", "japan", "sweden", "korea", "mexico", "china", "gold"}
        hot_uncovered = [k for k, v in sorted(counts.items(), key=lambda x: -x[1])
                        if v >= 20 and k not in covered and k != "canary"]
        out["brain"] = {"source": scan_src, "counts": counts, "hot_uncovered": hot_uncovered}
    except Exception as e:
        out["brain"] = {"error": str(e)[:120]}
    gate("G0_brain_scan", scan_src is not None,
         f"src={scan_src} top={sorted(counts.items(), key=lambda x: -x[1])[:6]} uncovered={out['brain'].get('hot_uncovered')}")

    # ── G1: engine settled ──
    ok1 = False
    dl = time.time() + 300
    while time.time() < dl:
        try:
            if LAM.get_function_configuration(FunctionName=FN).get("LastUpdateStatus") == "Successful":
                src = zsrc(FN)
                if 'VERSION = "2.2.0"' in src and "build_gssi" in src and "hk_yield_pair" in src and '"switzerland"' in src:
                    ok1 = True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_engine_220_settled", ok1, "markers in zip")

    # ── G2: run + CH/SG/HK ──
    t_inv = datetime.now(timezone.utc).isoformat()
    invoke_resilient(FN)
    feed = None
    dl = time.time() + 540
    while time.time() < dl:
        try:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/sovereign-stress.json")["Body"].read())
            if j.get("version") == "2.2.0" and (j.get("generated_at") or "") > t_inv:
                feed = j; break
        except Exception: pass
        time.sleep(20)
    wg = (feed or {}).get("wgb_sovereigns") or {}
    ch = (wg.get("switzerland") or {})
    sg_bp = (wg.get("singapore") or {}).get("yield_yoy_bp")
    hk_bp = (wg.get("hong_kong") or {}).get("yield_yoy_bp")
    errs = " | ".join(e for e in ((feed or {}).get("errors") or []) if "yoy/" in e)
    gate("G2_ch_sg_hk", bool(feed) and isinstance(ch.get("stress_0_100"), (int, float))
         and (isinstance(sg_bp, (int, float)) or isinstance(hk_bp, (int, float))),
         f"CH={ch.get('stress_0_100')} (CDS {ch.get('cds_bp')}bp) SG_bp={sg_bp} HK_bp={hk_bp} yoy_errs=[{errs[:120]}]")
    out["ch_sg_hk"] = {"switzerland": ch.get("stress_0_100"), "sg_bp": sg_bp, "hk_bp": hk_bp}

    # ── G3: GSSI feed validation ──
    g, ok3, det3 = None, False, "no gssi feed"
    dl = time.time() + 120
    while time.time() < dl:
        try:
            g = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/sovereign-gssi.json")["Body"].read())
            if (g.get("generated_at") or "") > t_inv: break
        except Exception: pass
        time.sleep(15)
    if g:
        sw = g.get("series_weekly") or []
        comps = [c for c in (g.get("components") or []) if c.get("stress") is not None]
        names = {c["name"] for c in comps}
        sc = g.get("crisis_scorecard") or []
        by = {r["crisis"]: r for r in sc}
        must = ["Lehman", "COVID crash", "Euro debt crisis (IT/ES)"]
        must_ok = all(by.get(mn, {}).get("detected") and by[mn].get("lead_days") is not None for mn in must)
        det = (g.get("detection") or {}).get("detected") or 0
        ok3 = (len(sw) >= 1500 and sw[0]["d"] <= "1991" and
               isinstance((g.get("latest") or {}).get("yoy_pct"), (int, float)) and
               len(comps) >= 14 and {"ch_unemployment", "chf_safe_haven", "mexico"} <= names and
               len(sc) == 14 and det >= 8 and must_ok)
        det3 = (f"pts={len(sw)} first={sw[0]['d'] if sw else None} now={json.dumps(g.get('latest'))[:90]} "
                f"alive_comps={len(comps)} detected={det}/14 must_ok={must_ok}")
        out["gssi"] = {"latest": g.get("latest"), "detected": f"{det}/14",
                       "leads": {mn: by.get(mn, {}).get("lead_days") for mn in must},
                       "elapsed_s": g.get("elapsed_s")}
    gate("G3_gssi_validated", ok3, det3)

    # ── G4: page live ──
    need = ["gssi-chart", "renderGSSI", "Global Sovereign Stress Index", "Crisis-detection scorecard",
            "sovereign-gssi.json", "gssi-canaries"]
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
    gate("G4_page_live", ok4, f"missing={missing}")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3386.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
