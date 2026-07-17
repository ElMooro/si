"""ops 3384 — Sovereign Stress v2.0.0: global desk (CL/PE/NL), history, league.

Khalid: enhance sovereign-stress.html, polish UX, add Chile/Peru/Netherlands,
surface ALL engine data, improve exponentially. Shipped (everything existing
kept):
  ENGINE v2.0.0 — WGB module generalized beyond Asia via shared wgb_entry():
  new wgb_sovereigns dict = 7 sovereigns (KR/SG/HK/TW + CHILE/PERU/
  NETHERLANDS) with region + country-ETF tags; asia_sovereigns unchanged for
  back-compat. NEW daily HISTORY LEDGER (data/sovereign-stress-history.json,
  400d: europe score + per-country stress/CDS/composite) → Δ5/Δ21 deltas in
  feed. Transition SIGNALS via shared signals_emit: sovereign crossing hot
  (≥65 with Δ5≥+10) → country-ETF DOWN [5,21] into the grading loop.
  PAGE — 🌐 Global Sovereign Bond Desk (region-grouped, flags, rating/ETF
  badges, CDS + default-prob + spread + CB rate, Δ5 chips, signal banner);
  🏆 League Table unifying bond-desk stress · SovCISS pct · composite with
  60-session sparklines from the ledger; 📈 Markets & Spreads card (VIX,
  S&P, BTP-Bund etc + bond_market_read — previously computed, never shown);
  🔗 Cross-Checks card (crisis-composite/eurodollar/CDS-proxy + sources/
  errors/runtime meta); synthesis read paragraph into the hero. 13-behavior
  jsdom harness PASS_ALL pre-push; legacy sections proven intact.

Gates:
  G1  engine 2.0.0 settled (zip markers wgb_entry + HIST_KEY)
  G2  fresh feed: version 2.0.0; chile+peru+netherlands present in
      wgb_sovereigns with >=2 of 3 scored; deltas + signals_fired keys
  G3  history ledger exists, today's row present, includes chile
  G4  page live: all v2 markers
"""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) ops-3384"}
FN = "justhodl-sovereign-stress"

def invoke_resilient(fn, itype="Event", payload=b"{}", tries=6):
    for k in range(tries):
        try:
            return LAM.invoke(FunctionName=fn, InvocationType=itype, Payload=payload)
        except Exception as e:
            if "TooManyRequests" in str(e) or "Rate Exceeded" in str(e):
                time.sleep(15 * (k + 1)); continue
            raise
    raise RuntimeError("throttled")

def zsrc(fn):
    info = LAM.get_function(FunctionName=fn)
    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
        return zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")

with report("3384_sovereign_v2") as rep:
    rep.heading("ops 3384 — Sovereign Stress v2.0.0")
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
                if 'VERSION = "2.0.0"' in src and "wgb_entry" in src and "HIST_KEY" in src:
                    ok1 = True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_engine_v2_settled", ok1, "markers in zip")

    t_inv = datetime.now(timezone.utc).isoformat()
    invoke_resilient(FN, "Event")
    feed = None
    dl = time.time() + 480
    while time.time() < dl:
        try:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/sovereign-stress.json")["Body"].read())
            if j.get("version") == "2.0.0" and (j.get("generated_at") or "") > t_inv:
                feed = j; break
        except Exception: pass
        time.sleep(20)
    wg = (feed or {}).get("wgb_sovereigns") or {}
    new3 = {k: (wg.get(k) or {}).get("stress_0_100") for k in ("chile", "peru", "netherlands")}
    scored = sum(1 for v in new3.values() if isinstance(v, (int, float)))
    gate("G2_new_sovereigns", bool(feed) and all(k in wg for k in new3) and scored >= 2
         and "deltas" in (feed or {}) and "signals_fired" in (feed or {}),
         f"new3={new3} scored={scored} wgb_n={len(wg)} fired={len((feed or {}).get('signals_fired') or [])}")
    out["new3"] = new3

    ok3, det3 = False, "no ledger"
    try:
        h = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/sovereign-stress-history.json")["Body"].read())
        rows = h.get("rows") or []
        today = datetime.now(timezone.utc).date().isoformat()
        last = rows[-1] if rows else {}
        ok3 = last.get("date") == today and "chile" in (last.get("countries") or {})
        det3 = f"rows={len(rows)} last={last.get('date')} chile_in_last={'chile' in (last.get('countries') or {})}"
    except Exception as e:
        det3 = str(e)[:120]
    gate("G3_ledger_live", ok3, det3)

    need = ["wgb-desk", "sov-league", "renderSovV2", "Global Sovereign Bond Desk",
            "sovereign-stress-history.json", "synth-read", "sov-signals-banner"]
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

    if feed:
        out["snapshot"] = {"europe": (feed.get("europe_stress") or {}).get("score_0_100"),
                           "regime": (feed.get("europe_stress") or {}).get("regime"),
                           "wgb": {k: (v or {}).get("stress_0_100") for k, v in wg.items()},
                           "errors": len(feed.get("errors") or [])}
    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"], "|", json.dumps(out.get("snapshot", {}))[:220])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3384.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
