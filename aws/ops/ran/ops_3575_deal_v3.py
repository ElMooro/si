"""ops 3575 — deal-scanner v3 institutional layer live-proof: settle + markers,
fresh v3 feed (event taxonomy, universe join ≥4200, guards), history ledger
born, signals honor the institutional bar, page boards served. Runs AFTER
ops_3574 (alphabetical) so the exhaustive universe is already on S3."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
DDB = boto3.client("dynamodb", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3575)"}
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-deal-scanner"

with report("3575_deal_v3") as rep:
    rep.heading("ops 3575 — deal-scanner v3 institutional layer")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:420]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:380]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G1 settle + zip markers + config 1024/600
    ok1 = False; cfg = {}; dl = time.time() + 660
    while time.time() < dl:
        try:
            cfg = LAM.get_function_configuration(FunctionName=FN)
            if cfg.get("LastUpdateStatus") == "Successful":
                info = LAM.get_function(FunctionName=FN)
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
                    src = zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
                if all(m in src for m in ('VERSION = "3.0.0"', "def classify_event", "def load_8k_set",
                                          "deal-history.json", "pop_since_announce",
                                          'd.get("event_type") != "ma_target"')):
                    ok1 = True; break
        except Exception:
            pass
        time.sleep(12)
    gate("G1_settled_v3", ok1 and cfg.get("MemorySize") == 1024 and cfg.get("Timeout") == 600,
         f"markers ok={ok1} mem={cfg.get('MemorySize')} timeout={cfg.get('Timeout')}")

    # G2 fresh v3 run
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    j = None; dl = time.time() + 560
    while time.time() < dl:
        try:
            o = S3C.get_object(Bucket=BUCKET, Key="data/deal-scanner.json")
            cand = json.loads(o["Body"].read())
            if cand.get("version") == "3.0.0" and cand.get("generated_at", "") > t0.isoformat()[:19]:
                j = cand; break
        except Exception:
            pass
        time.sleep(15)
    if j:
        cv = j.get("coverage") or {}
        un = cv.get("universe") or {}
        dl0 = (j.get("deals") or [{}])[0]
        gate("G2_feed_v3",
             len(j.get("by_event") or {}) >= 8 and (un.get("n_listed") or 0) >= 4200
             and "event_type" in dl0 and "counterparty_quality" in dl0 and "promo_risk" in dl0,
             f"deals={len(j.get('deals') or [])} by_event_keys={len(j.get('by_event') or {})} "
             f"universe={un.get('n_listed')} (adr={un.get('n_adr')}) 8k_3d={cv.get('n_8k_item101_3d')} "
             f"events_hit={cv.get('events_with_deals')}/8 sample_ev={dl0.get('event_type')}")
        out["summary"] = {k: j["summary"].get(k) for k in
                          ("n_deals", "n_green", "n_ai_mega", "signals_logged", "signals")}
        out["base_rates"] = j.get("base_rates")
        out["universe"] = un
    else:
        gate("G2_feed_v3", False, "no fresh v3 feed within window")

    # G3 history ledger born
    try:
        h = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/deal-history.json")["Body"].read())
        gate("G3_history_ledger", (h.get("n") or 0) >= 5,
             f"entries={h.get('n')} base_rate_types={list((h.get('base_rates') or {}).keys())}")
    except Exception as e:
        gate("G3_history_ledger", False, str(e)[:200])

    # G4 signals honor the institutional bar (check via feed + DDB metadata)
    try:
        sigs = ((j or {}).get("summary") or {}).get("signals") or []
        bar_ok = all(s0.get("event_type") != "ma_target" for s0 in sigs)
        det = f"logged={len(sigs)} {[s0.get('ticker') for s0 in sigs]} bar_ok={bar_ok}"
        if sigs:
            tk = sigs[0]["ticker"]
            sid = f"deal-win#{tk}#{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
            it = DDB.get_item(TableName="justhodl-signals", Key={"signal_id": {"S": sid}}).get("Item")
            if it:
                md = json.loads(it.get("metadata", {}).get("S", "{}"))
                det += f" · DDB {sid}: event={md.get('event_type')} 8k={md.get('confirmed_8k')} pop={md.get('pop_since_announce_pct')}"
                bar_ok = bar_ok and "event_type" in md
        gate("G4_signal_bar", bar_ok, det + " (0 logged = correct silence, bar is strict)")
    except Exception as e:
        gate("G4_signal_bar", False, str(e)[:200])

    # G5 served page boards
    ok5 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/deal-scanner.html", headers=UA), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            if all(m in html for m in ("Event Mix — what kind of deals hit the tape",
                                       "Base Rates — what deals actually return",
                                       "US-Listed Monitored", "Institutional layer:")):
                ok5 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G5_page_boards", ok5, "served markers: Event Mix / Base Rates / US-Listed tile / institutional note")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3575.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
