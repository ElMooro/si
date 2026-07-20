"""ops 3577 — deal-scanner v3.1: capital-structure taxonomy (financing events
declassified from wins/signals — EPR/LASE/MRAI class fix), plural-contracts
classifier fix, word-boundary counterparties, share-flows dilution join,
backfill mode, page institutional layer (context line, capstr board, signal
columns, filter chips, universe/8-K/source context). Inlines 3576 config
enforcement (deploy workflow stomps to 512/300)."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3577)"}
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-deal-scanner"

with report("3577_deal_v31") as rep:
    rep.heading("ops 3577 — deal-scanner v3.1 (taxonomy fix + institutional page layer)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:420]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:380]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G1 settle + v3.1 markers + config self-heal (3576 pattern inlined)
    ok1 = False; dl = time.time() + 660
    while time.time() < dl:
        try:
            cfg = LAM.get_function_configuration(FunctionName=FN)
            if cfg.get("LastUpdateStatus") == "Successful":
                info = LAM.get_function(FunctionName=FN)
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
                    src = zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
                if all(m in src for m in ('VERSION = "3.1.0"', '"capital_structure", "other"',
                                          "def load_shareflows", "backfill_pages",
                                          '("ma_target", "capital_structure")')):
                    ok1 = True; break
        except Exception:
            pass
        time.sleep(12)
    cfg_note = ""
    try:
        cfg = LAM.get_function_configuration(FunctionName=FN)
        if cfg.get("MemorySize") != 1024 or cfg.get("Timeout") != 600:
            LAM.update_function_configuration(FunctionName=FN, MemorySize=1024, Timeout=600)
            dl2 = time.time() + 180
            while time.time() < dl2:
                cfg = LAM.get_function_configuration(FunctionName=FN)
                if cfg.get("LastUpdateStatus") == "Successful" and cfg.get("MemorySize") == 1024:
                    break
                time.sleep(6)
            cfg_note = " (workflow stomped config again — re-enforced 1024/600)"
    except Exception as e:
        cfg_note = f" cfg-heal err {str(e)[:80]}"
    gate("G1_settled_v31", ok1 and cfg.get("MemorySize") == 1024 and cfg.get("Timeout") == 600,
         f"markers ok={ok1} mem={cfg.get('MemorySize')} timeout={cfg.get('Timeout')}{cfg_note}")

    # G2 fresh v3.1 feed — capital_structure classified out of wins
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    j = None; dl = time.time() + 560
    while time.time() < dl:
        try:
            cand = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/deal-scanner.json")["Body"].read())
            if cand.get("version") == "3.1.0" and cand.get("generated_at", "") > t0.isoformat()[:19]:
                j = cand; break
        except Exception:
            pass
        time.sleep(15)
    if j:
        be = j.get("by_event") or {}
        wins = (j["summary"].get("green_highlights") or []) + (j["summary"].get("ai_megadeals") or [])
        # slim board entries lack event_type — cross-check via full deals list
        evmap = {d["symbol"] + "|" + (d.get("title") or "")[:40]: d.get("event_type")
                 for d in j.get("deals") or []}
        leak = [w["symbol"] for w in wins
                if evmap.get(w["symbol"] + "|" + (w.get("title") or "")[:40]) == "capital_structure"]
        n_cs = (be.get("capital_structure") or {}).get("n", 0)
        gate("G2_feed_v31", len(be) >= 9 and not leak,
             f"by_event={len(be)} keys · capital_structure n={n_cs} · wins-leak={leak or 'none'} "
             f"· deals={len(j.get('deals') or [])} · dilution-joined="
             f"{sum(1 for d in (j.get('deals') or []) if d.get('dilution'))}")
    else:
        gate("G2_feed_v31", False, "no fresh 3.1.0 feed within window")

    # G3 served page — static source markers (data-independent)
    ok3 = False; dl = time.time() + 330
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/deal-scanner.html", headers=UA), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            if all(m in html for m in ("Capital structure (financing)", "Reference universe:",
                                       "window.__dsSet", "diluting ", "8-Ks (Item 1.01")):
                ok3 = True; break
        except Exception:
            pass
        time.sleep(15)
    gate("G3_page_v31", ok3, "served: capstr label + universe line + filter chips + dilution + 8-K count")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3577.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
