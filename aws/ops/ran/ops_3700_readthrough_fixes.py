"""ops 3700 — justhodl-readthrough v1.0.1 fix verification.

ops 3699 passed all gates but surfaced two real defects in the live board:
  1. CLBK -54.78% ("second step conversion + $1.7B stock offering") was classified
     MA on the word "Acquisition" and propagated across 22 phantom beneficiaries.
     A capital-structure event is not demand and a collapse is not a read-through.
  2. inherit_env=true pulled only 3 vars from confluence-meta — no TELEGRAM_*,
     so the alert path was dead on arrival.

v1.0.1: STRUCTURAL_EXCLUDE lexicon + positive-only catalyst gate + explicit
inherit_env from alert-sentinel/news-wire.

Gates prove the fixes in-account, not in theory.
"""
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

REGION = "us-east-1"
FN = "justhodl-readthrough"
BUCKET = "justhodl-dashboard-live"
KEY = "data/readthrough.json"

LAM = boto3.client("lambda", REGION, config=Config(read_timeout=90, retries={"max_attempts": 0}))
S3C = boto3.client("s3", REGION)

with report("3700_readthrough_fixes") as rep:
    rep.heading("ops 3700 — readthrough v1.0.1 fix verification")
    out = {"gates": {}}
    fails = []
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3700.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:900]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:860])
            rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        # ── G1: Telegram credentials now present ──
        cfg = LAM.get_function_configuration(FunctionName=FN)
        envk = sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())
        need = ["FMP_KEY", "POLYGON_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
        missing = [k for k in need if k not in envk]
        gate("G1_telegram_env", not missing,
             f"env keys={envk} missing={missing}")
        out["env_keys"] = envk

        # ── G2: code version actually shipped ──
        import base64
        import io as _io
        import urllib.request
        import zipfile
        loc = LAM.get_function(FunctionName=FN)["Code"]["Location"]
        z = zipfile.ZipFile(_io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
        src = z.read("lambda_function.py").decode("utf-8", "ignore")
        ok2 = ('VERSION = "1.0.1"' in src and "STRUCTURAL_EXCLUDE" in src
               and 'if q["chg_pct"] < GAP_MIN_PCT' in src)
        gate("G2_code_shipped", ok2,
             f'version_line={"1.0.1" if chr(49)+".0.1" in src else "?"} '
             f'structural_exclude={"STRUCTURAL_EXCLUDE" in src} '
             f'positive_only={chr(105)+chr(102)+" q[" in src} bytes={len(src)}')

        # ── G3: live invoke, gate on S3 freshness (never sync-gate an engine) ──
        before = None
        try:
            before = S3C.head_object(Bucket=BUCKET, Key=KEY)["LastModified"]
        except Exception:
            pass
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        ok3, waited = False, 0
        t0 = time.time()
        while time.time() - t0 < 420:
            time.sleep(15)
            try:
                h = S3C.head_object(Bucket=BUCKET, Key=KEY)
                if before is None or h["LastModified"] > before:
                    ok3, waited = True, round(time.time() - t0, 1)
                    break
            except Exception:
                pass
        gate("G3_invoke", ok3, f"feed refreshed={ok3} waited={waited}s")

        doc = json.loads(S3C.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()) if ok3 else {}

        # ── G4: no negative catalysts, no capital-structure events ──
        evs = doc.get("events") or []
        neg = [e["ticker"] for e in evs if (e.get("move_pct") or 0) < 0]
        struct = [e["ticker"] for e in evs
                  if any(x in (e.get("headline") or "").lower()
                         for x in ("second step conversion", "stock offering",
                                   "public offering", "reverse split"))]
        gate("G4_no_phantom_catalysts", not neg and not struct,
             f"version={doc.get('version')} events={len(evs)} "
             f"negative={neg} structural={struct}")

        # ── G5: board integrity — every row must carry real, non-null mechanics ──
        rows = doc.get("beneficiaries") or []
        bad = [r["ticker"] for r in rows
               if r.get("expected_move_pct") is None
               or r.get("realized_ex_beta_pct") is None
               or r.get("tier") not in doc.get("tiers", {})]
        gate("G5_row_integrity", not bad,
             f"rows={len(rows)} malformed={bad[:8]} "
             f"unpriced={doc.get('n_unpriced', 0)} picks={len(doc.get('top_picks') or [])} "
             f"status={doc.get('status')} degraded={doc.get('degraded')}")

        out["board"] = {
            "version": doc.get("version"), "status": doc.get("status"),
            "n_events": doc.get("n_events", 0),
            "n_beneficiaries": doc.get("n_beneficiaries", 0),
            "n_unpriced": doc.get("n_unpriced", 0),
            "elapsed_s": doc.get("elapsed_s"),
            "events": [{"ticker": e.get("ticker"), "move": e.get("move_pct"),
                        "type": e.get("type"), "order": e.get("order_value_str"),
                        "benef": e.get("n_beneficiaries"),
                        "unpriced": e.get("n_unpriced"),
                        "headline": (e.get("headline") or "")[:110]} for e in evs[:8]],
            "top_rows": [{"ticker": r.get("ticker"), "tier": r.get("tier"),
                          "via": r.get("catalyst_ticker"),
                          "exp": r.get("expected_move_pct"),
                          "real": r.get("realized_ex_beta_pct"),
                          "gap": r.get("residual_pct"),
                          "status": r.get("status"),
                          "score": r.get("catch_up_score")} for r in rows[:12]],
        }
        print("\n=== CATALYSTS (v1.0.1) ===")
        for e in out["board"]["events"]:
            print(f"  {e['ticker']:6} {str(e['move']):>7}%  {e['type']:20} "
                  f"benef={e['benef']} unpriced={e['unpriced']}")
            print(f"         {e['headline']}")
        if not out["board"]["events"]:
            print("  (no qualifying catalyst — real answer, nothing invented)")
        print("\n=== TOP BENEFICIARY ROWS ===")
        for r in out["board"]["top_rows"]:
            print(f"  {r['ticker']:6} {str(r['tier']):26} via {str(r['via']):6} "
                  f"exp={str(r['exp']):>7} real={str(r['real']):>7} gap={str(r['gap']):>7} "
                  f"{str(r['status']):9} score={r['score']}")

    except Exception:
        out["crash"] = traceback.format_exc()[-1500:]
        print("CRASH:", out["crash"][-500:])

    out["verdict"] = ("CRASH" if out.get("crash") else
                      ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3700.json").write_text(json.dumps(out, indent=2, default=str))
    # House rule (preflight): a failed gate must exit non-zero so the runner
    # goes red and the script is NOT auto-moved to ran/ as if it succeeded.
    if out["verdict"] != "PASS_ALL":
        sys.exit(1)
    sys.exit(0)
