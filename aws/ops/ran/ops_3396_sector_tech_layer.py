"""ops 3396 — fleet-audit gap fill: sector-ETF technical layer, E2E.

AUDIT VERDICT (Khalid: "is there an engine that analyzes sector ETFs for
acc/dist, institutions, options, dollar in/out, double tops/bottoms, MA
breaks 200/100/50/20"):
  COVERED — acc/dist: accumulation-radar (Wyckoff OBV/CMF/RSI + top/bottom
  scores, all 11 XL*). Institutions: capital-flow v2 $-layer + 13F desk +
  fusion institutional lens. Options: options-flow carries XL* rows
  (flow_5d). Dollar in/out: sectors $-tornado + etf-fund-flows + fusion.
  GAPS — double top/bottom detectors existed (chart-patterns) but scanned
  S&P 500 STOCKS only; no engine computed a sector ETF's OWN 20/50/100/200
  ladder (100-DMA existed NOWHERE in the fleet; FinViz lacks SMA100 too).

SHIPPED (extend-don't-duplicate): chart-patterns gains sector_scan() over
the 11 SPDRs + SPY — same detect_double_top/bottom, plus the full MA ladder
with fresh-cross flags (5-session) and a posture verdict; emitted under
sector_etfs. sector-capital-fusion joins it with accumulation-radar Wyckoff
phases and options-flow flow_5d into row.technicals (display layer — the
six verdict lenses untouched). sector-flow.html gains a self-contained
"🩻 Sector Technical Health" section.

Gates: G1/G3 deploy markers · G2 fresh sector_etfs: 12 rows, >=10 with
numeric d100, d200 present, postures set; live snapshot captured ·
G3b fresh fusion with technicals joined >=9 sectors · G4 page live.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3396)"}


def invoke_resilient(fn, tries=6):
    for k in range(tries):
        try:
            return LAM.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
        except Exception as e:  # noqa: BLE001
            if "TooManyRequests" in str(e) or "Rate Exceeded" in str(e):
                time.sleep(15 * (k + 1))
                continue
            raise
    raise RuntimeError("throttled")


def zsrc(fn):
    info = LAM.get_function(FunctionName=fn)
    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
        return zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")


with report("3396_sector_tech_layer") as rep:
    rep.heading("ops 3396 — sector technical layer E2E")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:360]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:300]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(n)

    ok1 = False
    dl = time.time() + 300
    while time.time() < dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-chart-patterns").get("LastUpdateStatus") == "Successful":
                if "sector_scan" in zsrc("justhodl-chart-patterns"):
                    ok1 = True
                    break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(12)
    gate("G1_patterns_deployed", ok1, "sector_scan marker in zip")

    t_inv = datetime.now(timezone.utc).isoformat()
    invoke_resilient("justhodl-chart-patterns")
    se = None
    dl = time.time() + 540
    while time.time() < dl:
        try:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",
                                          Key="data/chart-patterns.json")["Body"].read())
            if (j.get("generated_at") or j.get("as_of") or "") > t_inv and j.get("sector_etfs"):
                se = j["sector_etfs"]
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(20)
    d100_n = sum(1 for r in (se or {}).values()
                 if isinstance(((r.get("ma") or {}).get("d100") or {}).get("sma"), (int, float)))
    d200_n = sum(1 for r in (se or {}).values() if (r.get("ma") or {}).get("d200"))
    pats = {k: ("double_top" if r.get("double_top") else "double_bottom")
            for k, r in (se or {}).items() if r.get("double_top") or r.get("double_bottom")}
    gate("G2_sector_ladder", bool(se) and len(se) >= 11 and d100_n >= 10 and d200_n >= 10,
         f"rows={len(se or {})} d100={d100_n} d200={d200_n} patterns_today={pats}")
    if se:
        out["ladder_snapshot"] = {k: {"posture": r.get("posture"),
                                      "ladder": r.get("ladder_score"),
                                      "d200_above": ((r.get("ma") or {}).get("d200") or {}).get("above"),
                                      "fresh": [w for w in ("d20", "d50", "d100", "d200")
                                                if ((r.get("ma") or {}).get(w) or {}).get("fresh_cross")]}
                                  for k, r in se.items()}
        out["patterns_today"] = pats

    ok3 = False
    dl = time.time() + 240
    while time.time() < dl:
        try:
            if "technicals" in zsrc("justhodl-sector-capital-fusion"):
                ok3 = True
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(12)
    gate("G3_fusion_deployed", ok3, "technicals join in zip")

    t2 = datetime.now(timezone.utc).isoformat()
    invoke_resilient("justhodl-sector-capital-fusion")
    fus, joined = None, 0
    dl = time.time() + 300
    while time.time() < dl:
        try:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",
                                          Key="data/sector-capital-fusion.json")["Body"].read())
            if (j.get("generated_at") or "") > t2 and j.get("technicals_joined"):
                fus = j
                joined = sum(1 for r in (j.get("sectors") or [])
                             if (r.get("technicals") or {}).get("ma"))
                if joined >= 9:
                    break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(15)
    wy = sum(1 for r in ((fus or {}).get("sectors") or [])
             if (r.get("technicals") or {}).get("wyckoff"))
    gate("G3b_fusion_joined", bool(fus) and joined >= 9,
         f"sectors_with_ma={joined} with_wyckoff={wy}")

    need = ["Sector Technical Health", "ops 3396", "sector-capital-fusion.json"]
    ok4, missing = False, need
    dl = time.time() + 240
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    f"https://justhodl.ai/sector-flow.html?t={int(time.time())}",
                    headers=UA), timeout=25) as r:
                b = r.read().decode("utf-8", "replace")
            missing = [m for m in need if m not in b]
            if not missing:
                ok4 = True
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(12)
    gate("G4_page_live", ok4, f"missing={missing}")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3396.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
