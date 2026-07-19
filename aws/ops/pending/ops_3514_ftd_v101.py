"""ops 3514 — equity-FTD v1.0.1 regate: spike now requires a prior-mean
BASE >= 20k shares (3513-L2 exposed EME at 33,910x off an 81-share
mean — denominator artifact, not a squeeze); per-row `base` published
for transparency; the withvol gate corrected (top lists cap at 60).

  M1 live force run: >=4 files, universe >10k, EVERY qualifier passes
     every floor AND has base >= 20k; top-spike base printed; signals/
     logged printed in full; vol coverage >=40 across top rows
  M2 signals table spot-check: any logged ftd-squeeze row today has
     schema v2, benchmark SPY, regime snapshot present
"""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-equity-ftd"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")

with report("3514_ftd_v101") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:560]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3514 — FTD v1.0.1 (min-base spike)")
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={"FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"},
                  timeout=600, memory=512,
                  description="Equity FTD v1.0.1 min-base (ops 3514)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    r = lam.invoke(FunctionName=FN,
                   Payload=json.dumps({"force": True}).encode())
    doc = json.loads(s3c.get_object(Bucket=BUCKET,
                                    Key="data/equity-ftd.json")["Body"].read())
    F = doc["floors"]; quals = doc.get("qualifiers") or []
    ok_floors = all((q.get("px") or 0) >= F["price"]
                    and (q.get("avg20") or 0) >= F["avg20"]
                    and (q.get("usd") or 0)*1e6 >= F["usd"]
                    and (q.get("spike") or 0) >= F["spike"]
                    and (q.get("dtc_peak") or 0) >= F["dtc_peak"]
                    and (q.get("base") or 0) >= 20000 for q in quals)
    withvol = sum(1 for x in (doc.get("top_dollars") or [])
                  + (doc.get("top_spikes") or []) if x.get("avg20"))
    gate("M1_live", len(doc.get("files") or []) >= 4
         and doc["universe_n"] > 10000 and ok_floors and withvol >= 40
         and all((x.get("base") or 0) >= 20000
                 for x in (doc.get("top_spikes") or []) if x.get("spike")),
         {"files": doc["files"], "n_candidates": doc["n_candidates"],
          "top_spikes_5": [(x["t"], x["spike"], x["base"], x["dtc_peak"])
                           for x in (doc.get("top_spikes") or [])[:5]],
          "qualifiers": quals, "signals": doc.get("signals"),
          "logged": doc.get("logged"), "withvol": withvol})
    try:
        tbl = ddb.Table("justhodl-signals")
        today = datetime.now(timezone.utc).date().isoformat()
        found = None
        for s in (doc.get("signals") or [])[:3]:
            it = tbl.get_item(Key={"signal_id": f"ftd-squeeze#{s}#{today}"}
                              ).get("Item")
            if it:
                found = {"id": it["signal_id"],
                         "schema": it.get("schema_version"),
                         "bench": it.get("benchmark"),
                         "regime": bool((it.get("metadata") or {})
                                        .get("regime")),
                         "conf": str(it.get("confidence"))}
                break
        gate("M2_signal_row", (not doc.get("signals"))
             or (found and found["schema"] == "2"
                 and found["bench"] == "SPY" and found["regime"]),
             found or "no signals emitted today (floors honest)")
    except Exception as e:
        gate("M2_signal_row", False, str(e)[:280])

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3514.json").write_text(
        json.dumps({"ops": 3514, "fails": fails}))
sys.exit(0)
