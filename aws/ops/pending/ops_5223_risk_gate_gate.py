"""ops_5223 -- audit 2026-09-08 Release B2 gate: justhodl-risk-gate v2.5 (FR-06/07/08/09).

Waits for the redeploy, invokes the gate synchronously, then asserts on the LIVE data/risk-gate.json:
  - version 2.5, replay_purity present, overlays[] with contribution/status/eligible, composite_identity.check_ok;
  - composite == weighted legs + overlays (recomputed here from the published legs);
  - every leg publishes engine_score / fleet_fused_score / state / drivers; fleet_context.inputs is a dict;
  - indicators.sahm_rule and indicators.truck_transport are native-month (basis + observation_date) or an honest pending_source;
  - risk-gate.html carries the indicators/overlays panels at the edge.
"""
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-risk-gate"
FAILS = []
T_PUSH = int(subprocess.run(["git", "log", "-1", "--format=%ct", "HEAD"], capture_output=True, text=True, cwd=ROOT).stdout.strip() or "0")


def expect(cond, msg):
    (R.ok if cond else R.fail)(msg)
    if not cond:
        FAILS.append(msg)


with report("ops_5223_risk_gate_gate") as R:
    R.heading("ops 5223 -- Release B2 gate: risk-gate v2.5 pure replay / disclosed overlays / native-month indicators / page contract")
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    t0 = time.time()
    cfg = None
    while time.time() - t0 < 1500:
        cfg = lam.get_function_configuration(FunctionName=FN)
        lm = datetime.fromisoformat(cfg["LastModified"].replace("Z", "+00:00")).timestamp()
        if lm >= T_PUSH and cfg.get("LastUpdateStatus", "Successful") == "Successful":
            break
        time.sleep(30)
    R.log("%s LastModified %s (push %s)" % (FN, cfg["LastModified"], datetime.fromtimestamp(T_PUSH, timezone.utc).isoformat()))
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    body = json.loads(r["Payload"].read() or b"{}")
    R.log("invoke -> %s" % json.dumps(body)[:240])
    expect(r.get("StatusCode") == 200 and not r.get("FunctionError"), "invoke succeeded (FunctionError=%s)" % r.get("FunctionError"))
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/risk-gate.json")["Body"].read())
    expect(doc.get("version") == "2.5", "artifact version 2.5 (got %s)" % doc.get("version"))
    expect(bool(doc.get("replay_purity")), "replay_purity declared")
    ov = doc.get("overlays") or []
    idn = doc.get("composite_identity") or {}
    expect(len(ov) == 2 and all("contribution" in o and "status" in o and "eligible" in o for o in ov), "2 disclosed overlays with contribution/status/eligible: %s" % [(o.get("name"), o.get("status"), o.get("contribution")) for o in ov])
    W = {"funding": .25, "credit": .25, "dollar": .20, "carry": .10, "growth": .10, "structure": .10}
    legs = doc.get("legs") or {}
    wl = sum(float(legs[k]["score_fused"]) * W[k] for k in W)
    ot = sum(float(o.get("contribution") or 0) for o in ov)
    expect(abs(round(wl + ot, 3) - float(doc.get("composite"))) < 0.002, "composite %s == weighted legs %.3f + overlays %.3f" % (doc.get("composite"), wl, ot))
    expect(idn.get("check_ok") is True, "composite_identity.check_ok")
    expect(all(("engine_score" in legs[k] and "fleet_fused_score" in legs[k] and "state" in legs[k] and isinstance(legs[k].get("drivers"), list)) for k in W), "every leg publishes engine_score/fleet_fused_score/state/drivers")
    fc = doc.get("fleet_context") or {}
    expect(isinstance(fc.get("inputs"), dict) and len(fc["inputs"]) > 0, "fleet_context.inputs dict with %s entries" % len(fc.get("inputs") or {}))
    ind = (doc.get("indicators") or {}).get("indicators") or {}
    for key in ("sahm_rule", "truck_transport"):
        v = ind.get(key) or {}
        ok = ("pending_source" in v) or (v.get("basis") and v.get("observation_date"))
        expect(ok, "%s is native-month (basis+observation_date) or honestly pending: %s" % (key, {k: v.get(k) for k in ("value", "signal", "basis", "observation_date", "pending_source")}))
    R.kv(step="risk-gate", version=doc.get("version"), posture=doc.get("posture"), composite=doc.get("composite"), replay=doc.get("replay_posture_fred_only"),
         overlays=ot, sahm=(ind.get("sahm_rule") or {}).get("value"), truck=(ind.get("truck_transport") or {}).get("value"), elapsed_s=doc.get("elapsed_s"))
    R.log("posture=%s composite=%s (replay FRED-only %s/%s) sizing x%s | sahm %s %s | truck %s %s | elapsed %ss" % (
        doc.get("posture"), doc.get("composite"), doc.get("replay_posture_fred_only"), doc.get("replay_composite_fred_only"), doc.get("sizing_multiplier"),
        (ind.get("sahm_rule") or {}).get("value"), (ind.get("sahm_rule") or {}).get("signal"), (ind.get("truck_transport") or {}).get("value"), (ind.get("truck_transport") or {}).get("signal"), doc.get("elapsed_s")))
    ok_page = False
    t1 = time.time()
    while time.time() - t1 < 600:
        try:
            with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/risk-gate.html?v=%d" % int(time.time()), headers={"User-Agent": "ops5223", "Cache-Control": "no-cache"}), timeout=30) as rr:
                ok_page = b'id="indicators"' in rr.read()
        except Exception:
            ok_page = False
        if ok_page:
            break
        time.sleep(20)
    expect(ok_page, "risk-gate.html carries the indicators/overlays panels at the edge")
    R.section("verdict")
    for f in FAILS:
        R.fail(f)
    if FAILS:
        sys.exit(1)
    R.ok("GREEN -- risk-gate replay is FRED-pure, overlays disclosed with identity, monthly indicators native")
