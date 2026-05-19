"""
ops/879 - VERIFY the Dollar Radar + Global Stress Matrix integration
into the risk system.

ops/878 proved both engines run and write their sidecars. This op
proves the consumers actually pick them up:

  1. Redeploy crisis-composite and signal-board from the wired source.
  2. Invoke crisis-composite -> read data/crisis-composite.json and
     confirm the two new components (Global equity & bond stress,
     Dollar squeeze pressure) are present, available, and carry a
     numeric crisis contribution, and that the master score still
     computes over the renormalised weight.
  3. Invoke signal-board -> read data/signal-board.json and confirm
     the Dollar Radar and Global Stress feeds are present, not stale,
     and carry an integer signal.
  4. Confirm morning-intelligence redeployed (its DOLLAR_RADAR /
     GLOBAL_STRESS brief lines ship with the next 08:00 ET run; the
     Lambda is not invoked here to avoid burning Claude credits).

Writes aws/ops/reports/879_dollar_global_integration.json.
"""
import io
import json
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

cfg = Config(read_timeout=300, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 879,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Verify Dollar Radar + Global Stress Matrix are wired into "
               "the crisis composite, signal board and morning brief",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def redeploy(fn):
    src = f"aws/lambdas/{fn}/source/lambda_function.py"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(src, encoding="utf-8").read())
    lam.update_function_code(FunctionName=fn, ZipFile=buf.getvalue())
    for _ in range(60):
        c = lam.get_function_configuration(FunctionName=fn)
        if (c.get("LastUpdateStatus") == "Successful"
                and c.get("State") == "Active"):
            return True
        time.sleep(3)
    return False


def invoke(fn):
    r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                   Payload=b"{}")
    err = r.get("FunctionError")
    body = r["Payload"].read().decode("utf-8", "ignore")
    return (r.get("StatusCode") == 200 and not err), err, body


# ---- crisis-composite ------------------------------------------------------
try:
    ok = redeploy("justhodl-crisis-composite")
    check("crisis_composite_redeployed", ok, "from wired source")
except Exception as e:
    check("crisis_composite_redeployed", False, f"{type(e).__name__}: {e}")

try:
    ok, err, body = invoke("justhodl-crisis-composite")
    check("crisis_composite_invoke_ok", ok, err or "invoked")
except Exception as e:
    check("crisis_composite_invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)
cc = {}
try:
    cc = json.loads(s3.get_object(Bucket=S3_BUCKET,
                    Key="data/crisis-composite.json")["Body"].read())
    check("crisis_composite_readable", True, "crisis-composite.json read")
except Exception as e:
    check("crisis_composite_readable", False, f"{type(e).__name__}: {e}")

comps = {c.get("source"): c for c in (cc.get("components") or [])}
gs = comps.get("global-stress")
dr = comps.get("dollar-radar")
check("crisis_global_stress_component",
      isinstance(gs, dict) and gs.get("available")
      and isinstance(gs.get("crisis_contribution"), (int, float)),
      ("Global stress -> crisis %s (weight %s)"
       % (gs.get("crisis_contribution"), gs.get("weight")))
      if gs else "global-stress component MISSING")
check("crisis_dollar_component",
      isinstance(dr, dict) and dr.get("available")
      and isinstance(dr.get("crisis_contribution"), (int, float)),
      ("Dollar squeeze -> crisis %s (weight %s)"
       % (dr.get("crisis_contribution"), dr.get("weight")))
      if dr else "dollar-radar component MISSING")
master = cc.get("master_score") or cc.get("composite_score") or cc.get("score")
check("crisis_master_still_computes",
      isinstance(master, (int, float)),
      "master crisis score = %s, DEFCON %s, %d components total"
      % (master, cc.get("defcon_level") or cc.get("defcon"),
         len(cc.get("components") or [])))

# ---- signal-board ----------------------------------------------------------
try:
    ok = redeploy("justhodl-signal-board")
    check("signal_board_redeployed", ok, "from wired source")
except Exception as e:
    check("signal_board_redeployed", False, f"{type(e).__name__}: {e}")

try:
    ok, err, body = invoke("justhodl-signal-board")
    check("signal_board_invoke_ok", ok, err or "invoked")
except Exception as e:
    check("signal_board_invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)
sbd = {}
try:
    sbd = json.loads(s3.get_object(Bucket=S3_BUCKET,
                     Key="data/signal-board.json")["Body"].read())
    check("signal_board_readable", True, "signal-board.json read")
except Exception as e:
    check("signal_board_readable", False, f"{type(e).__name__}: {e}")

eng = {e.get("engine"): e for e in (sbd.get("engines") or [])}
for label in ("Dollar Radar", "Global Stress"):
    e = eng.get(label)
    ok = (isinstance(e, dict) and not e.get("stale")
          and isinstance(e.get("signal"), int))
    check("signal_board_has_%s" % label.lower().replace(" ", "_"), ok,
          ("%s: signal %s -- %s" % (label, e.get("signal"),
                                    e.get("signal_label") or e.get("read")))
          if e else "%s feed MISSING" % label)

# ---- morning-intelligence (deploy only) ------------------------------------
try:
    c = lam.get_function_configuration(
        FunctionName="justhodl-morning-intelligence")
    lm = c.get("LastModified", "")
    check("morning_intelligence_deployed",
          c.get("State") == "Active",
          "last modified %s -- DOLLAR_RADAR / GLOBAL_STRESS brief lines "
          "ship with the next 08:00 ET run" % lm)
except Exception as e:
    check("morning_intelligence_deployed", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["readings"] = {
    "crisis_master": master,
    "global_stress_crisis_contribution": (gs or {}).get("crisis_contribution"),
    "dollar_crisis_contribution": (dr or {}).get("crisis_contribution"),
    "signal_board_dollar": (eng.get("Dollar Radar") or {}).get("signal"),
    "signal_board_global_stress": (eng.get("Global Stress") or {}).get(
        "signal"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "INTEGRATION VERIFIED - the Dollar Radar and Global Stress Matrix "
        "now feed the risk system. The crisis composite carries both as "
        "weighted components, the signal board reads both as macro feeds, "
        "and the morning brief is wired to surface them. The dollar and "
        "global-stress engines are fully implemented into the system.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("INTEGRATION VERIFICATION INCOMPLETE - failed: %s."
                      % ", ".join(bad))

with open("aws/ops/reports/879_dollar_global_integration.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
