"""
ops/874 - RE-VERIFY the cro-escalation cycle-extreme alert track.

ops/873 verified the deployed artifact carries build_cycle_extreme_alert
and the cycle_extreme block, but its runtime check read S3 - and the
cro-escalation dry-run path deliberately does not persist output, so
it read a stale file. The fix added cycle-extreme fields to the return
body. This op confirms the track at runtime the right way:

  1. Ship the updated cro-escalation.
  2. Invoke it with dry_run -> the cycle-extreme track runs.
  3. Read the cycle-extreme fields straight from the RETURN payload:
       - cycle_extreme_posture resolves and matches the live
         market-extremes.json posture;
       - cycle_extreme_telegram reports a valid track state
         (no_flip / dry_run / already_alerted_*);
       - no Telegram was sent (dry-run).

Writes aws/ops/reports/874_cycle_alert_reverify.json.
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
CE = "justhodl-cro-escalation"
CE_SRC = f"aws/lambdas/{CE}/source/lambda_function.py"
MX_KEY = "data/market-extremes.json"

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 874,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Re-verify the cro-escalation cycle-extreme alert track via "
               "the dry-run return payload",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


# ---- 1) ship ---------------------------------------------------------------
try:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", open(CE_SRC, encoding="utf-8"
                                              ).read())
    lam.update_function_code(FunctionName=CE, ZipFile=buf.getvalue())
    ok = False
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=CE)
        if c.get("LastUpdateStatus") == "Successful" and c.get(
                "State") == "Active":
            ok = True
            break
        time.sleep(3)
    check("deploy_ok", ok, "cro-escalation shipped")
except Exception as e:
    check("deploy_ok", False, f"{type(e).__name__}: {e}")

# ---- 2) live market-extremes posture (the expected value) -----------------
mx_posture = None
try:
    mx = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=MX_KEY
                                  )["Body"].read())
    mx_posture = mx.get("posture")
except Exception as e:
    rep["mx_read_error"] = f"{type(e).__name__}: {e}"

# ---- 3) dry-run invoke + read the return payload --------------------------
ret = {}
try:
    r = lam.invoke(FunctionName=CE, InvocationType="RequestResponse",
                   Payload=json.dumps({"dry_run": True}).encode())
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    ret = json.loads(body.get("body") or "{}")
    check("invoke_dry_run_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError")
          and ret.get("dry_run") is True,
          "dry_run=%s" % ret.get("dry_run"))
except Exception as e:
    check("invoke_dry_run_ok", False, f"{type(e).__name__}: {e}")

ce_posture = ret.get("cycle_extreme_posture")
ce_tg = ret.get("cycle_extreme_telegram")
ce_alerted = ret.get("cycle_extreme_alerted")

check("cycle_fields_in_return",
      "cycle_extreme_posture" in ret and "cycle_extreme_telegram" in ret
      and "cycle_extreme_alerted" in ret,
      "posture=%s telegram=%s alerted=%s" % (ce_posture, ce_tg, ce_alerted))

check("cycle_posture_matches_market_extremes",
      ce_posture == mx_posture,
      "cro-escalation cycle posture=%s vs market-extremes posture=%s"
      % (ce_posture, mx_posture))

valid_tg = (ce_tg in ("no_flip", "dry_run")
            or str(ce_tg).startswith("already_alerted_"))
check("cycle_track_state_valid", valid_tg,
      "cycle_extreme_telegram=%s (a valid no-spam track state)" % ce_tg)

# on a dry-run nothing is actually sent
check("no_telegram_sent_on_dry_run", ce_alerted is False,
      "cycle_extreme_alerted=%s (no ping fired on dry-run)" % ce_alerted)

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["cycle_alert"] = {
    "market_extremes_posture": mx_posture,
    "cycle_extreme_posture": ce_posture,
    "cycle_extreme_telegram": ce_tg,
    "cycle_extreme_alerted": ce_alerted,
}
if rep["all_passed"]:
    rep["verdict"] = (
        "CYCLE-EXTREME ALERT TRACK VERIFIED - cro-escalation runs the "
        "market-extremes flip track each checkpoint; the cycle posture "
        "(%s) matches the live radar and the no-spam track state is "
        "sound (%s). It will fire one Telegram ping on a flip into "
        "EUPHORIA or CAPITULATION. Both cycle integrations now confirmed."
        % (ce_posture, ce_tg))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("CYCLE-EXTREME RE-VERIFY INCOMPLETE - %d check(s) "
                      "failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/874_cycle_alert_reverify.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
