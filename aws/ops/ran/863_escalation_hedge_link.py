"""
ops/863 - VERIFY the escalation -> hedge-sleeve link.

justhodl-cro-escalation now derives, at ALERT/SEVERE, the specific
sleeve adjustment the intraday move implies - in the Hedge Planner's
own vocabulary (ADD / HARVEST / SWITCH-REVIEW / OPEN) - instead of a
generic "review the sleeve".

This op ships the updated function and proves the logic with simulated
tapes (all dry-run, so no Telegram traffic):

  1. Ship the function.
  2. EQUITY_CRASH ALERT, vol moderate - against the live standing
     sleeve the read should be ADD (right class, budget eaten) or
     HARVEST, and the escalation message must carry a "Hedge
     implication:" block.
  3. EQUITY_CRASH SEVERE, vol exploding - the read should be HARVEST
     (the convex VIX leg is richly in-the-money).
  4. CREDIT_EVENT ALERT - a credit-led tape against an equity sleeve
     should read SWITCH_REVIEW (class mismatch).
  5. WATCH-grade tape - the implication must be null; a wobble carries
     no sleeve action.
  6. One real live-tape invoke confirms cro-escalation.json ships the
     hedge_implication field; the deployed risk-desk.html renders it.

Writes aws/ops/reports/863_escalation_hedge_link.json.
"""
import io
import json
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
FN = "justhodl-cro-escalation"
ESC_KEY = "data/cro-escalation.json"
PAGE_URL = "https://justhodl.ai/risk-desk.html"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))

TAPE_EQ_ALERT = {"spy_pct": -2.4, "vix_pct": 18.0, "vix": 27.0,
                 "hyg_pct": -0.5, "qqq_pct": -2.6, "iwm_pct": -2.9,
                 "tlt_pct": 0.6}
TAPE_EQ_SEVERE = {"spy_pct": -3.6, "vix_pct": 55.0, "vix": 37.0,
                  "hyg_pct": -0.9, "qqq_pct": -4.1, "iwm_pct": -4.6,
                  "tlt_pct": 1.1}
TAPE_CREDIT = {"spy_pct": -1.1, "vix_pct": 14.0, "vix": 23.0,
               "hyg_pct": -2.2, "qqq_pct": -1.3, "iwm_pct": -1.5,
               "tlt_pct": -0.3}
TAPE_WATCH = {"spy_pct": -1.1, "vix_pct": 9.0, "vix": 20.0,
              "hyg_pct": -0.2, "qqq_pct": -1.2, "iwm_pct": -1.3,
              "tlt_pct": 0.2}

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 863,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Verify the escalation -> hedge-sleeve link - the intraday "
               "tripwire now surfaces the specific sleeve adjustment "
               "(ADD / HARVEST / SWITCH-REVIEW / OPEN) at ALERT/SEVERE",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def invoke(payload):
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=json.dumps(payload).encode())
    raw = r["Payload"].read().decode("utf-8", "ignore")
    try:
        outer = json.loads(raw)
        inner = json.loads(outer.get("body") or "{}")
    except Exception:
        outer, inner = {}, {}
    return r.get("StatusCode"), r.get("FunctionError"), inner


# ---- 1) ship ---------------------------------------------------------------
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zb = buf.getvalue()
try:
    lam.update_function_code(FunctionName=FN, ZipFile=zb)
    for _ in range(30):
        if lam.get_function_configuration(
                FunctionName=FN).get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    check("deploy_ok", True, "function code updated")
except Exception as e:
    check("deploy_ok", False, f"{type(e).__name__}: {e}")

time.sleep(3)

# ---- 2) EQUITY_CRASH ALERT -------------------------------------------------
try:
    sc, fe, r = invoke({"simulate": TAPE_EQ_ALERT, "dry_run": True})
    imp = r.get("hedge_implication") or {}
    act = imp.get("recommended_action")
    msg = r.get("message") or ""
    ok = (sc == 200 and not fe and r.get("severity") == 2
          and act in ("ADD", "HARVEST")
          and "Hedge implication:" in msg)
    check("equity_alert_implies_sleeve_action", ok,
          "severity=%s action=%s leg=%s, message carries block=%s"
          % (r.get("severity"), act, imp.get("target_leg"),
             "Hedge implication:" in msg))
except Exception as e:
    check("equity_alert_implies_sleeve_action", False,
          f"{type(e).__name__}: {e}")

# ---- 3) EQUITY_CRASH SEVERE -> HARVEST -------------------------------------
try:
    sc, fe, r = invoke({"simulate": TAPE_EQ_SEVERE, "dry_run": True})
    imp = r.get("hedge_implication") or {}
    ok = (sc == 200 and not fe and r.get("severity") == 3
          and imp.get("recommended_action") == "HARVEST"
          and imp.get("target_leg"))
    check("vol_spike_implies_harvest", ok,
          "severity=%s action=%s leg=%s -- VIX explosion harvests the "
          "convex leg" % (r.get("severity"), imp.get("recommended_action"),
                          imp.get("target_leg")))
except Exception as e:
    check("vol_spike_implies_harvest", False, f"{type(e).__name__}: {e}")

# ---- 4) CREDIT tape vs equity sleeve -> SWITCH-REVIEW ----------------------
try:
    sc, fe, r = invoke({"simulate": TAPE_CREDIT, "dry_run": True})
    imp = r.get("hedge_implication") or {}
    # the standing sleeve is whatever is live; a credit tape against a
    # non-CREDIT_EVENT sleeve must read SWITCH_REVIEW
    act = imp.get("recommended_action")
    tape_cls = imp.get("tape_scenario_class")
    matched = imp.get("sleeve_class_match")
    ok = (sc == 200 and not fe and r.get("severity") == 2
          and tape_cls == "CREDIT_EVENT"
          and (act == "SWITCH_REVIEW" if matched is False
               else act in ("ADD", "HARVEST")))
    check("class_mismatch_implies_switch", ok,
          "tape_class=%s standing_match=%s action=%s"
          % (tape_cls, matched, act))
except Exception as e:
    check("class_mismatch_implies_switch", False, f"{type(e).__name__}: {e}")

# ---- 5) WATCH grade -> no implication --------------------------------------
try:
    sc, fe, r = invoke({"simulate": TAPE_WATCH, "dry_run": True})
    ok = (sc == 200 and not fe and r.get("severity") == 1
          and r.get("hedge_implication") in (None, {}))
    check("watch_carries_no_implication", ok,
          "severity=%s hedge_implication=%s (must be null at WATCH)"
          % (r.get("severity"), r.get("hedge_implication")))
except Exception as e:
    check("watch_carries_no_implication", False, f"{type(e).__name__}: {e}")

# ---- 6) feed ships the field + page renders it -----------------------------
try:
    sc, fe, r = invoke({})  # real live-tape, writes cro-escalation.json
    time.sleep(2)
    esc = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key=ESC_KEY)["Body"].read())
    check("feed_ships_implication_field", "hedge_implication" in esc,
          "cro-escalation.json carries hedge_implication (=%s at live "
          "severity %s)" % (esc.get("hedge_implication"),
                            esc.get("severity_label")))
except Exception as e:
    check("feed_ships_implication_field", False, f"{type(e).__name__}: {e}")

try:
    req = urllib.request.Request(PAGE_URL,
                                 headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        page = resp.read().decode("utf-8", "ignore")
    check("page_renders_implication",
          resp.status == 200 and "hedge_implication" in page
          and "Hedge implication" in page,
          "risk-desk.html intraday strip renders the implication line")
except Exception as e:
    check("page_renders_implication", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
if rep["all_passed"]:
    rep["verdict"] = (
        "ESCALATION -> HEDGE LINK LIVE - the intraday tripwire no longer "
        "just says 'review the sleeve'. At ALERT/SEVERE it classifies the "
        "live tape into a Hedge Planner sleeve class, reads the standing "
        "sleeve, and surfaces the specific action - ADD when the budget is "
        "eaten, HARVEST when the convex VIX leg is in-the-money, "
        "SWITCH-REVIEW on a class mismatch, OPEN when unhedged - in the "
        "Telegram ping, the feed and the Risk Desk strip. A watch-grade "
        "wobble correctly carries no implication.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("ESCALATION HEDGE-LINK VERIFICATION INCOMPLETE - %d "
                      "check(s) failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/863_escalation_hedge_link.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
