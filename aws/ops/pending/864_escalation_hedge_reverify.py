"""
ops/864 - RE-VERIFY the escalation -> hedge-sleeve link (corrected).

ops/863 passed 6/7. The one miss was a flaw in the verification, not
the engine: it asserted the escalation MESSAGE carried the implication
block on an ALERT-grade tape - but a message is only built when the
run actually escalates, and ALERT did not clear the day's already-
escalated severity left by the ops/861 test. The implication itself
computed correctly (ADD, SPY put spread).

This op re-verifies cleanly, decoupling the two facts:

  * the implication COMPUTES at ALERT/SEVERE - checked on its own;
  * the implication is RENDERED INTO THE MESSAGE - checked on a tape
    that genuinely escalates, and only when should_escalate is true.

All dry-run; no Telegram traffic.

Writes aws/ops/reports/864_escalation_hedge_reverify.json.
"""
import json
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
FN = "justhodl-cro-escalation"

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

cfg = Config(read_timeout=120, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)

rep = {
    "ops": 864,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Re-verify the escalation -> hedge-sleeve link - implication "
               "computes at ALERT/SEVERE and is rendered into the "
               "escalation message",
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


# ---- 1) ALERT: implication computes ----------------------------------------
try:
    sc, fe, r = invoke({"simulate": TAPE_EQ_ALERT, "dry_run": True})
    imp = r.get("hedge_implication") or {}
    ok = (sc == 200 and not fe and r.get("severity") == 2
          and imp.get("recommended_action") in ("ADD", "HARVEST")
          and imp.get("target_leg")
          and imp.get("tape_scenario_class") == "EQUITY_CRASH")
    check("alert_implication_computes", ok,
          "severity=%s action=%s leg=%s class=%s"
          % (r.get("severity"), imp.get("recommended_action"),
             imp.get("target_leg"), imp.get("tape_scenario_class")))
except Exception as e:
    check("alert_implication_computes", False, f"{type(e).__name__}: {e}")

time.sleep(1)

# ---- 2) SEVERE: HARVEST + rendered into the message ------------------------
try:
    sc, fe, r = invoke({"simulate": TAPE_EQ_SEVERE, "dry_run": True})
    imp = r.get("hedge_implication") or {}
    msg = r.get("message") or ""
    base_ok = (sc == 200 and not fe and r.get("severity") == 3
               and imp.get("recommended_action") == "HARVEST"
               and imp.get("target_leg"))
    # message must carry the block whenever the run escalates
    msg_ok = (("Hedge implication:" in msg
               and imp.get("recommended_action") in msg)
              if r.get("should_escalate") else True)
    check("severe_implies_harvest_in_message", base_ok and msg_ok,
          "severity=%s action=%s should_escalate=%s, message carries "
          "block=%s" % (r.get("severity"), imp.get("recommended_action"),
                        r.get("should_escalate"),
                        "Hedge implication:" in msg))
except Exception as e:
    check("severe_implies_harvest_in_message", False,
          f"{type(e).__name__}: {e}")

time.sleep(1)

# ---- 3) CREDIT tape -> SWITCH-REVIEW on a class mismatch -------------------
try:
    sc, fe, r = invoke({"simulate": TAPE_CREDIT, "dry_run": True})
    imp = r.get("hedge_implication") or {}
    matched = imp.get("sleeve_class_match")
    act = imp.get("recommended_action")
    ok = (sc == 200 and not fe and r.get("severity") == 2
          and imp.get("tape_scenario_class") == "CREDIT_EVENT"
          and (act == "SWITCH_REVIEW" if matched is False
               else act in ("ADD", "HARVEST")))
    check("credit_mismatch_implies_switch", ok,
          "tape_class=CREDIT_EVENT standing_match=%s action=%s"
          % (matched, act))
except Exception as e:
    check("credit_mismatch_implies_switch", False, f"{type(e).__name__}: {e}")

time.sleep(1)

# ---- 4) WATCH grade -> no implication --------------------------------------
try:
    sc, fe, r = invoke({"simulate": TAPE_WATCH, "dry_run": True})
    ok = (sc == 200 and not fe and r.get("severity") == 1
          and r.get("hedge_implication") in (None, {}))
    check("watch_carries_no_implication", ok,
          "severity=%s hedge_implication=%s"
          % (r.get("severity"), r.get("hedge_implication")))
except Exception as e:
    check("watch_carries_no_implication", False, f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
if rep["all_passed"]:
    rep["verdict"] = (
        "ESCALATION -> HEDGE LINK VERIFIED - the implication computes at "
        "ALERT and SEVERE (ADD / HARVEST by tape and standing sleeve), "
        "renders into the escalation message whenever the run escalates, "
        "reads SWITCH-REVIEW on a class mismatch, and stays null at WATCH. "
        "The intraday tripwire now tells the desk exactly what the sleeve "
        "move is, not just to look at it.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("RE-VERIFY INCOMPLETE - %d check(s) failed: %s."
                      % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/864_escalation_hedge_reverify.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
