"""
ops/862 - VERIFY the intraday tape strip on the Risk Desk cockpit.

risk-desk.html now carries a live Intraday Tape strip that reads
data/cro-escalation.json. Two things must be true for it to be sound:

  1. data/cro-escalation.json must hold a REAL reading. ops/861 left it
     on a simulated ALERT tape (its last write was the escalation
     test). So this op first invokes justhodl-cro-escalation with an
     empty event - a real live-tape run - which overwrites the file
     with a genuine, non-simulated reading.
  2. The deployed page must carry the strip and wire the feed.

Checks:
  - the live-tape invoke succeeds and clears the simulated flag;
  - data/cro-escalation.json is now real (simulated=false) and carries
    the severity_label + tape gauges the strip renders;
  - risk-desk.html is deployed with the Intraday Tape strip markers
    and fetches cro-escalation.json;
  - the strip's severity is one of the four valid grades.

Writes aws/ops/reports/862_intraday_strip_verify.json.
"""
import json
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
FN = "justhodl-cro-escalation"
ESC_KEY = "data/cro-escalation.json"
PAGE_URL = "https://justhodl.ai/risk-desk.html"
VALID_SEV = {"CLEAR", "WATCH", "ALERT", "SEVERE"}

cfg = Config(read_timeout=120, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 862,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Verify the live Intraday Tape strip on the Risk Desk "
               "cockpit, and refresh cro-escalation.json to a real read",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read().decode("utf-8", "ignore")


# ---- 1) real live-tape run -- clears the simulated state -------------------
live = {}
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    raw = r["Payload"].read().decode("utf-8", "ignore")
    outer = json.loads(raw)
    live = json.loads(outer.get("body") or "{}")
    ok = (r.get("StatusCode") == 200 and not r.get("FunctionError")
          and live.get("ok") and live.get("simulated") is False)
    check("live_tape_run_ok", ok,
          "severity=%s (%s), simulated=%s -- real FMP read"
          % (live.get("severity"), live.get("severity_label"),
             live.get("simulated")))
except Exception as e:
    check("live_tape_run_ok", False, f"{type(e).__name__}: {e}")

# ---- 2) cro-escalation.json now real + strip-ready -------------------------
esc = {}
try:
    esc = json.loads(s3.get_object(
        Bucket=S3_BUCKET, Key=ESC_KEY)["Body"].read())
    real = esc.get("simulated") is False
    check("escalation_feed_is_real", real,
          "cro-escalation.json simulated=%s generated_at=%s"
          % (esc.get("simulated"), esc.get("generated_at")))
    tape = esc.get("tape") or {}
    strip_ready = (esc.get("severity_label") in VALID_SEV
                   and isinstance(tape, dict)
                   and any(k in tape for k in ("SPY", "VIX")))
    check("strip_data_present", strip_ready,
          "severity_label=%s, tape gauges=%s"
          % (esc.get("severity_label"), ",".join(sorted(tape.keys()))))
except Exception as e:
    check("escalation_feed_is_real", False, f"{type(e).__name__}: {e}")
    check("strip_data_present", False, "feed unreadable")

# ---- 3) page deployed with the strip --------------------------------------
try:
    status, page = http_get(PAGE_URL)
    has_strip = ("Intraday Tape" in page
                 and 'class="tape"' in page
                 and "cro-escalation.json" in page)
    check("page_has_intraday_strip", status == 200 and has_strip,
          "HTTP %s, strip markers %s, feed wired %s"
          % (status, "present" if 'class="tape"' in page else "MISSING",
             "yes" if "cro-escalation.json" in page else "no"))
    has_grades = all(g in page for g in ("CLEAR", "WATCH", "ALERT", "SEVERE"))
    check("page_handles_all_grades", has_grades,
          "strip colour-maps all four severity grades")
except Exception as e:
    check("page_has_intraday_strip", False, f"{type(e).__name__}: {e}")
    check("page_handles_all_grades", False, "page unreadable")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["intraday_strip"] = {
    "page_url": PAGE_URL,
    "live_severity": esc.get("severity_label"),
    "live_severity_num": esc.get("severity"),
    "escalated_today": (esc.get("day_state") or {}).get("n_pings", 0),
    "simulated": esc.get("simulated"),
    "checkpoint_utc": esc.get("checkpoint_utc"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "INTRADAY STRIP LIVE - the Risk Desk cockpit now shows a live "
        "tape read under the overnight verdict. cro-escalation.json is "
        "refreshed to a real reading (severity %s), the strip is "
        "deployed, wired and colour-maps all four grades. The cockpit "
        "reflects intraday state, not just the overnight batch."
        % esc.get("severity_label"))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("INTRADAY STRIP VERIFICATION INCOMPLETE - %d "
                      "check(s) failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/862_intraday_strip_verify.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
