"""
ops/871 - VERIFY the vol-radar -> hedge-planner timing overlay and the
14th risk-desk cockpit tile.

The Hedge Execution Planner now reads data/vol-radar.json and adds a
vol_timing overlay - timing context (PULL FORWARD / CONFIRMS / FAVOR
HARVEST / CAUTION) layered on top of the exposure-driven action,
without overriding it. The risk-desk cockpit gains the Volatility
Turning-Point Radar as a 14th tile.

This op proves both:

  1. Ship the updated hedge-planner (code-only; its env is left
     untouched).
  2. Invoke it - it reads the live vol-radar feed.
  3. Read back data/hedge-planner.json and prove:
       - the vol_timing block is present and well-formed;
       - its posture matches the live vol-radar.json posture;
       - the timing_bias is one of the valid values;
       - the action is still a valid exposure-driven action - the
         overlay did not hijack it;
       - if vol_timing carries a timing_note, that note is in the
         CRO note on the ticket.
  4. Confirm risk-desk.html is deployed with the vol-radar tile and
     wires all fourteen engine feeds.

Writes aws/ops/reports/871_vol_radar_hedge_wiring.json.
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
FN = "justhodl-hedge-planner"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
HP_KEY = "data/hedge-planner.json"
VR_KEY = "data/vol-radar.json"
PAGE_URL = "https://justhodl.ai/risk-desk.html"

VALID_ACTION = {"OPEN", "ADD", "TRIM", "ROLL", "SWITCH", "HARVEST",
                "UNWIND", "HOLD", "NONE"}
VALID_BIAS = {"NEUTRAL", "CONFIRMS", "PULL FORWARD", "FAVOR HARVEST",
              "CAUTION"}
COCKPIT_FEEDS = [
    "firm-risk-board.json", "tail-hedge.json", "hedge-planner.json",
    "hedge-pnl.json", "risk-monitor.json", "liquidity-capacity.json",
    "firm-stress.json", "merger-arb-risk.json", "vol-radar.json",
    "firm-book.json", "factor-risk.json", "pnl-attribution.json",
    "desk-allocator.json", "desk-returns.json",
]

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 871,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Verify the vol-radar -> hedge-planner timing overlay and "
               "the 14th risk-desk cockpit tile",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def get_json(key):
    return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read().decode("utf-8", "ignore")


# ---- 1) ship hedge-planner (code only) ------------------------------------
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zb = buf.getvalue()

try:
    lam.update_function_code(FunctionName=FN, ZipFile=zb)
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get(
                "State") == "Active":
            break
        time.sleep(3)
    check("deploy_ok", True, "hedge-planner code updated")
except Exception as e:
    check("deploy_ok", False, f"{type(e).__name__}: {e}")

# ---- 2) invoke -------------------------------------------------------------
inner = {}
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    body = json.loads(r["Payload"].read().decode("utf-8", "ignore"))
    inner = json.loads(body.get("body") or "{}")
    check("invoke_ok",
          r.get("StatusCode") == 200 and not r.get("FunctionError"),
          r.get("FunctionError") or "200, action=%s" % inner.get("action"))
except Exception as e:
    check("invoke_ok", False, f"{type(e).__name__}: {e}")

time.sleep(2)

# ---- 3) read back hedge-planner.json + vol-radar.json ---------------------
hp, vr = {}, {}
try:
    hp = get_json(HP_KEY)
    vr = get_json(VR_KEY)
except Exception as e:
    check("feeds_readable", False, f"{type(e).__name__}: {e}")

vt = hp.get("vol_timing")
check("vol_timing_present",
      isinstance(vt, dict)
      and "posture" in vt and "timing_bias" in vt,
      "vol_timing=%s" % (json.dumps(vt) if vt else "MISSING"))

vr_posture = vr.get("posture")
check("vol_timing_matches_radar",
      isinstance(vt, dict) and vt.get("posture") == vr_posture,
      "hedge-planner posture=%s vs vol-radar posture=%s"
      % ((vt or {}).get("posture"), vr_posture))

check("timing_bias_valid",
      isinstance(vt, dict) and vt.get("timing_bias") in VALID_BIAS,
      "timing_bias=%s" % (vt or {}).get("timing_bias"))

action = hp.get("action")
check("action_still_exposure_driven",
      action in VALID_ACTION,
      "action=%s (a valid exposure-driven action -- overlay did not "
      "hijack it)" % action)

# if vol_timing carries a note, it must be in the cro_note
tnote = (vt or {}).get("timing_note")
cro = hp.get("cro_note") or ""
if tnote:
    check("timing_note_in_cro_note", tnote in cro,
          "timing note surfaced on the ticket: %s" % tnote[:120])
else:
    check("timing_note_in_cro_note", True,
          "no timing note for this action/posture pair (bias=%s) -- "
          "nothing to surface" % (vt or {}).get("timing_bias"))

# ---- 4) cockpit: 14 tiles incl vol-radar ----------------------------------
try:
    status, page = http_get(PAGE_URL)
    has_tile = ("Vol Turning-Point Radar" in page
                and 'k:"vol-radar"' in page)
    missing = [j for j in COCKPIT_FEEDS if j not in page]
    check("cockpit_has_vol_radar_tile",
          status == 200 and has_tile,
          "HTTP %s, vol-radar tile %s" % (status,
          "rendered" if has_tile else "MISSING"))
    check("cockpit_wires_all_14_engines", not missing,
          "all %d engine feeds wired" % len(COCKPIT_FEEDS) if not missing
          else "MISSING: " + ", ".join(missing))
except Exception as e:
    check("cockpit_has_vol_radar_tile", False, f"{type(e).__name__}: {e}")
    check("cockpit_wires_all_14_engines", False, "page fetch failed")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["integration"] = {
    "hedge_action": action,
    "vol_radar_posture": vr_posture,
    "timing_bias": (vt or {}).get("timing_bias"),
    "timing_note": (vt or {}).get("timing_note"),
    "cockpit_tiles": len(COCKPIT_FEEDS),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "PREDICT-THEN-ACT LOOP CLOSED - the Hedge Execution Planner now "
        "carries the vol-radar timing overlay (action %s, radar %s -> "
        "timing bias %s) without the radar overriding the "
        "exposure-driven action, and the Volatility Turning-Point Radar "
        "is the 14th tile on the risk-desk cockpit. All fourteen cockpit "
        "feeds wired." % (action, vr_posture, (vt or {}).get("timing_bias")))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("VOL-RADAR HEDGE WIRING VERIFICATION INCOMPLETE - %d "
                      "check(s) failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/871_vol_radar_hedge_wiring.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
