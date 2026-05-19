"""
ops/869 - VERIFY the economic calendar -> morning intelligence brief
wiring.

The morning brief (justhodl-morning-intelligence) now reads
data/econ-calendar.json and carries an ECON_CALENDAR line: the next
major US release with its consensus and countdown, the tier-one
releases due this week, and the recent surprise tally.

The morning brief Lambda always sends a Telegram message and calls
the model on invoke, so this op proves the wiring WITHOUT triggering
an off-schedule brief:

  1. Ship the updated function.
  2. Download the DEPLOYED code artifact and confirm it actually
     contains the integration - the econ_calendar feed key, the
     econ_next_major metric and the ECON_CALENDAR brief line.
  3. Read the live data/econ-calendar.json and confirm it is fresh
     and carries the fields the integration consumes.
  4. Apply the exact extraction transform to the live feed and prove
     the ECON_CALENDAR brief line renders with real content.

No Telegram message, no model call. Writes
aws/ops/reports/869_econ_brief_wiring.json.
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
FN = "justhodl-morning-intelligence"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
EC_KEY = "data/econ-calendar.json"

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 869,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Verify the econ-calendar -> morning intelligence brief "
               "wiring (deployed artifact + live feed + render transform)",
    "checks": [],
}


def check(name, ok, detail=""):
    rep["checks"].append({"check": name, "ok": bool(ok),
                          "detail": str(detail)[:240]})
    return ok


def http_get_bytes(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.read()


# ---- 1) ship ---------------------------------------------------------------
src_text = open(SRC, encoding="utf-8").read()
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", src_text)
zb = buf.getvalue()

try:
    cfg0 = lam.get_function_configuration(FunctionName=FN)
    lam.update_function_code(FunctionName=FN, ZipFile=zb)
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get(
                "State") == "Active":
            break
        time.sleep(3)
    check("deploy_ok", True, "code updated, runtime=%s"
          % cfg0.get("Runtime"))
except Exception as e:
    check("deploy_ok", False, f"{type(e).__name__}: {e}")

# ---- 2) inspect the DEPLOYED artifact -------------------------------------
deployed_src = ""
try:
    info = lam.get_function(FunctionName=FN)
    loc = info["Code"]["Location"]
    zbytes = http_get_bytes(loc)
    with zipfile.ZipFile(io.BytesIO(zbytes)) as z:
        name = next((n for n in z.namelist()
                     if n.endswith("lambda_function.py")), None)
        if name:
            deployed_src = z.read(name).decode("utf-8", "ignore")
    markers = {
        "econ_calendar feed key": '"econ_calendar":"data/econ-calendar.json"'
        in deployed_src,
        "econ_next_major metric": "econ_next_major" in deployed_src,
        "ECON_CALENDAR brief line": "ECON_CALENDAR:" in deployed_src,
    }
    check("deployed_artifact_has_integration", all(markers.values()),
          ", ".join("%s=%s" % (k, v) for k, v in markers.items()))
except Exception as e:
    check("deployed_artifact_has_integration", False,
          f"{type(e).__name__}: {e}")

# ---- 3) live econ-calendar feed -------------------------------------------
ec = {}
try:
    ec = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=EC_KEY
                                  )["Body"].read())
    ts = ec.get("generated_at")
    age_h = None
    if ts:
        t = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        age_h = round((datetime.now(timezone.utc) - t).total_seconds()
                      / 3600.0, 1)
    has_fields = (isinstance(ec.get("next_major"), dict)
                  and isinstance(ec.get("counts"), dict)
                  and isinstance(ec.get("recent_surprise_tally"), dict))
    check("live_feed_fresh_and_shaped",
          ec.get("feed_status") == "ok" and has_fields
          and age_h is not None and age_h <= 36,
          "feed_status=%s, age=%sh, next_major/counts/tally present=%s"
          % (ec.get("feed_status"), age_h, has_fields))
except Exception as e:
    check("live_feed_fresh_and_shaped", False,
          f"{type(e).__name__}: {e}")

# ---- 4) apply the extraction transform + render the brief line ------------
try:
    m = {
        "econ_next_major": (ec.get("next_major") or {}).get("event"),
        "econ_next_major_days": (ec.get("next_major") or {}).get(
            "days_until"),
        "econ_next_major_consensus": (ec.get("next_major") or {}).get(
            "consensus"),
        "econ_this_week_releases": (ec.get("counts") or {}).get("this_week"),
        "econ_this_week_tier1": (ec.get("counts") or {}).get(
            "this_week_tier1"),
        "econ_recent_above": (ec.get("recent_surprise_tally") or {}).get(
            "above"),
        "econ_recent_below": (ec.get("recent_surprise_tally") or {}).get(
            "below"),
        "econ_this_week_tier1_events": [
            str(e.get("event")) + " (" + str(e.get("date")) + ")"
            for e in (ec.get("this_week") or []) if e.get("tier1")
        ][:4],
    }
    line = ("ECON_CALENDAR: next major US release: "
            + str(m.get("econ_next_major") or "none") + " in "
            + str(m.get("econ_next_major_days")
                  if m.get("econ_next_major_days") is not None else "?")
            + "d (consensus " + str(m.get("econ_next_major_consensus")
                                    or "?") + "). This week: "
            + str(m.get("econ_this_week_releases") or 0) + " releases, "
            + str(m.get("econ_this_week_tier1") or 0) + " tier-1"
            + ((" - " + "; ".join(m.get("econ_this_week_tier1_events") or []))
               if m.get("econ_this_week_tier1_events") else "")
            + ". Recent surprises: " + str(m.get("econ_recent_above") or 0)
            + " above / " + str(m.get("econ_recent_below") or 0)
            + " below consensus.")
    rep["rendered_brief_line"] = line
    # non-trivial = it names a real release OR a real surprise count
    non_trivial = (m.get("econ_next_major") not in (None, "none")
                   or (m.get("econ_recent_above") or 0) > 0
                   or (m.get("econ_this_week_releases") or 0) > 0)
    check("brief_line_renders_with_real_data", non_trivial,
          line[:200])
except Exception as e:
    check("brief_line_renders_with_real_data", False,
          f"{type(e).__name__}: {e}")

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
if rep["all_passed"]:
    rep["verdict"] = (
        "ECON CALENDAR WIRED INTO THE MORNING BRIEF - the deployed "
        "morning-intelligence artifact carries the integration, the live "
        "econ-calendar feed is fresh and well-shaped, and the extraction "
        "transform renders a real ECON_CALENDAR line. From the next 8AM ET "
        "run the brief opens the day with the macro release schedule.")
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("ECON BRIEF WIRING VERIFICATION INCOMPLETE - %d "
                      "check(s) failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/869_econ_brief_wiring.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
