"""
ops/866 - VERIFY the Risk Desk cockpit at thirteen engines.

The Hedge Overlay Scorecard (justhodl-hedge-pnl, verified by ops/865)
is now wired into risk-desk.html as the thirteenth tile, in the
Synthesis, Hedge & Execution group - completing the hedge lifecycle on
one screen: synthesise -> size -> execute -> score.

This op proves the updated page:

  1. GET the deployed risk-desk.html - live, cockpit markers present.
  2. The page JS wires all thirteen risk-stack engine feeds, the
     hedge-pnl.json feed included.
  3. The Hedge Overlay Scorecard feed reads back from S3, is fresh and
     carries the verdict + verdict_color the tile renders, and that
     verdict_color resolves in the cockpit colour map.
  4. The directory still links the scorecard page.

Writes aws/ops/reports/866_cockpit_13_verify.json.
Self-sufficient: S3 reads + public HTTP GETs, no Lambda deploy.
"""
import json
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGE_URL = "https://justhodl.ai/risk-desk.html"
DIR_URL = "https://justhodl.ai/directory.html"
FRESH_HOURS = 40.0

# the full thirteen-engine risk stack the cockpit now renders
ENGINE_FEEDS = [
    "firm-risk-board.json", "tail-hedge.json", "hedge-planner.json",
    "hedge-pnl.json",
    "risk-monitor.json", "liquidity-capacity.json",
    "firm-stress.json", "merger-arb-risk.json",
    "firm-book.json", "factor-risk.json", "pnl-attribution.json",
    "desk-allocator.json", "desk-returns.json",
]
# colours the cockpit COL map must resolve (verdict_color can be yellow)
COCKPIT_COLORS = {"green", "orange", "red", "cyan", "yellow", "dim"}

cfg = Config(read_timeout=60, connect_timeout=20, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 866,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Re-verify the Risk Desk cockpit after wiring the Hedge "
               "Overlay Scorecard in as the thirteenth engine tile",
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


def age_hours(doc):
    ts = doc.get("generated_at") if isinstance(doc, dict) else None
    if not ts:
        return None
    try:
        t = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return round((datetime.now(timezone.utc) - t).total_seconds()
                     / 3600.0, 1)
    except Exception:
        return None


# ---- 1) page deployed ------------------------------------------------------
page = ""
try:
    status, page = http_get(PAGE_URL)
    ok = (status == 200 and "Risk Desk" in page
          and "Risk Stack Status Board" in page
          and "Hedge Overlay Scorecard" in page)
    check("page_deployed_with_scorecard_tile", ok,
          "HTTP %s, %d bytes, scorecard tile %s"
          % (status, len(page),
             "rendered" if "Hedge Overlay Scorecard" in page else "MISSING"))
except Exception as e:
    check("page_deployed_with_scorecard_tile", False,
          "%s: %s" % (type(e).__name__, e))

# ---- 2) all thirteen feeds wired ------------------------------------------
missing = [j for j in ENGINE_FEEDS if j not in page]
check("page_wires_all_13_engines", not missing,
      "all %d engine feeds referenced (hedge-pnl included)"
      % len(ENGINE_FEEDS) if not missing
      else "MISSING from page JS: " + ", ".join(missing))

# ---- 3) the scorecard tile will populate ----------------------------------
hp = {}
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key="data/hedge-pnl.json")
    hp = json.loads(obj["Body"].read())
    age = age_hours(hp)
    fresh = age is not None and age <= FRESH_HOURS
    verdict = hp.get("verdict")
    vcolor = hp.get("verdict_color")
    ok = (bool(hp.get("generated_at")) and fresh
          and verdict not in (None, "")
          and vcolor in COCKPIT_COLORS)
    check("scorecard_tile_populates", ok,
          "age %sh, verdict=%s, verdict_color=%s (resolves in cockpit "
          "map: %s)" % (age, verdict, vcolor, vcolor in COCKPIT_COLORS))
except Exception as e:
    check("scorecard_tile_populates", False,
          "%s: %s" % (type(e).__name__, e))

# ---- 4) directory links the scorecard -------------------------------------
try:
    dstatus, dpage = http_get(DIR_URL)
    check("directory_links_scorecard",
          dstatus == 200 and "/hedge-pnl.html" in dpage,
          "directory %s, hedge-pnl link %s"
          % (dstatus, "present" if "/hedge-pnl.html" in dpage else "MISSING"))
except Exception as e:
    check("directory_links_scorecard", False,
          "%s: %s" % (type(e).__name__, e))

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["cockpit"] = {
    "page_url": PAGE_URL,
    "engines_on_cockpit": len(ENGINE_FEEDS),
    "scorecard_verdict": hp.get("verdict"),
    "scorecard_maturity": hp.get("maturity"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "RISK DESK COCKPIT AT THIRTEEN - the Hedge Overlay Scorecard is "
        "the thirteenth tile, completing the hedge lifecycle in the "
        "Synthesis, Hedge & Execution group: synthesise -> size -> "
        "execute -> score (current verdict %s, %s). All thirteen feeds "
        "wired, directory linked."
        % (hp.get("verdict"), hp.get("maturity")))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("COCKPIT RE-VERIFY INCOMPLETE - %d check(s) failed: "
                      "%s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/866_cockpit_13_verify.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
