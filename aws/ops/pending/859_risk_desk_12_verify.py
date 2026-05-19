"""
ops/859 - RE-VERIFY the Risk Desk cockpit after wiring in the Hedge
Execution Planner as the twelfth engine tile.

ops/857 proved the cockpit at eleven engines. The Hedge Execution
Planner (justhodl-hedge-planner, verified by ops/858) is now wired in
as the twelfth tile, in the Synthesis, Hedge & Execution group. This op
proves the updated page is sound:

  1. GET the deployed risk-desk.html - live, cockpit markers present,
     and the new group label rendered.
  2. The page JS now wires all twelve risk-stack engine feeds, the
     hedge-planner.json feed included.
  3. The Hedge Execution Planner feed reads back from S3, is fresh and
     carries the action + action_color the tile renders.
  4. The directory still links both the Risk Desk and the Hedge Planner.

Writes aws/ops/reports/859_risk_desk_12_verify.json.
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

# The full twelve-engine risk stack the cockpit now renders.
ENGINE_FEEDS = [
    "firm-risk-board.json", "tail-hedge.json", "hedge-planner.json",
    "risk-monitor.json", "liquidity-capacity.json",
    "firm-stress.json", "merger-arb-risk.json",
    "firm-book.json", "factor-risk.json", "pnl-attribution.json",
    "desk-allocator.json", "desk-returns.json",
]

cfg = Config(read_timeout=60, connect_timeout=20, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 859,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Re-verify the Risk Desk cockpit after wiring the Hedge "
               "Execution Planner in as the twelfth engine tile",
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


# ---- 1) page deployed + new group rendered --------------------------------
page = ""
try:
    status, page = http_get(PAGE_URL)
    ok = (status == 200
          and "Risk Desk" in page
          and "Risk Stack Status Board" in page
          and "Synthesis, Hedge" in page)
    check("page_deployed_with_exec_group", ok,
          "HTTP %s, %d bytes, exec group %s"
          % (status, len(page),
             "rendered" if "Synthesis, Hedge" in page else "MISSING"))
except Exception as e:
    check("page_deployed_with_exec_group", False,
          "%s: %s" % (type(e).__name__, e))

# ---- 2) all twelve engine feeds wired -------------------------------------
missing = [j for j in ENGINE_FEEDS if j not in page]
check("page_wires_all_12_engines", not missing,
      "all %d engine feeds referenced (hedge-planner included)"
      % len(ENGINE_FEEDS) if not missing
      else "MISSING from page JS: " + ", ".join(missing))

# ---- 3) the Hedge Execution Planner tile will populate --------------------
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key="data/hedge-planner.json")
    hp = json.loads(obj["Body"].read())
    age = age_hours(hp)
    fresh = age is not None and age <= FRESH_HOURS
    action = hp.get("action")
    color = hp.get("action_color")
    ok = (bool(hp.get("generated_at")) and fresh
          and action not in (None, "")
          and color in ("green", "cyan", "orange", "red", "dim"))
    check("hedge_planner_tile_populates", ok,
          "age %sh, action=%s, action_color=%s" % (age, action, color))
except Exception as e:
    hp = {}
    check("hedge_planner_tile_populates", False,
          "%s: %s" % (type(e).__name__, e))

# ---- 4) directory links both pages ----------------------------------------
try:
    dstatus, dpage = http_get(DIR_URL)
    ok = (dstatus == 200
          and "/risk-desk.html" in dpage
          and "/hedge-planner.html" in dpage)
    check("directory_links_both", ok,
          "directory %s, risk-desk %s, hedge-planner %s"
          % (dstatus,
             "linked" if "/risk-desk.html" in dpage else "MISSING",
             "linked" if "/hedge-planner.html" in dpage else "MISSING"))
except Exception as e:
    check("directory_links_both", False, "%s: %s" % (type(e).__name__, e))

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["cockpit"] = {
    "page_url": PAGE_URL,
    "engines_on_cockpit": len(ENGINE_FEEDS),
    "hedge_planner_action": hp.get("action"),
    "hedge_planner_headline": hp.get("headline"),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "RISK DESK COCKPIT COMPLETE - the Hedge Execution Planner is the "
        "twelfth engine tile, in the Synthesis, Hedge & Execution group. "
        "The cockpit now carries the full decision spine on one screen: "
        "diagnose (Firm Risk Board) -> size (Tail Hedge Overlay) -> "
        "execute (Hedge Planner, current action %s). All twelve feeds "
        "wired, directory linked." % hp.get("action"))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("RISK DESK COCKPIT RE-VERIFY INCOMPLETE - %d check(s) "
                      "failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/859_risk_desk_12_verify.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
