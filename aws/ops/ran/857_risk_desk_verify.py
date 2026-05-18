"""
ops/857 - VERIFY the Risk Desk (the CRO cockpit over the firm risk stack).

risk-desk.html is a read-only landing page: it fetches the eleven
risk-stack engine JSONs from S3 client-side, leads with the Firm Risk
Board verdict and renders every engine as a status tile. This op proves
the page is sound end-to-end without anyone opening a browser:

  1. GET the deployed page from GitHub Pages - it must be live and carry
     the cockpit markers (title, status board, the eleven engine JSON
     names its tiles fetch).
  2. For each of the eleven risk engines, read its live S3 output and
     prove the tile will populate: object present, JSON valid, carries
     generated_at, reasonably fresh, and - where the engine exposes a
     posture - the posture field the page reads is non-empty.
  3. Prove the hero source: the Firm Risk Board carries firm_posture,
     headline, cro_brief and a binding_constraint.
  4. Prove the strategy-desk strip: the Desk Allocator carries a
     non-empty desks[] with names and capital weights.
  5. Confirm the directory links the new page.

Writes aws/ops/reports/857_risk_desk_verify.json.

Self-sufficient: pure S3 reads + public HTTP GETs, no Lambda deploy.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGE_URL = "https://justhodl.ai/risk-desk.html"
DIR_URL = "https://justhodl.ai/directory.html"
FRESH_HOURS = 40.0

# Mirror of the page's engine config: (key, json file, posture field or None).
ENGINES = [
    ("firm-risk-board", "firm-risk-board.json", "firm_posture"),
    ("tail-hedge",      "tail-hedge.json",      "status"),
    ("risk-monitor",    "risk-monitor.json",    "risk_posture"),
    ("liquidity-capacity", "liquidity-capacity.json", "liquidity_posture"),
    ("firm-stress",     "firm-stress.json",     "posture"),
    ("merger-arb-risk", "merger-arb-risk.json", "posture"),
    ("firm-book",       "firm-book.json",       None),
    ("factor-risk",     "factor-risk.json",     None),
    ("pnl-attribution", "pnl-attribution.json", "posture"),
    ("desk-allocator",  "desk-allocator.json",  None),
    ("desk-returns",    "desk-returns.json",    None),
]

cfg = Config(read_timeout=60, connect_timeout=20, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)

rep = {
    "ops": 857,
    "ts": datetime.now(timezone.utc).isoformat(),
    "subject": "Verify the Risk Desk (the CRO cockpit landing page over the "
               "firm's eleven-engine risk stack)",
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


# ---- 1) page is deployed ---------------------------------------------------
page = ""
try:
    status, page = http_get(PAGE_URL)
    ok = (status == 200
          and "Risk Desk" in page
          and "Risk Stack Status Board" in page)
    check("page_deployed", ok,
          "HTTP %s, %d bytes, cockpit markers %s"
          % (status, len(page), "present" if ok else "MISSING"))
except Exception as e:
    check("page_deployed", False, "%s: %s" % (type(e).__name__, e))

# ---- 2) page wires every engine JSON --------------------------------------
missing = [j for _, j, _ in ENGINES if j not in page]
check("page_wires_all_engines", not missing,
      "all %d engine feeds referenced" % len(ENGINES) if not missing
      else "MISSING from page JS: " + ", ".join(missing))

# ---- 3) every engine feed populates a tile --------------------------------
feeds = {}
postures = {}
n_fresh = 0
for key, jf, pf in ENGINES:
    s3key = "data/" + jf
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=s3key)
        doc = json.loads(obj["Body"].read())
        feeds[key] = doc
        age = age_hours(doc)
        fresh = age is not None and age <= FRESH_HOURS
        if fresh:
            n_fresh += 1
        posture_val = None
        posture_ok = True
        if pf:
            posture_val = doc.get(pf)
            posture_ok = posture_val not in (None, "")
            postures[key] = posture_val
        has_gen = bool(doc.get("generated_at"))
        ok = has_gen and fresh and posture_ok
        detail = "age %sh" % age
        if pf:
            detail += ", %s=%s" % (pf, posture_val)
        else:
            detail += ", informational tile (no posture)"
        check("feed_" + key, ok, detail)
    except Exception as e:
        feeds[key] = None
        check("feed_" + key, False, "%s: %s" % (type(e).__name__, e))

# ---- 4) hero source: the Firm Risk Board ----------------------------------
board = feeds.get("firm-risk-board") or {}
bc = board.get("binding_constraint") or {}
hero_ok = (board.get("firm_posture")
           and board.get("headline")
           and board.get("cro_brief")
           and bc.get("label"))
check("hero_source_complete", hero_ok,
      "firm_posture=%s binding=%s cro_brief=%s"
      % (board.get("firm_posture"), bc.get("label"),
         "present" if board.get("cro_brief") else "MISSING"))

# ---- 5) strategy-desk strip ------------------------------------------------
alloc = feeds.get("desk-allocator") or {}
desks = alloc.get("desks") if isinstance(alloc, dict) else None
desks = desks if isinstance(desks, list) else []
desk_ok = (len(desks) >= 1
           and all(isinstance(d, dict)
                   and (d.get("name") or d.get("key"))
                   and "capital_weight_pct" in d
                   for d in desks))
check("desk_strip_populated", desk_ok,
      "%d strategy desks with names + capital weights" % len(desks))

# ---- 6) directory links the page ------------------------------------------
try:
    dstatus, dpage = http_get(DIR_URL)
    check("directory_links_page",
          dstatus == 200 and "/risk-desk.html" in dpage,
          "directory %s, link %s"
          % (dstatus, "present" if "/risk-desk.html" in dpage else "MISSING"))
except Exception as e:
    check("directory_links_page", False, "%s: %s" % (type(e).__name__, e))

# ---- summary ---------------------------------------------------------------
n_ok = sum(1 for c in rep["checks"] if c["ok"])
n_tot = len(rep["checks"])
rep["summary"] = "%d/%d checks passed" % (n_ok, n_tot)
rep["all_passed"] = n_ok == n_tot
rep["risk_desk"] = {
    "page_url": PAGE_URL,
    "engines_reporting": sum(1 for v in feeds.values() if v is not None),
    "engines_total": len(ENGINES),
    "feeds_fresh": n_fresh,
    "firm_posture": board.get("firm_posture"),
    "firm_headline": board.get("headline"),
    "binding_constraint": bc.get("label"),
    "live_postures": postures,
    "strategy_desks": len(desks),
}
if rep["all_passed"]:
    rep["verdict"] = (
        "RISK DESK LIVE - the CRO cockpit is deployed and sound. The page "
        "leads with the Firm Risk Board verdict (firm posture %s, binding "
        "constraint %s) and renders all %d risk engines as live status "
        "tiles, %d/%d feeds fresh, over a %d-desk allocation strip. Pure "
        "read-only aggregation - the Firm Risk Board stays the single "
        "source of truth."
        % (board.get("firm_posture"), bc.get("label"), len(ENGINES),
           n_fresh, len(ENGINES), len(desks)))
else:
    bad = [c["check"] for c in rep["checks"] if not c["ok"]]
    rep["verdict"] = ("RISK DESK VERIFICATION INCOMPLETE - %d check(s) "
                      "failed: %s." % (len(bad), ", ".join(bad)))

with open("aws/ops/reports/857_risk_desk_verify.json", "w",
          encoding="utf-8") as f:
    json.dump(rep, f, indent=1)
print(json.dumps(rep, indent=1))
