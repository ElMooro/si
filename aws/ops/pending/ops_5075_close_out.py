"""ops_5075 -- GDELT has no backlog; clear the banner; keep the rest moving.

ops 5074 settled GDELT definitively. All 12 shards ran, all 7,381 slots
were attempted, and every one returned 404 again:

    RECOVERED 0 · PERMANENT 7,381 · REMAINING 0

GDELT never published those 15-minute files -- 2,585 of them in 2020
alone, which is the shape of a source outage, not a crawler miss. So the
"7,381 gaps" that looked like the fleet's second-largest backlog is zero
backlog. The lane is complete. What was wrong was the reporting: a
counter that cannot distinguish "we failed to fetch" from "it does not
exist" will show a permanent number forever and make a healthy lane look
broken. The state now records that distinction so the card stops
claiming a gap that no longer exists.

Two things still open from 5074, both reporting rather than data:

  census-us STALE -- its timeseries state has not been written since
  2026-08-25T23:49, six days. It was given a trigger in ops 5061; this
  invokes it directly to prove the walker itself still works, because
  a wired-but-broken lane and an unwired one look identical from here.
  While it stays stale the banner reads DEGRADED and everything else on
  the page is judged by it.

  dead-lanes chip absent -- the sentinel ran and rewrote every other
  pipeline but the new check was not in the deployed code, which points
  at the deploy rather than the check.

  P0 GDELT: record permanent-vs-gap so the card tells the truth
  P1 census-us: invoke and prove the state moves
  P2 sentinel: deployed code timestamp, then re-run
  P3 BOJ and census-econ progress
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
GST = "data/warm/gdelt-full/_state/state.json"
CST = "data/warm/census-us/_state/state.json"

cfg = Config(read_timeout=600, retries={"max_attempts": 2})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=LIVE,
                                        Key=k)["Body"].read())
    except Exception:
        return None


with report("ops_5075_close_out") as R:
    fails = []
    out = {"op": "ops_5075"}

    R.section("P0 GDELT: gap vs never-published")
    perm = set()
    for k in range(12):
        d = jget("data/_state/gdelt-backfill-s%d.json" % k) or {}
        perm |= set(d.get("permanent") or [])
    R.log("  slots proven 404 at source: %s" % f"{len(perm):,}")
    g = jget(GST) or {}
    R.log("  state before: files=%s gaps=%s" % (g.get("files"),
                                                g.get("gaps")))
    if g and len(perm) >= 7000:
        g["gaps_permanent"] = len(perm)
        g["gaps_permanent_verified"] = NOW.isoformat(timespec="seconds")
        g["gaps_note"] = ("every one of these 15-min slots was re-"
                          "requested and returned 404: GDELT never "
                          "published them. Not a crawler gap.")
        g["gaps"] = max(0, int(g.get("gaps") or 0) - len(perm))
        try:
            s3.put_object(Bucket=LIVE, Key=GST,
                          Body=json.dumps(g, default=str).encode(),
                          ContentType="application/json")
            R.log("  state after : gaps=%s  gaps_permanent=%s" % (
                g["gaps"], g["gaps_permanent"]))
            R.log("  the card will now read a real gap count instead of "
                  "a permanent source artefact")
        except Exception as e:
            R.log("  write err %s" % str(e)[:110])
            fails.append("P0:write")
    else:
        R.log("  not enough permanent evidence to rewrite the counter")
    out["gdelt_permanent"] = len(perm)

    R.section("P1 census-us: does the walker still work")
    c0 = jget(CST) or {}
    R.log("  before: phase=%s n_done=%s rows=%s updated_at=%s" % (
        c0.get("phase"), c0.get("n_done"),
        f"{c0.get('rows_total') or 0:,}", c0.get("updated_at")))
    try:
        r = lam.invoke(FunctionName="justhodl-census-us",
                       InvocationType="RequestResponse", Payload=b"{}")
        R.log("  invoke status=%s err=%s" % (r.get("StatusCode"),
                                             r.get("FunctionError")))
        R.log("  %s" % (r["Payload"].read() or b"")[:300])
        if r.get("FunctionError"):
            fails.append("P1:funcerror")
    except Exception as e:
        R.log("  invoke err %s" % str(e)[:180])
    moved = False
    for i in range(10):
        time.sleep(45)
        c1 = jget(CST) or {}
        if c1.get("updated_at") != c0.get("updated_at"):
            moved = True
            R.log("  updated_at moved: %s -> %s (phase=%s)" % (
                c0.get("updated_at"), c1.get("updated_at"),
                c1.get("phase")))
            break
    if not moved:
        R.log("  state did NOT move -- the walker is broken, not merely "
              "untriggered; that is a different repair")
        fails.append("P1:nomove")
    out["census_moved"] = moved

    R.section("P2 sentinel deploy")
    try:
        c = lam.get_function_configuration(
            FunctionName="justhodl-import-sentinel")
        R.log("  deployed LastModified=%s CodeSize=%s" % (
            c.get("LastModified"), c.get("CodeSize")))
        age_h = (NOW - datetime.strptime(
            (c.get("LastModified") or "")[:19],
            "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)).total_seconds() / 3600.0
        R.log("  code age %.1f h -- the dead-lanes check was committed "
              "at 02:01 UTC" % age_h)
        if age_h > 12:
            R.log("  the deploy never picked it up; that is why the chip "
                  "is absent")
    except Exception as e:
        R.log("  cfg err %s" % str(e)[:110])
    prev = (jget("data/import-health.json") or {}).get("generated_at")
    try:
        lam.invoke(FunctionName="justhodl-import-sentinel",
                   InvocationType="Event", Payload=b"{}")
    except Exception:
        pass
    h = {}
    for i in range(12):
        time.sleep(30)
        h = jget("data/import-health.json") or {}
        if h.get("generated_at") != prev:
            break
    dl = next((p for p in (h.get("pipelines") or [])
               if p.get("name") == "dead-lanes"), None)
    R.log("  dead-lanes: %s" % (dl.get("status") if dl else "ABSENT"))
    R.log("  overall=%s worst=%s" % (h.get("overall"), h.get("worst")))
    out["overall"] = h.get("overall")
    out["dead_lanes"] = (dl or {}).get("status")

    R.section("P3 the lanes that are importing")
    tot = {"done": 0, "codes": 0, "rows": 0}
    kw = {"Bucket": LIVE, "Prefix": "data/warm/boj-full/_state/",
          "MaxKeys": 1000}
    while True:
        rr = s3.list_objects_v2(**kw)
        for o in rr.get("Contents", []):
            if "api_" not in o["Key"]:
                continue
            d = jget(o["Key"]) or {}
            tot["done"] += int(d.get("done") or 0)
            tot["codes"] += len(d.get("codes") or [])
            tot["rows"] += int(d.get("rows") or 0)
        if not rr.get("IsTruncated"):
            break
        kw["ContinuationToken"] = rr.get("NextContinuationToken")
    R.log("  BOJ         %s / %s series (%.1f%%) · %s rows" % (
        f"{tot['done']:,}", f"{tot['codes']:,}",
        100.0 * tot["done"] / max(1, tot["codes"]), f"{tot['rows']:,}"))
    ce = sum(len((jget("data/_state/census-econ-s%d.json" % k) or {})
                 .get("done") or []) for k in range(12))
    R.log("  census-econ %s / 1,226 entries" % f"{ce:,}")
    n = 0
    kw = {"Bucket": LIVE, "Prefix": "data/warm/census-econ/",
          "MaxKeys": 1000}
    while True:
        rr = s3.list_objects_v2(**kw)
        n += len(rr.get("Contents", []))
        if not rr.get("IsTruncated"):
            break
        kw["ContinuationToken"] = rr.get("NextContinuationToken")
    R.log("  census-econ objects in S3: %s" % f"{n:,}")
    out.update(boj=tot, census_econ=ce, census_econ_objects=n)
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/close-out.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5075 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(gdelt_permanent=out.get("gdelt_permanent"),
         census_moved=out.get("census_moved"), overall=out.get("overall"),
         boj=tot["done"], census_econ=ce)
    R.log("ops 5075 GREEN")
