"""ops_5073 -- make the invisible failure mode visible, permanently.

Tonight's real finding was not any one lane. It was that five of the
fleet's largest gaps -- census-us, boj-full, gdelt-full, repo,
fundamental-census -- were not slow, throttled or refused by their
sources. They had no trigger, or one that had stopped firing, and sat
idle for between 4 and 16 days while their state documents looked
entirely healthy. Nothing checked for it, which is precisely why it went
unnoticed. Every fix tonight was cleanup; this is the part that stops it
recurring.

The check is staleness rather than "has a rule", deliberately. An
untriggered lane, a crashed lane and a throttled lane all stop writing
state, so staleness catches every cause with one signal and no
EventBridge or CloudWatch calls -- one list per provider, cheap enough
for every sweep. 48h is the threshold: against tonight's observed ages
it flags repo 387h, fundamental-census 171h, gdelt-full 141h,
hist-banker 116h and boj-full, while the 22.7h orchestrator-driven
cluster stays quiet. A chip that cried wolf on seventeen healthy lanes
would be worse than no chip.

  P0 deploy, run the sentinel, read the new dead-lanes pipeline
  P1 does it agree with what we know is actually dead
  P2 has overall left DEGRADED now census-us is triggered
  P3 the running imports: GDELT backfill, BOJ, census-econ
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
SENTINEL = "justhodl-import-sentinel"

cfg = Config(read_timeout=300, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=LIVE,
                                        Key=k)["Body"].read())
    except Exception:
        return None


with report("ops_5073_dead_lane_alarm") as R:
    fails = []
    out = {"op": "ops_5073"}

    R.section("P0 deploy + run the sentinel")
    for i in range(16):
        try:
            c = lam.get_function_configuration(FunctionName=SENTINEL)
            if (c.get("LastModified") or "")[:19] >= (
                    NOW - timedelta(minutes=14)).strftime(
                        "%Y-%m-%dT%H:%M:%S"):
                R.log("  code fresh %s" % c.get("LastModified"))
                break
        except Exception:
            pass
        time.sleep(20)
    before = jget("data/import-health.json") or {}
    R.log("  before: overall=%s worst=%s" % (before.get("overall"),
                                             before.get("worst")))
    try:
        lam.invoke(FunctionName=SENTINEL, InvocationType="Event",
                   Payload=b"{}")
        R.log("  sentinel kicked")
    except Exception as e:
        R.log("  invoke err %s" % str(e)[:110])
    h = before
    for i in range(14):
        time.sleep(30)
        h = jget("data/import-health.json") or {}
        if h.get("generated_at") != before.get("generated_at"):
            R.log("  health doc rewritten after %ds" % ((i + 1) * 30))
            break

    R.section("P1 the new pipeline")
    dl = next((p for p in (h.get("pipelines") or [])
               if p.get("name") == "dead-lanes"), None)
    if not dl:
        R.log("  dead-lanes pipeline ABSENT -- the check did not run")
        fails.append("P1:absent")
    else:
        R.log("  dead-lanes: %s" % dl.get("status"))
        R.log("  %s" % str(dl.get("detail"))[:300])
        out["dead_lanes"] = dl
    R.log("  all pipelines now:")
    for p in (h.get("pipelines") or []):
        R.log("    %-16s %-16s %s" % (p.get("name"), p.get("status"),
                                      str(p.get("detail"))[:70]))

    R.section("P2 has the banner cleared")
    R.log("  overall %s -> %s   worst %s -> %s" % (
        before.get("overall"), h.get("overall"),
        before.get("worst"), h.get("worst")))
    if h.get("overall") == "HEALTHY":
        R.log("  IMPORT HEALTHY -- first time since the census-us chip "
              "went stale")
    else:
        R.log("  still %s because of %s; that is now a lane with a name "
              "rather than a mystery" % (h.get("overall"),
                                         h.get("worst")))
    out["overall"] = h.get("overall")
    out["worst"] = h.get("worst")
    R.log("  incidents retained: %d (aged to 14 days by ops 5056)"
          % len(h.get("incidents") or []))

    R.section("P3 the running imports")
    bf = {"recovered": 0, "permanent": 0, "remaining": 0, "bytes": 0}
    for k in range(12):
        d = jget("data/_state/gdelt-backfill-s%d.json" % k) or {}
        bf["recovered"] += int(d.get("recovered") or 0)
        bf["permanent"] += len(d.get("permanent") or [])
        bf["remaining"] += int(d.get("remaining") or 0)
        bf["bytes"] += int(d.get("bytes") or 0)
    R.log("  gdelt backfill: recovered=%s permanent=%s remaining=%s "
          "%.2f GB" % (f"{bf['recovered']:,}", f"{bf['permanent']:,}",
                       f"{bf['remaining']:,}", bf["bytes"] / 1e9))
    if bf["recovered"] + bf["permanent"] >= 7381:
        R.log("  every one of the 7,381 slots is now resolved -- banked "
              "or proven unpublished")
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
    R.log("  boj: %s/%s series (%.1f%%) rows %s" % (
        f"{tot['done']:,}", f"{tot['codes']:,}",
        100.0 * tot["done"] / max(1, tot["codes"]), f"{tot['rows']:,}"))
    ce = sum(len((jget("data/_state/census-econ-s%d.json" % k) or {})
                 .get("done") or []) for k in range(12))
    R.log("  census-econ: %s/1226 entries" % f"{ce:,}")
    out.update(gdelt=bf, boj=tot, census_econ=ce)
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/dead-lane-alarm.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/dead-lane-alarm.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5073 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(dead_lanes=(out.get("dead_lanes") or {}).get("status"),
         overall=out.get("overall"),
         gdelt_recovered=bf["recovered"], boj=tot["done"],
         census_econ=ce)
    R.log("ops 5073 GREEN -- the silent failure mode now has a chip")
