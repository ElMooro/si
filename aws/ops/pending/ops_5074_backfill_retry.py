"""ops_5074 -- fix the silent shard crash; re-run backfill and sentinel.

ops 5072 reported "fanout -> invoked: 12" three times and zero state
documents, three times. Accepted invocations plus no evidence of work is
not a mystery, it is a worker dying before its first write -- and I let
it look like "never ran" for forty minutes.

Cause, reproduced offline: _now() returns a datetime and the engine's
_put_json calls json.dumps WITHOUT default=str, so
st["updated_at"] = _now() raises TypeError on the first checkpoint.
Twelve shards, twelve identical crashes, nothing written. Fixed by
storing an isoformat string.

The second fix matters more than the first: the worker now writes its
state document on ENTRY, recording started_at and todo_at_start before
it does anything that can throw. A worker that leaves no trace is
indistinguishable from one that was never invoked, and that ambiguity
cost this session an entire drain window.

ops 5073 also went RED -- the dead-lanes chip was absent from the health
document even though the sentinel ran and rewrote it. That points at the
deploy rather than the code, so this re-checks the deployed timestamp
before drawing any conclusion.

  P0 confirm BOTH deploys actually landed, by timestamp
  P1 one shard, synchronously, and read the response -- no fanout until
     a single worker is proven
  P2 fan out and drain
  P3 sentinel: is dead-lanes there now; has the banner cleared
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
GD = "justhodl-gdelt-full"
SENTINEL = "justhodl-import-sentinel"
SHARDS = 12

cfg = Config(read_timeout=900, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=LIVE,
                                        Key=k)["Body"].read())
    except Exception:
        return None


def bf():
    t = {"rec": 0, "perm": 0, "left": 0, "bytes": 0, "started": 0}
    for k in range(SHARDS):
        d = jget("data/_state/gdelt-backfill-s%d.json" % k)
        if not d:
            continue
        t["started"] += 1
        t["rec"] += int(d.get("recovered") or 0)
        t["perm"] += len(d.get("permanent") or [])
        t["left"] += int(d.get("remaining") or 0)
        t["bytes"] += int(d.get("bytes") or 0)
    return t


with report("ops_5074_backfill_retry") as R:
    fails = []
    out = {"op": "ops_5074"}

    R.section("P0 did both deploys land")
    for fn in (GD, SENTINEL):
        got = None
        for i in range(16):
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                lm = (c.get("LastModified") or "")[:19]
                if lm >= (NOW - timedelta(minutes=14)).strftime(
                        "%Y-%m-%dT%H:%M:%S"):
                    got = lm
                    break
            except Exception:
                pass
            time.sleep(20)
        R.log("  %-30s %s" % (fn, got or "NOT FRESH -- deploy lagging"))
        if not got:
            R.log("    (ops 5073's absent chip is explained by this, not "
                  "by the check itself)")

    R.section("P1 ONE shard, synchronously")
    try:
        r = lam.invoke(FunctionName=GD, InvocationType="RequestResponse",
                       Payload=json.dumps({"backfill": True, "shard": 0,
                                           "shards": SHARDS}).encode())
        body = (r["Payload"].read() or b"").decode("utf-8", "replace")
        R.log("  status=%s FunctionError=%s" % (r.get("StatusCode"),
                                                r.get("FunctionError")))
        R.log("  %s" % body[:420])
        if r.get("FunctionError"):
            R.log("  the worker still raises -- fanout would hide this "
                  "twelve times over")
            fails.append("P1:funcerror")
    except Exception as e:
        R.log("  invoke err %s" % str(e)[:200])
        fails.append("P1:invoke")
    s0 = jget("data/_state/gdelt-backfill-s0.json")
    if s0:
        R.log("  shard 0 state EXISTS: started_at=%s todo_at_start=%s "
              "recovered=%s permanent=%s" % (
                  s0.get("started_at"), s0.get("todo_at_start"),
                  s0.get("recovered"), len(s0.get("permanent") or [])))
    else:
        R.log("  shard 0 STILL wrote nothing -- write-on-entry did not "
              "take, so the crash is earlier than the first checkpoint")
        fails.append("P1:nostate")

    R.section("P2 fan out")
    b0 = bf()
    t0 = time.time()
    for cyc in range(2):
        try:
            r = lam.invoke(FunctionName=GD, InvocationType="Event",
                           Payload=json.dumps({"backfill_fanout": True,
                                               "shards": SHARDS}
                                              ).encode())
            R.log("  fanout sent")
        except Exception as e:
            R.log("  fanout err %s" % str(e)[:110])
        time.sleep(800)
        b = bf()
        R.log("  t+%2.0fmin shards_started=%d/%d recovered=%s "
              "permanent=%s remaining=%s %.2f GB" % (
                  (time.time() - t0) / 60.0, b["started"], SHARDS,
                  f"{b['rec']:,}", f"{b['perm']:,}", f"{b['left']:,}",
                  b["bytes"] / 1e9))
    b = bf()
    resolved = (b["rec"] + b["perm"]) - (b0["rec"] + b0["perm"])
    R.log("  resolved %s slots this run" % f"{resolved:,}")
    R.log("  RECOVERED %s · PERMANENT %s (404 again: never published) · "
          "REMAINING %s of 7,381" % (f"{b['rec']:,}", f"{b['perm']:,}",
                                     f"{b['left']:,}"))
    if b["started"] == 0:
        fails.append("P2:noshards")
    out.update(recovered=b["rec"], permanent=b["perm"],
               remaining=b["left"], shards_started=b["started"],
               gb=round(b["bytes"] / 1e9, 2))

    R.section("P3 sentinel + banner")
    try:
        lam.invoke(FunctionName=SENTINEL, InvocationType="Event",
                   Payload=b"{}")
    except Exception as e:
        R.log("  sentinel invoke err %s" % str(e)[:100])
    prev = (jget("data/import-health.json") or {}).get("generated_at")
    h = {}
    for i in range(14):
        time.sleep(30)
        h = jget("data/import-health.json") or {}
        if h.get("generated_at") != prev:
            break
    dl = next((p for p in (h.get("pipelines") or [])
               if p.get("name") == "dead-lanes"), None)
    if dl:
        R.log("  dead-lanes: %s -- %s" % (dl.get("status"),
                                          str(dl.get("detail"))[:220]))
        out["dead_lanes"] = dl.get("status")
    else:
        R.log("  dead-lanes still absent")
        fails.append("P3:absent")
    R.log("  overall=%s worst=%s" % (h.get("overall"), h.get("worst")))
    cs = jget("data/warm/census-us/_state/state.json") or {}
    R.log("  census-us state updated_at=%s (STALE clears when the "
          "timeseries walker next writes)" % cs.get("updated_at"))
    out["overall"] = h.get("overall")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/gdelt-backfill.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5074 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(recovered=out.get("recovered"), permanent=out.get("permanent"),
         remaining=out.get("remaining"),
         shards=out.get("shards_started"), overall=out.get("overall"))
    R.log("ops 5074 GREEN")
