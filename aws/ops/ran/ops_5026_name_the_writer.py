"""ops_5026 -- NAME the engine writing ~480k objects/day.

ops 5025 killed justhodl-signal-registry-ingest on the theory it was the
runaway writer. Its own evidence disproves that: the function's code is
    s3.get_object(Bucket, Key, Range='bytes=0-8191') -> ddb.put_item()
fired by EventBridge rule justhodl-signal-registry-s3-events on
{"source":["aws.s3"],"detail-type":["Object Created"]}. It is a READER.
Its 519k invocations/day were one-per-created-object -- the SYMPTOM, a
faithful counter of the storm, not its cause. Killing it removed the
GetObject + DynamoDB + EventBridge burn (real money) but cannot remove
one PutObject.

Cost Explorer, us-east-1, requests/day (ops 5024):
    PutObject   ~480,000/day EVERY day since Aug-9   <-- still burning
    GetObject   ~930,000/day (492k of it = the reader, now dead)
    ListBucket   42k -> 113k/day
    PutObjectForRepl@us-west-2 mirrored PUT 1:1 until ops 4988 killed
    replication on Aug-26; USW2-Requests-SIA-Tier1 $4.94 -> $0.02 -> 0.
So the us-west-2 half of the anomaly IS fixed. The us-east-1 half is not.

Fleet invocations are only ~10k/day once the reader is excluded, so no
engine is being invoked 480k times -- ONE run of ONE engine is writing
tens of thousands of objects. Guessing which cost us a session already.

ops 5024 armed S3 server access logging at 12:04 on Aug-28. Every line
now carries requester = arn:aws:sts::...:assumed-role/<role>/<SESSION>,
and for a Lambda the SESSION IS THE FUNCTION NAME. ~25h of logs exist.
This op reads them.

  P0 verify the Aug-28 state held (concurrency 0, replication gone,
     logging on, invocations collapsed)
  P1 current burn from Cost Explorer (Aug-24 -> today) -- is PutObject
     still ~480k/day after the kill?
  P2 ATTRIBUTION: parse a spread sample of access logs -> operation x
     requester x key-prefix, extrapolated to the real daily counts
  P3 S3 Inventory manifest (if delivered): exact prefix distribution of
     the 9.7M objects, so the purge can be aimed
  P4 bucket growth trend + verdict -> data/ops/s3-writer-attribution.json

GREEN = the writer is named with evidence. RED = access logs unreadable
(then attribution is impossible again and nothing may be killed on a
hunch).
"""
import csv
import gzip
import io
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

ACCT = "857687956942"
REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
LOGS = "justhodl-s3-access-logs-%s" % ACCT
FN_KILLED = "justhodl-signal-registry-ingest"
S3SVC = "Amazon Simple Storage Service"

MAX_LOG_FILES = 700          # spread sample across the window
MAX_LINES = 500000           # hard parse cap
MAX_INV_ROWS = 600000        # inventory rows to read

cfg = Config(read_timeout=90, retries={"max_attempts": 4})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
ce = boto3.client("ce", region_name="us-east-1", config=cfg)
cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)

NOW = datetime.now(timezone.utc)
TODAY = NOW.date()

# bucket_owner bucket [time] ip requester reqid operation key uri status
# error bytes_sent object_size total_time turnaround referer user_agent
LOG_RE = re.compile(
    r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) '
    r'("(?:[^"]|\\")*"|-) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
    r'("(?:[^"]|\\")*"|-) ("(?:[^"]|\\")*"|-)')


def short_requester(req):
    """arn:aws:sts::acct:assumed-role/role/SESSION -> SESSION (= fn name)."""
    if not req or req == "-":
        return "(anonymous)"
    if "/" in req:
        tail = req.rsplit("/", 1)[-1]
        if tail and tail != "-":
            return tail
    if ":user/" in req:
        return "iam:" + req.rsplit("/", 1)[-1]
    return req[-60:]


def key_prefix(key, depth=3):
    k = key.split("?")[0]
    parts = [p for p in k.split("/") if p]
    if len(parts) <= 1:
        return k[:60]
    return "/".join(parts[:depth]) + ("/" if len(parts) > depth else "")


with report("ops_5026_name_the_writer") as R:
    fails = []
    ev = {"as_of": NOW.isoformat(timespec="seconds"), "op": "ops_5026"}

    # ------------------------------------------------------------ P0
    R.section("P0 did the Aug-28 state hold")
    try:
        rc = lam.get_function_concurrency(FunctionName=FN_KILLED)
        n = rc.get("ReservedConcurrentExecutions")
        R.log("  %s reserved concurrency = %s  %s" % (
            FN_KILLED, n, "HELD" if n == 0 else "*** NOT 0 ***"))
        ev["killed_concurrency"] = n
    except Exception as e:
        R.log("  concurrency read err %s" % str(e)[:100])
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": FN_KILLED}],
            StartTime=NOW - timedelta(hours=30), EndTime=NOW,
            Period=3600, Statistics=["Sum"])
        pts = sorted((p["Timestamp"], int(p["Sum"]))
                     for p in r.get("Datapoints", []))
        R.log("  reader invocations/h (last 30h): %s" % " ".join(
            "%s=%d" % (t.strftime("%d/%H"), v) for t, v in pts[-14:]))
        ev["reader_hourly"] = [(t.isoformat(), v) for t, v in pts]
    except Exception as e:
        R.log("  reader metric err %s" % str(e)[:90])
    try:
        s3.get_bucket_replication(Bucket=LIVE)
        R.log("  replication: STILL CONFIGURED (!)")
        ev["replication"] = "present"
    except Exception:
        R.log("  replication: absent (4988 held)")
        ev["replication"] = "absent"
    try:
        lg = (s3.get_bucket_logging(Bucket=LIVE) or {}).get("LoggingEnabled")
        R.log("  access logging: %s" % (json.dumps(lg, default=str)[:140]
                                        if lg else "OFF"))
    except Exception as e:
        R.log("  logging read err %s" % str(e)[:80])

    # ------------------------------------------------------------ P1
    R.section("P1 current burn (Cost Explorer, Aug-24 -> today)")
    try:
        r = ce.get_cost_and_usage(
            TimePeriod={"Start": "2026-08-24",
                        "End": (TODAY + timedelta(days=1)).isoformat()},
            Granularity="DAILY", Metrics=["UnblendedCost", "UsageQuantity"],
            Filter={"Dimensions": {"Key": "SERVICE", "Values": [S3SVC]}},
            GroupBy=[{"Type": "DIMENSION", "Key": "OPERATION"},
                     {"Type": "DIMENSION", "Key": "REGION"}])
        daily = defaultdict(lambda: defaultdict(float))
        cost = defaultdict(lambda: defaultdict(float))
        for res in r.get("ResultsByTime", []):
            d = res["TimePeriod"]["Start"]
            for g in res.get("Groups", []):
                k = "%s@%s" % (g["Keys"][0], g["Keys"][1])
                daily[k][d] += float(
                    g["Metrics"]["UsageQuantity"]["Amount"])
                cost[k][d] += float(g["Metrics"]["UnblendedCost"]["Amount"])
        days = sorted({d for v in daily.values() for d in v})
        watch = [k for k in daily if k.split("@")[0] in
                 ("PutObject", "GetObject", "ListBucket", "PutObjectForRepl")]
        for k in sorted(watch):
            R.log("  %-30s %s" % (k[:30], " ".join(
                "%s=%.0fk" % (d[5:], daily[k].get(d, 0) / 1e3)
                for d in days)))
        put = daily.get("PutObject@us-east-1", {})
        ev["putobject_daily"] = {d: int(put.get(d, 0)) for d in days}
        full = [d for d in days if d < TODAY.isoformat()]
        latest_full = full[-1] if full else None
        R.log("  PutObject us-east-1 on last COMPLETE day %s: %.0f "
              "($%.2f)" % (latest_full, put.get(latest_full, 0),
                           cost["PutObject@us-east-1"].get(latest_full, 0)))
        ev["still_writing"] = bool(latest_full and
                                   put.get(latest_full, 0) > 100000)
    except Exception as e:
        R.log("  CE err %s" % str(e)[:160])
        ev["still_writing"] = None

    # ------------------------------------------------------------ P2
    R.section("P2 ATTRIBUTION from S3 server access logs")
    keys = []
    for back in (0, 1):
        d = TODAY - timedelta(days=back)
        pfx = "live/%s/%s/%s/%04d/%02d/%02d/" % (
            ACCT, REGION, LIVE, d.year, d.month, d.day)
        try:
            pag = s3.get_paginator("list_objects_v2")
            got = 0
            for pg in pag.paginate(Bucket=LOGS, Prefix=pfx):
                for o in pg.get("Contents", []):
                    keys.append((o["LastModified"], o["Key"], o["Size"]))
                    got += 1
                if got > 40000:
                    break
            R.log("  %s -> %d log objects" % (pfx, got))
        except Exception as e:
            R.log("  list %s err %s" % (pfx, str(e)[:100]))
    if not keys:
        R.log("  NO ACCESS LOGS FOUND -- cannot attribute")
        fails.append("P2:nologs")
    keys.sort()
    cutoff = NOW - timedelta(hours=26)
    keys = [k for k in keys if k[0] >= cutoff]
    R.log("  log objects in window: %d (%.1f MB)" % (
        len(keys), sum(k[2] for k in keys) / 1e6))
    step = max(1, len(keys) // MAX_LOG_FILES)
    sample = keys[::step][:MAX_LOG_FILES]
    R.log("  parsing an evenly spread sample of %d files (every %dth)"
          % (len(sample), step))

    op_req = defaultdict(int)
    put_by_req = defaultdict(int)
    put_bytes = defaultdict(int)
    put_prefix = defaultdict(int)
    put_req_prefix = defaultdict(int)
    get_by_req = defaultdict(int)
    list_by_req = defaultdict(int)
    ua_by_req = defaultdict(set)
    lines = 0
    bad = 0
    for _, k, _sz in sample:
        if lines >= MAX_LINES:
            break
        try:
            body = s3.get_object(Bucket=LOGS, Key=k)["Body"].read()
        except Exception:
            continue
        for raw in body.decode("utf-8", "replace").splitlines():
            lines += 1
            if lines >= MAX_LINES:
                break
            m = LOG_RE.match(raw)
            if not m:
                bad += 1
                continue
            requester = short_requester(m.group(5))
            operation = m.group(7)
            key = m.group(8)
            osize = m.group(13)
            ua = m.group(17)[:60]
            op_req[(operation, requester)] += 1
            if operation.startswith("REST.PUT.") or \
                    operation.startswith("REST.POST.") or \
                    operation.startswith("BATCH.DELETE"):
                put_by_req[requester] += 1
                ua_by_req[requester].add(ua)
                try:
                    put_bytes[requester] += int(osize)
                except ValueError:
                    pass
                p = key_prefix(key)
                put_prefix[p] += 1
                put_req_prefix[(requester, p)] += 1
            elif operation.startswith("REST.GET.OBJECT"):
                get_by_req[requester] += 1
            elif operation.startswith("REST.GET.BUCKET"):
                list_by_req[requester] += 1
    R.log("  parsed %d lines (%d unparsed) from %d files"
          % (lines, bad, len(sample)))
    if lines and not put_by_req:
        R.log("  no write operations in sample (!)")

    total_put = sum(put_by_req.values())
    R.log("  --- WRITE operations by requester (sample %d) ---" % total_put)
    for req, n in sorted(put_by_req.items(), key=lambda kv: -kv[1])[:15]:
        share = n / max(1, total_put)
        R.log("  %-46s %8d  %5.1f%%  avg=%.0fB  ua=%s" % (
            req[:46], n, 100 * share,
            put_bytes[req] / max(1, n), list(ua_by_req[req])[:1]))
    R.log("  --- WRITE operations by key prefix ---")
    for p, n in sorted(put_prefix.items(), key=lambda kv: -kv[1])[:15]:
        R.log("  %-60s %8d  %5.1f%%" % (p[:60], n,
                                        100 * n / max(1, total_put)))
    R.log("  --- writer x prefix (top pairs) ---")
    for (req, p), n in sorted(put_req_prefix.items(),
                              key=lambda kv: -kv[1])[:12]:
        R.log("  %-34s -> %-40s %7d" % (req[:34], p[:40], n))
    R.log("  --- READ by requester (top) ---")
    for req, n in sorted(get_by_req.items(), key=lambda kv: -kv[1])[:8]:
        R.log("  %-46s %8d" % (req[:46], n))
    R.log("  --- LIST by requester (top) ---")
    for req, n in sorted(list_by_req.items(), key=lambda kv: -kv[1])[:8]:
        R.log("  %-46s %8d" % (req[:46], n))

    # extrapolate the sample share onto the real Cost Explorer count
    real = 0
    try:
        pd = ev.get("putobject_daily") or {}
        full = [d for d in sorted(pd) if d < TODAY.isoformat()]
        real = pd.get(full[-1], 0) if full else 0
    except Exception:
        pass
    if total_put and real:
        R.log("  --- extrapolated to %d real PutObject/day ---" % real)
        for req, n in sorted(put_by_req.items(), key=lambda kv: -kv[1])[:6]:
            R.log("  %-46s ~%9.0f writes/day" % (
                req[:46], real * n / total_put))
    ev["put_by_requester_sample"] = dict(
        sorted(put_by_req.items(), key=lambda kv: -kv[1])[:20])
    ev["put_by_prefix_sample"] = dict(
        sorted(put_prefix.items(), key=lambda kv: -kv[1])[:20])
    ev["sample_lines"] = lines

    # ------------------------------------------------------------ P3
    R.section("P3 S3 Inventory -- where the 9.7M objects actually live")
    inv_pfx = "inventory/%s/daily-current/" % LIVE
    manifest = None
    try:
        pag = s3.get_paginator("list_objects_v2")
        mans = []
        for pg in pag.paginate(Bucket=LOGS, Prefix=inv_pfx):
            for o in pg.get("Contents", []):
                if o["Key"].endswith("manifest.json"):
                    mans.append((o["LastModified"], o["Key"]))
        if mans:
            mans.sort()
            manifest = mans[-1][1]
            R.log("  newest manifest: %s (%s)" % (manifest, mans[-1][0]))
        else:
            R.log("  no inventory manifest yet (first delivery <48h of "
                  "2026-08-28 12:04) -- prefix census deferred")
    except Exception as e:
        R.log("  inventory list err %s" % str(e)[:120])
    if manifest:
        try:
            man = json.loads(s3.get_object(Bucket=LOGS,
                                           Key=manifest)["Body"].read())
            files = man.get("files", [])
            schema = [c.strip() for c in man.get("fileSchema", "").split(",")]
            R.log("  inventory parts: %d  schema: %s" % (len(files), schema))
            ki = schema.index("Key") if "Key" in schema else 1
            si = schema.index("Size") if "Size" in schema else None
            cnt = defaultdict(int)
            byts = defaultdict(int)
            rows = 0
            for f in files:
                if rows >= MAX_INV_ROWS:
                    break
                b = s3.get_object(Bucket=LOGS, Key=f["key"])["Body"].read()
                txt = gzip.decompress(b).decode("utf-8", "replace")
                for row in csv.reader(io.StringIO(txt)):
                    rows += 1
                    if rows >= MAX_INV_ROWS:
                        break
                    if len(row) <= ki:
                        continue
                    p = key_prefix(row[ki])
                    cnt[p] += 1
                    if si is not None and len(row) > si:
                        try:
                            byts[p] += int(row[si] or 0)
                        except ValueError:
                            pass
            R.log("  read %d inventory rows; top prefixes by OBJECT COUNT:"
                  % rows)
            for p, n in sorted(cnt.items(), key=lambda kv: -kv[1])[:20]:
                R.log("  %-60s %9d  %8.1f GB" % (p[:60], n,
                                                 byts[p] / 1e9))
            ev["inventory_top_prefixes"] = dict(
                sorted(cnt.items(), key=lambda kv: -kv[1])[:25])
            ev["inventory_rows_read"] = rows
        except Exception as e:
            R.log("  inventory parse err %s" % str(e)[:160])

    # ------------------------------------------------------------ P4
    R.section("P4 growth + verdict")
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/S3", MetricName="NumberOfObjects",
            Dimensions=[{"Name": "BucketName", "Value": LIVE},
                        {"Name": "StorageType", "Value": "AllStorageTypes"}],
            StartTime=NOW - timedelta(days=12), EndTime=NOW,
            Period=86400, Statistics=["Average"])
        pts = sorted((p["Timestamp"].date().isoformat(), p["Average"])
                     for p in r.get("Datapoints", []))
        R.log("  objects: " + " ".join("%s=%.2fM" % (d[5:], n / 1e6)
                                       for d, n in pts))
        if len(pts) >= 2:
            R.log("  last-day delta: %+.0f objects" % (pts[-1][1] -
                                                       pts[-2][1]))
        ev["objects"] = [(d, int(n)) for d, n in pts]
    except Exception as e:
        R.log("  object metric err %s" % str(e)[:90])

    top_writer = None
    if put_by_req:
        top_writer, tw_n = max(put_by_req.items(), key=lambda kv: kv[1])
        share = 100.0 * tw_n / max(1, total_put)
        ev["top_writer"] = top_writer
        ev["top_writer_share_pct"] = round(share, 1)
        prefs = sorted(((n, p) for (rq, p), n in put_req_prefix.items()
                        if rq == top_writer), reverse=True)[:5]
        ev["top_writer_prefixes"] = [p for _, p in prefs]
        R.log("  WRITER NAMED: %s  (%.1f%% of all writes) into %s"
              % (top_writer, share, [p for _, p in prefs]))
    else:
        R.log("  writer NOT named -- sample contained no writes")
        fails.append("P4:unnamed")

    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/s3-writer-attribution.json",
                      Body=json.dumps(ev, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  evidence -> data/ops/s3-writer-attribution.json")
    except Exception as e:
        R.log("  evidence write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5026 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(top_writer=top_writer or "-",
         share_pct=ev.get("top_writer_share_pct", 0),
         still_writing=ev.get("still_writing"),
         sample_lines=lines)
    R.log("ops 5026 GREEN -- writer named from access logs, not inferred")
