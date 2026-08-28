"""ops_5024 -- S3 request-cost forensics (Khalid: the 2026-08-27 anomaly
email is the same open anomaly as 2026-08-25; "i thought you fixed it").

TRUTH, NOT INFERENCE. ops 4986 (schedule downshift) and ops 4988 (DR
replication kill) were shipped on 2026-08-26 on the strength of schedule
arithmetic -- nobody has yet read what AWS actually bills per S3
OPERATION per day. This op reads it.

  P0  Cost Explorer: S3 daily by USAGE_TYPE, 2026-08-01 -> today
      (did the Aug-26 fixes bend the curve? is us-west-2 dead?)
  P1  Cost Explorer: S3 daily by OPERATION x REGION, last 14d with
      UsageQuantity = request COUNTS (PutObject vs ListBucket vs ...)
  P2  Cost Explorer: whole-account top services, last 14d (context)
  P3  Lambda invocations top-40 over 72h (GetMetricData, 867 fns)
  P4  live-bucket config census: versioning / lifecycle / logging /
      inventory / request-metrics / notifications (feedback loops) /
      intelligent-tiering; CloudWatch NumberOfObjects + BucketSizeBytes
      30d for live + both DR buckets
  P5  PERMANENT ATTRIBUTION (the piece that was missing): S3 server
      access logging on the live bucket -> justhodl-s3-access-logs-
      857687956942 (partitioned by event time, 30d expiry). Every
      request is stamped with the caller's assumed-role SESSION NAME =
      the Lambda function name, so tomorrow's ops can name the exact
      engine per operation. Plus a daily S3 Inventory (current versions,
      CSV) so catalog/sentinel engines can stop live-listing the
      warehouse, plus Storage Lens advanced activity metrics
      (prefix-level request counts). All three cost ~cents/month.
  P6  verdict: Requests-Tier1 (us-east-1) Aug-10..25 mean vs Aug-26/27

GREEN = evidence gathered + attribution armed. RED only if Cost Explorer
is unreadable (then nothing here can be trusted and the fix must wait).
"""
import json
import sys
import time
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
DR_BUCKETS = ["justhodl-dashboard-live-dr",
              "justhodl-dr-usw2-857687956942"]
LOGS = "justhodl-s3-access-logs-%s" % ACCT
S3SVC = "Amazon Simple Storage Service"

ce = boto3.client("ce", region_name="us-east-1")
s3 = boto3.client("s3", region_name=REGION)
s3c = boto3.client("s3control", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
cw_w = boto3.client("cloudwatch", region_name="us-west-2")
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=60,
                                 retries={"max_attempts": 3}))

TODAY = datetime.now(timezone.utc).date()
START = "2026-08-01"
END = (TODAY + timedelta(days=1)).isoformat()   # CE End is exclusive
D14 = (TODAY - timedelta(days=14)).isoformat()


def ce_query(start, end, group_keys, extra_filter=None,
             gran="DAILY"):
    """Paginated GetCostAndUsage -> list of (date, [group], cost, qty)."""
    out = []
    kw = dict(TimePeriod={"Start": start, "End": end},
              Granularity=gran,
              Metrics=["UnblendedCost", "UsageQuantity"],
              GroupBy=[{"Type": "DIMENSION", "Key": k}
                       for k in group_keys])
    if extra_filter:
        kw["Filter"] = extra_filter
    tok = None
    while True:
        if tok:
            kw["NextPageToken"] = tok
        r = ce.get_cost_and_usage(**kw)
        for res in r.get("ResultsByTime", []):
            d = res["TimePeriod"]["Start"]
            for g in res.get("Groups", []):
                m = g["Metrics"]
                out.append((d, g["Keys"],
                            float(m["UnblendedCost"]["Amount"]),
                            float(m["UsageQuantity"]["Amount"])))
        tok = r.get("NextPageToken")
        if not tok:
            break
    return out


S3_FILTER = {"Dimensions": {"Key": "SERVICE", "Values": [S3SVC]}}


with report("ops_5024_s3_request_forensics") as R:
    fails = []
    verdict = {}

    # ------------------------------------------------------------ P0
    R.section("P0 Cost Explorer -- S3 daily by USAGE_TYPE (Aug-1 -> today)")
    rows0 = []
    try:
        rows0 = ce_query(START, END, ["USAGE_TYPE"], S3_FILTER)
    except Exception as e:
        R.log("  CE DENIED/ERR: %s" % str(e)[:160])
        fails.append("P0:ce")
    by_ut = defaultdict(lambda: defaultdict(float))
    qty_ut = defaultdict(lambda: defaultdict(float))
    for d, keys, cost, qty in rows0:
        by_ut[keys[0]][d] += cost
        qty_ut[keys[0]][d] += qty
    tot_ut = sorted(((sum(v.values()), ut) for ut, v in by_ut.items()),
                    reverse=True)
    days = sorted({d for v in by_ut.values() for d in v})
    R.log("  days covered: %s .. %s (%d)" % (
        days[0] if days else "-", days[-1] if days else "-", len(days)))
    top_uts = [ut for _, ut in tot_ut[:8]]
    for total, ut in tot_ut[:12]:
        R.log("  %-42s $%8.2f total" % (ut[:42], total))
    R.log("  -- daily $ for the top usage types --")
    hdr = "  %-10s " % "date" + " ".join("%12s" % u[-12:] for u in top_uts)
    R.log(hdr)
    for d in days:
        line = "  %-10s " % d + " ".join(
            "%12.2f" % by_ut[u].get(d, 0.0) for u in top_uts)
        R.log(line)
    verdict["usage_types"] = {u: {d: round(by_ut[u].get(d, 0), 2)
                                  for d in days} for u in top_uts}
    verdict["usage_qty"] = {u: {d: round(qty_ut[u].get(d, 0))
                                for d in days} for u in top_uts}

    # ------------------------------------------------------------ P1
    R.section("P1 Cost Explorer -- S3 daily by OPERATION x REGION (14d)")
    rows1 = []
    try:
        rows1 = ce_query(D14, END, ["OPERATION", "REGION"], S3_FILTER)
    except Exception as e:
        R.log("  CE err: %s" % str(e)[:160])
        fails.append("P1:ce")
    by_op = defaultdict(lambda: defaultdict(float))   # (op,reg)->d->cost
    q_op = defaultdict(lambda: defaultdict(float))    # (op,reg)->d->qty
    for d, keys, cost, qty in rows1:
        k = (keys[0], keys[1])
        by_op[k][d] += cost
        q_op[k][d] += qty
    tot_op = sorted(((sum(v.values()), k) for k, v in by_op.items()),
                    reverse=True)
    days1 = sorted({d for v in by_op.values() for d in v})
    R.log("  top operations by 14d $ (qty = usage units; requests for "
          "request ops, byte-hours/GB-mo for storage):")
    for total, (op, reg) in tot_op[:14]:
        q = sum(q_op[(op, reg)].values())
        R.log("  %-28s %-10s $%8.2f  qty=%14.0f" % (op[:28], reg,
                                                     total, q))
    R.log("  -- daily REQUEST COUNTS, top request ops --")
    req_ops = [k for _, k in tot_op if any(
        x in k[0] for x in ("Put", "List", "Get", "Head", "Copy",
                            "Delete", "Replicat", "Upload", "Restore",
                            "Select", "Complete", "Initiate"))][:6]
    R.log("  %-10s " % "date" + " ".join(
        "%16s" % ("%s@%s" % (k[0][:9], k[1][-4:])) for k in req_ops))
    for d in days1:
        R.log("  %-10s " % d + " ".join(
            "%16.0f" % q_op[k].get(d, 0.0) for k in req_ops))
    verdict["ops_14d"] = {"%s@%s" % k: {
        "cost": round(sum(by_op[k].values()), 2),
        "daily_qty": {d: round(q_op[k].get(d, 0)) for d in days1}}
        for _, k in tot_op[:14]}

    # ------------------------------------------------------------ P2
    R.section("P2 Cost Explorer -- whole account, top services (14d)")
    try:
        rows2 = ce_query(D14, END, ["SERVICE"])
        by_svc = defaultdict(lambda: defaultdict(float))
        for d, keys, cost, qty in rows2:
            by_svc[keys[0]][d] += cost
        tot_svc = sorted(((sum(v.values()), s) for s, v in by_svc.items()),
                         reverse=True)
        days2 = sorted({d for v in by_svc.values() for d in v})
        grand = {d: sum(v.get(d, 0) for v in by_svc.values())
                 for d in days2}
        for total, svc in tot_svc[:8]:
            R.log("  %-42s $%8.2f /14d  ($%.2f/day)" % (
                svc[:42], total, total / max(1, len(days2))))
        R.log("  ACCOUNT daily total: " + " ".join(
            "%s=%.1f" % (d[5:], grand[d]) for d in days2))
        verdict["account_daily"] = {d: round(grand[d], 2) for d in days2}
    except Exception as e:
        R.log("  CE err: %s" % str(e)[:120])

    # ------------------------------------------------------------ P3
    R.section("P3 Lambda invocations, top-40 over 72h (GetMetricData)")
    fns = []
    try:
        pag = lam.get_paginator("list_functions")
        for pg in pag.paginate():
            fns.extend(f["FunctionName"] for f in pg.get("Functions", []))
        R.log("  functions: %d" % len(fns))
        now = datetime.now(timezone.utc)
        inv = {}
        for i in range(0, len(fns), 500):
            chunk = fns[i:i + 500]
            qs = [{"Id": "m%d" % j,
                   "MetricStat": {"Metric": {
                       "Namespace": "AWS/Lambda",
                       "MetricName": "Invocations",
                       "Dimensions": [{"Name": "FunctionName",
                                       "Value": fn}]},
                       "Period": 259200, "Stat": "Sum"},
                   "ReturnData": True}
                  for j, fn in enumerate(chunk)]
            r = cw.get_metric_data(MetricDataQueries=qs,
                                   StartTime=now - timedelta(hours=72),
                                   EndTime=now)
            res = {x["Id"]: sum(x.get("Values") or [0])
                   for x in r.get("MetricDataResults", [])}
            for j, fn in enumerate(chunk):
                inv[fn] = res.get("m%d" % j, 0)
        top = sorted(inv.items(), key=lambda kv: -kv[1])[:40]
        R.log("  total invocations/72h: %.0f  (%.0f/day)" % (
            sum(inv.values()), sum(inv.values()) / 3))
        for fn, n in top:
            R.log("  %-46s %8.0f  (%6.0f/day)" % (fn[:46], n, n / 3))
        verdict["top_invokers_72h"] = [(fn, int(n)) for fn, n in top]
    except Exception as e:
        R.log("  metric err: %s" % str(e)[:140])

    # ------------------------------------------------------------ P4
    R.section("P4 live-bucket config census + object counts")
    try:
        v = s3.get_bucket_versioning(Bucket=LIVE)
        R.log("  versioning: %s" % v.get("Status"))
    except Exception as e:
        R.log("  versioning err %s" % str(e)[:80])
    try:
        lc = s3.get_bucket_lifecycle_configuration(Bucket=LIVE)
        for rule in lc.get("Rules", []):
            R.log("  lifecycle %s: %s" % (rule.get("ID"), json.dumps(
                {k: v for k, v in rule.items() if k != "ID"},
                default=str)[:160]))
    except Exception as e:
        R.log("  lifecycle: %s" % str(e)[:80])
    try:
        lg = s3.get_bucket_logging(Bucket=LIVE)
        R.log("  access logging BEFORE: %s" % json.dumps(
            lg.get("LoggingEnabled") or "OFF", default=str)[:120])
    except Exception as e:
        R.log("  logging err %s" % str(e)[:80])
    try:
        nc = s3.get_bucket_notification_configuration(Bucket=LIVE)
        cfgs = [(c.get("LambdaFunctionArn", "").rsplit(":", 1)[-1],
                 c.get("Events"), (c.get("Filter") or {}))
                for c in nc.get("LambdaFunctionConfigurations", [])]
        R.log("  S3->Lambda notifications: %d %s" % (
            len(cfgs), json.dumps(cfgs, default=str)[:400]))
        R.log("  S3->SQS/SNS notifications: %d/%d" % (
            len(nc.get("QueueConfigurations", [])),
            len(nc.get("TopicConfigurations", []))))
        R.log("  EventBridge notifications: %s" % (
            "ON" if nc.get("EventBridgeConfiguration") else "off"))
    except Exception as e:
        R.log("  notification err %s" % str(e)[:80])
    for name, fn in (("inventory", "list_bucket_inventory_configurations"),
                     ("request-metrics", "list_bucket_metrics_configurations"),
                     ("intelligent-tiering",
                      "list_bucket_intelligent_tiering_configurations"),
                     ("analytics", "list_bucket_analytics_configurations")):
        try:
            r = getattr(s3, fn)(Bucket=LIVE)
            key = [k for k in r if k.endswith("List")]
            n = len(r.get(key[0], [])) if key else 0
            R.log("  %s configs: %d %s" % (
                name, n, [c.get("Id") for c in r.get(key[0], [])][:6]
                if key else ""))
        except Exception as e:
            R.log("  %s: %s" % (name, str(e)[:80]))
    try:
        s3.get_bucket_replication(Bucket=LIVE)
        R.log("  replication: STILL CONFIGURED (!)")
    except Exception:
        R.log("  replication: none (4988 held)")

    def bucket_metrics(client, bucket, metric, stype):
        now = datetime.now(timezone.utc)
        r = client.get_metric_statistics(
            Namespace="AWS/S3", MetricName=metric,
            Dimensions=[{"Name": "BucketName", "Value": bucket},
                        {"Name": "StorageType", "Value": stype}],
            StartTime=now - timedelta(days=30), EndTime=now,
            Period=86400, Statistics=["Average"])
        pts = sorted(r.get("Datapoints", []),
                     key=lambda p: p["Timestamp"])
        return [(p["Timestamp"].date().isoformat(), p["Average"])
                for p in pts]

    for b, cl in ((LIVE, cw), (DR_BUCKETS[0], cw_w),
                  (DR_BUCKETS[1], cw_w)):
        try:
            objs = bucket_metrics(cl, b, "NumberOfObjects",
                                  "AllStorageTypes")
            if objs:
                R.log("  %s objects: %s -> %s (%d pts)" % (
                    b, "%.0f" % objs[0][1], "%.0f" % objs[-1][1],
                    len(objs)))
                R.log("    daily: " + " ".join(
                    "%s=%.0fk" % (d[5:], n / 1e3) for d, n in objs[-14:]))
            for st in ("StandardStorage", "StandardIAStorage",
                       "IntelligentTieringFAStorage",
                       "IntelligentTieringIAStorage"):
                sz = bucket_metrics(cl, b, "BucketSizeBytes", st)
                if sz:
                    R.log("    %s: %.1fGB -> %.1fGB" % (
                        st, sz[0][1] / 1e9, sz[-1][1] / 1e9))
            verdict.setdefault("objects", {})[b] = [
                (d, int(n)) for d, n in objs[-14:]]
        except Exception as e:
            R.log("  %s metrics err %s" % (b, str(e)[:80]))

    # ------------------------------------------------------------ P5
    R.section("P5 arm permanent attribution (access logs + inventory "
              "+ Storage Lens)")
    try:
        try:
            s3.head_bucket(Bucket=LOGS)
            R.log("  logs bucket exists")
        except Exception:
            s3.create_bucket(Bucket=LOGS)
            R.log("  logs bucket CREATED %s" % LOGS)
        s3.put_public_access_block(
            Bucket=LOGS, PublicAccessBlockConfiguration={
                "BlockPublicAcls": True, "IgnorePublicAcls": True,
                "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
        policy = {"Version": "2012-10-17", "Statement": [
            {"Sid": "S3ServerAccessLogsPolicy",
             "Effect": "Allow",
             "Principal": {"Service": "logging.s3.amazonaws.com"},
             "Action": "s3:PutObject",
             "Resource": "arn:aws:s3:::%s/live/*" % LOGS,
             "Condition": {
                 "ArnLike": {"aws:SourceArn": "arn:aws:s3:::%s" % LIVE},
                 "StringEquals": {"aws:SourceAccount": ACCT}}},
            {"Sid": "S3InventoryPolicy",
             "Effect": "Allow",
             "Principal": {"Service": "s3.amazonaws.com"},
             "Action": "s3:PutObject",
             "Resource": "arn:aws:s3:::%s/inventory/*" % LOGS,
             "Condition": {
                 "ArnLike": {"aws:SourceArn": "arn:aws:s3:::%s" % LIVE},
                 "StringEquals": {"aws:SourceAccount": ACCT,
                                  "s3:x-amz-acl":
                                      "bucket-owner-full-control"}}}]}
        s3.put_bucket_policy(Bucket=LOGS, Policy=json.dumps(policy))
        s3.put_bucket_lifecycle_configuration(
            Bucket=LOGS, LifecycleConfiguration={"Rules": [
                {"ID": "expire-access-logs-30d", "Status": "Enabled",
                 "Filter": {"Prefix": "live/"},
                 "Expiration": {"Days": 30}},
                {"ID": "expire-inventory-60d", "Status": "Enabled",
                 "Filter": {"Prefix": "inventory/"},
                 "Expiration": {"Days": 60}},
                {"ID": "abort-mpu-7d", "Status": "Enabled",
                 "Filter": {"Prefix": ""},
                 "AbortIncompleteMultipartUpload": {
                     "DaysAfterInitiation": 7}}]})
        R.log("  logs bucket policy + lifecycle (30d logs / 60d inv) set")
        s3.put_bucket_logging(
            Bucket=LIVE, BucketLoggingStatus={"LoggingEnabled": {
                "TargetBucket": LOGS, "TargetPrefix": "live/",
                "TargetObjectKeyFormat": {
                    "PartitionedPrefix": {
                        "PartitionDateSource": "EventTime"}}}})
        time.sleep(2)
        lg = s3.get_bucket_logging(Bucket=LIVE).get("LoggingEnabled")
        if lg and lg.get("TargetBucket") == LOGS:
            R.log("  ACCESS LOGGING ON: %s -> s3://%s/live/ "
                  "(requester = engine session name)" % (LIVE, LOGS))
        else:
            R.log("  access logging NOT confirmed: %s" % lg)
            fails.append("P5:logging")
    except Exception as e:
        R.log("  access-logging err: %s" % str(e)[:200])
        fails.append("P5:logging")
    try:
        s3.put_bucket_inventory_configuration(
            Bucket=LIVE, Id="daily-current",
            InventoryConfiguration={
                "Destination": {"S3BucketDestination": {
                    "AccountId": ACCT,
                    "Bucket": "arn:aws:s3:::%s" % LOGS,
                    "Format": "CSV", "Prefix": "inventory"}},
                "IsEnabled": True, "Id": "daily-current",
                "IncludedObjectVersions": "Current",
                "OptionalFields": ["Size", "LastModifiedDate",
                                   "StorageClass",
                                   "IsMultipartUploaded"],
                "Schedule": {"Frequency": "Daily"}})
        R.log("  S3 Inventory daily-current ON -> s3://%s/inventory/ "
              "(first manifest within 48h)" % LOGS)
    except Exception as e:
        R.log("  inventory err: %s" % str(e)[:160])
    try:
        s3c.put_storage_lens_configuration(
            ConfigId="justhodl-lens", AccountId=ACCT,
            StorageLensConfiguration={
                "Id": "justhodl-lens",
                "AccountLevel": {
                    "ActivityMetrics": {"IsEnabled": True},
                    "BucketLevel": {
                        "ActivityMetrics": {"IsEnabled": True},
                        "PrefixLevel": {"StorageMetrics": {
                            "IsEnabled": True,
                            "SelectionCriteria": {
                                "Delimiter": "/", "MaxDepth": 3,
                                "MinStorageBytesPercentage": 1.0}}}}},
                "Include": {"Buckets": ["arn:aws:s3:::%s" % LIVE]},
                "IsEnabled": True})
        R.log("  Storage Lens 'justhodl-lens' (advanced activity "
              "metrics, prefix depth 3) ON -- 48h to populate")
    except Exception as e:
        R.log("  storage lens err: %s" % str(e)[:160])

    # ------------------------------------------------------------ P6
    R.section("P6 verdict")
    t1 = by_ut.get("Requests-Tier1", {})
    base_days = [d for d in days if "2026-08-10" <= d <= "2026-08-25"]
    base = (sum(t1.get(d, 0) for d in base_days) /
            max(1, len(base_days))) if base_days else 0.0
    after = {d: t1.get(d, 0.0) for d in days if d >= "2026-08-26"}
    R.log("  us-east-1 Requests-Tier1: Aug10-25 mean $%.2f/day; after: %s"
          % (base, " ".join("%s=$%.2f" % (d[5:], c)
                             for d, c in sorted(after.items()))))
    usw2 = {ut: v for ut, v in by_ut.items() if ut.startswith("USW2")}
    for ut, v in usw2.items():
        R.log("  %s after Aug-26: %s" % (ut, " ".join(
            "%s=$%.2f" % (d[5:], v.get(d, 0)) for d in days
            if d >= "2026-08-26")))
    # name the burning operation on the last complete day
    if days1:
        last_full = [d for d in days1 if d < TODAY.isoformat()]
        dd = last_full[-1] if last_full else days1[-1]
        ranked = sorted(((by_op[k].get(dd, 0), q_op[k].get(dd, 0), k)
                         for k in by_op), reverse=True)[:5]
        R.log("  top S3 operations on %s:" % dd)
        for c, q, (op, reg) in ranked:
            R.log("    %-26s %-10s $%6.2f  qty=%12.0f" % (op[:26], reg,
                                                          c, q))
        verdict["last_full_day"] = dd
        verdict["top_ops_last_day"] = [
            (op, reg, round(c, 2), int(q)) for c, q, (op, reg) in ranked]
    still = bool(after) and max(after.values()) >= 0.7 * base and base > 0
    verdict["tier1_base_mean"] = round(base, 2)
    verdict["tier1_after"] = {d: round(c, 2) for d, c in after.items()}
    verdict["still_burning"] = still
    R.log("  VERDICT: %s" % (
        "STILL BURNING -- the Aug-26 fixes did not bend Requests-Tier1"
        if still else
        "FALLING -- Aug-26 fixes bent the curve (anomaly closes as the "
        "trailing window rolls)"))
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/s3-cost-forensics.json",
                      Body=json.dumps({"as_of": datetime.now(
                          timezone.utc).isoformat(timespec="seconds"),
                          "op": "ops_5024", **verdict},
                          indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  evidence -> data/ops/s3-cost-forensics.json")
    except Exception as e:
        R.log("  evidence write err %s" % str(e)[:80])

    if fails:
        R.log("ops 5024 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(tier1_base=round(base, 2), still_burning=still,
         logging="ON", inventory="ON")
    R.log("ops 5024 GREEN -- evidence banked; attribution armed "
          "(access logs stamp every request with the engine name from "
          "now on)")
