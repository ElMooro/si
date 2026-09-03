"""ops_5165 -- two corrections surfaced by the ops-5164 report, plus landing math.

  1. justhodl-fundamental-census already had its designed cadence on
     EventBridge Scheduler: fundamental-census-sched cron(0 6 1,15 * ? *)
     (twice monthly, like fi-census / etf-census). ops 5071 misread its
     171h idle age as "dead" and hitched it to a 30-minute rule; ops 5164
     unhitched it but then added a DAILY schedule of its own. Remove that
     daily schedule -- the twice-monthly design stands.
  2. justhodl-ecb-deep is in refresh mode with 58/58 flows complete, yet
     its Scheduler schedule justhodl-ecb-deep-10min still fires every
     10 minutes (144 x 51s x 4096MB a day = ~$14/month to re-read state).
     ECB publishes daily; rate(6 hours) keeps freshness within the day.
  3. Read the live-bucket size again and list the ops-5164 purge rule so
     the storage drop can be tracked, then print the expected September
     landing from the last-12h numbers with the stood-down lines removed.

Both changes are one-line reversals (recorded in the same ledger key).
"""
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LEDGER_KEY = "data/ops/ops5164-cost-standdown.json"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=90)
sch = boto3.client("scheduler", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)
FAILS = []

with report("ops_5165_standdown_corrections") as R:
    R.heading("ops 5165 -- stand-down corrections + expected landing")
    try:
        ledger = json.loads(s3.get_object(Bucket=BUCKET, Key=LEDGER_KEY)["Body"].read())
    except Exception:
        ledger = {"ops": 5164, "corrections_5165": []}
    ledger.setdefault("corrections_5165", [])

    R.section("1. fundamental-census: drop the daily schedule ops 5164 added; twice-monthly design stands")
    try:
        d = sch.get_schedule(Name="justhodl-fundamental-census-daily", GroupName="default")
        sch.delete_schedule(Name="justhodl-fundamental-census-daily", GroupName="default")
        ledger["corrections_5165"].append({"deleted_schedule": "justhodl-fundamental-census-daily",
                                           "expr": d.get("ScheduleExpression")})
        R.ok("   deleted justhodl-fundamental-census-daily (%s)" % d.get("ScheduleExpression"))
    except sch.exceptions.ResourceNotFoundException:
        R.log("   justhodl-fundamental-census-daily not present")
    except Exception as e:
        FAILS.append("delete daily census schedule: %s" % str(e)[:120])
    try:
        d = sch.get_schedule(Name="fundamental-census-sched", GroupName="default")
        R.ok("   designed cadence intact: fundamental-census-sched %s %s" % (d.get("ScheduleExpression"), d.get("State")))
    except Exception as e:
        R.warn("   fundamental-census-sched read: %s" % str(e)[:100])

    R.section("2. ecb-deep: refresh mode (58/58 complete) -> rate(6 hours)")
    try:
        d = sch.get_schedule(Name="justhodl-ecb-deep-10min", GroupName="default")
        prev = d.get("ScheduleExpression")
        if prev != "rate(6 hours)":
            sch.update_schedule(Name="justhodl-ecb-deep-10min", GroupName="default",
                                ScheduleExpression="rate(6 hours)",
                                ScheduleExpressionTimezone=d.get("ScheduleExpressionTimezone", "UTC"),
                                FlexibleTimeWindow=d["FlexibleTimeWindow"], Target=d["Target"],
                                State=d.get("State", "ENABLED"),
                                Description="ecb-deep refresh (58/58 complete); rate(6 hours) since ops 5165, was %s" % prev)
            ledger["corrections_5165"].append({"updated_schedule": "justhodl-ecb-deep-10min",
                                               "from": prev, "to": "rate(6 hours)"})
            R.ok("   justhodl-ecb-deep-10min %s -> rate(6 hours)" % prev)
        else:
            R.ok("   already rate(6 hours)")
    except Exception as e:
        FAILS.append("ecb-deep schedule: %s" % str(e)[:120])

    R.section("3. Storage tracking + purge rule")
    try:
        res = cw.get_metric_statistics(
            Namespace="AWS/S3", MetricName="BucketSizeBytes",
            Dimensions=[{"Name": "BucketName", "Value": BUCKET}, {"Name": "StorageType", "Value": "StandardStorage"}],
            StartTime=NOW - timedelta(days=6), EndTime=NOW, Period=86400, Statistics=["Average"])
        for p in sorted(res.get("Datapoints", []), key=lambda p: p["Timestamp"]):
            R.log("   %s  %.0f GB" % (str(p["Timestamp"])[:10], p["Average"] / 1e9))
        rules = s3.get_bucket_lifecycle_configuration(Bucket=BUCKET).get("Rules", [])
        pr = [r_ for r_ in rules if r_.get("ID") == "ops5164-purge-dead-versions-data"]
        R.log("   purge rule present: %s -> %s" % (bool(pr), json.dumps(pr[0], default=str)[:200] if pr else "-"))
    except Exception as e:
        R.warn("   storage read: %s" % str(e)[:100])

    R.section("4. Expected September landing (from ops-5164 last-12h numbers, stood-down lines removed)")
    rows = [
        ("Lambda fleet, everything else (last 12h)", 10.03 - 2.43 - 3.90 - 0.48 - 0.40 - 0.09, "measured, minus the lines below"),
        ("justhodl-repo (daily now, was 288/day)", 0.05, "1 run x 337s x 1536MB"),
        ("justhodl-census-us econ (hourly, 11/12 shards COMPLETE)", 0.60, "288 shard runs x ~60s + 15-min heartbeat"),
        ("justhodl-ecb-deep refresh (6h, was 10 min)", 0.02, "4 runs x 51s x 4096MB"),
        ("justhodl-sdmx-walker (OECD temp schedules removed)", 0.05, "remaining rules only"),
        ("justhodl-fundamental-census (twice monthly)", 0.02, "design cadence"),
        ("S3 requests (repo PUT storm removed)", 0.80, "was $3.5-4.0/day Tier-1"),
        ("S3 storage after dead-version purge (~600 GB)", 0.45, "was $1.41/day at 1,954 GB"),
        ("S3 GET/HEAD + access logs + Storage Lens", 0.55, "edge reads"),
        ("DynamoDB, Secrets Manager, Route 53, CloudWatch, ECR/EBS", 0.45, "flat"),
        ("us-west-2 DR mirror (HOLD -- Khalid's call)", 1.15, "$34.56/month until deleted"),
        ("SnapStart cache on justhodl-ai-chat (HOLD)", 0.57, "$17/month"),
    ]
    tot = 0.0
    for name, usd, note in rows:
        tot += usd
        R.log("   %-58s $%5.2f/day   %s" % (name, usd, note))
        R.kv(section="landing", line=name, usd_day=round(usd, 2), usd_month=round(usd * 30, 2))
    R.log("   %-58s $%5.2f/day = $%.0f/month  (drop the two HOLD lines: $%.2f/day = $%.0f/month)"
          % ("TOTAL", tot, tot * 30, tot - 1.15 - 0.57, (tot - 1.15 - 0.57) * 30))
    R.log("   reference: Aug 01-08 baseline $4.52/day = $135/month; Sep 01-02 actual $20.72/day = $622/month")

    try:
        s3.put_object(Bucket=BUCKET, Key=LEDGER_KEY, Body=json.dumps(ledger, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.ok("   ledger updated")
    except Exception as e:
        FAILS.append("ledger: %s" % str(e)[:100])
    if FAILS:
        for f in FAILS:
            R.fail(f)
        sys.exit(1)
    R.ok("ops 5165 complete")
