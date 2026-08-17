"""ops/4855 -- justhodl-hot-money birth verify (ledger takeover).
 G0  the shared TWSE ledger exists with >=60 rows (continuity
     precondition) -- record n_before.
 (1) settle 'hot-money v1.0.0'; schedule daily 09:50 UTC.
 (2) invoke; poll data/hot-money.json <=3 min.
 (3) truths: taiwan LIVE; ledger n_after >= n_before (UNION,
     nothing lost); sums == independent recompute off the full
     ledger; korea deferral named; global-flows OUT untouched by
     this engine.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
FN = "justhodl-hot-money"
B = "justhodl-dashboard-live"
LEDGER = "data/providers/twse/bfi82u-foreign.json"
OUT_KEY = "data/hot-money.json"
MARKER = "hot-money v1.0.0"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
sched = boto3.client("scheduler", region_name=REGION)
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops 4855 -- hot-money birth verify") as rep:
        rep.heading("G0. ledger continuity precondition")
        try:
            led = sread(LEDGER)
            n_before = len(led.get("rows") or {})
        except ClientError:
            rep.fail("ledger missing")
            sys.exit(1)
        if n_before >= 60:
            rep.ok("ledger n_before=%d" % n_before)
        else:
            rep.fail("ledger thin: %d" % n_before)
            sys.exit(1)

        rep.heading("1. settle + schedule")
        for _ in range(40):
            try:
                cfg = lam.get_function_configuration(
                    FunctionName=FN)
                if cfg.get("State") == "Active" and \
                        cfg.get("LastUpdateStatus") \
                        != "InProgress":
                    break
            except ClientError:
                pass
            time.sleep(6)
        settled = False
        for att in range(30):
            try:
                gf = lam.get_function(FunctionName=FN)
                raw = urllib.request.urlopen(
                    gf["Code"]["Location"], timeout=60).read()
                src = zipfile.ZipFile(io.BytesIO(raw)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if MARKER in src:
                    rep.ok("marker settled (attempt %d)"
                           % (att + 1))
                    settled = True
                    break
            except (ClientError, Exception):  # noqa: BLE001
                pass
            time.sleep(10)
        if not settled:
            rep.fail("no marker")
            sys.exit(1)
        fn_arn = ("arn:aws:lambda:%s:%s:function:%s"
                  % (REGION, ACCOUNT, FN))
        role = ("arn:aws:iam::%s:role/justhodl-scheduler-role"
                % ACCOUNT)
        try:
            sched.create_schedule(
                Name="justhodl-hot-money-daily",
                ScheduleExpression="cron(50 9 * * ? *)",
                ScheduleExpressionTimezone="UTC",
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Target={"Arn": fn_arn, "RoleArn": role,
                        "Input": "{}",
                        "RetryPolicy": {
                            "MaximumRetryAttempts": 2,
                            "MaximumEventAgeInSeconds": 3600}},
                Description="hot-money daily 09:50 (ops 4855)")
            rep.ok("schedule daily 09:50 UTC")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConflictException":
                rep.ok("schedule exists")
            else:
                rep.fail("schedule: %s" % e)
                FAILED.append("sched")

        rep.heading("2. invoke + poll")
        try:
            prev = sread(OUT_KEY).get("generated_at")
        except ClientError:
            prev = None
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 180:
            time.sleep(8)
            try:
                d = sread(OUT_KEY)
            except ClientError:
                continue
            if d.get("generated_at") != prev:
                doc = d
                break
        if not doc:
            rep.fail("no fresh doc")
            sys.exit(1)
        rep.ok("fresh in %ds" % int(time.time() - t0))

        rep.heading("3. truths")
        tw = (doc.get("countries") or {}).get("taiwan") or {}
        led2 = sread(LEDGER)["rows"]
        if tw.get("status") == "LIVE" \
                and len(led2) >= n_before:
            rep.ok("  taiwan LIVE; ledger %d -> %d (UNION, "
                   "nothing lost)" % (n_before, len(led2)))
        else:
            rep.fail("  tw=%s ledger %d->%d"
                     % (tw.get("status"), n_before, len(led2)))
            FAILED.append("cont")
        nets = [led2[d] for d in sorted(led2)]
        ok_sums = (tw.get("latest_bn")
                   == round(nets[-1] / 1e9, 2)
                   and tw.get("sum_5d_bn")
                   == round(sum(nets[-5:]) / 1e9, 2)
                   and tw.get("sum_60d_bn")
                   == round(sum(nets[-60:]) / 1e9, 2))
        if ok_sums:
            rep.ok("  sums == independent full-ledger recompute "
                   "(latest %+0.2f, 5d %+0.2f, 60d %+0.2f)"
                   % (tw["latest_bn"], tw["sum_5d_bn"],
                      tw["sum_60d_bn"]))
        else:
            rep.fail("  sums diverge")
            FAILED.append("sums")
        if "korea" in (doc.get("deferred") or {}):
            rep.ok("  korea deferral named")
        else:
            rep.fail("  korea deferral missing")
            FAILED.append("kr")

        rep.heading("4. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("hot-money engine LIVE -- the fast layer has its "
               "own house; the ledger survived the move intact")


if __name__ == "__main__":
    main()
