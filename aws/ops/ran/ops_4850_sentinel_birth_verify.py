"""ops/4850 -- provider-window-sentinel birth verify.
 G0  >=6 banks under data/providers/tic-cslt/ + FRED_KEY donor.
 (1) Active + key heal + settle 'provider-window-sentinel v1.0.0';
     schedule Sun 09:00 UTC.
 (2) Event-invoke; poll data/provider-window-sentinel.json <=3min.
 (3) truths: LIVE; n_series >= 6; day-one verdicts all OK (REVISED
     tolerated with warn -- TIC revises; WINDOWED/UNVERIFIED =
     hard fail); bank counts echoed; sentinel wrote ONLY its own
     keys (banks untouched: sampled bank LastModified unchanged
     across the run).
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
FN = "justhodl-provider-window-sentinel"
B = "justhodl-dashboard-live"
OUT_KEY = "data/provider-window-sentinel.json"
PREFIX = "data/providers/tic-cslt/"
MARKER = "provider-window-sentinel v1.0.0"
DONORS = ("dollar-strength-agent", "justhodl-risk-gate")

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
    with report("ops 4850 -- window sentinel birth verify") as rep:
        rep.heading("G0. banks + donor key")
        keys = [o["Key"] for o in
                (s3.list_objects_v2(Bucket=B, Prefix=PREFIX)
                 .get("Contents") or [])]
        if len(keys) >= 6:
            rep.ok("banks: %d under %s" % (len(keys), PREFIX))
        else:
            rep.fail("only %d banks" % len(keys))
            sys.exit(1)
        key = None
        for d in DONORS:
            try:
                env = (lam.get_function_configuration(
                    FunctionName=d).get("Environment")
                    or {}).get("Variables", {})
                if env.get("FRED_KEY"):
                    key = env["FRED_KEY"]
                    rep.kv(donor=d)
                    break
            except ClientError:
                continue
        if not key:
            rep.fail("no FRED_KEY donor")
            sys.exit(1)
        probe_bank = keys[0]
        lm_before = s3.head_object(Bucket=B, Key=probe_bank
                                   )["LastModified"]

        rep.heading("1. active + key + settle + schedule")
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
        cfg = lam.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables", {})
        if not env.get("FRED_KEY"):
            env["FRED_KEY"] = key
            lam.update_function_configuration(
                FunctionName=FN,
                Environment={"Variables": env})
            for _ in range(20):
                if lam.get_function_configuration(
                        FunctionName=FN).get(
                        "LastUpdateStatus") == "Successful":
                    break
                time.sleep(3)
            rep.kv(env_FRED="HEALED")
        else:
            rep.kv(env_FRED="present")
        ok = False
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
                    ok = True
                    break
            except (ClientError, Exception):  # noqa: BLE001
                pass
            time.sleep(10)
        if not ok:
            rep.fail("no marker")
            sys.exit(1)
        fn_arn = ("arn:aws:lambda:%s:%s:function:%s"
                  % (REGION, ACCOUNT, FN))
        role = ("arn:aws:iam::%s:role/justhodl-scheduler-role"
                % ACCOUNT)
        try:
            sched.create_schedule(
                Name="justhodl-provider-window-sentinel-weekly",
                ScheduleExpression="cron(0 9 ? * SUN *)",
                ScheduleExpressionTimezone="UTC",
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Target={"Arn": fn_arn, "RoleArn": role,
                        "Input": "{}",
                        "RetryPolicy": {
                            "MaximumRetryAttempts": 2,
                            "MaximumEventAgeInSeconds": 3600}},
                Description="window sentinel Sun 09:00 "
                "(ops 4850)")
            rep.ok("schedule Sun 09:00 UTC")
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
        ser = doc.get("series") or {}
        if doc.get("status") == "LIVE" and len(ser) >= 6:
            rep.ok("  LIVE; %d series checked" % len(ser))
        else:
            rep.fail("  status=%s n=%d" % (doc.get("status"),
                                           len(ser)))
            FAILED.append("live")
        for sid, r in sorted(ser.items()):
            v = r.get("verdict")
            if v == "OK":
                rep.ok("  %-22s OK (bank=%s provider=%s)"
                       % (sid, r.get("bank_n"),
                          r.get("provider_n")))
            elif v == "REVISED":
                rep.warn("  %-22s REVISED n=%d (TIC revises; "
                         "informational)"
                         % (sid, r.get("n_revised")))
            else:
                rep.fail("  %-22s %s %s"
                         % (sid, v, r.get("why", "")))
                FAILED.append("v_" + sid)
        lm_after = s3.head_object(Bucket=B, Key=probe_bank
                                  )["LastModified"]
        if lm_before == lm_after:
            rep.ok("  banks untouched (sampled LastModified "
                   "unchanged) -- sentinel is read-only")
        else:
            rep.fail("  bank was MODIFIED by the sentinel run")
            FAILED.append("ro")
        if "alert" not in doc:
            rep.ok("  no windowing alert day-one (expected)")
        else:
            rep.warn("  ALERT on day one: %s" % doc["alert"])

        rep.heading("4. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("window sentinel LIVE -- the banks now have a "
               "watchman")


if __name__ == "__main__":
    main()
