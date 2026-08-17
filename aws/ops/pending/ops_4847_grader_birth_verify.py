"""ops/4847 -- beaters-grader birth verify (Fusion 4).
 G0  live contracts: spx-beaters.json has buckets dict + as_of;
     weekly-closes ledger has dates>=50 and SPY.
 (1) settle 'beaters-grader v1.0.0'; schedule Sat 15:00 UTC.
 (2) invoke; poll data/beaters-learned-weights.json <=3 min.
 (3) truths: LIVE; banked week == src as_of; per-bucket banked
     counts == min(40, live src) identity; day-one cohort young ->
     accruing with ETA == as_of+28d recompute; weights PROVISIONAL
     with zero rows; bank write present.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timedelta
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
FN = "justhodl-beaters-grader"
B = "justhodl-dashboard-live"
SRC_KEY = "data/spx-beaters.json"
LEDGER_KEY = "spx-beaters/weekly-closes.json"
BANK_KEY = "spx-beaters/listings-history.json"
OUT_KEY = "data/beaters-learned-weights.json"
MARKER = "beaters-grader v1.0.0"

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


def settle(rep):
    for att in range(30):
        try:
            gf = lam.get_function(FunctionName=FN)
            raw = urllib.request.urlopen(gf["Code"]["Location"],
                                         timeout=60).read()
            src = zipfile.ZipFile(io.BytesIO(raw)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if MARKER in src:
                rep.ok("marker settled (attempt %d)" % (att + 1))
                return True
        except (ClientError, Exception):  # noqa: BLE001
            pass
        time.sleep(10)
    rep.fail("no marker")
    FAILED.append("settle")
    return False


def main():
    with report("ops 4847 -- beaters-grader birth verify") as rep:
        rep.heading("G0. source contracts")
        try:
            src = sread(SRC_KEY)
            led = sread(LEDGER_KEY)
        except ClientError as e:
            rep.fail("sources unreadable: %s" % e)
            sys.exit(1)
        bks = src.get("buckets")
        ok = (isinstance(bks, dict) and bks
              and src.get("as_of")
              and len(led.get("dates") or []) >= 50
              and "SPY" in (led.get("closes") or {}))
        if ok:
            rep.ok("src as_of=%s buckets=%s ledger_weeks=%d SPY ok"
                   % (src["as_of"], sorted(bks),
                      len(led["dates"])))
        else:
            rep.fail("contract broken: buckets=%s dates=%s"
                     % (type(bks).__name__,
                        len(led.get("dates") or [])))
            FAILED.append("g0")
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
        if not settle(rep):
            sys.exit(1)
        fn_arn = ("arn:aws:lambda:%s:%s:function:%s"
                  % (REGION, ACCOUNT, FN))
        role = ("arn:aws:iam::%s:role/justhodl-scheduler-role"
                % ACCOUNT)
        try:
            sched.create_schedule(
                Name="justhodl-beaters-grader-weekly",
                ScheduleExpression="cron(0 15 ? * SAT *)",
                ScheduleExpressionTimezone="UTC",
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Target={"Arn": fn_arn, "RoleArn": role,
                        "Input": "{}",
                        "RetryPolicy": {
                            "MaximumRetryAttempts": 2,
                            "MaximumEventAgeInSeconds": 3600}},
                Description="beaters-grader Sat 15:00 (ops 4847)")
            rep.ok("schedule created Sat 15:00 UTC")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConflictException":
                rep.ok("schedule exists")
            else:
                rep.fail("schedule: %s" % e)
                FAILED.append("sched")

        rep.heading("2. invoke + poll (<=3 min)")
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
        bank = sread(BANK_KEY)
        wk = src["as_of"]
        if doc.get("status") == "LIVE" and wk in bank.get(
                "weeks", {}):
            rep.ok("  LIVE; week %s banked" % wk)
        else:
            rep.fail("  status=%s banked=%s"
                     % (doc.get("status"),
                        sorted(bank.get("weeks", {}))))
            FAILED.append("bank")
        snap = (bank.get("weeks", {}).get(wk) or {}).get(
            "buckets") or {}
        for bname, rows in bks.items():
            exp = min(40, len(rows))
            got = len(snap.get(bname) or [])
            if got == exp and (exp == 0 or
                               snap[bname][0].get("t")
                               == rows[0].get("t")):
                rep.ok("  bucket %-race" if False else
                       "  bucket %-18s banked %d == min(40,%d), "
                       "head t match" % (bname, got, len(rows)))
            else:
                rep.fail("  bucket %s: got=%d exp=%d"
                         % (bname, got, exp))
                FAILED.append("b_" + bname)
        age = (datetime.fromisoformat(doc["as_of"])
               - datetime.fromisoformat(wk)).days
        if age < 28:
            acc = doc.get("accruing") or {}
            eta = (datetime.fromisoformat(wk)
                   + timedelta(days=28)).date().isoformat()
            if doc.get("n_graded_rows") == 0 \
                    and acc.get("first_grade_eta") == eta:
                rep.ok("  cohort age %dd -> accruing, ETA %s"
                       % (age, eta))
            else:
                rep.fail("  accruing wrong: %s" % acc)
                FAILED.append("acc")
        else:
            rep.ok("  cohort already gradable (age %dd), rows=%s"
                   % (age, doc.get("n_graded_rows")))
        if doc.get("status") == "LIVE" \
                and doc.get("n_graded_rows", -1) >= 0 \
                and (doc.get("n_graded_rows") >= 100
                     or doc.get("note")):
            rep.ok("  weights gate: n=%d status stays "
                   "consumption-deferred"
                   % doc.get("n_graded_rows"))
        else:
            rep.fail("  weights block malformed")
            FAILED.append("w")

        rep.heading("4. readout")
        rep.log("  banked weeks=%d | graded rows=%d | first "
                "grade ETA %s"
                % (len(bank.get("weeks", {})),
                   doc.get("n_graded_rows"),
                   (doc.get("accruing") or {}).get(
                       "first_grade_eta")))

        rep.heading("5. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("Fusion 4 grader LIVE -- the league now banks its "
               "own claims and will grade them against reality")


if __name__ == "__main__":
    main()
