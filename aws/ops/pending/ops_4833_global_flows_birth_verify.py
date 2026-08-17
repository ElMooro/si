"""ops/4833 -- justhodl-global-flows birth verify (Peru live,
world deferrals named).
 G0  BCRP combined call answers with 4 series >= 24 quarters
     (probe-4832 re-confirmed at birth).
 (1) Active; settle 'global-flows v1.0.0'.
 (2) Scheduler justhodl-global-flows-weekly cron(0 12 ? * MON *).
 (3) Event-invoke; poll data/global-flows.json <=4 min.
 (4) truths: LIVE, peru 4/4 with latest == independent in-op BCRP
     refetch on two sampled series; banks under data/providers/bcrp
     with n>=24; five deferrals verbatim; composites honestly null;
     size sane.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
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
FN = "justhodl-global-flows"
B = "justhodl-dashboard-live"
OUT_KEY = "data/global-flows.json"
MARKER = "global-flows v1.0.0"
SCHED_NAME = "justhodl-global-flows-weekly"
SCHED_CRON = "cron(0 12 ? * MON *)"
SCHED_ROLE = f"arn:aws:iam::{ACCOUNT}:role/justhodl-scheduler-role"
SIDS = {"portfolio_total": "PN39285BQ",
        "portfolio_equity": "PN39286BQ",
        "portfolio_fixed_income": "PN39287BQ",
        "gov_bonds_nonresident": "PN39414FQ"}

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


def bcrp():
    now = datetime.now(timezone.utc)
    endq = "%d-%d" % (now.year, (now.month - 1) // 3 + 1)
    url = ("https://estadisticas.bcrp.gob.pe/estadisticas/series/"
           "api/%s/json/2012-1/%s"
           % ("-".join(SIDS.values()), endq))
    req = urllib.request.Request(url, headers={"User-Agent":
                                               "ops-4833"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


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


def wait_active(rep):
    for _ in range(40):
        try:
            cfg = lam.get_function_configuration(FunctionName=FN)
        except ClientError:
            time.sleep(6)
            continue
        if (cfg.get("State") == "Active"
                and cfg.get("LastUpdateStatus") != "InProgress"):
            rep.kv(state="Active")
            return True
        time.sleep(6)
    rep.fail("never Active")
    FAILED.append("active")
    return False


def ensure_schedule(rep):
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FN}"
    target = {"Arn": fn_arn, "RoleArn": SCHED_ROLE, "Input": "{}",
              "RetryPolicy": {"MaximumRetryAttempts": 2,
                              "MaximumEventAgeInSeconds": 3600}}
    try:
        sched.create_schedule(Name=SCHED_NAME,
                              ScheduleExpression=SCHED_CRON,
                              ScheduleExpressionTimezone="UTC",
                              FlexibleTimeWindow={"Mode": "OFF"},
                              State="ENABLED", Target=target,
                              Description="justhodl-global-flows "
                              "weekly (ops 4833)")
        rep.ok("schedule created -> %s" % SCHED_CRON)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConflictException":
            rep.ok("schedule exists")
        else:
            rep.fail("schedule: %s" % e)
            FAILED.append("schedule")


def main():
    with report("ops 4833 -- global-flows birth verify") as rep:
        rep.heading("G0. BCRP re-confirm at birth")
        try:
            j = bcrp()
            per = j.get("periods") or []
            ns = len((j.get("config") or {}).get("series") or [])
            if ns == 4 and len(per) >= 24:
                rep.ok("BCRP 4 series, %d quarters, last %s"
                       % (len(per), per[-1].get("name")))
            else:
                rep.fail("BCRP thin: series=%d periods=%d"
                         % (ns, len(per)))
                FAILED.append("g0")
        except Exception as e:  # noqa: BLE001
            rep.fail("BCRP dead: %s" % str(e)[:90])
            FAILED.append("g0")
        if FAILED:
            sys.exit(1)

        rep.heading("1. function + settle + schedule")
        if not wait_active(rep) or not settle(rep):
            sys.exit(1)
        ensure_schedule(rep)

        rep.heading("2. Event-invoke + poll (<=4 min)")
        try:
            prev = sread(OUT_KEY).get("generated_at")
        except ClientError:
            prev = None
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 240:
            time.sleep(10)
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
        pe = (doc.get("countries") or {}).get("peru") or {}
        if doc.get("status") == "LIVE" and pe.get("status") \
                == "LIVE":
            rep.ok("  LIVE; peru %s" % pe.get("latest_period"))
        else:
            rep.fail("  status=%s peru=%s why=%s"
                     % (doc.get("status"), pe.get("status"),
                        pe.get("why") or doc.get("why")))
            FAILED.append("live")
        j = bcrp()
        order = list(SIDS)
        last = (j.get("periods") or [])[-1]
        vals = last.get("values") or []
        for name in ("portfolio_total", "gov_bonds_nonresident"):
            iv = round(float(vals[order.index(name)]), 1)
            s = (pe.get("series") or {}).get(name) or {}
            if s.get("latest") == iv and s.get("period") \
                    == last.get("name"):
                rep.ok("  %s == independent BCRP refetch "
                       "(%+.1fM @ %s)" % (name, iv,
                                          last.get("name")))
            else:
                rep.fail("  %s diverges: eng=%s@%s ind=%s@%s"
                         % (name, s.get("latest"),
                            s.get("period"), iv,
                            last.get("name")))
                FAILED.append("ind_" + name)
        for name, sid in SIDS.items():
            try:
                bank = sread("data/providers/bcrp/%s.json" % sid)
                n = len(bank.get("rows") or {})
                if n >= 24:
                    rep.ok("  bank %s n=%d (Deny-Delete zone)"
                           % (sid, n))
                else:
                    rep.fail("  bank %s thin n=%d" % (sid, n))
                    FAILED.append("bank")
            except ClientError:
                rep.fail("  bank %s missing" % sid)
                FAILED.append("bank")
        dep = doc.get("deferred") or {}
        if set(dep) == {"taiwan_cbc", "taiwan_twse_daily", "korea",
                        "chile", "imf_layer"} \
                and all(v.get("why") for v in dep.values()):
            rep.ok("  five deferrals named with unlock reasons")
        else:
            rep.fail("  deferrals wrong: %s" % sorted(dep))
            FAILED.append("deferred")
        comp = doc.get("composites") or {}
        if (comp.get("cfi") or {}).get("value") is None \
                and (comp.get("hot_money") or {}).get("value") \
                is None:
            rep.ok("  composites honestly null")
        else:
            rep.fail("  composites not honest")
            FAILED.append("comp")

        rep.heading("4. readout")
        for k, s in (pe.get("series") or {}).items():
            rep.log("  %-26s %+8.1fM  4Q %+9.1f  z=%s  since %s"
                    % (k, s.get("latest", 0), s.get("sum_4q", 0),
                       s.get("z_all"), s.get("first")))

        rep.heading("5. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("justhodl-global-flows LIVE -- Peru measured, the "
               "world map honest about what unlocks next")


if __name__ == "__main__":
    main()
