"""ops/4824 -- justhodl-foreign-flows birth verify (TIC/CSLT).
 G0  the research doc's series ids are POST-CUTOFF claims, so every
     one of the six is LIVE-verified against FRED first (title +
     units + n_obs>=100 + first<=2000) using a donor FRED_KEY; any
     miss hard-stops BEFORE the engine is trusted (probe-then-wire
     applies to research docs).
 (1) function Active + FRED_KEY heal (donors dollar-strength-agent,
     justhodl-risk-gate); marker settle 'foreign-flows v1.0.0'.
 (2) Scheduler justhodl-foreign-flows-daily cron(30 21 * * ? *) --
     after the 4pm ET TIC release window.
 (3) Event-invoke; poll data/foreign-flows.json fresh <=5 min.
 (4) truths: LIVE 6/6; sampled series latest value+month ==
     independent in-op FRED refetch w/ unit conversion; all series
     share latest_month; signal latests == component sums (doc
     formulas); official_private deferred-not-guessed; permanent
     banks exist under data/providers/tic-cslt (Deny-Delete zone)
     with deep history; z in [-4,4]; size sane.
 (5) readout: six flows + three signals + latest month + release
     context (today IS a 4pm ET TIC release day).
"""
import gzip
import json
import sys
import time
import urllib.request
import io
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
FN = "justhodl-foreign-flows"
B = "justhodl-dashboard-live"
OUT_KEY = "data/foreign-flows.json"
BANK_FMT = "data/providers/tic-cslt/%s.json"
MARKER = "foreign-flows v1.0.0"
SCHED_NAME = "justhodl-foreign-flows-daily"
SCHED_CRON = "cron(30 21 * * ? *)"
SCHED_ROLE = f"arn:aws:iam::{ACCOUNT}:role/justhodl-scheduler-role"
DONORS = ("dollar-strength-agent", "justhodl-risk-gate")
SERIES = {"total": "FORLTTOTALNET99996",
          "treas": "FORTREASNET69995",
          "equity": "FORLTEQTYNET69995",
          "corp": "FORLTCORPNET99996",
          "agency": "FORLTAGCYNET99996",
          "tbills": "FORSTTREASNET99996"}

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


def donor_key(rep):
    for d in DONORS:
        try:
            env = (lam.get_function_configuration(FunctionName=d)
                   .get("Environment") or {}).get("Variables", {})
            if env.get("FRED_KEY"):
                rep.kv(fred_key_donor=d)
                return env["FRED_KEY"]
        except ClientError:
            continue
    rep.fail("no donor carries FRED_KEY")
    FAILED.append("fred_key")
    return None


def fred(path, sid, key, extra=""):
    url = ("https://api.stlouisfed.org/fred/%s?series_id=%s"
           "&api_key=%s&file_type=json%s" % (path, sid, key, extra))
    with urllib.request.urlopen(
            urllib.request.Request(url), timeout=60) as r:
        return json.loads(r.read())


def heal_key(rep, key):
    cfg = lam.get_function_configuration(FunctionName=FN)
    env = (cfg.get("Environment") or {}).get("Variables", {})
    if env.get("FRED_KEY"):
        rep.kv(env_FRED="present")
        return
    env["FRED_KEY"] = key
    lam.update_function_configuration(FunctionName=FN,
                                      Environment={"Variables": env})
    for _ in range(20):
        if lam.get_function_configuration(FunctionName=FN).get(
                "LastUpdateStatus") == "Successful":
            break
        time.sleep(3)
    rep.kv(env_FRED="HEALED")


def wait_active(rep):
    for _ in range(40):
        try:
            cfg = lam.get_function_configuration(FunctionName=FN)
        except ClientError:
            time.sleep(6)
            continue
        if (cfg.get("State") == "Active"
                and cfg.get("LastUpdateStatus") != "InProgress"):
            rep.kv(state="Active", mem=cfg.get("MemorySize"))
            return True
        time.sleep(6)
    rep.fail("never Active")
    FAILED.append("active")
    return False


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
    rep.fail("zip never carried %s" % MARKER)
    FAILED.append("settle")
    return False


def ensure_schedule(rep):
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FN}"
    target = {"Arn": fn_arn, "RoleArn": SCHED_ROLE, "Input": "{}",
              "RetryPolicy": {"MaximumRetryAttempts": 2,
                              "MaximumEventAgeInSeconds": 3600}}
    try:
        ex = sched.get_schedule(Name=SCHED_NAME)
        if (ex.get("State") == "ENABLED"
                and ex.get("ScheduleExpression") == SCHED_CRON):
            rep.ok(f"schedule {SCHED_NAME} already correct")
            return
        sched.update_schedule(Name=SCHED_NAME,
                              ScheduleExpression=SCHED_CRON,
                              ScheduleExpressionTimezone="UTC",
                              FlexibleTimeWindow={"Mode": "OFF"},
                              State="ENABLED", Target=target)
        rep.ok(f"schedule updated -> {SCHED_CRON}")
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    try:
        sched.create_schedule(Name=SCHED_NAME,
                              ScheduleExpression=SCHED_CRON,
                              ScheduleExpressionTimezone="UTC",
                              FlexibleTimeWindow={"Mode": "OFF"},
                              State="ENABLED", Target=target,
                              Description="justhodl-foreign-flows "
                              "daily TIC/CSLT (ops 4824)")
        rep.ok(f"schedule created -> {SCHED_CRON}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConflictException":
            rep.ok("schedule create raced -- ok")
        else:
            rep.fail(f"schedule: {e}")
            FAILED.append("schedule")


def main():
    with report("ops 4824 -- foreign-flows birth verify "
                "(TIC/CSLT)") as rep:
        rep.heading("G0. LIVE-verify research-doc series ids on "
                    "FRED")
        key = donor_key(rep)
        if not key:
            sys.exit(1)
        for name, sid in SERIES.items():
            try:
                meta = (fred("series", sid, key).get("seriess")
                        or [{}])[0]
                obs = fred("series/observations", sid, key,
                           "&observation_start=1985-01-01"
                           ).get("observations") or []
                title = meta.get("title") or ""
                first = obs[0]["date"] if obs else "9999"
                ok = (len(obs) >= 100 and first <= "2000-01-01"
                      and "foreign" in title.lower())
            except Exception as e:  # noqa: BLE001
                ok, title, first, obs = False, str(e)[:60], "?", []
            if ok:
                rep.ok("  G0 %-7s %-20s n=%d first=%s '%s'"
                       % (name, sid, len(obs), first, title[:58]))
            else:
                rep.fail("  G0 %-7s %-20s BROKEN (n=%d first=%s "
                         "%s)" % (name, sid, len(obs), first,
                                  title[:50]))
                FAILED.append("g0_" + sid)
        if FAILED:
            rep.fail("G0 broken -- the doc's ids do not survive "
                     "the live probe; fix before trusting")
            sys.exit(1)

        rep.heading("1. function + key + settle")
        if not wait_active(rep):
            sys.exit(1)
        heal_key(rep, key)
        if FAILED or not settle(rep):
            sys.exit(1)

        rep.heading("2. daily schedule (21:30 UTC)")
        ensure_schedule(rep)

        rep.heading("3. Event-invoke + poll (<=5 min)")
        try:
            prev = sread(OUT_KEY).get("generated_at")
        except ClientError:
            prev = None
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 300:
            time.sleep(12)
            try:
                d = sread(OUT_KEY)
            except ClientError:
                continue
            if d.get("generated_at") != prev:
                doc = d
                break
        if not doc:
            rep.fail("no fresh doc within 5 min")
            sys.exit(1)
        rep.ok("fresh doc in %ds runtime_ms=%s"
               % (int(time.time() - t0),
                  (doc.get("diag") or {}).get("runtime_ms")))

        rep.heading("4. truths")
        fb = doc.get("flows_bn") or {}
        if doc.get("status") == "LIVE" and len(fb) == 6:
            rep.ok("  LIVE v%s 6/6 latest_month=%s"
                   % (doc.get("v"), doc.get("latest_month")))
        else:
            rep.fail("  status=%s n=%d excluded=%s"
                     % (doc.get("status"), len(fb),
                        doc.get("excluded")))
            FAILED.append("live")
        for name in ("treas", "equity"):
            sid = SERIES[name]
            obs = fred("series/observations", sid, key,
                       "&observation_start=1985-01-01"
                       )["observations"]
            last = next((o for o in reversed(obs)
                         if o["value"] not in (".", "")), None)
            u = (fb.get(name) or {}).get("units_src", "")
            div = 1.0 if "billion" in u.lower() else 1000.0
            iv = round(float(last["value"]) / div, 1)
            if (fb.get(name, {}).get("latest") == iv
                    and fb[name].get("latest_month")
                    == last["date"]):
                rep.ok("  %s latest == independent FRED refetch "
                       "(%+.1fB @ %s)" % (name, iv, last["date"]))
            else:
                rep.fail("  %s diverges: eng=%s@%s ind=%s@%s"
                         % (name, fb.get(name, {}).get("latest"),
                            fb.get(name, {}).get("latest_month"),
                            iv, last["date"]))
                FAILED.append("ind_" + name)
        months = {v.get("latest_month") for v in fb.values()}
        if len(months) == 1:
            rep.ok("  all six series share latest_month")
        else:
            rep.warn("  mixed latest months: %s" % sorted(months))
        sig = doc.get("signals") or {}
        combos = {"risk_appetite": ("equity", "corp", "agency"),
                  "total_demand": ("treas", "agency", "corp",
                                   "equity")}
        for s, parts in combos.items():
            exp = round(sum(fb[p]["latest"] for p in parts), 1)
            got = (sig.get(s) or {}).get("latest_bn")
            if got is not None and abs(got - exp) <= 0.35:
                rep.ok("  %s latest == component sum (%+.1fB)"
                       % (s, got))
            else:
                rep.fail("  %s diverges: %s vs %s" % (s, got, exp))
                FAILED.append("sig_" + s)
        sh = (sig.get("safe_haven") or {}).get("latest_bn")
        exp = round(fb["treas"]["latest"] - fb["equity"]["latest"],
                    1)
        if sh is not None and abs(sh - exp) <= 0.35:
            rep.ok("  safe_haven latest == treas-equity (%+.1fB)"
                   % sh)
        else:
            rep.fail("  safe_haven diverges: %s vs %s" % (sh, exp))
            FAILED.append("sig_sh")
        if (sig.get("official_private") or {}).get("value") is None \
                and "DEFERRED" in str(
                    (sig.get("official_private") or {}).get("why")):
            rep.ok("  official_private deferred-not-guessed")
        else:
            rep.fail("  official_private not honestly deferred")
            FAILED.append("op_defer")
        zbad = [n for n, v in fb.items()
                if v.get("z_10y") is not None
                and not -4 <= v["z_10y"] <= 4]
        if not zbad:
            rep.ok("  all z_10y within [-4,4]")
        else:
            rep.fail("  z out of range: %s" % zbad)
            FAILED.append("z")
        for name, sid in SERIES.items():
            try:
                bank = sread(BANK_FMT % sid)
                rows = bank.get("rows") or {}
                first = min(rows) if rows else "9999"
                if len(rows) >= 100 and first <= "2000-01-01":
                    rep.ok("  bank %-20s n=%d first=%s "
                           "(Deny-Delete zone)" % (sid, len(rows),
                                                   first))
                else:
                    rep.fail("  bank %s thin n=%d first=%s"
                             % (sid, len(rows), first))
                    FAILED.append("bank_" + sid)
            except ClientError:
                rep.fail("  bank %s MISSING" % sid)
                FAILED.append("bank_" + sid)
        size = s3.head_object(Bucket=B, Key=OUT_KEY)["ContentLength"]
        rep.kv(out_kb=round(size / 1024, 1),
               new_release=doc.get("new_release"))

        rep.heading("5. readout")
        for n in ("total", "treas", "equity", "corp", "agency",
                  "tbills"):
            v = fb.get(n) or {}
            rep.log("  %-7s %+8.1fB  3m %+8.1f  12m %+8.1f  "
                    "z=%s  since %s"
                    % (n, v.get("latest", 0), v.get("sum_3m", 0),
                       v.get("sum_12m", 0), v.get("z_10y"),
                       v.get("first")))
        for s in ("risk_appetite", "safe_haven", "total_demand"):
            v = sig.get(s) or {}
            rep.log("  SIGNAL %-13s %+8.1fB  12m %+9.1f  z=%s"
                    % (s, v.get("latest_bn", 0),
                       v.get("sum_12m_bn", 0), v.get("z_10y")))
        rep.log("  NOTE today 4pm ET IS a TIC release (end-June "
                "data); the 21:30 UTC daily run flips new_release "
                "when FRED ingests it")

        rep.heading("6. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("justhodl-foreign-flows LIVE -- TIC/CSLT foreign "
               "capital flows banked permanently and signaled; "
               "dollar-view-first doctrine gains its missing organ")


if __name__ == "__main__":
    main()
