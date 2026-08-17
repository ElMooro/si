"""ops/4818 -- justhodl-base-rates birth verify (Fusion 1 spine).
 G0  FIELD-level contracts (boom_score doctrine -- container length
     is insufficient): ledger dates>=40 AND closes.SPY has >=40
     numeric values; universe.stocks>=3000.
 (1) function Active; zip marker settle 'base-rates v1.0.0'.
 (2) EventBridge Scheduler justhodl-base-rates-weekly
     cron(30 14 ? * SAT *) UTC -- after the 13:00 league run.
 (3) snapshot ledger LastModified; Event-invoke; poll
     data/base-rates.json fresh generated_at <= 6 min.
 (4) truths: LIVE + v1.0.0; 26w quintiles == INDEPENDENT in-op
     recompute (verbatim v1.2 oracle math on the LIVE ledger);
     thresholds equal; ledger LastModified UNCHANGED (read-only
     proof); current_assignments>=3000; assignments.q == league
     mom_quintile on sampled tickers when ledger vintages match;
     4w/13w rolling cohorts present; history row appended; size sane.
 (5) full odds readout (quintiles x horizons, comeback, dd bands).
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
FN = "justhodl-base-rates"
B = "justhodl-dashboard-live"
LEDGER_KEY = "spx-beaters/weekly-closes.json"
OUT_KEY = "data/base-rates.json"
HIST_KEY = "base-rates/history.json"
LEAGUE_KEY = "data/spx-beaters.json"
MARKER = "base-rates v1.0.0"
SCHED_NAME = "justhodl-base-rates-weekly"
SCHED_CRON = "cron(30 14 ? * SAT *)"
SCHED_ROLE = f"arn:aws:iam::{ACCOUNT}:role/justhodl-scheduler-role"

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


def oracle_26w(led):
    """Verbatim spx-beaters v1.2 base_rates() math on the LIVE
    ledger -- the independent recompute the engine must equal."""
    spy = [v for v in (led["closes"].get("SPY") or []) if v]
    if len(spy) < 53:
        return None, None
    spy_out = spy[-1] / spy[-27] - 1
    rows = []
    for t in led["closes"]:
        if t == "SPY":
            continue
        arr = [v for v in (led["closes"].get(t) or []) if v]
        if len(arr) < 53:
            continue
        rows.append((arr[-27] / arr[-53] - 1,
                     (arr[-1] / arr[-27] - 1) - spy_out))
    if len(rows) < 200:
        return None, None
    rows.sort()
    n = len(rows)
    quints = []
    for q in range(5):
        seg = rows[int(n * q / 5):int(n * (q + 1) / 5)]
        ex = sorted(x[1] for x in seg)
        beat = sum(1 for x in seg if x[1] > 0)
        quints.append({"n": len(seg),
                       "beat": round(100 * beat / len(seg), 1),
                       "med": round(ex[len(ex) // 2] * 100, 1),
                       "fmax": round(seg[-1][0] * 100, 1)})
    th = [q["fmax"] / 100 for q in quints[:-1]]
    return quints, th


def wait_active(rep):
    for _ in range(40):
        try:
            cfg = lam.get_function_configuration(FunctionName=FN)
        except ClientError:
            time.sleep(6)
            continue
        if (cfg.get("State") == "Active"
                and cfg.get("LastUpdateStatus") != "InProgress"):
            rep.kv(state="Active", mem=cfg.get("MemorySize"),
                   timeout=cfg.get("Timeout"))
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
        rep.ok(f"schedule {SCHED_NAME} updated -> {SCHED_CRON}")
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
                              Description="justhodl-base-rates weekly"
                              " odds spine (ops 4818)")
        rep.ok(f"schedule {SCHED_NAME} created -> {SCHED_CRON}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConflictException":
            rep.ok("schedule create raced -- ConflictException = ok")
        else:
            rep.fail(f"schedule create: {e}")
            FAILED.append("schedule")


def main():
    with report("ops 4818 -- base-rates spine birth verify") as rep:
        rep.heading("G0. FIELD-level feed contracts")
        try:
            led = sread(LEDGER_KEY)
        except ClientError:
            rep.fail("G0 ledger MISSING")
            sys.exit(1)
        nd = len(led.get("dates") or [])
        spy_n = len([v for v in (led.get("closes", {}).get("SPY")
                                 or []) if isinstance(v, (int, float))])
        if nd >= 40 and spy_n >= 40:
            rep.ok("  G0 ledger dates=%d closes.SPY numeric=%d"
                   % (nd, spy_n))
        else:
            rep.fail("  G0 ledger thin: dates=%d SPY=%d" % (nd, spy_n))
            FAILED.append("g0_ledger")
        try:
            uni = sread("data/universe.json")
            ns = len(uni.get("stocks") or uni.get("rows") or [])
        except ClientError:
            ns = 0
        if ns >= 3000:
            rep.ok("  G0 universe.stocks = %d" % ns)
        else:
            rep.fail("  G0 universe thin: %d" % ns)
            FAILED.append("g0_universe")
        if FAILED:
            rep.fail("G0 broken -- fix before invoke")
            sys.exit(1)

        rep.heading("1. function + settle")
        if not wait_active(rep) or not settle(rep):
            sys.exit(1)

        rep.heading("2. weekly schedule (Sat 14:30 UTC, post-league)")
        ensure_schedule(rep)

        rep.heading("3. Event-invoke + poll (<=6 min)")
        led_lm0 = s3.head_object(Bucket=B, Key=LEDGER_KEY
                                 )["LastModified"]
        try:
            prev = sread(OUT_KEY).get("generated_at")
        except ClientError:
            prev = None
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 360:
            time.sleep(15)
            try:
                d = sread(OUT_KEY)
            except ClientError:
                continue
            if d.get("generated_at") != prev:
                doc = d
                break
        if not doc:
            rep.fail("no fresh %s within 6 min" % OUT_KEY)
            sys.exit(1)
        rep.ok("fresh doc in %ds  runtime_ms=%s"
               % (int(time.time() - t0),
                  (doc.get("diag") or {}).get("runtime_ms")))

        rep.heading("4. truths")
        if doc.get("status") == "LIVE" and doc.get("v") == "1.0.0":
            rep.ok("  status LIVE v1.0.0")
        else:
            rep.fail("  status=%s v=%s" % (doc.get("status"),
                                           doc.get("v")))
            FAILED.append("status")
        led2 = sread(LEDGER_KEY)
        led_lm1 = s3.head_object(Bucket=B, Key=LEDGER_KEY
                                 )["LastModified"]
        if led_lm1 == led_lm0:
            rep.ok("  ledger LastModified UNCHANGED (read-only "
                   "proven)")
        else:
            rep.fail("  ledger was MODIFIED: %s -> %s"
                     % (led_lm0, led_lm1))
            FAILED.append("ledger_write")
        oq, oth = oracle_26w(led2)
        s0 = ((doc.get("cohorts") or {}).get("26w")
              or {}).get("s0_quintiles") or []
        if oq and len(s0) == 5 and all(
                a["n"] == b["n"] and a["beat_pct"] == b["beat"]
                and a["median_excess_pp"] == b["med"]
                for a, b in zip(s0, oq)):
            rep.ok("  26w s0 quintiles == independent in-op oracle "
                   "recompute")
        else:
            rep.fail("  s0 quintiles diverge from oracle: mine=%s "
                     "oracle=%s"
                     % (json.dumps([(q.get("n"), q.get("beat_pct"))
                                    for q in s0]),
                        json.dumps([(q["n"], q["beat"])
                                    for q in (oq or [])])))
            FAILED.append("oracle")
        if oth and doc.get("quintile_thresholds_26w") == oth:
            rep.ok("  thresholds == oracle")
        else:
            rep.fail("  thresholds diverge")
            FAILED.append("thresholds")
        asg = doc.get("current_assignments") or {}
        if len(asg) >= 3000:
            rep.ok("  current_assignments = %d" % len(asg))
        else:
            rep.fail("  assignments thin: %d" % len(asg))
            FAILED.append("assignments")
        try:
            lg = sread(LEAGUE_KEY)
        except ClientError:
            lg = None
        lg_last = (((lg or {}).get("diag") or {}).get("ledger")
                   or {}).get("last")
        eng_last = (((doc.get("diag") or {}).get("feeds")
                     or {}).get("ledger_last"))
        if lg and lg_last == eng_last:
            n_chk = n_ok = 0
            for bname, rows in (lg.get("buckets") or {}).items():
                for r in rows or []:
                    t, mq = r.get("t"), r.get("mom_quintile")
                    if not t or not mq or t not in asg:
                        continue
                    n_chk += 1
                    if asg[t].get("q") == mq:
                        n_ok += 1
                    if n_chk >= 25:
                        break
                if n_chk >= 25:
                    break
            if n_chk and n_ok == n_chk:
                rep.ok("  cross-engine: %d/%d sampled q == league "
                       "mom_quintile" % (n_ok, n_chk))
            elif n_chk:
                rep.fail("  cross-engine divergence: %d/%d"
                         % (n_ok, n_chk))
                FAILED.append("league_q")
            else:
                rep.warn("  no sampled league rows carried "
                         "mom_quintile")
        else:
            rep.warn("  league ledger vintage %s != %s -- "
                     "consistency check skipped" % (lg_last, eng_last))
        for hname, floor in (("4w", 15), ("13w", 8)):
            cell = (doc["cohorts"].get(hname) or {})
            nf = cell.get("n_formations") or 0
            if nf >= floor and (cell.get("n_obs") or 0) > 0:
                rep.ok("  %s rolling cohort: n=%d forms=%d"
                       % (hname, cell["n_obs"], nf))
            else:
                rep.fail("  %s cohort thin: forms=%d" % (hname, nf))
                FAILED.append("cohort_" + hname)
        cb = doc.get("comeback_26w")
        if cb:
            rep.ok("  comeback n=%s beat=%s%%"
                   % (cb.get("n"), cb.get("beat_pct")))
        else:
            rep.warn("  comeback cohort below n floor this tape "
                     "(honest None)")
        try:
            hist = sread(HIST_KEY)
            if (hist.get("rows") or [])[-1].get("as_of") \
                    == doc.get("as_of"):
                rep.ok("  history row appended for %s"
                       % doc["as_of"])
            else:
                rep.warn("  history row not for today")
        except ClientError:
            rep.fail("  history.json missing")
            FAILED.append("history")
        size = s3.head_object(Bucket=B, Key=OUT_KEY)["ContentLength"]
        if size < 1_500_000:
            rep.ok("  output size %.0f KB" % (size / 1024))
        else:
            rep.fail("  output bloated: %d bytes" % size)
            FAILED.append("size")

        rep.heading("5. odds readout")
        for hname in ("4w", "13w", "26w"):
            cell = doc["cohorts"].get(hname) or {}
            rep.log("  -- %s (n=%s, forms=%s) --"
                    % (hname, cell.get("n_obs"),
                       cell.get("n_formations")))
            for q in cell.get("quintiles") or []:
                rep.log("   Q%d beat=%5.1f%%  medEx=%+6.1fpp  "
                        "LB95=%5.1f%%  n=%d"
                        % (q["q"], q["beat_pct"],
                           q["median_excess_pp"],
                           q["wilson_lb95_pct"], q["n"]))
        for band, st in sorted(((doc["cohorts"].get("26w") or {})
                                .get("dd_bands") or {}).items()):
            rep.log("   dd %-12s beat=%5.1f%%  medEx=%+6.1fpp  n=%d"
                    % (band, st["beat_pct"], st["median_excess_pp"],
                       st["n"]))
        for t in list(asg)[:3]:
            rep.log("   assign %-6s %s" % (t, json.dumps(asg[t])))

        rep.heading("6. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("justhodl-base-rates LIVE -- Fusion 1 spine: one "
               "ledger now disciplines every odds claim the fleet "
               "makes")


if __name__ == "__main__":
    main()
