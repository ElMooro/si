"""ops/4811 -- justhodl-spx-beaters birth verify (weekly SPX-beat league).
 G0  key contracts against LIVE artifacts (universe.stocks,
     stock-buying.top, asset-compass.assets, rotation.assets,
     industry-boom.league, 13f-flows.t) BEFORE trusting any join.
 (1) function Active + POLYGON_API_KEY heal (donors equity-research,
     spx-ma); zip marker settle 'spx-beaters v1.0.0'.
 (2) EventBridge Scheduler justhodl-spx-beaters-weekly
     cron(0 13 ? * SAT *) UTC.
 (3) Event-invoke; poll data/spx-beaters.json <= 13 min (first run
     bootstraps <=30 weekly Polygon grouped-daily fetches).
 (4) truths: ledger weeks >= 20 (partial OK day-one, note honest),
     scanned stocks >= 3000, 8 buckets, every listed row score>=55 &
     n_legs>=2 & why non-empty & never {mom,industry}-only; regime
     context populated; SPY 6m return present once weeks>=27.
 (5) full league readout (top rows per bucket).
"""
import gzip
import json
import io
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
FN = "justhodl-spx-beaters"
B = "justhodl-dashboard-live"
OUT_KEY = "data/spx-beaters.json"
MARKER = "spx-beaters v1.0.0"
SCHED_NAME = "justhodl-spx-beaters-weekly"
SCHED_CRON = "cron(0 13 ? * SAT *)"
SCHED_ROLE = f"arn:aws:iam::{ACCOUNT}:role/justhodl-scheduler-role"
DONORS = ("justhodl-equity-research", "justhodl-spx-ma")

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


def g0(rep, key, container, hard=True):
    try:
        doc = sread(key)
    except ClientError:
        (rep.fail if hard else rep.warn)("  G0 %s: MISSING" % key)
        if hard:
            FAILED.append("g0_" + key)
        return 0
    v = doc.get(container)
    n = len(v) if isinstance(v, (list, dict)) else 0
    if n:
        rep.ok("  G0 %s.%s = %d" % (key, container, n))
    else:
        (rep.fail if hard else rep.warn)(
            "  G0 %s.%s empty/absent (top keys: %s)"
            % (key, container, sorted(doc)[:8]))
        if hard:
            FAILED.append("g0_" + key)
    return n


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


def heal_polygon(rep):
    cfg = lam.get_function_configuration(FunctionName=FN)
    env = (cfg.get("Environment") or {}).get("Variables", {})
    if env.get("POLYGON_API_KEY"):
        rep.kv(env_POLYGON="present")
        return
    for d in DONORS:
        try:
            src = lam.get_function_configuration(FunctionName=d)
            k = (src.get("Environment") or {}).get(
                "Variables", {}).get("POLYGON_API_KEY")
            if k:
                env["POLYGON_API_KEY"] = k
                lam.update_function_configuration(
                    FunctionName=FN,
                    Environment={"Variables": env})
                for _ in range(20):
                    if lam.get_function_configuration(
                            FunctionName=FN).get(
                            "LastUpdateStatus") == "Successful":
                        break
                    time.sleep(3)
                rep.kv(env_POLYGON="HEALED from " + d)
                return
        except ClientError:
            continue
    rep.fail("POLYGON_API_KEY absent in all donors -- momentum "
             "ledger cannot build")
    FAILED.append("polygon")


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
                              Description="justhodl-spx-beaters "
                              "weekly all-cap league (ops 4811)")
        rep.ok(f"schedule {SCHED_NAME} created -> {SCHED_CRON}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConflictException":
            rep.ok("schedule create raced -- ConflictException = "
                   "success")
        else:
            rep.fail(f"schedule create: {e}")
            FAILED.append("schedule")


def main():
    with report("ops 4811 -- spx-beaters birth verify") as rep:
        rep.heading("G0. live feed contracts")
        g0(rep, "data/universe.json", "stocks")
        g0(rep, "data/asset-compass.json", "assets")
        g0(rep, "data/rotation-dashboard.json", "assets")
        g0(rep, "data/industry-boom.json", "league")
        g0(rep, "data/stock-buying.json", "top", hard=False)
        g0(rep, "data/13f-flows-by-ticker.json", "t", hard=False)
        g0(rep, "data/best-setups.json", "setups", hard=False)
        g0(rep, "data/invest.json", "stock_picks", hard=False)
        g0(rep, "data/sp500.json", "members", hard=False)
        if FAILED:
            rep.fail("G0 contract broken -- fix joins before invoke")
            sys.exit(1)

        rep.heading("1. function + env + settle")
        if not wait_active(rep):
            sys.exit(1)
        heal_polygon(rep)
        if FAILED or not settle(rep):
            sys.exit(1)

        rep.heading("2. weekly schedule")
        ensure_schedule(rep)

        rep.heading("3. Event-invoke + poll (<=13 min)")
        t0 = time.time()
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        while time.time() - t0 < 780:
            time.sleep(20)
            try:
                d = sread(OUT_KEY)
                if d.get("marker") == MARKER and d.get("as_of", "") \
                        > datetime_floor:
                    doc = d
                    break
            except ClientError:
                pass
        if not doc:
            rep.fail("data/spx-beaters.json never appeared fresh")
            sys.exit(1)
        rep.ok("  fresh doc after ~%ds" % int(time.time() - t0))

        rep.heading("4. league truths")
        led = doc.get("ledger") or {}
        wk = led.get("weeks") or 0
        (rep.ok if wk >= 20 else rep.fail)(
            "  ledger weeks = %d (target %s, fetched_now %s)"
            % (wk, led.get("target"), led.get("fetched_now")))
        if wk < 20:
            FAILED.append("ledger")
        sc = (doc.get("scanned") or {}).get("stocks") or 0
        (rep.ok if sc >= 3000 else rep.fail)(
            "  scanned stocks = %d" % sc)
        if sc < 3000:
            FAILED.append("scanned")
        bks = doc.get("buckets") or {}
        if len(bks) != 8:
            rep.fail("  buckets = %s" % sorted(bks))
            FAILED.append("buckets")
        else:
            rep.ok("  buckets = 8: " + json.dumps(doc.get("counts")))
        bad = 0
        listed = 0
        for b, rows in bks.items():
            for r in rows:
                listed += 1
                if (r.get("score", 0) < doc.get("min_score", 55)
                        or r.get("n_legs", 0) < 2
                        or not r.get("why")
                        or set(r.get("legs") or {}) ==
                        {"mom", "industry"}):
                    bad += 1
        (rep.ok if bad == 0 and listed > 0 else rep.fail)(
            "  listed rows = %d, contract violations = %d"
            % (listed, bad))
        if bad or not listed:
            FAILED.append("rows")
        reg = doc.get("regime") or {}
        rep.kv(regime=json.dumps({k: reg.get(k) for k in
                                  ("risk_gate_sizing",
                                   "rotation_regime",
                                   "spx_erp_ttm_pct",
                                   "spy_ret_6m_pct")}))
        if wk >= 27 and reg.get("spy_ret_6m_pct") is None:
            rep.fail("  SPY 6m return missing despite %d weeks" % wk)
            FAILED.append("spy6")
        ms = doc.get("mom_status") or {}
        rep.kv(mom_status=json.dumps(ms))

        rep.heading("5. league readout (top of each bucket)")
        for b in ("large", "mid", "small", "micro", "etf_equity",
                  "etf_bond", "etf_commodity", "etf_crypto_alt"):
            rows = bks.get(b) or []
            if not rows:
                rep.warn("  %-14s (none >= %s yet)"
                         % (b, doc.get("min_score")))
                continue
            r = rows[0]
            rep.ok("  %-14s %-6s %5.1f  legs=%s"
                   % (b, r["t"], r["score"],
                      json.dumps(r.get("legs"))))
            rep.log("      why: " + " | ".join(r.get("why")[:3]))

        rep.heading("6. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("justhodl-spx-beaters LIVE -- weekly all-cap + ETF + "
               "asset-class beat-the-SPX league")


datetime_floor = __import__("datetime").datetime.now(
    __import__("datetime").timezone.utc).isoformat()

if __name__ == "__main__":
    main()
