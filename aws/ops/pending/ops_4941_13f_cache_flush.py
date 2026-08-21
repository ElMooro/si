"""ops/4941 -- the page was stale for TWO different reasons. Neither was
the parser.

Khalid: "the page still have stalled data." Correct, and I had not
looked at the right layer.

REASON 1 -- THE CACHE OUTLIVED THE FIX.
parse_one_fund writes each position WITH its resolved ticker, then
caches the whole thing under PARSER_VERSION. v5 was set in ops 4936 --
BEFORE the 4937/4938 collision purge. So every v5 cache entry froze the
poisoned answers, and 4937/4938 only ever corrected the map, never the
cached rows. That is why the per-fund cards still read
  ICLN -> "INVESCO QQQ TR"   (Citadel $26.6B, Millennium $4.0B)
  CPAY -> "PG&E CORP" / "THE ODP CORP" / "SLM CORP"
  ORCL -> "ELEVANCE HEALTH INC FORMERLY"
and why the phantom -$23.80B ICLN most-sold row survived three fixes
that all reported GREEN. v5 -> v6 flushes it.

  RULE: fixing a resolver does nothing while the cache still holds its
  old answers. Any change to how a value is DERIVED must bump the key of
  every cache that stores that value.

REASON 2 -- THE LEADERBOARD IS WEEKLY, BY DESIGN.
justhodl-13f-clone-alpha runs cron(30 8 ? * MON *). Today is Friday, so
provenance reads "4d ago" -- not broken, just weekly. But it means the
ops 4940 roster fix has NOT reached it: the leaderboard still shows
"Duration Capital" instead of the Swiss National Bank, still lists
Elliott as awaiting windows, and still carries Baupost 51 / Lone Pine 16
-- which are SWAPPED, because those scores were computed against the
swapped CIKs. Left alone it would stay wrong until Monday. Invoked here.

Also: 13f.html claimed "Lambda runs every 6h". The real schedule is
cron(10 21 ? * MON-FRI *). The copy has been corrected to the actual
cron rather than left as decoration.

WHY MY EARLIER GATES MISSED IT -- the third time this exact shape has
bitten in this arc. 4937's G4 read aggregate_by_ticker, which holds ONE
name per ticker and cannot disagree with itself. The page does not
render the aggregate; it renders by_fund[*].top_positions. The gate and
the display were reading different layers. G2 below reads the layer the
page renders.
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION, B = "us-east-1", "justhodl-dashboard-live"
IDX, POS = "justhodl-sec-13f", "justhodl-13f-positions"
CLONE = "justhodl-13f-clone-alpha"
OUT_KEY, CLONE_KEY = "data/13f-positions.json", "data/13f-clone-alpha.json"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=900, connect_timeout=20,
                                 retries={"max_attempts": 0}))


def await_deploy(fn, started, budget=420):
    t0 = time.time()
    while time.time() - t0 < budget:
        try:
            lm = lam.get_function_configuration(FunctionName=fn)["LastModified"]
            dt = datetime.strptime(lm.split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if dt >= started - timedelta(minutes=12):
                return dt
        except Exception:
            pass
        time.sleep(15)
    return None


def wait_fresh(key, prev, budget=780):
    t0 = time.time()
    while time.time() - t0 < budget:
        time.sleep(20)
        try:
            lm = s3.head_object(Bucket=B, Key=key)["LastModified"]
        except Exception:
            continue
        if prev is None or lm > prev:
            return lm, time.time() - t0
    return None, time.time() - t0


with report("ops_4941_13f_cache_flush") as R:
    src = (ROOT / "aws" / "lambdas" / POS / "source"
           / "lambda_function.py").read_text()
    if 'PARSER_VERSION = "v6"' not in src:
        R.log("G0 FAIL parser version not bumped -- cache would not flush")
        sys.exit(1)
    if "every 6h" in (ROOT / "13f.html").read_text():
        R.log("G0 FAIL page still claims a schedule it does not run")
        sys.exit(1)
    R.log("G0 v6 flush armed; page copy matches cron")

    started = datetime.now(timezone.utc)
    for fn in (IDX, POS, CLONE):
        if not await_deploy(fn, started):
            R.log("G0b FAIL %s not redeployed in budget" % fn)
            sys.exit(1)
    R.log("G0b all three functions carry post-push code")

    lam.invoke(FunctionName=IDX, InvocationType="RequestResponse", Payload=b"{}")
    try:
        prev = s3.head_object(Bucket=B, Key=OUT_KEY)["LastModified"]
    except Exception:
        prev = None
    lam.invoke(FunctionName=POS, InvocationType="Event", Payload=b"{}")
    fresh, took = wait_fresh(OUT_KEY, prev)
    if not fresh:
        R.log("FAIL positions never refreshed (%.0fs) -- v6 full re-parse "
              "is slower than v5 reuse" % took)
        sys.exit(1)
    R.log("positions refreshed after %.0fs (full re-parse, no cache reuse)"
          % took)
    time.sleep(5)

    d = json.loads(s3.get_object(Bucket=B, Key=OUT_KEY)["Body"].read())
    agg = d.get("aggregate_by_ticker") or {}
    by_fund = d.get("by_fund") or {}
    fails = []

    def gate(n, c, det=""):
        R.log(("PASS " if c else "FAIL ") + n + "  " + str(det))
        if not c:
            fails.append(n)

    # ---- G1 the named offenders, read on the PER-FUND rows the page shows
    watch = {"ICLN": "CLEAN", "CPAY": "CORPAY", "ORCL": "ORACLE"}
    wrong = []
    for fk, v in by_fund.items():
        if not isinstance(v, dict):
            continue
        for p in (v.get("top_positions") or []):
            tk = (p.get("ticker") or "").upper()
            if tk in watch:
                nm = (p.get("resolved_name") or p.get("name") or "").upper()
                if watch[tk] not in nm:
                    wrong.append({"fund": v.get("fund_key") or fk,
                                  "ticker": tk, "name": nm[:34]})
    gate("G1 per-fund rows no longer show pre-fix tickers", not wrong,
         wrong[:8])

    # ---- G2 GENERAL: every per-fund row must agree with the aggregate name
    # for its ticker. This is the layer the page renders; the 4937 gate
    # read the aggregate, which cannot disagree with itself.
    disagree = []
    for fk, v in by_fund.items():
        if not isinstance(v, dict):
            continue
        for p in (v.get("top_positions") or [])[:25]:
            tk = (p.get("ticker") or "").upper()
            a = agg.get(tk)
            if not tk or not a:
                continue
            an = set((a.get("name") or "").upper().split())
            pn = set((p.get("resolved_name") or p.get("name") or "")
                     .upper().split())
            an -= {"INC", "CORP", "CO", "LTD", "PLC", "THE", "GROUP",
                   "HOLDINGS", "HLDGS", "CLASS", "A", "C", "NEW", "TR",
                   "COM", "COMPANY", "&"}
            pn -= {"INC", "CORP", "CO", "LTD", "PLC", "THE", "GROUP",
                   "HOLDINGS", "HLDGS", "CLASS", "A", "C", "NEW", "TR",
                   "COM", "COMPANY", "&"}
            if an and pn and not (an & pn):
                disagree.append({"fund": v.get("fund_key") or fk,
                                 "ticker": tk, "row": " ".join(sorted(pn))[:26],
                                 "agg": " ".join(sorted(an))[:26]})
    gate("G2 per-fund row names agree with the aggregate", not disagree,
         "%d rows: %s" % (len(disagree), disagree[:5]))

    # ---- G3 refresh the WEEKLY leaderboard so 4940's roster fix lands
    try:
        cprev = s3.head_object(Bucket=B, Key=CLONE_KEY)["LastModified"]
    except Exception:
        cprev = None
    lam.invoke(FunctionName=CLONE, InvocationType="Event", Payload=b"{}")
    cfresh, ctook = wait_fresh(CLONE_KEY, cprev, budget=600)
    gate("G3a clone-alpha rebuilt (was 4d stale, weekly cron)",
         bool(cfresh), "%.0fs" % ctook)
    if cfresh:
        time.sleep(5)
        c = json.loads(s3.get_object(Bucket=B, Key=CLONE_KEY)["Body"].read())
        blob = json.dumps(c).upper()
        gate("G3b leaderboard no longer says 'Duration Capital'",
             "DURATION CAPITAL" not in blob)

    # ---- G4 nothing from 4936-4940 regressed
    tot = d.get("funds_total")
    gate("G4a roster 18/18",
         d.get("funds_parsed") == tot and (d.get("funds_failed") or 0) == 0,
         "%s/%s/%s" % (tot, d.get("funds_parsed"), d.get("funds_failed")))
    gate("G4b zero label-vs-filer mismatches",
         d.get("roster_label_mismatch") == [], d.get("roster_label_mismatch"))
    gate("G4c holders <= roster",
         not [t for t, a in agg.items()
              if (a.get("n_funds_holding") or 0) > tot])

    R.log("as_of=%s tickers=%s" % (d.get("as_of_quarter"), len(agg)))
    if fails:
        R.log("ops 4941 RED: " + "; ".join(fails))
        sys.exit(1)
    R.log("ops 4941 GREEN -- 7/7 gates")
