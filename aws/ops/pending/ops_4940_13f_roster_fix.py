"""ops/4940 -- fix the roster. Three of eighteen funds were the WRONG FIRM.

ops 4939 asked EDGAR what each CIK actually is. The answers were worse
than "stale":

  0001061165  SEC says LONE PINE CAPITAL LLC   -- labelled BAUPOST
  0001061768  SEC says BAUPOST GROUP LLC/MA    -- labelled LONE_PINE
      => Baupost and Lone Pine were SWAPPED. Every "Baupost (Klarman)"
         card on the page was Lone Pine's book and vice versa. Klarman's
         NBIS/STX/HD "new buys" were never Klarman's.

  0001286922  SEC says ADTERACTIVE INC, files no 13F-HR
      => the 18th "missing" fund. Elliott re-registered:
         0001791786 Elliott Investment Management L.P., current 2026-06-30.

  0001582202  SEC says Swiss National Bank -- labelled "Duration Capital"
      => $191B across 2,301 mega-cap names was never a boutique. It is a
         central bank's US equity book. Real data, wrong name. Relabelled,
         not deleted -- SNB is genuinely worth tracking.

THE ROSTER WAS DUPLICATED IN THREE ENGINES (justhodl-sec-13f builds the
filings index, justhodl-13f-positions parses, justhodl-13f-clone-alpha
scores skill). Fixing one would have silently changed nothing, because
positions reads its universe from the sec-13f index. All three are
patched from the same evidence, and the harness asserts they agree.

Clone-alpha matters most here: its leaderboard has been attributing
SKILL to the wrong manager. "Baupost 51 MARKET-LIKE" and "Lone Pine 16
FAMOUS != SKILLED" are swapped.

DURABLE FIX: _verify_roster_labels() now reads data.sec.gov every run and
publishes roster_label_mismatch[]. This bug survived months because
nothing ever compared the label we print to the filer we read. Now
something does, and disagreement is published rather than rendered as
fact.

Order matters: sec-13f must rebuild the index on the NEW CIKs before
positions can parse them. This ops chains them.

Local harness 9/9 across all three engines.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION, B = "us-east-1", "justhodl-dashboard-live"
IDX, POS = "justhodl-sec-13f", "justhodl-13f-positions"
OUT_KEY = "data/13f-positions.json"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=900, connect_timeout=20,
                                 retries={"max_attempts": 0}))


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


with report("ops_4940_13f_roster_fix") as R:
    for eng, checks in ((POS, ('"0001791786"', '"0001061768"')),
                        (IDX, ('"0001791786"',)),
                        ("justhodl-13f-clone-alpha", ('"0001791786"',))):
        src = (ROOT / "aws" / "lambdas" / eng / "source"
               / "lambda_function.py").read_text()
        for c in checks:
            if c not in src:
                R.log("G0 FAIL %s missing %s" % (eng, c))
                sys.exit(1)
    R.log("G0 all three engines carry the corrected roster")

    # ---- 1) rebuild the filings index on the NEW CIKs first
    R.log("invoking %s (index must lead)" % IDX)
    r = lam.invoke(FunctionName=IDX, InvocationType="RequestResponse",
                   Payload=b"{}")
    R.log("index rc=%s" % r["StatusCode"])

    # ---- 2) then re-parse positions (async: past the sync ceiling)
    try:
        prev = s3.head_object(Bucket=B, Key=OUT_KEY)["LastModified"]
    except Exception:
        prev = None
    lam.invoke(FunctionName=POS, InvocationType="Event", Payload=b"{}")
    fresh, took = wait_fresh(OUT_KEY, prev)
    if not fresh:
        R.log("FAIL positions payload never refreshed (%.0fs)" % took)
        sys.exit(1)
    R.log("positions refreshed after %.0fs" % took)
    time.sleep(5)

    d = json.loads(s3.get_object(Bucket=B, Key=OUT_KEY)["Body"].read())
    by_fund = d.get("by_fund") or {}
    fails = []

    def gate(n, c, det=""):
        R.log(("PASS " if c else "FAIL ") + n + "  " + str(det))
        if not c:
            fails.append(n)

    # ---- G1 the label must now match the filer, measured not asserted
    mism = d.get("roster_label_mismatch")
    gate("G1 zero label-vs-filer mismatches", mism == [], mism)

    # ---- G2 Elliott must now parse; it was the silent 18th
    ell = [v for v in by_fund.values()
           if isinstance(v, dict) and v.get("fund_key") == "ELLIOTT"]
    gate("G2 ELLIOTT now parses (was the missing fund)",
         bool(ell) and bool(ell[0].get("period_of_report")),
         ell[0].get("period_of_report") if ell else "ABSENT")

    # ---- G3 roster balance improves: 18 = parsed + failed, parsed >= 18
    gate("G3a roster balances",
         d.get("funds_total") == (d.get("funds_parsed") or 0)
         + (d.get("funds_failed") or 0),
         "%s/%s/%s" % (d.get("funds_total"), d.get("funds_parsed"),
                       d.get("funds_failed")))
    gate("G3b parsed count improved to 18",
         (d.get("funds_parsed") or 0) >= 18, d.get("funds_parsed"))

    # ---- G4 Baupost/Lone Pine no longer identical or swapped
    bp = next((v for v in by_fund.values() if isinstance(v, dict)
               and v.get("fund_key") == "BAUPOST"), {})
    lp = next((v for v in by_fund.values() if isinstance(v, dict)
               and v.get("fund_key") == "LONE_PINE"), {})
    gate("G4 BAUPOST and LONE_PINE are distinct books",
         bool(bp) and bool(lp)
         and bp.get("cik") != lp.get("cik")
         and bp.get("accession") != lp.get("accession"),
         {"baupost_cik": bp.get("cik"), "lone_pine_cik": lp.get("cik")})

    # ---- G5 nothing from 4936-4938 regressed
    agg = d.get("aggregate_by_ticker") or {}
    tot = d.get("funds_total")
    gate("G5a holders <= roster",
         not [t for t, a in agg.items()
              if (a.get("n_funds_holding") or 0) > tot])
    gate("G5b collisions all named",
         isinstance(d.get("cusip_unadjudicated"), list),
         len(d.get("cusip_unadjudicated") or []))

    R.log("as_of=%s parsed=%s/%s" % (d.get("as_of_quarter"),
                                     d.get("funds_parsed"),
                                     d.get("funds_total")))
    if fails:
        R.log("ops 4940 RED: " + "; ".join(fails))
        sys.exit(1)
    R.log("ops 4940 GREEN -- 7/7 gates")
