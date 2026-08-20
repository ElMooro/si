"""ops/4938 -- repair the regression ops 4937 caused, then re-accept.

4937 fixed the real thing: CPAY renders as CORPAY INC, ORCL as Oracle,
ICLN as iShares, and the phantom -$23.80B most-sold row is gone. It also
proved Hertz's $8.6B was bond principal all along -- it now sits in the
PRN bucket, which is why the NFE gate could never be satisfied the way I
wrote it.

But it BROKE more than it fixed, and the damage was mine:

  _purge_collisions decided the winner of a contested ticker with
  (src_rank, len(name)). Name length is not evidence. A longer WRONG
  name beat the correct short one, so the real cusips lost their own
  tickers: UBER, BRK-B, SE, MKSI, BILL, RIVN, BRKR all went unresolved
  and the page started printing raw cusips (90353T, 084670, 81141R,
  55306N, 090043, 76954A, 116794). The unresolved bucket went
  $4.70B -> $70.68B, 4.5% of the whole complex.

FIX: SEC company_tickers.json is authoritative in BOTH directions. The
winner of ticker T is now the claimant whose name best matches SEC's own
title for T (exact match, then token overlap, then src rank). Name length
survives only as a last-resort tiebreak when SEC cannot adjudicate.

Also: a purge that outruns re-resolution leaves real companies blank, so
the FMP budget goes 150 -> 600 and the OpenFIGI cap 400 -> 1200.

LESSON, third in this arc and the sharpest: 4936 shipped a gate that
could not fail; 4937 shipped a gate whose premise I had not verified;
4938 is here because I ranked on a proxy (name length) instead of on
evidence. When a tiebreak decides which of two records is TRUE, it must
consult an authority -- otherwise it is a coin flip wearing a sort key.

Local boto3-stub harness 14/14, fixtures built from the live regressions.
"""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION, B, FN = "us-east-1", "justhodl-dashboard-live", "justhodl-13f-positions"
OUT_KEY, MAP_KEY = "data/13f-positions.json", "data/13f-cusip-map.json"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=800, connect_timeout=20,
                                 retries={"max_attempts": 0}))

with report("ops_4938_13f_resolve_repair") as R:
    src = (ROOT / "aws" / "lambdas" / FN / "source"
           / "lambda_function.py").read_text()
    for k in ('"bytick": bytick', "budget=600", "_score(c)"):
        if k not in src:
            R.log("G0 FAIL producer missing %s" % k)
            sys.exit(1)
    R.log("G0 producer contract OK")

    t0 = time.time()
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    R.log("invoke rc=%s in %.0fs" % (r["StatusCode"], time.time() - t0))

    d = json.loads(s3.get_object(Bucket=B, Key=OUT_KEY)["Body"].read())
    agg = d.get("aggregate_by_ticker") or {}
    total = d.get("funds_total")
    fails = []

    def gate(n, c, det=""):
        R.log(("PASS " if c else "FAIL ") + n + "  " + str(det))
        if not c:
            fails.append(n)

    # ---- G1 the collision invariant must still hold
    gate("G1 one ticker <- exactly one cusip",
         d.get("cusip_collisions") == {},
         list(d.get("cusip_collisions") or {})[:8])

    # ---- G2 THE REGRESSION: named companies must own their own tickers.
    # These are the exact names the page printed as raw cusips.
    want = {"UBER": "UBER TECHNOLOGIES", "BRK-B": "BERKSHIRE HATHAWAY",
            "SE": "SEA", "MKSI": "MKS", "BILL": "BILL HOLDINGS",
            "RIVN": "RIVIAN", "BRKR": "BRUKER", "CPAY": "CORPAY"}
    missing = []
    for tk, frag in want.items():
        a = agg.get(tk) or {}
        nm = (a.get("name") or "").upper()
        if not a or frag.split()[0] not in nm:
            missing.append({"ticker": tk, "got": nm[:40] or "ABSENT"})
    gate("G2 major names hold their own tickers again", not missing, missing)

    # ---- G3 unresolved value must fall back toward the pre-purge baseline.
    # Pre-4937 it was $4.70B; 4937 blew it to $70.68B. Anything above $15B
    # means the purge is still outrunning re-resolution.
    unres = sum(float(a.get("total_value") or 0) for t, a in agg.items()
                if not (a.get("ticker") or "")
                or len(str(t)) == 9 and str(t)[:6].isalnum()
                and not str(t).isalpha())
    unres_named = sorted(
        [{"k": t, "name": (a.get("name") or "")[:34],
          "usd": round(float(a.get("total_value") or 0) / 1e9, 2)}
         for t, a in agg.items() if not (a.get("ticker") or "")],
        key=lambda r: -r["usd"])[:8]
    R.log("unresolved top: %s" % json.dumps(unres_named))
    gate("G3 unresolved book back under $15B (was $70.68B)",
         unres < 15e9, "$%.2fB" % (unres / 1e9))

    # ---- G4 4936/4937 wins must not regress
    gate("G4a roster balances", total == (d.get("funds_parsed") or 0)
         + (d.get("funds_failed") or 0),
         "%s/%s/%s" % (total, d.get("funds_parsed"), d.get("funds_failed")))
    gate("G4b holders <= roster",
         not [t for t, a in agg.items()
              if (a.get("n_funds_holding") or 0) > total])
    gate("G4c exits <= roster",
         not [t for t, a in agg.items()
              if (a.get("n_funds_exiting") or 0) > total])
    gate("G4d mcap_suspect still published honestly",
         isinstance(d.get("mcap_suspect"), list),
         len(d.get("mcap_suspect") or []))

    R.log("as_of=%s parsed=%s/%s tickers=%s"
          % (d.get("as_of_quarter"), d.get("funds_parsed"), total, len(agg)))
    if fails:
        R.log("ops 4938 RED: " + "; ".join(fails))
        sys.exit(1)
    R.log("ops 4938 GREEN -- 7/7 gates")
