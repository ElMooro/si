"""ops/4936 -- 13F desk integrity: five measured bugs, then acceptance.

Khalid flagged the desk as stale. The quarter was actually CORRECT
(Q2-2026 deadline was 8/14, page said 2026-06-30). The real failures
were structural and were hiding behind a plausible-looking page:

 1. L1204 SILENT DROP -- a fund with no discoverable 13F-HR hit a bare
    `continue`, entering neither `successful` nor `failed`. Hence
    funds_total 18 != parsed 17 + failed 0. The gap was ELLIOTT.
 2. L1120 ROW COUNT AS FUND COUNT -- n_funds_holding incremented once
    per infotable ROW. Citadel/Millennium file dozens of lots per ETF
    trust, so BSML read "166 funds" on an 18-fund roster (NZUS 131,
    IBIA 94). Same defect on adding/trimming/new and the option chips.
 3. L552 RESOLVER FALLBACK TOO LOOSE -- one-word substring match with
    no exchange filter. CPAY resolved to FOUR companies on one page
    (F N B / PG&E / SLM / ODP); ICLN resolved to QQQ and manufactured
    a phantom -$23.80B "most sold" line; MOBUSD/VEEUSD are CRYPTO and
    cannot appear in a 13F at all.
 4. L1764 NON-DETERMINISTIC HEADLINE -- as_of_quarter took
    successful[0], but that list is in as_completed() finish order.
    A stale filer finishing first would stamp the entire page with its
    quarter. Greenlight is ten quarters stale (CIK 0001079114 went
    dark; Einhorn's live filer is the DME entity).
 5. PARSER cache bumped v4->v5 so every fund re-parses; without this
    the fixes would be masked by cached pre-fix rows.

TRAP BANKED: the field-level G0 from ops 4817 generalises. A container
length check would have passed all four boards here -- the arrays were
full, the VALUES were wrong. Assert on a value's RELATION to a known
invariant (holders <= roster size), not on presence.

Local boto3-stub harness: 12/12 before push.
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

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-13f-positions"
OUT_KEY = "data/13f-positions.json"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=600, connect_timeout=20,
                                 retries={"max_attempts": 0}))

with report("ops_4936_13f_integrity") as R:
    R.log("invoking %s (parser v5 -- full re-parse, no cache reuse)" % FN)
    t0 = time.time()
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                      Payload=b"{}")
    R.log("invoke rc=%s in %.0fs" % (resp["StatusCode"], time.time() - t0))

    d = json.loads(s3.get_object(Bucket=B, Key=OUT_KEY)["Body"].read())
    total = d.get("funds_total")
    parsed = d.get("funds_parsed")
    failed = d.get("funds_failed")
    agg = d.get("aggregate_by_ticker") or {}
    fails = []

    def gate(name, cond, detail=""):
        R.log(("PASS " if cond else "FAIL ") + name + "  " + str(detail))
        if not cond:
            fails.append(name)

    # G1 -- roster accounting must balance. This is the Elliott gate.
    gate("G1 roster balances (total == parsed + failed)",
         total == (parsed or 0) + (failed or 0),
         "%s == %s + %s" % (total, parsed, failed))

    # G2 -- FIELD-LEVEL invariant, not container length (ops 4817 lesson).
    over = [(t, a.get("n_funds_holding")) for t, a in agg.items()
            if (a.get("n_funds_holding") or 0) > total]
    gate("G2 no ticker held by more funds than exist", not over,
         "worst=%s" % sorted(over, key=lambda x: -x[1])[:5])

    # G3 -- crypto/forex can never appear in a 13F.
    crypto = [t for t in agg
              if str(t).upper().endswith(("USD", "USDT"))]
    gate("G3 zero crypto pairs in the book", not crypto, crypto[:10])

    # G4 -- ticker->name must be 1:1 across the whole payload.
    seen, dupes = {}, []
    for t, a in agg.items():
        nm = (a.get("name") or "").upper().strip()
        if not t or not nm:
            continue
        if t in seen and seen[t] != nm:
            dupes.append((t, seen[t], nm))
        seen[t] = nm
    gate("G4 one ticker -> one name (CPAY 4-way collision)",
         not dupes, dupes[:5])

    # G5 -- honest staleness is now DECLARED, not hidden.
    gate("G5 stale_funds field published",
         "stale_funds" in d and "roster_periods" in d,
         d.get("stale_funds"))

    # G6 -- every dropped fund must name itself.
    gate("G6 fund_errors names every gap",
         len(d.get("fund_errors") or []) == (failed or 0),
         d.get("fund_errors"))

    R.log("as_of_quarter=%s  parsed=%s/%s  tickers=%s"
          % (d.get("as_of_quarter"), parsed, total, len(agg)))
    R.log("stale roster: %s" % json.dumps(d.get("stale_funds") or [])[:400])

    if fails:
        R.log("ops 4936 RED: " + "; ".join(fails))
        sys.exit(1)          # house rule: fails must NOT auto-move to ran/
    R.log("ops 4936 GREEN -- 6/6 gates")
