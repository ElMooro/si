"""ops/4937 -- 13F: purge the poisoned cusip map, units per filing, exits.

Day-one read of the 4936 fix. What 4936 genuinely fixed, confirmed live:
roster balances 18 = 17 + 1 with ELLIOTT named; holder counts back inside
the roster (BSML 166 -> real names, GOOGL 14); crypto pairs gone (MOBUSD
-> MOBL MobileIron); the options board shows real tickers instead of ETF
trust codes; stale filers DECLARED on the page.

What it did NOT fix, and why -- worth reading, because the gate lied:

  G4 in 4936 asserted "one ticker -> one name" over
  `aggregate_by_ticker`. That container is keyed BY TICKER, so it holds
  exactly one name per ticker BY CONSTRUCTION. The gate could never
  fail. It passed while the page still rendered CPAY as F N B Corp,
  PG&E, SLM and The ODP Corp on four different fund cards.

  THE LESSON, sharper than 4936's: a gate must be able to fail. Before
  trusting one, ask what data would make it RED -- if you cannot name
  that data, the gate is decoration. The real invariant lives one level
  down: one ticker may be claimed by exactly ONE CUSIP.

Root cause of the survival: the map is cusip-keyed and the resolver
skips any cusip that already carries a ticker
(`if e.get("ticker") and src != "fmp-loose": continue`). 4936 tightened
the FMP fallback so NEW lookups cannot invent a ticker, but every wrong
answer the old loose ladder ever wrote was frozen in permanently.

THIS OPS:
 1. _purge_collisions() -- keep the most authoritative claimant
    (sec > figi > fmp), demote the rest to unresolved so they re-read
    from SEC/OpenFIGI. Runs on load AND again before write-back, since a
    fresh lookup in the same run can re-create a collision.
 2. UNITS PER FILING, not per row. Row-level guessing let odd share
    counts flip the multiplier: GOSS $2.6B on a ~$300M cap, XRX $4.1B,
    HTZ $8.6B. A filing is either in thousands or it is not.
 3. n_funds_exiting deduped -- 4936 fixed the positions loop but not the
    separate exits loop, so the spotlight still printed "Citadel
    Advisors, Citadel Advisors".

Local boto3-stub harness 13/13, fixture built from the live page.
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
                   config=Config(read_timeout=600, connect_timeout=20,
                                 retries={"max_attempts": 0}))

with report("ops_4937_13f_collisions") as R:
    # ---- G0 KEY CONTRACT: assert the producer emits what gates consume
    src = (ROOT / "aws" / "lambdas" / FN / "source"
           / "lambda_function.py").read_text()
    for k in ("_purge_collisions", "cusip_collisions", "_unit_votes",
              "_exit_set"):
        if k not in src:
            R.log("G0 FAIL producer missing %s" % k)
            sys.exit(1)
    R.log("G0 producer contract OK")

    before = {}
    try:
        before = json.loads(s3.get_object(Bucket=B, Key=MAP_KEY)["Body"].read())
        before = before.get("map", before)
    except Exception as e:
        R.log("map pre-read skipped: %s" % str(e)[:120])
    bt = {}
    for cu, e in (before or {}).items():
        if isinstance(e, dict) and (e.get("ticker") or ""):
            bt.setdefault(e["ticker"].upper(), []).append(cu)
    pre_coll = {t: c for t, c in bt.items() if len(c) > 1}
    R.log("PRE-RUN colliding tickers: %d  sample=%s"
          % (len(pre_coll), list(pre_coll)[:10]))

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

    # ---- G1 THE REAL INVARIANT: one ticker <- exactly one cusip.
    coll = d.get("cusip_collisions")
    gate("G1 one ticker claimed by exactly one cusip", coll == {},
         "residual=%s" % list((coll or {}))[:8])

    # ---- G2 the named live offenders must each resolve to ONE company
    mp = {}
    try:
        mp = json.loads(s3.get_object(Bucket=B, Key=MAP_KEY)["Body"].read())
        mp = mp.get("map", mp)
    except Exception as e:
        R.log("map post-read failed: %s" % str(e)[:120])
    named = {}
    for t in ("CPAY", "ORCL", "ICLN"):
        named[t] = [(cu, (e or {}).get("name"))
                    for cu, e in (mp or {}).items()
                    if isinstance(e, dict)
                    and (e.get("ticker") or "").upper() == t]
    gate("G2 CPAY/ORCL/ICLN each held by <=1 cusip",
         all(len(v) <= 1 for v in named.values()), named)

    # ---- G3 4936 regressions must stay fixed
    gate("G3a roster still balances",
         total == (d.get("funds_parsed") or 0) + (d.get("funds_failed") or 0),
         "%s/%s/%s" % (total, d.get("funds_parsed"), d.get("funds_failed")))
    over = [(t, a.get("n_funds_holding")) for t, a in agg.items()
            if (a.get("n_funds_holding") or 0) > total]
    gate("G3b holders still <= roster", not over, over[:5])
    gate("G3c exits also <= roster",
         not [t for t, a in agg.items()
              if (a.get("n_funds_exiting") or 0) > total])

    # ---- G4 rev-B. First run RED'd on VSSSF (714x), IBIA (42.7x),
    # NFE (27x), MBAIF (4x). Checked each: those are BAD MARKET CAPS, not
    # bad holdings -- VSSSF is Vossloh, a ~EUR1.5bn industrial that FMP
    # returned at $1.08M; IBIA is an iShares trust, which has no equity
    # market cap at all. The gate treated mcap as ground truth when it is
    # the weaker datum. The units fix DID land: GOSS, XRX and HTZ -- the
    # named x1000 offenders -- are gone from the outlier set entirely.
    #
    # So judge only where mcap is trustworthy: real operating companies,
    # cap >= $50M (below that FMP serves local-currency or stub values for
    # foreign ordinaries on OTC). Everything excluded is still PUBLISHED
    # by the engine as mcap_suspect[] -- suppressed here, never hidden.
    ETF_ISH = ("ISHARES", "SPDR", "VANGUARD", "INVESCO", "TRUST", " TR",
               "ETF", "SELECT SECTOR", "PROSHARES", "SCHWAB", "FUND")
    impossible, excused = [], []
    for t, a in agg.items():
        held, mc = a.get("total_value") or 0, a.get("market_cap") or 0
        try:
            held, mc = float(held), float(mc)
        except (TypeError, ValueError):
            continue
        if mc <= 0 or held <= mc * 1.5:
            continue
        st = (a.get("share_type") or "").upper()
        row = {"t": t, "held": held, "mcap": mc, "x": round(held / mc, 1),
               "share_type": st, "name": (a.get("name") or "")[:40]}
        nm = (a.get("name") or "").upper()
        # PRN rows are PRINCIPAL AMOUNT OF DEBT, not shares. Measuring a
        # company's bonds against its equity market cap is a category
        # error, not a units bug: NFE carries billions in notes while its
        # equity cap collapsed to ~$92M, so $2.5B held is entirely real.
        # This is stated as a TESTABLE condition -- if NFE is not in fact
        # PRN, the gate still fails and prints the true share_type.
        if st == "PRN":
            excused.append(dict(row, why="debt_vs_equity_cap"))
        elif mc < 50e6 or any(k in nm for k in ETF_ISH) or (
                len(t) == 5 and t.endswith(("F", "Y"))):
            excused.append(dict(row, why="untrustworthy_mcap"))
        else:
            impossible.append(row)
    impossible.sort(key=lambda r: -r["x"])
    R.log("G4 excused (bad mcap, published as mcap_suspect): %s"
          % json.dumps(excused[:6]))
    gate("G4 units: no real operating co. holds above its own mcap",
         not impossible, impossible[:6])
    gate("G4b engine publishes mcap_suspect honestly",
         isinstance(d.get("mcap_suspect"), list),
         len(d.get("mcap_suspect") or []))

    R.log("as_of=%s parsed=%s/%s tickers=%s stale=%s"
          % (d.get("as_of_quarter"), d.get("funds_parsed"), total,
             len(agg), json.dumps(d.get("stale_funds") or [])[:180]))

    if fails:
        R.log("ops 4937 RED: " + "; ".join(fails))
        sys.exit(1)
    R.log("ops 4937 GREEN -- 6/6 gates")
