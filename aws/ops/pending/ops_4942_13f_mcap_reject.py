"""ops/4942 -- reject an implausible market cap instead of publishing it.

ops 4937b added mcap_suspect[] so the bad caps were VISIBLE. That was
half a fix: the flagged value still populated market_cap and cap_tier,
so the renderer went on badging and ranking with it. Vossloh -- a roughly
EUR1.5bn industrial that FMP returns at $1.08M against $775M held --
kept showing as MICRO-CAP and kept its place on the small/mid-cap board.

  A flag the renderer ignores is not a fix. If a value is not fit to
  publish, it must not be published -- writing it and annotating it
  elsewhere just moves the lie one field over.

Three impossibility tests, strongest first:
  1. held > market_cap x1.5. 13F filers cannot own more of a company
     than it is worth. total_value excludes option rows (options are
     split out before accumulation), so this is a clean equity test.
  2. cap < $3M against a nine-figure position -- a stub value, typical
     of foreign ordinaries quoted on OTC.
  3. ETF / trust names have no equity market cap at all.

On rejection: market_cap and cap_tier go null, mcap_rejected records
why, and the row is published in mcap_suspect with the value that was
thrown away. An unknown cap is honest. A wrong one ranks.

Live rows this clears: VSSSF 714x, IBIA 42.7x, NFE 27x, MBAIF 4x.
Local harness 6/6 -- all four rejected, AAPL and CRI untouched.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION, B = "us-east-1", "justhodl-dashboard-live"
POS, OUT_KEY = "justhodl-13f-positions", "data/13f-positions.json"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=900, connect_timeout=20,
                                 retries={"max_attempts": 0}))


def artifact_ready(fn, marker, budget=480):
    """ops 4942b: the artifact carrying the change is NOT enough.

    The first attempt confirmed '_MCAP_REJECTED' was in the deployed zip
    at 15:03:31, invoked immediately, and got a payload built by the OLD
    code -- old mcap_suspect schema, market_cap still populated. Cause:
    UpdateFunctionCode swaps the zip and returns straight away, but the
    function sits at LastUpdateStatus=InProgress for a few seconds more,
    and invokes in that window still execute the PREVIOUS code.

      So the ladder is three rungs, not two:
        repo has it   ->  artifact has it   ->  function is READY to run it
      4940 checked rung one, 4941 added rung two, this adds rung three.

    Require both the marker AND State=Active + LastUpdateStatus=Successful.
    """
    t0 = time.time()
    while time.time() - t0 < budget:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            state = cfg.get("State")
            upd = cfg.get("LastUpdateStatus")
            if state == "Active" and upd == "Successful":
                loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
                z = zipfile.ZipFile(io.BytesIO(
                    urllib.request.urlopen(loc, timeout=90).read()))
                for n in z.namelist():
                    if n.endswith("lambda_function.py"):
                        if marker in z.read(n).decode("utf-8", "replace"):
                            R.log("artifact ready: state=%s update=%s"
                                  % (state, upd))
                            return True
            else:
                R.log("waiting: state=%s lastUpdate=%s" % (state, upd))
        except Exception as e:
            R.log("artifact poll: %s" % str(e)[:70])
        time.sleep(15)
    return False


with report("ops_4942_13f_mcap_reject") as R:
    # marker taken from the payload-emitting region -- the LAST thing the
    # edit touched -- so a half-applied artifact cannot pass.
    if not artifact_ready(POS, "rejected_market_cap"):
        R.log("G0 FAIL deployed function not ready with the rejection pass")
        sys.exit(1)
    R.log("G0 deployed function is Active and carries the rejection pass")
    time.sleep(10)          # let the update settle before first invoke

    try:
        prev = s3.head_object(Bucket=B, Key=OUT_KEY)["LastModified"]
    except Exception:
        prev = None
    lam.invoke(FunctionName=POS, InvocationType="Event", Payload=b"{}")
    t0, fresh = time.time(), None
    while time.time() - t0 < 780:
        time.sleep(20)
        try:
            lm = s3.head_object(Bucket=B, Key=OUT_KEY)["LastModified"]
        except Exception:
            continue
        if prev is None or lm > prev:
            fresh = lm
            break
    if not fresh:
        R.log("FAIL payload never refreshed")
        sys.exit(1)
    R.log("payload refreshed after %.0fs" % (time.time() - t0))
    time.sleep(5)

    d = json.loads(s3.get_object(Bucket=B, Key=OUT_KEY)["Body"].read())
    agg = d.get("aggregate_by_ticker") or {}
    fails = []

    def gate(n, c, det=""):
        R.log(("PASS " if c else "FAIL ") + n + "  " + str(det))
        if not c:
            fails.append(n)

    # ---- G1 no SURVIVING market cap may be impossible. This is the test
    # 4937b could not make, because it flagged without rejecting.
    left = []
    for t, a in agg.items():
        mc, held = a.get("market_cap") or 0, a.get("total_value") or 0
        try:
            mc, held = float(mc), float(held)
        except (TypeError, ValueError):
            continue
        if mc > 0 and held > mc * 1.5:
            left.append({"t": t, "x": round(held / mc, 1),
                         "name": (a.get("name") or "")[:30]})
    left.sort(key=lambda r: -r["x"])
    gate("G1 zero surviving impossible market caps", not left, left[:6])

    # ---- G2 the four named rows must be rejected, not merely flagged
    named = {}
    for t in ("VSSSF", "IBIA", "NFE", "MBAIF"):
        a = agg.get(t)
        if a:
            named[t] = {"market_cap": a.get("market_cap"),
                        "cap_tier": a.get("cap_tier"),
                        "rejected": a.get("mcap_rejected")}
    gate("G2 VSSSF/IBIA/NFE/MBAIF carry null cap AND a reason",
         all(v["market_cap"] is None and v["cap_tier"] is None
             and v["rejected"] for v in named.values()) if named else True,
         named)

    # ---- G3 the discarded values are still published, not hidden
    sus = d.get("mcap_suspect")
    gate("G3 rejected caps published with the value thrown away",
         isinstance(sus, list)
         and all("rejected_market_cap" in r and "reason" in r for r in sus),
         "%d rows: %s" % (len(sus or []),
                          [r.get("ticker") for r in (sus or [])[:6]]))

    # ---- G4 rejection must not gut real coverage
    kept = sum(1 for a in agg.values() if (a.get("market_cap") or 0) > 0)
    gate("G4 real mcap coverage survives (>=400 tickers)", kept >= 400, kept)

    # ---- G5 nothing from 4936-4941 regressed
    tot = d.get("funds_total")
    gate("G5a roster 18/18",
         d.get("funds_parsed") == tot and (d.get("funds_failed") or 0) == 0,
         "%s/%s/%s" % (tot, d.get("funds_parsed"), d.get("funds_failed")))
    gate("G5b zero label-vs-filer mismatches",
         d.get("roster_label_mismatch") == [])
    gate("G5c holders <= roster",
         not [t for t, a in agg.items()
              if (a.get("n_funds_holding") or 0) > tot])

    R.log("as_of=%s tickers=%s mcap_kept=%s rejected=%s"
          % (d.get("as_of_quarter"), len(agg), kept, len(sus or [])))
    if fails:
        R.log("ops 4942 RED: " + "; ".join(fails))
        sys.exit(1)
    R.log("ops 4942 GREEN -- 7/7 gates")
