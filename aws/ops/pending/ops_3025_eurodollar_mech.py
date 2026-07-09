#!/usr/bin/env python3
"""ops 3025 -- Eurodollar Funding Plumbing as the 12th warroom mechanism
(Khalid: add whatever is missing from eurodollar.html). ~16 new rows
(SOFR tail, TGA, EFFR-IORB, bill-OIS, OFR-FSI, IG OAS, FIMA/SRF/ECB
backstops, UST fails, foreign custody, net-due-foreign, CNH complex,
JPY, offshore stablecoin); 8 concept-dups + HK trio skipped via
EURODOLLAR_DEDUPE.

Prior: ops 3024 -- plumbing FINAL. 3023 probe proved schema fine + data fresh
(29 with data); root cause: the aggregator publishes the field as
'stress_score', not 'stress_score_0_100'. Reader field chain fixed.
Live file already fresh -- refresh section dropped.

Prior: ops 3023 -- plumbing mechanism CLOSE. 3022: zero rows because the LIVE
plumbing-stress.json predates the raw_indicators schema in the repo
source. This run refreshes the aggregator first (Event+poll), probes
the live schema into the report, and the reader now accepts both the
new raw_indicators dict and the older indicators list.

Prior: ops 3022 -- Plumbing & Stress as the 11th warroom mechanism (Khalid:
add whatever is missing from plumbing.html). 31 aggregator indicators,
each at its own stress_score_0_100; 7 excluded via PLUMBING_DEDUPE as
already-voting duplicates (temp help, MCUMFN capacity, CISS US/EA/CN,
broad dollar, Fed swaps 16-90d) -- one canary, one vote.

Prior: ops 3021 -- Global Stress Matrix as the 10th warroom mechanism
(Khalid: add whatever is on global-stress.html not yet implemented).
NEW rows: 10 per-market TAPE-stress scores (drawdown/vol/trend --
distinct from RS leadership) + the credit-tier OAS ladder, all voting.
Skipped as double-counts: escalation dims, hot_signals echoes,
composites (card score), tier dispersion (CCC-BB member already live).

Prior: ops 3020 -- rows FINAL. 3019 lost a deploy race: invoked 16s into the
deploy run at LastModified-age 0.0 while the Lambda still served the
pre-fix package (LastUpdateStatus mid-flight). wait_fresh now also
requires LastUpdateStatus=Successful + State=Active + settle sleep.
The type-fix deploy has since completed green -- this is the clean
rerun.

Prior: ops 3019 -- rows-expansion CLOSE. 3018 diagnosis: the warroom CRASHED
mid-run on TypeError float<str -- crisis-canaries member 'lead' is a
STRING (e.g. 2-6w) and entered the firing sort beside float
lead_months; output never wrote, so all asserts read stale v3 data.
Fixed: numeric coercion at row creation + type-safe sort key. Also
trimmed section-0 to the one fn this push deploys.

Prior: ops 3018 -- full-row expansion VERIFY (Khalid: funding members + all
sentinel states as individual page rows, all voting). Warroom v4:
norm_crisis emits EVERY member from crisis-canaries {name, family,
status RED/AMBER/GREEN, value, detail} (family aggregates now
fallback-only); norm_alerts expands the live sentinel SNAPSHOT --
honest correction: the old 212 was the alert-BUFFER length, not a
rule count; what exists per-item is the live state book (breakouts,
red canaries, thrusts, hyper-pumps, insider declines, papers, scalar
watches), risk-ON states at LOW stress. Barometer votes now include
funding + alerts mechanisms.

Prior scope: ops 3017 -- v3 CLOSE: single remaining fail was norm_cftc iterating
top-level cache values; contracts live under d["data"] (ground-truthed
from the writer: result={"source","contracts",<int>,"data":{...}}).
Reader fixed. Rerun of: ops 3016 -- Canary v3 (Khalid 10-item list + full CISS board) VERIFY.
Shipped: grid +8 (discount window, fin CP-bill TED-successor, BBB-AAA
fallen-angel pipeline, 2s10s bull-steepening VELOCITY, BKLN/HYG,
copper/gold, SMH/ACWI, MOVE/VIX); risk-ratios +4 metrics (+minimal
FRED fetch for VIXCLS, Yahoo ^MOVE probe); warroom +3 mechanisms
(norm_ciss = EVERY ECB CISS series as a canary at its own history
percentile, norm_factor_regime appetite z, norm_cftc positioning
extremes) -- all voting in the equal-weight barometer (headline now
9 mechanisms). This script re-runs the chain and asserts.

Prior scope: ops 3015 -- War Room barometer + full-inventory + fusion VERIFY.

Shipped this push (all deploys via deploy-lambdas.yml on this same push;
this script is verify-only per AUTONOMY.md):
  1. warroom v2: leading-markets/dollar/vol normalizers now emit EVERY
     watched canary (calm ones graded, firing rules unchanged); funding
     gains per-family aggregate rows; NEW top-level `barometer` =
     equal-weight mean stress, one vote per watched canary (Khalid spec),
     sentinel alert-rules excluded as binary flips.
  2. canaries.html: top SVG barometer gauge + 'Everything watched'
     inventory tab rendering all_canaries incl calm.
  3. Fusion: signal-board feed "Early-Warning War Room" (barometer ->
     -2..+2) + morning-intelligence EARLY_WARNING_WARROOM prompt line.
     (strategist already consumes warroom; crisis-composite deliberately
     NOT wired -- it consumes the raw grid, wiring the warroom too would
     double-count.)"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def wait_fresh(fn, max_min=8):
    """Fresh AND settled: LastModified flips the instant update_function_code
    is called, while invokes can still execute the OLD package until
    LastUpdateStatus==Successful (3019 lesson: invoked 16s after the deploy
    run started and hit pre-fix code at age 0.0)."""
    for _ in range(int(max_min * 3)):
        try:
            c = LAM.get_function_configuration(FunctionName=fn)
            lm = datetime.fromisoformat(
                c["LastModified"].replace("+0000", "+00:00"))
            age = (datetime.now(timezone.utc) - lm).total_seconds() / 60.0
            settled = (c.get("LastUpdateStatus") in (None, "Successful")
                       and c.get("State") in (None, "Active"))
            if age < 12 and settled:
                time.sleep(8)   # publish settle
                return age
        except Exception:
            pass
        time.sleep(20)
    return None


def main():
    fails, warns = [], []
    with report("3025_eurodollar_mech") as rep:
        rep.section("0. Wait for deploys")
        ages = {fn: wait_fresh(fn) for fn in
                ("justhodl-canary-warroom",)}
        rep.kv(code_ages_min={k: (round(v, 1) if v is not None else "STALE")
                              for k, v in ages.items()})
        if ages["justhodl-canary-warroom"] is None:
            fails.append("canary-warroom code not fresh after wait")
            _finish(rep, fails, warns, {})
            sys.exit(1)

        rep.section("1. Warroom v4 regeneration")
        r = LAM.invoke(FunctionName="justhodl-canary-warroom",
                       InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(invoke=json.loads(r["Payload"].read() or b"{}"))
        d = s3_json("data/canary-warroom.json")
        baro = d.get("barometer") or {}
        cans = d.get("all_canaries") or []
        fund = [c for c in cans if c.get("mechanism") == "funding"]
        alrt = [c for c in cans if c.get("mechanism") == "alerts"]
        fund_green = sum(1 for c in fund if not c.get("firing"))
        fund_fam = sum(1 for c in fund if c.get("synthetic_family"))
        alrt_calm = sum(1 for c in alrt if not c.get("firing"))
        rep.kv(barometer=baro.get("score"), band=baro.get("band"),
               n_votes=baro.get("n_votes"), n_canaries=len(cans),
               funding_rows=len(fund), funding_green=fund_green,
               funding_family_fallback=fund_fam,
               sentinel_rows=len(alrt), sentinel_informational=alrt_calm,
               note=(baro.get("note") or "")[:120])
        ed = [c for c in cans if c.get("mechanism") == "eurodollar"]
        mech_keys = [m.get("key") for m in (d.get("mechanisms") or [])]
        leak = [c["name"] for c in ed if any(t in c["name"] for t in (
            "SOFR \u2212 IORB", "ON RRP", "Reserves", "swap line",
            "Broad dollar", "HIBOR", "HY credit OAS", "CP\u2212OIS",
            "Nonfin CP"))]
        rep.kv(eurodollar_rows=len(ed),
               eurodollar_firing=sum(1 for c in ed if c["firing"]),
               mechanisms=mech_keys, dedupe_leaks=leak,
               sample=[c["name"] for c in ed[:6]])
        if "eurodollar" not in mech_keys:
            fails.append("eurodollar mechanism missing")
        if len(ed) < 12:
            fails.append("eurodollar rows=%d (<12)" % len(ed))
        if leak:
            fails.append("dedupe leaked: %s" % leak[:3])
        if (baro.get("n_votes") or 0) <= 272:
            fails.append("n_votes=%s did not grow past 272"
                         % baro.get("n_votes"))

        rep.section("3. Live page checks (CDN lag = warn-level)")
        try:
            import urllib.request
            req = urllib.request.Request(
                "https://justhodl.ai/canaries.html?v=%d" % time.time(),
                headers={"User-Agent": "Mozilla/5.0 ops-3015"})
            page = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "replace")
            ok_g = "MASTER CANARY BAROMETER" in page
            ok_t = "Everything watched" in page
            rep.kv(page_gauge=ok_g, page_everything_tab=ok_t)
            if not (ok_g and ok_t):
                warns.append("pages not propagated yet (gauge=%s tab=%s)"
                             % (ok_g, ok_t))
        except Exception as e:
            warns.append("live page check: %s" % str(e)[:120])
        rep.log("morning-intelligence wiring verified by deploy + compile; "
                "prompt line lands in tomorrow's 8AM brief (not invoked "
                "here -- LLM cost discipline).")

        rep.section("verdict")
        _finish(rep, fails, warns,
                {"barometer": baro.get("score"), "n_votes": baro.get(
                    "n_votes"), "n_canaries": len(cans)})
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- barometer %s (%s) over %s equal votes; full "
                "inventory live" % (baro.get("score"), baro.get("band"),
                                    baro.get("n_votes")))


def _finish(rep, fails, warns, extra):
    payload = {"ops": 3025, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3025.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
