#!/usr/bin/env python3
"""ops 3058 (Arc C retry) -- ARC C (items 6-11): six fleet joins in one v3.3 pass -- Stovall cycle conditioning, smart-money chips (whales/DP/capital-flow/options), Wyckoff phase per ETF, XLY/XLP risk-appetite strip, revision-breadth hits, APAC lead-lag chips of Khalid's 17-item IR upgrade (items 1+2+3):
RRG quadrant map w/ trails, dated quadrant transitions logged to the
closed loop (IMPROVING->LEADING graded fwd vs SPY), and TRUE internal
breadth (% of top-25 holdings above own 50/200-DMA via radar's new
ma_state). Sequential: radar 1.3.1 first (publishes ma_state), then
IR 3.1 (consumes it + holdings for all 40)."""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3j(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"]
                      .read())


def wait_fresh(fn, rep):
    t0 = datetime.now(timezone.utc)
    diff = subprocess.run(["git", "diff", "--name-only", "HEAD^",
                           "HEAD"], capture_output=True, text=True,
                          timeout=20, cwd=str(AWS_DIR.parent)).stdout
    need = ("aws/lambdas/%s/" % fn) in diff
    for _ in range(30):
        c = LAM.get_function_configuration(FunctionName=fn)
        lm = datetime.fromisoformat(
            c["LastModified"].replace("+0000", "+00:00"))
        ok = (c.get("LastUpdateStatus") in (None, "Successful")
              and c.get("State") in (None, "Active"))
        if ok and ((not need) or lm >= t0 - timedelta(seconds=90)):
            time.sleep(6)
            return True
        time.sleep(20)
    return False


def run_wait(fn, key, polls=40):
    prev = ""
    try:
        prev = s3j(key).get("generated_at", "")
    except Exception:
        pass
    LAM.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
    for _ in range(polls):
        time.sleep(20)
        try:
            c = s3j(key)
            if c.get("generated_at", "") > prev:
                return c
        except Exception:
            pass
    return None


def main():
    fails, warns = [], []
    with report("3058_fleet_joins") as rep:
        rep.section("0. Phase-detector (publishes phases_all)")
        if not wait_fresh("justhodl-phase-detector", rep):
            fails.append("phase-detector never fresh")
            _fin(rep, fails, warns)
            sys.exit(1)
        pd = run_wait("justhodl-phase-detector",
                      "data/phase-detector.json")
        if not pd:
            fails.append("no fresh phase-detector")
            _fin(rep, fails, warns)
            sys.exit(1)
        pa = pd.get("phases_all") or {}
        rep.kv(phases_all_n=len(pa))
        if len(pa) < 450:
            fails.append("phases_all=%d (<450)" % len(pa))

        rep.section("1. Radar ma_state (already live from 3054)")
        r = s3j("data/accumulation-radar.json")
        ms = r.get("ma_state") or {}
        rep.kv(radar_v=r.get("version"), ma_state_n=len(ms))
        if len(ms) < 400:
            fails.append("ma_state=%d (<400)" % len(ms))

        rep.section("2. IR 3.1 (RRG + transitions + breadth)")
        if not wait_fresh("justhodl-industry-rotation", rep):
            fails.append("IR never fresh")
            _fin(rep, fails, warns)
            sys.exit(1)
        d = run_wait("justhodl-industry-rotation",
                     "data/industry-rotation.json")
        if not d:
            fails.append("no fresh IR json")
            _fin(rep, fails, warns)
            sys.exit(1)
        g = d.get("rrg") or {}
        quads = {}
        trail_ok = 0
        for t, e in g.items():
            quads[e.get("quadrant")] = quads.get(e.get("quadrant"),
                                                 0) + 1
            if len(e.get("trail") or []) >= 6:
                trail_ok += 1
        lad = d.get("ladder") or []
        ib = [x for x in lad if x.get("internal_breadth")]
        rep.kv(ir_version=d.get("version"), rrg_n=len(g),
               quadrants=json.dumps(quads), trails_ok=trail_ok,
               transitions=len(d.get("rrg_transitions") or []),
               breadth_rows=len(ib),
               breadth_sample=json.dumps(
                   [{"etf": x["etf"], **x["internal_breadth"]}
                    for x in ib[:4]]))
        if len(g) < 34:
            fails.append("rrg=%d ETFs (<34)" % len(g))
        if trail_ok < 30:
            fails.append("trails short: %d ok" % trail_ok)
        if len(quads) < 3:
            warns.append("only %d quadrants populated: %s"
                         % (len(quads), quads))
        if not d.get("rrg_transitions"):
            warns.append("transitions empty -- first pass seeds "
                         "quadrants, crossings accrue from tomorrow")
        if len(ib) < 28:
            fails.append("breadth on %d rows (<28)" % len(ib))
        for x in ib:
            b = x["internal_breadth"]
            if not (0 <= b["pct_above_50d"] <= 100):
                fails.append("%s breadth out of range" % x["etf"])
                break

        rep.section("2b. Arc B: EW/CW + MA events")
        ew = d.get("ew_cw") or {}
        me = d.get("ma_events") or []
        rep.kv(ir_v=d.get("version"), ew_pairs=len(ew),
               ew_sample=json.dumps({k: v for k, v in
                                     list(ew.items())[:4]}),
               ma_events=len(me),
               ma_sample=json.dumps(me[:5]))
        if d.get("version") != "3.3":
            fails.append("IR v=%s (want 3.3)" % d.get("version"))
        if len(ew) < 8:
            fails.append("ew_cw pairs=%d (<8)" % len(ew))
        for k, v in ew.items():
            if v.get("read") not in ("BROAD", "NARROW", "THINNING"):
                fails.append("%s ew read invalid" % k)
                break
        if not isinstance(me, list):
            fails.append("ma_events not a list")
        if not me:
            warns.append("no MA crossings today (honest -- events "
                         "accrue daily)")

        rep.section("2c. Arc C: six fleet joins")
        jh = d.get("join_hits") or {}
        cc = d.get("cycle_context")
        ra = d.get("risk_appetite")
        rep.kv(join_hits=json.dumps(jh),
               cycle=json.dumps(cc) if cc else "None",
               appetite=json.dumps({k: ra.get(k) for k in
                                    ("xly_xlp", "read",
                                     "vs_126d_ma_pct",
                                     "factor_regime_z")})
               if ra else "None",
               wyckoff_sample=json.dumps(
                   [{"etf": x["etf"], **x["wyckoff"]}
                    for x in lad if x.get("wyckoff")][:4]),
               apac_sample=json.dumps(
                   [x.get("apac") for x in lad
                    if x.get("apac")][:1]))
        if not cc or not cc.get("phase_bucket"):
            fails.append("cycle_context missing/unbucketed")
        if not ra or ra.get("xly_xlp") is None:
            fails.append("risk_appetite missing")
        avail = sum(1 for x in lad
                    if (pa.get(x["etf"]) or {}).get("p")
                    not in (None, "NEUTRAL"))
        rep.kv(wyckoff_available=avail)
        if (jh.get("wyckoff") or 0) != avail:
            fails.append("wyckoff join=%s but %d available"
                         % (jh.get("wyckoff"), avail))
        if avail < 6:
            warns.append("only %d ETFs have non-neutral Wyckoff "
                         "phase (honest -- ETFs trend quietly)"
                         % avail)
        if (jh.get("whales") or 0) < 10:
            fails.append("whales join=%s (<10)" % jh.get("whales"))
        if (jh.get("rev_hits") or 0) < 3:
            warns.append("rev_hits low: %s" % jh.get("rev_hits"))
        if not any(x.get("apac") for x in lad):
            warns.append("no APAC chips (feed schema miss -- "
                         "warn-level)")
        for k in ("dark_pool", "capital_flow", "options"):
            if not jh.get(k):
                warns.append("%s join=0 (membership may be honest)"
                             % k)

        rep.section("3. Page (warn-level)")
        try:
            import urllib.request
            req = urllib.request.Request(
                "https://justhodl.ai/industry-rotation.html?cb=%d"
                % time.time(),
                headers={"User-Agent": "Mozilla/5.0 ops-3058"})
            pg = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "replace")
            rep.kv(page_rrg="renderRRG" in pg,
                   page_breadth="internal_breadth" in pg)
            if "renderRRG" not in pg:
                warns.append("page not propagated")
        except Exception as e:
            warns.append("page: %s" % str(e)[:70])

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- Arc A live")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3058.json").write_text(json.dumps(
        {"ops": 3058, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
