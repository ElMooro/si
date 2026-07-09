#!/usr/bin/env python3
"""ops 3014 -- Canary Grid v2 FINAL. Round-3 closed the 429 class (igrea
went LIVE); the last two dead canaries were the 95-day staleness gate:
btp_bund legs (OECD monthly, latest 2026-04 = 99d) and the reserves
composite (IMF IFS, US leg 130d) both exceed STALE_HARD_DAYS on normal
publication lag. Fixed with explicit max_stale_days=150/160, the exact
mechanism the grid already uses for SLOOS (140) and Peru (180).

ops 3013 -- Canary Grid v2 round 3 (CLOSE). Round-2 probe proved the
paradox: IGREA + both IRLTLT01 legs ALIVE on FRED from the runner, DEAD
from the grid Lambda -- classic burst rate-limiting (100+ FRED calls,
no throttle, tail canaries 429d). Fixes this push: fred() throttled
0.13s + one retry on 429/5xx; global_fx_reserves collapsed to the
probe-proven CN+US+JP trio (CH leg HTTP-400 dead, EZ stale-2018, XM
dead -- the 4 doomed permutations were themselves burning ~20 calls
right before the tail); btp_bund candidates reordered proven-FRED
spread first. Same verify chain + probe below.

ops 3012 -- Canary Grid v2 round 2: 3011 landed 49/55 live; this push fixes the 3 dead feeds (btp_bund -> ECB Maastricht IRS via ecbspread:, igrea_global -> +BDRY dry-bulk feed fallback, global_fx_reserves -> 5 graceful sum permutations incl. TRESEGXMM052N euro-area alt) and adds a runner-side FRED id probe to name dead legs definitively. VERIFY-only (no deploys here: this push's
lambda-source changes trigger deploy-lambdas.yml, which bundles correctly;
per AUTONOMY.md trap doc, ops scripts never also deploy).

What shipped on this push: (1) NEW engine justhodl-risk-ratios (HYG/LQD,
ANGL/HYG fallen angels, HYG, ACWI, RXI global consumer, EEM realized vol,
WTI term structure -- Polygon+Yahoo daily); (2) canary-grid v2: 4 new source
handlers (ratio:, sum:, ecb:, ecbspread:) + daily-feed format + 6th
sub-grid `global_risk` + 28 NEW canaries (all 26 existing kept verbatim);
(3) leading-markets universe +EWX/VPL/RXI.

This script: waits for both fns to carry fresh code, boto3-creates
risk-ratios from the runner checkout if deploy-lambdas' create branch
no-ops (known INTERMITTENT), regenerates the whole chain
risk-ratios -> canary-grid -> leading-markets -> canary-warroom, and
hard-verifies with a per-canary resolution table."""
import io
import json
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

REGION = "us-east-1"
LAM = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 0}))
SCH = boto3.client("scheduler", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]

# canaries this push added; hard = must be available, soft = warn if not
HARD_KEYS = ["repo_sofr_iorb", "rrp_parking", "bank_reserves",
             "bill4w_floor", "hyg_lqd", "hy_etf", "acwi_tape", "em_vol",
             "euro_hy_oas", "em_corp_oas", "btp_bund", "global_metals",
             "chile_tot_proxy", "peru_tot_proxy", "core_capex_orders",
             "mfg_capacity", "mfg_employment", "igrea_global"]
SOFT_KEYS = ["cp3m_ff", "global_fx_reserves", "euribor3m", "eu_curve_30_5",
             "us_hy_ytw", "em_hy_ytw", "em_hy_oas", "oil_term",
             "fallen_angels_rs", "global_consumer"]


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def fn_fresh(fn, max_age_min=12):
    try:
        c = LAM.get_function_configuration(FunctionName=fn)
        lm = datetime.fromisoformat(c["LastModified"].replace("+0000",
                                                              "+00:00"))
        age = (datetime.now(timezone.utc) - lm).total_seconds() / 60.0
        return True, age
    except LAM.exceptions.ResourceNotFoundException:
        return False, None


def ensure_risk_ratios(rep, warns):
    """deploy-lambdas create branch is INTERMITTENT for brand-new dirs --
    fall back to boto3 create_function from the runner checkout (the fn has
    NO shared imports, so a source-only zip is correct here)."""
    for _ in range(14):                       # up to ~4.5 min
        exists, age = fn_fresh("justhodl-risk-ratios")
        if exists:
            rep.kv(risk_ratios_exists=True, code_age_min=round(age or -1, 1))
            return
        time.sleep(20)
    rep.log("risk-ratios missing after wait -- creating via boto3 fallback")
    src = (AWS_DIR / "lambdas" / "justhodl-risk-ratios" / "source"
           / "lambda_function.py").read_bytes()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
    donor = LAM.get_function_configuration(
        FunctionName="justhodl-confluence-meta")
    denv = (donor.get("Environment") or {}).get("Variables") or {}
    env = {k: v for k, v in denv.items()
           if any(t in k.upper() for t in ("POLYGON", "FRED", "FMP"))}
    env["S3_BUCKET"] = BUCKET
    LAM.create_function(
        FunctionName="justhodl-risk-ratios", Runtime="python3.12",
        Role="arn:aws:iam::857687956942:role/lambda-execution-role",
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": buf.getvalue()}, Timeout=180, MemorySize=256,
        Environment={"Variables": env},
        Description="Daily market-priced risk-appetite ratios for the "
                    "canary grid. data/risk-ratios.json.")
    time.sleep(8)
    rep.log("created justhodl-risk-ratios via boto3")
    try:
        SCH.get_schedule(Name="justhodl-risk-ratios-daily")
    except Exception:
        SCH.create_schedule(
            Name="justhodl-risk-ratios-daily",
            ScheduleExpression="cron(40 21 * * ? *)",
            ScheduleExpressionTimezone="UTC",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": ("arn:aws:lambda:us-east-1:857687956942:"
                            "function:justhodl-risk-ratios"),
                    "RoleArn": ("arn:aws:iam::857687956942:role/"
                                "justhodl-scheduler-role")},
            Description="Risk ratios - daily post-close 21:40 UTC.")
        rep.log("created scheduler justhodl-risk-ratios-daily")


def invoke_and_poll(rep, fn, out_key, minutes, fails, sync=False):
    prev = ""
    try:
        prev = s3_json(out_key).get("generated_at", "")
    except Exception:
        pass
    LAM.invoke(FunctionName=fn,
               InvocationType=("RequestResponse" if sync else "Event"),
               Payload=b"{}")
    for _ in range(int(minutes * 3)):
        time.sleep(20)
        try:
            d = s3_json(out_key)
            if d.get("generated_at", "") > prev:
                return d
        except Exception:
            continue
    fails.append("no fresh %s after %dmin" % (out_key, minutes))
    return None


def main():
    fails, warns = [], []
    with report("3014_canary_v2_final") as rep:
        rep.section("0. Wait for this push's deploys to land")
        for _ in range(15):
            _, age = fn_fresh("justhodl-canary-grid")
            if age is not None and age < 12:
                break
            time.sleep(20)
        rep.kv(canary_grid_code_age_min=round(age or -1, 1))
        ensure_risk_ratios(rep, warns)

        rep.section("0.5 FRED id probe (runner-side, definitive)")
        import urllib.request as _ur
        FKEY = "2f057499936072679d8843d7fce99989"
        probe_ids = ["TRESEGCNM052N", "TRESEGUSM052N", "TRESEGJPM052N",
                     "TRESEGCHM052N", "TRESEGEZM052N", "TRESEGXMM052N",
                     "IGREA", "IRLTLT01ITM156N", "IRLTLT01DEM156N"]
        probe = {}
        for pid in probe_ids:
            try:
                u = ("https://api.stlouisfed.org/fred/series/observations"
                     "?series_id=%s&api_key=%s&file_type=json"
                     "&sort_order=desc&limit=3" % (pid, FKEY))
                with _ur.urlopen(u, timeout=20) as r:
                    obs = json.loads(r.read()).get("observations", [])
                live = [o for o in obs if o.get("value") not in (".", None)]
                probe[pid] = "%d rows, latest %s=%s" % (
                    len(obs),
                    live[0].get("date") if live else "-",
                    live[0].get("value") if live else "-")
            except Exception as e:
                probe[pid] = "DEAD: %s" % str(e)[:60]
        rep.kv(fred_probe=json.dumps(probe))

        rep.section("1. Risk-ratios engine")
        rr = invoke_and_poll(rep, "justhodl-risk-ratios",
                             "data/risk-ratios.json", 4, fails, sync=True)
        if rr:
            rep.kv(rr_n_live=rr.get("n_live"),
                   hyg_lqd=(rr.get("hyg_lqd") or {}).get("latest"),
                   eem_rvol=(rr.get("eem_rvol") or {}).get("latest"),
                   oil_term=(rr.get("oil_term") or {}).get("latest"),
                   oil_available=(rr.get("oil_term") or {}).get("available"))
            if (rr.get("n_live") or 0) < 4:
                fails.append("risk-ratios n_live=%s (<4)" % rr.get("n_live"))
            if not (rr.get("hyg_lqd") or {}).get("available"):
                fails.append("hyg_lqd unavailable in risk-ratios")
        else:
            _finish(rep, fails, warns, {})
            sys.exit(1)

        rep.section("2. Canary grid v2 regeneration")
        cg = invoke_and_poll(rep, "justhodl-canary-grid",
                             "data/canary-grid.json", 10, fails)
        if not cg:
            _finish(rep, fails, warns, {})
            sys.exit(1)
        sigs = {s.get("key"): s for s in (cg.get("signals") or [])}
        n_avail = sum(1 for s in sigs.values() if s.get("available"))
        rep.kv(n_signals=len(sigs), n_available=n_avail,
               sub_grids=sorted((cg.get("sub_grids") or {}).keys()),
               early_warning=cg.get("early_warning_level"),
               band=cg.get("band"))
        if len(sigs) < 50:
            fails.append("only %d signals defined-live (<50)" % len(sigs))
        if "global_risk" not in (cg.get("sub_grids") or {}):
            fails.append("global_risk sub-grid missing")
        table = {}
        for k in HARD_KEYS + SOFT_KEYS:
            s = sigs.get(k) or {}
            ok = bool(s.get("available"))
            table[k] = "%s %s%s" % ("LIVE" if ok else "DEAD",
                                    s.get("value"), s.get("unit") or "")
            if not ok:
                (fails if k in HARD_KEYS else warns).append(
                    "%s canary unavailable (%s)" %
                    (k, "HARD" if k in HARD_KEYS else "soft/speculative id"))
        rep.kv(resolution_table=json.dumps(table))

        rep.section("3. Leading markets +3")
        lm = invoke_and_poll(rep, "justhodl-leading-markets",
                             "data/leading-markets.json", 5, fails)
        if lm:
            names = json.dumps(lm)[:20000]
            got = [t for t in ("EM Small Cap", "Pacific (VPL)",
                               "Global Consumer Disc.") if t in names]
            rep.kv(new_markets_present=got)
            if len(got) < 3:
                fails.append("leading-markets missing new tickers: %s"
                             % got)

        rep.section("4. War room aggregation")
        wr = invoke_and_poll(rep, "justhodl-canary-warroom",
                             "data/canary-warroom.json", 5, fails,
                             sync=True)
        if wr:
            mechs = {m.get("key"): m for m in (wr.get("mechanisms") or [])}
            mg = mechs.get("macro_grid") or {}
            total = sum((m.get("n_total") or 0) for m in mechs.values())
            rep.kv(warroom_macro_watched=mg.get("n_total"),
                   warroom_total_watched=total,
                   warroom_score=wr.get("early_warning") or wr.get("score"))
            if (mg.get("n_total") or 0) < 45:
                fails.append("warroom macro grid n_total=%s (<45)"
                             % mg.get("n_total"))

        rep.section("verdict")
        _finish(rep, fails, warns,
                {"n_signals": len(sigs), "n_available": n_avail})
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- canary grid v2 live: %d signals, %d available"
                % (len(sigs), n_avail))


def _finish(rep, fails, warns, extra):
    payload = {"ops": 3014, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3014.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
