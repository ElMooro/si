#!/usr/bin/env python3
"""ops 2964 -- Dollar Radar v3: BBDXY replica (Bloomberg Dollar Spot,
back-engineered) + the 3 missing constituents (AUD, SGD, TWD).

Deploys justhodl-dollar-radar from the repo source (env preserved from the
live config), fires a synchronous run, then hard-verifies the published
data/dollar-radar.json: schema 3.0, bbdxy block available with all 12
constituents, weights sum, fresh timestamp, finite levels/changes, series
depth, contribution attribution present, and a sanity cross-check of the
replica's 1m move against the Fed broad dollar index. Live-site checks
(CF worker path + dollar.html panel) are warn-level since pages.yml and
CDN cache lag this workflow by a couple of minutes.
"""
import json
import math
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-dollar-radar"
OUT_KEY = "data/dollar-radar.json"
EXPECTED_W = {"EUR": 29.47, "JPY": 12.38, "CAD": 11.65, "GBP": 10.27,
              "MXN": 9.62, "CNH": 7.00, "CHF": 4.47, "AUD": 4.39,
              "KRW": 3.16, "INR": 2.83, "SGD": 2.61, "TWD": 2.15}


def http_get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2964",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def main():
    fails, warns = [], []
    with report("2964_bbdxy_replica") as rep:
        rep.section("0. Preserve live env")
        cfg = LAM.get_function_configuration(FunctionName=FN)
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        rep.kv(env_keys=sorted(env.keys()), timeout=cfg.get("Timeout"),
               memory=cfg.get("MemorySize"))
        if "FRED_KEY" not in env and "FRED_API_KEY" not in env:
            warns.append("no FRED key in env (SSM/default fallback in code)")

        rep.section("1. Deploy v3 from repo source")
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=env, timeout=180, memory=256,
                      description=("Dollar Radar v3 - FRED dollar family + "
                                   "13 bilaterals + BBDXY replica "
                                   "(back-engineered Bloomberg Dollar Spot, "
                                   "12 constituents, contribution "
                                   "attribution, breadth vs synthetic DXY) "
                                   "+ canary composite + patterns."),
                      create_function_url=False, smoke=False)

        rep.section("2. Synchronous full run")
        t0 = time.time()
        resp = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                          Payload=json.dumps({"source": "ops-2964"}).encode())
        body = json.loads(resp["Payload"].read() or b"{}")
        rep.kv(invoke_seconds=round(time.time() - t0, 1),
               status=resp.get("StatusCode"),
               fn_error=resp.get("FunctionError"),
               body_status=body.get("statusCode"))
        if resp.get("FunctionError") or body.get("statusCode") != 200:
            fails.append("invoke failed: %s" % json.dumps(body)[:400])

        rep.section("3. Verify published JSON")
        d = json.loads(S3.get_object(Bucket=BUCKET,
                                     Key=OUT_KEY)["Body"].read())
        age_min = None
        try:
            gen = datetime.fromisoformat(d.get("generated_at", ""))
            age_min = (datetime.now(timezone.utc) - gen).total_seconds() / 60
        except Exception:
            pass
        rep.kv(schema=d.get("schema_version"), age_min=round(age_min or -1, 1),
               bilaterals=len(d.get("bilaterals") or []),
               regime=d.get("regime"))
        if d.get("schema_version") != "3.0":
            fails.append("schema_version != 3.0")
        if age_min is None or age_min > 15:
            fails.append("output stale (age_min=%s)" % age_min)
        bils = {b.get("currency") for b in d.get("bilaterals") or []}
        for c in ("AUD", "SGD", "TWD"):
            if c not in bils:
                fails.append("bilateral %s missing" % c)
        rep.kv(bilateral_ccys=sorted(bils))

        bb = d.get("bbdxy") or {}
        rep.kv(bbdxy_available=bb.get("available"), level=bb.get("level"),
               as_of=bb.get("as_of"), missing=bb.get("missing"),
               weights_effective=bb.get("weights_effective"))
        if not bb.get("available"):
            fails.append("bbdxy block not available: %s" % bb.get("note"))
        else:
            cons = bb.get("constituents") or []
            got_w = {c["currency"]: c["target_weight_pct"] for c in cons}
            if got_w != EXPECTED_W:
                fails.append("constituent weights mismatch: %s" % got_w)
            wsum = round(sum(got_w.values()), 2)
            if abs(wsum - 100.0) > 0.05:
                fails.append("weights sum %.2f != 100" % wsum)
            present = [c for c in cons if c.get("present")]
            rep.kv(constituents=len(cons), present=len(present),
                   weights_sum=wsum)
            if len(present) < 11:
                fails.append("only %d/12 constituents live" % len(present))
            for f in ("level", "chg_1m_pct", "chg_1y_pct"):
                v = bb.get(f)
                if v is None or not math.isfinite(float(v)):
                    fails.append("bbdxy.%s not finite: %s" % (f, v))
            if len(bb.get("series") or []) < 200:
                fails.append("bbdxy.series too short: %d"
                             % len(bb.get("series") or []))
            ds = (bb.get("dxy_synth") or {}).get("series") or []
            if len(ds) < 200:
                fails.append("dxy_synth.series too short: %d" % len(ds))
            n_contrib = sum(1 for c in cons
                            if c.get("contrib_1m_pp") is not None)
            rep.kv(contrib_rows_1m=n_contrib,
                   breadth_1m_pp=bb.get("breadth_spread_1m_pp"),
                   breadth_verdict=bb.get("breadth_verdict"),
                   vs_fed_broad_1m_pp=bb.get("vs_fed_broad_1m_pp"))
            if n_contrib < 11:
                fails.append("contribution attribution incomplete "
                             "(%d/12)" % n_contrib)
            # attribution must reconstruct the index move (log-additive)
            c1m = bb.get("chg_1m_pct")
            if c1m is not None and n_contrib >= 11:
                s_pp = sum(c.get("contrib_1m_pp") or 0.0 for c in cons)
                log_idx = math.log(1 + c1m / 100.0) * 100.0
                rep.kv(contrib_sum_pp=round(s_pp, 3),
                       log_index_1m_pp=round(log_idx, 3))
                if abs(s_pp - log_idx) > 0.30:
                    fails.append("contributions do not reconstruct the "
                                 "1m move: sum=%.3f vs log-index=%.3f"
                                 % (s_pp, log_idx))
            # sanity vs Fed broad: different baskets, but a wild gap means
            # a sign/inversion bug somewhere
            vfb = bb.get("vs_fed_broad_1m_pp")
            if vfb is not None and abs(vfb) > 5.0:
                fails.append("replica vs Fed broad 1m gap %.2fpp -- "
                             "inversion bug suspected" % vfb)

        rep.section("4. Live-path checks (warn-level)")
        try:
            live = json.loads(http_get(
                "https://justhodl.ai/data/dollar-radar.json?ops2964=%d"
                % int(time.time())))
            ok = (live.get("bbdxy") or {}).get("available") is True
            rep.kv(cf_worker_serves_v3=ok,
                   cf_schema=live.get("schema_version"))
            if not ok:
                warns.append("CF worker path not yet serving v3 (cache lag)")
        except Exception as e:
            warns.append("CF worker fetch: %s" % e)
        html_ok = False
        for _ in range(6):
            try:
                page = http_get("https://justhodl.ai/dollar.html?o=%d"
                                % int(time.time()))
                if "BBDXY Replica" in page:
                    html_ok = True
                    break
            except Exception:
                pass
            time.sleep(25)
        rep.kv(dollar_html_panel_live=html_ok)
        if not html_ok:
            warns.append("dollar.html panel not live yet (pages.yml lag) -- "
                         "re-check in a few minutes")

        rep.section("verdict")
        rep.kv(fails=fails, warns=warns)
        out = {"ops": 2964, "function": FN, "fails": fails, "warns": warns,
               "verdict": "PASS" if not fails else "FAIL",
               "bbdxy_level": bb.get("level"),
               "bbdxy_chg_1m_pct": bb.get("chg_1m_pct"),
               "breadth_spread_1m_pp": bb.get("breadth_spread_1m_pp"),
               "constituents_present": len([c for c in
                                            (bb.get("constituents") or [])
                                            if c.get("present")]),
               "ts": datetime.now(timezone.utc).isoformat()}
        rp = AWS_DIR / "ops" / "reports" / "2964.json"
        rp.parent.mkdir(parents=True, exist_ok=True)
        rp.write_text(json.dumps(out, indent=1))
        rep.log("report written: %s" % rp)
        if fails:
            rep.log("FAILED: %s" % "; ".join(fails))
            sys.exit(1)
        rep.log("PASS -- BBDXY replica live with full attribution")


main()
sys.exit(0)
