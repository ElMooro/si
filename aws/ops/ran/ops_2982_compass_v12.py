#!/usr/bin/env python3
"""ops 2982 -- ASSET COMPASS v1.2: forecast ledger (monthly ER vectors,
edge-accuracy-style grading at 12m -- the credibility unlock), 31x31
correlation matrix with greedy clusters + most-diversifying pairs, and
the deterministic scenario table (+/-100bp, recession, inflation shock)
from empirical 3y betas (rate/BEI/SPY via the same OLS as the gold acid
test). v1.2 block is failure-isolated: it can never kill v1.1 output.

Verify: race-safe deploy wait; invoke; schema_version 1.2; ledger
created at data/compass-forecast-ledger.json with vintage #1 (>=15
assets, er+price each) and WARMING_UP grading eta ~+360d; correlations
matrix square/symmetric with unit diagonal over >=26 tickers; factor
betas sane (TLT rate beta in [-28,-8], SPY spy_beta ~1); scenario table
covers >=22 assets with TLT +100bp in [-28,-8] and SPY recession in
[-35,-15]; page serves the three new sections live (runner-side).
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=310, connect_timeout=10,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-asset-compass"


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2982",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def main():
    fails, warns = [], []
    hl = {}
    with report("2982_compass_v12") as rep:

        rep.section("1. Race-safe deploy wait")
        time.sleep(75)
        fresh = False
        for _ in range(50):
            cfg = LAM.get_function_configuration(FunctionName=FN)
            lm = datetime.fromisoformat(
                cfg["LastModified"].replace("+0000", "+00:00"))
            age = (datetime.now(timezone.utc) - lm).total_seconds()
            if cfg.get("LastUpdateStatus") == "Successful" \
                    and age < 1800:
                env_n = len((cfg.get("Environment") or {})
                            .get("Variables") or {})
                rep.kv(deploy_age_s=int(age), env_vars=env_n)
                if env_n < 3:
                    fails.append("env nuked: %d vars" % env_n)
                fresh = True
                break
            time.sleep(8)
        if not fresh:
            fails.append("no successful deploy in window")
        if fails:
            _write(rep, fails, warns, hl)
            return

        rep.section("2. Invoke (full compass run)")
        t0 = time.time()
        resp = LAM.invoke(FunctionName=FN, Payload=b"{}")
        body = json.loads(resp["Payload"].read() or b"{}")
        rep.kv(invoke_seconds=round(time.time() - t0, 1),
               fn_error=resp.get("FunctionError"),
               body=json.dumps(body)[:200])
        if resp.get("FunctionError"):
            fails.append("invoke error: %s" % json.dumps(body)[:300])
            _write(rep, fails, warns, hl)
            return

        rep.section("3. Doc verify")
        d = s3_json("data/asset-compass.json")
        rep.kv(schema=d.get("schema_version"),
               assets=len(d.get("assets") or []),
               doc_warns=json.dumps(d.get("warns"))[:250])
        hl["schema"] = d.get("schema_version")
        if d.get("schema_version") != "1.2":
            fails.append("schema_version %s != 1.2"
                         % d.get("schema_version"))
        for w in (d.get("warns") or []):
            if "v12" in str(w):
                fails.append("v12 block warned inside engine: %s" % w)

        co = d.get("correlations") or {}
        tks = co.get("tickers") or []
        M = co.get("matrix") or []
        rep.kv(corr_tickers=len(tks),
               clusters=len(co.get("clusters") or []),
               div_pairs=json.dumps(
                   co.get("most_diversifying_pairs"))[:200])
        hl["corr_tickers"] = len(tks)
        hl["clusters_n"] = len(co.get("clusters") or [])
        hl["top_div_pair"] = (co.get("most_diversifying_pairs")
                              or [{}])[0]
        if len(tks) < 26 or len(M) != len(tks):
            fails.append("matrix shape off: %d tickers, %d rows"
                         % (len(tks), len(M)))
        else:
            bad_sym = 0
            for i in range(0, len(tks), 5):
                if abs((M[i][i] or 0) - 1.0) > 1e-6:
                    bad_sym += 1
                for j in range(0, len(tks), 7):
                    if M[i][j] != M[j][i]:
                        bad_sym += 1
            if bad_sym:
                fails.append("matrix not symmetric/unit-diag "
                             "(%d spot fails)" % bad_sym)

        fb = d.get("factor_betas") or {}
        tlt = fb.get("TLT") or {}
        spy = fb.get("SPY") or {}
        rep.kv(tlt_beta=json.dumps(tlt)[:140],
               spy_beta=json.dumps(spy)[:140])
        hl["tlt_rate_beta"] = tlt.get("rate_beta_pct_per_100bp")
        hl["spy_spy_beta"] = spy.get("spy_beta")
        if not (-28 <= (tlt.get("rate_beta_pct_per_100bp") or 0) <= -8):
            fails.append("TLT rate beta implausible: %s"
                         % tlt.get("rate_beta_pct_per_100bp"))
        if abs((spy.get("spy_beta") or 0) - 1.0) > 0.05:
            fails.append("SPY self-beta != 1: %s" % spy.get("spy_beta"))

        sc = (d.get("scenarios") or {}).get("assets") or {}
        rep.kv(scenario_assets=len(sc),
               tlt_scen=json.dumps(sc.get("TLT"))[:160],
               spy_scen=json.dumps(sc.get("SPY"))[:160])
        hl["scenario_assets"] = len(sc)
        hl["tlt_scenarios"] = sc.get("TLT")
        hl["spy_scenarios"] = sc.get("SPY")
        if len(sc) < 22:
            fails.append("scenario table thin: %d assets" % len(sc))
        t = sc.get("TLT") or {}
        s_ = sc.get("SPY") or {}
        if not (-28 <= (t.get("plus_100bp_pct") or 0) <= -8):
            fails.append("TLT +100bp scenario off: %s"
                         % t.get("plus_100bp_pct"))
        if not (-35 <= (s_.get("recession_pct") or 0) <= -15):
            fails.append("SPY recession scenario off: %s"
                         % s_.get("recession_pct"))

        led = d.get("forecast_ledger") or {}
        rep.kv(ledger=json.dumps(led)[:220])
        hl["ledger"] = led
        if not led or led.get("entries_n", 0) < 1:
            fails.append("forecast ledger missing vintage #1: %s"
                         % json.dumps(led)[:150])
        raw = s3_json("data/compass-forecast-ledger.json")
        e0 = (raw.get("entries") or [{}])[0]
        rep.kv(vintage1_date=e0.get("date"), vintage1_n=e0.get("n"))
        if (e0.get("n") or 0) < 15:
            fails.append("vintage #1 thin: %s assets" % e0.get("n"))
        else:
            a0 = list((e0.get("assets") or {}).values())[0]
            if not (a0.get("er_1y_pct") is not None
                    and a0.get("price")):
                fails.append("vintage rows missing er/price: %s"
                             % json.dumps(a0)[:100])

        rep.section("4. Page live (runner-side)")
        page_ok = False
        for _ in range(9):
            try:
                st, html = http_get("https://justhodl.ai/"
                                    "asset-compass.html?v=%d"
                                    % int(time.time()))
                page_ok = (st == 200 and "Scenario Table" in html
                           and "Correlation Structure" in html
                           and "Forecast Ledger" in html
                           and 'id="scen"' in html)
                if page_ok:
                    break
            except Exception:
                pass
            time.sleep(10)
        rep.kv(page_v12_live=page_ok)
        if not page_ok:
            fails.append("asset-compass.html missing v1.2 sections live")

        if not fails:
            rep.ok("COMPASS v1.2 LIVE: ledger vintage #1 (%s assets, "
                   "first grade %s) | matrix %dx%d, %d clusters, top "
                   "diversifier %s | TLT +100bp %s%%, SPY recession "
                   "%s%% | page live"
                   % (e0.get("n"),
                      led.get("first_grade_eta") or (led.get(
                          "latest_grade") or {}).get("vintage"),
                      len(tks), len(tks), hl["clusters_n"],
                      json.dumps(hl["top_div_pair"]),
                      t.get("plus_100bp_pct"), s_.get("recession_pct")))
        _write(rep, fails, warns, hl)


def _write(rep, fails, warns, hl):
    out = {"ops": 2982, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "ts": datetime.now(timezone.utc).isoformat()}
    out.update(hl)
    rp = AWS_DIR / "ops" / "reports" / "2982.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    if fails:
        sys.exit(1)


main()
sys.exit(0)
