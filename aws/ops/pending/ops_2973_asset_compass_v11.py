#!/usr/bin/env python3
"""ops 2973 -- Asset Compass v1.1: the exponential upgrade Khalid asked
for, without scrapping v1.0. Universe 18 -> 31 (junk bonds HYG + LQD/EMB/
MUB credit sleeve, AMLP midstream, EWJ/FXI/INDA international, DBA/URA/
UNG/PPLT commodities, SOL). New per-asset layers, all deterministic and
zero-LLM: (a) WHY/WHY-NOT reads generated from the engine's own numbers
(cash hurdle, asymmetry+gate, trend, duration-vs-rate-path, gold
real-rate mechanism, OAS spread cushion, correlation, structural flags);
(b) HORIZON with its mechanism (duration immunization / premium
realization / spread cycle / halving cycle / roll decay); (c) 90d
correlation-to-SPY + a live Diversifiers board; (d) credit ER = carry -
published loss assumption - duration x curve shift, with FRED OAS level
+ 10y percentile context; (e) excess-vs-cash and premium-per-unit-of-
downside on every modeled asset. UNG is hard-barred from ACTIONABLE
(structural contango decay), USO carries ROLL_DRAG.

Sequence: (0) probes; (1) wait for the parallel deploy-lambdas code-only
update (env-preserving; commit must NOT carry [skip-deploy]); (2) invoke
synchronously; (3) hard-verify schema 1.1 -- every new invariant plus
every v1.0 invariant (acid-test gold beta, survival gate, honest
er=None); (4) live page v2 check (Diversifiers section + public JSON).
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
OUT_KEY = "data/asset-compass.json"
PAGE = "https://justhodl.ai/asset-compass.html"
VALID_BO = {"SQUEEZE", "COILED", "BREAKOUT", "EXTENDED", "TRENDING", "NONE"}
EXPECTED_N = 31


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2973",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def fail(rep, fails, msg):
    fails.append(msg)
    rep.fail(msg)


def main():
    fails, warns = [], []
    hl = {}
    with report("2973_asset_compass_v11") as rep:

        rep.section("1. Wait for the parallel deploy-lambdas code update")
        fresh = False
        for _ in range(45):
            cfg = LAM.get_function_configuration(FunctionName=FN)
            lm = datetime.fromisoformat(
                cfg["LastModified"].replace("+0000", "+00:00"))
            age = (datetime.now(timezone.utc) - lm).total_seconds()
            if cfg.get("LastUpdateStatus") == "Successful" and age < 720:
                env_n = len((cfg.get("Environment") or {})
                            .get("Variables") or {})
                rep.kv(code_sha=cfg["CodeSha256"][:12],
                       deploy_age_s=int(age), env_var_count=env_n)
                if env_n < 3:
                    fail(rep, fails, "env looks nuked post-deploy: %d vars"
                         % env_n)
                fresh = True
                break
            time.sleep(8)
        if not fresh:
            fail(rep, fails, "no fresh deploy within ~6min -- [skip-deploy]"
                 " leaked into the commit?")
        if fails:
            _write(rep, fails, warns, hl)
            return

        rep.section("2. Synchronous run")
        t0 = time.time()
        resp = LAM.invoke(FunctionName=FN, Payload=b"{}")
        body = json.loads(resp["Payload"].read() or b"{}")
        rep.kv(invoke_seconds=round(time.time() - t0, 1),
               status=resp.get("StatusCode"),
               fn_error=resp.get("FunctionError"),
               body=json.dumps(body)[:200])
        if resp.get("FunctionError"):
            fail(rep, fails, "invoke FunctionError: %s"
                 % json.dumps(body)[:300])
            _write(rep, fails, warns, hl)
            return

        rep.section("3. Hard verify schema 1.1")
        d = s3_json(OUT_KEY)
        age_min = (datetime.now(timezone.utc) - datetime.fromisoformat(
            d.get("generated_at", "").replace("Z", "+00:00"))
        ).total_seconds() / 60.0
        assets = d.get("assets") or []
        amap = {a.get("ticker"): a for a in assets}
        rep.kv(schema=d.get("schema_version"), age_min=round(age_min, 1),
               assets_n=len(assets),
               elapsed_engine_s=d.get("elapsed_s"))
        if d.get("schema_version") != "1.1":
            fail(rep, fails, "schema %r != 1.1" % d.get("schema_version"))
        if age_min > 10:
            fail(rep, fails, "doc stale: %.1f min" % age_min)
        if len(assets) != EXPECTED_N:
            fail(rep, fails, "assets %d != %d" % (len(assets), EXPECTED_N))

        priced = [a for a in assets if a.get("price") is not None
                  or a.get("ticker") == "CASH"]
        modeled = [a for a in assets if a.get("er_1y_pct") is not None]
        hl["priced_n"] = len(priced)
        hl["er_modeled_n"] = len(modeled)
        rep.kv(priced=len(priced), er_modeled=len(modeled))
        if len(priced) < 27:
            fail(rep, fails, "only %d priced" % len(priced))
        if len(modeled) < 17:
            fail(rep, fails, "only %d ER-modeled (expect >=17 of 20)"
                 % len(modeled))

        # credit sleeve
        hyg = amap.get("HYG") or {}
        hc = hyg.get("er_components") or {}
        hl["hyg_er"] = hyg.get("er_1y_pct")
        hl["hyg_carry"] = hc.get("carry_ttm_yield_pct")
        hl["hyg_oas_pctile"] = hc.get("oas_pctile_10y")
        if hyg.get("er_1y_pct") is None:
            fail(rep, fails, "HYG (junk bonds) not ER-modeled")
        else:
            for k in ("carry_ttm_yield_pct", "credit_loss_assumption_pct",
                      "duration_effect_pct"):
                if hc.get(k) is None:
                    fail(rep, fails, "HYG components missing %s" % k)
            if not -5.0 <= hyg["er_1y_pct"] <= 15.0:
                fail(rep, fails, "HYG ER out of band: %r"
                     % hyg["er_1y_pct"])
        cc = d.get("credit_context") or {}
        if not any((v or {}).get("pctile_10y") is not None
                   for v in cc.values()):
            fail(rep, fails, "credit_context missing OAS percentile")
        for tk in ("LQD", "EMB", "MUB", "AMLP"):
            if (amap.get(tk) or {}).get("er_1y_pct") is None:
                warns.append("%s not ER-modeled (yield fetch empty?)" % tk)

        # hurdle + excess
        rf = (d.get("hurdle") or {}).get("cash_rf_pct")
        if not isinstance(rf, (int, float)):
            fail(rep, fails, "hurdle.cash_rf_pct missing")
        n_ex = sum(1 for a in modeled
                   if a.get("excess_vs_cash_pp") is not None)
        rep.kv(rf=rf, excess_stamped=n_ex)
        if n_ex < len(modeled) - 2:
            fail(rep, fails, "excess_vs_cash stamped on only %d/%d modeled"
                 % (n_ex, len(modeled)))

        # horizons + reads on every asset
        no_hz = [a["ticker"] for a in assets
                 if not ((a.get("horizon") or {}).get("hold"))]
        if no_hz:
            fail(rep, fails, "assets missing horizon: %s" % no_hz)
        no_rd = [a["ticker"] for a in assets
                 if not ((a.get("read") or {}).get("net"))]
        if no_rd:
            fail(rep, fails, "assets missing read.net: %s" % no_rd)
        with_lines = sum(1 for a in assets
                         if (a.get("read") or {}).get("bull")
                         or (a.get("read") or {}).get("bear"))
        rep.kv(reads_with_lines=with_lines)
        if with_lines < 25:
            fail(rep, fails, "only %d assets have bull/bear lines"
                 % with_lines)

        # correlations
        n_corr = sum(1 for a in assets
                     if a.get("corr_spy_90d") is not None)
        rep.kv(corr_stamped=n_corr,
               spy_corr=(amap.get("SPY") or {}).get("corr_spy_90d"))
        if n_corr < 25:
            fail(rep, fails, "corr_spy_90d on only %d assets" % n_corr)
        if (amap.get("SPY") or {}).get("corr_spy_90d") != 1.0:
            fail(rep, fails, "SPY self-corr != 1.0")
        if "diversifiers" not in (d.get("boards") or {}):
            fail(rep, fails, "boards.diversifiers missing")

        # decay invariants
        ung = amap.get("UNG") or {}
        uflags = " ".join(ung.get("flags") or [])
        if "STRUCTURAL_DECAY" not in uflags:
            fail(rep, fails, "UNG missing STRUCTURAL_DECAY flag")
        if (ung.get("asym") or {}).get("status") == "ACTIONABLE":
            fail(rep, fails, "INVARIANT BROKEN: UNG is ACTIONABLE")
        if "ROLL_DRAG" not in " ".join((amap.get("USO") or {})
                                       .get("flags") or []):
            fail(rep, fails, "USO missing ROLL_DRAG flag")

        # crypto: BTC/ETH hard, SOL soft
        for tk in ("BTC", "ETH"):
            if not (amap.get(tk) or {}).get("price"):
                fail(rep, fails, "%s unpriced" % tk)
        if not (amap.get("SOL") or {}).get("price"):
            warns.append("SOL unpriced (all three crypto sources failed "
                         "for it) -- soft-fail, row flags NO_DATA")
        elif "LOW_N" not in " ".join((amap.get("SOL") or {})
                                     .get("flags") or []):
            fail(rep, fails, "SOL missing LOW_N flag")

        # v1.0 invariants must still hold
        b = d.get("betas") or {}
        hl["gold_beta"] = b.get("gold_vs_real_rate_pct_per_100bp")
        if not (isinstance(hl["gold_beta"], (int, float))
                and hl["gold_beta"] < 0
                and (b.get("gold_beta_obs") or 0) >= 250):
            fail(rep, fails, "acid test broken: gold beta %r obs %r"
                 % (hl["gold_beta"], b.get("gold_beta_obs")))
        for a in assets:
            st = (a.get("asym") or {}).get("status")
            tr = (a.get("trend") or {}).get("label")
            if st == "ACTIONABLE" and tr == "DOWNTREND" \
                    and not a.get("structural"):
                fail(rep, fails, "survival-gate invariant broken: %s"
                     % a["ticker"])
            bo = (a.get("breakout") or {}).get("state")
            if bo and bo not in VALID_BO:
                fail(rep, fails, "invalid breakout state %r on %s"
                     % (bo, a["ticker"]))
        for tk in ("DBC", "USO", "CPER", "BTC", "ETH", "UNG", "DBA",
                   "URA", "PPLT", "SOL"):
            if (amap.get(tk) or {}).get("er_1y_pct") is not None:
                fail(rep, fails, "honesty broken: %s has fabricated ER"
                     % tk)

        boards = d.get("boards") or {}
        hl["er_top3"] = (boards.get("er_ranking") or [])[:3]
        hl["diversifiers"] = boards.get("diversifiers")
        hl["asym_top3"] = (boards.get("asymmetry_ranking") or [])[:3]
        hl["hyg_read"] = (amap.get("HYG") or {}).get("read")

        rep.section("4. Live page v2")
        page_ok = False
        for _ in range(30):
            try:
                st, htmlb = http_get(PAGE + "?v=%d" % int(time.time()))
                page_ok = (st == 200
                           and "Diversifiers · Right Now" in htmlb
                           and "clsfilter" in htmlb
                           and "toggleRead" in htmlb)
                if page_ok:
                    break
            except Exception:
                pass
            time.sleep(10)
        rep.kv(page_v2_live=page_ok)
        if not page_ok:
            fail(rep, fails, "page v2 markers never appeared live "
                 "(pages deploy failed?)")
        try:
            st, pj = http_get("https://justhodl.ai/data/asset-compass.json"
                              "?t=%d" % int(time.time()))
            if json.loads(pj).get("schema_version") != "1.1":
                fail(rep, fails, "public JSON not yet 1.1 (CDN cache?)")
        except Exception as e:
            warns.append("public JSON fetch flaky: %s" % str(e)[:80])

        if not fails:
            rep.ok("v1.1 LIVE: %d assets / %d modeled; HYG ER %s%% "
                   "(carry %s, OAS pctile %s); reads+horizons on all; "
                   "corr on %d; UNG barred; page v2 live"
                   % (len(assets), len(modeled), hl.get("hyg_er"),
                      hl.get("hyg_carry"), hl.get("hyg_oas_pctile"),
                      n_corr))
        _write(rep, fails, warns, hl)


def _write(rep, fails, warns, hl):
    out = {"ops": 2973, "function": FN, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "ts": datetime.now(timezone.utc).isoformat()}
    out.update(hl)
    rp = AWS_DIR / "ops" / "reports" / "2973.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    rep.log("report written: %s" % rp)
    if fails:
        sys.exit(1)


main()
sys.exit(0)
