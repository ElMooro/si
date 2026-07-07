#!/usr/bin/env python3
"""ops 2967 -- Industry Compass module inside justhodl-equity-research
(schema v2.1). The stock-vs-industry laggard-catchup layer Khalid asked
for: official Finviz industry join (perf gaps quantified per window),
stock-level Grinold-Kroner 12m expected return with every component
published (div yield + net buyback + inflation + real EPS growth + 25%
P/E reversion to own 10y median), the laggard-catchup asymmetry screen
(industry pumped + stock lagged + growth intact + cheaper than industry),
and rate sensitivity vs the market-implied next-12m path consumed from
the live asset-compass engine. Zero LLM cost in the module.

Sequence: (0) probe data/finviz-groups.json (>=100 industries carrying
pe + perf fields) and data/asset-compass.json macro anchors from the
runner; (1) wait for the parallel deploy-lambdas run to land the new
code (code-only, env-preserving update -- this commit must NOT carry
[skip-deploy]); (2) force-refresh ORCL through the Lambda URL
(async-cold-path aware); (3) hard-verify the industry_compass block --
match confidence, four perf windows + gaps, ER band + all components,
screen verdict + published thresholds, rate read -- plus v2.0 regression
(technicals/quant_risk still available, Claude synthesis still live).
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
FN = "justhodl-equity-research"
TICKER = "ORCL"
DOC_KEY = "equity-research/%s.json" % TICKER
VERDICTS = {"LAGGARD_CATCHUP", "PARTIAL", "NONE"}
COMP_KEYS = {"div_yield_pct", "net_buyback_yield_pct", "inflation_pct",
             "real_eps_growth_pct", "pe_reversion_pct", "pe_now",
             "pe_median_10y"}


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def http_get(url, timeout=290):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2967",
                                               "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def main():
    fails, warns = [], []
    hl = {}   # headline fields for the report json
    with report("2967_industry_compass") as rep:

        rep.section("0. Sibling-feed probes from the runner")
        try:
            fg = s3_json("data/finviz-groups.json")
            inds = fg.get("industries") or []
            with_pe = sum(1 for r in inds if r.get("pe") is not None)
            with_ph = sum(1 for r in inds if r.get("perf_h") is not None)
            rep.kv(industries=len(inds), with_pe=with_pe, with_perf_h=with_ph,
                   fg_age=fg.get("generated_at"))
            if len(inds) < 100 or with_pe < 80 or with_ph < 80:
                fails.append("finviz-groups too thin: %d inds / %d pe / %d "
                             "perf_h" % (len(inds), with_pe, with_ph))
        except Exception as e:
            fails.append("cannot read data/finviz-groups.json: %s" % e)
        try:
            ac = s3_json("data/asset-compass.json")
            mf = ac.get("macro_forward") or {}
            infl = mf.get("infl_1y_expected_pct")
            rdir = mf.get("rf_direction_next_year")
            rep.kv(ac_infl_1y=infl, ac_rf_dir=rdir)
            if not isinstance(infl, (int, float)) or rdir not in (
                    "HIGHER", "LOWER", "FLAT"):
                fails.append("asset-compass macro anchors malformed: "
                             "infl=%r dir=%r" % (infl, rdir))
        except Exception as e:
            fails.append("cannot read data/asset-compass.json: %s" % e)
        if fails:
            rep.fail("aborting before deploy-wait: %s" % fails)
            _write(rep, fails, warns, hl)
            return

        rep.section("1. Wait for the parallel deploy-lambdas code update")
        fresh = False
        for _ in range(45):
            cfg = LAM.get_function_configuration(FunctionName=FN)
            lm = datetime.fromisoformat(
                cfg["LastModified"].replace("+0000", "+00:00"))
            age = (datetime.now(timezone.utc) - lm).total_seconds()
            if cfg.get("LastUpdateStatus") == "Successful" and age < 720:
                rep.kv(code_sha=cfg["CodeSha256"][:12], deploy_age_s=int(age),
                       timeout_s=cfg.get("Timeout"),
                       memory_mb=cfg.get("MemorySize"))
                env_n = len((cfg.get("Environment") or {})
                            .get("Variables") or {})
                rep.kv(env_var_count=env_n)
                if env_n < 5:
                    fails.append("env bundle looks nuked: only %d vars "
                                 "survived the deploy" % env_n)
                fresh = True
                break
            time.sleep(8)
        if not fresh:
            fails.append("no fresh deploy within ~6min -- was [skip-deploy] "
                         "left in the commit message?")
            _write(rep, fails, warns, hl)
            return
        if fails:
            _write(rep, fails, warns, hl)
            return

        rep.section("2. Force-refresh %s via the Lambda URL" % TICKER)
        url = LAM.get_function_url_config(FunctionName=FN)["FunctionUrl"]
        start = time.time()
        d = {}
        try:
            body = http_get(url + "?ticker=%s&refresh=1" % TICKER)
            d = json.loads(body)
        except Exception as e:
            warns.append("direct URL call raised (%s) -- falling back to "
                         "S3 poll" % str(e)[:80])
        if (not d.get("industry_compass")) and (
                d.get("poll_s3_url") or d.get("status") == "generating"
                or not d.get("generated_at")):
            rep.log("  cold/async path -- polling %s" % DOC_KEY)
            for _ in range(32):
                time.sleep(9)
                try:
                    d2 = s3_json(DOC_KEY)
                    g = d2.get("generated_at", "")
                    if g and datetime.fromisoformat(
                            g.replace("Z", "+00:00")).timestamp() > start - 30:
                        d = d2
                        break
                except Exception:
                    pass
        gen_s = int(time.time() - start)
        rep.kv(gen_seconds=gen_s, generated_at=d.get("generated_at"),
               schema=d.get("schema_version"))
        hl["gen_seconds"] = gen_s
        if not d.get("generated_at"):
            fails.append("no fresh %s document produced" % TICKER)
            _write(rep, fails, warns, hl)
            return

        rep.section("3. Hard verify: industry_compass + v2.0 regression")
        if d.get("schema_version") != "2.1":
            fails.append("schema_version %r != 2.1" % d.get("schema_version"))
        ic = d.get("industry_compass") or {}
        hl["ticker"] = TICKER
        if not ic.get("available"):
            fails.append("industry_compass unavailable: %s"
                         % (ic.get("error") or ic)[:160])
            _write(rep, fails, warns, hl)
            return

        hl["fmp_industry"] = ic.get("fmp_industry")
        hl["finviz_industry"] = ic.get("finviz_industry")
        hl["match_confidence"] = ic.get("match_confidence")
        if not ic.get("finviz_industry") or (ic.get("match_confidence")
                                             or 0) < 0.5:
            fails.append("industry match failed for %s: fmp=%r finviz=%r "
                         "conf=%r" % (TICKER, ic.get("fmp_industry"),
                                      ic.get("finviz_industry"),
                                      ic.get("match_confidence")))

        sp = ic.get("stock_perf") or {}
        ip = ic.get("industry_perf") or {}
        gaps = ic.get("perf_gap_pp") or {}
        missing_sp = [w for w in ("perf_m", "perf_q", "perf_h", "perf_y")
                      if sp.get(w) is None]
        if missing_sp:
            fails.append("stock_perf windows missing: %s" % missing_sp)
        if ip.get("perf_h") is None:
            fails.append("industry_perf.perf_h missing")
        if len([w for w in gaps.values() if w is not None]) < 3:
            fails.append("perf_gap_pp too sparse: %s" % gaps)
        hl["gap_h_pp"] = gaps.get("perf_h")
        hl["gap_q_pp"] = gaps.get("perf_q")

        er = ic.get("expected_return_1y") or {}
        erv = er.get("er_1y_pct")
        comp = er.get("components") or {}
        hl["er_1y_pct"] = erv
        if not isinstance(erv, (int, float)) or not -30.0 <= erv <= 60.0:
            fails.append("er_1y_pct out of band: %r" % erv)
        missing_c = sorted(COMP_KEYS - set(comp.keys()))
        if missing_c:
            fails.append("er components missing keys: %s" % missing_c)
        ci = comp.get("inflation_pct")
        if not isinstance(ci, (int, float)) or not 0.0 <= ci <= 8.0:
            fails.append("component inflation out of band: %r" % ci)
        if comp.get("flag"):
            fails.append("module fell back instead of consuming "
                         "asset-compass macro: %s" % comp.get("flag"))
        hl["components"] = {k: comp.get(k) for k in sorted(COMP_KEYS)}

        sc = ic.get("laggard_catchup") or {}
        hl["screen_verdict"] = sc.get("verdict")
        if sc.get("verdict") not in VERDICTS:
            fails.append("screen verdict %r not in %s (N/A means the join "
                         "silently degraded)" % (sc.get("verdict"),
                                                 sorted(VERDICTS)))
        if len(sc.get("why") or []) < 4:
            fails.append("screen why-lines < 4: %s" % (sc.get("why"),))
        if not sc.get("thresholds"):
            fails.append("screen thresholds not published")

        rs = ic.get("rate_sensitivity") or {}
        hl["rate_read"] = rs.get("read")
        if rs.get("duration_bucket") not in ("LONG", "MID", "SHORT"):
            fails.append("rate duration_bucket %r invalid"
                         % rs.get("duration_bucket"))
        if rs.get("rate_env_next_12m") not in ("HIGHER", "LOWER", "FLAT"):
            fails.append("rate env %r invalid (asset-compass not consumed?)"
                         % rs.get("rate_env_next_12m"))
        if not (rs.get("read") or "").strip():
            fails.append("rate_sensitivity.read empty")

        # v2.0 regression -- the module must not have broken the desk
        tech = d.get("technicals") or {}
        qr = d.get("quant_risk") or {}
        es = d.get("executive_summary") or ""
        hl["exec_summary_len"] = len(es)
        hl["claude_model"] = (d.get("meta") or {}).get("claude_model")
        if not tech.get("available"):
            fails.append("REGRESSION: technicals unavailable")
        if not qr.get("available"):
            fails.append("REGRESSION: quant_risk unavailable")
        if len(es) < 200:
            fails.append("REGRESSION: Claude synthesis short/absent "
                         "(%d chars)" % len(es))

        rep.kv(**{k: v for k, v in hl.items() if k != "components"})
        rep.kv(components=json.dumps(hl.get("components")))
        if fails:
            for f in fails:
                rep.fail(f)
        else:
            rep.ok("industry_compass live on %s: %s vs %s (conf %s), "
                   "gap_h %spp, ER %s%%, screen %s"
                   % (TICKER, hl.get("fmp_industry"),
                      hl.get("finviz_industry"), hl.get("match_confidence"),
                      hl.get("gap_h_pp"), erv, hl.get("screen_verdict")))
        _write(rep, fails, warns, hl)


def _write(rep, fails, warns, hl):
    out = {"ops": 2967, "function": FN, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "ts": datetime.now(timezone.utc).isoformat()}
    out.update(hl)
    rp = AWS_DIR / "ops" / "reports" / "2967.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    rep.log("report written: %s" % rp)


main()
sys.exit(0)
