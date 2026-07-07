#!/usr/bin/env python3
"""ops 2968 -- HOTFIX: restore justhodl-equity-research's env bundle and
prove Industry Compass (ops 2967) in production.

Incident: the 2967 deploy left equity-research with only 2 env vars. Root
cause: deploy-lambdas' config path read the existing env with
`... || echo "{}"`, so a transient read failure was treated as an empty
env and the MERGE became a REPLACE with the config-inherited vars. The
workflow is hardened in this same commit (read failure now skips the
config update and preserves env). This script:

  (0) reports the surviving env keys (facts before repair), pulls the
      standard secrets bundle from donor justhodl-confluence-meta;
  (1) fill-gaps merge (survivors win on conflicts), update config, wait,
      re-read, and hard-assert the bundle is whole (ANTHROPIC + FMP +
      POLYGON present, >= 6 vars);
  (2) re-run the full ops-2967 verification: force-refresh ORCL through
      the Lambda URL (async-cold aware) and hard-verify the
      industry_compass block + v2.0 regression (technicals/quant_risk
      available, Claude synthesis live -- which also proves the restored
      ANTHROPIC key end-to-end).
Every failure is rep.fail()'d immediately so run-ops goes red on any
early return (fixes the 2967 cosmetic gap where the run showed green on
a FAIL report).
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
                                 retries={"max_attempts": 2}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-equity-research"
DONOR = "justhodl-confluence-meta"
TICKER = "ORCL"
DOC_KEY = "equity-research/%s.json" % TICKER
BUNDLE_KEYS = ("ANTHROPIC_API_KEY", "FMP_KEY", "FRED_KEY", "FRED_API_KEY",
               "POLYGON_KEY", "POLYGON_API_KEY", "TELEGRAM_BOT_TOKEN",
               "TELEGRAM_CHAT_ID")
VERDICTS = {"LAGGARD_CATCHUP", "PARTIAL", "NONE"}
COMP_KEYS = {"div_yield_pct", "net_buyback_yield_pct", "inflation_pct",
             "real_eps_growth_pct", "pe_reversion_pct", "pe_now",
             "pe_median_10y"}


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def http_get(url, timeout=290):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2968",
                                               "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def env_of(fn):
    cfg = LAM.get_function_configuration(FunctionName=fn)
    return (cfg.get("Environment") or {}).get("Variables") or {}


def fail(rep, fails, msg):
    fails.append(msg)
    rep.fail(msg)


def main():
    fails, warns = [], []
    hl = {}
    with report("2968_er_env_restore") as rep:

        rep.section("0. Facts: surviving env + donor bundle")
        cur = env_of(FN)
        rep.kv(surviving_keys=sorted(cur.keys()), surviving_n=len(cur))
        hl["env_keys_before"] = sorted(cur.keys())
        donor = env_of(DONOR)
        pulled = {k: v for k, v in donor.items() if k in BUNDLE_KEYS and v}
        rep.kv(donor_keys_pulled=sorted(pulled.keys()))
        if "ANTHROPIC_API_KEY" not in pulled and \
                "ANTHROPIC_API_KEY" not in cur:
            fail(rep, fails, "donor %s lacks ANTHROPIC_API_KEY and it did "
                             "not survive -- cannot restore synthesis" % DONOR)
            _write(rep, fails, warns, hl)
            return

        rep.section("1. Fill-gaps merge + config update + re-read assert")
        merged = dict(pulled)
        merged.update(cur)          # survivors win on conflicts
        LAM.update_function_configuration(
            FunctionName=FN, Environment={"Variables": merged})
        LAM.get_waiter("function_updated").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2, "MaxAttempts": 30})
        after = env_of(FN)
        hl["env_keys_after"] = sorted(after.keys())
        rep.kv(env_keys_after=sorted(after.keys()), env_n_after=len(after))
        if len(after) < 6:
            fail(rep, fails, "bundle still thin after restore: %d vars"
                 % len(after))
        for k in ("ANTHROPIC_API_KEY", "FMP_KEY"):
            if not after.get(k):
                fail(rep, fails, "restored env missing %s" % k)
        if not (after.get("POLYGON_API_KEY") or after.get("POLYGON_KEY")):
            fail(rep, fails, "restored env missing POLYGON key")
        if fails:
            _write(rep, fails, warns, hl)
            return

        rep.section("2. Re-verify Industry Compass on %s (fresh)" % TICKER)
        url = LAM.get_function_url_config(FunctionName=FN)["FunctionUrl"]
        start = time.time()
        d = {}
        try:
            d = json.loads(http_get(url + "?ticker=%s&refresh=1" % TICKER))
        except Exception as e:
            warns.append("direct URL call raised (%s) -- S3 poll fallback"
                         % str(e)[:80])
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
                            g.replace("Z", "+00:00")).timestamp() \
                            > start - 30:
                        d = d2
                        break
                except Exception:
                    pass
        gen_s = int(time.time() - start)
        hl["gen_seconds"] = gen_s
        rep.kv(gen_seconds=gen_s, generated_at=d.get("generated_at"),
               schema=d.get("schema_version"))
        if not d.get("generated_at"):
            fail(rep, fails, "no fresh %s document produced" % TICKER)
            _write(rep, fails, warns, hl)
            return

        if d.get("schema_version") != "2.1":
            fail(rep, fails, "schema_version %r != 2.1"
                 % d.get("schema_version"))
        ic = d.get("industry_compass") or {}
        hl["ticker"] = TICKER
        if not ic.get("available"):
            fail(rep, fails, "industry_compass unavailable: %s"
                 % str(ic.get("error") or ic)[:160])
            _write(rep, fails, warns, hl)
            return

        hl["fmp_industry"] = ic.get("fmp_industry")
        hl["finviz_industry"] = ic.get("finviz_industry")
        hl["match_confidence"] = ic.get("match_confidence")
        if not ic.get("finviz_industry") or (ic.get("match_confidence")
                                             or 0) < 0.5:
            fail(rep, fails, "industry match failed: fmp=%r finviz=%r "
                 "conf=%r" % (ic.get("fmp_industry"),
                              ic.get("finviz_industry"),
                              ic.get("match_confidence")))

        sp = ic.get("stock_perf") or {}
        ip = ic.get("industry_perf") or {}
        gaps = ic.get("perf_gap_pp") or {}
        missing_sp = [w for w in ("perf_m", "perf_q", "perf_h", "perf_y")
                      if sp.get(w) is None]
        if missing_sp:
            fail(rep, fails, "stock_perf windows missing: %s" % missing_sp)
        if ip.get("perf_h") is None:
            fail(rep, fails, "industry_perf.perf_h missing")
        if len([v for v in gaps.values() if v is not None]) < 3:
            fail(rep, fails, "perf_gap_pp too sparse: %s" % gaps)
        hl["gap_h_pp"] = gaps.get("perf_h")
        hl["gap_q_pp"] = gaps.get("perf_q")

        er = ic.get("expected_return_1y") or {}
        erv = er.get("er_1y_pct")
        comp = er.get("components") or {}
        hl["er_1y_pct"] = erv
        if not isinstance(erv, (int, float)) or not -30.0 <= erv <= 60.0:
            fail(rep, fails, "er_1y_pct out of band: %r" % erv)
        missing_c = sorted(COMP_KEYS - set(comp.keys()))
        if missing_c:
            fail(rep, fails, "er components missing: %s" % missing_c)
        ci = comp.get("inflation_pct")
        if not isinstance(ci, (int, float)) or not 0.0 <= ci <= 8.0:
            fail(rep, fails, "component inflation out of band: %r" % ci)
        if comp.get("flag"):
            fail(rep, fails, "module fell back instead of consuming "
                 "asset-compass macro: %s" % comp.get("flag"))
        hl["components"] = {k: comp.get(k) for k in sorted(COMP_KEYS)}

        sc = ic.get("laggard_catchup") or {}
        hl["screen_verdict"] = sc.get("verdict")
        if sc.get("verdict") not in VERDICTS:
            fail(rep, fails, "screen verdict %r invalid (N/A = silent "
                 "degrade)" % sc.get("verdict"))
        if len(sc.get("why") or []) < 4:
            fail(rep, fails, "screen why-lines < 4")
        if not sc.get("thresholds"):
            fail(rep, fails, "screen thresholds not published")

        rs = ic.get("rate_sensitivity") or {}
        hl["rate_read"] = rs.get("read")
        if rs.get("duration_bucket") not in ("LONG", "MID", "SHORT"):
            fail(rep, fails, "rate duration_bucket %r invalid"
                 % rs.get("duration_bucket"))
        if rs.get("rate_env_next_12m") not in ("HIGHER", "LOWER", "FLAT"):
            fail(rep, fails, "rate env %r invalid" %
                 rs.get("rate_env_next_12m"))
        if not (rs.get("read") or "").strip():
            fail(rep, fails, "rate_sensitivity.read empty")

        tech = d.get("technicals") or {}
        qr = d.get("quant_risk") or {}
        es = d.get("executive_summary") or ""
        hl["exec_summary_len"] = len(es)
        hl["claude_model"] = (d.get("meta") or {}).get("claude_model")
        if not tech.get("available"):
            fail(rep, fails, "REGRESSION: technicals unavailable")
        if not qr.get("available"):
            fail(rep, fails, "REGRESSION: quant_risk unavailable")
        if len(es) < 200:
            fail(rep, fails, "REGRESSION: Claude synthesis short/absent "
                 "(%d chars) -- restored ANTHROPIC key not proven" % len(es))

        rep.kv(**{k: v for k, v in hl.items()
                  if k not in ("components", "env_keys_before",
                               "env_keys_after")})
        rep.kv(components=json.dumps(hl.get("components")))
        if not fails:
            rep.ok("env restored (%d vars) + industry_compass proven on "
                   "%s: %s vs %s (conf %s), gap_h %spp, ER %s%%, screen %s"
                   % (len(after), TICKER, hl.get("fmp_industry"),
                      hl.get("finviz_industry"), hl.get("match_confidence"),
                      hl.get("gap_h_pp"), erv, hl.get("screen_verdict")))
        _write(rep, fails, warns, hl)


def _write(rep, fails, warns, hl):
    out = {"ops": 2968, "function": FN, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "ts": datetime.now(timezone.utc).isoformat()}
    out.update(hl)
    rp = AWS_DIR / "ops" / "reports" / "2968.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    rep.log("report written: %s" % rp)


main()
sys.exit(0)
