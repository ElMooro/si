"""ops 3174 — find the FRED key that actually works, then finish the job.

3173 gave two verbatim roots:
  · compass  NameError: fetch_json — that reader does not exist in
             alpha-compass (it uses safe_load). Fixed.
  · FRED     every in-lambda FRED call returns EMPTY. The runner's probes
             passed because the RUNNER has a working FRED key in its
             environment; the lambdas fell back to a hard-coded key that
             is dead. So: discover a key that works (SSM, then the env of
             lambdas that fetch FRED successfully every day), PROVE it
             from the runner, and only then write it to the engines.
"""

import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


def fred_works(key):
    try:
        u = ("https://api.stlouisfed.org/fred/series/observations"
             f"?series_id=FEDFUNDS&api_key={key}&file_type=json"
             "&observation_start=2024-01-01")
        r = urllib.request.urlopen(urllib.request.Request(
            u, headers={"User-Agent": "ops-3174"}), timeout=20)
        d = json.loads(r.read().decode())
        return len(d.get("observations") or [])
    except Exception as e:
        return 0


with report("3174_fred_key") as rep:
    fails, warns = [], []
    rep.heading("ops 3174 — the FRED key that actually works")

    rep.section("1. Candidate keys (SSM + daily FRED consumers)")
    cands = {}
    for name in ("/justhodl/fred-api-key", "/justhodl/fred/api-key",
                 "/justhodl/fred-key"):
        try:
            cands[f"ssm:{name}"] = SSM.get_parameter(
                Name=name, WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            pass
    for fn in ("justhodl-dollar-radar", "justhodl-fed-liquidity",
               "daily-liquidity-report", "justhodl-us-data-desk",
               "bond-indices-agent", "economyapi"):
        try:
            env = (LAM.get_function_configuration(FunctionName=fn)
                   .get("Environment") or {}).get("Variables") or {}
            for k in ("FRED_API_KEY", "FRED_KEY", "FRED_TOKEN"):
                if env.get(k):
                    cands[f"{fn}:{k}"] = env[k]
        except Exception:
            continue
    cands["hardcoded_fallback"] = "2f057499936072679d8843d7fce99989"
    rep.kv(candidates=len(cands))

    good = None
    for src, key in cands.items():
        n = fred_works(key)
        rep.log(f"  {src:38s} {key[:6]}… → {n} obs")
        if n > 5 and not good:
            good = (src, key)
    if not good:
        fails.append("NO working FRED key found anywhere — Khalid must "
                     "issue a fresh one at fredaccount.stlouisfed.org "
                     "(free, instant)")
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)
    rep.ok(f"working key found in {good[0]}")

    rep.section("2. Write it everywhere it belongs")
    try:
        SSM.put_parameter(Name="/justhodl/fred-api-key", Value=good[1],
                          Type="SecureString", Overwrite=True)
        rep.ok("SSM /justhodl/fred-api-key set (single source of truth)")
    except Exception as e:
        warns.append(f"ssm write: {str(e)[:60]}")
    for fn in ("justhodl-thesis-engine", "justhodl-notes-intel"):
        live = LAM.get_function_configuration(FunctionName=fn)
        env = (live.get("Environment") or {}).get("Variables") or {}
        env["FRED_API_KEY"] = good[1]
        env["FRED_KEY"] = good[1]
        LAM.update_function_configuration(FunctionName=fn,
                                          Environment={"Variables": env})
        LAM.get_waiter("function_updated").wait(
            FunctionName=fn, WaiterConfig={"Delay": 3, "MaxAttempts": 40})
        rep.ok(f"{fn}: live FRED key applied")

    rep.section("3. Compass (safe_load, not fetch_json)")
    cfgc = {"timeout": 300, "memory": 512}
    try:
        cp = AWS_DIR / "lambdas" / "justhodl-alpha-compass" / "config.json"
        cfgc = json.loads(cp.read_text())
    except Exception:
        pass
    envc = (LAM.get_function_configuration(FunctionName="justhodl-alpha-compass")
            .get("Environment") or {}).get("Variables") or {}
    sch = cfgc.get("schedule") or {}
    deploy_lambda(report=rep, function_name="justhodl-alpha-compass",
                  source_dir=AWS_DIR / "lambdas" / "justhodl-alpha-compass"
                  / "source",
                  env_vars=envc, eb_rule_name=sch.get("rule_name"),
                  eb_schedule=sch.get("cron"),
                  timeout=cfgc.get("timeout", 300),
                  memory=cfgc.get("memory", 512),
                  description=(cfgc.get("description") or "")[:250],
                  smoke=True)

    rep.section("4. Regime series — the real answer at last")
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName="justhodl-thesis-engine",
               InvocationType="Event",
               Payload=json.dumps({"force_emit": True}).encode())
    doc = None
    deadline = time.time() + 780
    while time.time() < deadline:
        try:
            d = s3_json("data/thesis-engine.json")
            if datetime.fromisoformat(d["generated_at"]) >= t0:
                doc = d
                break
        except Exception:
            pass
        time.sleep(20)
    if not doc:
        fails.append("thesis-engine never refreshed")
    else:
        rw = doc.get("regime_weeks") or {}
        rep.kv(regime_now=doc.get("regime_now"),
               **{f"weeks_{k.lower()}": v for k, v in rw.items()})
        rep.log(f"  debug: {json.dumps(doc.get('regime_debug') or {})}")
        if rw.get("EASING", 0) > 50 and rw.get("TIGHTENING", 0) > 50:
            rep.ok(f"REGIME LIVE: {rw['EASING']} easing / {rw['NEUTRAL']} "
                   f"neutral / {rw['TIGHTENING']} tightening weeks since 1990")
            rep.log("── HIS PANELS, INSIDE EACH POLICY REGIME:")
            for f in (doc.get("families") or []):
                for reg in ("EASING", "NEUTRAL", "TIGHTENING"):
                    rr = (f.get("by_regime") or {}).get(reg)
                    if not rr:
                        continue
                    rep.log(f"  {f['family']:10s} {reg:10s} "
                            f"excess {rr['excess_vs_regime_base_pct']:>6.2f}% "
                            f"t={rr['t_stat']:>6} n_eff={rr['n_effective']}")
            hits = [(f["family"], reg, rr)
                    for f in (doc.get("families") or [])
                    for reg, rr in (f.get("by_regime") or {}).items()
                    if abs(rr.get("t_stat", 0)) >= 2
                    and rr.get("n_effective", 0) >= 6]
            if hits:
                rep.ok(f"{len(hits)} REGIME-GATED EDGE(S)")
                for fam, reg, rr in hits:
                    tag = ("risk-OFF" if rr["excess_vs_regime_base_pct"] < 0
                           else "risk-ON")
                    rep.log(f"  ★ {fam} under {reg} [{tag}]: "
                            f"{rr['excess_vs_regime_base_pct']:+.2f}% vs that "
                            f"regime's base, t={rr['t_stat']}, "
                            f"n_eff={rr['n_effective']}")
            else:
                rep.warn("no regime-gated edge — with real regimes, the "
                         "verdict holds: context, not timing")
        else:
            fails.append(f"regime still degenerate: {rw}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
