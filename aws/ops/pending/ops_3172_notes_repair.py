"""ops 3172 — repair the four defects 3171 exposed. No hand-waving.

3171 shipped the notes engine but the run surfaced four real faults:
  1. FRED returned NOTHING inside the lambdas (ff_obs=0) — a STALE
     FRED key in the function env was overriding the good one, and the
     failure was silent. That is why 3170/3171 both reported 1,746
     all-NEUTRAL regime weeks. series_source now retries the known-good
     key on every FRED call, and this op force-sets the env.
  2. master-ranker has source but no config.json (config-orphan) →
     dep() now falls back to the LIVE function configuration.
  3. alpha-compass smoke crashed: express() takes its indexes as
     PARAMETERS and my patch referenced a handler local. notes_idx is
     now threaded properly through build_card → express.
  4. MY OWN test notes polluted his brain: the ops-3161 scale probes
     ("ops3161 scale probe …") became the LATEST note on AAPL/NVDA/MU/
     STX. Purged here, then notes-intel re-run so his real research is
     what the engines read.
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FRED_GOOD = "2f057499936072679d8843d7fce99989"

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


def s3_put(key, doc):
    S3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(doc).encode(),
                  ContentType="application/json")


def dep(rep, fn, smoke=True, invoke_async=False, extra_env=None):
    live = LAM.get_function_configuration(FunctionName=fn)
    env = (live.get("Environment") or {}).get("Variables") or {}
    env.update(extra_env or {})
    cp = AWS_DIR / "lambdas" / fn / "config.json"
    if cp.exists():
        cfg = json.loads(cp.read_text())
    else:                       # config-orphan → live configuration
        rep.log(f"  {fn}: no repo config — using live function config")
        cfg = {"timeout": live.get("Timeout", 300),
               "memory": live.get("MemorySize", 512),
               "description": live.get("Description", "")}
    sch = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name=fn,
                  source_dir=AWS_DIR / "lambdas" / fn / "source",
                  env_vars=env, eb_rule_name=sch.get("rule_name"),
                  eb_schedule=sch.get("cron"),
                  timeout=cfg.get("timeout", 300),
                  memory=cfg.get("memory", 512),
                  description=(cfg.get("description") or "")[:250],
                  smoke=smoke)
    if invoke_async:
        LAM.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")


with report("3172_notes_repair") as rep:
    fails, warns = [], []
    rep.heading("ops 3172 — repair: purge test notes, FRED key, orphan, compass")

    rep.section("1. Purge MY test notes from his brain")
    m = s3_json("data/tradingview-notes.json")
    notes = m.get("notes") or []
    before = len(notes)
    junk = ("ops3161 scale probe", "e2e-3158-", "ops3158", "scale probe ")
    clean = [n for n in notes
             if not any(j in str(n.get("text") or "") for j in junk)]
    m["notes"] = clean
    s3_put("data/tradingview-notes.json", m)
    rep.kv(notes_before=before, notes_after=len(clean),
           test_notes_purged=before - len(clean))
    if before - len(clean):
        rep.ok(f"purged {before - len(clean)} of my own probe notes — his "
               "brain now holds only HIS research")
    else:
        rep.log("no probe notes found (already clean)")

    rep.section("2. FRED key forced on the series consumers")
    for fn in ("justhodl-thesis-engine", "justhodl-notes-intel"):
        try:
            live = LAM.get_function_configuration(FunctionName=fn)
            env = (live.get("Environment") or {}).get("Variables") or {}
            old = env.get("FRED_API_KEY", "")
            env["FRED_API_KEY"] = FRED_GOOD
            env["FRED_KEY"] = FRED_GOOD
            LAM.update_function_configuration(
                FunctionName=fn, Environment={"Variables": env})
            LAM.get_waiter("function_updated").wait(
                FunctionName=fn, WaiterConfig={"Delay": 3, "MaxAttempts": 40})
            rep.ok(f"{fn}: FRED key set "
                   f"(was {'empty' if not old else old[:6] + '…'})")
        except Exception as e:
            warns.append(f"{fn} env: {str(e)[:80]}")

    rep.section("3. Redeploy the three engines (shared bundle carries the fix)")
    t0 = datetime.now(timezone.utc)
    for fn, kw in (("justhodl-alpha-compass", {"smoke": True}),
                   ("justhodl-master-ranker", {"smoke": True}),
                   ("justhodl-notes-intel",
                    {"smoke": False, "invoke_async": True})):
        try:
            dep(rep, fn, **kw)
        except Exception as e:
            fails.append(f"{fn}: {str(e)[:140]}")

    rep.section("4. Notes re-index (clean corpus)")
    idx = None
    deadline = time.time() + 620
    while time.time() < deadline:
        try:
            d = s3_json("data/notes-index.json")
            if datetime.fromisoformat(d["generated_at"]) >= t0:
                idx = d
                break
        except Exception:
            pass
        time.sleep(15)
    if idx is None:
        warns.append("notes-index did not refresh in window (next daily run "
                     "picks up the clean corpus)")
    else:
        rep.kv(notes=idx.get("n_notes"), tickers=idx.get("n_tickers"),
               llm_views=idx.get("llm_views"))
        ix = idx.get("index") or {}
        for tk in ("NVDA", "AAPL", "MU", "STX", "DXY", "SPX"):
            v = ix.get(tk)
            if v:
                rep.log(f"  {tk:6s} {v['n_notes']:>3} notes · {v['stance']:8s}"
                        f" · last {v['last_note_at']} · "
                        f"{str(v.get('latest'))[:60]}")
        rep.ok("clean index rebuilt")

    rep.section("5. Regime series — the real test of the FRED fix")
    try:
        t1 = datetime.now(timezone.utc)
        dep(rep, "justhodl-thesis-engine", smoke=False, invoke_async=True,
            extra_env={"FRED_API_KEY": FRED_GOOD})
        doc = None
        deadline = time.time() + 760
        while time.time() < deadline:
            try:
                d = s3_json("data/thesis-engine.json")
                if datetime.fromisoformat(d["generated_at"]) >= t1:
                    doc = d
                    break
            except Exception:
                pass
            time.sleep(20)
        if not doc:
            fails.append("thesis-engine never refreshed")
        else:
            rw = doc.get("regime_weeks") or {}
            rd = doc.get("regime_debug") or {}
            rep.kv(regime_now=doc.get("regime_now"),
                   **{f"weeks_{k.lower()}": v for k, v in rw.items()})
            rep.log(f"  debug: {json.dumps(rd)}")
            if rw.get("EASING", 0) > 50 and rw.get("TIGHTENING", 0) > 50:
                rep.ok("REGIME SERIES REPAIRED — the regime-gated study is "
                       "now valid")
                for f in (doc.get("families") or []):
                    for reg in ("EASING", "NEUTRAL", "TIGHTENING"):
                        r = (f.get("by_regime") or {}).get(reg)
                        if not r:
                            continue
                        rep.log(f"  {f['family']:10s} {reg:10s} excess "
                                f"{r['excess_vs_regime_base_pct']:>6.2f}% vs "
                                f"regime base  t={r['t_stat']:>6} "
                                f"n_eff={r['n_effective']}")
                hits = [(f["family"], reg, r)
                        for f in (doc.get("families") or [])
                        for reg, r in (f.get("by_regime") or {}).items()
                        if abs(r.get("t_stat", 0)) >= 2
                        and r.get("n_effective", 0) >= 6]
                if hits:
                    rep.ok(f"{len(hits)} REGIME-GATED EDGE(S) — his panels "
                           "work inside specific policy states")
                    for fam, reg, r in hits:
                        tag = ("risk-OFF" if r["excess_vs_regime_base_pct"] < 0
                               else "risk-ON")
                        rep.log(f"  ★ {fam} under {reg} [{tag}]: "
                                f"{r['excess_vs_regime_base_pct']:+.2f}% vs "
                                f"that regime's base, t={r['t_stat']}, "
                                f"n_eff={r['n_effective']}")
                else:
                    rep.warn("no regime-gated edge even with the regime "
                             "series live — the panels are context, not "
                             "timing, in every policy state")
            else:
                fails.append(f"regime STILL degenerate: {rw} debug={rd}")
    except Exception as e:
        fails.append(f"thesis-engine: {str(e)[:120]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
