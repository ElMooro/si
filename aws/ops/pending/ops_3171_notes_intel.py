"""ops 3171 — KHALID'S BRAIN, WIRED INTO THE FLEET (+ regime-series fix).

His 3,573 TradingView notes were sitting in a mirror nobody read. This
op compiles them and pushes them into the engines that rank his names.

NEW ENGINE justhodl-notes-intel (daily 12:10 UTC):
  · per-note stance from a finance lexicon WITH negation handling
    ("not a buy" scores bearish, which naive lexicons get backwards)
  · per-ticker rollup: recency-weighted stance (18-month decay — a 2019
    opinion must never outrank a 2026 one), levels mentioned, themes,
    top terms, latest note
  · macro theme index for the untagged half of his brain
  · LLM leg (policy-gated, GLM): distils "Khalid's view" per top ticker;
    the deterministic index stands alone if the LLM is gated off
  → data/notes-index.json · data/notes-themes.json

WIRED INTO (his research now rides inside every ranking decision):
  · best-setups   khalid_note {n, stance, score, last, view} per setup
  · master-ranker khalid_note on every ranked name
  · alpha-compass khalid_note on express names

ALSO: thesis-engine regime series fix — 3170 shipped 1,746 all-NEUTRAL
weeks because __FF__/__BS__ were missing from a stale cache and the
failure was silent. Now force-refetched every run with debug counts.
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
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


def dep(rep, fn, donor=None, smoke=True, invoke_async=False):
    live = None
    try:
        live = LAM.get_function_configuration(FunctionName=fn)
    except Exception:
        pass
    env = ((live or {}).get("Environment") or {}).get("Variables") or {}
    if not env and donor:
        env = (LAM.get_function_configuration(FunctionName=donor)
               .get("Environment") or {}).get("Variables") or {}
        env = {k: v for k, v in env.items()
               if k in ("S3_BUCKET", "ANTHROPIC_API_KEY", "FRED_API_KEY",
                        "POLYGON_KEY")}
    env.setdefault("S3_BUCKET", BUCKET)
    cfg = json.loads((AWS_DIR / "lambdas" / fn / "config.json").read_text())
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


with report("3171_notes_intel") as rep:
    fails, warns = [], []
    rep.heading("ops 3171 — Khalid's brain wired into the fleet")

    rep.section("1. LLM policy window (restored at close)")
    mode0 = "on_demand"
    try:
        mode0 = SSM.get_parameter(Name="/justhodl/llm/mode")["Parameter"]["Value"]
        SSM.put_parameter(Name="/justhodl/llm/mode", Value="normal",
                          Type="String", Overwrite=True)
        rep.log(f"mode {mode0} → normal for this run (his explicit ask was "
                "to analyse EVERY note; restored at close)")
    except Exception as e:
        warns.append(f"ssm mode: {str(e)[:60]}")

    rep.section("2. Deploy notes-intel + run")
    t0 = datetime.now(timezone.utc)
    try:
        dep(rep, "justhodl-notes-intel", donor="justhodl-premortem-engine",
            smoke=False, invoke_async=True)
    except Exception as e:
        fails.append(f"notes-intel deploy: {str(e)[:150]}")
    idx = None
    deadline = time.time() + 620
    while time.time() < deadline and not fails:
        try:
            d = s3_json("data/notes-index.json")
            if datetime.fromisoformat(d["generated_at"]) >= t0:
                idx = d
                break
        except Exception:
            pass
        time.sleep(15)
    if idx is None and not fails:
        fails.append("notes-index.json never freshened")
    elif idx:
        rep.kv(notes=idx.get("n_notes"), tickers=idx.get("n_tickers"),
               macro_notes=idx.get("n_macro_notes"),
               llm_views=idx.get("llm_views"))
        ix = idx.get("index") or {}
        top = sorted(ix.items(), key=lambda kv: -kv[1]["n_notes"])[:12]
        rep.log("── his most-researched names (recency-weighted stance):")
        for tk, m in top:
            v = (m.get("llm_view") or {}).get("view") or m.get("latest", "")
            rep.log(f"  {tk:8s} {m['n_notes']:>3} notes  {m['stance']:8s} "
                    f"({m['stance_score']:+.2f})  last {m['last_note_at']}  "
                    f"{str(v)[:70]}")
        bull = sum(1 for m in ix.values() if m["stance"] == "BULLISH")
        bear = sum(1 for m in ix.values() if m["stance"] == "BEARISH")
        rep.kv(bullish_tickers=bull, bearish_tickers=bear,
               mixed=len(ix) - bull - bear)
        rep.ok(f"{idx['n_notes']} notes compiled → {idx['n_tickers']} "
               f"tickers indexed ({idx.get('llm_views', 0)} LLM views)")
        try:
            th = s3_json("data/notes-themes.json")
            rep.log("── macro themes in his untagged notes: " +
                    ", ".join(f"{k}={v}" for k, v in
                              sorted((th.get("theme_counts") or {}).items(),
                                     key=lambda kv: -kv[1])[:8]))
        except Exception:
            warns.append("themes doc unreadable")

    rep.section("3. Wire into the ranking engines")
    for fn in ("justhodl-best-setups", "justhodl-master-ranker",
               "justhodl-alpha-compass"):
        try:
            dep(rep, fn, smoke=True)
        except Exception as e:
            fails.append(f"{fn}: {str(e)[:120]}")
    joined = {}
    for fn, key, path in (
            ("best-setups", "data/best-setups.json", "setups"),
            ("master-ranker", "data/master-ranker.json", "top_tickers")):
        try:
            d = s3_json(key)
            rows = d.get(path) or d.get("rows") or []
            joined[fn] = sum(1 for r in rows if r.get("khalid_note"))
        except Exception:
            joined[fn] = None
    rep.kv(**{f"joined_{k.replace('-', '_')}": v for k, v in joined.items()})
    rep.log("(join counts populate on each engine's next scheduled run — "
            "the readers are deployed)")

    rep.section("4. Thesis-engine regime fix")
    try:
        dep(rep, "justhodl-thesis-engine", smoke=False, invoke_async=True)
        t1 = datetime.now(timezone.utc)
        doc = None
        deadline = time.time() + 700
        while time.time() < deadline:
            try:
                d = s3_json("data/thesis-engine.json")
                if datetime.fromisoformat(d["generated_at"]) >= t1:
                    doc = d
                    break
            except Exception:
                pass
            time.sleep(20)
        if doc:
            rw = doc.get("regime_weeks") or {}
            rd = doc.get("regime_debug") or {}
            rep.kv(regime_now=doc.get("regime_now"), **{
                f"weeks_{k.lower()}": v for k, v in rw.items()})
            rep.log(f"  regime debug: {json.dumps(rd)}")
            if rw.get("EASING", 0) > 50 and rw.get("TIGHTENING", 0) > 50:
                rep.ok("regime series REPAIRED — easing/tightening weeks "
                       "now populate; the regime-gated study is valid")
                for f in (doc.get("families") or []):
                    for reg, r in (f.get("by_regime") or {}).items():
                        rep.log(f"  {f['family']:10s} {reg:10s} excess "
                                f"{r['excess_vs_regime_base_pct']:>6.2f}% "
                                f"t={r['t_stat']:>6} n_eff={r['n_effective']}")
            else:
                fails.append(f"regime still degenerate: {rw} debug={rd}")
        else:
            warns.append("thesis-engine did not refresh in window")
    except Exception as e:
        warns.append(f"thesis-engine: {str(e)[:100]}")

    rep.section("5. Restore FinOps policy")
    try:
        SSM.put_parameter(Name="/justhodl/llm/mode", Value=mode0,
                          Type="String", Overwrite=True)
        rep.log(f"mode restored to {mode0}")
    except Exception as e:
        warns.append(f"mode restore: {str(e)[:60]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
