"""ops 3166 — Thesis Engine, HONEST statistics.

The first run printed t=8.59 / 92% hit rates. Those were inflated:
forward 21d returns sampled on EVERY day share 20 of 21 days with their
neighbours, so the effective sample is n/21 and a naive t is ~sqrt(21)
= 4.6x too big (verified offline: naive 9.42 -> corrected 2.06). And
with 56 theses tested, p<0.05 alone guarantees ~3 false positives.

Fixed in the engine before Khalid can act on a bad number:
  · overlap-corrected t (effective n = n / horizon)
  · hit_edge_pp = hit rate ABOVE SPY's own base rate (a 92% hit means
    nothing if the base rate was 88%)
  · Benjamini-Hochberg FDR at q=0.10 across every thesis tested
  · signals emit ONLY for FDR survivors that are firing today

Re-run and report what actually survives.

207 watchlists → named theses. Members resolved (FRED + Polygon +
formula evaluation), z-scored, activation index per thesis per day,
then an EVENT STUDY of top-quintile activation days against forward SPY
returns — so the answer arrives from 2-3 years of history instead of a
6-week wait.

Deploy (3008MB/900s — a first run fetches ~550 FRED series + ~1,700
Polygon histories; a gzipped state cache makes every later run cheap),
async invoke with force_emit, wait for the doc, then report:
  · theses scored + coverage
  · the strongest |t| theses (the ones that LEAD)
  · which are firing today
  · signals emitted into the live scorecard
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
FN = "justhodl-thesis-engine"
HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3166_thesis_stats") as rep:
    fails, warns = [], []
    rep.heading("ops 3166 — Thesis Engine")

    rep.section("1. Deploy")
    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
    env = {"S3_BUCKET": BUCKET}
    for donor, keys in (("justhodl-etf-fund-flows", ("POLYGON_KEY",)),
                        ("justhodl-dollar-radar",
                         ("FRED_API_KEY", "FRED_KEY"))):
        try:
            dv = (LAM.get_function_configuration(FunctionName=donor)
                  .get("Environment") or {}).get("Variables") or {}
            for k in keys:
                if dv.get(k):
                    env[k] = dv[k]
        except Exception as e:
            warns.append(f"donor {donor}: {str(e)[:60]}")
    rep.log(f"env keys: {sorted(env)}")
    sched = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env,
                  eb_rule_name=sched.get("rule_name"),
                  eb_schedule=sched.get("cron"),
                  timeout=cfg.get("timeout", 900),
                  memory=cfg.get("memory", 3008),
                  description=cfg.get("description", "")[:250],
                  smoke=False)

    rep.section("2. First run (cold: full history backfill)")
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName=FN, InvocationType="Event",
               Payload=json.dumps({"force_emit": True}).encode())
    rep.log("async invoke fired — backfilling ~550 FRED + ~1,700 Polygon "
            "series, then event-studying every thesis")
    doc = None
    deadline = time.time() + 880
    while time.time() < deadline:
        try:
            d = s3_json("data/thesis-engine.json")
            if datetime.fromisoformat(d["generated_at"]) >= t0:
                doc = d
                break
        except Exception:
            pass
        time.sleep(20)
    if doc is None:
        fails.append("thesis-engine.json never freshened (>880s)")
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)

    rep.section("3. Results")
    rep.kv(status=doc.get("status"), n_theses=doc.get("n_theses"),
           signals_logged=doc.get("signals_logged"),
           elapsed_s=doc.get("elapsed_s"),
           spy_base_21d=(doc.get("spy_base_rates_pct") or {}).get("d21"))
    rows = doc.get("theses") or []
    if doc.get("status") != "LIVE" or not rows:
        fails.append(f"engine status {doc.get('status')} / {len(rows)} rows")
    else:
        rep.log("── ranked by OVERLAP-CORRECTED t (FDR survivors first):")
        for r in rows[:14]:
            e = (r.get("event_study") or {}).get("d21") or {}
            rep.log(f"  {str(r['name'])[:38]:38s} "
                    f"act {str(r.get('activation_now')):>5}% "
                    f"| excess {str(e.get('excess_vs_base_pct')):>6}% "
                    f"hit-edge {str(e.get('hit_edge_pp')):>5}pp "
                    f"t={str(e.get('t_stat')):>6} "
                    f"n_eff={str(e.get('n_effective')):>5}"
                    f"{'  FDR✓' if r.get('fdr_pass') else ''}"
                    f"{' ★FIRING' if r.get('firing') else ''}")
        b = doc.get("spy_base_rates_pct") or {}
        rep.log(f"── SPY base rates (the bar every thesis must clear): "
                f"5d {b.get('d5')}% · 21d {b.get('d21')}% · 63d {b.get('d63')}%")
        sig = [r for r in rows if r.get("fdr_pass")]
        firing = [r for r in rows if r.get("firing")]
        rep.kv(fdr_survivors=len(sig), theses_firing_now=len(firing),
               n_fdr_survivors_doc=doc.get("n_fdr_survivors"))
        rep.ok(f"{len(rows)} theses scored · {len(sig)} survive BH-FDR "
               f"q=0.10 with overlap-corrected t · {len(firing)} firing")
        if sig:
            rep.log("── SURVIVORS (real leads after overlap + FDR):")
            for r in sig[:10]:
                e = r["event_study"]["d21"]
                d = "risk-OFF tell" if e["excess_vs_base_pct"] < 0 \
                    else "risk-ON tell"
                rep.log(f"  · {r['name']}: {d} — SPY 21d "
                        f"{e['excess_vs_base_pct']:+.2f}% vs base "
                        f"(hit edge {e['hit_edge_pp']:+.1f}pp), "
                        f"t={e['t_stat']}, n_eff={e['n_effective']}"
                        f"{'  [FIRING NOW]' if r.get('firing') else ''}")
        else:
            warns.append("ZERO theses survive FDR — the honest answer: on "
                         "~2y of history none of the 56 shows a 21d lead "
                         "over SPY that beats multiple testing. The naive "
                         "t=8.59 from the first run was overlap inflation.")

    rep.section("4. Page")
    import urllib.request
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"https://justhodl.ai/theses.html?t={int(time.time())}",
            headers={"User-Agent": "Mozilla/5.0 ops"}), timeout=15)
        if "Thesis Engine" in r.read().decode("utf-8", "replace"):
            rep.ok("theses.html live on CDN")
        else:
            warns.append("theses.html not yet on CDN (self-heals)")
    except Exception as e:
        warns.append(f"page: {str(e)[:60]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
