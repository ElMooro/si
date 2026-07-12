"""ops 3165 — Thesis Engine live: which of Khalid's theses actually leads?

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


with report("3165_thesis_engine") as rep:
    fails, warns = [], []
    rep.heading("ops 3165 — Thesis Engine")

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
        rep.log("── STRONGEST LEADS (|t| on 21d forward SPY):")
        for r in rows[:12]:
            e = (r.get("event_study") or {}).get("d21") or {}
            rep.log(f"  {str(r['name'])[:42]:42s} "
                    f"act {str(r.get('activation_now')):>5}% "
                    f"({str(r.get('activation_pctile')):>5}p) "
                    f"| SPY21 excess {str(e.get('excess_vs_base_pct')):>6}% "
                    f"hit {str(e.get('hit_rate_pct')):>5}% "
                    f"t={str(e.get('t_stat')):>6} n={e.get('n')}"
                    f"{'  ★FIRING' if r.get('firing') else ''}")
        sig = [r for r in rows
               if abs(((r.get("event_study") or {}).get("d21") or {})
                      .get("t_stat", 0)) >= 2
               and ((r.get("event_study") or {}).get("d21") or {})
               .get("n", 0) >= 20]
        firing = [r for r in rows if r.get("firing")]
        rep.kv(theses_with_significant_edge=len(sig),
               theses_firing_now=len(firing))
        rep.ok(f"{len(rows)} theses scored · {len(sig)} carry a "
               f"statistically significant 21d lead (|t|>=2, n>=20) · "
               f"{len(firing)} firing today")
        if sig:
            rep.log("── SIGNIFICANT (these are the ones that actually lead):")
            for r in sig[:8]:
                e = r["event_study"]["d21"]
                d = "risk-OFF tell" if e["excess_vs_base_pct"] < 0 \
                    else "risk-ON tell"
                rep.log(f"  · {r['name']}: {d} — SPY 21d "
                        f"{e['excess_vs_base_pct']:+.2f}% vs base, "
                        f"hit {e['hit_rate_pct']}%, t={e['t_stat']}, "
                        f"n={e['n']}")
        else:
            warns.append("no thesis clears |t|>=2 yet — with ~500 trading "
                         "days per thesis this is an honest result, not a "
                         "bug; the live scorecard keeps grading forward")

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
