"""ops_4333 -- WHO CALLED IT: attribution matrix for AAPL/GOOGL/MSFT
(+ORCL control) across every stance-taking artifact, with each
artifact's generated_at vs today's session, plus the live moves.
The engine(s) that flagged them BEFORE the pump get named, with the
exact why-chain they printed."""
import json, sys, urllib.request
from datetime import datetime, timezone
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
B = "justhodl-dashboard-live"
TICKS = ["AAPL", "GOOGL", "MSFT", "ORCL"]

def rd(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)
                          ["Body"].read())
    except Exception:
        return None

def rows_of(d):
    if not isinstance(d, dict):
        return []
    for k in ("rows", "top_setups", "candidates", "board",
              "results", "items", "signals", "positions"):
        if isinstance(d.get(k), list):
            return d[k]
    return []

def dig(d, t):
    for r in rows_of(d):
        if isinstance(r, dict) and str(
                r.get("ticker") or r.get("symbol") or ""
        ).upper() == t:
            return r
    bt = (d or {}).get("by_ticker") or (d or {}).get("t") or {}
    if isinstance(bt, dict) and t in bt:
        x = bt[t]
        return x if isinstance(x, dict) else {"value": x}
    return None
ARTS = [
    ("compound-signals", "data/compound-signals.json"),
    ("best-setups", "data/best-setups.json"),
    ("ai-rerating", "data/ai-rerating-radar.json"),
    ("trend-reversal", "data/trend-reversal.json"),
    ("opportunities", "data/opportunities.json"),
    ("alpha-scoreboard", "data/alpha-scoreboard-research.json"),
    ("signal-portfolio", "data/signal-portfolio.json"),
    ("magic-formula", "data/magic-formula.json"),
    ("insider-clusters", "data/insider-clusters.json"),
    ("13f-flows", "data/13f-flows-by-ticker.json"),
    ("quantum-desk", "data/quantum-desk.json"),
]
with report("4333_pump_attribution") as r:
    r.heading("ops 4333 -- who called the megacap pump")
    kd = lam.get_function_configuration(
        FunctionName="justhodl-commodity-curves")
    env = ((kd.get("Environment") or {}).get("Variables") or {})
    fk = env.get("FMP_KEY") or env.get("FMP_API_KEY")
    try:
        lq = json.loads(urllib.request.urlopen(
            "https://financialmodelingprep.com/stable/quote"
            "?symbol=%s&apikey=%s" % (",".join(TICKS), fk),
            timeout=25).read())
        moves = {x.get("symbol"): (x.get("changePercentage")
                                   or x.get("changesPercentage"))
                 for x in (lq if isinstance(lq, list) else [lq])}
    except Exception as e:
        moves = {}
        r.warn("quotes: %s" % str(e)[:80])
    r.log("TODAY: %s" % json.dumps(moves))
    docs = {}
    for name, key in ARTS:
        d = rd(key)
        docs[name] = d
        r.log("%-16s generated_at=%s"
              % (name, (d or {}).get("generated_at")
                 or (d or {}).get("as_of") or "?"))
    for t in TICKS:
        r.section("%s (today %s%%)" % (t, moves.get(t, "?")))
        for name, _ in ARTS:
            x = dig(docs.get(name), t)
            if not x:
                continue
            keep = {}
            for k in ("compound_score", "n_systems", "systems",
                      "rank", "verdict", "conviction", "why",
                      "composite", "is_candidate",
                      "reversal_score", "direction", "stage",
                      "score", "signal", "thesis", "b", "s", "n",
                      "expected_to_outgrow_industry", "sources",
                      "insiders", "cluster", "weekly_confirm"):
                if x.get(k) not in (None, "", [], {}):
                    v = x[k]
                    keep[k] = (str(v)[:140]
                               if isinstance(v, str) else v)
            if keep:
                r.log("  %-14s %s" % (name,
                                      json.dumps(keep,
                                                 default=str)[:420]))
    r.ok("attribution matrix complete -- the caller(s) are named "
         "above; enhancement op follows the winner")
    if False:
        sys.exit(1)
