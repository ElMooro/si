"""
justhodl-leadlag-graph — WHO MOVES BEFORE WHOM (the contagion feature, universe-wide)
=====================================================================================
The re-rating radar's contagion (MU revises up -> ACMR hasn't) was one hand-built edge.
This generalises it into a learned directed graph across the whole liquid universe:
for every ordered pair (A,B) it asks "does A's move today predict B's move in the next
1-3 days?" and keeps the edge ONLY when the lead is asymmetric — A-leads-B materially
stronger than B-leads-A. Symmetric co-movement (two chips that just trade together) is
rejected; only genuine lead->lag relationships survive. That asymmetry is the closest
a daily-horizon retail system gets to causal direction.

Then it turns the graph live: when a LEADER just made a notable move, it lists the
downstream FOLLOWERS expected to move next, with direction and lag — the window before
the market connects them. This is the one edge that beats the tape on *being early to
the connection* rather than on speed.

OUTPUT data/lead-lag-graph.json   SCHEDULE daily 13:00 UTC. Real prices, research only.
"""
import json
import time
import math
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/lead-lag-graph.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
POLYGON = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

UNIVERSE_CAP = 85
MIN_BARS = 45
MAX_LAG = 3
CORR_MIN = 0.34          # min forward lead-lag correlation to consider an edge
ASYM_MIN = 0.10          # A->B must beat B->A by this margin (lead, not co-move)
MOVE_THRESHOLD = 0.018   # |recent return| to call a leader "in motion" for live preds
SECTOR_ETFS = ["XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC"]
INDEX_ETFS = ["SPY", "QQQ", "IWM", "SMH", "XBI"]

s3 = boto3.client("s3", region_name="us-east-1")


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _get(url):
    try:
        return json.loads(urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "jh-ll"}), timeout=14).read())
    except Exception:
        return None


def hist(sym):
    """Return {date(str): close} for ~75 trading days. Self-discovering source."""
    q = urllib.parse.quote(sym)
    today = datetime.now(timezone.utc).date()
    frm = (today - timedelta(days=120)).isoformat()
    for url in (f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol={q}&from={frm}&apikey={FMP}",
                f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={q}&from={frm}&apikey={FMP}"):
        d = _get(url)
        rows = d if isinstance(d, list) else (d or {}).get("historical") if isinstance(d, dict) else None
        if isinstance(rows, list) and rows:
            out = {}
            for r in rows:
                dt = str(r.get("date") or "")[:10]
                c = r.get("close", r.get("price"))
                if dt and isinstance(c, (int, float)):
                    out[dt] = float(c)
            if len(out) >= MIN_BARS:
                return out
    # polygon range fallback
    d = _get(f"https://api.polygon.io/v2/aggs/ticker/{q}/range/1/day/{frm}/{today.isoformat()}?adjusted=true&sort=asc&limit=200&apiKey={POLYGON}")
    res = (d or {}).get("results") or []
    out = {}
    for r in res:
        if r.get("t") and r.get("c"):
            dt = datetime.fromtimestamp(r["t"] / 1000, timezone.utc).date().isoformat()
            out[dt] = float(r["c"])
    return out if len(out) >= MIN_BARS else {}


def pearson(xs, ys):
    n = len(xs)
    if n < 12:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy = math.sqrt(sum((y - my) ** 2 for y in ys))
    if dx == 0 or dy == 0:
        return None
    return num / (dx * dy)


def best_lead(a, b):
    """Max forward corr over lags: a_t predicts b_{t+k}. Returns (corr, lag)."""
    best, blag = 0.0, 0
    n = len(a)
    for k in range(1, MAX_LAG + 1):
        c = pearson(a[:n - k], b[k:])
        if c is not None and c > best:
            best, blag = c, k
    return best, blag


def build_universe():
    syms = set(SECTOR_ETFS) | set(INDEX_ETFS)
    d = _read("data/ai-infra-stack.json") or {}
    for layer in d.get("stack", []):
        for n in layer.get("names", []):
            if n.get("symbol") and (n.get("market_cap") or 0) > 8e8:   # liquid enough
                syms.add(n["symbol"])
    # round out with a few top momentum/deal names if present
    for key, lk in (("data/deal-scanner.json", "ai_deals"), ("data/ai-rerating-radar.json", "top_setups")):
        doc = _read(key) or {}
        for it in ((doc.get("summary", {}) or {}).get(lk, []) or [])[:12]:
            if isinstance(it, dict) and it.get("symbol"):
                syms.add(it["symbol"])
    return sorted(syms)[:UNIVERSE_CAP]


def lambda_handler(event, context):
    t0 = time.time()
    universe = build_universe()

    # fetch aligned price history
    closes = {}
    with ThreadPoolExecutor(max_workers=12) as ex:
        fut = {ex.submit(hist, s): s for s in universe}
        for f in as_completed(fut):
            h = f.result()
            if h:
                closes[fut[f]] = h
    if len(closes) < 10:
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps({"engine": "lead-lag-graph", "version": VERSION,
                                       "generated_at": datetime.now(timezone.utc).isoformat(),
                                       "error": "insufficient price history",
                                       "n_with_history": len(closes)}).encode(),
                      ContentType="application/json")
        return {"statusCode": 500, "body": "insufficient history (%d)" % len(closes)}

    # common date axis (intersection), then aligned log returns
    common = set.intersection(*[set(h.keys()) for h in closes.values()])
    dates = sorted(common)[-75:]
    if len(dates) < MIN_BARS:
        # relax: use union-aligned per pair is costly; trim universe to names covering the axis
        pass
    rets, recent = {}, {}
    for s, h in closes.items():
        series = [h[d] for d in dates if d in h]
        if len(series) < MIN_BARS:
            continue
        r = [math.log(series[i] / series[i - 1]) for i in range(1, len(series)) if series[i - 1] > 0]
        if len(r) >= MIN_BARS - 1:
            rets[s] = r
            recent[s] = round((math.exp(sum(r[-2:])) - 1) * 100, 2)   # ~2-day % move
    names = list(rets.keys())

    # directed asymmetric lead-lag edges
    edges = []
    for a in names:
        for b in names:
            if a == b:
                continue
            fwd, lag = best_lead(rets[a], rets[b])
            if fwd < CORR_MIN:
                continue
            rev, _ = best_lead(rets[b], rets[a])
            if fwd - rev < ASYM_MIN:
                continue
            edges.append({"from": a, "to": b, "lag_days": lag,
                          "lead_corr": round(fwd, 3), "reverse_corr": round(rev, 3),
                          "asymmetry": round(fwd - rev, 3)})
    edges.sort(key=lambda e: (e["asymmetry"], e["lead_corr"]), reverse=True)

    # node degrees -> leaders / followers
    out_deg, in_deg, out_str = {}, {}, {}
    for e in edges:
        out_deg[e["from"]] = out_deg.get(e["from"], 0) + 1
        in_deg[e["to"]] = in_deg.get(e["to"], 0) + 1
        out_str[e["from"]] = out_str.get(e["from"], 0.0) + e["asymmetry"]
    nodes = [{"symbol": s, "out_degree": out_deg.get(s, 0), "in_degree": in_deg.get(s, 0),
              "lead_strength": round(out_str.get(s, 0.0), 2), "recent_2d_pct": recent.get(s)}
             for s in names]
    leaders = sorted([n for n in nodes if n["out_degree"] > 0],
                     key=lambda n: (n["lead_strength"], n["out_degree"]), reverse=True)[:20]

    # LIVE predictions: a leader just moved -> followers expected to follow
    edge_by_from = {}
    for e in edges:
        edge_by_from.setdefault(e["from"], []).append(e)
    live = []
    for ld in leaders:
        s = ld["symbol"]
        mv = recent.get(s)
        if mv is None or abs(mv / 100.0) < MOVE_THRESHOLD:
            continue
        flw = []
        for e in sorted(edge_by_from.get(s, []), key=lambda x: x["asymmetry"], reverse=True)[:6]:
            flw.append({"symbol": e["to"], "expected_dir": "UP" if mv > 0 else "DOWN",
                        "lag_days": e["lag_days"], "lead_corr": e["lead_corr"]})
        if flw:
            live.append({"leader": s, "leader_move_2d_pct": mv,
                         "followers_expected": flw})
    live.sort(key=lambda x: abs(x["leader_move_2d_pct"]), reverse=True)

    out = {
        "engine": "lead-lag-graph", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Directed graph of who moves before whom (asymmetric lagged correlation). When a leader "
                  "moves, its downstream followers are expected to follow within the lag window.",
        "n_universe": len(universe), "n_with_history": len(closes), "n_nodes": len(names),
        "n_edges": len(edges), "axis_days": len(dates),
        "price_source": "FMP historical-eod / Polygon range (self-discovered)",
        "live_predictions": live[:12],
        "top_leaders": leaders,
        "edges": edges[:200],
        "nodes": sorted(nodes, key=lambda n: n["lead_strength"], reverse=True),
        "methodology": {
            "edge": "for each ordered pair, max forward Pearson corr of A_t vs B_{t+k} over k=1..%d; "
                    "kept only if corr>=%.2f AND lead beats reverse by >=%.2f (asymmetry = direction)" % (MAX_LAG, CORR_MIN, ASYM_MIN),
            "returns": "log returns on a common %d-day trading axis (date intersection)" % len(dates),
            "live": "leader with |2-day move|>=%.1f%% -> followers' expected direction = sign(move)" % (MOVE_THRESHOLD * 100),
        },
        "caveats": "Correlation-based lead-lag is NOT proven causation — spurious edges survive multiple "
                   "testing, the lookback is short, and relationships are regime-dependent and decay. "
                   "Predictions are probabilistic, not deterministic. Treat as a hypothesis generator that "
                   "the truth-layer ledger will grade over time, not a guarantee.",
        "disclaimer": "Real prices, research only — not investment advice.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[leadlag] universe={len(universe)} hist={len(closes)} nodes={len(names)} "
          f"edges={len(edges)} live={len(live)} axis={len(dates)}d {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "nodes": len(names),
            "edges": len(edges), "live": len(live), "with_history": len(closes)})}
