"""
justhodl-supply-chain-graph — NAMED SUPPLIER↔CUSTOMER GRAPH + SUPPLIER-LAGGARD ALPHA
═══════════════════════════════════════════════════════════════════════════════════
Bloomberg's SPLC ($24k/yr) maps who-supplies-whom on proprietary FactSet relationship
data. FMP's free supply-chain endpoint is DEAD (404 / legacy-blocked), so the prior
justhodl-supply-chain-linkage produces empty edges. This engine takes the honest path:

  • A CURATED, high-conviction supplier→customer edge map for the hubs where chain-pumps
    actually happen — AI compute, foundry, HBM/memory, semicap, networking, power/cooling,
    the Apple chain, and EV/battery. These are real, verifiable relationships (not a paid
    full-universe graph — scoped to the names that matter for the lead-lag alpha).
  • Live colouring: each node's 30d/5d performance + boom flag (from boom-radar).
  • THE ALPHA: when a hub/customer is BOOMING, its SUPPLIERS that have NOT moved are the
    lead-lag candidates (Cohen-Frazzini customer-momentum / supply-chain return predictability,
    JF 2008). Surfaced as supply_chain_laggards → signal-harvester (eng:supply-chain-graph),
    measure-before-trust. Complements rotation-chain (theme-tier lead-lag) with NAMED edges.

Powers the supply-chain.html force-directed visual (the SPLC-style big picture).
Honest scope: curated hubs, not "every stock" (that needs paid relationship data).
"""
import json
import os
import time
import urllib.request
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/supply-chain-graph.json"
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
S3 = boto3.client("s3", region_name=REGION)

# Curated supplier → customer edges. (supplier, customer, relationship). Real, verifiable.
EDGES = [
    # ── Foundry / litho / semicap (upstream of all compute) ──
    ("ASML", "TSM", "EUV lithography"), ("ASML", "INTC", "EUV lithography"),
    ("AMAT", "TSM", "deposition/etch"), ("LRCX", "TSM", "etch"), ("KLAC", "TSM", "process control"),
    ("TER", "TSM", "test"), ("ENTG", "TSM", "materials"), ("AMAT", "INTC", "equipment"),
    ("LRCX", "MU", "etch"), ("KLAC", "INTC", "metrology"), ("ACLS", "ON", "ion implant"),
    ("TSM", "NVDA", "foundry"), ("TSM", "AMD", "foundry"), ("TSM", "AAPL", "foundry"),
    ("TSM", "AVGO", "foundry"), ("TSM", "QCOM", "foundry"), ("TSM", "MRVL", "foundry"),
    # ── HBM / memory / packaging ──
    ("MU", "NVDA", "HBM memory"), ("MU", "AMD", "HBM memory"),
    ("AMKR", "NVDA", "advanced packaging"), ("AMKR", "AAPL", "packaging"),
    # ── Networking / connectivity ──
    ("AVGO", "AAPL", "RF/connectivity"), ("AVGO", "GOOGL", "custom TPU/networking"),
    ("MRVL", "AMZN", "custom silicon"), ("ANET", "MSFT", "datacenter switching"),
    ("ANET", "META", "datacenter switching"), ("CRDO", "AMZN", "connectivity"),
    # ── Compute hub → hyperscaler/customer (NVDA sells INTO these) ──
    ("NVDA", "MSFT", "GPUs"), ("NVDA", "META", "GPUs"), ("NVDA", "GOOGL", "GPUs"),
    ("NVDA", "AMZN", "GPUs"), ("NVDA", "ORCL", "GPUs"), ("NVDA", "TSLA", "GPUs"),
    ("NVDA", "SMCI", "GPUs"), ("NVDA", "DELL", "GPUs"), ("AMD", "MSFT", "GPUs/CPUs"),
    ("AMD", "META", "GPUs/CPUs"),
    # ── Servers / integration ──
    ("SMCI", "MSFT", "AI servers"), ("SMCI", "META", "AI servers"),
    ("DELL", "MSFT", "AI servers"), ("DELL", "CRWV", "AI servers"),
    # ── Power / cooling (AI tier-3) ──
    ("VRT", "MSFT", "liquid cooling"), ("VRT", "AMZN", "cooling/power"),
    ("VRT", "META", "cooling"), ("VRT", "GOOGL", "cooling"),
    ("VST", "MSFT", "power generation"), ("CEG", "MSFT", "nuclear power"),
    ("GEV", "MSFT", "grid/turbines"), ("ETN", "AMZN", "power management"),
    ("PWR", "GEV", "grid construction"),
    # ── Apple chain ──
    ("QCOM", "AAPL", "modem"), ("SWKS", "AAPL", "RF"), ("QRVO", "AAPL", "RF"),
    ("CRUS", "AAPL", "audio codec"), ("GLW", "AAPL", "cover glass"), ("TXN", "AAPL", "analog"),
    ("STM", "AAPL", "sensors"), ("GSAT", "AAPL", "satellite"),
    # ── EV / battery / auto chips ──
    ("ALB", "TSLA", "lithium"), ("LAC", "TSLA", "lithium"), ("ON", "TSLA", "power semis"),
    ("NXPI", "TSLA", "auto MCU"), ("MCHP", "TSLA", "microcontrollers"), ("NVDA", "TSLA", "compute"),
]

# Theme tags for colouring
THEME = {}
for t in ("ASML", "AMAT", "LRCX", "KLAC", "TER", "ENTG", "ACLS"): THEME[t] = "Semicap"
for t in ("TSM",): THEME[t] = "Foundry"
for t in ("MU", "AMKR"): THEME[t] = "Memory/Packaging"
for t in ("NVDA", "AMD"): THEME[t] = "AI Compute"
for t in ("AVGO", "MRVL", "ANET", "CRDO", "QCOM"): THEME[t] = "Networking/Connectivity"
for t in ("SMCI", "DELL"): THEME[t] = "AI Servers"
for t in ("VRT", "VST", "CEG", "GEV", "ETN", "PWR"): THEME[t] = "Power/Cooling"
for t in ("MSFT", "META", "GOOGL", "AMZN", "ORCL", "CRWV"): THEME[t] = "Hyperscaler"
for t in ("AAPL", "SWKS", "QRVO", "CRUS", "GLW", "TXN", "STM", "GSAT"): THEME[t] = "Apple Chain"
for t in ("TSLA", "ALB", "LAC", "ON", "NXPI", "MCHP"): THEME[t] = "EV/Auto"


def perf(ticker):
    end = date.today(); start = end - timedelta(days=50)
    try:
        with urllib.request.urlopen(urllib.request.Request(
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start.isoformat()}/{end.isoformat()}"
            f"?adjusted=true&sort=asc&limit=60&apiKey={POLY}", headers={"User-Agent": "jh/1"}), timeout=20) as r:
            res = json.loads(r.read()).get("results") or []
        closes = [x.get("c") for x in res if x.get("c")]
        if len(closes) < 6:
            return None
        p30 = round((closes[-1] / closes[-22] - 1) * 100, 2) if len(closes) >= 22 else None
        p5 = round((closes[-1] / closes[-6] - 1) * 100, 2)
        return {"perf_30d": p30, "perf_5d": p5, "price": round(closes[-1], 2)}
    except Exception:
        return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    nodes_set = sorted({s for s, _, _ in EDGES} | {c for _, c, _ in EDGES})
    # boom flags from boom-radar
    boom = set()
    try:
        br = json.loads(S3.get_object(Bucket=BUCKET, Key="data/boom-radar.json")["Body"].read())
        for r in (br.get("top_picks") or br.get("board") or br.get("high_conviction") or []):
            tk = r.get("ticker") if isinstance(r, dict) else r
            if tk:
                boom.add(str(tk).upper())
    except Exception as e:
        print(f"[scg] boom-radar unavailable: {str(e)[:60]}")

    perfs = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(perf, t): t for t in nodes_set}
        for f in as_completed(futs):
            p = f.result()
            if p:
                perfs[futs[f]] = p

    # roles: a node is a "hub/customer" if it has incoming edges; "supplier" if outgoing
    customers_of = {}   # supplier -> [customers]
    suppliers_of = {}   # customer -> [suppliers]
    for s, c, rel in EDGES:
        customers_of.setdefault(s, []).append((c, rel))
        suppliers_of.setdefault(c, []).append((s, rel))

    nodes = []
    for t in nodes_set:
        p = perfs.get(t, {})
        p30 = p.get("perf_30d")
        is_boom = t in boom or (p30 is not None and p30 >= 25)
        nodes.append({"ticker": t, "theme": THEME.get(t, "Other"),
                      "perf_30d": p30, "perf_5d": p.get("perf_5d"), "price": p.get("price"),
                      "is_boom": is_boom,
                      "n_suppliers": len(suppliers_of.get(t, [])),
                      "n_customers": len(customers_of.get(t, []))})
    edges = [{"supplier": s, "customer": c, "relationship": rel} for s, c, rel in EDGES]

    # ── ALPHA: suppliers of BOOMING customers that have NOT moved yet ──
    booming_hubs = [n["ticker"] for n in nodes if n["is_boom"]]
    laggards = []
    for hub in booming_hubs:
        hub_p = perfs.get(hub, {}).get("perf_30d")
        for sup, rel in suppliers_of.get(hub, []):
            sp = perfs.get(sup, {}).get("perf_30d")
            if sp is None or hub_p is None:
                continue
            gap = round(hub_p - sp, 1)
            if sp < 15 and gap >= 15:   # supplier lagging a booming customer
                laggards.append({"ticker": sup, "theme": THEME.get(sup, "Other"),
                                 "supplies_to": hub, "relationship": rel,
                                 "customer_perf_30d": hub_p, "own_perf_30d": sp,
                                 "lag_gap_pct": gap})
    # dedup supplier→best gap
    best = {}
    for l in laggards:
        k = l["ticker"]
        if k not in best or l["lag_gap_pct"] > best[k]["lag_gap_pct"]:
            best[k] = l
    laggards = sorted(best.values(), key=lambda x: x["lag_gap_pct"], reverse=True)

    top_picks = [{"ticker": l["ticker"], "direction": "long", "score": min(100, l["lag_gap_pct"]),
                  "supplies_to": l["supplies_to"], "own_perf_30d": l["own_perf_30d"],
                  "customer_perf_30d": l["customer_perf_30d"], "lag_gap_pct": l["lag_gap_pct"]}
                 for l in laggards][:20]

    payload = {
        "engine": "justhodl-supply-chain-graph", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": ("Named supplier↔customer graph for the hubs where chain-pumps happen "
                   "(AI/semis/power/Apple/EV). When a hub booms, its suppliers that haven't "
                   "moved are the lead-lag candidates (supply-chain return predictability)."),
        "n_nodes": len(nodes), "n_edges": len(edges),
        "booming_hubs": booming_hubs,
        "nodes": nodes, "edges": edges,
        "supply_chain_laggards": laggards[:30],
        "top_picks": top_picks,
        "data_source": "Curated supplier↔customer edge map + Polygon perf + boom-radar flags",
        "caveats": [
            "Curated high-conviction edges for the hubs that matter — NOT a paid full-universe "
            "graph (FMP supply-chain endpoint is dead; Bloomberg SPLC / FactSet are the paid "
            "sources). Supersedes the empty justhodl-supply-chain-linkage.",
            "MEASURE-BEFORE-TRUST: supply_chain_laggards → signal-harvester (eng:supply-chain-graph), "
            "graded forward vs SPY; NOT in decision engines until alpha-proven. Complements "
            "rotation-chain (theme-tier lead-lag) with named relationships.",
            "Supply-chain lead-lag is a documented anomaly (Cohen & Frazzini, JF 2008) but "
            "edges here are static/curated; refresh the map as relationships change.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[scg] nodes={len(nodes)} edges={len(edges)} booming={len(booming_hubs)} "
          f"laggards={len(laggards)} in {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_nodes": len(nodes), "n_edges": len(edges),
        "booming_hubs": booming_hubs[:10],
        "top_laggards": [(l["ticker"], l["supplies_to"], l["own_perf_30d"], l["customer_perf_30d"], l["lag_gap_pct"]) for l in laggards[:8]]})}
