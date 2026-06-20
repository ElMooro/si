"""
justhodl-supply-chain-graph — NAMED SUPPLIER↔CUSTOMER GRAPH + SUPPLIER-LAGGARD ALPHA
═══════════════════════════════════════════════════════════════════════════════════
Bloomberg SPLC ($24k/yr) maps who-supplies-whom on proprietary FactSet data. FMP's free
supply-chain endpoint is DEAD, so this takes the honest path: a CURATED, high-conviction
supplier→customer edge map across the sectors where chain-pumps happen — semiconductors
(litho/semicap/EDA/foundry/memory/power), AI compute + datacenter physical (power/cooling/
optical/connectors/EMS/storage), the Apple chain, aerospace & defense, electrical/automation,
trucks/rail/machinery, autos & parts, EV/battery/materials, energy services, and biopharma
supply. Live-coloured by perf + boom flags. THE ALPHA: when a hub/customer BOOMS, its
suppliers that haven't moved are the lead-lag candidates (Cohen-Frazzini, JF 2008) →
supply_chain_laggards → harvester (eng:supply-chain-graph), measure-before-trust.
Powers supply-chain.html (d3 SPLC-style visual). Perf via Polygon grouped-daily (scales).
Honest scope: curated hubs, not paid full-universe.
"""
import json
import os
import time
import urllib.request
from datetime import date, datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/supply-chain-graph.json"
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
S3 = boto3.client("s3", region_name=REGION)

# (supplier, customer, relationship) — curated, real relationships.
EDGES = [
    # ── Litho / semicap (upstream of all chips) ──
    ("ASML", "TSM", "EUV litho"), ("ASML", "INTC", "EUV litho"), ("ASML", "MU", "litho"),
    ("AMAT", "TSM", "deposition/etch"), ("AMAT", "INTC", "equipment"), ("AMAT", "MU", "equipment"),
    ("LRCX", "TSM", "etch"), ("LRCX", "MU", "etch"), ("LRCX", "INTC", "etch"),
    ("KLAC", "TSM", "process control"), ("KLAC", "INTC", "metrology"),
    ("TER", "TSM", "test"), ("TER", "NVDA", "test"), ("ENTG", "TSM", "materials"), ("ENTG", "INTC", "materials"),
    ("MKSI", "AMAT", "subsystems"), ("MKSI", "LRCX", "subsystems"),
    ("UCTT", "AMAT", "subsystems"), ("UCTT", "LRCX", "subsystems"),
    ("ICHR", "LRCX", "subsystems"), ("ICHR", "AMAT", "subsystems"),
    ("ONTO", "TSM", "metrology"), ("CAMT", "TSM", "inspection"),
    ("FORM", "TSM", "probe cards"), ("FORM", "INTC", "probe cards"),
    ("KLIC", "AMKR", "packaging tools"), ("AEHR", "ON", "test systems"), ("ACLS", "ON", "ion implant"),
    ("COHR", "ASML", "optics/lasers"),
    # ── EDA / IP ──
    ("SNPS", "TSM", "EDA"), ("SNPS", "NVDA", "EDA"), ("SNPS", "AMD", "EDA"),
    ("CDNS", "NVDA", "EDA"), ("CDNS", "AVGO", "EDA"), ("CDNS", "AMD", "EDA"),
    ("ARM", "AAPL", "CPU IP"), ("ARM", "NVDA", "CPU IP"), ("ARM", "QCOM", "CPU IP"),
    # ── Foundry → fabless ──
    ("TSM", "NVDA", "foundry"), ("TSM", "AMD", "foundry"), ("TSM", "AAPL", "foundry"),
    ("TSM", "AVGO", "foundry"), ("TSM", "QCOM", "foundry"), ("TSM", "MRVL", "foundry"),
    # ── HBM / memory / packaging ──
    ("MU", "NVDA", "HBM"), ("MU", "AMD", "HBM"), ("AMKR", "NVDA", "packaging"), ("AMKR", "AAPL", "packaging"),
    # ── Power-management semis into compute ──
    ("MPWR", "NVDA", "power mgmt"), ("MPWR", "DELL", "power mgmt"), ("VICR", "NVDA", "power modules"),
    ("ADI", "AAPL", "analog"), ("ADI", "TSLA", "analog"),
    # ── AI compute → customers ──
    ("NVDA", "MSFT", "GPUs"), ("NVDA", "META", "GPUs"), ("NVDA", "GOOGL", "GPUs"),
    ("NVDA", "AMZN", "GPUs"), ("NVDA", "ORCL", "GPUs"), ("NVDA", "TSLA", "GPUs"),
    ("NVDA", "CRWV", "GPUs"), ("NVDA", "DELL", "GPUs"), ("NVDA", "SMCI", "GPUs"),
    ("AMD", "MSFT", "GPUs/CPUs"), ("AMD", "META", "GPUs/CPUs"), ("AMD", "ORCL", "CPUs"),
    # ── Networking / optical / connectors / EMS / storage ──
    ("AVGO", "GOOGL", "custom TPU"), ("AVGO", "META", "networking"), ("AVGO", "AAPL", "RF/connectivity"),
    ("MRVL", "AMZN", "custom silicon"), ("MRVL", "MSFT", "custom silicon"),
    ("ANET", "MSFT", "switching"), ("ANET", "META", "switching"), ("ANET", "ORCL", "switching"),
    ("CRDO", "AMZN", "connectivity"), ("CRDO", "MSFT", "connectivity"),
    ("LITE", "AAPL", "lasers/3D sensing"), ("LITE", "NVDA", "optical"), ("COHR", "NVDA", "optical interconnect"),
    ("APH", "NVDA", "connectors"), ("APH", "AAPL", "connectors"),
    ("CLS", "AMZN", "ODM servers"), ("CLS", "NVDA", "ODM"),
    ("JBL", "AAPL", "EMS"), ("FLEX", "AAPL", "EMS"),
    ("STX", "AMZN", "HDD"), ("STX", "MSFT", "HDD"), ("WDC", "AMZN", "storage"),
    ("SMCI", "MSFT", "AI servers"), ("SMCI", "META", "AI servers"),
    ("DELL", "MSFT", "AI servers"), ("DELL", "CRWV", "AI servers"),
    # ── Datacenter physical: power / cooling ──
    ("VRT", "MSFT", "cooling/power"), ("VRT", "AMZN", "cooling"), ("VRT", "META", "cooling"),
    ("VRT", "GOOGL", "cooling"), ("VRT", "CRWV", "cooling"),
    ("VST", "MSFT", "power"), ("CEG", "MSFT", "nuclear power"), ("GEV", "MSFT", "grid/turbines"),
    ("ETN", "AMZN", "power mgmt"), ("ETN", "MSFT", "power mgmt"), ("PWR", "GEV", "grid construction"),
    ("NVT", "MSFT", "power/enclosures"), ("TT", "MSFT", "HVAC cooling"), ("JCI", "AMZN", "building/cooling"),
    ("CARR", "MSFT", "cooling"), ("CCJ", "CEG", "uranium fuel"),
    # ── Apple chain ──
    ("QCOM", "AAPL", "modem"), ("SWKS", "AAPL", "RF"), ("QRVO", "AAPL", "RF"),
    ("CRUS", "AAPL", "audio codec"), ("GLW", "AAPL", "cover glass"), ("TXN", "AAPL", "analog"),
    ("STM", "AAPL", "sensors"), ("GSAT", "AAPL", "satellite"), ("SYNA", "AAPL", "touch/IoT"),
    # ── Aerospace & Defense ──
    ("HWM", "BA", "aero structures"), ("HWM", "RTX", "engine components"), ("HWM", "LMT", "fasteners"),
    ("HEI", "BA", "aftermarket parts"), ("TDG", "BA", "aero components"), ("SPR", "BA", "fuselages"),
    ("GE", "BA", "jet engines"), ("RTX", "BA", "jet engines"),
    ("CW", "LMT", "defense components"), ("CW", "RTX", "components"),
    ("MRCY", "RTX", "defense electronics"), ("MRCY", "LMT", "electronics"),
    ("KTOS", "LMT", "drones/defense"), ("LHX", "LMT", "C4ISR"), ("LHX", "NOC", "electronics"),
    ("HII", "NOC", "shipbuilding"), ("AXON", "GD", "tactical"),
    ("MP", "LMT", "rare-earth magnets"),
    # ── Electrical / automation / grid ──
    ("ROK", "CAT", "factory automation"), ("ROK", "DE", "automation"),
    ("AME", "BA", "instruments"), ("PH", "BA", "motion/hydraulics"), ("DOV", "XOM", "equipment"),
    ("HUBB", "GEV", "grid components"), ("ATKR", "VRT", "electrical conduit"), ("PWR", "NEE", "grid construction"),
    # ── Trucks / machinery / rail ──
    ("CMI", "PCAR", "engines"), ("ALSN", "PCAR", "transmissions"), ("DAN", "PCAR", "drivetrain"),
    ("CMI", "DE", "engines"), ("WAB", "UNP", "locomotives/braking"), ("GBX", "UNP", "railcars"),
    ("TRN", "CSX", "railcars"),
    # ── Autos & parts ──
    ("APTV", "GM", "electronics"), ("APTV", "F", "electronics"), ("BWA", "GM", "powertrain"),
    ("LEA", "GM", "seats"), ("MGA", "GM", "contract mfg"), ("MGA", "F", "parts"),
    ("ALV", "GM", "safety"), ("GNTX", "GM", "mirrors"), ("AXL", "GM", "driveline"), ("DAN", "F", "drivetrain"),
    ("NXPI", "TSLA", "auto MCU"), ("MCHP", "TSLA", "MCU"), ("ON", "TSLA", "SiC power"), ("ON", "F", "SiC"),
    ("QCOM", "GM", "auto cockpit"),
    # ── EV / battery / materials ──
    ("ALB", "TSLA", "lithium"), ("LAC", "TSLA", "lithium"), ("SQM", "TSLA", "lithium"),
    ("MP", "TSLA", "rare-earth magnets"), ("FCX", "TSLA", "copper"),
    # ── Energy services ──
    ("SLB", "XOM", "oilfield services"), ("HAL", "XOM", "oilfield services"), ("BKR", "XOM", "LNG/oilfield"),
    ("SLB", "CVX", "oilfield services"),
    # ── Biopharma supply (life-science tools / CDMO / devices) ──
    ("WST", "LLY", "injection devices"), ("TMO", "LLY", "bioprocessing/CRO"), ("DHR", "LLY", "bioprocessing"),
    ("RGEN", "LLY", "bioprocessing"), ("CRL", "LLY", "preclinical CRO"), ("A", "LLY", "lab instruments"),
    ("TMO", "PFE", "CDMO"), ("DHR", "MRK", "bioprocessing"), ("WST", "PFE", "containment"),
]

# Theme tags
THEME = {}
def _tag(names, theme):
    for n in names:
        THEME[n] = theme
_tag(["ASML","AMAT","LRCX","KLAC","TER","ENTG","MKSI","UCTT","ICHR","ONTO","CAMT","FORM","KLIC","AEHR","ACLS"], "Semicap")
_tag(["SNPS","CDNS","ARM"], "EDA/IP")
_tag(["TSM"], "Foundry")
_tag(["MU","AMKR"], "Memory/Packaging")
_tag(["NVDA","AMD","INTC"], "AI Compute")
_tag(["AVGO","MRVL","ANET","CRDO","QCOM","COHR","LITE","APH"], "Networking/Optical")
_tag(["SMCI","DELL","CLS","JBL","FLEX"], "Servers/EMS")
_tag(["STX","WDC"], "Storage")
_tag(["VRT","VST","CEG","GEV","ETN","PWR","NVT","TT","JCI","CARR","CCJ","NEE","HUBB","ATKR"], "Power/Cooling/Grid")
_tag(["MSFT","META","GOOGL","AMZN","ORCL","CRWV"], "Hyperscaler")
_tag(["AAPL","SWKS","QRVO","CRUS","GLW","TXN","STM","GSAT","SYNA","ADI"], "Apple/Analog")
_tag(["BA","RTX","LMT","NOC","GD","GE","HWM","HEI","TDG","SPR","CW","MRCY","KTOS","LHX","HII","AXON","MP"], "Aero/Defense")
_tag(["ROK","AME","PH","DOV","MPWR","VICR"], "Automation/Power-semi")
_tag(["CMI","ALSN","DAN","WAB","GBX","TRN","CAT","DE","PCAR","UNP","CSX"], "Machinery/Rail")
_tag(["GM","F","TSLA","APTV","BWA","LEA","MGA","ALV","GNTX","AXL","NXPI","MCHP","ON","ALB","LAC","SQM","FCX"], "Auto/EV/Materials")
_tag(["XOM","CVX","SLB","HAL","BKR"], "Energy")
_tag(["LLY","PFE","MRK","WST","TMO","DHR","RGEN","CRL","A"], "Healthcare/Biopharma")

NODES = sorted({s for s, _, _ in EDGES} | {c for _, c, _ in EDGES})


def grouped(ds, nodeset):
    try:
        with urllib.request.urlopen(urllib.request.Request(
            f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{ds}?adjusted=true&apiKey={POLY}",
            headers={"User-Agent": "jh/1"}), timeout=25) as r:
            res = json.loads(r.read()).get("results") or []
        return ds, {x["T"]: x["c"] for x in res if x.get("T") in nodeset and x.get("c")}
    except Exception:
        return ds, {}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    nodeset = set(NODES)
    # grouped daily over last ~48 calendar days (~32 trading) → per-ticker series
    cal = [(date.today() - timedelta(days=i)).isoformat() for i in range(1, 50)]
    by_date = {}
    with ThreadPoolExecutor(max_workers=12) as ex:
        for ds, mp in ex.map(lambda d: grouped(d, nodeset), cal):
            if mp:
                by_date[ds] = mp
    dates = sorted(by_date.keys())
    perfs = {}
    for t in NODES:
        series = [by_date[d][t] for d in dates if t in by_date[d]]
        if len(series) >= 7:
            p30 = round((series[-1] / series[-22] - 1) * 100, 2) if len(series) >= 22 else None
            p5 = round((series[-1] / series[-6] - 1) * 100, 2)
            perfs[t] = {"perf_30d": p30, "perf_5d": p5, "price": round(series[-1], 2)}

    boom = set()
    try:
        br = json.loads(S3.get_object(Bucket=BUCKET, Key="data/boom-radar.json")["Body"].read())
        for r in (br.get("top_picks") or br.get("board") or br.get("high_conviction") or []):
            tk = r.get("ticker") if isinstance(r, dict) else r
            if tk:
                boom.add(str(tk).upper())
    except Exception as e:
        print(f"[scg] boom-radar unavailable: {str(e)[:60]}")

    customers_of, suppliers_of = {}, {}
    for s, c, rel in EDGES:
        customers_of.setdefault(s, []).append((c, rel))
        suppliers_of.setdefault(c, []).append((s, rel))

    deg = {}
    for s, c, _ in EDGES:
        deg[s] = deg.get(s, 0) + 1
        deg[c] = deg.get(c, 0) + 1

    nodes = []
    for t in NODES:
        p = perfs.get(t, {})
        p30 = p.get("perf_30d")
        is_boom = t in boom or (p30 is not None and p30 >= 25)
        nodes.append({"ticker": t, "theme": THEME.get(t, "Other"),
                      "perf_30d": p30, "perf_5d": p.get("perf_5d"), "price": p.get("price"),
                      "is_boom": is_boom, "degree": deg.get(t, 0),
                      "n_suppliers": len(suppliers_of.get(t, [])), "n_customers": len(customers_of.get(t, []))})
    edges = [{"supplier": s, "customer": c, "relationship": rel} for s, c, rel in EDGES]

    booming_hubs = [n["ticker"] for n in nodes if n["is_boom"]]
    laggards = []
    for hub in booming_hubs:
        hp = perfs.get(hub, {}).get("perf_30d")
        for sup, rel in suppliers_of.get(hub, []):
            sp = perfs.get(sup, {}).get("perf_30d")
            if sp is None or hp is None:
                continue
            gap = round(hp - sp, 1)
            if sp < 15 and gap >= 15:
                laggards.append({"ticker": sup, "theme": THEME.get(sup, "Other"), "supplies_to": hub,
                                 "relationship": rel, "customer_perf_30d": hp, "own_perf_30d": sp, "lag_gap_pct": gap})
    best = {}
    for l in laggards:
        k = l["ticker"]
        if k not in best or l["lag_gap_pct"] > best[k]["lag_gap_pct"]:
            best[k] = l
    laggards = sorted(best.values(), key=lambda x: x["lag_gap_pct"], reverse=True)
    top_picks = [{"ticker": l["ticker"], "direction": "long", "score": min(100, l["lag_gap_pct"]),
                  "supplies_to": l["supplies_to"], "own_perf_30d": l["own_perf_30d"],
                  "customer_perf_30d": l["customer_perf_30d"], "lag_gap_pct": l["lag_gap_pct"]} for l in laggards][:25]

    themes = sorted({n["theme"] for n in nodes})
    payload = {
        "engine": "justhodl-supply-chain-graph", "version": "1.1.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": ("Named supplier↔customer graph across semis/tech/datacenter/aero-defense/"
                   "industrial/auto/energy/biopharma. When a hub booms, its suppliers that "
                   "haven't moved are the lead-lag candidates (supply-chain return predictability)."),
        "n_nodes": len(nodes), "n_edges": len(edges), "n_themes": len(themes), "themes": themes,
        "booming_hubs": booming_hubs, "nodes": nodes, "edges": edges,
        "supply_chain_laggards": laggards[:40], "top_picks": top_picks,
        "data_source": "Curated supplier↔customer edge map + Polygon grouped-daily perf + boom-radar flags",
        "caveats": [
            "Curated high-conviction edges across the sectors where chain-pumps happen — NOT a "
            "paid full-universe graph (FMP supply-chain dead; Bloomberg SPLC / FactSet are paid). "
            "Supersedes the empty justhodl-supply-chain-linkage.",
            "MEASURE-BEFORE-TRUST: supply_chain_laggards → harvester (eng:supply-chain-graph), "
            "graded forward vs SPY; not in decision engines until alpha-proven. Complements "
            "rotation-chain (theme-tier lead-lag) with named relationships.",
            "Edges are static/curated; supply-chain lead-lag is a documented anomaly "
            "(Cohen & Frazzini, JF 2008). Refresh the map as relationships change.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[scg] nodes={len(nodes)} edges={len(edges)} themes={len(themes)} "
          f"booming={len(booming_hubs)} laggards={len(laggards)} in {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_nodes": len(nodes), "n_edges": len(edges), "n_themes": len(themes),
        "booming_hubs": booming_hubs[:12],
        "top_laggards": [(l["ticker"], l["supplies_to"], l["own_perf_30d"], l["customer_perf_30d"], l["lag_gap_pct"]) for l in laggards[:10]]})}
