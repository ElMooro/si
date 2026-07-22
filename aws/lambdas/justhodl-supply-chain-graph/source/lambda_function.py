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
    # ── NEOCLOUD / AI-DATACENTER CLUSTER (ops 3701) ──────────────────────────
    # The GPU-cloud and converted-miner operators are where the AI capex cycle
    # physically lands. Without them on the graph an NBIS/IREN/APLD capex print
    # has no suppliers to read through to and falls back to meaningless
    # same-industry software peers. Category-level curation, house standard.
    ("NVDA", "NBIS", "GPUs"), ("NVDA", "IREN", "GPUs"), ("NVDA", "APLD", "GPUs"),
    ("NVDA", "WULF", "GPUs"), ("NVDA", "CIFR", "GPUs"),
    ("DELL", "NBIS", "AI servers"), ("DELL", "IREN", "AI servers"),
    ("SMCI", "NBIS", "AI servers"), ("SMCI", "CRWV", "AI servers"),
    ("SMCI", "IREN", "AI servers"), ("SMCI", "APLD", "AI servers"),
    ("VRT", "NBIS", "liquid cooling/power"), ("VRT", "IREN", "liquid cooling"),
    ("VRT", "APLD", "liquid cooling/power"), ("VRT", "WULF", "cooling/power"),
    ("VRT", "CIFR", "cooling/power"), ("VRT", "GDS", "cooling/power"),
    ("ETN", "NBIS", "power distribution"), ("ETN", "CRWV", "power distribution"),
    ("ETN", "IREN", "power distribution"), ("ETN", "APLD", "switchgear/power"),
    ("ETN", "WULF", "switchgear/power"), ("ETN", "CIFR", "switchgear/power"),
    ("ETN", "GDS", "power distribution"),
    ("PWR", "APLD", "grid/EPC construction"), ("PWR", "WULF", "grid/EPC construction"),
    ("PWR", "CIFR", "grid/EPC construction"),
    ("GEV", "APLD", "grid/turbines"), ("GEV", "WULF", "grid/turbines"),
    ("NVT", "CRWV", "enclosures/liquid cooling"), ("NVT", "APLD", "enclosures/cooling"),
    ("NVT", "WULF", "enclosures/cooling"), ("NVT", "CIFR", "enclosures/cooling"),
    ("HUBB", "APLD", "electrical"), ("HUBB", "WULF", "electrical"), ("HUBB", "CIFR", "electrical"),
    ("MOD", "APLD", "datacenter cooling"), ("MOD", "CIFR", "datacenter cooling"),
    ("ANET", "CRWV", "switching"), ("ANET", "NBIS", "switching"),
    ("CRDO", "CRWV", "AEC connectivity"), ("CRDO", "NBIS", "AEC connectivity"),
    ("COHR", "CRWV", "optical interconnect"), ("LITE", "NBIS", "optical"),
    ("CIEN", "CRWV", "DCI optical transport"), ("CIEN", "NBIS", "DCI optical transport"),
    ("CIEN", "GDS", "DCI optical transport"),
    # neoclouds are themselves SUPPLIERS of compute to the hyperscalers — this is
    # the edge that makes them read through when a hyperscaler capex print lands.
    ("CRWV", "MSFT", "GPU cloud capacity"), ("CRWV", "META", "GPU cloud capacity"),
    ("NBIS", "MSFT", "GPU cloud capacity"), ("IREN", "MSFT", "GPU cloud capacity"),
    ("APLD", "CRWV", "AI datacenter leases"),
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
_tag(["AVGO","MRVL","ANET","CRDO","QCOM","COHR","LITE","APH","CIEN"], "Networking/Optical")
_tag(["SMCI","DELL","CLS","JBL","FLEX"], "Servers/EMS")
_tag(["STX","WDC"], "Storage")
_tag(["VRT","VST","CEG","GEV","ETN","PWR","NVT","TT","JCI","CARR","CCJ","NEE","HUBB","ATKR","MOD"], "Power/Cooling/Grid")
_tag(["MSFT","META","GOOGL","AMZN","ORCL"], "Hyperscaler")
_tag(["NBIS","CRWV","IREN","APLD","WULF","CIFR","GDS"], "Neocloud/AI-DC")
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


REL_KEY = "data/polygon-related-graph.json"


def _related_one(t):
    try:
        with urllib.request.urlopen(urllib.request.Request(
            f"https://api.polygon.io/v1/related-companies/{t}?apiKey={POLY}",
            headers={"User-Agent": "jh/1"}), timeout=12) as r:
            j = json.loads(r.read())
        return t, [x.get("ticker") for x in (j.get("results") or []) if x.get("ticker")]
    except Exception as e:
        print(f"[scg] related {t}: {str(e)[:50]}")
        return t, []


def _related_graph(tickers):
    """Polygon market-inferred relatedness (news co-mention + return similarity).
    Cached to S3 and reused for 6 days — relatedness is slow-moving."""
    try:
        doc = json.loads(S3.get_object(Bucket=BUCKET, Key=REL_KEY)["Body"].read())
        age_d = (datetime.now(timezone.utc)
                 - datetime.fromisoformat(doc["generated_at"])).total_seconds() / 86400
        if age_d < 6 and set(tickers) <= set(doc.get("by_ticker") or {}):
            print(f"[scg] related-graph cache hit (age {age_d:.1f}d)")
            return doc["by_ticker"]
    except Exception:
        pass
    rel = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        for t, rs in ex.map(_related_one, sorted(tickers)):
            rel[t] = rs
            time.sleep(0.05)
    mutual = sorted({tuple(sorted((a, b))) for a in rel for b in rel.get(a, [])
                     if b in rel and a in rel.get(b, [])})
    S3.put_object(Bucket=BUCKET, Key=REL_KEY,
                  Body=json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(),
                                   "n": len(rel), "by_ticker": rel,
                                   "mutual_pairs": [list(p) for p in mutual],
                                   "source": "polygon v1/related-companies (news co-mention + return similarity)"},
                                  separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[scg] related-graph rebuilt: {len(rel)} tickers, {len(mutual)} mutual pairs")
    return rel


def lambda_handler(event=None, context=None):
    t0 = time.time()
    nodeset = set(NODES)

    # ── ops 2706: dual-source graph. Layer 2 = Polygon relatedness, which
    #    (a) CONFIRMS curated edges when the market agrees, and (b) DISCOVERS
    #    liquidity-gated peer candidates beyond the curated hubs — the exact
    #    "paid full-universe graph" gap named in v1's caveats, closed keyless.
    rel = _related_graph(nodeset)
    def _confirm(a, b):
        f, r = b in (rel.get(a) or []), a in (rel.get(b) or [])
        return "mutual" if (f and r) else "one_way" if (f or r) else "none"
    edge_confirm = {(a, b): _confirm(a, b) for a, b, _ in EDGES}

    cand, cand_via, seen = [], {}, set(nodeset)
    try:
        import finviz as FV
        uni = FV.load_universe()
    except Exception as e:
        print(f"[scg] finviz universe unavailable ({str(e)[:40]}) — discovery gated off")
        uni = {}
    for hub in sorted(nodeset):
        added = 0
        for r in rel.get(hub) or []:
            if r in seen or added >= 2 or len(cand) >= 40:
                continue
            u = uni.get(r) or {}
            if u.get("asset_type") or u.get("etf_type"):
                continue
            px = u.get("price") or u.get("prev_close") or 0
            if px < 5 or (u.get("market_cap") or 0) < 2000 or (u.get("avg_volume") or 0) * 1000 < 500_000:
                continue
            seen.add(r); cand.append(r); cand_via.setdefault(r, hub); added += 1
    nodeset |= set(cand)
    print(f"[scg] confirm: mutual={sum(1 for v in edge_confirm.values() if v=='mutual')} "
          f"one_way={sum(1 for v in edge_confirm.values() if v=='one_way')} | discovered={len(cand)}")
    # grouped daily over last ~48 calendar days (~32 trading) → per-ticker series
    cal = [(date.today() - timedelta(days=i)).isoformat() for i in range(1, 50)]
    by_date = {}
    with ThreadPoolExecutor(max_workers=12) as ex:
        for ds, mp in ex.map(lambda d: grouped(d, nodeset), cal):
            if mp:
                by_date[ds] = mp
    dates = sorted(by_date.keys())
    perfs = {}
    ALL_TICKERS = list(NODES) + cand
    for t in ALL_TICKERS:
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
    for s, c, rl in EDGES:
        customers_of.setdefault(s, []).append((c, rl))
        suppliers_of.setdefault(c, []).append((s, rl))
    DISC_REL = "market-inferred peer (Polygon relatedness)"
    for r in cand:
        hub = cand_via[r]
        suppliers_of.setdefault(hub, []).append((r, DISC_REL))
        customers_of.setdefault(r, []).append((hub, DISC_REL))

    deg = {}
    for s, c, _ in EDGES:
        deg[s] = deg.get(s, 0) + 1
        deg[c] = deg.get(c, 0) + 1
    for r in cand:
        deg[r] = deg.get(r, 0) + 1
        deg[cand_via[r]] = deg.get(cand_via[r], 0) + 1

    nodes = []
    for t in ALL_TICKERS:
        p = perfs.get(t, {})
        p30 = p.get("perf_30d")
        is_boom = t in boom or (p30 is not None and p30 >= 25)
        nodes.append({"ticker": t,
                      "theme": THEME.get(t, "Market-Inferred" if t in cand_via else "Other"),
                      "origin": "polygon" if t in cand_via else "curated",
                      "perf_30d": p30, "perf_5d": p.get("perf_5d"), "price": p.get("price"),
                      "is_boom": is_boom, "degree": deg.get(t, 0),
                      "n_suppliers": len(suppliers_of.get(t, [])), "n_customers": len(customers_of.get(t, []))})
    edges = [{"supplier": s, "customer": c, "relationship": rl,
              "source": "curated", "confirm": edge_confirm[(s, c)]} for s, c, rl in EDGES]
    edges += [{"supplier": r, "customer": cand_via[r], "relationship": DISC_REL,
               "source": "polygon", "confirm": "one_way", "direction": "undirected"}
              for r in cand]

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
    top_picks = [{"ticker": l["ticker"], "direction": "long",
                  "score": round(min(100, l["lag_gap_pct"]) * (0.8 if "market-inferred" in (l.get("relationship") or "") else 1.0), 1),
                  "edge_source": "polygon" if "market-inferred" in (l.get("relationship") or "") else "curated",
                  "supplies_to": l["supplies_to"], "own_perf_30d": l["own_perf_30d"],
                  "customer_perf_30d": l["customer_perf_30d"], "lag_gap_pct": l["lag_gap_pct"]} for l in laggards][:25]

    themes = sorted({n["theme"] for n in nodes})
    payload = {
        "engine": "justhodl-supply-chain-graph", "version": "2.1.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": ("Named supplier↔customer graph across semis/tech/datacenter/aero-defense/"
                   "industrial/auto/energy/biopharma. When a hub booms, its suppliers that "
                   "haven't moved are the lead-lag candidates (supply-chain return predictability)."),
        "n_nodes": len(nodes), "n_edges": len(edges), "n_themes": len(themes), "themes": themes,
        "graph_stats": {"curated_edges": len(EDGES),
                        "confirmed_mutual": sum(1 for v in edge_confirm.values() if v == "mutual"),
                        "confirmed_one_way": sum(1 for v in edge_confirm.values() if v == "one_way"),
                        "unconfirmed": sum(1 for v in edge_confirm.values() if v == "none"),
                        "discovered_nodes": len(cand), "related_graph_key": REL_KEY},
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
            "Curated edges now market-CONFIRMED against Polygon relatedness (news co-mention + "
            "return similarity); dashed discovered edges are market-inferred peers, liquidity-gated, "
            "with a 0.8x score haircut until the harvester proves the tier. Supply-chain lead-lag "
            "is a documented anomaly (Cohen & Frazzini, JF 2008).",
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
