"""
justhodl-ai-infra-stack — OWN THE WHOLE STACK
=============================================
Thesis: don't bet on which AI model/app wins — own the INFRASTRUCTURE every
winner must buy. This engine maps the full AI build-out stack, layer by layer,
and ranks the names in each — ACROSS ALL CAPS with a small-cap tilt (smaller =
more upside) — by momentum + accumulation flow + bottleneck + revenue inflection.

STACK LAYERS (ordered, primary assignment by this order):
  silicon -> equipment -> memory -> foundry -> networking -> optical ->
  power_grid -> cooling -> datacenter_buildout -> datacenter_reits

Each layer = curated canonical SEEDS (always shown, so the whole stack is mapped
even where quiet) PLUS universe names in the layer's industries that are
SIGNAL-BACKED (flow signal or strong momentum) — this surfaces the micro/nano
enablers that hardcoded lists miss.

OVERLAYS (existing fresh S3 outputs + FMP):
  universe (cap/industry) · FMP /stable price-change (1M/3M) + profile (mktCap)
  options-flow · stealth-accumulation · short-pressure · microcap-float-squeeze
  finra-short · volatility-squeeze · pre-pump-signals · revenue-acceleration
  bottleneck-boom (AI-infra supply-bottleneck flag, if available)

OUTPUT data/ai-infra-stack.json   SCHEDULE daily 13:45 UTC. Real data, research only.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ai-infra-stack.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
s3 = boto3.client("s3", region_name="us-east-1")

CAP_BOOST = {"nano": 30, "micro": 25, "small": 18, "mid": 8, "large": 3, "mega": 0}
SMALL_BUCKETS = {"nano", "micro", "small"}

# ordered layers: (key, label, description, seed_tickers, industry_keywords)
LAYERS = [
    ("silicon", "Silicon / Compute", "GPUs, accelerators, custom ASICs, analog/RF — the brains.",
     ["NVDA", "AMD", "AVGO", "MRVL", "QCOM", "ARM", "INTC", "TXN", "ADI", "ON", "MCHP",
      "MPWR", "LSCC", "SITM", "AMBA", "NVTS", "CRUS", "ALGM", "POWI", "SLAB", "RMBS",
      "SMTC", "SWKS", "QRVO", "AOSL", "HIMX", "NVEC", "INDI", "GSIT", "MX"],
     ["Semiconductors"]),
    ("equipment", "Semi Equipment / WFE", "Litho, etch, deposition, metrology, test, subsystems — the toolmakers.",
     ["AMAT", "LRCX", "KLAC", "ASML", "TER", "ENTG", "MKSI", "ONTO", "ACLS", "UCTT",
      "ACMR", "COHU", "FORM", "KLIC", "AEIS", "NVMI", "CAMT", "ICHR", "PLAB", "VECO",
      "AMKR", "AEHR", "ASYS", "CCMP", "KOPN"],
     ["Semiconductor Equipment & Materials"]),
    ("memory", "Memory / HBM", "DRAM, NAND, HBM, controllers — the AI memory wall.",
     ["MU", "WDC", "STX", "SIMO", "NTAP", "PSTG"], []),
    ("foundry", "Foundry / Fab", "Wafer fabrication capacity.",
     ["TSM", "GFS", "UMC"], []),
    ("networking", "Networking / Interconnect", "Switches, NICs, fabric — moving the bits in the datacenter.",
     ["ANET", "CSCO", "JNPR", "EXTR", "COMM", "CALX", "DGII", "NTGR"],
     ["Communication Equipment"]),
    ("optical", "Optical / Photonics", "Transceivers, lasers, optical components — the photonics layer.",
     ["COHR", "LITE", "FN", "AAOI", "POET", "MTSI", "CIEN", "INFN", "OCC", "EMKR", "LASR"], []),
    ("power_grid", "Power / Grid", "Generation, electrical gear, grid, nuclear/SMR — the energy bottleneck.",
     ["VST", "CEG", "NRG", "GEV", "ETN", "POWL", "HUBB", "NVT", "GNRC", "BE", "PLUG",
      "AES", "TLN", "SMR", "OKLO", "LEU", "CCJ", "EOSE", "FLNC", "AGX", "BWXT"],
     ["Utilities - Independent Power Producers", "Electrical Equipment & Parts"]),
    ("cooling", "Cooling / Thermal", "Liquid cooling, thermal management, datacenter HVAC.",
     ["VRT", "SMCI", "MOD", "AAON", "CARR", "JCI", "KULR"], []),
    ("datacenter_buildout", "Datacenter Build-out", "Electrical/mechanical EPC building the datacenters.",
     ["PWR", "MYRG", "PRIM", "EME", "STRL", "IESC", "FIX", "APG", "ROAD"],
     ["Engineering & Construction"]),
    ("datacenter_reits", "Datacenter REITs", "Colocation / hyperscale real estate.",
     ["EQIX", "DLR", "IRM"], ["REIT - Specialty"]),
]


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[read] {key}: {e}")
        return None


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _fmp(path):
    url = f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
    try:
        raw = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "jh-aiis"}), timeout=12).read()
        return json.loads(raw)
    except Exception:
        return None


def fmp_changes(symbol):
    d = _fmp(f"stock-price-change?symbol={urllib.parse.quote(symbol)}")
    if isinstance(d, list) and d:
        d = d[0]
    if isinstance(d, dict):
        return _num(d.get("1M")), _num(d.get("3M"))
    return None, None


def fmp_mktcap(symbol):
    d = _fmp(f"profile?symbol={urllib.parse.quote(symbol)}")
    if isinstance(d, list) and d:
        d = d[0]
    if isinstance(d, dict):
        return _num(d.get("marketCap"))
    return None


def bucket_from_mc(mc):
    if not mc:
        return ""
    if mc < 5e7:
        return "nano"
    if mc < 3e8:
        return "micro"
    if mc < 2e9:
        return "small"
    if mc < 1e10:
        return "mid"
    if mc < 2e11:
        return "large"
    return "mega"


def build_universe_index(universe):
    idx, by_ind = {}, {}
    for s in (universe or {}).get("stocks", []):
        sym = s.get("symbol")
        if not sym:
            continue
        idx[sym] = {"name": s.get("name"), "industry": s.get("industry") or "",
                    "market_cap": s.get("market_cap"), "cap_bucket": s.get("cap_bucket") or ""}
        if idx[sym]["industry"]:
            by_ind.setdefault(idx[sym]["industry"], []).append(sym)
    return idx, by_ind


def build_flow_index():
    flows = {}

    def add(sym, t, extra=None):
        if sym:
            flows.setdefault(sym, []).append(t)

    def qual(doc, t, key="all_qualifying"):
        for q in (doc or {}).get(key, []) or []:
            if isinstance(q, dict):
                add(q.get("symbol"), t)

    qual(_read("data/options-flow.json"), "OPTIONS_UOA")
    st = _read("data/stealth-accumulation.json")
    for r in (st or {}).get("top_smart_money_only", []) or []:
        if (r.get("n_funds_buying") or 0) > 0 or (r.get("score") or 0) >= 70:
            add(r.get("ticker"), "SMART_MONEY_13F")
    for r in (st or {}).get("top_short_covering_only", []) or []:
        add(r.get("ticker"), "SHORT_COVERING")
    for n in (_read("data/short-pressure.json") or {}).get("names", []) or []:
        if "cover" in (n.get("state") or "").lower():
            add(n.get("ticker"), "SHORT_COVERING")
    qual(_read("data/microcap-float-squeeze.json"), "FLOAT_SQUEEZE")
    for r in (_read("data/finra-short.json") or {}).get("squeeze_candidates", []) or []:
        add(r.get("symbol"), "SHORT_SQUEEZE")
    qual(_read("data/volatility-squeeze.json"), "VOL_COILED_SPRING")
    qual(_read("data/pre-pump-signals.json"), "OBV_ACCUMULATION")
    ra = _read("data/revenue-acceleration.json")
    qual(ra, "REV_ACCELERATION")
    for q in ((ra or {}).get("summary") or {}).get("microcap_picks", []) or []:
        if isinstance(q, dict):
            add(q.get("symbol"), "REV_ACCEL_MICROCAP")
    return flows


def bottleneck_set():
    bb = _read("data/bottleneck-boom.json")
    out = set()
    if not bb:
        return out
    for k in ("all_qualifying", "candidates", "top_candidates"):
        for q in bb.get(k, []) or []:
            if isinstance(q, dict):
                s = q.get("symbol") or q.get("ticker")
                if s:
                    out.add(s)
    summ = bb.get("summary") or {}
    for k in ("top_25_overall", "tier_s", "tier_a"):
        for q in summ.get(k, []) or []:
            if isinstance(q, dict) and (q.get("symbol") or q.get("ticker")):
                out.add(q.get("symbol") or q.get("ticker"))
    return out


def lambda_handler(event, context):
    t0 = time.time()
    universe = _read("data/universe.json")
    uni, by_ind = build_universe_index(universe)
    flows = build_flow_index()
    bottleneck = bottleneck_set()
    rev_set = {q.get("symbol") for q in (_read("data/revenue-acceleration.json") or {}).get("all_qualifying", []) or []
               if isinstance(q, dict)}

    # 1) assemble candidate -> primary layer (first layer wins)
    primary, layer_of = {}, {}
    for key, label, desc, seeds, kws in LAYERS:
        for s in seeds:
            if s not in layer_of:
                layer_of[s] = key
                primary.setdefault(key, set()).add(s)
        for kw in kws:
            for sym in by_ind.get(kw, []):
                if sym not in layer_of:
                    # discovered (non-seed) — only if signal-backed (checked later); tag candidate
                    layer_of[sym] = key
                    primary.setdefault(key, set()).add(("disc", sym))

    # flatten symbols to fetch
    syms = set()
    for key, members in primary.items():
        for m in members:
            syms.add(m[1] if isinstance(m, tuple) else m)
    # fetch returns (all) + mktcap (missing from universe)
    sym_list = sorted(syms)
    ret = {}
    with ThreadPoolExecutor(max_workers=20) as ex:
        fut = {ex.submit(fmp_changes, s): s for s in sym_list}
        for f in as_completed(fut):
            ret[fut[f]] = f.result()
    missing_mc = [s for s in sym_list if not uni.get(s, {}).get("market_cap")]
    mc_extra = {}
    with ThreadPoolExecutor(max_workers=20) as ex:
        fut = {ex.submit(fmp_mktcap, s): s for s in missing_mc[:250]}
        for f in as_completed(fut):
            v = f.result()
            if v:
                mc_extra[fut[f]] = v

    def meta_for(sym):
        m = dict(uni.get(sym, {}))
        if not m.get("market_cap") and mc_extra.get(sym):
            m["market_cap"] = mc_extra[sym]
        if not m.get("cap_bucket"):
            m["cap_bucket"] = bucket_from_mc(m.get("market_cap"))
        return m

    stack, all_names = [], []
    for key, label, desc, seeds, kws in LAYERS:
        members = primary.get(key, set())
        rows = []
        for m in members:
            disc = isinstance(m, tuple)
            sym = m[1] if disc else m
            r1, r3 = ret.get(sym, (None, None))
            sig = flows.get(sym, [])
            # discovered names must be signal-backed or strongly trending
            if disc and not sig and not (r1 is not None and r1 > 15):
                continue
            mu = meta_for(sym)
            bkt = mu.get("cap_bucket") or ""
            small = bkt in SMALL_BUCKETS
            cb = CAP_BOOST.get(bkt, 5)
            mom = (r1 or 0) * 0.7 + (r3 or 0) * 0.3
            bn = sym in bottleneck
            rv = sym in rev_set
            comp = round(mom + len(sig) * 9 + (15 if bn else 0) + (12 if rv else 0) + cb, 1)
            rows.append({
                "symbol": sym, "name": mu.get("name"), "layer": key,
                "cap_bucket": bkt, "market_cap": mu.get("market_cap"), "is_small_cap": small,
                "ret_1m_pct": round(r1, 1) if r1 is not None else None,
                "ret_3m_pct": round(r3, 1) if r3 is not None else None,
                "flow_signals": sig, "bottleneck": bn, "rev_accel": rv,
                "source": "discovered" if disc else "seed", "composite": comp,
            })
        rows.sort(key=lambda x: x["composite"], reverse=True)
        moms = [x["ret_1m_pct"] for x in rows if x["ret_1m_pct"] is not None]
        stack.append({
            "layer": key, "label": label, "description": desc, "n_names": len(rows),
            "layer_heat_1m_pct": round(sum(moms) / len(moms), 1) if moms else None,
            "n_small_cap": sum(1 for x in rows if x["is_small_cap"]),
            "names": rows[:20],
        })
        all_names.extend(rows)

    # cross-stack top picks — small-cap tilted (composite already includes cap boost)
    top = sorted(all_names, key=lambda x: x["composite"], reverse=True)
    top_small = [x for x in top if x["is_small_cap"]][:15]
    hottest = sorted([s for s in stack if s["layer_heat_1m_pct"] is not None],
                     key=lambda s: s["layer_heat_1m_pct"], reverse=True)

    out = {
        "engine": "ai-infra-stack", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Own the infrastructure every AI winner must buy — agnostic to which model/app wins.",
        "summary": {
            "n_layers": len(stack), "n_names": len(all_names),
            "n_small_cap": sum(1 for x in all_names if x["is_small_cap"]),
            "hottest_layers": [{"layer": s["label"], "heat_1m_pct": s["layer_heat_1m_pct"]} for s in hottest[:5]],
            "top_picks": top[:20], "top_small_cap_picks": top_small,
        },
        "stack": stack,
        "methodology": {
            "membership": "curated canonical seeds per layer (always shown) + universe names in layer "
                          "industries that are signal-backed (flow signal or >15% 1M)",
            "cap_tilt": "composite adds cap boost nano+30/micro+25/small+18/mid+8/large+3/mega+0",
            "composite": "0.7*1M + 0.3*3M momentum + 9*n_flow_signals + 15*bottleneck + 12*rev_accel + cap_boost",
            "flow_signals": "options UOA / 13F / short-covering / float-squeeze / short-squeeze / "
                            "vol-coil / OBV-accum / rev-accel (small-cap sources included)",
        },
        "sources": ["universe", "FMP price-change+profile", "options-flow", "stealth-accumulation",
                    "short-pressure", "microcap-float-squeeze", "finra-short", "volatility-squeeze",
                    "pre-pump-signals", "revenue-acceleration", "bottleneck-boom"],
        "disclaimer": "Curated taxonomy + live overlays. Real data, research only — not advice.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[ai-infra-stack] layers={len(stack)} names={len(all_names)} "
          f"small={out['summary']['n_small_cap']} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_layers": len(stack),
            "n_names": len(all_names), "n_small_cap": out["summary"]["n_small_cap"]})}
