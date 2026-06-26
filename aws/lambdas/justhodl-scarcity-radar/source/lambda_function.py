"""
justhodl-scarcity-radar  ·  v1.1  —  THE NEXT-SHORTAGE RADAR (scarcity x stealth)
================================================================================
The Micron/RAM pattern: a few producers, inelastic supply, a demand inflection,
inventories drawing down, then a violent equity re-rating. The equities are the
LAST leg to move, so the alpha is the vertical whose LEADING INDICATORS are just
inflecting while the stock is still cheap, hated and quiet. This synthesizer fuses
the eight shortage engines into one board on two axes:

  SCARCITY (is a real shortage building?)
    = 0.45 vertical tightness (supply-inflection: real spot/PPI)
    + 0.30 company capture   (bottleneck-boom: rev accel + cheap P/S + inventory draw)
    + 0.25 pricing power     (chokepoint: irreplaceable oligopoly)

  STEALTH (is no one looking yet?)
    = 0.30 cheap (low P/S / discount-to-fair) + 0.30 un-moved (supply-chain laggard)
    + 0.25 quiet (narrative-vs-tape quiet accumulation) + 0.15 early phase

  composite = sqrt(scarcity x stealth)  (BOTH must be high)
  PRIME tier = scarcity>=60 AND stealth>=55 — the next shortage before it's obvious.

OUTPUT: data/scarcity-radar.json     SCHEDULE: daily.  Research, not advice.
"""
import json, time, boto3, math
from datetime import datetime, timezone
from decimal import Decimal

S3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/scarcity-radar.json"
VERSION = "1.1.0"

VERTICAL = {
    "SMH": "Semiconductors / memory", "SOXX": "Semiconductors", "AIQ": "AI compute",
    "BOTZ": "Robotics / automation", "ROBO": "Robotics", "COPX": "Copper miners",
    "PICK": "Base / industrial metals", "SLX": "Steel", "XME": "Metals & mining",
    "URA": "Uranium", "URNM": "Uranium miners", "NLR": "Nuclear", "LIT": "Lithium / battery",
    "REMX": "Rare earths", "GRID": "Grid / electrification", "XLU": "Utilities / power",
    "XLE": "Energy", "XOP": "E&P", "OIH": "Oil services", "AMLP": "Midstream",
    "GDX": "Gold miners", "GDXJ": "Junior gold", "SIL": "Silver", "XLB": "Materials",
    "ITA": "Aerospace & defense", "PPA": "Defense", "IYT": "Transports / logistics",
    "WOOD": "Timber", "XHB": "Homebuilders", "SLV": "Silver", "PAVE": "Infrastructure",
}
IND_MAP = [
    ("semiconductor", "SMH"), ("memory", "SMH"), ("chip", "SMH"), ("foundry", "SMH"),
    ("copper", "COPX"), ("aluminum", "PICK"), ("nickel", "PICK"), ("metal", "PICK"),
    ("mining", "XME"), ("steel", "SLX"), ("iron", "SLX"), ("uranium", "URA"),
    ("nuclear", "NLR"), ("lithium", "LIT"), ("battery", "LIT"), ("rare earth", "REMX"),
    ("gold", "GDX"), ("silver", "SIL"), ("oil", "XLE"), ("gas", "XLE"), ("energy", "XLE"),
    ("pipeline", "AMLP"), ("aerospace", "ITA"), ("defense", "ITA"), ("utilit", "XLU"),
    ("electric", "GRID"), ("power", "GRID"), ("transport", "IYT"), ("rail", "IYT"),
    ("trucking", "IYT"), ("chemical", "XLB"), ("timber", "WOOD"), ("forest", "WOOD"),
    ("construction", "PAVE"), ("machinery", "PAVE"),
    # AI-infra adjacent (networking/storage/optical for datacenters)
    ("communication equipment", "AIQ"), ("networking", "AIQ"), ("optical", "AIQ"),
    ("connector", "AIQ"), ("computer hardware", "SMH"), ("data storage", "SMH"),
    ("storage", "SMH"), ("electronic component", "SMH"), ("instruments & controls", "SMH"),
]


def _read(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


def map_vertical(industry, sector=None, theme=None):
    if theme and str(theme).upper() in VERTICAL:
        return str(theme).upper()
    s = (str(industry or "") + " " + str(sector or "")).lower()
    for kw, etf in IND_MAP:
        if kw in s:
            return etf
    return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    si = _read("data/supply-inflection.json") or {}
    bb = _read("data/bottleneck-boom.json") or {}
    cp = _read("data/chokepoint.json") or {}
    scg = _read("data/supply-chain-graph.json") or {}
    nvt = _read("data/narrative-vs-tape.json") or {}
    themes = _read("data/themes-detected.json") or {}
    idr = _read("data/inventory-drawdown.json") or {}

    by_theme = si.get("by_theme") or {}
    tight = {etf.upper(): (v.get("composite_inflection_score") or 0)
             for etf, v in by_theme.items() if isinstance(v, dict)}
    # inventory-drawdown sector overlay: a falling inventories-to-sales ratio in a vertical is
    # tightening one layer earlier than spot/PPI — boost that vertical's tightness.
    inv_sector_tight = {}
    for s in (idr.get("sector_drawdown") or []):
        etf = (s.get("theme_etf") or "").upper()
        if not etf:
            continue
        bump = clamp((-(s.get("chg_6m") or 0)) * 7.0)   # -8% 6m I/S ≈ 56 tightness
        inv_sector_tight[etf] = max(inv_sector_tight.get(etf, 0), bump)
    for etf, bump in inv_sector_tight.items():
        tight[etf] = max(tight.get(etf, 0), 0.6 * tight.get(etf, 0) + 0.4 * bump) if etf in tight else bump
    phase_by_etf = {str(th.get("etf", "")).upper(): th.get("phase") for th in (themes.get("themes") or [])}
    PHASE_STEALTH = {"DORMANT": 60, "NASCENT": 90, "EMERGING": 100, "EARLY": 90,
                     "ACCELERATING": 70, "EXTENDED": 15, None: 50}

    verticals = sorted(
        [{"theme_etf": etf, "vertical": VERTICAL.get(etf, etf), "tightness": round(sc, 1),
          "phase": phase_by_etf.get(etf),
          "top_signals": [s.get("signal") for s in (by_theme.get(etf, {}).get("top_signals") or [])[:3]]}
         for etf, sc in tight.items() if sc],
        key=lambda x: -x["tightness"])

    names = {}

    def rec(tk):
        return names.setdefault(tk.upper(), {"ticker": tk.upper(), "engines": set(), "capture": 0.0,
                                             "pricing_power": 0.0, "cheap": 0.0, "un_moved": 0.0,
                                             "quiet": 0.0, "industry": None, "sector": None,
                                             "theme": None, "notes": []})

    for r in (bb.get("ranks") or []):
        tk = r.get("ticker")
        if not tk:
            continue
        a = rec(tk); a["engines"].add("bottleneck-boom")
        a["industry"] = a["industry"] or r.get("industry"); a["sector"] = a["sector"] or r.get("sector")
        a["capture"] = clamp(max(a["capture"], r.get("score") or 0))
        ps = r.get("ps_ttm")
        if isinstance(ps, (int, float)) and ps > 0:
            a["cheap"] = clamp(max(a["cheap"], 100 - min(100, ps * 8)))
        accel = r.get("rev_accel_pp")
        if isinstance(accel, (int, float)) and accel > 0:
            a["un_moved"] = clamp(max(a["un_moved"], 30)); a["notes"].append(f"rev accel +{round(accel,1)}pp")

    for bk in ("cheap_chokepoint_book", "hidden_chokepoint_book", "highest_conviction_book", "all_chokepoints"):
        for r in (cp.get(bk) or []):
            tk = r.get("ticker")
            if not tk:
                continue
            a = rec(tk); a["engines"].add("chokepoint")
            a["industry"] = a["industry"] or r.get("industry"); a["sector"] = a["sector"] or r.get("sector")
            a["pricing_power"] = clamp(max(a["pricing_power"], r.get("criticality") or 0))
            disc = r.get("discount_to_fair_pct") or r.get("discount_pct")
            if isinstance(disc, (int, float)) and disc > 0:
                a["cheap"] = clamp(max(a["cheap"], min(100, disc * 2))); a["notes"].append(f"{round(disc)}% below fair")

    for r in (scg.get("supply_chain_laggards") or []):
        tk = r.get("ticker")
        if not tk:
            continue
        a = rec(tk); a["engines"].add("supply-chain-laggard"); a["theme"] = a["theme"] or r.get("theme")
        gap = r.get("lag_gap_pct")
        if isinstance(gap, (int, float)) and gap > 0:
            a["un_moved"] = clamp(max(a["un_moved"], min(100, gap * 2.5)))
            a["notes"].append(f"supplies {r.get('supplies_to')}, lags {round(gap)}%")

    for r in (nvt.get("quiet_accumulation") or []):
        tk = r.get("ticker")
        if not tk:
            continue
        a = rec(tk); a["engines"].add("quiet-accumulation"); a["quiet"] = 80.0
        a["notes"].append("quiet accumulation (low buzz, smart money in)")

    # inventory drawdown — DIO falling into rising demand = the cupboard emptying (capture,
    # one layer earlier than spot/PPI). Pull boom setups + the broader stock drawdown board.
    for r in ((idr.get("boom_setups") or []) + (idr.get("stock_drawdown_board") or [])):
        tk = r.get("ticker")
        dio_chg = r.get("dio_chg_pct")
        if not tk or not isinstance(dio_chg, (int, float)) or dio_chg >= 0:
            continue
        a = rec(tk); a["engines"].add("inventory-drawdown")
        a["industry"] = a["industry"] or r.get("industry"); a["sector"] = a["sector"] or r.get("sector")
        boom = r.get("boom_score")
        cap = boom if isinstance(boom, (int, float)) and boom > 0 else clamp((-dio_chg) * 3.0)
        a["capture"] = clamp(max(a["capture"], cap))
        a["notes"].append(f"DIO {round(dio_chg,1)}% (drawing down)")

    book = []
    for tk, a in names.items():
        etf = map_vertical(a["industry"], a["sector"], a["theme"])
        vt = tight.get(etf, 50.0) if etf else 50.0
        phase = phase_by_etf.get(etf) if etf else None
        scarcity = round(clamp(0.45 * vt + 0.30 * a["capture"] + 0.25 * a["pricing_power"]), 1)
        stealth = round(clamp(0.30 * a["cheap"] + 0.30 * a["un_moved"] + 0.25 * a["quiet"]
                              + 0.15 * PHASE_STEALTH.get(phase, 50)), 1)
        composite = round(math.sqrt(max(0, scarcity) * max(0, stealth)), 1)
        tier = "PRIME" if (scarcity >= 60 and stealth >= 55) else (
            "CANDIDATE" if (scarcity >= 45 and stealth >= 40) else "WATCH")
        book.append({"ticker": tk, "tier": tier, "scarcity": scarcity, "stealth": stealth,
                     "composite": composite, "vertical": VERTICAL.get(etf, etf) if etf else None,
                     "vertical_tightness": round(vt, 1), "phase": phase, "n_engines": len(a["engines"]),
                     "engines": sorted(a["engines"]), "industry": a["industry"], "why": "; ".join(a["notes"][:4])})
    book.sort(key=lambda r: -r["composite"])
    prime = [r for r in book if r["tier"] == "PRIME"]
    out = {
        "engine": "scarcity-radar", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "thesis": ("The next Micron before it's a Micron: fuses real spot/PPI tightness, company capture, "
                   "oligopoly pricing power, supply-chain laggards and quiet accumulation into a scarcity x stealth "
                   "score — the vertical tightening hard while its equities are still cheap, hated and quiet."),
        "method": "scarcity = 0.45 vertical-tightness + 0.30 capture + 0.25 pricing-power; "
                  "stealth = 0.30 cheap + 0.30 un-moved + 0.25 quiet + 0.15 early-phase; composite = sqrt(scarcity x stealth)",
        "vertical_tightness": verticals[:18],
        "stealth_shortage_board": book[:25],
        "prime_setups": prime[:15],
        "counts": {"names": len(book), "prime": len(prime),
                   "candidates": sum(1 for r in book if r["tier"] in ("PRIME", "CANDIDATE")),
                   "verticals_tightening": sum(1 for v in verticals if v["tightness"] >= 55)},
        "legend": {"PRIME": "shortage building AND still ignored (scarcity>=60 & stealth>=55)",
                   "CANDIDATE": "building but not yet extreme", "WATCH": "one axis only"},
        "sources": ["supply-inflection (real spot/PPI)", "inventory-drawdown (DIO + sector I/S)",
                    "bottleneck-boom", "chokepoint", "supply-chain-graph", "narrative-vs-tape", "themes-detected"],
        "disclaimer": "Synthesis of the platform's own engines — research, not advice.",
    }
    try:
        nowt = datetime.now(timezone.utc)
        tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
        logged = 0
        for r in (prime or book)[:8]:
            tbl.put_item(Item={
                "signal_id": f"scarcity-radar#{r['ticker']}#{nowt.date().isoformat()}",
                "signal_type": "scarcity_radar", "predicted_direction": "UP",
                "signal_value": str(r["composite"]), "confidence": Decimal("0.55"),
                "measure_against": "ticker_vs_benchmark", "benchmark": "SPY",
                "check_windows": ["day_5", "day_21", "day_63"], "outcomes": {}, "accuracy_scores": {},
                "status": "pending", "logged_at": nowt.isoformat(), "logged_epoch": int(nowt.timestamp()),
                "horizon_days_primary": 63, "schema_version": "2", "ttl": int(nowt.timestamp()) + 150 * 86400,
                "metadata": {"engine": "scarcity-radar", "v": VERSION, "tier": r["tier"],
                             "scarcity": r["scarcity"], "stealth": r["stealth"], "vertical": r["vertical"]},
                "rationale": f"{r['ticker']} scarcity {r['scarcity']} x stealth {r['stealth']} ({r['vertical']})"})
            logged += 1
        out["signals_logged"] = logged
    except Exception as e:
        print(f"[loop] {str(e)[:80]}")
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[scarcity-radar] names={len(book)} prime={len(prime)} "
          f"verticals_tightening={out['counts']['verticals_tightening']} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps(out["counts"])}
