"""
justhodl-metals-miners  the metals & miners catch-up screen.

A miner is a leveraged call on the metal it digs. The naive screen ranks
miners on cheapness or momentum in a vacuum and walks straight into two traps:

  * A cheap-looking miner on a metal that is rolling over is a value trap.
    Operating leverage cuts BOTH ways  the same leverage that prints money
    in an up-cycle destroys equity in a down-cycle. The metal trend is the
    master variable, not the miner's multiple.
  * "Gold miners" is not one bucket. Gold (monetary / real-rate driven),
    silver (hybrid), copper (industrial / China demand), uranium (a supply
    deficit + nuclear build-out) and lithium each run on a DIFFERENT cycle.
    Each complex is anchored to its own metal-trend regime.

This engine builds, per complex:
  1. a metal-trend regime from a liquid proxy ETF (real FMP returns),
  2. a curated universe of producers, royalty/streaming names, developers
     and the buyable metal / miner ETFs,
  3. a catch-up score that rewards a healthy miner that has LAGGED a metal
     which is genuinely working (earnings revisions lag spot  the operating
     leverage has not been priced yet), and penalises chasing leverage into
     a falling metal,
  4. a metal-trend-anchored price target propagated through each name's
     operating-leverage class.

Survivability is gauged from Altman-Z (house screener) and market-cap tier;
true AISC / cost curves are not on the free data plane and that limit is
stated honestly on every card. Royalty/streaming names are flagged as the
lower-volatility expression; sub-scale developers carry binary permit and
financing risk and are flagged as such.

INPUT   screener/data.json (house universe: beta, Altman-Z, targets)
        + FMP /stable/stock-price-change (real trailing returns)
        + FMP /stable/profile (price / cap / beta for names off-universe)
OUTPUT  screener/metals-miners.json          SCHEDULE  daily 14:10 UTC
Real data only. Research, not investment advice.
"""
import json
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "screener/metals-miners.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"
WORKERS = 8

# ---------------------------------------------------------------- universe --
# Curated, all US-listed. type: etf_metal (regime proxy) / etf_basket /
# royalty / major / producer / developer. lev = operating-leverage class
# multiple applied to metal moves (royalty steady ~1.3x, majors ~2x,
# producers ~2.4x, developers ~3x, basket ETFs ~1.8x, metal ETF 1.0x).
COMPLEXES = {
    "gold": {
        "label": "Gold", "proxy": "GLD",
        "driver": "monetary debasement, real rates and central-bank buying",
        "members": [
            ("GLD", "etf_metal", 1.0), ("GDX", "etf_basket", 1.9),
            ("GDXJ", "etf_basket", 2.5),
            ("FNV", "royalty", 1.3), ("WPM", "royalty", 1.3),
            ("RGLD", "royalty", 1.3), ("OR", "royalty", 1.4),
            ("SAND", "royalty", 1.5),
            ("NEM", "major", 2.0), ("GOLD", "major", 2.1),
            ("AEM", "major", 1.9), ("KGC", "producer", 2.4),
            ("AU", "producer", 2.5), ("GFI", "producer", 2.4),
            ("HMY", "producer", 2.9), ("EGO", "producer", 2.6),
            ("BTG", "producer", 2.6), ("IAG", "producer", 2.8),
            ("NGD", "developer", 3.0),
        ],
    },
    "silver": {
        "label": "Silver", "proxy": "SLV",
        "driver": "a monetary + industrial (solar / electronics) hybrid bid",
        "members": [
            ("SLV", "etf_metal", 1.0), ("SIL", "etf_basket", 2.2),
            ("SILJ", "etf_basket", 2.8),
            ("PAAS", "producer", 2.4), ("AG", "producer", 2.9),
            ("HL", "producer", 2.7), ("CDE", "producer", 2.8),
            ("EXK", "developer", 3.1), ("MAG", "developer", 2.6),
            ("GATO", "producer", 2.8),
        ],
    },
    "copper": {
        "label": "Copper", "proxy": "CPER",
        "driver": "industrial demand, the electrification build-out and China",
        "members": [
            ("CPER", "etf_metal", 1.0), ("COPX", "etf_basket", 2.0),
            ("FCX", "major", 2.2), ("SCCO", "major", 2.0),
            ("TECK", "major", 2.1), ("BHP", "major", 1.7),
            ("RIO", "major", 1.7), ("VALE", "major", 1.9),
            ("ERO", "producer", 2.7), ("HBM", "producer", 2.6),
        ],
    },
    "uranium": {
        "label": "Uranium", "proxy": "URA",
        "driver": "a structural supply deficit and the nuclear new-build cycle",
        "members": [
            ("URA", "etf_basket", 1.0), ("URNM", "etf_basket", 1.2),
            ("CCJ", "major", 2.0), ("NXE", "developer", 3.0),
            ("UEC", "developer", 2.9), ("DNN", "developer", 3.0),
            ("UUUU", "producer", 2.7), ("URG", "developer", 3.1),
            ("LEU", "producer", 2.8),
        ],
    },
    "lithium": {
        "label": "Lithium", "proxy": "LIT",
        "driver": "EV-battery demand against a brutal post-2023 price reset",
        "members": [
            ("LIT", "etf_basket", 1.0),
            ("ALB", "major", 2.3), ("SQM", "major", 2.1),
            ("LAC", "developer", 3.0), ("PLL", "developer", 3.1),
        ],
    },
}

ALL_SYMBOLS = sorted({m[0] for c in COMPLEXES.values() for m in c["members"]})

NAMES = {
    "GLD": "SPDR Gold Shares", "GDX": "VanEck Gold Miners ETF",
    "GDXJ": "VanEck Junior Gold Miners ETF", "FNV": "Franco-Nevada",
    "WPM": "Wheaton Precious Metals", "RGLD": "Royal Gold",
    "OR": "Osisko Gold Royalties", "SAND": "Sandstorm Gold",
    "NEM": "Newmont", "GOLD": "Barrick Gold", "AEM": "Agnico Eagle Mines",
    "KGC": "Kinross Gold", "AU": "AngloGold Ashanti", "GFI": "Gold Fields",
    "HMY": "Harmony Gold", "EGO": "Eldorado Gold", "BTG": "B2Gold",
    "IAG": "IAMGOLD", "NGD": "New Gold", "SLV": "iShares Silver Trust",
    "SIL": "Global X Silver Miners ETF", "SILJ": "Amplify Junior Silver ETF",
    "PAAS": "Pan American Silver", "AG": "First Majestic Silver",
    "HL": "Hecla Mining", "CDE": "Coeur Mining", "EXK": "Endeavour Silver",
    "MAG": "MAG Silver", "GATO": "Gatos Silver",
    "CPER": "US Copper Index Fund", "COPX": "Global X Copper Miners ETF",
    "FCX": "Freeport-McMoRan", "SCCO": "Southern Copper", "TECK": "Teck Resources",
    "BHP": "BHP Group", "RIO": "Rio Tinto", "VALE": "Vale",
    "ERO": "Ero Copper", "HBM": "Hudbay Minerals",
    "URA": "Global X Uranium ETF", "URNM": "Sprott Uranium Miners ETF",
    "CCJ": "Cameco", "NXE": "NexGen Energy", "UEC": "Uranium Energy",
    "DNN": "Denison Mines", "UUUU": "Energy Fuels", "URG": "Ur-Energy",
    "LEU": "Centrus Energy", "LIT": "Global X Lithium & Battery ETF",
    "ALB": "Albemarle", "SQM": "Sociedad Quimica y Minera",
    "LAC": "Lithium Americas", "PLL": "Piedmont Lithium",
}

GAIN_CEIL = 42.0   # implied-gain cap on a price target (%)


# ---------------------------------------------------------------- helpers --
def num(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def fmp_price_change(sym):
    """Real trailing returns for one symbol (1M/3M/6M/1Y, %)."""
    url = f"{BASE}/stock-price-change?symbol={sym}&apikey={FMP}"
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            d = json.loads(r.read())
        row = d[0] if isinstance(d, list) and d else (
            d if isinstance(d, dict) else {})
        return sym, {
            "r1m": num(row.get("1M")), "r3m": num(row.get("3M")),
            "r6m": num(row.get("6M")), "r1y": num(row.get("1Y")),
        }
    except Exception:
        return sym, None


def fmp_profile(sym):
    """Price / cap / beta / country for names not in the house universe."""
    url = f"{BASE}/profile?symbol={sym}&apikey={FMP}"
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            d = json.loads(r.read())
        row = d[0] if isinstance(d, list) and d else (
            d if isinstance(d, dict) else {})
        return sym, {
            "price": num(row.get("price")),
            "market_cap": num(row.get("marketCap")),
            "beta": num(row.get("beta")),
            "country": row.get("country"),
            "name": row.get("companyName"),
        }
    except Exception:
        return sym, None


def load_house_universe():
    """Pre-computed fundamentals from the house screener (Altman-Z, targets)."""
    try:
        sc = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="screener/data.json")["Body"].read())
    except Exception:
        return {}
    rows = sc.get("stocks")
    if not isinstance(rows, list):
        bs = sc.get("by_symbol") or {}
        rows = list(bs.values()) if isinstance(bs, dict) else []
    out = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        sy = (r.get("symbol") or "").upper()
        if not sy:
            continue
        out[sy] = {
            "name": r.get("name"),
            "beta": num(r.get("beta")),
            "market_cap": num(r.get("marketCap")),
            "altman_z": num(r.get("altmanZ")),
            "price": num(r.get("price")),
            "target": num(r.get("priceTargetMedian"))
            or num(r.get("priceTargetMean")),
            "target_upside": num(r.get("priceTargetUpsidePct")),
        }
    return out


# --------------------------------------------------------- metal regimes --
def metal_regime(pc):
    """Classify a complex's metal-trend regime from its proxy returns."""
    if not pc or pc.get("r3m") is None:
        return {"regime": "UNKNOWN", "score": 50, "favorable": False,
                "r1m": None, "r3m": None, "r6m": None}
    r1m, r3m = pc.get("r1m"), pc.get("r3m")
    r6m = pc.get("r6m")
    r6 = r6m if r6m is not None else r3m
    if r3m > 8 and r6 > 4:
        reg, fav = "STRONG UPTREND", True
    elif r3m > 2:
        reg, fav = "UPTREND", True
    elif r3m < -6:
        reg, fav = "DOWNTREND", False
    else:
        reg, fav = "NEUTRAL", False
    # 0-100 favourability score
    score = clamp(50 + r3m * 1.6 + r6 * 0.7 + (r1m or 0) * 0.5, 2, 99)
    return {"regime": reg, "score": round(score, 1), "favorable": fav,
            "r1m": r1m, "r3m": r3m, "r6m": r6m}


# ------------------------------------------------------------ survivability --
def survivability(rec, mtype):
    """0-100 balance-sheet survivability. Altman-Z where the house has it,
    else a market-cap-tier proxy. Metal ETFs carry no solvency risk."""
    if mtype in ("etf_metal", "etf_basket"):
        return 100, "ETF \u2014 no single-name solvency risk"
    z = rec.get("altman_z")
    cap = rec.get("market_cap") or 0
    if z is not None:
        if z >= 3.0:
            return 92, f"Altman-Z {z:.1f} \u2014 solid balance sheet"
        if z >= 1.8:
            return 68, f"Altman-Z {z:.1f} \u2014 adequate, watch debt"
        return 30, f"Altman-Z {z:.1f} \u2014 distress-zone, fragile to a metal downturn"
    # no Altman-Z: scale-tier proxy
    if cap >= 1.5e10:
        return 80, "Large diversified producer \u2014 scale buffers a down-cycle (Z n/a)"
    if cap >= 2e9:
        return 60, "Mid-scale producer \u2014 moderate cushion (Z n/a)"
    if cap >= 4e8:
        return 42, "Sub-scale \u2014 thin cushion, financing-dependent (Z n/a)"
    return 28, "Micro-cap developer \u2014 binary permit / financing risk (Z n/a)"


def grade(score):
    if score >= 78:
        return "A"
    if score >= 66:
        return "B"
    if score >= 52:
        return "C"
    return "D"


# ------------------------------------------------------------------ build --
def build_member(sym, mtype, lev, complex_key, cx, reg, house, pc):
    """Score one miner / ETF within its complex."""
    rec = dict(house.get(sym, {}))
    name = NAMES.get(sym) or rec.get("name") or sym
    price = rec.get("price")
    r3m = pc.get("r3m") if pc else None
    r1m = pc.get("r1m") if pc else None
    if price is None or r3m is None:
        return None

    metal_r3m = reg["r3m"]
    metal_r6m = reg["r6m"]
    # lag vs the metal: positive => the miner has under-run the metal's move
    lag = (metal_r3m - r3m) if metal_r3m is not None else 0.0

    surv, surv_note = survivability(rec, mtype)

    # ---- components -------------------------------------------------------
    # 1. metal alignment (35) - the master variable
    align = reg["score"]
    # 2. operating-leverage catch-up (25) - only counts in an UP regime
    if reg["favorable"] and lag > 0:
        # reward lag, scaled by the name's leverage class
        catchup = clamp(lag * 1.6 * (0.5 + lev / 4.0), 0, 100)
    elif not reg["favorable"]:
        catchup = 18.0   # no metal tailwind: catch-up thesis is void
    else:
        # favourable metal but the miner already led - less left in the move
        catchup = clamp(35 - abs(lag) * 0.8, 8, 45)
    # 3. survivability (25)
    # 4. recent participation (15) - is the name actually trending with peers
    part = clamp(50 + r3m * 1.4 + (r1m or 0) * 0.9, 4, 100)

    score = round(0.35 * align + 0.25 * catchup
                  + 0.25 * surv + 0.15 * part, 1)
    # hard penalty: leverage into a falling metal is a falling knife
    if reg["regime"] == "DOWNTREND" and mtype != "etf_metal":
        score = round(max(score - 24, 3), 1)

    # ---- price target -----------------------------------------------------
    # metal-trend-anchored: in a working metal, expect the lagged operating
    # leverage to feed through - a fraction of the gap, scaled by lev class.
    target = None
    implied = None
    if reg["favorable"] and lag > 0:
        conv = clamp(0.30 + 0.10 * (lev - 1.0), 0.30, 0.62)
        implied = clamp(lag * conv * (lev / 2.0), 0, GAIN_CEIL)
        if implied >= 3.0:
            target = round(price * (1 + implied / 100.0), 2)
            implied = round(implied, 1)
        else:
            implied = None
    # fall back to the house analyst target if no catch-up target was derived
    if target is None and rec.get("target") and rec["target"] > price:
        target = round(rec["target"], 2)
        implied = round((target / price - 1) * 100, 1)

    # ---- thesis + caveat --------------------------------------------------
    type_word = {
        "etf_metal": "the metal itself", "etf_basket": "a miner-basket ETF",
        "royalty": "a royalty / streaming name", "major": "a major producer",
        "producer": "a producer", "developer": "a developer",
    }[mtype]
    if reg["favorable"] and lag > 4:
        thesis = (f"{cx['label']} is in a {reg['regime'].lower()} on "
                  f"{cx['driver']}; {name} ({type_word}) has lagged the metal "
                  f"by {lag:.0f}pp over 3 months and the operating leverage "
                  f"has not yet been priced.")
    elif reg["favorable"]:
        thesis = (f"{cx['label']} is working ({reg['regime'].lower()}) and "
                  f"{name} is participating \u2014 a momentum-aligned hold rather "
                  f"than a fresh catch-up entry.")
    elif reg["regime"] == "DOWNTREND":
        thesis = (f"{cx['label']} is in a downtrend; {name}'s operating "
                  f"leverage is a headwind here \u2014 flagged, not endorsed.")
    else:
        thesis = (f"{cx['label']} is range-bound; {name} needs the metal to "
                  f"break out before the leverage thesis activates.")

    caveats = ["Operating leverage cuts both ways \u2014 it amplifies a metal "
               "downturn as hard as an up-cycle.",
               "Cost position is proxied by scale and solvency; true AISC / "
               "cost-curve data is not modelled."]
    if mtype == "developer":
        caveats.append("Developer-stage: carries binary permit, financing "
                        "and execution risk \u2014 size positions accordingly.")
    if mtype == "royalty":
        caveats.insert(0, "Royalty / streaming structure \u2014 the lower-"
                       "volatility, lower-leverage expression of the complex.")
    ctry = rec.get("country")
    if ctry and ctry not in ("US", "CA", "AU", None):
        caveats.append(f"Primary listing / jurisdiction exposure: {ctry} "
                       f"\u2014 factor in mining-jurisdiction risk.")

    return {
        "symbol": sym, "name": name, "complex": complex_key,
        "type": mtype, "leverage_class": lev,
        "price": price, "ret_1m": r1m, "ret_3m": r3m,
        "ret_6m": pc.get("r6m") if pc else None,
        "metal_lag_3m": round(lag, 1),
        "score": score, "grade": grade(score),
        "survivability": surv, "survivability_note": surv_note,
        "target": target, "implied_gain_pct": implied,
        "thesis": thesis, "caveats": caveats,
    }


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    house = load_house_universe()

    # real trailing returns + off-universe profiles, fanned out
    price_changes, profiles = {}, {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        pc_futs = {ex.submit(fmp_price_change, s): s for s in ALL_SYMBOLS}
        prof_need = [s for s in ALL_SYMBOLS
                     if s not in house or house[s].get("price") is None]
        pr_futs = {ex.submit(fmp_profile, s): s for s in prof_need}
        for f in as_completed(list(pc_futs) + list(pr_futs)):
            pass
        for f in pc_futs:
            sym, pc = f.result()
            if pc:
                price_changes[sym] = pc
        for f in pr_futs:
            sym, pr = f.result()
            if pr:
                profiles[sym] = pr

    # merge profile data into the house record where the house lacked it
    for sym, pr in profiles.items():
        h = house.setdefault(sym, {})
        for k, v in pr.items():
            if h.get(k) is None and v is not None:
                h[k] = v

    complexes_out = []
    all_candidates = []
    for ckey, cx in COMPLEXES.items():
        reg = metal_regime(price_changes.get(cx["proxy"]))
        members = []
        for sym, mtype, lev in cx["members"]:
            m = build_member(sym, mtype, lev, ckey, cx, reg,
                              house, price_changes.get(sym))
            if m:
                members.append(m)
        members.sort(key=lambda x: x["score"], reverse=True)
        complexes_out.append({
            "key": ckey, "label": cx["label"], "driver": cx["driver"],
            "proxy_etf": cx["proxy"], "metal_regime": reg["regime"],
            "metal_regime_score": reg["score"],
            "metal_ret_3m": reg["r3m"], "metal_ret_6m": reg["r6m"],
            "favorable": reg["favorable"], "miners": members,
        })
        all_candidates.extend(members)

    all_candidates.sort(key=lambda x: x["score"], reverse=True)
    top = all_candidates[:8]
    working = [c["label"] for c in complexes_out if c["favorable"]]

    if working:
        headline = ("Working complexes: " + ", ".join(working)
                    + f". Top catch-up name: {top[0]['name']} "
                    f"({top[0]['symbol']}, grade {top[0]['grade']})."
                    if top else "Working complexes: " + ", ".join(working))
    else:
        headline = ("No metal complex is in a confirmed uptrend \u2014 the "
                    "screen is defensive; wait for a metal to break out before "
                    "adding operating leverage.")

    payload = {
        "schema_version": "1.0",
        "engine": "justhodl-metals-miners",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 1),
        "headline": headline,
        "working_complexes": working,
        "complexes": complexes_out,
        "top_catch_up": top,
        "universe_count": len(all_candidates),
        "methodology": (
            "Each complex is anchored to a liquid proxy-ETF metal-trend "
            "regime (real FMP returns). A miner scores on metal alignment "
            "(35%), operating-leverage catch-up vs a working metal (25%), "
            "balance-sheet survivability (25%) and recent participation "
            "(15%). Leverage into a downtrending metal is penalised. Price "
            "targets propagate the lagged metal move through each name's "
            "operating-leverage class, capped at "
            f"{GAIN_CEIL:.0f}% implied gain."),
        "disclaimer": (
            "Research, not investment advice. Miners are leveraged bets on "
            "volatile metals; cost-curve / AISC data is not modelled and "
            "developer-stage names carry binary risk."),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=300")
    return {"statusCode": 200,
            "body": json.dumps({"complexes": len(complexes_out),
                                "universe": len(all_candidates),
                                "working": working})}
