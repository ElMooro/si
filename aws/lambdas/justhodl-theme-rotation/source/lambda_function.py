"""
justhodl-theme-rotation — WHERE THE MONEY IS ROTATING (40+ themes, RRG-style)
=============================================================================
Sectors (11) are too coarse and AI is one theme; the market actually rotates across
dozens of investable themes. This maps ~40 of them on a Relative Rotation Graph:
  • RS-Ratio    = the theme's relative strength vs SPY over the last ~month (level)
  • RS-Momentum = whether that relative strength is ACCELERATING (recent month vs prior)
and assigns the classic four quadrants:
  IMPROVING (weak but accelerating) → money rotating IN, the early window
  LEADING   (strong + accelerating) → the current momentum
  WEAKENING (strong but decelerating) → money starting to rotate OUT
  LAGGING   (weak + decelerating) → avoid

Each theme is an ETF (or a basket of seeds where no clean ETF exists). The seed
tickers of IMPROVING/LEADING themes feed the truth ledger via the harvester, so the
system learns which rotation calls actually pay.

OUTPUT data/theme-rotation.json   SCHEDULE daily 13:30 UTC. Real prices, research only.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/theme-rotation.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
POLYGON = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
BENCH = "SPY"
s3 = boto3.client("s3", region_name="us-east-1")

# theme -> (etf or None for basket, [representative seed tickers])
THEMES = {
    "Nuclear / Uranium": ("URA", ["CCJ", "LEU", "OKLO", "SMR", "UEC"]),
    "Robotics / Automation": ("BOTZ", ["ISRG", "ROK", "TER", "PATH"]),
    "Quantum Computing": ("QTUM", ["IONQ", "RGTI", "QBTS", "ARQQ"]),
    "Cybersecurity": ("CIBR", ["PANW", "CRWD", "ZS", "FTNT", "S"]),
    "Defense": ("ITA", ["LMT", "RTX", "GD", "NOC", "LHX"]),
    "Space": ("ARKX", ["RKLB", "LUNR", "ASTS", "PL"]),
    "Solar / Clean Energy": ("TAN", ["FSLR", "ENPH", "SEDG", "RUN"]),
    "Semiconductors": ("SMH", ["NVDA", "AVGO", "TSM", "AMD", "MU"]),
    "AI / Cloud Software": ("IGV", ["MSFT", "CRM", "NOW", "PLTR"]),
    "Biotech": ("XBI", ["VRTX", "MRNA", "BIIB", "ALNY"]),
    "Genomics": ("ARKG", ["CRSP", "NTLA", "BEAM", "TWST"]),
    "GLP-1 / Obesity": (None, ["LLY", "NVO", "VKTX", "AMGN"]),
    "Cannabis": ("MSOS", ["CGC", "TLRY", "GTBIF", "CRON"]),
    "Blockchain / Crypto": ("BLOK", ["COIN", "MSTR", "MARA", "RIOT"]),
    "Lithium / Battery": ("LIT", ["ALB", "SQM"]),
    "Rare Earth / Critical Minerals": ("REMX", ["MP", "USAR"]),
    "Gold Miners": ("GDX", ["NEM", "GOLD", "AEM"]),
    "Silver Miners": ("SIL", ["PAAS", "WPM", "AG"]),
    "Copper": ("COPX", ["FCX", "SCCO", "TECK"]),
    "Energy": ("XLE", ["XOM", "CVX", "COP"]),
    "Oil Services": ("OIH", ["SLB", "HAL", "BKR"]),
    "Natural Gas": ("FCG", ["EQT", "AR", "RRC"]),
    "Homebuilders": ("XHB", ["DHI", "LEN", "PHM"]),
    "Regional Banks": ("KRE", ["RF", "KEY", "CFG"]),
    "Fintech": ("FINX", ["SQ", "PYPL", "AFRM", "HOOD"]),
    "5G / Comms": ("FIVG", ["QCOM", "AMT", "ERIC"]),
    "Water": ("PHO", ["XYL", "AWK", "ECL"]),
    "Agriculture": ("MOO", ["DE", "ADM", "NTR", "CTVA"]),
    "Shipping": ("SEA", ["ZIM", "SBLK", "GSL"]),
    "Airlines / Travel": ("JETS", ["DAL", "UAL", "AAL"]),
    "Retail": ("XRT", ["AMZN", "TGT", "COST"]),
    "Electric Vehicles": ("DRIV", ["TSLA", "RIVN", "LCID"]),
    "Hydrogen / Fuel Cell": ("HDRO", ["PLUG", "BE", "BLDP"]),
    "Infrastructure": ("PAVE", ["PWR", "VMC", "MLM"]),
    "Power / Grid": ("GRID", ["ETN", "GEV", "POWL", "AGX"]),
    "Sports Betting": ("BETZ", ["DKNG", "FLUT", "PENN"]),
    "Internet": ("FDN", ["META", "GOOGL", "NFLX"]),
    "Digital Infra REITs": ("SRVR", ["EQIX", "DLR", "AMT"]),
    "Software / SaaS": ("WCLD", ["SNOW", "DDOG", "NET", "CRWD"]),
    "Steel / Materials": ("SLX", ["NUE", "STLD", "CLF"]),
}


def _get(url):
    try:
        return urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "jh-rot"}), timeout=14).read()
    except Exception:
        return None


def hist(sym):
    q = urllib.parse.quote(sym)
    frm = (datetime.now(timezone.utc).date() - timedelta(days=120)).isoformat()
    for url in (f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol={q}&from={frm}&apikey={FMP}",
                f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={q}&from={frm}&apikey={FMP}"):
        b = _get(url)
        if not b:
            continue
        try:
            d = json.loads(b)
        except Exception:
            continue
        rows = d if isinstance(d, list) else (d.get("historical") if isinstance(d, dict) else None)
        if isinstance(rows, list) and rows:
            out = {}
            for r in rows:
                dt = str(r.get("date") or "")[:10]
                c = r.get("close", r.get("price"))
                if dt and isinstance(c, (int, float)):
                    out[dt] = float(c)
            if len(out) >= 30:
                return out
    b = _get(f"https://api.polygon.io/v2/aggs/ticker/{q}/range/1/day/{frm}/{datetime.now(timezone.utc).date()}?adjusted=true&sort=asc&limit=200&apiKey={POLYGON}")
    if b:
        try:
            res = (json.loads(b) or {}).get("results") or []
            out = {datetime.fromtimestamp(r["t"]/1000, timezone.utc).date().isoformat(): float(r["c"])
                   for r in res if r.get("t") and r.get("c")}
            if len(out) >= 30:
                return out
        except Exception:
            pass
    return {}


def ret(series, dates, n):
    """% return over the last n trading days on the aligned date axis."""
    vals = [series[d] for d in dates if d in series]
    if len(vals) <= n or vals[-1 - n] <= 0:
        return None
    return vals[-1] / vals[-1 - n] - 1.0


def ret_window(series, dates, a, b):
    """% return between t-a and t-b (a>b), e.g. prior month a=42,b=21."""
    vals = [series[d] for d in dates if d in series]
    if len(vals) <= a or vals[-1 - a] <= 0:
        return None
    return vals[-1 - b] / vals[-1 - a] - 1.0


def lambda_handler(event, context):
    t0 = time.time()
    need = {BENCH}
    for etf, seeds in THEMES.values():
        if etf:
            need.add(etf)
        else:
            need.update(seeds)
    closes = {}
    with ThreadPoolExecutor(max_workers=14) as ex:
        fut = {ex.submit(hist, s): s for s in need}
        for f in as_completed(fut):
            h = f.result()
            if h:
                closes[fut[f]] = h
    if BENCH not in closes:
        return {"statusCode": 500, "body": "benchmark history unavailable"}

    common = set(closes[BENCH].keys())
    for h in closes.values():
        common &= set(h.keys())
    dates = sorted(common)[-75:]
    if len(dates) < 30:
        dates = sorted(closes[BENCH].keys())[-75:]
    spy_1m = ret(closes[BENCH], dates, 21)
    spy_3m = ret(closes[BENCH], dates, min(63, len(dates) - 2))
    spy_prev = ret_window(closes[BENCH], dates, 42, 21)

    def theme_series(etf, seeds):
        if etf and etf in closes:
            return closes[etf]
        # basket: equal-weight average of available seed closes on the axis
        avail = [closes[s] for s in seeds if s in closes]
        if not avail:
            return None
        merged = {}
        for d in dates:
            vs = [h[d] for h in avail if d in h]
            if vs:
                merged[d] = sum(vs) / len(vs)
        return merged or None

    rows = []
    for theme, (etf, seeds) in THEMES.items():
        ser = theme_series(etf, seeds)
        if not ser:
            continue
        r1 = ret(ser, dates, 21)
        r3 = ret(ser, dates, min(63, len(dates) - 2))
        rprev = ret_window(ser, dates, 42, 21)
        if r1 is None or spy_1m is None:
            continue
        rs_ratio = round((r1 - spy_1m) * 100, 2)                     # relative strength level (~1m)
        rs_mom = None
        if rprev is not None and spy_prev is not None:
            rs_mom = round(((r1 - spy_1m) - (rprev - spy_prev)) * 100, 2)   # acceleration
        q = "UNKNOWN"
        if rs_mom is not None:
            if rs_ratio >= 0 and rs_mom >= 0:
                q = "LEADING"
            elif rs_ratio >= 0 and rs_mom < 0:
                q = "WEAKENING"
            elif rs_ratio < 0 and rs_mom >= 0:
                q = "IMPROVING"
            else:
                q = "LAGGING"
        score = round(rs_ratio + 1.8 * (rs_mom or 0), 1)
        rows.append({
            "theme": theme, "etf": etf or "(basket)", "is_basket": etf is None,
            "ret_1m_pct": round(r1 * 100, 1), "ret_3m_pct": round(r3 * 100, 1) if r3 is not None else None,
            "rs_ratio": rs_ratio, "rs_momentum": rs_mom, "quadrant": q,
            "rotation_score": score, "seeds": seeds,
        })
    rows.sort(key=lambda x: x["rotation_score"], reverse=True)

    improving = [r for r in rows if r["quadrant"] == "IMPROVING"]
    improving.sort(key=lambda x: (x["rs_momentum"] or 0), reverse=True)
    leading = [r for r in rows if r["quadrant"] == "LEADING"]
    weakening = [r for r in rows if r["quadrant"] == "WEAKENING"]
    lagging = [r for r in rows if r["quadrant"] == "LAGGING"]
    rotating_in = improving + [r for r in leading if (r["rs_momentum"] or 0) > 0.5]

    # seed tickers of rotating-in/leading themes -> the harvester snapshots these
    picks, seen = [], set()
    for r in rotating_in + leading:
        for s in r["seeds"]:
            if s not in seen:
                seen.add(s)
                picks.append({"symbol": s, "theme": r["theme"], "quadrant": r["quadrant"],
                              "rotation_score": r["rotation_score"]})

    out = {
        "engine": "theme-rotation", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Relative-Rotation-Graph across 40+ themes. IMPROVING = money rotating in (early); "
                  "WEAKENING = rotating out. Seeds of rotating-in themes feed the ledger.",
        "benchmark": BENCH, "n_themes": len(rows), "axis_days": len(dates),
        "summary": {
            "rotating_in": rotating_in[:12],
            "leading": leading[:10],
            "improving": improving[:10],
            "weakening": weakening[:10],
            "lagging": lagging[:8],
            "top_picks": picks[:40],
            "quadrant_counts": {q: sum(1 for r in rows if r["quadrant"] == q)
                                for q in ("LEADING", "IMPROVING", "WEAKENING", "LAGGING")},
        },
        "themes": rows,
        "methodology": {
            "rs_ratio": "theme ~1m return minus SPY ~1m return (relative strength level)",
            "rs_momentum": "this month's relative strength minus last month's (acceleration)",
            "quadrants": "LEADING(+,+) IMPROVING(-,+) WEAKENING(+,-) LAGGING(-,-); IMPROVING is the early rotate-in",
            "score": "rs_ratio + 1.8 x rs_momentum (rewards acceleration over raw level)",
        },
        "caveats": "Short lookback; relative strength is momentum, not value — IMPROVING themes can fail to "
                   "follow through, and a 1-month window is noisy. A rotation map, not a timing guarantee. "
                   "The ledger will grade which quadrant calls actually pay. Research only, not advice.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
    print(f"[theme-rotation] themes={len(rows)} in={len(rotating_in)} leading={len(leading)} "
          f"weakening={len(weakening)} axis={len(dates)}d {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "themes": len(rows),
            "rotating_in": len(rotating_in), "leading": len(leading)})}
