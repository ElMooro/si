"""
justhodl-capital-flow-radar — INSTITUTIONAL CAPITAL FLOW RADAR
==============================================================
Thesis (the operator's): real dollars moving into/out of a sector's ETF complex
lead the sector's stocks. Accelerating inflow = a pump setting up; a sharp
outflow / flow reversal = the party is over. The dollar is the tide underneath
it all — a strengthening USD drains global risk liquidity.

This turns the rich per-ETF ETF-Global flow data (etf-flows/daily.json: daily/5d/
21d $ flow, %-of-AUM, 90d z-score, persistence) into SECTOR-COMPLEX flow regimes:

  For each complex (e.g. Semiconductors = SMH + SOXX + SOXL[3x bull] - SOXS[3x bear]):
    • net $ flow (core + leveraged-bull - leveraged-bear), 5d & 21d
    • velocity   — recent flow pace vs the 21d pace (accelerating?)
    • acceleration (2nd derivative of money)
    • breadth    — how many ETFs in the complex are taking money
    • persistence — consecutive days of one-way flow (sticky vs blip)
    • % of AUM   — size-normalised conviction
    • leveraged positioning — bull-lev vs bear-lev (retail-leverage extreme)
    • price-vs-flow DIVERGENCE — price up while money leaves = distribution/top
    • DOLLAR TIDE overlay (Massive FX synthetic USD)
  -> pump_probability 0-100 and a regime verdict:
     ACCELERATING_INFLOW (PUMP SETUP) · STEADY_INFLOW · DECELERATING (LATE) ·
     FLOW_REVERSAL / DISTRIBUTION (PARTY OVER) · DIVERGENCE (TOP WARNING) · OUTFLOW
  -> constituent cascade: the stocks that ride the sector flow, cross-referenced
     with the Massive options layer (gamma squeeze + unusual call flow) for the
     highest-conviction individual plays.

OUTPUT data/capital-flow-radar.json   SCHEDULE daily 22:30 UTC (after etf-fund-flows + massive-signals).
Real ETF Global + Massive FX data, research only — not advice.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/capital-flow-radar.json"
s3 = boto3.client("s3", region_name="us-east-1")

# Sector / theme COMPLEXES. core = unlevered ETFs; bull/bear = leveraged sentiment.
# `primary` is used for the price-vs-flow divergence read. Members not yet in the
# flow universe are simply skipped (and auto-activate once added). `stocks` ride the flow.
COMPLEXES = {
    # ── Broad-market positioning ──
    "S&P 500 (broad)": {"core": ["SPY", "VOO", "IVV"], "bull": ["SPXL", "UPRO", "SSO"], "bear": ["SPXS", "SPXU", "SDS", "SH"], "primary": "SPY", "stocks": []},
    "Nasdaq 100 (broad)": {"core": ["QQQ"], "bull": ["TQQQ", "QLD"], "bear": ["SQQQ", "QID"], "primary": "QQQ", "stocks": []},
    "Small Caps": {"core": ["IWM"], "bull": ["TNA", "UWM"], "bear": ["TZA", "TWM"], "primary": "IWM", "stocks": []},
    # ── Sectors / themes ──
    "Semiconductors": {"core": ["SMH", "SOXX"], "bull": ["SOXL"], "bear": ["SOXS"], "primary": "SMH",
                       "stocks": ["NVDA", "AMD", "AVGO", "MU", "TSM", "LRCX", "AMAT", "KLAC", "MRVL", "ON", "ARM", "SMCI"]},
    "Technology": {"core": ["XLK", "VGT"], "bull": ["TECL"], "bear": ["TECS"], "primary": "XLK",
                   "stocks": ["AAPL", "MSFT", "NVDA", "AVGO", "ORCL", "CRM", "ADBE", "AMD"]},
    "Software": {"core": ["IGV", "WCLD", "SKYY"], "bull": [], "bear": [], "primary": "IGV",
                 "stocks": ["MSFT", "CRM", "NOW", "ADBE", "SNOW", "PLTR", "DDOG", "NET", "PANW"]},
    "Cybersecurity": {"core": ["HACK", "CIBR"], "bull": [], "bear": [], "primary": "CIBR",
                      "stocks": ["PANW", "CRWD", "ZS", "FTNT", "NET", "S", "OKTA"]},
    "Robotics/AI": {"core": ["BOTZ"], "bull": [], "bear": [], "primary": "BOTZ",
                    "stocks": ["NVDA", "ISRG", "ABB", "ROK"]},
    "Biotech": {"core": ["XBI", "IBB"], "bull": ["LABU"], "bear": ["LABD"], "primary": "XBI",
                "stocks": ["VRTX", "REGN", "GILD", "AMGN", "MRNA", "BIIB", "ALNY"]},
    "Healthcare": {"core": ["XLV"], "bull": ["CURE"], "bear": [], "primary": "XLV",
                   "stocks": ["UNH", "JNJ", "LLY", "ABBV", "MRK", "PFE"]},
    "Energy": {"core": ["XLE", "XOP"], "bull": ["ERX", "GUSH"], "bear": ["ERY", "DRIP"], "primary": "XLE",
               "stocks": ["XOM", "CVX", "COP", "SLB", "EOG", "OXY", "FANG", "PSX"]},
    "Oil Services": {"core": ["OIH"], "bull": [], "bear": [], "primary": "OIH",
                     "stocks": ["SLB", "HAL", "BKR", "NOV"]},
    "Financials": {"core": ["XLF", "KBE"], "bull": ["FAS"], "bear": ["FAZ"], "primary": "XLF",
                   "stocks": ["JPM", "BAC", "WFC", "GS", "MS", "C", "SCHW"]},
    "Regional Banks": {"core": ["KRE"], "bull": ["DPST"], "bear": [], "primary": "KRE",
                       "stocks": ["RF", "KEY", "CFG", "HBAN", "FITB", "ZION"]},
    "Industrials": {"core": ["XLI"], "bull": ["DUSL"], "bear": [], "primary": "XLI",
                    "stocks": ["CAT", "DE", "GE", "HON", "UNP", "BA"]},
    "Aerospace & Defense": {"core": ["ITA"], "bull": ["DFEN"], "bear": [], "primary": "ITA",
                            "stocks": ["LMT", "RTX", "NOC", "GD", "BA", "LHX", "HWM"]},
    "Transports": {"core": ["IYT"], "bull": [], "bear": [], "primary": "IYT",
                   "stocks": ["UPS", "FDX", "UNP", "CSX", "NSC", "ODFL"]},
    "Infrastructure": {"core": ["PAVE"], "bull": [], "bear": [], "primary": "PAVE",
                       "stocks": ["ETN", "PWR", "VMC", "MLM", "URI"]},
    "Materials": {"core": ["XLB"], "bull": [], "bear": [], "primary": "XLB",
                  "stocks": ["LIN", "FCX", "SHW", "APD", "NUE"]},
    "Gold/Silver (metal)": {"core": ["GLD", "IAU", "SLV"], "bull": ["UGL", "AGQ"], "bear": ["GLL", "ZSL"], "primary": "GLD", "stocks": []},
    "Gold Miners": {"core": ["GDX", "GDXJ"], "bull": ["NUGT", "JNUG"], "bear": ["DUST", "JDST"], "primary": "GDX",
                    "stocks": ["NEM", "GOLD", "AEM", "WPM", "FNV", "KGC"]},
    "Copper/Mining": {"core": ["COPX", "XME"], "bull": [], "bear": [], "primary": "COPX",
                      "stocks": ["FCX", "SCCO", "TECK", "VALE"]},
    "Uranium": {"core": ["URA", "URNM"], "bull": [], "bear": [], "primary": "URA",
                "stocks": ["CCJ", "UEC", "DNN", "NXE", "UUUU"]},
    "Clean Energy": {"core": ["ICLN", "TAN"], "bull": [], "bear": [], "primary": "TAN",
                     "stocks": ["FSLR", "ENPH", "SEDG", "RUN", "NEE"]},
    "Lithium/EV Materials": {"core": ["LIT"], "bull": [], "bear": [], "primary": "LIT",
                             "stocks": ["ALB", "SQM", "LAC"]},
    "Electric Vehicles": {"core": ["KARS"], "bull": [], "bear": [], "primary": "KARS",
                          "stocks": ["TSLA", "RIVN", "LCID", "NIO", "LI"]},
    "Homebuilders": {"core": ["ITB", "XHB"], "bull": ["NAIL"], "bear": [], "primary": "ITB",
                     "stocks": ["DHI", "LEN", "PHM", "NVR", "TOL", "KBH"]},
    "Retail": {"core": ["XRT"], "bull": ["RETL"], "bear": [], "primary": "XRT",
               "stocks": ["AMZN", "HD", "LOW", "TJX", "COST", "TGT"]},
    "Consumer Discretionary": {"core": ["XLY"], "bull": ["WANT"], "bear": [], "primary": "XLY",
                               "stocks": ["AMZN", "TSLA", "HD", "MCD", "NKE", "LOW"]},
    "Consumer Staples": {"core": ["XLP"], "bull": [], "bear": [], "primary": "XLP",
                         "stocks": ["PG", "KO", "PEP", "COST", "WMT"]},
    "Utilities": {"core": ["XLU"], "bull": ["UTSL"], "bear": [], "primary": "XLU",
                  "stocks": ["NEE", "DUK", "SO", "D", "AEP"]},
    "Real Estate": {"core": ["XLRE"], "bull": ["DRN"], "bear": ["DRV"], "primary": "XLRE",
                    "stocks": ["PLD", "AMT", "EQIX", "SPG", "O"]},
    "Communications": {"core": ["XLC"], "bull": [], "bear": [], "primary": "XLC",
                       "stocks": ["GOOGL", "META", "NFLX", "DIS", "TMUS"]},
    "China": {"core": ["KWEB", "FXI", "MCHI"], "bull": ["YINN", "CWEB"], "bear": ["YANG"], "primary": "KWEB",
              "stocks": ["BABA", "PDD", "JD", "BIDU", "NIO", "LI", "XPEV"]},
    "Innovation/ARK": {"core": ["ARKK", "ARKW", "ARKG"], "bull": ["WEBL"], "bear": ["WEBS"], "primary": "ARKK",
                       "stocks": ["TSLA", "COIN", "ROKU", "HOOD", "PLTR", "RBLX"]},
    "Crypto": {"core": ["IBIT", "FBTC", "BITO", "ETHA", "ARKB"], "bull": ["BITX", "ETHU"], "bear": ["BITI"], "primary": "IBIT",
               "stocks": ["COIN", "MSTR", "MARA", "RIOT", "CLSK", "HUT"]},
}


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _massive_price_5d(etf, key):
    """5-day % return of an ETF from Massive daily aggregates (for price-vs-flow divergence)."""
    try:
        to = datetime.now(timezone.utc).date()
        frm = to - timedelta(days=12)
        url = ("https://api.massive.com/v2/aggs/ticker/%s/range/1/day/%s/%s"
               "?adjusted=true&sort=desc&limit=8&apiKey=%s" % (etf, frm, to, key))
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl"})
        with urllib.request.urlopen(req, timeout=10) as r:
            res = (json.loads(r.read()) or {}).get("results") or []
        closes = [b.get("c") for b in res if b.get("c")]
        if len(closes) >= 6:
            return round((closes[0] - closes[5]) / closes[5] * 100, 2)
    except Exception:
        pass
    return None


def compute_leveraged_sentiment(fmap):
    """Net leveraged ETF flows per theme = where investors are aggressively long vs short."""
    themes = {}
    agg_bull = agg_bear = 0.0
    for tkr, m in fmap.items():
        if m.get("category") != "leveraged":
            continue
        sub = m.get("subcategory") or ""
        f5 = m.get("flow_5d_usd") or 0
        if "bull" in sub or "_long" in sub or sub.endswith("treasury"):
            direction = "bull"
        elif "bear" in sub or "_short" in sub:
            direction = "bear"
        else:
            continue
        parts = sub.split("_")
        if parts and parts[0][:-1].isdigit() and parts[0].endswith("x"):
            parts = parts[1:]
        if parts and parts[-1] in ("bull", "bear", "long", "short"):
            parts = parts[:-1]
        theme = "_".join(parts) or "other"
        t = themes.setdefault(theme, {"bull": 0.0, "bear": 0.0, "etfs": []})
        if direction == "bull":
            t["bull"] += f5
            agg_bull += max(0.0, f5)
        else:
            t["bear"] += f5
            agg_bear += max(0.0, f5)
        t["etfs"].append(tkr)
    rows = []
    for th, v in themes.items():
        net = v["bull"] - v["bear"]
        rows.append({"theme": th, "bull_lev_flow_5d": round(v["bull"], 0), "bear_lev_flow_5d": round(v["bear"], 0),
                     "net_leveraged_flow_5d": round(net, 0),
                     "positioning": "BULLISH" if net > 0 else "BEARISH" if net < 0 else "NEUTRAL",
                     "etfs": v["etfs"]})
    rows.sort(key=lambda x: -x["net_leveraged_flow_5d"])
    return {
        "most_bullish_positioning": rows[:10],
        "most_bearish_positioning": [r for r in rows[::-1] if r["net_leveraged_flow_5d"] < 0][:10],
        "by_theme": rows,
        "aggregate_bull_lev_inflow_5d": round(agg_bull, 0),
        "aggregate_bear_lev_inflow_5d": round(agg_bear, 0),
        "risk_appetite": ("RISK_ON" if agg_bull > agg_bear * 1.2 else "RISK_OFF" if agg_bear > agg_bull * 1.2 else "BALANCED"),
        "note": "Net leveraged-ETF flow (bull-lev inflow minus bear-lev inflow) per theme — where investors are "
                "aggressively positioned long vs short. Extremes are a contrarian tell.",
    }


def lambda_handler(event, context):
    t0 = time.time()
    flows = _read("etf-flows/daily.json") or {}
    fmap = {m.get("ticker"): m for m in (flows.get("metrics") or []) if m.get("ticker") and not m.get("error")}
    massive = _read("data/massive-signals.json") or {}
    prepump = {x.get("symbol") for x in (massive.get("top_prepump") or []) if x.get("symbol")}

    # ── Dollar tide (Massive FX synthetic USD) ──
    fx = _read("data/polygon-fx-regime.json") or {}
    usd_20d = (fx.get("regime_metrics") or {}).get("usd_synthetic_20d_pct")
    if usd_20d is None:
        usd_regime, usd_mult = "UNKNOWN", 1.0
    elif usd_20d >= 1.5:
        usd_regime, usd_mult = "USD_STRENGTHENING (risk headwind)", 0.92
    elif usd_20d <= -1.5:
        usd_regime, usd_mult = "USD_WEAKENING (risk tailwind)", 1.06
    else:
        usd_regime, usd_mult = "USD_NEUTRAL", 1.0
    dollar_tide = {"usd_synthetic_20d_pct": usd_20d, "regime": usd_regime,
                   "fx_signals": fx.get("regime_signals") or [],
                   "note": "Broad USD 20d momentum (Massive FX). Strong/strengthening USD drains global risk "
                           "liquidity (headwind for inflows); weakening USD is a tailwind."}

    # ── price for divergence (primary ETF of each complex), parallel ──
    key = None
    try:
        from massive import get_massive_key
        key = get_massive_key()
    except Exception:
        pass
    price_5d = {}
    if key:
        primaries = list({c["primary"] for c in COMPLEXES.values() if c["primary"] in fmap})
        with ThreadPoolExecutor(max_workers=10) as ex:
            fut = {ex.submit(_massive_price_5d, p, key): p for p in primaries}
            for f in as_completed(fut):
                price_5d[fut[f]] = f.result()

    def s_flow(etfs, field):
        vals = [fmap[e].get(field) for e in etfs if e in fmap and fmap[e].get(field) is not None]
        return sum(vals) if vals else 0.0

    out_complexes = []
    for name, c in COMPLEXES.items():
        core, bull, bear = c["core"], c.get("bull", []), c.get("bear", [])
        present = [e for e in core + bull + bear if e in fmap]
        core_present = [e for e in core if e in fmap]
        if not core_present:
            continue
        net_5d = s_flow(core, "flow_5d_usd") + s_flow(bull, "flow_5d_usd") - s_flow(bear, "flow_5d_usd")
        net_21d = s_flow(core, "flow_21d_usd") + s_flow(bull, "flow_21d_usd") - s_flow(bear, "flow_21d_usd")
        net_daily = s_flow(core, "daily_flow_usd") + s_flow(bull, "daily_flow_usd") - s_flow(bear, "daily_flow_usd")
        aum = s_flow(core, "aum_usd") + s_flow(bull, "aum_usd") + s_flow(bear, "aum_usd")
        pct_aum_5d = round(net_5d / aum * 100, 3) if aum else None

        pace_5d = net_5d / 5.0
        pace_21d = net_21d / 21.0
        velocity_ratio = round(pace_5d / pace_21d, 2) if abs(pace_21d) > 1 else None
        acceleration = round(pace_5d - pace_21d, 0)               # $/day 2nd derivative
        accelerating = acceleration > 0 and net_5d > 0
        z = [fmap[e].get("flow_zscore_90d") for e in core_present if fmap[e].get("flow_zscore_90d") is not None]
        z_mean = round(sum(z) / len(z), 2) if z else None
        persist = [fmap[e].get("persistence_days") for e in core_present if fmap[e].get("persistence_days") is not None]
        persistence = max(persist) if persist else 0
        # breadth: inflow across core+bull, plus bear OUTflow counts as bullish breadth
        bull_set = [e for e in core + bull if e in fmap]
        n_bull = sum(1 for e in bull_set if (fmap[e].get("flow_5d_usd") or 0) > 0)
        n_bull += sum(1 for e in bear if e in fmap and (fmap[e].get("flow_5d_usd") or 0) < 0)
        breadth = round(n_bull / max(1, len(bull_set) + len([e for e in bear if e in fmap])), 2)
        # leveraged positioning
        bull_flow = s_flow(bull, "flow_5d_usd")
        bear_flow = s_flow(bear, "flow_5d_usd")
        lev_positioning = ("crowded_bull" if bull_flow > 0 and bear_flow < 0
                           else "crowded_bear" if bear_flow > 0 and bull_flow < 0 else "mixed")
        p5 = price_5d.get(c["primary"])
        divergence = bool(p5 is not None and p5 > 3.0 and net_5d < 0)        # price up, money leaving

        # ── pump probability 0-100 ──
        score = 50.0
        if accelerating:
            score += 15
        if z_mean is not None:
            score += 15 if z_mean >= 2 else 10 if z_mean >= 1 else -10 if z_mean <= -1.5 else 0
        if breadth >= 0.6:
            score += 8
        if persistence >= 3:
            score += 7
        if pct_aum_5d is not None and pct_aum_5d >= 1.0:
            score += 8
        if lev_positioning == "crowded_bull" and net_5d > 0:
            score += 4
        if net_5d < 0:
            score -= 20
        if divergence:
            score -= 25
        score *= usd_mult
        score = max(0, min(100, round(score, 1)))

        # ── regime verdict ──
        if divergence:
            regime = "DIVERGENCE — TOP WARNING"
            verdict = "Price is rising while real money leaves the complex — classic distribution into strength; the party is ending."
        elif net_5d < 0 and (z_mean is not None and z_mean <= -1.0):
            regime = "FLOW_REVERSAL — DISTRIBUTION / PARTY OVER"
            verdict = "Sharp, well-below-trend outflow — money is exiting the sector; expect the move to roll over."
        elif net_5d < 0:
            regime = "OUTFLOW"
            verdict = "Net money leaving the complex — headwind for the sector's stocks."
        elif accelerating and breadth >= 0.5 and net_5d > 0:
            regime = "ACCELERATING_INFLOW — PUMP SETUP"
            verdict = "Money is flowing in and ACCELERATING across the complex — the early window before the sector's stocks run."
        elif net_5d > 0 and acceleration <= 0:
            regime = "INFLOW_DECELERATING — LATE"
            verdict = "Still net inflow but the pace is fading — trend intact yet late; tighten stops, don't chase."
        else:
            regime = "STEADY_INFLOW — TREND INTACT"
            verdict = "Steady positive flow — the sector uptrend is funded but not accelerating."

        # ── constituent cascade: stocks that ride the flow, flagged if Massive options agree ──
        top_conviction = [s for s in c.get("stocks", []) if s in prepump]

        out_complexes.append({
            "complex": name, "members_present": present, "primary": c["primary"],
            "net_flow_5d_usd": round(net_5d, 0), "net_flow_21d_usd": round(net_21d, 0),
            "net_flow_daily_usd": round(net_daily, 0), "pct_aum_5d": pct_aum_5d,
            "velocity_ratio": velocity_ratio, "acceleration_usd_per_day": acceleration,
            "accelerating": accelerating, "breadth": breadth, "persistence_days": persistence,
            "flow_zscore_90d": z_mean, "leveraged_positioning": lev_positioning,
            "bull_lev_flow_5d": round(bull_flow, 0), "bear_lev_flow_5d": round(bear_flow, 0),
            "price_5d_pct": p5, "flow_price_divergence": divergence,
            "pump_probability": score, "regime": regime, "verdict": verdict,
            "ref_stocks": c.get("stocks", []),
            "top_conviction_stocks": top_conviction,
        })

    leveraged_sentiment = compute_leveraged_sentiment(fmap)
    out_complexes.sort(key=lambda x: -x["pump_probability"])
    pump_setups = [c for c in out_complexes if "PUMP SETUP" in c["regime"]]
    party_over = [c for c in out_complexes if ("PARTY OVER" in c["regime"] or "TOP WARNING" in c["regime"])]
    # highest-conviction single names: pump-setup sector AND Massive options agree
    cascade = []
    for c in pump_setups:
        for s in c["top_conviction_stocks"]:
            cascade.append({"symbol": s, "complex": c["complex"], "pump_probability": c["pump_probability"],
                            "why": "sector capital inflow accelerating + Massive options flag (gamma/calls)"})

    out = {
        "engine": "capital-flow-radar", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Real dollars into/out of a sector's ETF complex lead the sector's stocks. Accelerating inflow = "
                  "pump setup; flow reversal / outflow = party over. The dollar is the tide underneath.",
        "dollar_tide": dollar_tide,
        "leveraged_sentiment": leveraged_sentiment,
        "n_complexes": len(out_complexes),
        "pump_setups": pump_setups,
        "party_over_alerts": party_over,
        "top_pick_cascade": cascade,
        "complexes": out_complexes,
        "methodology": {
            "net_flow": "core ETF $ flow + leveraged-bull flow - leveraged-bear flow (ETF Global creations/redemptions)",
            "velocity": "5d flow pace vs 21d flow pace (ratio > 1 with positive flow = accelerating inflow)",
            "acceleration": "5d pace minus 21d pace ($/day) — the 2nd derivative of money",
            "breadth": "share of the complex's ETFs taking money (bear-lev outflow counts as bullish)",
            "divergence": "primary ETF price up >3% (5d) while net flow is negative = distribution / top",
            "dollar_tide": "broad USD 20d momentum scales the score (strong USD = liquidity headwind)",
            "pump_probability": "0-100 from velocity + z-score + breadth + persistence + %AUM + leverage, "
                                "penalised for outflow / divergence, scaled by the dollar tide",
        },
        "caveats": "ETF flows are daily (T+1) and are a positioning/conviction signal, not a timing trigger — flow "
                   "can persist past a top and reverse before a bottom. Leveraged-ETF flow is a sentiment proxy, not "
                   "1:1 underlying buying. Real ETF Global + Massive FX data; research only, not advice.",
        "sources": ["etf-flows/daily.json (ETF Global creations/redemptions via Massive)",
                    "polygon-fx-regime (Massive FX / synthetic USD)", "massive-signals (Massive options overlay)",
                    "Massive daily aggregates (ETF price for divergence)"],
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
    print("[capital-flow-radar] complexes=%d pump_setups=%d party_over=%d usd_regime=%s %.1fs"
          % (len(out_complexes), len(pump_setups), len(party_over), usd_regime, out["elapsed_s"]))
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_complexes": len(out_complexes),
            "pump_setups": len(pump_setups), "party_over": len(party_over)})}
