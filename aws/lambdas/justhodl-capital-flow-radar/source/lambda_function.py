"""
justhodl-capital-flow-radar — INSTITUTIONAL CAPITAL FLOW RADAR  (v2)
====================================================================
Thesis: real dollars into/out of a sector's ETF complex lead the sector's stocks.
Accelerating inflow = a pump setting up; a sharp outflow / flow reversal = the
party is over. The dollar is the tide underneath. Leveraged 2x/3x bull-vs-bear
ETF flows reveal what leveraged money (retail + tactical institutional) is
positioned long/short on.

v2 adds, on top of the sector-complex flow regime + pump/dump verdict:
  • full leveraged membership per complex (3x/2x bull AND bear)
  • a LEVERAGED POSITIONING board: net bull-lev minus bear-lev flow per theme AND
    per single-stock mega-cap (NVDA/TSLA/AAPL/META/AMZN/MSFT/AMD/MSTR/COIN/…),
    ranked most-bullish / most-bearish, plus an aggregate RISK_ON / RISK_OFF read.

OUTPUT data/capital-flow-radar.json   SCHEDULE daily 22:30 UTC.
Real ETF Global + Massive FX data, research only — not advice.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "2.1.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/capital-flow-radar.json"
STATE_KEY = "data/capital-flow-radar-state.json"
s3 = boto3.client("s3", region_name="us-east-1")

TELEGRAM_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TELEGRAM_CHAT = "8678089260"


def send_telegram(text):
    try:
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT, "text": text,
            "parse_mode": "HTML", "disable_web_page_preview": "true"}).encode()
        req = urllib.request.Request(
            "https://api.telegram.org/bot%s/sendMessage" % TELEGRAM_TOKEN, data=data)
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception as e:
        print("[telegram] err", str(e)[:120])
        return False

# Sector / theme COMPLEXES. core = unlevered; bull/bear = leveraged sentiment legs.
COMPLEXES = {
    "Semiconductors": {"core": ["SMH", "SOXX"], "bull": ["SOXL", "USD"], "bear": ["SOXS", "SSG"], "primary": "SMH",
                       "stocks": ["NVDA", "AMD", "AVGO", "MU", "TSM", "LRCX", "AMAT", "KLAC", "MRVL", "ON", "ARM", "SMCI"]},
    "Technology": {"core": ["XLK", "QQQ", "VGT"], "bull": ["TQQQ", "TECL", "ROM"], "bear": ["SQQQ", "TECS", "REW"], "primary": "XLK",
                   "stocks": ["AAPL", "MSFT", "NVDA", "AVGO", "ORCL", "CRM", "ADBE", "AMD"]},
    "Software": {"core": ["IGV", "WCLD", "SKYY"], "bull": [], "bear": [], "primary": "IGV",
                 "stocks": ["MSFT", "CRM", "NOW", "ADBE", "SNOW", "PLTR", "DDOG", "NET"]},
    "Biotech": {"core": ["XBI", "IBB"], "bull": ["LABU", "BIB"], "bear": ["LABD", "BIS"], "primary": "XBI",
                "stocks": ["VRTX", "REGN", "GILD", "AMGN", "MRNA", "BIIB", "ALNY"]},
    "Energy": {"core": ["XLE", "XOP", "OIH"], "bull": ["ERX", "GUSH", "DIG"], "bear": ["ERY", "DRIP", "DUG"], "primary": "XLE",
               "stocks": ["XOM", "CVX", "COP", "SLB", "EOG", "OXY", "FANG", "PSX"]},
    "Oil": {"core": ["USO"], "bull": ["UCO"], "bear": ["SCO"], "primary": "USO", "stocks": []},
    "Natural Gas": {"core": ["UNG"], "bull": ["BOIL"], "bear": ["KOLD"], "primary": "UNG", "stocks": []},
    "Financials": {"core": ["XLF", "KRE", "KBE"], "bull": ["FAS", "DPST", "UYG"], "bear": ["FAZ", "SKF"], "primary": "XLF",
                   "stocks": ["JPM", "BAC", "WFC", "GS", "MS", "C", "SCHW"]},
    "Clean Energy": {"core": ["ICLN", "TAN"], "bull": [], "bear": [], "primary": "TAN",
                     "stocks": ["FSLR", "ENPH", "SEDG", "RUN", "NEE", "STEM"]},
    "China": {"core": ["KWEB", "FXI", "MCHI"], "bull": ["YINN", "CWEB"], "bear": ["YANG"], "primary": "KWEB",
              "stocks": ["BABA", "PDD", "JD", "BIDU", "NIO", "LI", "XPEV"]},
    "Innovation/ARK": {"core": ["ARKK", "ARKW", "ARKG"], "bull": [], "bear": [], "primary": "ARKK",
                       "stocks": ["TSLA", "COIN", "ROKU", "HOOD", "PLTR", "RBLX"]},
    "Crypto": {"core": ["IBIT", "FBTC", "BITO", "ETHA", "ARKB"], "bull": ["BITX", "ETHU", "BITU", "ETHT"], "bear": ["BITI", "SBIT"], "primary": "IBIT",
               "stocks": ["COIN", "MSTR", "MARA", "RIOT", "CLSK", "HUT"]},
    "Gold": {"core": ["GLD", "IAU"], "bull": ["UGL"], "bear": ["GLL"], "primary": "GLD",
             "stocks": ["NEM", "GOLD", "AEM", "WPM", "FNV"]},
    "Gold Miners": {"core": ["GDX", "GDXJ"], "bull": ["NUGT", "JNUG"], "bear": ["DUST", "JDST"], "primary": "GDX",
                    "stocks": ["NEM", "GOLD", "AEM", "WPM", "AU"]},
    "Silver": {"core": ["SLV"], "bull": ["AGQ"], "bear": ["ZSL"], "primary": "SLV", "stocks": ["PAAS", "AG", "HL"]},
    "Copper/Mining": {"core": ["COPX", "XME", "CPER"], "bull": [], "bear": [], "primary": "XME",
                      "stocks": ["FCX", "SCCO", "TECK", "VALE", "RIO"]},
    "Uranium": {"core": ["URA", "URNM"], "bull": [], "bear": [], "primary": "URA",
                "stocks": ["CCJ", "UEC", "DNN", "NXE", "UUUU"]},
    "Homebuilders": {"core": ["ITB", "XHB"], "bull": ["NAIL"], "bear": [], "primary": "ITB",
                     "stocks": ["DHI", "LEN", "PHM", "NVR", "TOL", "KBH"]},
    "Retail": {"core": ["XRT"], "bull": ["RETL"], "bear": [], "primary": "XRT",
               "stocks": ["AMZN", "WMT", "COST", "TGT", "HD", "LOW"]},
    "Small Caps": {"core": ["IWM"], "bull": ["TNA", "UWM", "SAA"], "bear": ["TZA", "TWM", "RWM"], "primary": "IWM", "stocks": []},
    "S&P 500 Broad": {"core": ["SPY", "VOO", "IVV"], "bull": ["SPXL", "UPRO", "SSO", "SPUU"], "bear": ["SPXS", "SPXU", "SDS", "SH"],
                      "primary": "SPY", "stocks": []},
    "Nasdaq Broad": {"core": ["QQQ"], "bull": ["TQQQ", "QLD"], "bear": ["SQQQ", "QID", "PSQ"], "primary": "QQQ", "stocks": []},
    "Dow": {"core": ["DIA"], "bull": ["UDOW", "DDM"], "bear": ["SDOW", "DXD", "DOG"], "primary": "DIA", "stocks": []},
    "Industrials": {"core": ["XLI", "PAVE"], "bull": ["DUSL", "UXI"], "bear": [], "primary": "XLI",
                    "stocks": ["CAT", "DE", "GE", "HON", "UNP", "BA"]},
    "Aerospace/Defense": {"core": ["ITA"], "bull": ["DFEN"], "bear": [], "primary": "ITA",
                          "stocks": ["RTX", "LMT", "NOC", "GD", "BA", "LHX"]},
    "Transports": {"core": ["IYT"], "bull": ["TPOR"], "bear": [], "primary": "IYT",
                   "stocks": ["UPS", "FDX", "UNP", "CSX", "NSC", "ODFL"]},
    "Airlines": {"core": ["JETS"], "bull": [], "bear": [], "primary": "JETS",
                 "stocks": ["DAL", "UAL", "AAL", "LUV", "ALK"]},
    "Materials": {"core": ["XLB"], "bull": ["UYM"], "bear": ["SMN"], "primary": "XLB",
                  "stocks": ["LIN", "FCX", "NEM", "SHW", "APD"]},
    "Healthcare": {"core": ["XLV"], "bull": ["CURE", "RXL"], "bear": [], "primary": "XLV",
                   "stocks": ["UNH", "JNJ", "LLY", "ABBV", "MRK", "PFE"]},
    "Consumer Discretionary": {"core": ["XLY"], "bull": ["WANT", "UCC"], "bear": [], "primary": "XLY",
                               "stocks": ["AMZN", "TSLA", "HD", "MCD", "NKE", "LOW"]},
    "Consumer Staples": {"core": ["XLP"], "bull": [], "bear": [], "primary": "XLP",
                         "stocks": ["PG", "KO", "PEP", "COST", "WMT"]},
    "Utilities": {"core": ["XLU"], "bull": ["UTSL", "UPW"], "bear": [], "primary": "XLU",
                  "stocks": ["NEE", "DUK", "SO", "D", "AEP"]},
    "Real Estate": {"core": ["XLRE"], "bull": ["DRN", "URE"], "bear": ["DRV", "SRS"], "primary": "XLRE",
                    "stocks": ["PLD", "AMT", "EQIX", "SPG", "O"]},
    "Communications/Internet": {"core": ["XLC"], "bull": ["WEBL"], "bear": ["WEBS"], "primary": "XLC",
                                "stocks": ["GOOGL", "META", "NFLX", "DIS", "TMUS"]},
    # ── EXPANSION v3 — new complexes (FANG+ mega-cap tech + global regions) ──
    "Mega-Cap Tech (FANG+)": {"core": ["QQQ"], "bull": ["FNGU", "BULZ"], "bear": ["FNGD", "BERZ"], "primary": "QQQ",
                              "stocks": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AVGO", "TSLA"]},
    "Europe": {"core": ["EFA", "VEA", "EWG", "EWU"], "bull": ["EURL"], "bear": [], "primary": "EFA", "stocks": []},
    "Emerging Markets": {"core": ["EEM", "VWO"], "bull": ["EDC"], "bear": ["EDZ"], "primary": "EEM", "stocks": []},
    "India": {"core": ["INDA"], "bull": ["INDL"], "bear": [], "primary": "INDA", "stocks": []},
    "Brazil": {"core": ["EWZ"], "bull": ["BRZU"], "bear": [], "primary": "EWZ", "stocks": ["VALE", "PBR", "ITUB", "NU"]},
}

# Single-stock leveraged — direct leveraged-positioning read per mega-cap (board only).
SINGLE_STOCK_LEV = {
    "NVDA": {"bull": ["NVDL", "NVDX", "NVDU"], "bear": ["NVDS"]},
    "TSLA": {"bull": ["TSLL", "TSLR"], "bear": ["TSLQ", "TSLS"]},
    "AAPL": {"bull": ["AAPU"], "bear": ["AAPD"]},
    "META": {"bull": ["METU"], "bear": ["METD"]},
    "AMZN": {"bull": ["AMZU"], "bear": ["AMZD"]},
    "MSFT": {"bull": ["MSFU"], "bear": ["MSFD"]},
    "AMD": {"bull": ["AMDL"], "bear": ["AMDD"]},
    "GOOGL": {"bull": ["GGLL"], "bear": ["GGLS"]},
    "MSTR": {"bull": ["MSTU", "MSTX"], "bear": ["MSTZ"]},
    "COIN": {"bull": ["CONL"], "bear": ["CONI"]},
    "PLTR": {"bull": ["PLTU"], "bear": []},
    "SMCI": {"bull": ["SMCL"], "bear": []},
    "AVGO": {"bull": ["AVGX"], "bear": []},
}


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _massive_price_5d(etf, key):
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


def lambda_handler(event, context):
    t0 = time.time()
    flows = _read("etf-flows/daily.json") or {}
    fmap = {m.get("ticker"): m for m in (flows.get("metrics") or []) if m.get("ticker") and not m.get("error")}
    massive = _read("data/massive-signals.json") or {}
    prepump = {x.get("symbol") for x in (massive.get("top_prepump") or []) if x.get("symbol")}

    # ── Dollar tide ──
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

    # ── price for divergence (primary ETF of each complex) ──
    key = None
    try:
        from massive import get_massive_key
        key = get_massive_key()
    except Exception:
        pass
    price_5d = {}
    if key:
        primaries = list({c["primary"] for c in COMPLEXES.values() if c["primary"] in fmap})
        with ThreadPoolExecutor(max_workers=12) as ex:
            fut = {ex.submit(_massive_price_5d, p, key): p for p in primaries}
            for f in as_completed(fut):
                price_5d[fut[f]] = f.result()

    def flow5(etf):
        return (fmap.get(etf) or {}).get("flow_5d_usd") or 0.0

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
        pace_5d, pace_21d = net_5d / 5.0, net_21d / 21.0
        velocity_ratio = round(pace_5d / pace_21d, 2) if abs(pace_21d) > 1 else None
        acceleration = round(pace_5d - pace_21d, 0)
        accelerating = acceleration > 0 and net_5d > 0
        z = [fmap[e].get("flow_zscore_90d") for e in core_present if fmap[e].get("flow_zscore_90d") is not None]
        z_mean = round(sum(z) / len(z), 2) if z else None
        persist = [fmap[e].get("persistence_days") for e in core_present if fmap[e].get("persistence_days") is not None]
        persistence = max(persist) if persist else 0
        bull_set = [e for e in core + bull if e in fmap]
        n_bull = sum(1 for e in bull_set if flow5(e) > 0) + sum(1 for e in bear if e in fmap and flow5(e) < 0)
        breadth = round(n_bull / max(1, len(bull_set) + len([e for e in bear if e in fmap])), 2)
        bull_flow, bear_flow = s_flow(bull, "flow_5d_usd"), s_flow(bear, "flow_5d_usd")
        lev_positioning = ("crowded_bull" if bull_flow > 0 and bear_flow < 0
                           else "crowded_bear" if bear_flow > 0 and bull_flow < 0 else "mixed")
        p5 = price_5d.get(c["primary"])
        divergence = bool(p5 is not None and p5 > 3.0 and net_5d < 0)

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
        score = max(0, min(100, round(score * usd_mult, 1)))

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
            "ref_stocks": c.get("stocks", []), "top_conviction_stocks": top_conviction,
        })

    out_complexes.sort(key=lambda x: -x["pump_probability"])
    pump_setups = [c for c in out_complexes if "PUMP SETUP" in c["regime"]]
    party_over = [c for c in out_complexes if ("PARTY OVER" in c["regime"] or "TOP WARNING" in c["regime"])]
    cascade = []
    for c in pump_setups:
        for s in c["top_conviction_stocks"]:
            cascade.append({"symbol": s, "complex": c["complex"], "pump_probability": c["pump_probability"],
                            "why": "sector capital inflow accelerating + Massive options flag (gamma/calls)"})

    # ── LEVERAGED POSITIONING BOARD ──
    def lev_entry(label, kind, bull, bear):
        present_legs = [e for e in bull + bear if e in fmap]
        if not present_legs:
            return None
        bull_in = sum(flow5(e) for e in bull if e in fmap)
        bear_in = sum(flow5(e) for e in bear if e in fmap)
        net = bull_in - bear_in   # bull inflow + bear outflow = bullish; bear inflow = bearish
        stance = ("BULLISH" if net > 5e6 else "BEARISH" if net < -5e6 else "NEUTRAL")
        return {"name": label, "kind": kind, "net_lev_positioning_5d": round(net, 0),
                "bull_lev_flow_5d": round(bull_in, 0), "bear_lev_flow_5d": round(bear_in, 0),
                "stance": stance, "legs": present_legs}

    board = []
    for name, c in COMPLEXES.items():
        e = lev_entry(name, "sector", c.get("bull", []), c.get("bear", []))
        if e:
            board.append(e)
    for sym, c in SINGLE_STOCK_LEV.items():
        e = lev_entry(sym, "single_stock", c.get("bull", []), c.get("bear", []))
        if e:
            board.append(e)
    board.sort(key=lambda x: -x["net_lev_positioning_5d"])
    agg_bull = sum(flow5(e) for c in COMPLEXES.values() for e in c.get("bull", []) if e in fmap)
    agg_bull += sum(flow5(e) for c in SINGLE_STOCK_LEV.values() for e in c.get("bull", []) if e in fmap)
    agg_bear = sum(flow5(e) for c in COMPLEXES.values() for e in c.get("bear", []) if e in fmap)
    agg_bear += sum(flow5(e) for c in SINGLE_STOCK_LEV.values() for e in c.get("bear", []) if e in fmap)
    if agg_bull > agg_bear * 1.3 and agg_bull > 0:
        risk_appetite = "RISK_ON — leveraged money is net positioning long"
    elif agg_bear > agg_bull * 1.3 and agg_bear > 0:
        risk_appetite = "RISK_OFF — leveraged money is hedging / positioning short"
    else:
        risk_appetite = "BALANCED"
    leveraged_board = {
        "risk_appetite": risk_appetite,
        "aggregate_bull_lev_inflow_5d": round(agg_bull, 0),
        "aggregate_bear_lev_inflow_5d": round(agg_bear, 0),
        "most_bullish_positioning": [b for b in board if b["stance"] == "BULLISH"][:8],
        "most_bearish_positioning": [b for b in board[::-1] if b["stance"] == "BEARISH"][:8],
        "all": board,
        "note": "Net = bull-leverage inflow minus bear-leverage inflow (bear-ETF inflow is a bearish vote). "
                "Reads what leveraged money — retail + tactical institutions — is positioned long/short on. "
                "Extreme one-sided positioning is also a contrarian flag.",
    }

    out = {
        "engine": "capital-flow-radar", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Real dollars into/out of a sector's ETF complex lead the sector's stocks. Accelerating inflow = "
                  "pump setup; flow reversal / outflow = party over. Dollar is the tide; leveraged bull-vs-bear "
                  "flows reveal long/short positioning.",
        "dollar_tide": dollar_tide,
        "leveraged_positioning": leveraged_board,
        "n_complexes": len(out_complexes),
        "pump_setups": pump_setups,
        "party_over_alerts": party_over,
        "top_pick_cascade": cascade,
        "complexes": out_complexes,
        "methodology": {
            "net_flow": "core ETF $ flow + leveraged-bull flow - leveraged-bear flow (ETF Global creations/redemptions)",
            "velocity": "5d flow pace vs 21d flow pace (>1 with positive flow = accelerating inflow)",
            "acceleration": "5d pace minus 21d pace ($/day) — the 2nd derivative of money",
            "breadth": "share of the complex's ETFs taking money (bear-lev outflow counts as bullish)",
            "divergence": "primary ETF price up >3% (5d) while net flow is negative = distribution / top",
            "dollar_tide": "broad USD 20d momentum scales the score (strong USD = liquidity headwind)",
            "leveraged_positioning": "bull-lev inflow minus bear-lev inflow per theme & single stock = long/short read",
        },
        "caveats": "ETF flows are daily (T+1) and are a positioning/conviction signal, not a timing trigger — flow can "
                   "persist past a top and reverse before a bottom. Leveraged-ETF flow is a sentiment proxy (with daily "
                   "rebalance decay), not 1:1 underlying buying. Real ETF Global + Massive FX data; research only.",
        "sources": ["etf-flows/daily.json (246 ETFs incl full 2x/3x bull+bear suite + FANG+ 3x, 2x sector, region, FX, single-stock — ETF Global via Massive)",
                    "polygon-fx-regime (Massive FX / synthetic USD)", "massive-signals (Massive options overlay)",
                    "Massive daily aggregates (ETF price for divergence)"],
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")

    # ── TRANSITION ALERTS: fire Telegram only on NEW pump-setups / party-over / risk flip ──
    try:
        prior = _read(STATE_KEY) or {}
        first_run = not prior.get("generated_at")
        prev_pumps = set(prior.get("pump_setups", []))
        prev_party = set(prior.get("party_over", []))
        prev_risk = prior.get("risk_appetite_short")
        cur_pump_map = {c["complex"]: c for c in pump_setups}
        cur_party_map = {c["complex"]: c for c in party_over}
        cur_pumps = set(cur_pump_map)
        cur_party = set(cur_party_map)
        new_pumps = [cur_pump_map[n] for n in (cur_pumps - prev_pumps)]
        new_party = [cur_party_map[n] for n in (cur_party - prev_party)]
        risk_short = risk_appetite.split(" ")[0]
        lines = []
        if new_pumps:
            lines.append("🟢 <b>NEW PUMP SETUPS</b> (accelerating capital inflow)")
            for c in sorted(new_pumps, key=lambda x: -x["pump_probability"]):
                stks = ", ".join((c.get("top_conviction_stocks") or c.get("ref_stocks") or [])[:4])
                lines.append("• <b>%s</b> — pump %s · net5d $%.0fM%s"
                             % (c["complex"], c["pump_probability"], c["net_flow_5d_usd"]/1e6,
                                (" · " + stks) if stks else ""))
        if new_party:
            lines.append("🔴 <b>PARTY OVER / TOP WARNING</b> (capital leaving into strength)")
            for c in sorted(new_party, key=lambda x: x["pump_probability"]):
                lines.append("• <b>%s</b> — net5d $%.0fM · %s"
                             % (c["complex"], c["net_flow_5d_usd"]/1e6,
                                "DIVERGENCE" if c.get("flow_price_divergence") else "outflow"))
        if prev_risk and risk_short != prev_risk:
            lines.append("⚖️ Leveraged risk appetite flipped: <b>%s → %s</b>" % (prev_risk, risk_short))
        if lines and not first_run:
            header = "📡 <b>Capital Flow Radar</b>\n"
            send_telegram(header + "\n".join(lines)
                          + "\n\nhttps://justhodl.ai/capital-flow-radar.html")
        s3.put_object(Bucket=S3_BUCKET, Key=STATE_KEY,
                      Body=json.dumps({
                          "pump_setups": sorted(cur_pumps),
                          "party_over": sorted(cur_party),
                          "risk_appetite_short": risk_short,
                          "generated_at": out["generated_at"],
                      }).encode(), ContentType="application/json")
    except Exception as e:
        print("[capital-flow-radar] alert/state err", str(e)[:160])

    print("[capital-flow-radar v2] complexes=%d pumps=%d party_over=%d risk=%s lev_board=%d %.1fs"
          % (len(out_complexes), len(pump_setups), len(party_over), risk_appetite.split(" ")[0], len(board), out["elapsed_s"]))
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_complexes": len(out_complexes),
            "pump_setups": len(pump_setups), "party_over": len(party_over), "risk_appetite": risk_appetite})}
