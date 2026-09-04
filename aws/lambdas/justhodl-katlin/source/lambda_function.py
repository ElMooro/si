"""justhodl-katlin v1.0.0 -- KATLIN: the buy desk. An institutional war-room engine
that decides (1) whether it is safe to be in risk at all, and (2) which assets --
stocks, ETFs (sector/industry/country/commodity/bond wrappers) and crypto -- sit
at the bottom of a long-term downtrend with the fingerprints of accumulation on
them, i.e. the huge-reward / low-risk asymmetric entries.

Khalid's spec (2026-09-04), verbatim intent:
  * monitor risk through the bond market, macro + where we are in the cycle,
    treasuries, black swan -- and say plainly whether to stay in cash / short
    treasuries;
  * opportunities only on strict measures: BELOW the 200-day MA (preferably below
    the 250-day), huge inflows (ETF/capital), oversold RSI, IN ACCUMULATION
    (declining volume + very tight lower Bollinger band), a catalyst the engine
    can sniff and NAME, momentum about to arrive, deep catalysts the public does
    not see (a metal tied to an industry boom, a port/transport boom in the
    exporter's country, whales / institutions / dark pool / options accumulation);
  * long time frames (3M, 1M, W) decide candidacy; daily and 4h ONLY snipe the
    entry; best risk/reward, nothing that already took off; downtrend broken or
    bottom confirmed (double bottoms) in the long-term frame;
  * check dilution, P/E and the other metrics; plain-English summary for normies
    on every pick; keep itself sharp (daily, self-graded).

Brain doctrine honoured (constitutional): macro gates SIZING before selection
[nmq5x0cp7zp4j] -- the war room caps exposure before any pick is shown; credit
spreads are visible liquidity; the dollar view comes first.

ARCHITECTURE (extend-don't-duplicate: every leg is a fleet feed already probed
by ops 5080 for fortress or read here through the same keys)
  1. WAR ROOM   data/bond-warroom.json, data/auction-desk.json, data/risk-gate.json,
                data/blackswan-watch.json, data/crisis-composite.json, data/tail-risk.json,
                data/regime-composite.json, data/vol-regime.json, data/vix-curve.json,
                data/credit-stress.json, data/global-recession.json,
                data/global-business-cycle.json, data/dollar-radar.json,
                data/global-liquidity.json, data/cross-asset-regime.json,
                data/crypto-cycle-risk.json, data/yield-curve.json
                -> risk thermometer 0-100, hard vetoes, POSTURE + exposure cap.
  2. SIGNALS    house bar warehouse data/warm/polygon-full/grouped/* (every US stock
                and ETF, adjusted OHLCV, ~5y) resampled to W / 1M / 3M; crypto from
                the Katlin crypto lane data/warm/katlin/crypto-bars/{SYM}.json.gz
                (Polygon X:{SYM}USD daily, banked with full history, incremental).
                Location vs SMA/EMA 200/250, multi-frame RSI (+divergence), weekly
                downtrend-line break, weekly/daily double bottoms, higher lows,
                monthly/quarterly trend break, Wyckoff-style accumulation
                (volume dry-up, Bollinger bandwidth percentile, lower-band hug,
                OBV / A/D divergence, up-down volume, absorption on the heaviest
                down days, range contraction), momentum-arrival signals (RS line,
                weekly MACD, ROC cross, squeeze lean), tail-risk stats.
  3. FUSION     finviz-universe, fundamental-census-matrix (dilution, Altman,
                Piotroski, Beneish, leverage, coverage), industry-boom, industry-
                rotation (RRG), etf-flows/daily + etf-true-flows (inflows),
                etf-flows/stock-exposure-lookup (constituent pressure), 13F flows
                by ticker (institutions + whales), dark-pool xray, insider radar +
                clusters, congress trades, options-analytics, catalyst /
                catalyst-calendar / deal-history / backlog / estimate-revisions,
                floor-audit, short-interest, portwatch (ports by country/industry),
                commodity-curves, crypto-etf-flows / exchange-flows / stablecoin /
                coinbase-premium, fedwatch.
  4. GATES      location, oversold, accumulation, inflows, bottom structure,
                catalyst, knife guard, quality -> KATLIN_PRIME > READY > BASING >
                WATCH > SCREENED; composite 0-100, conviction, asymmetry, plan.
  5. SNIPER     the shortlist gets 4-hour bars (Polygon, banked under
                data/warm/katlin/intraday-4h/) -> entry state + trigger levels.
  6. HONESTY    snapshots under data/katlin/history/ are self-graded at 21/63
                sessions; mode "backtest" walks the price gates forward through
                the warehouse (no look-ahead) into data/katlin-backtest.json.

OUTPUT data/katlin.json (+ history). SCHEDULE 04:10 UTC Tue-Sat (after fortress,
after polygon-full lands the prior US session); backtest weekly Sunday.
"""
import array
import bisect
import gzip
import json
import math
import os
import re
import time
import traceback
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.2.0"
ENGINE = "justhodl-katlin"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/katlin.json"
HIST_PREFIX = "data/katlin/history/"
BACKTEST_KEY = "data/katlin-backtest.json"
BARS_ROOT = "data/warm/polygon-full/grouped/"
CRYPTO_ROOT = "data/warm/katlin/crypto-bars/"
INTRADAY_ROOT = "data/warm/katlin/intraday-4h/"
POLY_KEY = os.environ.get("POLYGON_API_KEY") or os.environ.get("POLYGON_KEY") or ""
FMP_KEY = os.environ.get("FMP_KEY") or os.environ.get("FMP_API_KEY") or ""
SHARES_ROOT = "data/warm/katlin/shares/"

P = {
    "sessions": 1260,            # ~5 years of daily bars -> 20 quarterly, 60 monthly, 260 weekly bars
    "min_sessions": 400,
    "min_price": 2.0,
    "min_adv_usd": 1.5e6,
    "min_mcap": 1.5e8,
    "min_etf_aum": 5.0e7,
    "location_max_above_sma200_pct": 0.0,   # gate: close must be BELOW SMA200 (spec)
    "knife_3m_ret_pct": -40.0,
    "knife_dist_sma200_pct": -45.0,
    "rsi_w_oversold": 40.0, "rsi_d_oversold": 35.0, "rsi_m_oversold": 45.0,
    "accum_gate": 55.0, "inflow_gate": 55.0, "structure_gate": 40.0, "catalyst_gate": 35.0,
    "shortlist": 70,             # names that get 4h sniper bars
    "crypto_symbols": ["BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE", "AVAX", "LINK", "DOT", "LTC",
                       "BCH", "UNI", "ATOM", "NEAR", "APT", "ARB", "OP", "SUI", "POL", "MATIC", "TRX",
                       "XLM", "HBAR", "ICP", "FIL", "AAVE", "MKR", "INJ", "TIA", "SEI", "RENDER", "RNDR",
                       "FET", "TAO", "ONDO", "ALGO", "VET", "ETC", "STX", "IMX", "GRT", "LDO", "RUNE",
                       "SAND", "MANA", "AXS", "CRV", "SNX", "COMP", "ENS", "PEPE", "SHIB", "WIF", "BONK",
                       "JUP", "PYTH", "JTO", "TON", "KAS", "XMR", "ZEC", "DASH", "EOS", "XTZ", "FLOW",
                       "MINA", "ROSE", "CFX", "KAVA", "EGLD", "NEO", "IOTA", "QNT", "CHZ", "GALA", "APE",
                       "ENA", "W", "STRK", "ZK", "EIGEN", "HYPE", "PENDLE", "AERO", "VIRTUAL", "AR",
                       "AKT", "THETA", "FLR", "ASTR", "WLD", "ZRO"],
}
WEIGHTS = {"structure": 18, "accumulation": 16, "inflows": 14, "oversold": 12, "location": 12,
           "catalyst": 12, "momentum": 8, "quality": 8}
GATE_PILLARS = ["structure", "accumulation", "inflows", "oversold", "location", "quality"]
TIER_ORDER = ["KATLIN_PRIME", "READY", "BASING", "WATCH", "SCREENED"]
BENCH = ["SPY", "QQQ", "IWM", "TLT", "IEF", "SHY", "BIL", "HYG", "LQD", "UUP", "GLD", "SLV", "CPER",
         "USO", "DBC", "DBB", "BTC"]
SECTOR_ETF = {"Technology": "XLK", "Healthcare": "XLV", "Financial": "XLF", "Consumer Cyclical": "XLY",
              "Consumer Defensive": "XLP", "Industrials": "XLI", "Energy": "XLE", "Utilities": "XLU",
              "Real Estate": "XLRE", "Basic Materials": "XLB", "Communication Services": "XLC"}
IND_ETF = {"Semiconductors": "SMH", "Semiconductor Equipment & Materials": "SMH", "Software - Application": "IGV",
           "Software - Infrastructure": "IGV", "Internet Content & Information": "FDN", "Biotechnology": "XBI",
           "Drug Manufacturers - General": "PPH", "Residential Construction": "ITB", "Building Products & Equipment": "XHB",
           "Banks - Regional": "KRE", "Banks - Diversified": "KBE", "Oil & Gas E&P": "XOP", "Oil & Gas Equipment & Services": "OIH",
           "Oil & Gas Midstream": "AMLP", "Gold": "GDX", "Copper": "COPX", "Steel": "SLX", "Other Industrial Metals & Mining": "XME",
           "Uranium": "URA", "Specialty Retail": "XRT", "Apparel Retail": "XRT", "Railroads": "IYT", "Trucking": "IYT",
           "Integrated Freight & Logistics": "IYT", "Marine Shipping": "BDRY", "Airlines": "JETS", "Aerospace & Defense": "ITA",
           "Solar": "TAN", "Utilities - Renewable": "ICLN", "Engineering & Construction": "PAVE", "Electrical Equipment & Parts": "GRID",
           "Auto Manufacturers": "KARS", "Auto Parts": "KARS", "Medical Devices": "IHI", "Medical Instruments & Supplies": "IHI",
           "Insurance - Property & Casualty": "KIE", "Insurance - Diversified": "KIE", "REIT - Residential": "REZ",
           "REIT - Office": "XLRE", "REIT - Retail": "XLRE", "REIT - Industrial": "XLRE", "REIT - Healthcare Facilities": "XLRE",
           "Agricultural Inputs": "MOO", "Farm Products": "MOO", "Lithium": "LIT", "Silver": "SIL", "Aluminum": "XME",
           "Coal": "XME", "Restaurants": "XLY", "Lodging": "XLY", "Resorts & Casinos": "BJK", "Gambling": "BJK",
           "Telecom Services": "XLC", "Entertainment": "XLC", "Capital Markets": "IAI", "Asset Management": "IAI",
           "Credit Services": "XLF", "Chemicals": "XLB", "Specialty Chemicals": "XLB", "Packaging & Containers": "XLB",
           "Health Information Services": "XLV", "Medical Care Facilities": "XLV", "Diagnostics & Research": "XLV",
           "Communication Equipment": "XLK", "Computer Hardware": "XLK", "Electronic Components": "XLK",
           "Scientific & Technical Instruments": "XLK", "Information Technology Services": "XLK", "Consumer Electronics": "XLK"}
COUNTRY_ETF = {"China": "FXI", "Hong Kong": "FXI", "Japan": "EWJ", "Germany": "EWG", "United Kingdom": "EWU", "France": "EWQ",
               "Canada": "EWC", "Brazil": "EWZ", "Mexico": "EWW", "India": "INDA", "South Korea": "EWY", "Taiwan": "EWT",
               "Australia": "EWA", "Chile": "ECH", "Peru": "EPU", "South Africa": "EZA", "Argentina": "ARGT", "Israel": "EIS",
               "Netherlands": "EWN", "Switzerland": "EWL", "Italy": "EWI", "Spain": "EWP", "Singapore": "EWS", "Indonesia": "EIDO",
               "Vietnam": "VNM", "Turkey": "TUR", "Ireland": "EIRL", "Greece": "GREK", "Colombia": "GXG", "Philippines": "EPHE"}
# which fleet commodity / physical-economy read applies to an industry (the "stuff the public does not see")
INDUSTRY_PHYSICAL = {"Copper": ["copper"], "Steel": ["steel", "iron"], "Gold": ["gold"], "Silver": ["silver"],
                     "Uranium": ["uranium"], "Lithium": ["lithium"], "Aluminum": ["aluminum", "aluminium"],
                     "Coal": ["coal"], "Oil & Gas E&P": ["crude", "wti", "brent", "natural gas"],
                     "Oil & Gas Equipment & Services": ["crude", "wti"], "Oil & Gas Midstream": ["natural gas", "crude"],
                     "Oil & Gas Refining & Marketing": ["crack", "gasoline", "crude"], "Agricultural Inputs": ["corn", "wheat", "soy"],
                     "Farm Products": ["corn", "wheat", "soy", "cattle"], "Marine Shipping": ["freight", "baltic", "shipping"],
                     "Other Industrial Metals & Mining": ["copper", "nickel", "zinc", "aluminum"], "Semiconductors": ["semiconductor"]}
LEV_RX = re.compile(r"\b(2x|3x|-1x|ultra|bull|bear|inverse|short|leveraged|daily .*(2x|3x)|proshares ultra|direxion)\b", re.I)
OVERLAY_RX = re.compile(r"\bvix\b|volatility|market neutral|anti-beta|buffer|defined outcome|covered call|buywrite|buy-write|"
                        r"yieldmax|premium income|target[- ]\d+|select income|hedged equity|managed futures|interval fund|"
                        r"options? income|enhanced (options|income)|option strateg|income advantage|autocallable|0dte|daily target|"
                        r"\bhedged\b|floor etf|income shares|equity premium|call writ|put writ|collar|dividend income (etf|fund)|defiance .*income|"
                        r"kurv|roundhill .*(income|yield)|neos|amplify .*income", re.I)
MONEY_RX = re.compile(r"treasury bill|t-bill|\b0-3 month|\b1-3 month|\b0-1 year|1-12 month|ultra[- ]?short|floating rate|floating-rate|"
                      r"money market|short duration|short-duration|cash reserve|overnight|3-month|6-month|senior loan|bank loan|"
                      r"enhanced short|short maturity|\bshort[- ]term (treasury|bond|government|corporate)", re.I)
BOND_RX = re.compile(r"\b(treasur|bond|credit|muni|municipal|corporate|high yield|aggregate|fixed income|tips|"
                     r"floating rate|bank loan|mortgage|mbs|duration|t-bill|bill|govt|government|yield)\b", re.I)
COMMOD_RX = re.compile(r"\b(gold|silver|platinum|palladium|copper|oil|crude|natural gas|gasoline|uranium|lithium|"
                       r"commodit|agricultur|corn|wheat|soybean|sugar|coffee|cocoa|cattle|metals|mining|carbon)\b", re.I)
CRYPTO_ETF_RX = re.compile(r"\b(bitcoin|ether|ethereum|crypto|solana|blockchain)\b", re.I)
CURRENCY_RX = re.compile(r"\b(currency|dollar index|yen|euro trust|pound|franc|yuan|renminbi|peso|real trust|krona)\b", re.I)
COUNTRY_RX = re.compile(r"\b(msci [a-z ]+|china|japan|india|brazil|mexico|korea|taiwan|germany|united kingdom|canada|"
                        r"australia|emerging markets|em |europe|eafe|latin america|asia|frontier|vietnam|indonesia|"
                        r"chile|peru|argentina|turkey|south africa|israel|switzerland|italy|spain|france|netherlands|"
                        r"singapore|philippines|greece|poland|saudi|uae|qatar|thailand|malaysia|colombia|nordic|pacific)\b", re.I)
REIT_RX = re.compile(r"\b(reit|real estate|property)\b", re.I)
TICKER_OK = re.compile(r"^[A-Z]{1,5}(\.[AB])?$")

s3 = boto3.client("s3", region_name="us-east-1")
LOG = []
DEGRADED = []


def log(msg):
    line = "[katlin] " + str(msg)
    print(line)
    LOG.append(line[:300])



ROW_ERRS = {}


def row_error(kind, sym, e):
    """count distinct row failures; the FIRST occurrence of each message carries its traceback into the log so a
    systematic shape mismatch is diagnosable from one run instead of a 5,000-line 'row error' wall."""
    msg = "%s: %s" % (type(e).__name__, str(e)[:100])
    n = ROW_ERRS.get(msg, 0) + 1
    ROW_ERRS[msg] = n
    if n == 1:
        log("%s row error %s -> %s | %s" % (kind, sym, msg, " | ".join(traceback.format_exc().strip().splitlines()[-4:])))
    elif n in (10, 100, 1000):
        log("%s row error x%d: %s" % (kind, n, msg))


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def s3_json(key, default=None):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body)
        return json.loads(body)
    except Exception as e:
        log("feed miss %s: %s" % (key, str(e)[:80]))
        return default


def s3_put_json(key, obj, gz=False):
    body = json.dumps(obj, separators=(",", ":"), default=str).encode()
    if gz:
        body = gzip.compress(body)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body,
                  ContentType="application/json", **({"ContentEncoding": "gzip"} if gz else {}))
    return len(body)


def list_keys(prefix):
    out = []
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        out.extend(o["Key"] for o in r.get("Contents") or [])
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    return out


def fnum(x):
    if x is None or isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        return None if (isinstance(x, float) and (math.isnan(x) or math.isinf(x))) else float(x)
    try:
        s = str(x).strip().replace(",", "").replace("$", "").replace("%", "")
        if s in ("", "-", "--", "N/A", "None", "nan"):
            return None
        mult = 1.0
        if s[-1] in "KMBT":
            mult = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[s[-1]]
            s = s[:-1]
        return float(s) * mult
    except Exception:
        return None


def clamp(v, lo=0.0, hi=100.0):
    return None if v is None else max(lo, min(hi, v))


def rnd(v, n=2):
    return None if v is None else round(v, n)


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def std(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def lin_map(v, x0, y0, x1, y1):
    if v is None:
        return None
    if v <= min(x0, x1):
        return y0 if x0 <= x1 else y1
    if v >= max(x0, x1):
        return y1 if x0 <= x1 else y0
    return y0 + (y1 - y0) * (v - x0) / (x1 - x0)


def pct_rank(values):
    """percentile rank (0-100) of each value within the list (None-safe)."""
    idx = [i for i, v in enumerate(values) if v is not None]
    out = [None] * len(values)
    if len(idx) < 2:
        return out
    srt = sorted(idx, key=lambda i: values[i])
    n = len(srt)
    for r, i in enumerate(srt):
        out[i] = 100.0 * r / (n - 1)
    return out


def sma(vals, n):
    if len(vals) < n:
        return None
    return sum(vals[-n:]) / n


def sma_at(vals, n, end):
    if end < n:
        return None
    return sum(vals[end - n:end]) / n


def ema_series(vals, n):
    if len(vals) < n:
        return []
    k = 2.0 / (n + 1)
    e = sum(vals[:n]) / n
    out = [None] * (n - 1) + [e]
    for v in vals[n:]:
        e = v + k * (e - v)
        out.append(e)
    return out


def ema_last(vals, n):
    s = ema_series(vals, n)
    return s[-1] if s else None


def rsi_series(c, n=14):
    if len(c) < n + 2:
        return []
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = c[i] - c[i - 1]
        if d > 0:
            gains += d
        else:
            losses -= d
    ag, al = gains / n, losses / n
    out = [None] * n + [100.0 - 100.0 / (1 + ag / al) if al else 100.0]
    for i in range(n + 1, len(c)):
        d = c[i] - c[i - 1]
        ag = (ag * (n - 1) + max(d, 0.0)) / n
        al = (al * (n - 1) + max(-d, 0.0)) / n
        out.append(100.0 - 100.0 / (1 + ag / al) if al else 100.0)
    return out


def bb_series(c, n=20, k=2.0):
    """returns lists (mid, upper, lower, bandwidth_pct, pct_b) aligned to c."""
    m = len(c)
    mid = [None] * m
    up = [None] * m
    lo = [None] * m
    bw = [None] * m
    pb = [None] * m
    if m < n:
        return mid, up, lo, bw, pb
    s = sum(c[:n])
    s2 = sum(x * x for x in c[:n])
    for i in range(n - 1, m):
        if i >= n:
            s += c[i] - c[i - n]
            s2 += c[i] * c[i] - c[i - n] * c[i - n]
        mu = s / n
        var = max(s2 / n - mu * mu, 0.0)
        sd = math.sqrt(var)
        mid[i] = mu
        up[i] = mu + k * sd
        lo[i] = mu - k * sd
        bw[i] = (up[i] - lo[i]) / mu * 100 if mu else None
        pb[i] = (c[i] - lo[i]) / (up[i] - lo[i]) if (up[i] - lo[i]) > 1e-12 else 0.5
    return mid, up, lo, bw, pb


def atr_series(h, lo, c, n=20):
    m = len(c)
    out = [None] * m
    if m < n + 1:
        return out
    trs = [h[0] - lo[0]]
    for i in range(1, m):
        trs.append(max(h[i] - lo[i], abs(h[i] - c[i - 1]), abs(lo[i] - c[i - 1])))
    a = sum(trs[1:n + 1]) / n
    out[n] = a
    for i in range(n + 1, m):
        a = (a * (n - 1) + trs[i]) / n
        out[i] = a
    return out


def macd_hist(c, fast=12, slow=26, sig=9):
    if len(c) < slow + sig + 2:
        return []
    ef = ema_series(c, fast)
    es = ema_series(c, slow)
    line = []
    for a, b in zip(ef, es):
        line.append((a - b) if (a is not None and b is not None) else None)
    vals = [x for x in line if x is not None]
    if len(vals) < sig + 1:
        return []
    sg = ema_series(vals, sig)
    hist = [None] * (len(line) - len(vals)) + [(v - s) if s is not None else None for v, s in zip(vals, sg)]
    return hist


def linreg_slope(vals):
    """OLS slope per bar, normalised by the mean level (fraction per bar)."""
    xs = [v for v in vals if v is not None]
    n = len(xs)
    if n < 3:
        return None
    mx = (n - 1) / 2.0
    my = sum(xs) / n
    sxx = sum((i - mx) ** 2 for i in range(n))
    sxy = sum((i - mx) * (y - my) for i, y in enumerate(xs))
    if sxx == 0 or my == 0:
        return None
    return (sxy / sxx) / abs(my)


def swing_points(vals, w=3):
    """indexes of swing highs and lows (strict local extrema over +-w bars)."""
    highs, lows = [], []
    n = len(vals)
    for i in range(w, n - w):
        v = vals[i]
        if all(v > vals[j] for j in range(i - w, i + w + 1) if j != i):
            highs.append(i)
        if all(v < vals[j] for j in range(i - w, i + w + 1) if j != i):
            lows.append(i)
    return highs, lows


def pctile_of(v, sample):
    xs = [x for x in sample if x is not None]
    if v is None or len(xs) < 20:
        return None
    return 100.0 * sum(1 for x in xs if x <= v) / len(xs)


# ── bars ───────────────────────────────────────────────────────────────────
class Bars:
    __slots__ = ("d", "c", "h", "l", "v", "o")

    def __init__(self):
        self.d = array.array("i")
        self.c = array.array("d")
        self.h = array.array("d")
        self.l = array.array("d")
        self.v = array.array("d")
        self.o = array.array("d")

    def pos_at_or_before(self, idx):
        p = bisect.bisect_right(self.d, idx) - 1
        return p if p >= 0 else None


def session_keys(n):
    now = datetime.now(timezone.utc)
    keys = []
    for yr in range(now.year - 7, now.year + 1):
        keys.extend(k for k in list_keys(BARS_ROOT + "%d/" % yr) if k.endswith(".json.gz"))
    keys.sort()
    return keys[-n:]


def load_session(key, keep):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    j = json.loads(gzip.decompress(body))
    out = []
    for r in j.get("results") or []:
        t = r.get("T")
        c = r.get("c")
        if not t or c is None or c <= 0:
            continue
        if keep is not None:
            if t not in keep:
                continue
        elif not TICKER_OK.match(t):
            continue
        out.append((t, float(c), float(r.get("h") or c), float(r.get("l") or c),
                    float(r.get("v") or 0.0), float(r.get("o") or c)))
    return out


def load_bars(keys, keep, workers=14):
    bars = {}
    dates = [k.rsplit("/", 1)[1][:10] for k in keys]
    chunk = 28
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for start in range(0, len(keys), chunk):
            sub = keys[start:start + chunk]
            for off, rows in enumerate(ex.map(lambda k: load_session(k, keep), sub)):
                idx = start + off
                for t, c, h, lo, v, o in rows:
                    b = bars.get(t)
                    if b is None:
                        b = Bars()
                        bars[t] = b
                    b.d.append(idx)
                    b.c.append(c)
                    b.h.append(h)
                    b.l.append(lo)
                    b.v.append(v)
                    b.o.append(o)
            if (start // chunk) % 10 == 0:
                log("bars %d/%d sessions, %d tickers" % (min(start + chunk, len(keys)), len(keys), len(bars)))
    return dates, bars


# ── crypto lane (Polygon, banked with full history in OUR warehouse) ────────
def poly_get(url, timeout=40):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-katlin/%s" % VERSION})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def poly_aggs(ticker, mult, span, frm, to):
    if not POLY_KEY:
        raise RuntimeError("no POLYGON key in env")
    out = []
    url = ("https://api.polygon.io/v2/aggs/ticker/%s/range/%d/%s/%s/%s?adjusted=true&sort=asc&limit=50000&apiKey=%s"
           % (urllib.parse.quote(ticker), mult, span, frm, to, POLY_KEY))
    for _ in range(6):
        j = poly_get(url)
        out.extend(j.get("results") or [])
        nxt = j.get("next_url")
        if not nxt:
            break
        url = nxt + ("&" if "?" in nxt else "?") + "apiKey=" + POLY_KEY
    return out


def bank_crypto_symbol(sym, today):
    """incremental bank: data/warm/katlin/crypto-bars/{SYM}.json.gz {symbol, source, rows:[[date,o,h,l,c,v]], banked_at}"""
    key = CRYPTO_ROOT + sym + ".json.gz"
    doc = s3_json(key, None) or {"symbol": sym, "source": "polygon X:%sUSD daily" % sym, "rows": []}
    rows = doc.get("rows") or []
    last = rows[-1][0] if rows else None
    frm = (datetime.strptime(last, "%Y-%m-%d") - timedelta(days=3)).strftime("%Y-%m-%d") if last else \
        (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=365 * 6)).strftime("%Y-%m-%d")
    try:
        res = poly_aggs("X:%sUSD" % sym, 1, "day", frm, today)
    except Exception as e:
        return doc, "err:%s" % str(e)[:60]
    have = {r[0]: i for i, r in enumerate(rows)}
    added = 0
    for r in res:
        d = datetime.fromtimestamp(r["t"] / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")
        row = [d, float(r.get("o") or r["c"]), float(r.get("h") or r["c"]), float(r.get("l") or r["c"]),
               float(r["c"]), float(r.get("v") or 0.0)]
        if d in have:
            rows[have[d]] = row
        else:
            rows.append(row)
            have[d] = len(rows) - 1
            added += 1
    rows.sort(key=lambda x: x[0])
    doc["rows"] = rows
    doc["banked_at"] = now_iso()
    doc["n"] = len(rows)
    if added or not last:
        s3_put_json(key, doc, gz=True)
    return doc, "ok:+%d" % added


def load_crypto(today, symbols):
    """returns (dates, bars) on the crypto's own calendar (every day)."""
    docs = {}
    status = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        for sym, (doc, st) in zip(symbols, ex.map(lambda s: bank_crypto_symbol(s, today), symbols)):
            docs[sym] = doc
            status[sym] = st
    # global calendar = union of dates
    alld = set()
    for doc in docs.values():
        for r in doc.get("rows") or []:
            alld.add(r[0])
    dates = sorted(alld)
    idx = {d: i for i, d in enumerate(dates)}
    bars = {}
    for sym, doc in docs.items():
        rows = doc.get("rows") or []
        if len(rows) < 200:
            continue
        b = Bars()
        for d, o, h, l, c, v in rows:
            if c <= 0:
                continue
            b.d.append(idx[d])
            b.o.append(o)
            b.h.append(h)
            b.l.append(l)
            b.c.append(c)
            b.v.append(v * c)   # dollar volume (Polygon crypto v is in base units)
        bars[sym] = b
    nerr = sum(1 for s in status.values() if s.startswith("err"))
    log("crypto lane: %d symbols banked, %d with >=200 days, %d errors (%s)" % (
        len(docs), len(bars), nerr, ", ".join("%s=%s" % (k, v) for k, v in list(status.items())[:5])))
    if nerr and nerr >= len(symbols) // 2:
        DEGRADED.append("crypto lane: %d/%d Polygon errors" % (nerr, len(symbols)))
    return dates, bars


# ── resampling to W / 1M / 3M ───────────────────────────────────────────────

# -- dilution lane (FMP enterprise-values, banked 7 days in OUR warehouse) -------------------------------------------
def s3_json_quiet(key):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body)
        return json.loads(body)
    except Exception:
        return None


def fmp_shares_yoy(sym, today):
    """share count y/y from FMP /stable/enterprise-values (numberOfShares per quarter). Banked at
    data/warm/katlin/shares/{T}.json for 7 days so the lane costs ~0 calls on most runs. Returns (yoy_pct, status)."""
    key = SHARES_ROOT + sym + ".json"
    doc = s3_json_quiet(key)
    if doc and doc.get("banked_at") and (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(doc["banked_at"][:10], "%Y-%m-%d")).days <= 7:
        return doc.get("yoy_pct"), "banked"
    if not FMP_KEY:
        return None, "no FMP key"
    try:
        url = "https://financialmodelingprep.com/stable/enterprise-values?%s" % urllib.parse.urlencode({"symbol": sym.replace(".", "-"), "period": "quarter", "limit": 6, "apikey": FMP_KEY})
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-katlin/%s" % VERSION})
        with urllib.request.urlopen(req, timeout=25) as r:
            rows = json.loads(r.read())
        pts = [(str(x.get("date"))[:10], fnum(x.get("numberOfShares"))) for x in (rows or []) if isinstance(x, dict) and fnum(x.get("numberOfShares"))]
        pts = [x for x in pts if x[1] and x[1] > 0]
        pts.sort(reverse=True)
        yoy = None
        if len(pts) >= 2:
            d0, s0 = pts[0]
            ref = next((x for x in pts if (datetime.strptime(d0, "%Y-%m-%d") - datetime.strptime(x[0], "%Y-%m-%d")).days >= 300), None)
            if ref:
                yoy = (s0 / ref[1] - 1.0) * 100.0
        s3_put_json(key, {"symbol": sym, "source": "fmp enterprise-values quarterly numberOfShares", "banked_at": today, "points": pts, "yoy_pct": rnd(yoy, 2)})
        return yoy, "fmp"
    except Exception as e:
        return None, "err %s" % str(e)[:60]


def dilution_lane(rows, ranks, today):
    """fill share-count y/y for tiered stock rows the census matrix does not cover; heavy dilution knocks the quality gate."""
    todo = [r for r in rows if r["asset_class"] == "stock" and r["tier"] in ("KATLIN_PRIME", "READY", "BASING")
            and (r.get("quality") or {}).get("share_count_yoy_pct") is None][:400]
    if not todo:
        return
    t0 = time.time()
    st = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        for r, (yoy, status) in zip(todo, ex.map(lambda r_: fmp_shares_yoy(r_["ticker"], today), todo)):
            st[status.split(" ")[0]] = st.get(status.split(" ")[0], 0) + 1
            if yoy is None:
                continue
            q = r["quality"]
            q["share_count_yoy_pct"] = rnd(yoy, 1)
            q["shares_source"] = "fmp"
            if yoy >= 15:
                q["red_flags"].append("heavy dilution: share count +%.0f%% y/y (FMP)" % yoy)
                r["gates"]["quality"] = False
            elif yoy >= 6:
                q["notes"].append("share count +%.0f%% y/y (dilution, FMP)" % yoy)
            elif yoy <= -1:
                q["notes"].append("shares shrinking %.0f%% y/y (buybacks, FMP)" % -yoy)
            if r["pillars"].get("quality") is not None:
                r["pillars"]["quality"] = rnd(clamp(r["pillars"]["quality"] + (lin_map(yoy, 12, -15, -4, 10) if yoy > -4 else 6)), 1)
            gates_and_tier(r)
            composite(r, ranks)
    log("dilution lane: %d names, %s, %.0fs" % (len(todo), st, time.time() - t0))


def resample(b, dates, how):
    """OHLCV per period. how in ('W','M','Q'). returns dict of lists o,h,l,c,v and 'end' (last daily pos)."""
    o, h, l, c, v, end = [], [], [], [], [], []
    cur = None
    for p in range(len(b.d)):
        ds = dates[b.d[p]]
        y, m, dd = int(ds[:4]), int(ds[5:7]), int(ds[8:10])
        if how == "W":
            key = datetime(y, m, dd).isocalendar()[:2]
        elif how == "M":
            key = (y, m)
        else:
            key = (y, (m - 1) // 3)
        if key != cur:
            cur = key
            o.append(b.o[p])
            h.append(b.h[p])
            l.append(b.l[p])
            c.append(b.c[p])
            v.append(b.v[p])
            end.append(p)
        else:
            h[-1] = max(h[-1], b.h[p])
            l[-1] = min(l[-1], b.l[p])
            c[-1] = b.c[p]
            v[-1] += b.v[p]
            end[-1] = p
    return {"o": o, "h": h, "l": l, "c": c, "v": v, "end": end}

# ── market context ──────────────────────────────────────────────────────────
def market_context(spy):
    c = list(spy.c)
    out = {"sessions": len(c), "last": c[-1] if c else None}
    if len(c) > 260:
        out["sma200"] = sma(c, 200)
        out["dist_sma200_pct"] = (c[-1] / out["sma200"] - 1) * 100
        out["ret_1m_pct"] = (c[-1] / c[-22] - 1) * 100
        out["ret_3m_pct"] = (c[-1] / c[-64] - 1) * 100
        hi = max(c[-252:])
        out["dd_from_52w_high_pct"] = (c[-1] / hi - 1) * 100
        rs = rsi_series(c)
        out["rsi_d"] = rs[-1] if rs else None
    return out


# ── the signal library (one asset, daily bars + resampled frames) ───────────
def structure_weekly(W):
    """weekly downtrend-line break, double bottom, higher lows, base age -- the long-term bottom evidence."""
    c, h, l = W["c"], W["h"], W["l"]
    n = len(c)
    out = {"lt_downtrend": None, "lt_trend_break": None, "trendline_level": None, "weeks_since_break": None,
           "double_bottom": None, "higher_lows_w": 0, "lower_lows_w": 0, "base_weeks": None, "sma40_falling": None,
           "above_ema10_w": None, "notes": []}
    if n < 40:
        return out
    look = min(n, 104)
    seg_h = h[-look:]
    seg_l = l[-look:]
    seg_c = c[-look:]
    hi_idx, lo_idx = swing_points(seg_h, 3)
    _, lo_idx2 = swing_points(seg_l, 3)
    # weekly 40-SMA (~200d) slope and location
    s40 = sma(c, 40)
    s40p = sma_at(c, 40, n - 10)
    out["sma40_falling"] = (s40 is not None and s40p is not None and s40 < s40p)
    e10 = ema_last(c, 10)
    out["above_ema10_w"] = (e10 is not None and c[-1] > e10)
    # downtrend line: anchored at the highest swing high of the window, then every LOWER swing high extends the chain;
    # the chain ends at the first swing high that overshoots the previous one (= the break). The line is drawn through
    # the last two chain points and projected to today.
    desc = []
    if hi_idx:
        anchor = max(hi_idx, key=lambda i: seg_h[i])
        desc = [anchor]
        for i in hi_idx:
            if i <= anchor:
                continue
            if seg_h[i] < seg_h[desc[-1]]:
                desc.append(i)
            else:
                break
    lower_highs = len(desc) - 1
    out["lower_highs_w"] = lower_highs
    cur = look - 1
    if lower_highs >= 1:
        i1, i2 = desc[-2], desc[-1]
        slope = (seg_h[i2] - seg_h[i1]) / float(i2 - i1)
        level = seg_h[i2] + slope * (cur - i2)
        out["trendline_level"] = level
        out["lt_downtrend"] = True
        above = seg_c[-1] > level
        wk = None
        if above:
            wk = 0
            for k in range(1, cur - i2 + 1):
                lv = seg_h[i2] + slope * (cur - k - i2)
                if seg_c[-1 - k] <= lv:
                    break
                wk = k
        out["lt_trend_break"] = bool(above)
        out["weeks_since_break"] = wk
        if above:
            out["notes"].append("weekly close above the downtrend line drawn through %d lower highs (%.2f)" % (lower_highs, level))
    else:
        out["lt_downtrend"] = bool(out["sma40_falling"] and c[-1] < (s40 or c[-1]))
        out["lt_trend_break"] = False if out["lt_downtrend"] else None
    # higher lows / lower lows at the end (swing lows on weekly LOW)
    lows = [(i, seg_l[i]) for i in lo_idx2]
    hl = ll = 0
    for k in range(len(lows) - 1, 0, -1):
        if lows[k][1] > lows[k - 1][1]:
            if ll:
                break
            hl += 1
        else:
            if hl:
                break
            ll += 1
    out["higher_lows_w"] = hl
    out["lower_lows_w"] = ll
    # double bottom: two swing lows within 6%, >=5 and <=60 weeks apart, intervening peak >= 8% above the lower low
    db = None
    for a in range(len(lows) - 1, 0, -1):
        ia, la = lows[a]
        if look - 1 - ia > 26:
            break
        for bq in range(a - 1, -1, -1):
            ib, lb = lows[bq]
            gap = ia - ib
            if gap < 5:
                continue
            if gap > 60:
                break
            lo_ = min(la, lb)
            if abs(la - lb) / lo_ > 0.06 or la < lb * 0.94:
                continue
            peak = max(seg_h[ib:ia + 1])
            if peak / lo_ - 1 < 0.08:
                continue
            state = "CONFIRMED" if seg_c[-1] > peak else ("FORMING" if seg_c[-1] > la * 1.03 else "UNCONFIRMED")
            db = {"low1": lb, "low2": la, "neckline": peak, "weeks_apart": gap, "state": state,
                  "weeks_since_low2": look - 1 - ia, "room_to_neckline_pct": (peak / seg_c[-1] - 1) * 100}
            break
        if db:
            break
    out["double_bottom"] = db
    if db:
        out["notes"].append("weekly double bottom %s (lows %.2f / %.2f, neckline %.2f)" % (db["state"].lower(), db["low1"], db["low2"], db["neckline"]))
    # base age: weeks since the 52w low, and whether that low still holds
    lo52 = min(l[-52:])
    i52 = len(l) - 1 - list(reversed(l[-52:])).index(lo52)
    out["base_weeks"] = n - 1 - i52
    out["low_52w"] = lo52
    return out


def structure_monthly_quarterly(M, Q):
    out = {"rsi_m": None, "m_close_vs_sma12_pct": None, "m_higher_low": None, "m_lower_lows": 0,
           "q_lower_highs": 0, "q_break": None, "q_ret_pct": None, "m_ret_pct": None}
    c = M["c"]
    if len(c) >= 16:
        r = rsi_series(c)
        out["rsi_m"] = r[-1] if r else None
        s12 = sma(c, 12)
        out["m_close_vs_sma12_pct"] = (c[-1] / s12 - 1) * 100 if s12 else None
        _, lo = swing_points(M["l"], 2)
        if len(lo) >= 2:
            out["m_higher_low"] = M["l"][lo[-1]] > M["l"][lo[-2]]
            ll = 0
            for k in range(len(lo) - 1, 0, -1):
                if M["l"][lo[k]] < M["l"][lo[k - 1]]:
                    ll += 1
                else:
                    break
            out["m_lower_lows"] = ll
        out["m_ret_pct"] = (c[-1] / c[-2] - 1) * 100
    qc, qh = Q["c"], Q["h"]
    if len(qc) >= 6:
        lh = 0
        for k in range(len(qh) - 2, 0, -1):
            if qh[k] < qh[k - 1]:
                lh += 1
            else:
                break
        out["q_lower_highs"] = lh
        out["q_break"] = bool(qc[-1] > qh[-2]) if len(qh) >= 2 else None
        out["q_ret_pct"] = (qc[-1] / qc[-2] - 1) * 100
    return out


def rsi_block(c_d, W):
    out = {}
    rd = rsi_series(c_d)
    out["rsi_d"] = rd[-1] if rd else None
    rw = rsi_series(W["c"])
    out["rsi_w"] = rw[-1] if rw else None
    out["rsi_w_min_12w"] = min(x for x in rw[-12:] if x is not None) if rw and any(x is not None for x in rw[-12:]) else None
    out["rsi_w_turning_up"] = (out["rsi_w"] is not None and out["rsi_w_min_12w"] is not None and out["rsi_w_min_12w"] <= 35 and out["rsi_w"] > out["rsi_w_min_12w"] + 3)
    # bullish divergence: price lower low while RSI higher low (weekly 26w and daily 60d)
    def diverge(cl, rs, look):
        if len(cl) < look + 5 or len(rs) < look + 5:
            return None
        seg = cl[-look:]
        rseg = rs[-look:]
        _, lows = swing_points(seg, 3)
        lows = [i for i in lows if rseg[i] is not None]
        if len(lows) < 2:
            return None
        a, b = lows[-2], lows[-1]
        return bool(seg[b] <= seg[a] * 1.005 and rseg[b] > rseg[a] + 2)
    out["rsi_div_w"] = diverge(W["c"], rw, 26)
    out["rsi_div_d"] = diverge(c_d, rd, 60)
    return out


def accumulation_block(b, mkt_c=None):
    """Wyckoff / Chaikin / O'Neil style volume-structure accumulation read, each leg with evidence."""
    c, h, l, v, o = list(b.c), list(b.h), list(b.l), list(b.v), list(b.o)
    n = len(c)
    out = {"score": None, "legs": {}, "evidence": [], "bbw_pctile": None, "pct_b": None, "squeeze": None,
           "vol_ratio_20_120": None, "obv_slope_40": None, "price_slope_40": None, "ad_slope_40": None,
           "updown_vol_20": None, "absorption_clv": None, "atr_pctile": None, "lower_half_days_20": None}
    if n < 140:
        return out
    legs = {}
    ev = []
    # 1. volume dry-up (supply exhaustion)
    v20 = mean(v[-20:])
    v120 = mean(v[-120:])
    vr = (v20 / v120) if (v20 is not None and v120) else None
    out["vol_ratio_20_120"] = vr
    if vr is not None:
        legs["vol_dryup"] = lin_map(vr, 0.55, 100, 1.25, 0)
        if vr <= 0.8:
            ev.append("volume has dried up: last 20 days run %.0f%% below the 6-month average" % ((1 - vr) * 100))
    # 2. Bollinger bandwidth percentile vs own year + squeeze (TTM)
    mid, up, lo, bw, pb = bb_series(c, 20, 2.0)
    bwn = bw[-1]
    out["bbw_pctile"] = pctile_of(bwn, bw[-252:])
    out["pct_b"] = pb[-1]
    atr = atr_series(h, l, c, 20)
    kc_w = 1.5 * atr[-1] if atr[-1] else None
    sq = bool(kc_w and up[-1] is not None and (up[-1] - lo[-1]) < 2 * kc_w)
    out["squeeze"] = sq
    if out["bbw_pctile"] is not None:
        legs["bb_tight"] = lin_map(out["bbw_pctile"], 5, 100, 60, 0)
        if out["bbw_pctile"] <= 20:
            ev.append("Bollinger bands are in the tightest %.0f%% of the year%s" % (out["bbw_pctile"], " (TTM squeeze on)" if sq else ""))
    # 3. lower-band hug: how many of the last 20 closes sat in the lower half of the band, and current %B
    lh = sum(1 for x in pb[-20:] if x is not None and x < 0.5)
    out["lower_half_days_20"] = lh
    if pb[-1] is not None:
        legs["lower_band"] = 0.5 * lin_map(lh, 8, 0, 18, 100) + 0.5 * lin_map(pb[-1], 0.85, 0, 0.15, 100)
        if lh >= 13 and pb[-1] <= 0.4:
            ev.append("price is hugging the lower Bollinger band (%d of the last 20 closes in the lower half)" % lh)
    # 4. OBV vs price divergence (40 sessions)
    obv = [0.0]
    for i in range(1, n):
        obv.append(obv[-1] + (v[i] if c[i] > c[i - 1] else -v[i] if c[i] < c[i - 1] else 0.0))
    scale = max(mean(v[-40:]) or 1.0, 1.0)
    obv_slope = linreg_slope([x / scale for x in obv[-40:]]) if scale else None
    # OBV normalised by average volume -> slope in "days of volume per day"; price slope in fraction/day
    xs = [x / scale for x in obv[-40:]]
    ob_lr = None
    if len(xs) >= 3:
        mx = (len(xs) - 1) / 2.0
        my = sum(xs) / len(xs)
        sxx = sum((i - mx) ** 2 for i in range(len(xs)))
        ob_lr = sum((i - mx) * (y - my) for i, y in enumerate(xs)) / sxx
    ps = linreg_slope(c[-40:])
    out["obv_slope_40"] = ob_lr
    out["price_slope_40"] = ps
    if ob_lr is not None and ps is not None:
        if ob_lr > 0.02 and ps <= 0.0005:
            legs["obv_div"] = 100.0
            ev.append("on-balance volume is rising while price is flat/down -- shares are being absorbed on the way down")
        elif ob_lr > 0.02:
            legs["obv_div"] = 65.0
        elif ob_lr > -0.02:
            legs["obv_div"] = 40.0
        else:
            legs["obv_div"] = 5.0
    # 5. accumulation/distribution line slope (Chaikin)
    ad = [0.0]
    for i in range(n):
        rng = h[i] - l[i]
        clv = ((c[i] - l[i]) - (h[i] - c[i])) / rng if rng > 1e-12 else 0.0
        ad.append(ad[-1] + clv * v[i])
    xs = [x / scale for x in ad[-40:]]
    ad_lr = None
    if len(xs) >= 3:
        mx = (len(xs) - 1) / 2.0
        my = sum(xs) / len(xs)
        sxx = sum((i - mx) ** 2 for i in range(len(xs)))
        ad_lr = sum((i - mx) * (y - my) for i, y in enumerate(xs)) / sxx
    out["ad_slope_40"] = ad_lr
    if ad_lr is not None:
        legs["ad_line"] = lin_map(ad_lr, -0.04, 0, 0.04, 100)
        if ad_lr > 0.015 and (ps or 0) <= 0.0005:
            ev.append("the accumulation/distribution line is rising against a flat tape (closes keep landing near the highs of the day)")
    # 6. up-volume / down-volume ratio (20 sessions)
    uv = sum(v[i] for i in range(n - 20, n) if c[i] > c[i - 1])
    dv = sum(v[i] for i in range(n - 20, n) if c[i] < c[i - 1])
    udr = (uv / dv) if dv > 0 else (3.0 if uv > 0 else None)
    out["updown_vol_20"] = udr
    if udr is not None:
        legs["updown_vol"] = lin_map(udr, 0.6, 0, 1.6, 100)
        if udr >= 1.3:
            ev.append("up days carry %.1fx the volume of down days over the last month" % udr)
    # 7. absorption: closing location on the five heaviest-volume DOWN days of the last 60 sessions
    downs = [i for i in range(n - 60, n) if c[i] < c[i - 1]]
    downs.sort(key=lambda i: -v[i])
    top = downs[:5]
    if len(top) >= 3:
        clvs = []
        for i in top:
            rng = h[i] - l[i]
            clvs.append(((c[i] - l[i]) - (h[i] - c[i])) / rng if rng > 1e-12 else 0.0)
        acl = mean(clvs)
        out["absorption_clv"] = acl
        legs["absorption"] = lin_map(acl, -0.6, 0, 0.5, 100)
        if acl >= 0.2:
            ev.append("on its heaviest selling days the stock closed in the upper part of the range -- buyers absorbed the supply")
    # 8. range contraction (ATR% percentile vs own year)
    atrp = [(a / cc * 100) if (a and cc) else None for a, cc in zip(atr, c)]
    out["atr_pctile"] = pctile_of(atrp[-1], atrp[-252:])
    if out["atr_pctile"] is not None:
        legs["range_contraction"] = lin_map(out["atr_pctile"], 5, 100, 70, 0)
    # 9. effort vs result: high volume, no new low (last 20 vs prior 40)
    lo20 = min(l[-20:])
    lo60 = min(l[-60:-20])
    if lo20 >= lo60 * 0.99 and vr is not None and vr < 0.95:
        ev.append("the 20-day low is holding above the prior 2-month low on shrinking volume (Wyckoff test of support)")
        legs["support_test"] = 100.0
    elif lo20 >= lo60 * 0.99:
        legs["support_test"] = 60.0
    else:
        legs["support_test"] = 0.0
    W = {"vol_dryup": 18, "bb_tight": 17, "lower_band": 10, "obv_div": 15, "ad_line": 10, "updown_vol": 10,
         "absorption": 10, "range_contraction": 5, "support_test": 5}
    num = den = 0.0
    for k, w in W.items():
        if legs.get(k) is not None:
            num += legs[k] * w
            den += w
    out["score"] = (num / den) if den >= 60 else None
    out["legs"] = {k: rnd(x, 1) for k, x in legs.items()}
    out["evidence"] = ev
    return out


def momentum_block(b, spy_c, dates, W):
    """'momentum about to arrive' -- RS line vs SPY turning first, weekly MACD histogram turning up from below zero,
    daily ROC crossing positive, squeeze lean (Carter), price vs 10-week EMA."""
    c = list(b.c)
    n = len(c)
    out = {"score": None, "legs": {}, "evidence": [], "rs_63_pct": None, "rs_slope_20": None, "macd_w_turn": None,
           "roc20_cross": None, "squeeze_lean": None, "ret_1m_pct": None, "ret_3m_pct": None, "ret_6m_pct": None, "ret_12m_pct": None}
    if n < 140 or len(spy_c) < 140:
        return out
    ev = []
    legs = {}
    # returns
    for k, nn in (("ret_1m_pct", 22), ("ret_3m_pct", 64), ("ret_6m_pct", 127), ("ret_12m_pct", 252)):
        if n > nn:
            out[k] = (c[-1] / c[-1 - nn] - 1) * 100
    # RS line vs SPY on aligned sessions
    rs = []
    for p in range(max(0, n - 130), n):
        sp = b.d[p]
        if sp < len(spy_c) and spy_c[sp]:
            rs.append(c[p] / spy_c[sp])
    if len(rs) >= 70:
        out["rs_63_pct"] = (rs[-1] / rs[-64] - 1) * 100
        out["rs_slope_20"] = linreg_slope(rs[-20:])
        rs_hi_60 = max(rs[-60:])
        rs_lead = rs[-1] >= rs_hi_60 * 0.995 and c[-1] < max(c[-60:]) * 0.97
        legs["rs_turn"] = lin_map(out["rs_slope_20"] or 0.0, -0.003, 0, 0.003, 100)
        if rs_lead:
            legs["rs_turn"] = 100.0
            ev.append("relative strength vs the S&P is at a 3-month high before price is -- money is rotating in ahead of the move")
        elif (out["rs_slope_20"] or 0) > 0.001:
            ev.append("relative strength vs the S&P has turned up over the last month")
    # weekly MACD histogram turning up from below zero
    hist = macd_hist(W["c"])
    hv = [x for x in hist if x is not None]
    if len(hv) >= 4:
        turn = hv[-1] > hv[-2] > hv[-3] and hv[-3] < 0
        out["macd_w_turn"] = bool(turn)
        legs["macd_w"] = 100.0 if turn else (60.0 if (hv[-1] > hv[-2] and hv[-1] < 0) else (30.0 if hv[-1] < 0 else 45.0))
        if turn:
            ev.append("the weekly MACD histogram has been rising for 2+ weeks from below zero -- the classic early-turn footprint")
    # daily 20-day ROC crossing above zero within the last 10 sessions
    roc = [(c[i] / c[i - 20] - 1) * 100 for i in range(n - 30, n)]
    cross = None
    for k in range(len(roc) - 1, 0, -1):
        if roc[k] > 0 and roc[k - 1] <= 0:
            cross = len(roc) - 1 - k
            break
    out["roc20_cross"] = cross
    legs["roc_cross"] = 100.0 if (cross is not None and cross <= 10) else (55.0 if roc[-1] > 0 else 20.0)
    if cross is not None and cross <= 10:
        ev.append("20-day momentum flipped positive %d session(s) ago" % cross)
    # squeeze lean: linear regression of (close - mid of BB/KC) over 20 sessions
    mid, up, lo, bw, pb = bb_series(c, 20, 2.0)
    h, l = list(b.h), list(b.l)
    lean = []
    for i in range(n - 20, n):
        if mid[i] is None:
            continue
        hh = max(h[i - 19:i + 1])
        ll = min(l[i - 19:i + 1])
        lean.append(c[i] - ((hh + ll) / 2 + mid[i]) / 2)
    if len(lean) >= 10:
        sl = linreg_slope([x + abs(min(lean)) + 1e-6 for x in lean]) if lean else None
        lean_up = lean[-1] > lean[-4]
        out["squeeze_lean"] = "up" if lean_up else "down"
        legs["squeeze_lean"] = 80.0 if lean_up else 25.0
    # price vs 10-week EMA
    e10 = ema_last(W["c"], 10)
    if e10:
        legs["ema10_w"] = 80.0 if W["c"][-1] > e10 else 25.0
    WT = {"rs_turn": 30, "macd_w": 25, "roc_cross": 15, "squeeze_lean": 15, "ema10_w": 15}
    num = den = 0.0
    for k, w in WT.items():
        if legs.get(k) is not None:
            num += legs[k] * w
            den += w
    out["score"] = (num / den) if den >= 55 else None
    out["legs"] = {k: rnd(x, 1) for k, x in legs.items()}
    out["evidence"] = ev
    return out


def location_block(b):
    c, l = list(b.c), list(b.l)
    n = len(c)
    out = {"sma200": None, "sma250": None, "ema200": None, "ema250": None, "dist_sma200_pct": None, "dist_sma250_pct": None,
           "dist_ema200_pct": None, "dist_ema250_pct": None, "days_below_sma200": None, "sma200_slope_20_pct": None,
           "below_sma200": None, "below_sma250": None, "pos_52w_pct": None, "dd_52w_pct": None, "max_dd_1y_pct": None,
           "low_52w": None, "high_52w": None, "low_20d": None, "low_60d": None}
    if n < 260:
        return out
    s200 = sma(c, 200)
    s250 = sma(c, 250)
    e200 = ema_last(c, 200)
    e250 = ema_last(c, 250)
    px = c[-1]
    out["sma50"] = sma(c, 50)
    out["sma100"] = sma(c, 100)
    out.update({"sma200": s200, "sma250": s250, "ema200": e200, "ema250": e250,
                "dist_sma200_pct": (px / s200 - 1) * 100, "dist_sma250_pct": (px / s250 - 1) * 100,
                "dist_ema200_pct": (px / e200 - 1) * 100 if e200 else None, "dist_ema250_pct": (px / e250 - 1) * 100 if e250 else None})
    out["below_sma200"] = px < s200
    out["below_sma250"] = px < s250
    s200p = sma_at(c, 200, n - 20)
    out["sma200_slope_20_pct"] = (s200 / s200p - 1) * 100 if s200p else None
    k = 0
    for i in range(n - 1, 199, -1):
        m = sma_at(c, 200, i + 1)
        if c[i] < m:
            k += 1
        else:
            break
        if k >= 400:
            break
    out["days_below_sma200"] = k
    hi = max(c[-252:])
    lo = min(c[-252:])
    out["high_52w"] = hi
    out["low_52w"] = lo
    out["pos_52w_pct"] = (px - lo) / (hi - lo) * 100 if hi > lo else None
    out["dd_52w_pct"] = (px / hi - 1) * 100
    peak = c[-252]
    mdd = 0.0
    for x in c[-252:]:
        peak = max(peak, x)
        mdd = min(mdd, x / peak - 1)
    out["max_dd_1y_pct"] = mdd * 100
    out["low_20d"] = min(l[-20:])
    out["low_60d"] = min(l[-60:])
    return out


def risk_block(b, spy_c):
    c = list(b.c)
    n = len(c)
    out = {"vol_ann_pct": None, "beta_1y": None, "cvar5_pct": None, "adv_usd_20d": None, "worst_day_1y_pct": None, "gap_risk_pct": None}
    if n < 130:
        return out
    rets = [(c[i] / c[i - 1] - 1) for i in range(max(1, n - 252), n)]
    sd = std(rets)
    out["vol_ann_pct"] = sd * math.sqrt(252) * 100 if sd else None
    srt = sorted(rets)
    k = max(1, len(srt) // 20)
    out["cvar5_pct"] = mean(srt[:k]) * 100
    out["worst_day_1y_pct"] = srt[0] * 100
    out["adv_usd_20d"] = mean([b.v[i] * b.c[i] for i in range(n - 20, n)])
    xs, ys = [], []
    for p in range(max(1, n - 252), n):
        s0, s1 = b.d[p - 1], b.d[p]
        if s1 - s0 == 1 and s1 < len(spy_c) and spy_c[s0] and spy_c[s1]:
            xs.append(spy_c[s1] / spy_c[s0] - 1)
            ys.append(c[p] / c[p - 1] - 1)
    if len(xs) >= 120:
        mx, my = mean(xs), mean(ys)
        sxx = sum((x - mx) ** 2 for x in xs)
        out["beta_1y"] = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx if sxx else None
    gaps = [abs(b.o[i] / c[i - 1] - 1) for i in range(max(1, n - 252), n)]
    out["gap_risk_pct"] = max(gaps) * 100 if gaps else None
    return out


def price_signals(b, dates, spy_c):
    """everything the bar warehouse can say about one asset; None where it cannot be computed."""
    W = resample(b, dates, "W")
    M = resample(b, dates, "M")
    Q = resample(b, dates, "Q")
    sig = {"last": b.c[-1], "sessions": len(b.d), "n_weeks": len(W["c"]), "n_months": len(M["c"]), "n_quarters": len(Q["c"])}
    sig["loc"] = location_block(b)
    sig["rsi"] = rsi_block(list(b.c), W)
    sig["struct_w"] = structure_weekly(W)
    sig["struct_mq"] = structure_monthly_quarterly(M, Q)
    sig["accum"] = accumulation_block(b)
    sig["mom"] = momentum_block(b, spy_c, dates, W)
    sig["risk"] = risk_block(b, spy_c)
    # daily double bottom (short-term confirmation of the weekly base)
    c = list(b.c)
    l = list(b.l)
    _, lows = swing_points(l[-160:], 4)
    ddb = None
    if len(lows) >= 2:
        seg_l = l[-160:]
        seg_h = list(b.h)[-160:]
        for a in range(len(lows) - 1, 0, -1):
            ia = lows[a]
            if 159 - ia > 40:
                break
            for q in range(a - 1, -1, -1):
                ib = lows[q]
                gap = ia - ib
                if gap < 10 or gap > 120:
                    continue
                la, lb = seg_l[ia], seg_l[ib]
                lo_ = min(la, lb)
                if abs(la - lb) / lo_ > 0.05 or la < lb * 0.95:
                    continue
                peak = max(seg_h[ib:ia + 1])
                if peak / lo_ - 1 < 0.05:
                    continue
                ddb = {"low1": lb, "low2": la, "neckline": peak, "days_apart": gap,
                       "state": "CONFIRMED" if c[-1] > peak else ("FORMING" if c[-1] > la * 1.02 else "UNCONFIRMED")}
                break
            if ddb:
                break
    sig["double_bottom_d"] = ddb
    return sig


def structure_score(sig):
    """bottom evidence 0-100 with named legs; CONFIRMED / FORMING / NONE."""
    sw, mq, rs, dd = sig["struct_w"], sig["struct_mq"], sig["rsi"], sig.get("double_bottom_d")
    pts = 0.0
    legs = []
    db = sw.get("double_bottom")
    if db:
        if db["state"] == "CONFIRMED":
            pts += 32
            legs.append("weekly double bottom confirmed")
        elif db["state"] == "FORMING":
            pts += 16
            legs.append("weekly double bottom forming")
    if sw.get("lt_trend_break"):
        pts += 26
        legs.append("weekly downtrend line broken")
    hl = sw.get("higher_lows_w") or 0
    if hl >= 2:
        pts += 16
        legs.append("%d higher lows on the weekly chart" % hl)
    elif hl == 1:
        pts += 8
        legs.append("first higher low on the weekly chart")
    if mq.get("m_higher_low"):
        pts += 10
        legs.append("monthly higher low")
    if mq.get("q_break"):
        pts += 8
        legs.append("quarter closed above the prior quarter's high")
    if rs.get("rsi_div_w"):
        pts += 8
        legs.append("weekly RSI bullish divergence")
    elif rs.get("rsi_div_d"):
        pts += 4
        legs.append("daily RSI bullish divergence")
    if dd and dd["state"] == "CONFIRMED":
        pts += 8
        legs.append("daily double bottom confirmed")
    elif dd and dd["state"] == "FORMING":
        pts += 3
    if sw.get("above_ema10_w") and (sw.get("lt_downtrend") or hl):
        pts += 5
        legs.append("back above the 10-week EMA")
    # penalties: still making lower lows on the weekly / monthly
    if (sw.get("lower_lows_w") or 0) >= 2 and not sw.get("lt_trend_break"):
        pts -= 18
        legs.append("still making lower lows on the weekly chart")
    if (mq.get("m_lower_lows") or 0) >= 3 and not mq.get("m_higher_low"):
        pts -= 8
    score = clamp(pts)
    state = "CONFIRMED" if (score >= 50 and ((db and db["state"] == "CONFIRMED") or sw.get("lt_trend_break"))) else \
        ("FORMING" if score >= P["structure_gate"] else "NONE")
    return score, state, legs


def oversold_score(sig):
    rs, mq = sig["rsi"], sig["struct_mq"]
    parts = []
    legs = []
    w = rs.get("rsi_w")
    if w is not None:
        parts.append((lin_map(w, 25, 100, 55, 0), 50))
        if w <= P["rsi_w_oversold"]:
            legs.append("weekly RSI %.0f (oversold)" % w)
    d = rs.get("rsi_d")
    if d is not None:
        parts.append((lin_map(d, 22, 100, 50, 0), 25))
        if d <= P["rsi_d_oversold"]:
            legs.append("daily RSI %.0f" % d)
    m = mq.get("rsi_m")
    if m is not None:
        parts.append((lin_map(m, 30, 100, 60, 0), 25))
        if m <= P["rsi_m_oversold"]:
            legs.append("monthly RSI %.0f" % m)
    if rs.get("rsi_w_turning_up"):
        legs.append("weekly RSI turning up from %.0f" % rs["rsi_w_min_12w"])
    if rs.get("rsi_div_w"):
        legs.append("weekly RSI making a higher low while price made a lower low")
    if not parts:
        return None, False, legs
    score = sum(v * w for v, w in parts) / sum(w for _, w in parts)
    if rs.get("rsi_w_turning_up") or rs.get("rsi_div_w"):
        score = clamp(score + 10)
    gate = bool((w is not None and w <= P["rsi_w_oversold"]) or (d is not None and d <= P["rsi_d_oversold"]) or
                (m is not None and m <= P["rsi_m_oversold"]) or rs.get("rsi_w_turning_up") or rs.get("rsi_div_w"))
    return score, gate, legs


def location_score(sig):
    loc = sig["loc"]
    d200, d250 = loc.get("dist_sma200_pct"), loc.get("dist_sma250_pct")
    if d200 is None:
        return None, False, []
    legs = []
    # spec: below the 200 (required), preferably below the 250; further below = more reward room, until it is a knife
    s = lin_map(d200, 5, 10, -30, 95)
    if d250 is not None and d250 < 0:
        s = clamp(s + 8)
        legs.append("%.0f%% below the 250-day average" % (-d250))
    if d200 < 0:
        legs.insert(0, "%.0f%% below the 200-day average" % (-d200))
    if (loc.get("days_below_sma200") or 0) >= 120:
        legs.append("below the 200-day for %d sessions" % loc["days_below_sma200"])
    gate = bool(d200 <= P["location_max_above_sma200_pct"])
    return clamp(s), gate, legs


def knife_guard(sig):
    loc, sw, mom = sig["loc"], sig["struct_w"], sig["mom"]
    r3 = mom.get("ret_3m_pct")
    d200 = loc.get("dist_sma200_pct")
    new_low = (loc.get("low_52w") is not None and sig["last"] <= loc["low_52w"] * 1.01)
    knife = False
    why = None
    if r3 is not None and r3 <= P["knife_3m_ret_pct"] and not sw.get("lt_trend_break") and (sw.get("higher_lows_w") or 0) == 0:
        knife, why = True, "fell %.0f%% in 3 months with no higher low yet" % (-r3)
    elif d200 is not None and d200 <= P["knife_dist_sma200_pct"] and new_low and not sw.get("lt_trend_break"):
        knife, why = True, "%.0f%% below the 200-day and still printing 52-week lows" % (-d200)
    elif new_low and (sw.get("lower_lows_w") or 0) >= 3 and (sig["rsi"].get("rsi_w") or 50) < 25 and not sig["rsi"].get("rsi_div_w"):
        knife, why = True, "three lower weekly lows into a fresh 52-week low with no divergence"
    return knife, why

# ── feeds (fusion) ──────────────────────────────────────────────────────────
def to_poly(t):
    return str(t or "").upper().replace("-", ".")


def to_fv(t):
    return str(t or "").upper().replace(".", "-")


def load_feeds():
    F = {"asof": {}}
    t0 = time.time()
    fv = s3_json("data/finviz-universe.json", {}) or {}
    F["finviz"] = fv.get("by_ticker") or {}
    F["asof"]["finviz"] = fv.get("generated_at")
    cm = s3_json("data/fundamental-census-matrix.json", {}) or {}
    cols, tks = cm.get("cols") or {}, cm.get("tickers") or []
    want = ["altman_z", "piotroski_f", "beneish_m", "roic_pct", "fcf_yield_pct", "net_buyback_yield_pct", "share_count_yoy_pct",
            "netdebt_to_ebitda_ttm", "interest_coverage_ttm", "revenue_yoy_pct", "eps_yoy_pct", "revenue_cagr_3y_pct",
            "earnings_in_days", "upside_pct", "peg_ttm", "ev_ebitda_ttm", "pe_ttm", "ps_ttm", "fcf_ev_yield_pct", "cash",
            "totalDebt", "netDebt", "mcap", "sloan_accruals_pct", "inst_net_usd_m", "whale_net_usd_m"]
    census = {}
    if isinstance(tks, list) and isinstance(cols, dict):
        for i, tk in enumerate(tks):
            row = {}
            for w in want:
                col = cols.get(w)
                if isinstance(col, list) and i < len(col):
                    row[w] = fnum(col[i])
            census[str(tk).upper()] = row
    F["census"] = census
    F["asof"]["census"] = cm.get("generated_at")
    ib = s3_json("data/industry-boom.json", {}) or {}
    F["boom"] = {str(r.get("industry")): r for r in (ib.get("league") or []) if isinstance(r, dict)}
    F["asof"]["industry_boom"] = ib.get("generated_at")
    ir = s3_json("data/industry-rotation.json", {}) or {}
    F["rotation"] = {r.get("etf"): r for r in (ir.get("ladder") or []) if isinstance(r, dict) and r.get("etf")}
    F["rrg"] = ir.get("rrg") if isinstance(ir.get("rrg"), dict) else {}
    F["asof"]["industry_rotation"] = ir.get("generated_at")
    ef = s3_json("etf-flows/daily.json", {}) or {}
    F["flows_poly"] = {m.get("ticker"): m for m in (ef.get("metrics") or []) if isinstance(m, dict) and m.get("ticker") and not m.get("error")}
    F["asof"]["etf_flows"] = ef.get("generated_at") or ef.get("as_of")
    tf = s3_json("data/etf-true-flows.json", {}) or {}
    F["flows_true"] = tf.get("by_etf") or {}
    F["asof"]["etf_true_flows"] = tf.get("generated_at") or tf.get("as_of")
    se = s3_json("etf-flows/stock-exposure-lookup.json", {}) or {}
    F["stock_exposure"] = se if isinstance(se, dict) else {}
    f13 = s3_json("data/13f-flows-by-ticker.json", {}) or {}
    F["f13"] = f13.get("t") or {}
    F["asof"]["f13"] = f13.get("as_of")
    dp = s3_json("data/dark-pool.json", {}) or {}
    F["dark"] = dp.get("xray_map") if isinstance(dp.get("xray_map"), dict) else {}
    F["asof"]["dark_pool"] = dp.get("generated_at") or dp.get("as_of")
    F["dark_week"] = dp.get("latest_week") or dp.get("week")
    ins = s3_json("data/insider-radar.json", {}) or {}
    ib_ = {}
    for r in ins.get("latest_buys") or []:
        if isinstance(r, dict) and r.get("ticker"):
            e = ib_.setdefault(str(r["ticker"]).upper(), {"n_buys": 0, "usd": 0.0, "last": None, "cluster": False})
            e["n_buys"] += 1
            e["usd"] += fnum(r.get("value")) or 0.0
            if not e["last"] or str(r.get("date")) > e["last"]:
                e["last"] = str(r.get("date"))
    for r in ins.get("clusters") or []:
        if isinstance(r, dict) and r.get("ticker"):
            e = ib_.setdefault(str(r["ticker"]).upper(), {"n_buys": 0, "usd": 0.0, "last": None, "cluster": False})
            e["cluster"] = True
            e["n_insiders"] = r.get("n_insiders")
    F["insider"] = ib_
    F["asof"]["insider"] = ins.get("generated_at")
    ic = s3_json("data/insider-clusters.json", {}) or {}
    F["insider_clusters"] = {str(r.get("ticker")).upper(): r for r in (ic.get("clusters") or []) if isinstance(r, dict) and r.get("ticker")}
    pt = s3_json("data/political-trades.json", {}) or {}
    cong = {}
    cut60 = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%d")
    for t_ in pt.get("trades_recent_50") or []:
        if not isinstance(t_, dict) or not t_.get("ticker") or str(t_.get("transaction_date") or "")[:10] < cut60:
            continue
        e = cong.setdefault(str(t_["ticker"]).upper(), {"buys": 0, "sells": 0, "buy_usd_max": 0.0})
        if str(t_.get("transaction_type") or "").lower() in ("purchase", "buy"):
            e["buys"] += 1
            e["buy_usd_max"] += fnum(t_.get("amount_max_usd")) or 0.0
        else:
            e["sells"] += 1
    for c_ in pt.get("clusters_top_10") or []:
        if isinstance(c_, dict) and c_.get("ticker"):
            cong.setdefault(str(c_["ticker"]).upper(), {"buys": 0, "sells": 0, "buy_usd_max": 0.0})["cluster"] = {"direction": c_.get("direction"), "n_members": c_.get("n_members")}
    F["congress"] = cong
    oa = s3_json("data/options-analytics.json", {}) or {}
    F["options"] = {r.get("ticker"): r for r in (oa.get("board") or []) if isinstance(r, dict) and r.get("ticker")}
    F["asof"]["options"] = oa.get("generated_at")
    pof = s3_json("data/polygon-options-flow.json", {}) or {}
    smb = {}
    for r in (pof.get("smart_money_blocks") or []) + (pof.get("bullish_call_flow") or []) + (pof.get("notable_flow") or []):
        if isinstance(r, dict) and r.get("ticker"):
            e = smb.setdefault(str(r["ticker"]).upper(), {"n": 0, "bull": 0, "premium": 0.0})
            e["n"] += 1
            side = str(r.get("side") or r.get("sentiment") or r.get("type") or "").lower()
            if "call" in side or "bull" in side:
                e["bull"] += 1
            e["premium"] += fnum(r.get("premium") or r.get("premium_usd") or r.get("notional")) or 0.0
    F["opt_blocks"] = smb
    ct = s3_json("data/catalyst.json", {}) or {}
    F["catalyst"] = ct.get("by_ticker") or {}
    F["asof"]["catalyst"] = ct.get("as_of")
    cc = s3_json("data/catalyst-calendar.json", {}) or {}
    cal = {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    horizon = (datetime.now(timezone.utc) + timedelta(days=45)).strftime("%Y-%m-%d")
    for ev in cc.get("events") or []:
        if not isinstance(ev, dict):
            continue
        tk = str(ev.get("ticker") or ev.get("symbol") or "").upper()
        d = str(ev.get("date") or "")[:10]
        if not tk or not d or d < today or d > horizon:
            continue
        cal.setdefault(tk, []).append({"date": d, "type": ev.get("type"), "title": (ev.get("title") or "")[:90], "impact": ev.get("impact")})
    for tk in cal:
        cal[tk].sort(key=lambda x: x["date"])
    F["calendar"] = cal
    F["asof"]["catalyst_calendar"] = cc.get("as_of") or cc.get("generated_at")
    dh = s3_json("data/deal-history.json", {}) or {}
    contracts = {}
    cut90 = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
    for e in (dh.get("entries") or {}).values():
        if not isinstance(e, dict) or str(e.get("event_type") or "") not in ("contract_win", "govt_contract", "licensing_supply"):
            continue
        if (e.get("announce") or "") < cut90:
            continue
        sym = str(e.get("sym") or "").upper()
        val = fnum(e.get("val"))
        if not sym or not val:
            continue
        c = contracts.setdefault(sym, {"n": 0, "usd": 0.0, "vs_mcap_pct": 0.0, "last": None})
        c["n"] += 1
        c["usd"] += val
        c["vs_mcap_pct"] += fnum(e.get("vs_mc")) or 0.0
        if not c["last"] or e["announce"] > c["last"]:
            c["last"] = e["announce"]
    F["contracts"] = contracts
    bk = s3_json("data/backlog.json", {}) or {}
    F["backlog"] = bk.get("by_ticker") or {}
    er = s3_json("data/estimate-revisions.json", {}) or {}
    rev = {}
    for k in ("upward_revisions", "downward_revisions"):
        for r in er.get(k) or []:
            if isinstance(r, dict) and r.get("ticker"):
                rev[str(r["ticker"]).upper()] = {"eps_rev_pct": fnum(r.get("eps_rev_pct")), "direction": r.get("direction")}
    F["revisions"] = rev
    fa = s3_json("data/floor-audit.json", {}) or {}
    F["floor_deep"] = fa.get("tickers") or {}
    F["floor_screen"] = {str(r.get("ticker")).upper(): r for r in (fa.get("screen") or []) if isinstance(r, dict)}
    si = s3_json("data/short-interest.json", {}) or {}
    F["short"] = si.get("by_ticker") or {}
    pw = s3_json("data/portwatch.json", {}) or {}
    # PortWatch shape (ops 3846+): ports[] rows carry z / vs_baseline_pct / yoy_pct and an industry_exposure block whose
    # industries[] rows name the import-canary industry line, the exporter's share and exposure_pct. Aggregate per industry:
    # share-weighted port yoy plus the ports/countries behind it. exporters[] gives the country read (verdict + avg vs baseline).
    pw_ind = {}
    for prt in (pw.get("ports") or []):
        if not isinstance(prt, dict):
            continue
        y = fnum(prt.get("yoy_pct"))
        if y is None:
            y = fnum(prt.get("vs_baseline_pct"))
        ie = prt.get("industry_exposure") if isinstance(prt.get("industry_exposure"), dict) else {}
        for ln in (ie.get("industries") or []):
            if not isinstance(ln, dict) or not ln.get("industry") or y is None:
                continue
            e = pw_ind.setdefault(str(ln["industry"]), {"w": 0.0, "wy": 0.0, "ports": [], "countries": set()})
            w = fnum(ln.get("share_pct")) or 1.0
            e["w"] += w
            e["wy"] += w * y
            if len(e["ports"]) < 4 and prt.get("name") not in e["ports"]:
                e["ports"].append(prt.get("name"))
            if prt.get("country"):
                e["countries"].add(str(prt["country"]))
    for k, e in list(pw_ind.items()):
        pw_ind[k] = {"yoy_pct": (e["wy"] / e["w"]) if e["w"] else None, "ports": e["ports"], "countries": sorted(e["countries"])[:4]}
    F["ports_countries"] = {}
    for ex in (pw.get("exporters") or []):
        if isinstance(ex, dict) and ex.get("country"):
            F["ports_countries"][str(ex["country"])] = {"yoy_pct": fnum(ex.get("avg_vs_baseline_pct")), "verdict": ex.get("verdict"), "n_ports": ex.get("n_ports"), "ports": ex.get("ports")}
    F["ports"] = pw_ind
    F["asof"]["portwatch"] = pw.get("generated_at")
    cmc = s3_json("data/commodity-curves.json", {}) or {}
    F["commod"] = cmc
    F["asof"]["commodity_curves"] = cmc.get("generated_at")
    sy = s3_json("data/sympathetic-momentum.json", {}) or {}
    F["sympathetic"] = sy
    F["fedwatch"] = s3_json("data/fedwatch.json", {}) or {}
    # crypto feeds
    F["crypto_etf"] = s3_json("data/crypto-etf-flows.json", {}) or {}
    F["crypto_exch"] = s3_json("data/crypto-exchange-flows.json", {}) or {}
    F["stablecoin"] = s3_json("data/stablecoin-flow.json", {}) or {}
    F["coinbase"] = s3_json("data/coinbase-premium.json", {}) or {}
    F["crypto_cycle"] = s3_json("data/crypto-cycle-risk.json", {}) or {}
    F["crypto_score"] = s3_json("data/crypto-scorecard.json", {}) or {}
    # war-room feeds
    for name, key in (("bond_warroom", "data/bond-warroom.json"), ("auction", "data/auction-desk.json"), ("risk_gate", "data/risk-gate.json"),
                      ("blackswan", "data/blackswan-watch.json"), ("crisis", "data/crisis-composite.json"), ("tail", "data/tail-risk.json"),
                      ("regime", "data/regime-composite.json"), ("vol", "data/vol-regime.json"), ("vix", "data/vix-curve.json"),
                      ("credit", "data/credit-stress.json"), ("recession", "data/global-recession.json"), ("gbc", "data/global-business-cycle.json"),
                      ("dollar", "data/dollar-radar.json"), ("liquidity", "data/global-liquidity.json"), ("xasset", "data/cross-asset-regime.json"),
                      ("yc", "data/yield-curve.json"), ("fortress", "data/fortress.json"), ("accum_radar", "data/accumulation-radar.json"),
                      ("whales", "data/whales.json"), ("stealth", "data/stealth-accumulation.json"), ("squeeze", "data/volatility-squeeze.json")):
        F[name] = s3_json(key, {}) or {}
    F["backtest"] = s3_json(BACKTEST_KEY, None)
    # secondary accumulation reads from the fleet (radar / whales / stealth / fortress) -> per-ticker booleans.
    # Fleet payloads are shape-polymorphic (accumulation-radar's `bottoms` is {"stocks":[...],"etfs":[...]}, whales may
    # publish a list or a keyed map) -- flatten() takes list / dict-of-lists / dict-of-dicts / dict-keyed-by-ticker.
    def flatten(x, depth=0):
        if isinstance(x, list):
            return [r for r in x if r is not None]
        if isinstance(x, dict) and depth < 2:
            if any(k in x for k in ("ticker", "symbol")):
                return [x]
            out = []
            for k, v in x.items():
                if isinstance(v, (list, dict)):
                    out.extend(flatten(v, depth + 1))
                elif isinstance(k, str) and TICKER_OK.match(k) and isinstance(v, (int, float, str, bool)):
                    out.append({"ticker": k})
            return out
        return []

    def tk_of(r):
        if isinstance(r, dict):
            return r.get("ticker") or r.get("symbol")
        return r if isinstance(r, str) else None

    acc = {}
    for grp in ("accumulating", "bottoms", "confirmed_bottoms"):
        for r in flatten(F["accum_radar"].get(grp)):
            tk = tk_of(r)
            if tk:
                acc.setdefault(str(tk).upper(), set()).add("accumulation-radar:%s" % ((r.get("phase") if isinstance(r, dict) else None) or grp.replace("_", " ")))
    for r in flatten(F["whales"].get("fresh_accumulation") or F["whales"].get("whale_inflow_leaders")):
        tk = tk_of(r)
        if tk:
            acc.setdefault(str(tk).upper(), set()).add("whales:fresh accumulation")
    for k in ("primary", "signals", "picks", "top"):
        for r in flatten(F["stealth"].get(k)):
            tk = tk_of(r)
            if tk:
                acc.setdefault(str(tk).upper(), set()).add("stealth-accumulation")
    for r in flatten(F["fortress"].get("stocks") or F["fortress"].get("board")):
        if isinstance(r, dict) and tk_of(r) and r.get("tier") in ("FORTRESS_COIL", "COILED", "ACCUMULATING"):
            acc.setdefault(str(tk_of(r)).upper(), set()).add("fortress:%s" % r["tier"].lower())
    for grp in ("tier_s", "tier_a"):
        for r in flatten(F["squeeze"].get(grp)):
            tk = tk_of(r)
            if tk:
                acc.setdefault(str(tk).upper(), set()).add("volatility-squeeze")
    F["fleet_accum"] = {k: sorted(v) for k, v in acc.items()}
    log("feeds in %.1fs: finviz=%d census=%d boom=%d rotation=%d flows=%d/%d f13=%d dark=%d insider=%d congress=%d options=%d blocks=%d "
        "catalyst=%d calendar=%d contracts=%d backlog=%d floor=%d ports=%d fleet_accum=%d warroom=%s" % (
            time.time() - t0, len(F["finviz"]), len(census), len(F["boom"]), len(F["rotation"]), len(F["flows_poly"]), len(F["flows_true"]),
            len(F["f13"]), len(F["dark"]), len(ib_), len(cong), len(F["options"]), len(smb), len(F["catalyst"]), len(cal), len(contracts),
            len(F["backlog"]), len(F["floor_deep"]), len(pw_ind), len(acc),
            ",".join(k for k in ("bond_warroom", "auction", "risk_gate", "blackswan", "crisis", "tail", "regime", "vol", "vix", "credit",
                                 "recession", "gbc", "dollar", "liquidity", "xasset") if F[k])))
    return F


# ── WAR ROOM: risk thermometer -> posture ───────────────────────────────────
def _first(d, *paths):
    for p in paths:
        cur = d
        ok = True
        for k in p.split("."):
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok and cur not in (None, "", {}):
            return cur
    return None


def war_room(F):
    """Each leg -> risk 0-100 (100 = get out) with a plain read; hard vetoes; posture + exposure cap.
    Missing feeds are NAMED, never defaulted."""
    legs = []
    vetoes = []
    missing = []

    def add(name, source, risk, read, value=None, weight=1.0, asof=None):
        if risk is None:
            missing.append(name)
            return
        legs.append({"leg": name, "source": source, "risk": rnd(clamp(risk), 0), "read": read, "value": value, "weight": weight,
                     "asof": asof, "flag": "RED" if risk >= 70 else ("AMBER" if risk >= 45 else "GREEN")})

    try:
        bw = F["bond_warroom"]
        hb = bw.get("heartbeat") or {}
        eq = bw.get("equity_risk") or {}
        ed = bw.get("eurodollar_shortage") or {}
        add("Bond heartbeat", "bond-warroom", fnum(hb.get("score")), "%s -- %s" % (hb.get("regime"), (hb.get("headline") or "")[:140]),
            fnum(hb.get("score")), 1.5, bw.get("generated_at"))
        eqs = str(eq.get("state") or "")
        if eqs:
            r = 85 if "DUMP" in eqs.upper() else (25 if "PUMP" in eqs.upper() else (55 if "FLIGHT" in eqs.upper() else 40))
            add("Bond volatility -> stocks", "bond-warroom", r, "%s: %s" % (eqs, (eq.get("text") or "")[:140]), eqs, 1.5, bw.get("generated_at"))
            if "DUMP" in eqs.upper() and str(eq.get("level") or "").upper() == "HIGH":
                vetoes.append("bond desk flags DUMP RISK (high): bonds selling off hard today")
        else:
            missing.append("Bond volatility -> stocks")
        eds = str(ed.get("state") or "")
        if eds:
            add("Eurodollar shortage", "bond-warroom", 85 if "SHORTAGE" in eds.upper() else (50 if "WATCH" in eds.upper() else 20),
                "%s (%s pts)" % (eds, ed.get("points")), ed.get("points"), 1.0, bw.get("generated_at"))
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        au = F["auction"]
        av = _first(au, "verdict", "today.verdict") or {}
        tags = [str(t) for t in (av.get("tags") or [])]
        if av:
            tg = " ".join(tags).upper()
            r = 20 if ("BULLISH" in tg or "LIQUIDITY" in tg) else (75 if ("BEARISH" in tg or "TIGHT" in tg or "FAIL" in tg) else 45)
            add("Treasury auction desk", "auction-desk", r, (av.get("headline") or "")[:150] + (" [" + ", ".join(tags[:3]) + "]" if tags else ""), tags[:3], 0.8, au.get("generated_at"))
        else:
            missing.append("Treasury auction desk")
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        rg = F["risk_gate"]
        post = str(rg.get("posture") or "")
        if post:
            r = {"RISK_ON": 15, "NEUTRAL": 40, "RISK_OFF": 72, "SEVERE": 95}.get(post, 50)
            add("Risk gate (brain)", "risk-gate", r, "%s -- sizing x%s" % (post, rg.get("sizing_multiplier")), post, 2.0, rg.get("generated_at"))
            if post == "SEVERE":
                vetoes.append("risk-gate posture SEVERE (sizing x%s)" % rg.get("sizing_multiplier"))
        else:
            missing.append("Risk gate (brain)")
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        bs = F["blackswan"]
        strip = bs.get("strip") or {}
        baro = bs.get("barometer")
        bval = fnum(baro.get("value") if isinstance(baro, dict) else baro)
        alarm = str(strip.get("alarm") or "")
        if bval is not None or alarm:
            # barometer = systemic stress 0-100; strip.alarm = today's extreme movers (RED when >=2 canaries printed a red move).
            # A RED strip on a low barometer is a tape event, so it lifts the leg to AMBER territory but does not veto by itself.
            r = bval if bval is not None else (60 if alarm.upper() in ("RED", "ALARM", "ACUTE") else 40)
            if alarm.upper() in ("RED", "ALARM", "ACUTE"):
                r = max(r, 55)
            add("Black-swan watch", "blackswan-watch", r, "barometer %s, %s red extremes today%s" % (rnd(bval, 0), strip.get("n_red"), (" -- strip " + alarm) if alarm else ""),
                bval, 1.5, bs.get("as_of"))
            if (bval is not None and bval >= 80) or (alarm.upper() in ("RED", "ALARM", "ACUTE") and bval is not None and bval >= 55):
                vetoes.append("black-swan barometer %s with a %s strip" % (rnd(bval, 0), alarm or "extreme"))
        else:
            missing.append("Black-swan watch")
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        cr = F["crisis"]
        mcs = fnum(cr.get("master_crisis_score"))
        dl = fnum(cr.get("defcon_level"))
        if mcs is not None:
            add("Crisis composite", "crisis-composite", mcs, "%s (DEFCON %s) -- %s" % (cr.get("defcon_name"), dl, ", ".join(str(x) for x in (cr.get("primary_drivers") or [])[:3])),
                mcs, 1.5, cr.get("generated_at"))
            if (dl is not None and dl <= 2) or mcs >= 80:
                vetoes.append("crisis composite %s / DEFCON %s" % (rnd(mcs, 0), dl))
        else:
            missing.append("Crisis composite")
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        tl = F["tail"]
        rows_ = tl.get("indices") if isinstance(tl.get("indices"), list) else []
        spy_row = next((r_ for r_ in rows_ if isinstance(r_, dict) and str(r_.get("ticker")).upper() == "SPY"), (rows_[0] if rows_ and isinstance(rows_[0], dict) else {}))
        p10 = fnum(tl.get("p_drop_10")) if tl.get("p_drop_10") is not None else fnum(spy_row.get("p_drop_10"))
        gauge = tl.get("system_tail_gauge")
        if isinstance(gauge, (int, float)):
            gauge = "system tail gauge %.0f/100 (%s)" % (gauge, tl.get("tail_regime") or "")
        if p10 is not None:
            add("Options tail risk (P[-10%])", "tail-risk", lin_map(p10 * (100 if p10 <= 1 else 1), 4, 20, 25, 90),
                "%.0f%% option-implied chance of a 10%% drop; %s" % (p10 * (100 if p10 <= 1 else 1), (gauge if isinstance(gauge, str) else (gauge or {}).get("label") if isinstance(gauge, dict) else "")), p10, 1.0, tl.get("generated_at"))
        else:
            missing.append("Options tail risk")
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        rc = F["regime"]
        mr = str(rc.get("meta_regime") or "")
        if mr:
            mru = mr.upper()
            r = 80 if ("CRISIS" in mru or "RISK-OFF" in mru or "RISK_OFF" in mru or "BEAR" in mru) else (55 if ("CAUTION" in mru or "TRANSITION" in mru or "LATE" in mru) else 25)
            add("Regime composite", "regime-composite", r, "%s / %s -- %s" % (mr, rc.get("meta_class"), (rc.get("meta_narrative") or "")[:120]), mr, 1.2, rc.get("generated_at"))
        else:
            missing.append("Regime composite")
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        vr = F["vol"]
        vrg = str(vr.get("composite_regime") or "")
        if vrg:
            vu = vrg.upper()
            add("Volatility regime", "vol-regime", 85 if ("CRISIS" in vu or "EXTREME" in vu) else (65 if ("HIGH" in vu or "STRESS" in vu or "ELEVATED" in vu) else 25),
                "%s (score %s)" % (vrg, vr.get("composite_score")), vrg, 1.0, vr.get("as_of"))
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        vx = F["vix"]
        vxr = str(vx.get("composite_regime") or "")
        cur = vx.get("current") or {}
        if vxr:
            vu = vxr.upper()
            add("VIX term structure", "vix-curve", 85 if "BACKWARD" in vu else (55 if ("FLAT" in vu or "STRESS" in vu) else 25),
                "%s -- VIX %s" % (vxr, cur.get("VIX") or cur.get("vix")), vxr, 1.0, vx.get("generated_at"))
            if "BACKWARD" in vu:
                vetoes.append("VIX curve in backwardation")
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        cs = F["credit"]
        csr = str(cs.get("composite_regime") or "")
        hy = fnum(_first(cs, "current_bps.hy_oas", "current_bps.HY_OAS", "current_bps.hy", "current.hy_oas"))
        if csr:
            cu = csr.upper()
            add("Credit spreads (visible liquidity)", "credit-stress", 85 if ("CRISIS" in cu or "SEVERE" in cu) else (60 if ("STRESS" in cu or "WIDEN" in cu or "ELEVATED" in cu) else 25),
                "%s%s" % (csr, (" -- HY OAS %sbp" % rnd(hy, 0)) if hy is not None else ""), csr, 1.5, cs.get("generated_at"))
        else:
            missing.append("Credit spreads")
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        gr = F["recession"]
        gp = fnum(gr.get("global_recession_prob_pct"))
        if gp is not None:
            add("Global recession probability", "global-recession", lin_map(gp, 10, 15, 60, 90), "%.0f%% (%s)" % (gp, gr.get("band")), gp, 1.0, gr.get("generated_at"))
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        gbc = F["gbc"]
        ph = str(_first(gbc, "aggregate.global_phase", "global_phase", "phase") or "")
        dp6 = _first(gbc, "downturn_probability_6m", "composite.downturn_probability_6m")
        if isinstance(dp6, dict):
            dp6 = _first(dp6, "probability_now", "probability", "p_now")
        dp6 = fnum(dp6)
        cli = fnum(_first(gbc, "aggregate.global_avg_cli", "global_avg_cli", "cli_level"))
        if ph or dp6 is not None:
            pu = ph.upper()
            r = dp6 * 100 if (dp6 is not None and dp6 <= 1) else (dp6 if dp6 is not None else (70 if ("CONTRACT" in pu or "DOWNTURN" in pu) else 50 if "SLOW" in pu else 25))
            add("Global business cycle", "global-business-cycle", r, "phase %s%s%s" % (ph or "?", (" -- CLI %.1f" % cli) if cli is not None else "",
                                                                                 (" -- 6m downturn prob %.0f%%" % (dp6 * 100 if dp6 <= 1 else dp6)) if dp6 is not None else ""), ph, 1.0, gbc.get("generated_at"))
        else:
            missing.append("Global business cycle")
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        dr = F["dollar"]
        drg = str(dr.get("regime") or "")
        dpr = dr.get("dollar_pressure")
        if drg:
            du = drg.upper()
            add("Dollar (view first)", "dollar-radar", 70 if ("STRONG" in du or "SQUEEZE" in du or "SHORTAGE" in du or "RISING" in du) else (25 if ("WEAK" in du or "FALLING" in du) else 45),
                "%s -- %s" % (drg, (dr.get("headline") or "")[:120]), drg, 1.2, dr.get("generated_at"))
        else:
            missing.append("Dollar")
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        gl = F["liquidity"]
        imp = fnum(gl.get("impulse_13w") or gl.get("global_impulse_13w_pct"))
        glr = str(gl.get("regime") or "")
        if imp is not None or glr:
            r = lin_map(imp, 3, 15, -3, 75) if imp is not None else (25 if "EXPAND" in glr.upper() else 65 if "CONTRACT" in glr.upper() else 45)
            add("Global liquidity impulse", "global-liquidity", r, "%s -- 13w impulse %s%%" % (glr, rnd(imp, 1)), imp, 1.2, gl.get("generated_at"))
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        xa = F["xasset"]
        xs = fnum(xa.get("risk_score"))
        if xs is not None:
            add("Cross-asset regime", "cross-asset-regime", xs, "%s (%s)" % (xa.get("regime"), xa.get("risk_label")), xs, 1.0, xa.get("generated_at"))
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        cc = F["crypto_cycle"]
        dumps = fnum(cc.get("dump_risk_score"))
        crypto_risk = dumps
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])
    try:
        yc = F["yc"]
        ten = ((yc.get("nominal_yields") or {}).get("10Y") or {})
        y10 = fnum(ten.get("value")) if isinstance(ten, dict) else None
    except Exception as e_:
        missing.append("leg error: %s" % str(e_)[:80])

    # thermometer
    num = sum(l["risk"] * l["weight"] for l in legs)
    den = sum(l["weight"] for l in legs)
    therm = (num / den) if den else None
    nred = sum(1 for l in legs if l["flag"] == "RED")
    if therm is None:
        posture, cap = "UNKNOWN", 25
    elif vetoes or therm >= 72:
        posture, cap = "CASH_OR_TBILLS", 10
    elif therm >= 55 or nred >= 3:
        posture, cap = "DEFENSIVE", 35
    elif therm >= 38:
        posture, cap = "SELECTIVE", 65
    else:
        posture, cap = "FULL_RISK", 100
    try:
        sz = fnum((F.get("risk_gate") or {}).get("sizing_multiplier"))
        if sz is not None and 0 < sz <= 1.5 and posture != "UNKNOWN":
            cap = int(min(cap, round(sz * 100)))
    except Exception:
        pass
    words = {"FULL_RISK": "green light -- deploy into the best asymmetric setups",
             "SELECTIVE": "amber -- only the highest-conviction bottoms, smaller size, keep dry powder",
             "DEFENSIVE": "mostly cash / short treasuries -- nibble only confirmed bottoms with tight stops",
             "CASH_OR_TBILLS": "stand aside in cash / T-bills -- the bond and crisis desks say the floor can drop",
             "UNKNOWN": "war-room feeds unavailable -- treat as SELECTIVE"}
    brief = []
    if therm is not None:
        brief.append("Risk thermometer %.0f/100 across %d fleet legs (%d red, %d amber). Posture %s: %s." % (
            therm, len(legs), nred, sum(1 for l in legs if l["flag"] == "AMBER"), posture.replace("_", " "), words[posture]))
    if vetoes:
        brief.append("Hard vetoes active: " + "; ".join(vetoes) + ".")
    top = sorted(legs, key=lambda l: -l["risk"])[:3]
    if top:
        brief.append("Loudest warnings: " + "; ".join("%s (%s)" % (l["leg"], l["read"][:80]) for l in top) + ".")
    calm = sorted(legs, key=lambda l: l["risk"])[:2]
    if calm:
        brief.append("Supportive: " + "; ".join("%s (%s)" % (l["leg"], l["read"][:70]) for l in calm) + ".")
    if ph or cli is not None:
        brief.append("Cycle: global phase %s%s." % (ph or "unknown", (", CLI %.1f" % cli) if cli is not None else ""))
    if crypto_risk is not None:
        brief.append("Crypto dump-risk %.0f/100 from the crypto cycle engine." % crypto_risk)
    return {"posture": posture, "exposure_cap_pct": cap, "thermometer": rnd(therm, 1), "n_red": nred, "vetoes": vetoes,
            "legs": legs, "missing": missing, "brief": " ".join(brief), "crypto_dump_risk": crypto_risk, "y10": y10,
            "cycle": {"phase": ph or None, "cli": cli, "downturn_prob_6m": dp6, "recession_prob_pct": gp},
            "words": words[posture]}


# ── inflows / catalyst / quality legs ───────────────────────────────────────
def flow_legs(etf, F):
    legs = {}
    m = F["flows_poly"].get(etf)
    if m:
        legs["z90"] = fnum(m.get("flow_zscore_90d"))
        legs["pct_aum_21d"] = fnum(m.get("pct_aum_21d"))
        legs["flow_21d_usd"] = fnum(m.get("flow_21d_usd"))
        legs["quadrant"] = m.get("quadrant")
    t = F["flows_true"].get(etf)
    if t:
        nf20 = fnum(t.get("net_flow_20d_usd"))
        aum = fnum(t.get("aum_est_b"))
        legs["true_flow_20d_usd"] = nf20
        if nf20 is not None and aum:
            legs["true_pct_aum_20d"] = nf20 / (aum * 1e9) * 100
    fv = F["finviz"].get(to_fv(etf))
    if fv:
        legs["fv_flows_1m_pct"] = fnum(fv.get("flows_1m_pct"))
        f1, a = fnum(fv.get("flows_1m")), fnum(fv.get("aum"))
        if legs["fv_flows_1m_pct"] is None and f1 is not None and a:
            legs["fv_flows_1m_pct"] = f1 / a * 100
        legs["fv_flows_1m_usd"] = f1
        legs["fv_flows_3m_usd"] = fnum(fv.get("flows_3m"))
    parts = []
    if legs.get("z90") is not None:
        parts.append(clamp(50 + 25 * legs["z90"]))
    if legs.get("pct_aum_21d") is not None:
        parts.append(clamp(50 + 12.5 * legs["pct_aum_21d"]))
    if legs.get("true_pct_aum_20d") is not None:
        parts.append(clamp(50 + 12.5 * legs["true_pct_aum_20d"]))
    if legs.get("fv_flows_1m_pct") is not None:
        parts.append(clamp(50 + 12.5 * legs["fv_flows_1m_pct"]))
    legs["score"] = mean(parts)
    legs["n_sources"] = len(parts)
    legs["major"] = bool((legs.get("z90") or 0) >= 1.0 or (legs.get("pct_aum_21d") or 0) >= 2.0 or (legs.get("true_pct_aum_20d") or 0) >= 2.0 or (legs.get("fv_flows_1m_pct") or 0) >= 2.0)
    return legs


def inflow_block(sym, fv, cs, F, mcap, adv, asset_class, industry_etf):
    """capital flowing INTO the asset or its wrapper: ETF flows, constituent pressure, 13F/whales, dark pool, insiders, congress, options."""
    legs = {}
    ev = []
    parts = []
    if asset_class in ("stock",):
        fl = flow_legs(industry_etf, F) if industry_etf else {}
        legs["industry_etf"] = industry_etf
        legs["industry_flow"] = {k: rnd(v, 2) if isinstance(v, float) else v for k, v in fl.items()}
        if fl.get("score") is not None:
            parts.append((fl["score"], 30))
            if fl.get("major"):
                ev.append("its industry ETF %s is seeing major inflows (%s%% of AUM over 21d, z %s)" % (industry_etf, rnd(fl.get("pct_aum_21d") or fl.get("fv_flows_1m_pct"), 1), rnd(fl.get("z90"), 1)))
        se = F["stock_exposure"].get(sym) or F["stock_exposure"].get(to_fv(sym)) or {}
        p21 = fnum(se.get("total_aggregate_flow_21d_usd"))
        if p21 is not None and mcap:
            pm = p21 / mcap * 100
            legs["etf_pressure_pct_mcap"] = pm
            legs["etf_pressure_days_adv"] = (p21 / adv) if adv else None
            parts.append((lin_map(pm, -0.5, 20, 1.0, 100), 15))
            if pm >= 0.3:
                ev.append("ETFs holding it were forced to buy $%.0fM of stock over 21 days (%.2f%% of its market cap)" % (p21 / 1e6, pm))
        f13 = F["f13"].get(sym) or {}
        inst = fnum(f13.get("n"))
        whale = fnum(f13.get("wn"))
        legs["inst_net_usd"] = inst
        legs["whale_net_usd"] = whale
        legs["funds_holding"] = f13.get("nf")
        if inst is not None and mcap:
            pm = inst / mcap * 100
            parts.append((lin_map(pm, -1.5, 15, 1.5, 100), 20))
            if pm >= 0.5:
                ev.append("13F filers added a net $%.0fM last quarter (%.1f%% of market cap)%s" % (inst / 1e6, pm, (", whales +$%.0fM" % (whale / 1e6)) if (whale or 0) > 0 else ""))
        dk = F["dark"].get(sym) or {}
        dpp, acc, st = fnum(dk.get("dp")), fnum(dk.get("acc")), str(dk.get("st") or "")
        legs["dark_pool_pct"] = dpp
        legs["dark_pool_accel"] = acc
        legs["dark_pool_state"] = st or None
        if dpp is not None:
            s = lin_map(dpp, 30, 35, 55, 85)
            if "ACCUM" in st.upper():
                s = clamp(s + 15)
            if acc is not None and acc > 0:
                s = clamp(s + 5)
            parts.append((s, 15))
            if dpp >= 45 or "ACCUM" in st.upper():
                ev.append("dark-pool share %.0f%% of volume%s -- institutions are working orders off-exchange" % (dpp, (" (" + st.lower() + ")") if st else ""))
        ins = F["insider"].get(sym)
        icl = F["insider_clusters"].get(sym)
        legs["insider_buys"] = ins["n_buys"] if ins else 0
        legs["insider_usd"] = ins["usd"] if ins else None
        legs["insider_cluster"] = bool((ins and ins.get("cluster")) or icl)
        if ins or icl:
            parts.append((100 if legs["insider_cluster"] else (70 if (ins and ins["n_buys"] >= 2) else 55), 10))
            if legs["insider_cluster"]:
                ev.append("insider buying cluster (%s insiders%s)" % ((icl or {}).get("n_insiders") or ins.get("n_insiders") or "several", (", $%.1fM" % (fnum((icl or {}).get("total_value")) / 1e6)) if fnum((icl or {}).get("total_value")) else ""))
            elif ins["n_buys"]:
                ev.append("%d open-market insider buy(s) recently" % ins["n_buys"])
        elif fnum(fv.get("insider_trans_pct")) is not None:
            it = fnum(fv.get("insider_trans_pct"))
            parts.append((lin_map(it, -10, 20, 10, 90), 6))
        cg = F["congress"].get(sym)
        if cg and cg.get("buys"):
            parts.append((80 if cg.get("cluster") else 65, 5))
            ev.append("%d Congress purchase(s) disclosed in 60 days" % cg["buys"])
        it2 = fnum(fv.get("inst_trans_pct"))
        legs["inst_trans_pct"] = it2
        if it2 is not None:
            parts.append((lin_map(it2, -8, 10, 8, 90), 8))
        opt = F["options"].get(sym) or {}
        blk = F["opt_blocks"].get(sym)
        o_s = None
        if opt:
            pcr, npm, sig = fnum(opt.get("pcr_vol")), fnum(opt.get("net_premium_usd")), str(opt.get("signal") or "")
            o_s = 50.0
            if pcr is not None:
                o_s += lin_map(pcr, 1.4, -20, 0.5, 20)
            if npm is not None and npm > 0:
                o_s += 15
            if "BULL" in sig.upper() or "ACCUM" in sig.upper():
                o_s += 15
            legs["options"] = {"pcr_vol": pcr, "net_premium_usd": npm, "signal": sig or None, "iv_rank": fnum(opt.get("iv_rank")), "n_unusual": opt.get("n_unusual")}
        if blk:
            o_s = (o_s or 50.0) + min(20, 6 * blk["bull"])
            legs["options_blocks"] = blk
            if blk["bull"]:
                ev.append("%d bullish smart-money option block(s) printed (%.1fM premium)" % (blk["bull"], blk["premium"] / 1e6))
        if o_s is not None:
            parts.append((clamp(o_s), 8))
            if o_s >= 70 and not blk:
                ev.append("options tape leans to call accumulation (put/call %s, net premium %s)" % (rnd(legs.get("options", {}).get("pcr_vol"), 2), rnd((legs.get("options", {}).get("net_premium_usd") or 0) / 1e6, 1)))
        sh = F["short"].get(sym) or {}
        legs["short_float_pct"] = fnum(fv.get("short_float_pct"))
        legs["days_to_cover"] = fnum(sh.get("days_to_cover"))
        legs["si_change_pct"] = fnum(sh.get("si_change_pct"))
    elif asset_class == "crypto":
        ce, cx, sc, cb = F["crypto_etf"], F["crypto_exch"], F["stablecoin"], F["coinbase"]
        if sym == "BTC":
            v = fnum(ce.get("btc_30d_usd") or ce.get("btc_30d"))
            reg = str(ce.get("btc_etf_regime") or ce.get("regime") or "")
            if v is not None or reg:
                s = 50 + (lin_map(v, -2e9, -35, 2e9, 35) if v is not None else (20 if "INFLOW" in reg.upper() else -20 if "OUTFLOW" in reg.upper() else 0))
                parts.append((clamp(s), 35))
                legs["etf_flow_30d_usd"] = v
                legs["etf_regime"] = reg or None
                if (v or 0) > 3e8 or "INFLOW" in reg.upper():
                    ev.append("spot-ETF inflows: $%.1fB over 30 days (%s)" % ((v or 0) / 1e9, reg))
            nf = fnum(cx.get("netflow_btc") or cx.get("btc_netflow") or cx.get("netflow_today"))
            reg2 = str(cx.get("btc_regime") or cx.get("regime") or "")
            if nf is not None or reg2:
                s = 50 + (lin_map(nf, 20000, -30, -20000, 30) if nf is not None else (25 if "OUTFLOW" in reg2.upper() or "ACCUM" in reg2.upper() else -20 if "INFLOW" in reg2.upper() else 0))
                parts.append((clamp(s), 30))
                legs["exchange_netflow_btc"] = nf
                legs["exchange_regime"] = reg2 or None
                if (nf or 0) < 0 or "OUTFLOW" in reg2.upper():
                    ev.append("coins are leaving exchanges (net %s BTC) -- holders are moving to cold storage" % rnd(nf, 0))
        elif sym == "ETH":
            v = fnum(ce.get("eth_30d"))
            reg = str(ce.get("eth_etf_regime") or "")
            if v is not None or reg:
                parts.append((clamp(50 + (lin_map(v, -1e9, -35, 1e9, 35) if v is not None else (20 if "INFLOW" in reg.upper() else -20 if "OUTFLOW" in reg.upper() else 0))), 35))
                legs["etf_flow_30d_usd"] = v
                legs["etf_regime"] = reg or None
            nf = fnum(cx.get("eth_netflow_30d") or cx.get("eth_netflow"))
            if nf is not None:
                parts.append((clamp(50 + lin_map(nf, 300000, -30, -300000, 30)), 30))
                legs["exchange_netflow_eth"] = nf
        st = fnum(_first(sc, "net_flow_7d_usd", "stablecoin_net_7d", "supply_change_30d_pct", "summary.net_7d"))
        sreg = str(_first(sc, "regime", "signal", "summary.regime") or "")
        if st is not None or sreg:
            s = 50 + (lin_map(st, -3e9, -25, 3e9, 25) if st is not None else (20 if "INFLOW" in sreg.upper() or "EXPAND" in sreg.upper() else -20 if "OUTFLOW" in sreg.upper() else 0))
            parts.append((clamp(s), 20))
            legs["stablecoin"] = sreg or st
            if "INFLOW" in sreg.upper() or "EXPAND" in sreg.upper() or (st or 0) > 1e9:
                ev.append("stablecoin supply is expanding -- fresh dollars are parked on-chain waiting to buy (%s)" % (sreg or rnd(st / 1e9, 1)))
        prem = fnum(_first(cb, "premium_pct", "current.premium_pct", "premium", "coinbase_premium_pct"))
        if prem is not None:
            parts.append((clamp(50 + lin_map(prem, -0.5, -25, 0.5, 25)), 15))
            legs["coinbase_premium_pct"] = prem
            if prem > 0.1:
                ev.append("Coinbase premium %.2f%% -- US institutional bid is above the global price" % prem)
    else:  # ETF
        fl = flow_legs(sym, F)
        legs["own_flow"] = {k: rnd(v, 2) if isinstance(v, float) else v for k, v in fl.items()}
        if fl.get("score") is not None:
            parts.append((fl["score"], 70))
            if fl.get("major"):
                ev.append("major inflows into the fund itself: %s%% of AUM over 21 days (z %s, $%sM true 20d)" % (
                    rnd(fl.get("pct_aum_21d") or fl.get("fv_flows_1m_pct"), 1), rnd(fl.get("z90"), 1), rnd((fl.get("true_flow_20d_usd") or 0) / 1e6, 0)))
        rot = F["rotation"].get(sym) or {}
        if rot:
            ls = fnum(rot.get("leadership_score"))
            if ls is not None:
                parts.append((ls, 15))
            legs["rotation_tag"] = rot.get("tag")
        rr = F["rrg"].get(sym)
        if isinstance(rr, dict) and rr.get("quadrant"):
            q = str(rr["quadrant"]).upper()
            parts.append((85 if "IMPROV" in q else 70 if "LEAD" in q else 35 if "WEAK" in q else 20, 15))
            legs["rrg_quadrant"] = rr["quadrant"]
    if not parts:
        return None, legs, ev
    score = sum(v * w for v, w in parts) / sum(w for _, w in parts)
    legs["n_legs"] = len(parts)
    return score, legs, ev



_STOP = {"and", "the", "of", "general", "other", "misc", "services", "products", "equipment", "specialty", "diversified", "industrial", "industries"}


def _fuzzy_industry(table, industry):
    """match a Finviz industry ('Semiconductor Equipment & Materials') to an import-canary line ('Semiconductors') by shared word stems."""
    if not table or not industry:
        return None
    toks = {t[:6] for t in re.findall(r"[a-z]+", str(industry).lower()) if len(t) >= 5 and t not in _STOP}
    if not toks:
        return None
    best, best_n = None, 0
    for k, v in table.items():
        kt = {t[:6] for t in re.findall(r"[a-z]+", str(k).lower()) if len(t) >= 5 and t not in _STOP}
        n_ = len(toks & kt)
        if n_ > best_n:
            best, best_n = v, n_
    return best


def catalyst_block(sym, fv, cs, F, mcap, asset_class, industry, country, industry_etf):
    """named catalysts: scheduled events, contracts/backlog, revisions, industry boom, ports/physical economy, commodity curves,
    peers waking up, short-squeeze fuel, rates. Each with a plain sentence; score 0-100."""
    items = []
    pts = 0.0
    if asset_class == "stock":
        cal = F["calendar"].get(sym) or []
        for e in cal[:3]:
            imp = str(e.get("impact") or "").upper()
            w = 18 if imp in ("HIGH", "3") else 10
            pts += w
            items.append({"kind": "scheduled", "when": e["date"], "text": "%s on %s%s" % (e.get("title") or e.get("type") or "event", e["date"], " (high impact)" if imp in ("HIGH", "3") else ""), "strength": w})
        eid = fnum(cs.get("earnings_in_days"))
        if eid is None:
            ed = str(fv.get("earnings_date") or "")
            try:
                if ed and ed not in ("-", ""):
                    dt = datetime.strptime(ed.split(" ")[0], "%b %d")
                    now = datetime.now(timezone.utc)
                    dt = dt.replace(year=now.year)
                    if dt < now.replace(tzinfo=None) - timedelta(days=30):
                        dt = dt.replace(year=now.year + 1)
                    eid = (dt - now.replace(tzinfo=None)).days
            except Exception:
                eid = None
        if eid is not None and 0 <= eid <= 45 and not any(i["kind"] == "scheduled" and "earn" in i["text"].lower() for i in items):
            pts += 12
            items.append({"kind": "earnings", "when": "%dd" % int(eid), "text": "earnings in %d days -- the base can resolve on the print" % int(eid), "strength": 12})
        ct = F["contracts"].get(sym)
        if ct and mcap:
            vm = ct["usd"] / mcap * 100
            w = min(25, 5 + vm * 2)
            pts += w
            items.append({"kind": "contracts", "text": "%d contract/licensing win(s) in 90 days worth $%.0fM = %.1f%% of market cap" % (ct["n"], ct["usd"] / 1e6, vm), "strength": w})
        bk = F["backlog"].get(sym) or {}
        rpo, ryoy = fnum(bk.get("rpo")), fnum(bk.get("rpo_yoy"))
        if rpo and mcap and (bk.get("demand_accelerating") or (ryoy or 0) >= 15):
            w = 14
            pts += w
            items.append({"kind": "backlog", "text": "backlog (RPO) $%.1fB = %.0f%% of market cap and growing %s%% y/y%s" % (rpo / 1e9, rpo / mcap * 100, rnd(ryoy, 0), " (accelerating)" if bk.get("demand_accelerating") else ""), "strength": w})
        rv = F["revisions"].get(sym) or {}
        if (rv.get("eps_rev_pct") or 0) > 2:
            w = 10
            pts += w
            items.append({"kind": "revisions", "text": "analysts are revising EPS up (%+.1f%%) while the stock sits at the lows" % rv["eps_rev_pct"], "strength": w})
        boom = F["boom"].get(industry) or {}
        comp = boom.get("comp") if isinstance(boom.get("comp"), dict) else {}
        rg = fnum(comp.get("rev_mean"))
        br = fnum(comp.get("rev_breadth"))
        bs = fnum(boom.get("boom_score"))
        if rg is not None and rg >= 12:
            w = min(16, 6 + rg / 4)
            pts += w
            items.append({"kind": "industry_boom", "boom_score": bs, "text": "its industry is booming: %s revenue growth %.0f%% y/y%s" % (industry, rg, (" with %.0f%% of names growing" % br) if br is not None else ""), "strength": w})
        elif bs is not None and bs >= 70:
            pts += 8
            items.append({"kind": "industry_boom", "boom_score": bs, "text": "industry boom score %.0f/100 (%s)" % (bs, industry), "strength": 8})
        # physical economy: ports of the exporter's country + industry
        pw = F["ports"].get(industry) or _fuzzy_industry(F["ports"], industry) or {}
        yoy = fnum(pw.get("yoy_pct") or pw.get("yoy"))
        if yoy is not None and yoy >= 5:
            w = min(14, 4 + yoy / 3)
            pts += w
            items.append({"kind": "ports", "text": "port traffic tied to %s is up %.0f%% y/y (%s) -- physical demand the market has not priced" % (industry, yoy, ", ".join(str(x) for x in (pw.get("ports") or [])[:3]) or "IMF PortWatch"), "strength": w})
        pc = F["ports_countries"].get(country) if country else None
        cy = fnum((pc or {}).get("yoy_pct") or (pc or {}).get("yoy")) if pc else None
        if cy is not None and cy >= 6 and country not in ("USA", "United States"):
            pts += 8
            items.append({"kind": "ports", "text": "%s's port activity is up %.0f%% y/y -- an export boom in its home country" % (country, cy), "strength": 8})
        # commodity curve tied to the industry (backwardation = physical tightness)
        for kw in INDUSTRY_PHYSICAL.get(industry, []):
            hit = commodity_read(F, kw)
            if hit:
                pts += hit["strength"]
                items.append(hit)
                break
        # peers waking up
        sy = F["sympathetic"]
        for r in (sy.get("signals") or sy.get("rows") or sy.get("board") or []):
            if isinstance(r, dict) and str(r.get("ticker") or "").upper() == sym:
                pts += 8
                items.append({"kind": "peers", "text": "peers in its group are already moving (sympathetic-momentum engine flags it as a laggard about to catch up)", "strength": 8})
                break
        # squeeze fuel
        sf = fnum(fv.get("short_float_pct"))
        dtc = fnum((F["short"].get(sym) or {}).get("days_to_cover"))
        if sf is not None and sf >= 12 and (dtc or 0) >= 4:
            pts += 8
            items.append({"kind": "squeeze", "text": "%.0f%% of the float is short with %.1f days to cover -- fuel for a violent squeeze once it turns" % (sf, dtc), "strength": 8})
        # rates tailwind for rate-sensitive groups
        if any(k in (industry or "") for k in ("REIT", "Utilities", "Residential Construction", "Banks - Regional")) or (fv.get("sector") in ("Real Estate", "Utilities")):
            fw = F["fedwatch"]
            pcut = fnum(_first(fw, "next_meeting.p_cut", "p_cut_next", "prob_cut_next_meeting", "next.cut_prob", "summary.p_cut"))
            if pcut is not None and (pcut if pcut > 1 else pcut * 100) >= 55:
                pts += 6
                items.append({"kind": "rates", "text": "rate-sensitive group with a %.0f%% market-implied chance of a Fed cut at the next meeting" % (pcut if pcut > 1 else pcut * 100), "strength": 6})
        cat = F["catalyst"].get(sym) or {}
        csx = fnum(cat.get("score"))
        if csx is not None and csx >= 55:
            pts += 8
            cls = [str(c.get("class")) for c in (cat.get("catalysts") or []) if isinstance(c, dict) and c.get("class")]
            items.append({"kind": "catalyst_engine", "text": "the catalyst engine rates it %.0f/100 (%s)" % (csx, ", ".join(cls[:3]) or "mixed"), "strength": 8})
    elif asset_class == "crypto":
        cc = F["crypto_cycle"]
        dr = fnum(cc.get("dump_risk_score"))
        if dr is not None and dr <= 35:
            pts += 12
            items.append({"kind": "cycle", "text": "crypto cycle-risk engine reads dump risk %.0f/100 -- the cycle is not late" % dr, "strength": 12})
        ce = F["crypto_etf"]
        reg = str(ce.get("btc_etf_regime") or ce.get("regime") or "")
        if "INFLOW" in reg.upper():
            pts += 10
            items.append({"kind": "flows", "text": "spot-ETF regime is %s -- Wall Street demand is switched on" % reg, "strength": 10})
        cal = F["calendar"].get(sym) or []
        for e in cal[:2]:
            pts += 10
            items.append({"kind": "scheduled", "when": e["date"], "text": "%s on %s" % (e.get("title") or e.get("type"), e["date"]), "strength": 10})
    else:  # ETF: rotation / RRG / commodity curve / country ports / rates
        rot = F["rotation"].get(sym) or {}
        if str(rot.get("tag") or "").upper() in ("EMERGING", "IMPROVING", "TURNING", "LEADER"):
            pts += 12
            items.append({"kind": "rotation", "text": "industry-rotation engine tags it %s (%s)" % (rot.get("tag"), rot.get("name") or sym), "strength": 12})
        rr = F["rrg"].get(sym)
        if isinstance(rr, dict) and "IMPROV" in str(rr.get("quadrant") or "").upper():
            pts += 10
            items.append({"kind": "rrg", "text": "RRG quadrant IMPROVING -- relative momentum is turning before relative strength", "strength": 10})
        name = str(fv.get("company") or "")
        for kw in ("gold", "silver", "copper", "uranium", "lithium", "oil", "natural gas", "steel", "corn", "wheat", "coal", "platinum", "palladium", "aluminum"):
            if kw in name.lower():
                hit = commodity_read(F, kw)
                if hit:
                    pts += hit["strength"]
                    items.append(hit)
                break
        for cty, etf in COUNTRY_ETF.items():
            if etf == sym:
                pc = F["ports_countries"].get(cty)
                cy = fnum((pc or {}).get("yoy_pct") or (pc or {}).get("yoy")) if pc else None
                if cy is not None and cy >= 5:
                    pts += 10
                    items.append({"kind": "ports", "text": "%s's port throughput is up %.0f%% y/y -- the physical economy is accelerating under the index" % (cty, cy), "strength": 10})
                break
        if BOND_RX.search(name) or REIT_RX.search(name):
            fw = F["fedwatch"]
            pcut = fnum(_first(fw, "next_meeting.p_cut", "p_cut_next", "prob_cut_next_meeting", "next.cut_prob", "summary.p_cut"))
            if pcut is not None and (pcut if pcut > 1 else pcut * 100) >= 55:
                pts += 8
                items.append({"kind": "rates", "text": "%.0f%% market-implied chance of a Fed cut at the next meeting -- a duration tailwind" % (pcut if pcut > 1 else pcut * 100), "strength": 8})
    named = [i for i in items if (i.get("kind") not in ("earnings", "scheduled", "catalyst_engine", "squeeze"))
             and not (i.get("kind") == "industry_boom" and (i.get("boom_score") is None or i["boom_score"] < 65))]
    return clamp(pts * 1.6), items, len(named)


def commodity_read(F, kw):
    cm = F["commod"]
    for r in (cm.get("fred_metrics") or []) + (cm.get("etf_metrics") or []):
        if not isinstance(r, dict):
            continue
        nm = (str(r.get("name") or "") + " " + str(r.get("symbol") or r.get("ticker") or "")).lower()
        if kw not in nm:
            continue
        ch20 = fnum(r.get("change_20d_pct") or r.get("chg_20d_pct") or r.get("ret_20d_pct"))
        shape = str(r.get("curve_shape") or r.get("structure") or r.get("regime") or "")
        bits = []
        w = 0.0
        if "BACKWARD" in shape.upper():
            bits.append("its futures curve is in backwardation (physical tightness)")
            w += 10
        if ch20 is not None and ch20 >= 4:
            bits.append("%s is up %.0f%% over 20 days" % (r.get("name") or kw, ch20))
            w += 6
        if bits:
            return {"kind": "commodity", "text": "; ".join(bits), "strength": w}
        return None
    return None


def quality_block(sym, fv, cs, F, asset_class, mcap):
    """dilution, valuation, balance sheet, forensic -- red flags veto, score 0-100 (industry-neutral valuation added later)."""
    out = {"red_flags": [], "notes": [], "score": None}
    if asset_class != "stock":
        if asset_class == "etf":
            aum = fnum(fv.get("aum"))
            if aum is not None and aum < 1e8:
                aum *= 1e6
            out["aum_usd"] = aum
            out["score"] = 70.0
        else:
            out["score"] = 65.0
        return out
    pts = []
    sc = fnum(cs.get("share_count_yoy_pct"))
    out["share_count_yoy_pct"] = sc
    out["net_buyback_yield_pct"] = fnum(cs.get("net_buyback_yield_pct"))
    if sc is not None:
        if sc >= 15:
            out["red_flags"].append("heavy dilution: share count +%.0f%% y/y" % sc)
        elif sc >= 6:
            out["notes"].append("share count +%.0f%% y/y (dilution)" % sc)
        elif sc <= -1:
            out["notes"].append("shares shrinking %.0f%% y/y (buybacks)" % -sc)
        pts.append((lin_map(sc, 12, 0, -4, 100), 20))
    pe, fpe, ps, evb, fcfy = fnum(fv.get("pe")), fnum(fv.get("fwd_pe")), fnum(fv.get("ps")), fnum(cs.get("ev_ebitda_ttm") or fv.get("ev_ebitda")), fnum(cs.get("fcf_yield_pct"))
    out.update({"pe": pe, "fwd_pe": fpe, "ps": ps, "ev_ebitda": evb, "fcf_yield_pct": fcfy, "peg": fnum(fv.get("peg")), "pb": fnum(fv.get("pb"))})
    vp = []
    if pe is not None and pe > 0:
        vp.append(lin_map(pe, 45, 10, 8, 100))
    if fpe is not None and fpe > 0:
        vp.append(lin_map(fpe, 40, 10, 8, 100))
    if evb is not None and evb > 0:
        vp.append(lin_map(evb, 30, 10, 5, 100))
    if fcfy is not None:
        vp.append(lin_map(fcfy, -2, 10, 10, 100))
    if ps is not None and ps > 0:
        vp.append(lin_map(ps, 15, 10, 1, 100))
    if vp:
        out["valuation_raw"] = mean(vp)
        pts.append((mean(vp), 30))
    az, pf, bm = fnum(cs.get("altman_z")), fnum(cs.get("piotroski_f")), fnum(cs.get("beneish_m"))
    nd, ic = fnum(cs.get("netdebt_to_ebitda_ttm")), fnum(cs.get("interest_coverage_ttm"))
    out.update({"altman_z": az, "piotroski_f": pf, "beneish_m": bm, "netdebt_to_ebitda": nd, "interest_coverage": ic})
    fd = F["floor_deep"].get(sym) or {}
    rw = fnum(fd.get("runway_months"))
    out["runway_months"] = rw
    out["floor_coverage"] = fnum(fd.get("coverage")) or fnum((F["floor_screen"].get(sym) or {}).get("approx_coverage"))
    if az is not None:
        pts.append((lin_map(az, 0.8, 0, 4, 100), 15))
        if az < 1.1 and (rw is None or rw < 12):
            out["red_flags"].append("distress zone: Altman Z %.1f%s" % (az, (", runway %.0f months" % rw) if rw is not None else ""))
        elif az < 1.8:
            out["notes"].append("Altman Z %.1f (grey zone)" % az)
    if pf is not None:
        pts.append((lin_map(pf, 2, 0, 8, 100), 10))
        if pf <= 2:
            out["notes"].append("Piotroski %d/9 (weak fundamentals)" % int(pf))
    if bm is not None:
        pts.append((lin_map(bm, -1.0, 0, -3.0, 100), 10))
        if bm > -1.78:
            out["red_flags"].append("Beneish M-score %.2f -- earnings-manipulation risk" % bm)
    if nd is not None:
        pts.append((lin_map(nd, 5, 0, 0, 100), 10))
        if nd >= 4.5:
            out["red_flags"].append("net debt %.1fx EBITDA" % nd)
        elif nd >= 3:
            out["notes"].append("net debt %.1fx EBITDA" % nd)
    if ic is not None and ic < 1.5 and (nd or 0) > 0:
        out["red_flags"].append("interest coverage %.1fx" % ic)
    if rw is not None and rw < 9:
        out["red_flags"].append("cash runway %.0f months" % rw)
    rg = fnum(cs.get("revenue_yoy_pct")) if cs.get("revenue_yoy_pct") is not None else fnum(fv.get("sales_yoy_ttm"))
    out["revenue_yoy_pct"] = rg
    out["eps_yoy_pct"] = fnum(cs.get("eps_yoy_pct"))
    if rg is not None:
        pts.append((lin_map(rg, -15, 10, 25, 100), 5))
    if not pts:
        return out
    out["score"] = sum(v * w for v, w in pts) / sum(w for _, w in pts)
    if out["red_flags"]:
        out["score"] = clamp(out["score"] - 20 * len(out["red_flags"]))
    return out

# ── universe ────────────────────────────────────────────────────────────────
def classify_etf(fv):
    name = str(fv.get("company") or "")
    et = str(fv.get("etf_type") or "").lower()
    if LEV_RX.search(name) or "leveraged" in et or "inverse" in et:
        return None
    if OVERLAY_RX.search(name):
        return None
    if MONEY_RX.search(name):
        return None
    if CRYPTO_ETF_RX.search(name):
        return "crypto_etf"
    if BOND_RX.search(name) or "bond" in et or "fixed" in et:
        return "bond"
    if COMMOD_RX.search(name) or "commod" in et:
        return "commodity"
    if CURRENCY_RX.search(name) or "currenc" in et:
        return "currency"
    if REIT_RX.search(name):
        return "real_estate"
    if COUNTRY_RX.search(name) or "country" in et or "region" in et or "emerging" in et or "international" in et:
        return "country"
    if "equit" in et or "sector" in et or "industry" in et or "size" in et or "growth" in et or "value" in et or "dividend" in et or not et:
        return "equity_etf"
    return None


def is_etf_row(fv):
    at = str(fv.get("asset_type") or "").lower()
    return "etf" in at or bool(fv.get("etf_type")) or bool(fv.get("aum"))


def build_universe(F):
    stocks, etfs = {}, {}
    for tk, fv in F["finviz"].items():
        if not isinstance(fv, dict):
            continue
        sym = to_poly(tk)
        if not TICKER_OK.match(sym):
            continue
        if is_etf_row(fv):
            cls = classify_etf(fv)
            if not cls:
                continue
            aum = fnum(fv.get("aum"))
            if aum is not None and aum < 1e8:
                aum *= 1e6
            if aum is not None and aum < P["min_etf_aum"]:
                continue
            etfs[sym] = cls
        else:
            mcap = fnum(fv.get("market_cap"))
            if mcap is not None and mcap < 1e8:
                mcap *= 1e6
            if mcap is not None and mcap < P["min_mcap"]:
                continue
            ind = str(fv.get("industry") or "")
            if ind.startswith("Shell Companies") or ind.startswith("Exchange Traded") or ind.startswith("Closed-End"):
                continue
            stocks[sym] = mcap
    keep = set(stocks) | set(etfs) | set(BENCH) | set(SECTOR_ETF.values()) | set(IND_ETF.values()) | set(COUNTRY_ETF.values())
    return stocks, etfs, keep


# ── row builder ─────────────────────────────────────────────────────────────
def build_row(sym, asset_class, b, dates, spy_c, F, mkt, sub_class=None):
    sig = price_signals(b, dates, spy_c)
    fv = F["finviz"].get(to_fv(sym)) or {}
    cs = F["census"].get(sym) or {}
    mcap = fnum(fv.get("market_cap"))
    if mcap is not None and mcap < 1e8:
        mcap *= 1e6
    if mcap is None:
        mcap = fnum(cs.get("mcap"))
    industry = str(fv.get("industry") or "")
    sector = str(fv.get("sector") or "")
    country = str(fv.get("country") or "")
    ind_etf = IND_ETF.get(industry) or SECTOR_ETF.get(sector)
    loc_s, loc_gate, loc_legs = location_score(sig)
    os_s, os_gate, os_legs = oversold_score(sig)
    st_s, st_state, st_legs = structure_score(sig)
    knife, knife_why = knife_guard(sig)
    acc = sig["accum"]
    acc_s = acc["score"]
    fleet_acc = F["fleet_accum"].get(sym) or []
    if acc_s is not None and fleet_acc:
        acc_s = clamp(acc_s + min(12, 4 * len(fleet_acc)))
    adv = sig["risk"].get("adv_usd_20d")
    in_s, in_legs, in_ev = inflow_block(sym, fv, cs, F, mcap, adv, asset_class, ind_etf)
    cat_s, cat_items, n_named = catalyst_block(sym, fv, cs, F, mcap, asset_class, industry, country, ind_etf)
    q = quality_block(sym, fv, cs, F, asset_class, mcap)
    mom = sig["mom"]
    r = {"ticker": sym, "name": (fv.get("company") or sym)[:60], "asset_class": asset_class, "sub_class": sub_class,
         "sector": sector or None, "industry": industry or None, "country": country or None, "industry_etf": ind_etf,
         "last": rnd(sig["last"], 4 if sig["last"] < 1 else 2), "mcap": mcap, "adv_usd": rnd(adv, 0),
         "sessions": sig["sessions"], "n_weeks": sig["n_weeks"], "n_months": sig["n_months"], "n_quarters": sig["n_quarters"],
         # location
         "sma50": rnd(sig["loc"].get("sma50"), 2), "sma100": rnd(sig["loc"].get("sma100"), 2),
         "sma200": rnd(sig["loc"].get("sma200"), 2), "sma250": rnd(sig["loc"].get("sma250"), 2), "ema200": rnd(sig["loc"].get("ema200"), 2), "ema250": rnd(sig["loc"].get("ema250"), 2),
         "dist_sma200_pct": rnd(sig["loc"].get("dist_sma200_pct"), 1), "dist_sma250_pct": rnd(sig["loc"].get("dist_sma250_pct"), 1),
         "dist_ema200_pct": rnd(sig["loc"].get("dist_ema200_pct"), 1), "dist_ema250_pct": rnd(sig["loc"].get("dist_ema250_pct"), 1),
         "days_below_sma200": sig["loc"].get("days_below_sma200"), "sma200_slope_20_pct": rnd(sig["loc"].get("sma200_slope_20_pct"), 2),
         "below_sma200": sig["loc"].get("below_sma200"), "below_sma250": sig["loc"].get("below_sma250"),
         "pos_52w_pct": rnd(sig["loc"].get("pos_52w_pct"), 0), "dd_52w_pct": rnd(sig["loc"].get("dd_52w_pct"), 1), "max_dd_1y_pct": rnd(sig["loc"].get("max_dd_1y_pct"), 1),
         "low_52w": rnd(sig["loc"].get("low_52w"), 2), "high_52w": rnd(sig["loc"].get("high_52w"), 2),
         # rsi
         "rsi_d": rnd(sig["rsi"].get("rsi_d"), 1), "rsi_w": rnd(sig["rsi"].get("rsi_w"), 1), "rsi_m": rnd(sig["struct_mq"].get("rsi_m"), 1),
         "rsi_w_turning_up": sig["rsi"].get("rsi_w_turning_up"), "rsi_div_w": sig["rsi"].get("rsi_div_w"), "rsi_div_d": sig["rsi"].get("rsi_div_d"),
         # structure
         "lt_downtrend": sig["struct_w"].get("lt_downtrend"), "lt_trend_break": sig["struct_w"].get("lt_trend_break"), "weeks_since_break": sig["struct_w"].get("weeks_since_break"),
         "trendline_level": rnd(sig["struct_w"].get("trendline_level"), 2), "double_bottom_w": sig["struct_w"].get("double_bottom"), "double_bottom_d": sig.get("double_bottom_d"),
         "higher_lows_w": sig["struct_w"].get("higher_lows_w"), "lower_lows_w": sig["struct_w"].get("lower_lows_w"), "lower_highs_w": sig["struct_w"].get("lower_highs_w"),
         "base_weeks": sig["struct_w"].get("base_weeks"), "sma40w_falling": sig["struct_w"].get("sma40_falling"), "above_ema10_w": sig["struct_w"].get("above_ema10_w"),
         "m_higher_low": sig["struct_mq"].get("m_higher_low"), "m_lower_lows": sig["struct_mq"].get("m_lower_lows"), "m_close_vs_sma12_pct": rnd(sig["struct_mq"].get("m_close_vs_sma12_pct"), 1),
         "q_lower_highs": sig["struct_mq"].get("q_lower_highs"), "q_break": sig["struct_mq"].get("q_break"), "q_ret_pct": rnd(sig["struct_mq"].get("q_ret_pct"), 1), "m_ret_pct": rnd(sig["struct_mq"].get("m_ret_pct"), 1),
         "structure_state": st_state, "structure_legs": st_legs,
         # accumulation
         "accum_legs": acc["legs"], "accum_evidence": acc["evidence"], "fleet_accumulation": fleet_acc,
         "bbw_pctile": rnd(acc.get("bbw_pctile"), 0), "pct_b": rnd(acc.get("pct_b"), 2), "squeeze": acc.get("squeeze"), "vol_ratio_20_120": rnd(acc.get("vol_ratio_20_120"), 2),
         "obv_slope_40": rnd(acc.get("obv_slope_40"), 3), "ad_slope_40": rnd(acc.get("ad_slope_40"), 3), "updown_vol_20": rnd(acc.get("updown_vol_20"), 2),
         "absorption_clv": rnd(acc.get("absorption_clv"), 2), "atr_pctile": rnd(acc.get("atr_pctile"), 0), "lower_half_days_20": acc.get("lower_half_days_20"),
         # momentum arrival
         "mom_legs": mom["legs"], "mom_evidence": mom["evidence"], "rs_63_pct": rnd(mom.get("rs_63_pct"), 1), "rs_slope_20": rnd(mom.get("rs_slope_20"), 4),
         "macd_w_turn": mom.get("macd_w_turn"), "roc20_cross_days": mom.get("roc20_cross"), "squeeze_lean": mom.get("squeeze_lean"),
         "ret_1m_pct": rnd(mom.get("ret_1m_pct"), 1), "ret_3m_pct": rnd(mom.get("ret_3m_pct"), 1), "ret_6m_pct": rnd(mom.get("ret_6m_pct"), 1), "ret_12m_pct": rnd(mom.get("ret_12m_pct"), 1),
         # risk
         "vol_ann_pct": rnd(sig["risk"].get("vol_ann_pct"), 1), "beta_1y": rnd(sig["risk"].get("beta_1y"), 2), "cvar5_pct": rnd(sig["risk"].get("cvar5_pct"), 2),
         "worst_day_1y_pct": rnd(sig["risk"].get("worst_day_1y_pct"), 1), "gap_risk_pct": rnd(sig["risk"].get("gap_risk_pct"), 1),
         # fusion
         "inflow_legs": in_legs, "inflow_evidence": in_ev, "catalysts": cat_items, "n_named_catalysts": n_named, "quality": q,
         "knife": knife, "knife_why": knife_why,
         "pillars": {"location": rnd(loc_s, 1), "oversold": rnd(os_s, 1), "structure": rnd(st_s, 1), "accumulation": rnd(acc_s, 1),
                     "inflows": rnd(in_s, 1), "catalyst": rnd(cat_s, 1), "momentum": rnd(mom["score"], 1), "quality": rnd(q.get("score"), 1)},
         "gates": {"location": bool(loc_gate), "washout": bool(_washout(sig)), "oversold": bool(os_gate), "accumulation": bool(acc_s is not None and acc_s >= P["accum_gate"]),
                   "inflows": bool(in_s is not None and in_s >= P["inflow_gate"]), "structure": st_state != "NONE",
                   "catalyst": bool(cat_s >= P["catalyst_gate"]), "not_knife": not knife, "quality": not q.get("red_flags")},
         "location_legs": loc_legs, "oversold_legs": os_legs}
    return r



def _washout(sig):
    """tradability: a real drawdown (>=10% off the 52-week high, or >=6% under the 200-day) and >=10% annualised volatility."""
    loc, rk = sig["loc"], sig["risk"]
    dd = loc.get("dd_52w_pct")
    d200 = loc.get("dist_sma200_pct")
    vol = rk.get("vol_ann_pct")
    if vol is not None and vol < 10.0:
        return False
    return (dd is not None and dd <= -10.0) or (d200 is not None and d200 <= -6.0)


def gates_and_tier(r):
    g = r["gates"]
    n = sum(1 for k in ("location", "oversold", "accumulation", "inflows", "structure", "catalyst") if g.get(k))
    r["gates_passed"] = n + (1 if g["not_knife"] else 0) + (1 if g["quality"] else 0)
    core = g["location"] and g["not_knife"] and g["quality"] and g.get("washout", True)
    if not core:
        tier = "WATCH" if (g["location"] and n >= 3) else "SCREENED"
        if not g["quality"] or not g["not_knife"]:
            tier = "SCREENED" if n < 4 else "WATCH"
        if g["location"] and not g.get("washout", True):
            tier = "WATCH" if n >= 3 else "SCREENED"
    elif g["oversold"] and g["accumulation"] and g["inflows"] and g["structure"] and g["catalyst"] and r["structure_state"] == "CONFIRMED" and (r.get("n_named_catalysts") or 0) >= 1:
        tier = "KATLIN_PRIME"
    elif g["accumulation"] and g["structure"] and (g["oversold"] or g["inflows"]) and (n >= 4):
        tier = "READY"
    elif g["accumulation"] and (g["oversold"] or g["structure"] or g["inflows"]):
        tier = "BASING"
    elif n >= 2:
        tier = "WATCH"
    else:
        tier = "SCREENED"
    r["tier"] = tier
    return tier


def composite(r, ranks):
    p = r["pillars"]
    num = den = 0.0
    for k, w in WEIGHTS.items():
        v = p.get(k)
        if v is None:
            continue
        rk = ranks.get(k, {}).get(r["ticker"])
        blended = 0.5 * v + 0.5 * rk if rk is not None else v
        num += blended * w
        den += w
    if den < 0.6 * sum(WEIGHTS.values()):
        r["composite"] = None
        r["conviction"] = None
        return
    r["composite"] = rnd(num / den, 1)
    gp = [p.get(k) for k in GATE_PILLARS if p.get(k) is not None]
    r["conviction"] = rnd(math.exp(sum(math.log(max(x, 1.0)) for x in gp) / len(gp)), 1) if gp else None
    cov = den / sum(WEIGHTS.values())
    r["evidence_coverage_pct"] = rnd(cov * 100, 0)
    if r["knife"]:
        r["composite"] = rnd(r["composite"] * 0.6, 1)
    if r["quality"].get("red_flags"):
        r["composite"] = rnd(r["composite"] * 0.75, 1)


def trade_plan(r):
    px = r["last"]
    dbw = r.get("double_bottom_w") or {}
    dbd = r.get("double_bottom_d") or {}
    # stop: the nearest structural low under price (weekly double-bottom low, daily double-bottom low, 60-day low, 52-week low)
    cands = [x for x in (dbw.get("low2"), dbd.get("low2"), r.get("low_60d"), r.get("low_52w")) if x and px and x < px]
    stop = None
    if cands:
        near = max(cands)                       # the closest support below price
        stop = near * 0.97
        if stop < px * 0.75:                    # support too far to be a stop -- use the volatility budget instead
            stop = px * 0.80
    if stop is None or stop >= px:
        stop = px * 0.90 if px else None
    # targets: nearest structural level above price first, then the next one / the 52-week high
    levels = []
    for lab, v in (("neckline", dbw.get("neckline")), ("50-day average", r.get("sma50")), ("100-day average", r.get("sma100")),
                   ("200-day average", r.get("sma200")), ("52-week high", r.get("high_52w"))):
        if v and px and v > px * 1.04:
            levels.append((v, lab))
    levels.sort()
    t1, t1_label = (levels[0] if levels else (None, None))
    t2, t2_label = (levels[-1] if len(levels) >= 2 else (None, None))
    if t2 and t1 and t2 < t1 * 1.08 and len(levels) >= 3:
        t2, t2_label = levels[-1]
    up1 = (t1 / px - 1) * 100 if (t1 and px) else None
    up2 = (t2 / px - 1) * 100 if (t2 and px) else None
    dn = (1 - stop / px) * 100 if (stop and px) else None
    rr = min(up1 / dn, 10.0) if (up1 and dn) else None
    rr2 = min(up2 / dn, 10.0) if (up2 and dn) else None
    sigma_m = (r.get("vol_ann_pct") or 40.0) / math.sqrt(12)
    asym = (median([x for x in (up1, up2) if x]) / max(dn or 10.0, 0.5 * sigma_m)) if (up1 or up2) else None
    trig = None
    if dbw.get("neckline") and dbw.get("state") != "CONFIRMED":
        trig = dbw["neckline"]
    elif r.get("trendline_level") and not r.get("lt_trend_break"):
        trig = r["trendline_level"]
    r["plan"] = {"entry": px, "stop": rnd(stop, 2), "target_1": rnd(t1, 2), "target_1_label": t1_label,
                 "target_2": rnd(t2, 2), "target_2_label": t2_label, "upside_1_pct": rnd(up1, 1), "upside_2_pct": rnd(up2, 1),
                 "downside_pct": rnd(dn, 1), "rr_1": rnd(rr, 1), "rr_2": rnd(rr2, 1), "asymmetry": rnd(min(asym, 25) if asym else None, 1),
                 "confirmation_trigger": rnd(trig, 2)}
    r["asymmetry"] = r["plan"]["asymmetry"]
    r["rr"] = max(x for x in (r["plan"]["rr_1"], r["plan"]["rr_2"]) if x is not None) if (r["plan"]["rr_1"] or r["plan"]["rr_2"]) else None
    r["plan"]["rr_best"] = r["rr"]


def why_text(r):
    """plain English for normies -- built only from the numbers that exist on the row."""
    s = []
    nm = r["name"] if r["name"] != r["ticker"] else r["ticker"]
    ac = {"stock": "stock", "etf": "fund", "crypto": "coin"}.get(r["asset_class"], "asset")
    d200 = r.get("dist_sma200_pct")
    if d200 is not None and d200 < 0:
        s.append("%s (%s) is a %s trading %.0f%% below its 200-day average%s." % (
            nm, r["ticker"], ac, -d200, (" and %.0f%% below the 250-day" % -r["dist_sma250_pct"]) if (r.get("dist_sma250_pct") or 0) < 0 else "",))
    else:
        s.append("%s (%s) sits %s%% %s its 200-day average." % (nm, r["ticker"], rnd(abs(d200 or 0), 0), "above" if (d200 or 0) >= 0 else "below"))
    dbs = r.get("days_below_sma200") or 0
    if dbs >= 120:
        s.append("It has been under that average for %d sessions, so this is a long downtrend, not a dip." % dbs)
    elif dbs >= 40:
        s.append("It has been under that average for %d sessions -- a multi-month breakdown that is now old enough to base." % dbs)
    elif dbs:
        s.append("It slipped under that average only %d sessions ago, so treat the location as a fresh breakdown rather than a washed-out base." % dbs)
    st = r.get("structure_legs") or []
    if r["structure_state"] == "CONFIRMED":
        s.append("On the long-term chart the bottom looks CONFIRMED: " + ", ".join(st[:3]) + ".")
    elif r["structure_state"] == "FORMING":
        s.append("A bottom is FORMING on the weekly chart: " + ", ".join(st[:3]) + " -- not confirmed yet.")
    elif st:
        s.append("Weekly structure: " + ", ".join(st[:2]) + ".")
    os_ = r.get("oversold_legs") or []
    if os_:
        s.append("Momentum is washed out: " + ", ".join(os_[:3]) + ".")
    ae = r.get("accum_evidence") or []
    if ae:
        s.append("Accumulation fingerprints: " + "; ".join(ae[:3]) + ".")
    if r.get("fleet_accumulation"):
        s.append("Other JustHodl engines already flag it (%s)." % ", ".join(r["fleet_accumulation"][:3]))
    ie = r.get("inflow_evidence") or []
    if ie:
        s.append("Money is flowing in: " + "; ".join(ie[:3]) + ".")
    ci = [c["text"] for c in (r.get("catalysts") or [])[:3]]
    if ci:
        s.append("Catalysts the engine can name: " + "; ".join(ci) + ".")
    me = r.get("mom_evidence") or []
    if me:
        s.append("Momentum is starting to arrive: " + "; ".join(me[:2]) + ".")
    q = r.get("quality") or {}
    bits = []
    if q.get("pe") and q["pe"] > 0:
        bits.append("P/E %.0f" % q["pe"])
    if q.get("fwd_pe") and q["fwd_pe"] > 0:
        bits.append("forward P/E %.0f" % q["fwd_pe"])
    if q.get("ev_ebitda") and q["ev_ebitda"] > 0:
        bits.append("EV/EBITDA %.0f" % q["ev_ebitda"])
    if q.get("fcf_yield_pct") is not None:
        bits.append("FCF yield %.1f%%" % q["fcf_yield_pct"])
    if q.get("share_count_yoy_pct") is not None:
        bits.append("share count %+.0f%% y/y" % q["share_count_yoy_pct"])
    if bits:
        s.append("Valuation and dilution check: " + ", ".join(bits) + ".")
    if q.get("red_flags"):
        s.append("RED FLAGS: " + "; ".join(q["red_flags"]) + ".")
    elif q.get("notes"):
        s.append("Watch: " + "; ".join(q["notes"][:2]) + ".")
    if r.get("knife"):
        s.append("KNIFE WARNING: %s -- wait for a higher low before touching it." % r["knife_why"])
    pl = r.get("plan") or {}
    if pl.get("stop") and (pl.get("target_1") or pl.get("target_2")):
        s.append("Plan: buy around %s, stop %s (%.0f%% risk), first target %s (%s, +%.0f%%)%s -- best reward/risk %sx." % (
            r["last"], pl["stop"], pl.get("downside_pct") or 0, pl.get("target_1") or pl.get("target_2"), pl.get("target_1_label") or pl.get("target_2_label"),
            pl.get("upside_1_pct") or pl.get("upside_2_pct") or 0, (", then %s (+%.0f%%)" % (pl["target_2"], pl["upside_2_pct"])) if (pl.get("target_1") and pl.get("target_2")) else "",
            pl.get("rr_best") or "?"))
    if pl.get("confirmation_trigger"):
        s.append("Confirmation trigger: a weekly close above %s." % pl["confirmation_trigger"])
    sn = r.get("sniper")
    if sn and sn.get("state"):
        s.append("4-hour entry: %s" % sn["text"])
    r["why"] = " ".join(s)


# ── 4h sniper lane ──────────────────────────────────────────────────────────
def bank_intraday(sym, asset_class, today):
    tick = ("X:%sUSD" % sym) if asset_class == "crypto" else sym
    key = INTRADAY_ROOT + sym + ".json.gz"
    frm = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=70)).strftime("%Y-%m-%d")
    try:
        res = poly_aggs(tick, 4, "hour", frm, today)
    except Exception as e:
        return None, "err:%s" % str(e)[:50]
    rows = [[datetime.fromtimestamp(r["t"] / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%MZ"), float(r.get("o") or r["c"]), float(r.get("h") or r["c"]),
             float(r.get("l") or r["c"]), float(r["c"]), float(r.get("v") or 0.0)] for r in res if r.get("c")]
    doc = {"symbol": sym, "source": "polygon %s 4h" % tick, "rows": rows, "banked_at": now_iso(), "n": len(rows)}
    if rows:
        s3_put_json(key, doc, gz=True)
    return doc, "ok:%d" % len(rows)


def sniper_read(doc, r):
    rows = (doc or {}).get("rows") or []
    if len(rows) < 40:
        return {"state": None, "text": "not enough 4h bars", "n": len(rows)}
    c = [x[4] for x in rows]
    h = [x[2] for x in rows]
    l = [x[3] for x in rows]
    v = [x[5] for x in rows]
    rs = rsi_series(c)
    rsi4 = rs[-1] if rs else None
    e20 = ema_last(c, 20)
    e50 = ema_last(c, 50)
    mid, up, lo, bw, pb = bb_series(c, 20, 2.0)
    hi_idx, lo_idx = swing_points(h[-60:], 2)
    _, lo_idx2 = swing_points(l[-60:], 2)
    seg_h = h[-60:]
    seg_l = l[-60:]
    desc = []
    for i in hi_idx:
        if not desc or seg_h[i] < seg_h[desc[-1]]:
            desc.append(i)
        else:
            desc = [i]
    micro_line = None
    if len(desc) >= 2:
        i1, i2 = desc[-2], desc[-1]
        slope = (seg_h[i2] - seg_h[i1]) / float(i2 - i1)
        micro_line = seg_h[i2] + slope * (59 - i2)
    hl = len(lo_idx2) >= 2 and seg_l[lo_idx2[-1]] > seg_l[lo_idx2[-2]]
    last = c[-1]
    vol_ratio = (mean(v[-6:]) / mean(v[-60:])) if mean(v[-60:]) else None
    recent_high = max(h[-12:])
    out = {"rsi_4h": rnd(rsi4, 1), "ema20_4h": rnd(e20, 2), "ema50_4h": rnd(e50, 2), "pct_b_4h": rnd(pb[-1], 2), "higher_low_4h": bool(hl),
           "micro_downtrend_line": rnd(micro_line, 2), "vol_ratio_6_60": rnd(vol_ratio, 2), "n": len(rows), "last_4h": rnd(last, 2), "asof": rows[-1][0]}
    if micro_line and last > micro_line and hl and (vol_ratio or 0) >= 1.0:
        out["state"] = "SNIPE_NOW"
        out["text"] = "4h chart broke its micro downtrend (%s) on a higher low with volume %.1fx -- entry is live; stop under the last 4h low %s." % (rnd(micro_line, 2), vol_ratio or 0, rnd(min(l[-8:]), 2))
    elif hl and e20 and abs(last / e20 - 1) <= 0.02 and (rsi4 or 50) <= 55:
        out["state"] = "SNIPE_PULLBACK"
        out["text"] = "4h is holding a higher low and sitting on its 20-EMA (%s) -- buy the hold, stop under %s." % (rnd(e20, 2), rnd(min(l[-8:]), 2))
    elif rsi4 is not None and rsi4 <= 32 and pb[-1] is not None and pb[-1] <= 0.15:
        out["state"] = "WAIT_TURN"
        out["text"] = "4h RSI %.0f at the lower band -- wait for the first 4h higher low, then buy above %s." % (rsi4, rnd(recent_high, 2))
    elif micro_line:
        out["state"] = "WAIT_BREAK"
        out["text"] = "4h still under its micro downtrend line at %s -- trigger is a 4h close above it%s." % (rnd(micro_line, 2), " (higher low already in)" if hl else "")
    else:
        out["state"] = "WAIT"
        out["text"] = "no 4h trigger yet; the level to beat is %s." % rnd(recent_high, 2)
    return out


def run_sniper(rows, today):
    short = [r for r in rows if r["tier"] in ("KATLIN_PRIME", "READY", "BASING")]
    short.sort(key=lambda r: -(r.get("composite") or 0))
    short = short[:P["shortlist"]]
    if not short or not POLY_KEY:
        return 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        res = list(ex.map(lambda r: bank_intraday(r["ticker"], r["asset_class"], today), short))
    n = 0
    errs = 0
    for r, (doc, st) in zip(short, res):
        if doc:
            r["sniper"] = sniper_read(doc, r)
            n += 1
        else:
            errs += 1
            r["sniper"] = {"state": None, "text": st}
    log("sniper: %d/%d shortlist names got 4h bars (%d errors)" % (n, len(short), errs))
    if errs and errs >= len(short) // 2:
        DEGRADED.append("4h sniper lane: %d/%d Polygon errors" % (errs, len(short)))
    return n


# ── snapshots, self-grading, changes ────────────────────────────────────────
def snapshot_and_base_rates(rows, bars, dates, session):
    picks = [{"t": r["ticker"], "c": r["asset_class"], "tier": r["tier"], "score": r.get("composite"), "px": r["last"]} for r in rows if r["tier"] in ("KATLIN_PRIME", "READY", "BASING")]
    s3_put_json(HIST_PREFIX + session + ".json.gz", {"session": session, "version": VERSION, "picks": picks, "spy": bars["SPY"].c[-1] if "SPY" in bars else None}, gz=True)
    keys = sorted(k for k in list_keys(HIST_PREFIX) if k.endswith(".json.gz"))
    idx = {d: i for i, d in enumerate(dates)}
    cur = idx.get(session, len(dates) - 1)
    out = {"n_snapshots": len(keys), "by_tier": {}, "status": "accruing"}
    graded = {}
    for k in keys[-90:]:
        d = k.rsplit("/", 1)[1][:10]
        if d not in idx or cur - idx[d] < 21:
            continue
        snap = s3_json(k, None)
        if not snap:
            continue
        i0 = idx[d]
        spy0 = snap.get("spy")
        spy = bars.get("SPY")
        for p in snap.get("picks") or []:
            b = bars.get(p["t"])
            if not b or p.get("c") == "crypto":
                continue
            p0 = b.pos_at_or_before(i0)
            if p0 is None:
                continue
            for horizon in (21, 63):
                if cur - i0 < horizon:
                    continue
                p1 = b.pos_at_or_before(i0 + horizon)
                if p1 is None or b.d[p1] - i0 < horizon - 3:
                    continue
                ret = (b.c[p1] / b.c[p0] - 1) * 100
                ex = None
                if spy and spy0:
                    ps = spy.pos_at_or_before(i0 + horizon)
                    if ps is not None:
                        ex = ret - (spy.c[ps] / spy0 - 1) * 100
                g = graded.setdefault((p["tier"], horizon), {"n": 0, "ret": [], "ex": [], "hit": 0})
                g["n"] += 1
                g["ret"].append(ret)
                if ex is not None:
                    g["ex"].append(ex)
                    g["hit"] += 1 if ex > 0 else 0
    for (tier, hz), g in graded.items():
        out["by_tier"].setdefault(tier, {})["%ds" % hz] = {"n": g["n"], "median_ret_pct": rnd(median(g["ret"]), 2), "median_excess_vs_spy_pct": rnd(median(g["ex"]), 2),
                                                             "hit_rate_pct": rnd(100.0 * g["hit"] / len(g["ex"]), 0) if g["ex"] else None}
    if graded:
        out["status"] = "graded"
    return out


def session_changes(rows, prev):
    prev_tier = {}
    for r in (prev or {}).get("picks") or []:
        prev_tier[r.get("ticker")] = r.get("tier")
    new_prime = [r["ticker"] for r in rows if r["tier"] == "KATLIN_PRIME" and prev_tier.get(r["ticker"]) != "KATLIN_PRIME"]
    new_ready = [r["ticker"] for r in rows if r["tier"] in ("KATLIN_PRIME", "READY") and prev_tier.get(r["ticker"]) not in ("KATLIN_PRIME", "READY")]
    cur = {r["ticker"]: r["tier"] for r in rows}
    dropped = [t for t, tr in prev_tier.items() if tr in ("KATLIN_PRIME", "READY") and cur.get(t) not in ("KATLIN_PRIME", "READY")]
    return {"new_prime": new_prime[:30], "new_ready": new_ready[:40], "dropped": dropped[:40], "prev_session": (prev or {}).get("session")}


# ── walk-forward backtest of the PRICE gates (no look-ahead) ────────────────
def lite_gates(b, pos, spy_c, dates):
    """point-in-time: below SMA200, weekly RSI<=40 or divergence, weekly structure FORMING+, accumulation>=55 (price legs only)."""
    if pos < 300:
        return None
    sub = Bars()
    sub.d.extend(b.d[:pos + 1])
    sub.c.extend(b.c[:pos + 1])
    sub.h.extend(b.h[:pos + 1])
    sub.l.extend(b.l[:pos + 1])
    sub.v.extend(b.v[:pos + 1])
    sub.o.extend(b.o[:pos + 1])
    sig = price_signals(sub, dates, spy_c[:b.d[pos] + 1])
    loc_s, loc_gate, _ = location_score(sig)
    os_s, os_gate, _ = oversold_score(sig)
    st_s, st_state, _ = structure_score(sig)
    knife, _ = knife_guard(sig)
    acc = sig["accum"]["score"]
    return {"location": bool(loc_gate), "oversold": bool(os_gate), "structure": st_state != "NONE", "confirmed": st_state == "CONFIRMED",
            "accumulation": bool(acc is not None and acc >= P["accum_gate"]), "knife": knife, "mom": sig["mom"]["score"]}


def run_backtest(event):
    t0 = time.time()
    keys = session_keys(int(event.get("sessions") or P["sessions"]))
    F = {"finviz": (s3_json("data/finviz-universe.json", {}) or {}).get("by_ticker") or {}}
    stocks, etfs, keep = build_universe(F)
    # sample the universe for tractability: the 900 largest by market cap + all ETFs
    top = sorted(stocks.items(), key=lambda kv: -(kv[1] or 0))[:int(event.get("n_stocks") or 800)]
    etf_keep = [t for t, c in etfs.items() if c in ("equity_etf", "country", "commodity", "bond", "real_estate")][:int(event.get("n_etfs") or 300)]
    keep = set(t for t, _ in top) | set(etf_keep) | {"SPY"}
    dates, bars = load_bars(keys, keep)
    spy = bars.get("SPY")
    if not spy:
        raise RuntimeError("no SPY bars")
    spy_c = [None] * len(dates)
    for p in range(len(spy.d)):
        spy_c[spy.d[p]] = spy.c[p]
    step = int(event.get("step") or 21)
    horizons = (21, 63, 126)
    cohorts = {}
    per_date = []
    n_obs = 0
    for pos_idx in range(320, len(dates) - 21, step):
        if time.time() - t0 > 780:
            log("backtest budget hit at %s" % dates[pos_idx])
            break
        d_rows = {"date": dates[pos_idx], "n": 0, "cohorts": {}}
        spy_fwd = {}
        for hz in horizons:
            if pos_idx + hz < len(dates) and spy_c[pos_idx] and spy_c[min(pos_idx + hz, len(dates) - 1)]:
                spy_fwd[hz] = (spy_c[pos_idx + hz] / spy_c[pos_idx] - 1) * 100
        for tk, b in bars.items():
            if tk == "SPY":
                continue
            p = b.pos_at_or_before(pos_idx)
            if p is None or b.d[p] != pos_idx or p < 300:
                continue
            g = lite_gates(b, p, spy_c, dates)
            if not g:
                continue
            n_obs += 1
            labels = []
            if g["location"] and not g["knife"]:
                labels.append("below200")
                if g["oversold"]:
                    labels.append("below200+oversold")
                if g["accumulation"]:
                    labels.append("below200+accum")
                if g["structure"]:
                    labels.append("below200+structure")
                if g["oversold"] and g["accumulation"] and g["structure"]:
                    labels.append("KATLIN_price_3of3")
                if g["confirmed"] and g["accumulation"]:
                    labels.append("confirmed_bottom+accum")
            elif g["location"] and g["knife"]:
                labels.append("knife")
            else:
                labels.append("above200")
            for hz in horizons:
                if hz not in spy_fwd:
                    continue
                p1 = b.pos_at_or_before(pos_idx + hz)
                if p1 is None or b.d[p1] < pos_idx + hz - 3:
                    continue
                ret = (b.c[p1] / b.c[p] - 1) * 100
                ex = ret - spy_fwd[hz]
                # max adverse excursion over the horizon (worst close)
                lo = min(b.c[q] for q in range(p, p1 + 1))
                mae = (lo / b.c[p] - 1) * 100
                for lab in labels:
                    c = cohorts.setdefault(lab, {}).setdefault(hz, {"n": 0, "ret": [], "ex": [], "mae": [], "hit": 0})
                    c["n"] += 1
                    c["ret"].append(ret)
                    c["ex"].append(ex)
                    c["mae"].append(mae)
                    c["hit"] += 1 if ex > 0 else 0
                    if hz == 63:
                        dc = d_rows["cohorts"].setdefault(lab, {"n": 0, "ex": []})
                        dc["n"] += 1
                        dc["ex"].append(ex)
        d_rows["n"] = sum(v["n"] for v in d_rows["cohorts"].values())
        d_rows["spy_63"] = rnd(spy_fwd.get(63), 2)
        d_rows["cohorts"] = {k: {"n": v["n"], "median_excess_63": rnd(median(v["ex"]), 2)} for k, v in d_rows["cohorts"].items()}
        per_date.append(d_rows)
    table = {}
    for lab, byh in cohorts.items():
        table[lab] = {}
        for hz, c in byh.items():
            table[lab]["%ds" % hz] = {"n": c["n"], "median_ret_pct": rnd(median(c["ret"]), 2), "mean_excess_pct": rnd(mean(c["ex"]), 2),
                                      "median_excess_pct": rnd(median(c["ex"]), 2), "hit_rate_pct": rnd(100.0 * c["hit"] / c["n"], 0),
                                      "median_max_adverse_pct": rnd(median(c["mae"]), 2), "p10_ret_pct": rnd(sorted(c["ret"])[max(0, len(c["ret"]) // 10)], 2)}
    doc = {"engine": ENGINE, "version": VERSION, "mode": "backtest", "as_of": now_iso(), "sessions": len(dates), "first": dates[0], "last": dates[-1],
           "n_obs": n_obs, "n_dates": len(per_date), "step": step, "universe": {"stocks": len(top), "etfs": len(etf_keep)},
           "cohorts": table, "per_date": per_date, "elapsed_s": rnd(time.time() - t0, 1),
           "note": "point-in-time PRICE gates only (below SMA200, oversold, weekly structure, volume-accumulation); flows/fundamentals/catalysts are not backtested -- no look-ahead. "
                   "Excess = asset return minus SPY over the same window; MAE = worst close inside the window. above200 is the base rate."}
    s3_put_json(BACKTEST_KEY, doc)
    log("backtest done: %d obs, %d dates, %.0fs" % (n_obs, len(per_date), time.time() - t0))
    return {"ok": True, "n_obs": n_obs, "n_dates": len(per_date), "elapsed_s": doc["elapsed_s"]}


def validation_summary(bt):
    if not bt:
        return {"status": "no backtest yet -- runs Sundays", "cohorts": None}
    co = bt.get("cohorts") or {}
    pick = {}
    for lab in ("above200", "below200", "below200+oversold", "below200+accum", "below200+structure", "KATLIN_price_3of3", "confirmed_bottom+accum", "knife"):
        if lab in co:
            pick[lab] = co[lab]
    return {"status": "walk-forward %s..%s, %s obs over %s dates (as of %s)" % (bt.get("first"), bt.get("last"), bt.get("n_obs"), bt.get("n_dates"), bt.get("as_of")),
            "cohorts": pick, "note": bt.get("note")}


# ── panels for the command desk (war-room table shape) ──────────────────────
def desk_panels(rows, wr):
    def pick_rows(tier, n=40):
        out = []
        for r in [x for x in rows if x["tier"] == tier][:n]:
            out.append({"key": r["ticker"], "label": r["ticker"], "name": r["name"], "asset_class": r["asset_class"], "last": r["last"], "kind": "price",
                        "dod": r.get("ret_1m_pct"), "d5": r.get("ret_3m_pct"), "d20": r.get("dist_sma200_pct"), "unit": "pct",
                        "flag": "GREEN" if tier == "KATLIN_PRIME" else ("AMBER" if tier == "READY" else ""), "score": r.get("composite"), "rr": r.get("rr"),
                        "rsi_w": r.get("rsi_w"), "structure": r["structure_state"], "sniper": (r.get("sniper") or {}).get("state"), "why": (r.get("why") or "")[:260]})
        return out
    war = [{"key": l["leg"], "label": l["leg"], "last": l["risk"], "kind": "index", "unit": "", "flag": l["flag"], "read": l["read"][:160], "source": l["source"], "asof": l.get("asof")} for l in wr["legs"]]
    return {"war_room": war, "prime": pick_rows("KATLIN_PRIME"), "ready": pick_rows("READY", 60), "basing": pick_rows("BASING", 60),
            "etfs": pick_rows_class(rows, "etf"), "crypto": pick_rows_class(rows, "crypto")}


def pick_rows_class(rows, cls, n=40):
    out = []
    for r in [x for x in rows if x["asset_class"] == cls and x["tier"] != "SCREENED"][:n]:
        out.append({"key": r["ticker"], "label": r["ticker"], "name": r["name"], "tier": r["tier"], "last": r["last"], "kind": "price", "dod": r.get("ret_1m_pct"),
                    "d5": r.get("ret_3m_pct"), "d20": r.get("dist_sma200_pct"), "unit": "pct", "flag": "GREEN" if r["tier"] == "KATLIN_PRIME" else ("AMBER" if r["tier"] == "READY" else ""),
                    "score": r.get("composite"), "rsi_w": r.get("rsi_w"), "structure": r["structure_state"], "why": (r.get("why") or "")[:200]})
    return out


DEFINITIONS = {
    "washout gate": "an asymmetric bottom needs a real drawdown: at least 10% off the 52-week high or 6% under the 200-day, with annualised volatility of 10% or more. Money-market and ultra-short bond funds that sit a hair under a flat average are never buy candidates.",
    "posture": "The war room's decision BEFORE any pick: FULL_RISK / SELECTIVE / DEFENSIVE / CASH_OR_TBILLS, from a weighted risk thermometer over the bond desk, auction desk, brain risk-gate, black-swan watch, crisis composite, options tail risk, regime, volatility, VIX curve, credit spreads, recession probability, business cycle, dollar, global liquidity and cross-asset regime. Hard vetoes force CASH_OR_TBILLS.",
    "exposure_cap_pct": "Maximum share of the portfolio the posture allows in risk assets today. Brain doctrine: macro gates sizing before selection.",
    "location": "Distance to the 200- and 250-day simple/exponential averages. Gate: close BELOW the 200-day (the spec); below the 250-day is a bonus. Further below = more reward room, until the knife guard trips.",
    "oversold": "RSI on the weekly (<=40), daily (<=35) and monthly (<=45) frames, plus weekly RSI turning up from a washout and bullish RSI divergence (price lower low, RSI higher low).",
    "structure": "Long-term bottom evidence on weekly/monthly/quarterly bars: weekly double bottom (two lows within 6%, neckline break = CONFIRMED), a close above the weekly downtrend line drawn through descending swing highs, higher weekly lows, a monthly higher low, a quarterly close above the prior quarter's high. Lower lows still printing subtract.",
    "accumulation": "Wyckoff/Chaikin/O'Neil volume-structure read: volume dry-up (20d vs 120d), Bollinger bandwidth percentile vs the asset's own year and TTM squeeze, lower-band hugging, OBV rising against flat price, A/D line slope, up/down volume ratio, absorption (closing location on the heaviest down days), ATR contraction, and the 20-day low holding above the 2-month low. Fleet engines (accumulation-radar, whales, stealth-accumulation, fortress, volatility-squeeze) add confirmation points.",
    "inflows": "Capital arriving: industry-ETF flows (Polygon creations z-score, % of AUM, true flows, Finviz), ETF constituent pressure, 13F net institutional and whale dollars vs market cap, dark-pool share/acceleration/state, insider clusters, Congress buys, institutional ownership change, options accumulation (put/call, net premium, smart-money blocks). Crypto: spot-ETF flows, exchange net flows, stablecoin supply, Coinbase premium. ETFs: their own flows plus rotation leadership and RRG quadrant.",
    "catalyst": "Named, dated where possible: scheduled events (catalyst calendar), earnings inside 45 days, contract wins vs market cap, backlog acceleration, upward EPS revisions, industry boom (revenue growth + breadth), IMF PortWatch traffic by industry and by the exporter's country, commodity futures-curve backwardation for the metal/energy the industry depends on, peers waking up, short-squeeze fuel, Fed-cut odds for rate-sensitive groups.",
    "momentum": "Signs that momentum is about to arrive: RS line vs SPY at a 3-month high before price, weekly MACD histogram rising from below zero, 20-day ROC crossing positive, squeeze lean, price above the 10-week EMA.",
    "quality": "Dilution (share count y/y, net buyback yield), valuation (P/E, forward P/E, EV/EBITDA, FCF yield, P/S), Altman Z, Piotroski, Beneish, net debt/EBITDA, interest coverage, floor-audit runway. Red flags veto the buy tiers.",
    "knife": "Guard against catching a collapse: -40% in 3 months with no higher low; -45% below the 200-day while printing 52-week lows; three lower weekly lows into a fresh low without divergence.",
    "tiers": "KATLIN_PRIME = every gate incl. CONFIRMED bottom and a named catalyst; READY = accumulation + structure + (oversold or inflows), 4+ gates; BASING = accumulation plus one more; WATCH = partial; SCREENED = failed.",
    "composite": "0-100: pillar scores (structure 18, accumulation 20, inflows 16, oversold 10, location 8, catalyst 10, momentum 8, quality 10), each blended 50/50 with its cross-sectional rank; knife x0.6, red flags x0.75. Conviction = geometric mean of the six gate pillars.",
    "plan": "Stop under the double-bottom / 52-week low (floored at -20%); target 1 = 200-day average (mean reversion) or the neckline; target 2 = 52-week high; reward/risk = upside/downside; asymmetry = median upside / max(downside, half a monthly sigma), capped 25.",
    "sniper": "Only for the shortlist and only for timing: 4-hour bars -- micro downtrend-line break on a higher low with volume (SNIPE_NOW), higher low holding the 20-EMA (SNIPE_PULLBACK), RSI washout at the lower band (WAIT_TURN), or the level a 4h close must clear (WAIT_BREAK).",
    "validation": "Weekly walk-forward through the whole bar warehouse: the PRICE gates are scored point-in-time every 21 sessions and their 21/63/126-session excess returns vs SPY, hit rates and max adverse excursion are published by cohort. Flows/fundamentals/catalysts are not backtested. Daily snapshots of the picks are self-graded at 21/63 sessions.",
}


# ── handler ─────────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    event = event or {}
    if event.get("mode") == "backtest":
        return run_backtest(event)
    t0 = time.time()
    LOG.clear()
    DEGRADED.clear()
    ROW_ERRS.clear()
    F = load_feeds()
    stocks, etfs, keep = build_universe(F)
    keys = session_keys(P["sessions"])
    if len(keys) < P["min_sessions"]:
        raise RuntimeError("bar warehouse too thin: %d sessions" % len(keys))
    dates, bars = load_bars(keys, keep)
    session = dates[-1]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    spy = bars.get("SPY")
    if not spy:
        raise RuntimeError("SPY missing from the bar warehouse")
    spy_c = [None] * len(dates)
    for p in range(len(spy.d)):
        spy_c[spy.d[p]] = spy.c[p]
    mkt = market_context(spy)
    log("universe: %d stocks, %d etfs, bars for %d tickers, %d sessions %s..%s" % (len(stocks), len(etfs), len(bars), len(dates), dates[0], dates[-1]))
    wr = war_room(F)
    log("war room: %s (therm %s, %d legs, vetoes %s, missing %s)" % (wr["posture"], wr["thermometer"], len(wr["legs"]), wr["vetoes"], wr["missing"]))
    rows = []
    n_thin = n_hyg = 0
    t1 = time.time()
    for sym, mcap in stocks.items():
        b = bars.get(sym)
        if not b or len(b.d) < P["min_sessions"]:
            n_thin += 1
            continue
        if b.c[-1] < P["min_price"]:
            n_hyg += 1
            continue
        if mean([b.v[i] * b.c[i] for i in range(len(b.d) - 20, len(b.d))]) < P["min_adv_usd"]:
            n_hyg += 1
            continue
        if b.d[-1] < len(dates) - 3:
            n_thin += 1
            continue
        try:
            rows.append(build_row(sym, "stock", b, dates, spy_c, F, mkt))
        except Exception as e:
            row_error("stock", sym, e)
        if time.time() - t1 > 520:
            DEGRADED.append("stock scoring stopped early at %d names (time budget)" % len(rows))
            break
    log("stocks scored: %d (thin %d, hygiene %d) in %.0fs" % (len(rows), n_thin, n_hyg, time.time() - t1))
    if ROW_ERRS:
        log("row errors by message: %s" % sorted(ROW_ERRS.items(), key=lambda kv: -kv[1])[:6])
    for sym, cls in etfs.items():
        b = bars.get(sym)
        if not b or len(b.d) < P["min_sessions"] or b.d[-1] < len(dates) - 3:
            continue
        try:
            rows.append(build_row(sym, "etf", b, dates, spy_c, F, mkt, sub_class=cls))
        except Exception as e:
            row_error("etf", sym, e)
    # crypto: own calendar, own SPY alignment (use BTC-relative RS -> pass BTC closes as the 'market')
    try:
        syms = list(dict.fromkeys(P["crypto_symbols"]))
        cdates, cbars = load_crypto(today, syms)
        btc = cbars.get("BTC")
        btc_c = [None] * len(cdates)
        if btc:
            for p in range(len(btc.d)):
                btc_c[btc.d[p]] = btc.c[p]
        for sym, b in cbars.items():
            if b.d[-1] < len(cdates) - 3:
                continue
            try:
                r = build_row(sym, "crypto", b, cdates, btc_c if btc else [None] * len(cdates), F, mkt, sub_class="coin")
                r["name"] = sym + "/USD"
                rows.append(r)
            except Exception as e:
                row_error("crypto", sym, e)
    except Exception as e:
        DEGRADED.append("crypto lane failed: %s" % str(e)[:120])
    # cross-sectional pillar ranks within asset class, industry-neutral valuation adjust for stocks
    ranks = {}
    for cls in ("stock", "etf", "crypto"):
        sub = [r for r in rows if r["asset_class"] == cls]
        for k in WEIGHTS:
            vals = [r["pillars"].get(k) for r in sub]
            pr = pct_rank(vals)
            for r, v in zip(sub, pr):
                ranks.setdefault(k, {})[r["ticker"]] = v
    for r in rows:
        gates_and_tier(r)
        composite(r, ranks)
        trade_plan(r)
    try:
        dilution_lane(rows, ranks, today)
    except Exception as e:
        DEGRADED.append("dilution lane failed: %s" % str(e)[:100])
    # posture applied: in DEFENSIVE / CASH the buy tiers are demoted to their evidence but flagged
    for r in rows:
        r["posture_note"] = None
        if wr["posture"] == "CASH_OR_TBILLS" and r["tier"] in ("KATLIN_PRIME", "READY"):
            r["posture_note"] = "war room says CASH/T-BILLS -- watchlist only until the veto clears"
        elif wr["posture"] == "DEFENSIVE" and r["tier"] in ("KATLIN_PRIME", "READY"):
            r["posture_note"] = "war room DEFENSIVE -- half size, confirmed bottoms only"
    rows.sort(key=lambda r: (TIER_ORDER.index(r["tier"]), -(r.get("composite") or 0)))
    run_sniper(rows, today)
    for r in rows:
        why_text(r)
    prev = s3_json(OUT_KEY, None)
    changes = session_changes(rows, prev)
    base_rates = snapshot_and_base_rates(rows, bars, dates, session)
    published = [r for r in rows if r["tier"] != "SCREENED"]
    tiers = {t: sum(1 for r in rows if r["tier"] == t) for t in TIER_ORDER}
    gates = {g: sum(1 for r in rows if r["gates"].get(g)) for g in ("location", "oversold", "accumulation", "inflows", "structure", "catalyst", "not_knife", "quality")}
    top_picks = [{"ticker": r["ticker"], "score": r.get("composite"), "tier": r["tier"], "asset_class": r["asset_class"]} for r in rows if r["tier"] in ("KATLIN_PRIME", "READY")][:50]
    out = {"engine": ENGINE, "version": VERSION, "schema": "1.0", "generated_at": now_iso(), "as_of": session, "session": session, "elapsed_s": rnd(time.time() - t0, 1),
           "war_room": wr, "market": {k: rnd(v, 2) if isinstance(v, float) else v for k, v in mkt.items()},
           "universe": {"stocks_in_universe": len(stocks), "etfs_in_universe": len(etfs), "crypto_symbols": len(P["crypto_symbols"]), "scored": len(rows),
                        "stocks_scored": sum(1 for r in rows if r["asset_class"] == "stock"), "etfs_scored": sum(1 for r in rows if r["asset_class"] == "etf"),
                        "crypto_scored": sum(1 for r in rows if r["asset_class"] == "crypto"), "sessions": len(dates), "first_session": dates[0], "published": len(published)},
           "tiers": tiers, "gates": gates, "weights": WEIGHTS, "params": {k: v for k, v in P.items() if k != "crypto_symbols"},
           "top_picks": top_picks,
           "picks": [r for r in published if r["tier"] in ("KATLIN_PRIME", "READY", "BASING")][:300],
           "watch": [{k: r.get(k) for k in ("ticker", "name", "asset_class", "sub_class", "sector", "industry", "last", "dist_sma200_pct", "dist_sma250_pct", "rsi_w", "rsi_d",
                                            "structure_state", "composite", "gates", "pillars", "knife", "tier")} for r in published if r["tier"] == "WATCH"][:400],
           "panels": desk_panels(rows, wr), "changes": changes, "base_rates": base_rates, "validation": validation_summary(F.get("backtest")),
           "feeds_asof": F["asof"], "definitions": DEFINITIONS, "degraded": DEGRADED, "log": LOG[-60:]}
    n = s3_put_json(OUT_KEY, out)
    log("wrote %s (%.1f MB) tiers=%s posture=%s" % (OUT_KEY, n / 1e6, tiers, wr["posture"]))
    return {"ok": True, "session": session, "posture": wr["posture"], "thermometer": wr["thermometer"], "tiers": tiers, "scored": len(rows),
            "bytes": n, "elapsed_s": out["elapsed_s"], "degraded": DEGRADED}
