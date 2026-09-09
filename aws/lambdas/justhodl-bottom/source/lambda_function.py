"""justhodl-bottom v1.0.0 -- BOTTOM: the Wyckoff bottom desk across every asset class.

Khalid's spec (2026-09-09, from his paper "How to spot a bottom"), verbatim intent:
  A true bottom is not a price point, it is a PROCESS with three events and one confirmation:
    1. SELLING CLIMAX (SC)  -- a steep decline that ends in one (or a few) very wide-range bars on
       climactic volume (2-3x average or more): the panic, absorbed by large operators.
    2. AUTOMATIC RALLY (AR) -- the sharp technical bounce that follows because sellers are spent;
       the AR high and the SC low define the range. The crowd buys this bounce; the professional does not.
    3. SECONDARY TEST (ST)  -- price drifts back toward the SC low. The signal is NOT the price
       level, it is the VOLUME: the test must arrive on significantly diminished volume with
       narrowing bars ("knock on the door again and hear only silence").
  TRIGGER: never buy the level -- buy the reaction away from it: price trading above the HIGH of
       the candle that made the ST low.
  PROTECTION: the stop sits just below the absolute low of the test; a break invalidates the thesis.
  Scenarios: a test on RISING volume that breaks the low = FAILED (downtrend continues; stand aside /
       short); multiple tests are fine only if each arrives on LOWER volume than the last; an ST that
       holds ABOVE the SC low (higher low) is exceptional strength -- the precursor to a swift markup.

Built the way a fund's internal pattern desk is built:
  * every instrument with real volume in the HOUSE warehouse: US stocks + every ETF wrapper (bonds,
    gold, silver, platinum/palladium, copper/industrial metals, energy, agriculture, broad commodity,
    currencies, countries, sectors, real estate, crypto wrappers) from data/warm/polygon-full/grouped,
    plus spot crypto from the Polygon daily bank Katlin keeps in data/warm/katlin/crypto-bars;
  * one causal state machine per instrument per frame (DAILY and WEEKLY) over ~5 years of bars, so
    every completed sequence in history is an EVENT with a measured outcome -- the base rates on the
    page are real, not quoted: hit rates by asset class / score / test depth, the stop-hit rate, and
    the paper's own counterfactual measured (buying the automatic rally vs waiting for the trigger);
  * fleet confirmation joins (accumulation-radar, phase-detector, fortress, katlin, 13F, insiders,
    dark pool, ETF flows) are EVIDENCE on the row, never gates -- the tape decides, the fleet confirms;
  * zero external API calls in the daily run except the incremental crypto bank (Polygon, warehouse
    first); no fabricated data anywhere -- a missing input is a diagnostic, never a default.

Output: data/bottom.json (+ data/bottom/history/{session}.json.gz). Consumers: bottom.html, katlin
(structure join), fortress (evidence field), the fusion layer (adapter "bottom"), alert-router
(check_bottom), the harvester (top_picks -> eng:bottom), the command desk.
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
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3
from managed_secret import managed_secret  # env first, then SSM -- no literal credentials

VERSION = "1.2.0"
ENGINE = "justhodl-bottom"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/bottom.json"
HIST_PREFIX = "data/bottom/history/"
BARS_ROOT = "data/warm/polygon-full/grouped/"
CRYPTO_ROOT = "data/warm/katlin/crypto-bars/"     # shared warehouse bank (katlin writes the same format)
VALIDATION_ONLY = False
POLY_KEY = managed_secret(("POLYGON_API_KEY", "POLYGON_KEY", "POLY_KEY"), ("/justhodl/polygon/api-key",))

s3 = boto3.client("s3", region_name="us-east-1")
LOG = []
DEGRADED = []
ROW_ERRS = {}

P = {
    "sessions": 1260,             # ~5y of daily bars; weekly frame resampled from them
    "min_sessions": 250,
    "min_weeks": 60,
    "min_price": 2.0,
    "min_adv_usd": 1.0e6,
    "min_mcap": 1.5e8,
    "min_etf_aum": 5.0e7,
    "board_rows": 320,            # rows carried with a chart payload
    "class_rows": 80,
    "recent_closed_bars": {"D": 25, "W": 8},   # a closed event stays visible on the row this long
    # --- the algorithm (daily / weekly) ---
    "D": {"atr_n": 14, "vol_n": 50, "vol_regime_n": 250, "decline_lookback": 120, "decline_min_atr": 6.0, "decline_min_pct": 12.0,
          "newlow_lookback": 120, "vol_dominance_n": 60, "downtrend_min_bars": 15, "crash_pct": 25.0, "ma_n": 50, "range_n": 252, "pos_max_pct": 35.0,
          "climax_vol_x": 2.0, "climax_range_x": 1.5, "cluster_bars": 3,
          "ar_window": 15, "ar_min_atr": 2.0, "ar_confirm_bars": 2,
          "test_zone_atr": 1.0, "undercut_atr": 0.25, "spring_atr": 1.0, "spring_recover_bars": 3,
          "max_test_bars": 90, "trigger_window": 15, "max_wait_bars": 40, "outcome_bars": 63,
          "stop_buffer_atr": 0.10, "fail_vol_x": 0.70, "fail_vol_avg_x": 1.5},
    "W": {"atr_n": 10, "vol_n": 26, "vol_regime_n": 104, "decline_lookback": 52, "decline_min_atr": 5.0, "decline_min_pct": 20.0,
          "newlow_lookback": 26, "vol_dominance_n": 26, "downtrend_min_bars": 6, "crash_pct": 35.0, "ma_n": 26, "range_n": 104, "pos_max_pct": 35.0,
          "climax_vol_x": 1.8, "climax_range_x": 1.4, "cluster_bars": 2,
          "ar_window": 8, "ar_min_atr": 1.5, "ar_confirm_bars": 1,
          "test_zone_atr": 1.0, "undercut_atr": 0.25, "spring_atr": 1.0, "spring_recover_bars": 2,
          "max_test_bars": 40, "trigger_window": 8, "max_wait_bars": 20, "outcome_bars": 26,
          "stop_buffer_atr": 0.10, "fail_vol_x": 0.70, "fail_vol_avg_x": 1.4},
    "decline_min_pct_by_desk": {"bonds": 6.0, "currencies": 5.0, "gold_metals": 10.0, "commodities": 10.0, "countries": 10.0, "equity_etfs": 10.0, "stocks": 12.0, "crypto": 18.0},
    "decline_min_pct_by_desk_W": {"bonds": 10.0, "currencies": 8.0, "gold_metals": 15.0, "commodities": 15.0, "countries": 15.0, "equity_etfs": 15.0, "stocks": 20.0, "crypto": 30.0},
    "crypto_symbols": ["BTC", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE", "AVAX", "LINK", "DOT", "LTC",
                       "BCH", "UNI", "ATOM", "NEAR", "APT", "ARB", "OP", "SUI", "POL", "TRX",
                       "XLM", "HBAR", "ICP", "FIL", "AAVE", "MKR", "INJ", "TIA", "SEI", "RENDER",
                       "FET", "TAO", "ONDO", "ALGO", "VET", "ETC", "STX", "IMX", "GRT", "LDO", "RUNE",
                       "SAND", "MANA", "CRV", "PEPE", "SHIB", "WIF", "BONK", "JUP", "PYTH", "TON", "KAS",
                       "XMR", "ZEC", "XTZ", "FLOW", "ENA", "STRK", "EIGEN", "HYPE", "PENDLE", "AERO", "WLD"],
}

# ---- asset-class taxonomy (ETF wrappers by name/type; the board groups these into desks) ----------------------------
LEV_RX = re.compile(r"\b(2x|3x|-1x|-2x|-3x|ultra|ultrapro|bull|bear|inverse|short|leveraged|direxion|proshares ultra)\b", re.I)
OVERLAY_RX = re.compile(r"\bvix\b|volatility|market neutral|anti-beta|buffer|defined outcome|covered call|buywrite|buy-write|"
                        r"option income|premium income|yieldmax|target[- ]?\d|managed futures|hedged equity|\bcollar\b", re.I)
MONEY_RX = re.compile(r"treasury bill|t-bill|\b0-3 month|\b1-3 month|\b0-1 year|1-12 month|ultra[- ]?short|floating rate|floating-rate|"
                      r"money market|cash reserve|short maturity|\bsgov\b|\bbil\b", re.I)
CRYPTO_ETF_RX = re.compile(r"\b(bitcoin|ether|ethereum|crypto|solana|blockchain|digital asset)\b", re.I)
GOLD_MINER_RX = re.compile(r"gold.*(miner|explorer|producer|royalt)|(miner|explorer).*gold|junior gold", re.I)
GOLD_RX = re.compile(r"\bgold\b", re.I)
SILVER_MINER_RX = re.compile(r"silver.*(miner|explorer)", re.I)
SILVER_RX = re.compile(r"\bsilver\b", re.I)
PGM_RX = re.compile(r"platinum|palladium|precious metal", re.I)
COPPER_RX = re.compile(r"\bcopper\b", re.I)
IND_METAL_RX = re.compile(r"steel|aluminum|aluminium|nickel|lithium|uranium|rare earth|strategic metal|base metal|industrial metal|"
                          r"metals? (&|and) mining|\bmining\b|battery metal|tin\b|zinc", re.I)
ENERGY_CMD_RX = re.compile(r"\b(crude|oil fund|brent|wti|natural gas|gasoline|heating oil|carbon|energy fund)\b", re.I)
AGRI_RX = re.compile(r"agricultur|\bcorn\b|\bwheat\b|soybean|\bsugar\b|\bcoffee\b|\bcocoa\b|\bcotton\b|livestock|cattle|grains", re.I)
BROAD_CMD_RX = re.compile(r"commodit|\bgsci\b|bloomberg roll select|diversified futures", re.I)
BOND_GOVT_RX = re.compile(r"treasur|government|\btips\b|inflation[- ]protected|muni|municipal|agency|mortgage[- ]backed|\bmbs\b|zero coupon", re.I)
BOND_CREDIT_RX = re.compile(r"corporate|high yield|junk|credit|investment grade|bank loan|senior loan|leveraged loan|preferred|convertible|"
                            r"aggregate|total bond|core bond|fixed income|\bbond\b|\bnote\b|\bclo\b|\bcredit\b", re.I)
BOND_INTL_RX = re.compile(r"emerging market.*(bond|debt|local)|international (bond|treasury)|global bond|(bond|debt).*emerging", re.I)
CURRENCY_RX = re.compile(r"\b(currency|dollar index|dollar bullish|dollar bearish|yen|euro trust|pound|franc|yuan|renminbi|peso|real trust|krona|"
                         r"canadian dollar|australian dollar)\b", re.I)
COUNTRY_RX = re.compile(r"\b(msci [a-z ]+|china|japan|india|brazil|mexico|korea|taiwan|germany|united kingdom|canada|australia|indonesia|"
                        r"vietnam|singapore|hong kong|israel|turkey|south africa|chile|colombia|argentina|saudi|europe|pacific|asia|"
                        r"emerging|frontier|latin|eafe|acwi|international|all world|ex-us|ex us|global)\b", re.I)
REIT_RX = re.compile(r"\b(reit|real estate|property|homebuilder)\b", re.I)
SECTOR_RX = re.compile(r"select sector|\bsector\b|semiconductor|software|bank|biotech|health care|healthcare|energy|financial|industrial|"
                       r"materials|utilities|consumer|technology|communication|retail|transport|aerospace|defense|solar|clean energy|"
                       r"infrastructure|oil & gas|exploration|equipment|internet|cloud|cyber|robotics|artificial intelligence", re.I)
TICKER_OK = re.compile(r"^[A-Z]{1,5}(\.[AB])?$")

# desks shown on the page; sub_class -> desk
DESK = {"stock": "stocks", "equity_etf": "equity_etfs", "sector": "equity_etfs", "country": "countries", "real_estate": "equity_etfs",
        "bond_govt": "bonds", "bond_credit": "bonds", "bond_intl": "bonds",
        "gold": "gold_metals", "gold_miners": "gold_metals", "silver": "gold_metals", "silver_miners": "gold_metals", "pgm": "gold_metals",
        "copper": "gold_metals", "industrial_metals": "gold_metals",
        "energy": "commodities", "agriculture": "commodities", "commodity_broad": "commodities",
        "currency": "currencies", "crypto_etf": "crypto", "crypto": "crypto"}
DESK_LABEL = {"stocks": "Stocks", "equity_etfs": "Equity & sector ETFs", "countries": "Countries", "bonds": "Bonds", "gold_metals": "Gold & metals",
              "commodities": "Commodities", "currencies": "Currencies", "crypto": "Crypto"}
# wrappers that must be on the board regardless of the AUM screen (the asset classes Khalid named)
CORE_WRAPPERS = {
    "gold": ["GLD", "IAU", "GLDM", "SGOL", "OUNZ", "BAR", "AAAU"], "gold_miners": ["GDX", "GDXJ", "RING", "SGDM", "GOAU"],
    "silver": ["SLV", "SIVR"], "silver_miners": ["SIL", "SILJ"], "pgm": ["PPLT", "PALL", "PLTM"],
    "copper": ["CPER", "COPX", "COPJ"], "industrial_metals": ["DBB", "XME", "PICK", "SLX", "LIT", "URA", "URNM", "REMX", "NLR"],
    "energy": ["USO", "BNO", "UNG", "UGA", "UCO"], "agriculture": ["DBA", "CORN", "WEAT", "SOYB", "CANE", "JO", "NIB", "MOO"],
    "commodity_broad": ["DBC", "GSG", "PDBC", "COMT", "BCI"],
    "bond_govt": ["TLT", "IEF", "IEI", "SHY", "TIP", "VTIP", "ZROZ", "EDV", "GOVT", "MUB", "MBB", "SPTL", "VGLT"],
    "bond_credit": ["LQD", "HYG", "JNK", "VCIT", "VCLT", "AGG", "BND", "BKLN", "SRLN", "PFF", "CWB", "IGIB", "USHY"],
    "bond_intl": ["EMB", "PCY", "EMLC", "BNDX", "BWX", "IGOV", "VWOB"],
    "currency": ["UUP", "UDN", "FXE", "FXY", "FXB", "FXA", "FXC", "FXF", "CEW", "CYB"],
    "crypto_etf": ["IBIT", "FBTC", "GBTC", "BITO", "ETHA", "ETHE", "BITB", "ARKB", "HODL", "ETHW"],
    "country": ["EEM", "VWO", "EFA", "FXI", "MCHI", "KWEB", "EWJ", "EWY", "EWT", "INDA", "EWZ", "EWW", "EWG", "EWU", "EWC", "EWA", "EZA",
                "TUR", "ARGT", "ECH", "VNM", "EIDO", "THD", "EPOL", "KSA", "EWH", "EWS", "GXC", "EWI", "EWP", "EWQ", "EWL", "EWD", "EWN"],
    "sector": ["XLK", "XLV", "XLF", "XLY", "XLP", "XLE", "XLI", "XLB", "XLU", "XLRE", "XLC", "SMH", "IGV", "XBI", "KRE", "XOP", "OIH",
               "ITB", "XHB", "XRT", "IYT", "JETS", "ITA", "TAN", "ICLN", "PAVE", "IHI", "KIE", "XAR", "SOXX", "ARKK", "FDN", "HACK"],
    "equity_etf": ["SPY", "QQQ", "IWM", "DIA", "RSP", "VTI", "MDY", "IJR", "VTV", "VUG", "MTUM", "QUAL", "IWD", "IWF"],
    "real_estate": ["VNQ", "IYR", "XLRE", "REM", "REZ", "SRVR"],
}
BENCH = ["SPY", "QQQ", "IWM", "TLT", "GLD", "UUP", "HYG"]


# ---- helpers --------------------------------------------------------------------------------------------------------
def log(msg):
    line = "[bottom] " + str(msg)
    print(line)
    LOG.append(line[:300])


def row_error(kind, sym, e):
    msg = "%s: %s" % (type(e).__name__, str(e)[:100])
    n = ROW_ERRS.get(msg, 0) + 1
    ROW_ERRS[msg] = n
    if n == 1:
        log("%s row error %s -> %s | %s" % (kind, sym, msg, " | ".join(traceback.format_exc().strip().splitlines()[-4:])))
    elif n in (10, 100, 1000):
        log("%s row error x%d: %s" % (kind, n, msg))


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def s3_json(key, default=None, quiet=False):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body)
        return json.loads(body)
    except Exception as e:
        if not quiet:
            log("feed miss %s: %s" % (key, str(e)[:80]))
        return default


def s3_put_json(key, obj, gz=False):
    body = json.dumps(obj, separators=(",", ":"), default=str, allow_nan=False).encode()
    if gz:
        body = gzip.compress(body)
    if VALIDATION_ONLY:
        return len(body)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json", **({"ContentEncoding": "gzip"} if gz else {}))
    return len(body)


def list_keys(prefix):
    out, tok = [], None
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
        if not s or s in ("-", "--", "n/a", "N/A", "None"):
            return None
        mult = 1.0
        if s[-1] in "KMBT":
            mult = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[s[-1]]
            s = s[:-1]
        v = float(s) * mult
        return None if (math.isnan(v) or math.isinf(v)) else v
    except Exception:
        return None


def rnd(v, n=2):
    v = fnum(v)
    return None if v is None else round(v, n)


def clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else 0.5 * (xs[n // 2 - 1] + xs[n // 2])


def linreg_slope(vals):
    n = len(vals)
    if n < 3:
        return None
    mx = (n - 1) / 2.0
    my = sum(vals) / n
    num = sum((i - mx) * (v - my) for i, v in enumerate(vals))
    den = sum((i - mx) ** 2 for i in range(n))
    return num / den if den else None


def pct_rank(sorted_vals, v):
    if not sorted_vals or v is None:
        return None
    return 100.0 * bisect.bisect_left(sorted_vals, v) / len(sorted_vals)


# ---- bars -----------------------------------------------------------------------------------------------------------
class Bars:
    __slots__ = ("d", "c", "h", "l", "v", "o")

    def __init__(self):
        self.d = array.array("i")
        self.c = array.array("d")
        self.h = array.array("d")
        self.l = array.array("d")
        self.v = array.array("d")
        self.o = array.array("d")


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
        out.append((t, float(c), float(r.get("h") or c), float(r.get("l") or c), float(r.get("v") or 0.0), float(r.get("o") or c)))
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
                    b.d.append(idx); b.c.append(c); b.h.append(h); b.l.append(lo); b.v.append(v); b.o.append(o)
            if (start // chunk) % 10 == 0:
                log("bars %d/%d sessions, %d tickers" % (min(start + chunk, len(keys)), len(keys), len(bars)))
    try:
        repair_splits(dates, bars)
    except Exception as e:
        DEGRADED.append("split repair skipped: %s" % str(e)[:100])
    return dates, bars


# split repair: the grouped store was backfilled adjusted through 2026-08-31; katlin banks Polygon's split calendar daily
SPLITS_KEY = "data/warm/katlin/splits.json"


def repair_splits(dates, bars):
    doc = s3_json(SPLITS_KEY, None, quiet=True) or {}
    splits = [x for x in (doc.get("splits") or []) if x.get("ticker") in bars and x.get("split_from") and x.get("split_to")]
    n_fixed = 0
    for x in splits:
        b = bars[x["ticker"]]
        cut = bisect.bisect_left(dates, str(x.get("execution_date"))[:10])
        if cut <= 0:
            continue
        f = float(x["split_from"]) / float(x["split_to"])
        if f <= 0 or abs(f - 1.0) < 1e-9:
            continue
        for p in range(len(b.d)):
            if b.d[p] < cut:
                b.c[p] *= f; b.h[p] *= f; b.l[p] *= f; b.o[p] *= f; b.v[p] /= f
        n_fixed += 1
    if n_fixed:
        log("split repair: %d names rebased (calendar banked %s)" % (n_fixed, doc.get("banked_at")))
    return n_fixed


# ---- crypto lane: the shared Polygon daily bank in OUR warehouse (same format katlin writes) ------------------------
def poly_get(url, timeout=40):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-bottom/%s" % VERSION})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def poly_aggs(ticker, mult, span, frm, to):
    out = []
    url = "https://api.polygon.io/v2/aggs/ticker/%s/range/%d/%s/%s/%s?%s" % (
        urllib.parse.quote(ticker), mult, span, frm, to, urllib.parse.urlencode({"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": POLY_KEY}))
    for _ in range(8):
        j = poly_get(url)
        out.extend(j.get("results") or [])
        nxt = j.get("next_url")
        if not nxt:
            break
        url = nxt + ("&" if "?" in nxt else "?") + "apiKey=" + POLY_KEY
    return out


def bank_crypto_symbol(sym, today):
    key = CRYPTO_ROOT + sym + ".json.gz"
    doc = s3_json(key, None, quiet=True) or {"symbol": sym, "source": "polygon X:%sUSD daily" % sym, "rows": []}
    rows = doc.get("rows") or []
    last = rows[-1][0] if rows else None
    if last == today or not POLY_KEY:
        return doc, "cached" if last == today else "no-key"
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
        row = [d, float(r.get("o") or r["c"]), float(r.get("h") or r["c"]), float(r.get("l") or r["c"]), float(r["c"]), float(r.get("v") or 0.0)]
        if d in have:
            rows[have[d]] = row
        else:
            rows.append(row); have[d] = len(rows) - 1; added += 1
    rows.sort(key=lambda x: x[0])
    doc["rows"] = rows
    doc["banked_at"] = now_iso()
    doc["n"] = len(rows)
    if added or not last:
        s3_put_json(key, doc, gz=True)
    return doc, "ok:+%d" % added


def load_crypto(today, symbols):
    docs, status = {}, {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        for sym, (doc, st) in zip(symbols, ex.map(lambda s: bank_crypto_symbol(s, today), symbols)):
            docs[sym] = doc; status[sym] = st
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
            b.d.append(idx[d]); b.o.append(o); b.h.append(h); b.l.append(l); b.c.append(c); b.v.append(v * c)   # dollar volume
        bars[sym] = b
    nerr = sum(1 for s in status.values() if s.startswith("err"))
    log("crypto lane: %d symbols, %d with >=200 days, %d errors" % (len(docs), len(bars), nerr))
    if nerr and nerr >= max(1, len(symbols) // 2):
        DEGRADED.append("crypto lane: %d/%d Polygon errors" % (nerr, len(symbols)))
    return dates, bars


# ---- resampling (daily -> weekly, ISO weeks) -------------------------------------------------------------------------
def resample_weekly(b, dates):
    """returns dict of lists o,h,l,c,v,d (d = index into `dates` of the week's last session) or None."""
    o, h, l, c, v, d = [], [], [], [], [], []
    cur = None
    for p in range(len(b.d)):
        ds = dates[b.d[p]]
        wk = datetime.strptime(ds, "%Y-%m-%d").isocalendar()[:2]
        if wk != cur:
            cur = wk
            o.append(b.o[p]); h.append(b.h[p]); l.append(b.l[p]); c.append(b.c[p]); v.append(b.v[p]); d.append(b.d[p])
        else:
            h[-1] = max(h[-1], b.h[p]); l[-1] = min(l[-1], b.l[p]); c[-1] = b.c[p]; v[-1] += b.v[p]; d[-1] = b.d[p]
    return {"o": o, "h": h, "l": l, "c": c, "v": v, "d": d}


# ---- indicators ----------------------------------------------------------------------------------------------------
def wilder_atr(h, l, c, n):
    out = [None] * len(c)
    if len(c) < n + 1:
        return out
    trs = []
    for i in range(1, len(c)):
        trs.append(max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])))
    a = sum(trs[:n]) / n
    out[n] = a
    for i in range(n + 1, len(c)):
        a = (a * (n - 1) + trs[i - 1]) / n
        out[i] = a
    return out


def sma_prev(vals, n):
    """SMA of the PRIOR n bars (excludes the current bar) -- the climax bar must not inflate its own average."""
    out = [None] * len(vals)
    s = 0.0
    for i in range(len(vals)):
        if i >= n:
            out[i] = s / n
            s -= vals[i - n]
        s += vals[i]
    return out


def rolling_max_prev(vals, n, min_bars=20):
    """max of the PRIOR n bars (window i-n .. i-1, the current bar excluded). Monotonic deque, O(n).
    None until at least `min_bars` prior bars exist."""
    out = [None] * len(vals)
    dq = deque()   # indices with decreasing values
    for i in range(len(vals)):
        while dq and dq[0] < i - n:
            dq.popleft()
        if dq and i >= min(n, min_bars):
            out[i] = vals[dq[0]]
        while dq and vals[dq[-1]] <= vals[i]:
            dq.pop()
        dq.append(i)
    return out


def rolling_min_prev(vals, n, min_bars=8):
    out = [None] * len(vals)
    dq = deque()
    for i in range(len(vals)):
        while dq and dq[0] < i - n:
            dq.popleft()
        if dq and i >= min(n, min_bars):
            out[i] = vals[dq[0]]
        while dq and vals[dq[-1]] >= vals[i]:
            dq.pop()
        dq.append(i)
    return out


def sma_series(vals, n):
    out = [None] * len(vals)
    s = 0.0
    for i, x in enumerate(vals):
        s += x
        if i >= n:
            s -= vals[i - n]
        if i >= n - 1:
            out[i] = s / n
    return out


# ---- THE ALGORITHM: one causal pass per instrument per frame ---------------------------------------------------------
def _mk_event(i, frame):
    return {"frame": frame, "state": "CLIMAX", "sc_i": i, "sc_end": i, "sc_low": None, "sc_vol": None, "sc_vol_x": None, "sc_range_x": None,
            "sc_clv": None, "sc_decline_pct": None, "sc_close_off_low": None, "pre_high": None, "pre_high_i": None,
            "ar_i": None, "ar_high": None, "ar_valid_i": None, "ar_buy_px": None, "ar_rally_atr": None, "ar_rally_pct": None,
            "tests": [], "st_i": None, "st_low": None, "st_bar_high": None, "st_vol_ratio_sc": None, "st_vol_ratio_avg": None,
            "st_depth_atr": None, "st_depth_class": None, "st_retrace_frac": None, "approach_vol_slope": None, "approach_range_x": None,
            "spring": False, "undercut_i": None, "trig_i": None, "trig_level": None, "trig_fill": None, "trig_vol_x": None, "stop": None,
            "target_1": None, "target_2": None, "sos_i": None, "stop_i": None, "end_i": None, "outcome": None,
            "mfe_pct": None, "mae_pct": None, "ret_21": None, "ret_63": None, "ar_ret_21": None, "ar_ret_63": None, "ar_hole": None,
            "why_closed": None, "atr": None}


def _close_event(E, i, outcome, why):
    E["state"] = outcome
    E["outcome"] = outcome
    E["end_i"] = i
    E["why_closed"] = why


def detect(o, h, l, c, v, prm, frame, decline_min_pct=None, diag=None):
    """Causal Wyckoff bottom state machine. Returns (events, active) where events are closed sequences and active is the
    open one (or None). Every index is a position in the arrays given.

    The climax gates (v1.2.0, per Wyckoff/SMI: 'the highest intensity of speculative supply within a downtrend ... only after
    a move has been in effect for some time ... if it does not have this it is not a selling climax'):
      1. a real decline: close >= decline_min_pct and >= decline_min_atr under the 120-bar high (weekly: 52)
      2. a prolonged one: that high printed >= downtrend_min_bars earlier, or the decline is a crash (>= crash_pct)
      3. at the LOW of the move: the bar makes a new 120-bar low (weekly: 26) and sits below the 50-bar average (weekly: 26)
         with the close in the bottom pos_max_pct of the 252-bar range (weekly: 104)
      4. climactic volume: >= climax_vol_x times the prior 50-bar average, the HIGHEST volume of the prior 60 bars, and
         >= climax_vol_x times the prior 250-bar average when that history exists (an ETF whose volume regime is simply
         growing does not qualify)
      5. climactic spread: range >= climax_range_x ATR."""
    n = len(c)
    dmin = decline_min_pct if decline_min_pct is not None else prm["decline_min_pct"]
    atr = wilder_atr(h, l, c, prm["atr_n"])
    vavg = sma_prev(v, prm["vol_n"])
    vreg = sma_prev(v, prm["vol_regime_n"])
    vmax = rolling_max_prev(v, prm["vol_dominance_n"], min_bars=prm["vol_dominance_n"] // 2)
    hi_prev = rolling_max_prev(h, prm["decline_lookback"], min_bars=min(prm["decline_lookback"], 40))
    hi_range = rolling_max_prev(h, prm["range_n"], min_bars=min(prm["range_n"], 60))
    lo_range = rolling_min_prev(l, prm["range_n"], min_bars=min(prm["range_n"], 60))
    lo_prev = rolling_min_prev(l, prm["newlow_lookback"], min_bars=min(prm["newlow_lookback"], 40))
    ma = sma_prev(c, prm["ma_n"])
    rng = [h[i] - l[i] for i in range(n)]
    events = []
    E = None
    warm = max(prm["atr_n"] + 1, prm["vol_n"] + 1, prm["ma_n"] + 1, 40)
    D = diag if diag is not None else {}

    def bump(k):
        D[k] = D.get(k, 0) + 1

    def is_climax(i):
        a = atr[i]; va = vavg[i]; hp = hi_prev[i]; lp = lo_prev[i]
        if a is None or va is None or hp is None or lp is None or a <= 0 or va <= 0:
            return None
        vol_x = v[i] / va
        if vol_x < prm["climax_vol_x"]:
            return None
        bump("vol_x_ok")
        range_x = rng[i] / a
        if range_x < prm["climax_range_x"]:
            bump("rej_range"); return None
        if l[i] > lp:                                  # the low of the whole move, not a 3-week dip
            bump("rej_not_new_low"); return None
        decline_pct = 100.0 * (c[i] / hp - 1.0)
        decline_atr = (hp - c[i]) / a
        if decline_atr < prm["decline_min_atr"] or -decline_pct < dmin:
            bump("rej_shallow"); return None
        j0 = max(0, i - prm["decline_lookback"])
        pre_i = max(range(j0, i), key=lambda k: h[k]) if i > j0 else i
        if (i - pre_i) < prm["downtrend_min_bars"] and -decline_pct < prm["crash_pct"]:
            bump("rej_not_prolonged"); return None      # the move has not been in effect for some time
        m = ma[i]
        if m is not None and c[i] >= m:
            bump("rej_above_ma"); return None            # a climax prints under the average, not on it
        hr, lr = hi_range[i], lo_range[i]
        if hr is not None and lr is not None and hr > lr:
            pos = 100.0 * (c[i] - lr) / (hr - lr)
            if pos > prm["pos_max_pct"]:
                bump("rej_position"); return None        # not at the lows of its own yearly range
        else:
            pos = None
        vm = vmax[i]
        if vm is not None and v[i] < vm:
            bump("rej_not_dominant"); return None        # not the heaviest volume of the decline
        vr = vreg[i]
        if vr is not None and vr > 0 and v[i] < prm["climax_vol_x"] * vr:
            bump("rej_volume_regime"); return None      # heavy only vs a recently rising volume regime
        bump("climax")
        clv = (c[i] - l[i]) / rng[i] if rng[i] > 0 else 0.5
        pre_vol = mean([v[k] for k in range(max(0, i - 5), i)]) if i >= 5 else None
        return {"vol_x": vol_x, "range_x": range_x, "clv": clv, "decline_pct": decline_pct, "pre_high": hp, "pre_high_i": pre_i,
                "pos_range_pct": pos, "vol_vs_regime": (v[i] / vr) if (vr and vr > 0) else None, "prelim_vol_x": (pre_vol / va) if (pre_vol and va) else None,
                "bars_down": i - pre_i}

    for i in range(warm, n):
        a = atr[i]
        if a is None or a <= 0:
            continue
        # ---------------- no open sequence: look for a selling climax
        if E is None:
            cx = is_climax(i)
            if cx is None:
                continue
            E = _mk_event(i, frame)
            _fill_climax(E, i, l, v, a, cx)
            continue
        st = E["state"]
        sc_low = E["sc_low"]
        # ---------------- CLIMAX: waiting for the automatic rally (climax clusters allowed)
        if st == "CLIMAX":
            bars_since = i - E["sc_end"]
            # a lower low on climactic volume within the cluster window extends the climax
            if bars_since <= prm["cluster_bars"] and l[i] < sc_low and vavg[i] and v[i] >= prm["climax_vol_x"] * 0.75 * vavg[i]:
                E["sc_end"] = i; E["sc_low"] = l[i]; E["sc_vol"] = max(E["sc_vol"], v[i])
                E["sc_vol_x"] = max(E["sc_vol_x"], v[i] / vavg[i]); E["sc_range_x"] = max(E["sc_range_x"], rng[i] / a)
                E["sc_clv"] = (c[i] - l[i]) / rng[i] if rng[i] > 0 else 0.5; E["sc_close_off_low"] = E["sc_clv"] >= 0.3; E["atr"] = a
                continue
            # a decisive close under the climax low without a rally: the climax was not THE climax
            if c[i] < sc_low - 0.5 * E["atr"]:
                cx = is_climax(i)
                _close_event(E, i, "NO_RALLY", "closed under the climax low before any rally")
                events.append(E); E = None
                if cx is not None:   # this bar is itself a (deeper) climax -> a new sequence starts here
                    E = _mk_event(i, frame)
                    _fill_climax(E, i, l, v, a, cx)
                continue
            # rally tracking (and the post-climax volume signature: it must diminish)
            if E.get("post_vols") is None:
                E["post_vols"] = []
            if len(E["post_vols"]) < 5:
                E["post_vols"].append(v[i])
            if E["ar_high"] is None or h[i] > E["ar_high"]:
                E["ar_high"] = h[i]; E["ar_i"] = i
            rally_atr = (E["ar_high"] - sc_low) / E["atr"]
            if E["ar_valid_i"] is None and rally_atr >= prm["ar_min_atr"]:
                E["ar_valid_i"] = i; E["ar_buy_px"] = c[i]      # the crowd buys the bounce here
            if E["ar_valid_i"] is not None:
                # AR peak confirmed once `ar_confirm_bars` bars fail to exceed it
                if i - E["ar_i"] >= prm["ar_confirm_bars"]:
                    E["ar_rally_atr"] = rally_atr; E["ar_rally_pct"] = 100.0 * (E["ar_high"] / sc_low - 1.0)
                    pv = E.get("post_vols") or []
                    E["post_vol_ratio"] = (mean(pv) / E["sc_vol"]) if (pv and E["sc_vol"]) else None
                    E["state"] = "TESTING"; E["test_start_i"] = i
                    E["appr"] = {"lows": [], "vols": [], "rngs": []}
                    continue
            if bars_since > prm["ar_window"] and E["ar_valid_i"] is None:
                _close_event(E, i, "NO_RALLY", "no automatic rally within %d bars" % prm["ar_window"])
                events.append(E); E = None
            continue
        # ---------------- TESTING: price drifts back toward the climax low -- watch the volume
        if st == "TESTING":
            ap = E["appr"]
            ap["lows"].append(l[i]); ap["vols"].append(v[i]); ap["rngs"].append(rng[i])
            va = vavg[i] or E["sc_vol"] / max(E["sc_vol_x"], 1.0)
            in_zone = l[i] <= sc_low + prm["test_zone_atr"] * E["atr"]
            # failure: a close through the low on returning supply
            if c[i] < sc_low - prm["undercut_atr"] * E["atr"]:
                supply = v[i] >= prm["fail_vol_x"] * E["sc_vol"] or v[i] >= prm["fail_vol_avg_x"] * va
                if supply or c[i] < sc_low - prm["spring_atr"] * E["atr"]:
                    E["tests"].append({"i": i, "low": l[i], "vol_ratio_sc": v[i] / E["sc_vol"], "kind": "FAILED"})
                    _close_event(E, i, "FAILED", "test broke the climax low on %s volume" % ("rising" if supply else "a deep undercut"))
                    events.append(E); E = None
                    continue
                if E["undercut_i"] is None:
                    E["undercut_i"] = i
            if E["undercut_i"] is not None and i - E["undercut_i"] > prm["spring_recover_bars"] and c[i] < sc_low:
                E["tests"].append({"i": i, "low": min(ap["lows"]), "vol_ratio_sc": v[i] / E["sc_vol"], "kind": "FAILED"})
                _close_event(E, i, "FAILED", "undercut not recovered within %d bars" % prm["spring_recover_bars"])
                events.append(E); E = None
                continue
            # a breakout above the range before any test = V-bottom (recorded, not a Wyckoff entry)
            if c[i] > E["ar_high"] and not in_zone and (not E.get("touched_zone")):
                _close_event(E, i, "NO_TEST_BREAKOUT", "price left the range upward before testing the low (V-bottom)")
                events.append(E); E = None
                continue
            if in_zone:
                E["touched_zone"] = True
                if E.get("cand_i") is None or l[i] < E["cand_low"]:
                    E["cand_i"] = i; E["cand_low"] = l[i]
                    continue
            if E.get("cand_i") is not None and l[i] > E["cand_low"] and c[i] > E["cand_low"]:
                # the test low printed and price turned -> measure the test (the paper's core)
                ti = E["cand_i"]
                sc_vol = E["sc_vol"]
                vr_sc = v[ti] / sc_vol if sc_vol else None
                vr_avg = v[ti] / va if va else None
                depth_atr = (E["cand_low"] - sc_low) / E["atr"]
                if depth_atr > prm["undercut_atr"]:
                    depth_class = "HIGHER_LOW"
                elif depth_atr >= -prm["undercut_atr"]:
                    depth_class = "EQUAL_LOW"
                else:
                    depth_class = "SPRING" if c[i] > sc_low else "UNDERCUT"
                k = max(1, len(ap["vols"]))
                vs = linreg_slope(ap["vols"][-min(k, 12):]) if k >= 3 else None
                vol_slope = (vs / va) if (vs is not None and va) else None
                appr_rng = mean(ap["rngs"][-min(k, 8):]) / E["atr"] if ap["rngs"] else None
                retrace = (E["ar_high"] - E["cand_low"]) / (E["ar_high"] - sc_low) if E["ar_high"] > sc_low else None
                prev = [t for t in E["tests"] if t["kind"] == "TEST"]
                lower_than_prev = (not prev) or (vr_sc is not None and prev[-1]["vol_ratio_sc"] is not None and vr_sc < prev[-1]["vol_ratio_sc"])
                E["tests"].append({"i": ti, "low": E["cand_low"], "bar_high": h[ti], "vol_ratio_sc": vr_sc, "vol_ratio_avg": vr_avg,
                                   "depth_atr": depth_atr, "depth_class": depth_class, "lower_vol_than_prev": lower_than_prev, "kind": "TEST"})
                E["st_i"] = ti; E["st_low"] = E["cand_low"]; E["st_bar_high"] = h[ti]; E["st_vol_ratio_sc"] = vr_sc; E["st_vol_ratio_avg"] = vr_avg
                E["st_depth_atr"] = depth_atr; E["st_depth_class"] = depth_class; E["st_retrace_frac"] = retrace
                E["approach_vol_slope"] = vol_slope; E["approach_range_x"] = appr_rng; E["spring"] = depth_class == "SPRING"
                E["state"] = "ST_CONFIRMED"; E["st_confirm_i"] = i; E["cand_i"] = None
                # the paper's trigger can fire on the very bar that confirms the turn
                if c[i] > E["st_bar_high"]:
                    _trigger(E, i, c, v, va, prm, sma_ref=None)
                continue
            if i - E["ar_i"] > prm["max_test_bars"]:
                _close_event(E, i, "EXPIRED", "no test of the low within %d bars of the rally high" % prm["max_test_bars"])
                events.append(E); E = None
            continue
        # ---------------- ST_CONFIRMED: the test printed on the tape -- wait for the reaction AWAY from it
        if st == "ST_CONFIRMED":
            va = vavg[i] or E["sc_vol"] / max(E["sc_vol_x"], 1.0)
            if c[i] > E["st_bar_high"]:
                _trigger(E, i, c, v, va, prm, sma_ref=None)
                continue
            if l[i] < E["st_low"] - prm["undercut_atr"] * E["atr"]:
                supply = v[i] >= prm["fail_vol_x"] * E["sc_vol"] or v[i] >= prm["fail_vol_avg_x"] * va
                if supply or c[i] < sc_low - prm["spring_atr"] * E["atr"]:
                    E["tests"].append({"i": i, "low": l[i], "vol_ratio_sc": v[i] / E["sc_vol"], "kind": "FAILED"})
                    _close_event(E, i, "FAILED", "test low broken on %s volume" % ("rising" if supply else "a deep undercut"))
                    events.append(E); E = None
                    continue
                # a quieter, lower test: back to TESTING with the new candidate (the paper's 3rd/4th test on lower volume)
                E["state"] = "TESTING"; E["appr"] = {"lows": [l[i]], "vols": [v[i]], "rngs": [rng[i]]}
                E["cand_i"] = i; E["cand_low"] = l[i]; E["touched_zone"] = True
                continue
            if i - E["st_i"] > prm["max_wait_bars"]:
                _close_event(E, i, "EXPIRED", "no trigger within %d bars of the test" % prm["max_wait_bars"])
                events.append(E); E = None
            continue
        # ---------------- TRIGGERED / MARKUP: manage the position by the paper's rules and record the outcome
        if st in ("TRIGGERED", "MARKUP"):
            k = i - E["trig_i"]
            fill = E["trig_fill"]
            E["mfe_pct"] = max(E["mfe_pct"] if E["mfe_pct"] is not None else -1e9, 100.0 * (h[i] / fill - 1.0))
            E["mae_pct"] = min(E["mae_pct"] if E["mae_pct"] is not None else 1e9, 100.0 * (l[i] / fill - 1.0))
            if c[i] < E["stop"]:
                E["stop_i"] = i
                E["exit_ret"] = 100.0 * (c[i] / fill - 1.0)
                _close_event(E, i, "STOPPED", "closed below the protective stop under the test low")
                events.append(E); E = None
                continue
            if st == "TRIGGERED" and c[i] > E["ar_high"]:
                E["state"] = "MARKUP"; E["sos_i"] = i
            if k >= prm["outcome_bars"]:
                _close_event(E, i, "COMPLETED", "outcome window closed")
                events.append(E); E = None
            continue
    for ev in events:
        _outcomes(ev, c, l, h, prm)
    if E is not None:
        _outcomes(E, c, l, h, prm)
    return events, E


def _outcomes(ev, c, l, h, prm):
    """post-pass measured from the arrays (never from the state the sequence ended in):
    * trigger buyer: raw +21/+63 returns, stop-managed returns (exit at the stop when it hit first), target-1 hit
    * AR buyer (the crowd's entry on the first bounce): raw +21/+63 returns, worst drawdown, and whether the tape
      later undercut the climax low (the paper's 'hole twice as deep')."""
    n = len(c)
    ti = ev.get("trig_i")
    if ti is not None and ev.get("trig_fill"):
        fill = ev["trig_fill"]
        end = min(n - 1, ti + prm["outcome_bars"])
        for w in (21, 63):
            j = ti + w
            ev["ret_%d" % w] = 100.0 * (c[j] / fill - 1.0) if j < n else None
        # the paper's stop and two alternatives, each managed independently from the arrays (exit = first close below the level)
        levels = {"paper": ev["stop"], "climax": min(ev["sc_low"], ev["st_low"]) - 0.25 * ev["atr"], "wide": ev["st_low"] - 1.0 * ev["atr"]}
        ev["stops"] = {}
        for rule, lvl in levels.items():
            hit_i = next((k for k in range(ti + 1, end + 1) if c[k] < lvl), None)
            rec = {"level": lvl, "hit": hit_i is not None, "hit_bar": (hit_i - ti) if hit_i is not None else None}
            for w in (21, 63):
                j = ti + w
                if hit_i is not None and hit_i - ti <= w:
                    rec["mret_%d" % w] = 100.0 * (c[hit_i] / fill - 1.0)
                else:
                    rec["mret_%d" % w] = 100.0 * (c[j] / fill - 1.0) if j < n else None
            ev["stops"][rule] = rec
        ev["mret_21"] = ev["stops"]["paper"]["mret_21"]
        ev["mret_63"] = ev["stops"]["paper"]["mret_63"]
        t1 = ev.get("target_1")
        ev["t1_hit"] = bool(t1 and any(h[k] >= t1 for k in range(ti + 1, end + 1))) if end > ti else None
        ev["t1_before_stop"] = None
        if ev["t1_hit"]:
            t1_i = next(k for k in range(ti + 1, end + 1) if h[k] >= t1)
            ph = ev["stops"]["paper"]["hit_bar"]
            ev["t1_before_stop"] = ph is None or (t1_i - ti) < ph
        ev["stop_hit"] = ev["stops"]["paper"]["hit"]
    ai = ev.get("ar_valid_i")
    if ai is not None and ev.get("ar_buy_px"):
        px = ev["ar_buy_px"]
        for w in (21, 63):
            j = ai + w
            ev["ar_ret_%d" % w] = 100.0 * (c[j] / px - 1.0) if j < n else None
        end = min(n - 1, ai + 63)
        lows = [l[k] for k in range(ai + 1, end + 1)]
        ev["ar_mae_pct"] = 100.0 * (min(lows) / px - 1.0) if lows else None
        lo = ev["sc_low"] - prm["undercut_atr"] * ev["atr"]
        ev["ar_hole"] = bool(lows and min(lows) < lo)


def _fill_climax(E, i, l, v, a, cx):
    E["sc_low"] = l[i]; E["sc_vol"] = v[i]; E["sc_vol_x"] = cx["vol_x"]; E["sc_range_x"] = cx["range_x"]; E["sc_clv"] = cx["clv"]
    E["sc_decline_pct"] = cx["decline_pct"]; E["pre_high"] = cx["pre_high"]; E["pre_high_i"] = cx["pre_high_i"]; E["atr"] = a
    E["sc_close_off_low"] = cx["clv"] >= 0.3
    E["sc_pos_range_pct"] = cx.get("pos_range_pct"); E["sc_vol_vs_regime"] = cx.get("vol_vs_regime"); E["sc_prelim_vol_x"] = cx.get("prelim_vol_x")
    E["sc_bars_down"] = cx.get("bars_down")


def _trigger(E, i, c, v, va, prm, sma_ref=None):
    E["state"] = "TRIGGERED"; E["trig_i"] = i; E["trig_level"] = E["st_bar_high"]; E["trig_fill"] = c[i]
    E["trig_vol_x"] = (v[i] / va) if va else None
    E["stop"] = E["st_low"] - prm["stop_buffer_atr"] * E["atr"]
    E["target_1"] = E["ar_high"]
    E["target_2"] = E["pre_high"]



# ---- scoring: the evidence ladder of the paper (climax -> rally -> test -> trigger) ---------------------------------
def score_event(E, prm):
    s = {"climax": 0.0, "rally": 0.0, "test": 0.0, "trigger": 0.0}
    reasons, risks = [], []
    # climax quality (0-25)
    vx = E.get("sc_vol_x") or 0.0
    s["climax"] += clamp(10.0 * (vx - 1.0), 0, 16)              # 2x=10, 2.6x=16
    s["climax"] += clamp(3.0 * ((E.get("sc_range_x") or 0) - 1.0), 0, 4)
    if E.get("sc_close_off_low"):
        s["climax"] += 3
        reasons.append("climax bar closed %.0f%% off its low (absorption)" % (100 * (E.get("sc_clv") or 0)))
    dp = E.get("sc_decline_pct") or 0.0
    s["climax"] += clamp(-dp / 10.0, 0, 2)
    reasons.insert(0, "selling climax: %.1fx average volume (heaviest of the decline), %.1fx ATR range, %.0f%% under the swing high after %s bars down" % (vx, E.get("sc_range_x") or 0, -dp, E.get("sc_bars_down") if E.get("sc_bars_down") is not None else "?"))
    pv = E.get("post_vol_ratio")
    if pv is not None:
        if pv <= 0.6:
            reasons.append("volume diminished to %.0f%% of the climax over the next bars" % (100 * pv))
        elif pv >= 1.0:
            risks.append("volume did NOT diminish after the climax (%.0f%%) -- supply still active" % (100 * pv))
            s["climax"] = max(0.0, s["climax"] - 6)
    if (E.get("sc_prelim_vol_x") or 0) >= 1.3:
        reasons.append("preliminary supply: volume was already %.1fx average into the climax" % E["sc_prelim_vol_x"])
    # rally quality (0-15)
    if E.get("ar_valid_i") is not None:
        ra = E.get("ar_rally_atr") or 0.0
        s["rally"] = clamp(5.0 * ra, 0, 15)
        reasons.append("automatic rally %.1f%% (%.1f ATR) set the range top" % (E.get("ar_rally_pct") or 0, ra))
    # test quality (0-40): the paper's heart -- volume at the test vs the climax
    if E.get("st_i") is not None:
        vr = E.get("st_vol_ratio_sc")
        if vr is not None:
            pts = clamp(22.0 * (1.0 - vr) / 0.8, 0, 22)     # 0.2 -> 22, 0.6 -> 11, 1.0 -> 0
            s["test"] += pts
            if vr <= 0.5:
                reasons.append("secondary test on %.0f%% of climax volume -- supply exhausted" % (100 * vr))
            elif vr <= 0.8:
                reasons.append("secondary test on %.0f%% of climax volume (diminished)" % (100 * vr))
            else:
                risks.append("test volume %.0f%% of the climax -- not a quiet test" % (100 * vr))
        vra = E.get("st_vol_ratio_avg")
        if vra is not None and vra <= 1.0:
            s["test"] += 4
        vs = E.get("approach_vol_slope")
        if vs is not None and vs < 0:
            s["test"] += 4
            reasons.append("volume shrank on the way back down")
        ar_x = E.get("approach_range_x")
        if ar_x is not None and ar_x <= 0.9:
            s["test"] += 4
            reasons.append("bars narrowed into the test (%.2f ATR)" % ar_x)
        dc = E.get("st_depth_class")
        if dc == "HIGHER_LOW":
            s["test"] += 6
            reasons.append("higher low: buyers stepped in before the old low -- exceptional strength")
        elif dc == "EQUAL_LOW":
            s["test"] += 4
        elif dc == "SPRING":
            s["test"] += 3
            reasons.append("spring: undercut on light volume, recovered")
        tests = [t for t in E.get("tests") or [] if t.get("kind") == "TEST"]
        if len(tests) >= 2:
            if all(t.get("lower_vol_than_prev") for t in tests[1:]):
                s["test"] += min(4, 2 * (len(tests) - 1))
                reasons.append("%d tests, each on lower volume than the last" % len(tests))
            else:
                risks.append("%d tests but volume did not keep shrinking" % len(tests))
    # trigger / confirmation (0-20)
    st = E.get("state")
    if st == "MARKUP":
        s["trigger"] = 20
        reasons.append("sign of strength: closed above the rally high")
    elif st in ("TRIGGERED", "COMPLETED"):
        s["trigger"] = 12 + (4 if (E.get("trig_vol_x") or 0) >= 1.0 else 0)
        reasons.append("triggered: closed above the high of the test candle%s" % (" on above-average volume" if (E.get("trig_vol_x") or 0) >= 1.0 else ""))
    elif st == "ST_CONFIRMED":
        s["trigger"] = 5
    total = clamp(sum(s.values()))
    if st in ("FAILED", "STOPPED"):
        total = min(total, 25.0)
        risks.insert(0, "sequence failed: %s" % E.get("why_closed"))
    grade = "A" if total >= 75 else ("B" if total >= 60 else ("C" if total >= 45 else "D"))
    return round(total, 1), grade, {k: round(x, 1) for k, x in s.items()}, reasons, risks


# ---- universe & taxonomy --------------------------------------------------------------------------------------------
def to_poly(t):
    return str(t or "").upper().replace("-", ".")


def to_fv(t):
    return str(t or "").upper().replace(".", "-")


def classify_etf(fv):
    name = str(fv.get("company") or "")
    et = str(fv.get("etf_type") or "").lower()
    if LEV_RX.search(name) or "leveraged" in et or "inverse" in et or OVERLAY_RX.search(name) or MONEY_RX.search(name):
        return None
    if CRYPTO_ETF_RX.search(name):
        return "crypto_etf"
    if GOLD_MINER_RX.search(name):
        return "gold_miners"
    if SILVER_MINER_RX.search(name):
        return "silver_miners"
    if GOLD_RX.search(name):
        return "gold"
    if SILVER_RX.search(name):
        return "silver"
    if PGM_RX.search(name):
        return "pgm"
    if COPPER_RX.search(name):
        return "copper"
    if IND_METAL_RX.search(name):
        return "industrial_metals"
    if BOND_INTL_RX.search(name):
        return "bond_intl"
    if BOND_GOVT_RX.search(name) and (BOND_CREDIT_RX.search(name) or "bond" in et or "fixed" in et or "treasur" in name.lower() or "tips" in name.lower() or "muni" in name.lower()):
        return "bond_govt"
    if BOND_CREDIT_RX.search(name) or "bond" in et or "fixed" in et:
        return "bond_credit"
    if ENERGY_CMD_RX.search(name) and not SECTOR_RX.search(name.replace("energy fund", "")):
        return "energy"
    if AGRI_RX.search(name) and "commod" in et:
        return "agriculture"
    if AGRI_RX.search(name) and not SECTOR_RX.search(name):
        return "agriculture"
    if BROAD_CMD_RX.search(name) or "commod" in et:
        return "commodity_broad"
    if CURRENCY_RX.search(name) or "currenc" in et:
        return "currency"
    if REIT_RX.search(name):
        return "real_estate"
    if COUNTRY_RX.search(name) or "country" in et or "region" in et or "emerging" in et or "international" in et:
        return "country"
    if SECTOR_RX.search(name) or "sector" in et or "industry" in et:
        return "sector"
    if "equit" in et or "size" in et or "growth" in et or "value" in et or "dividend" in et or not et:
        return "equity_etf"
    return None


def is_etf_row(fv):
    at = str(fv.get("asset_type") or "").lower()
    return "etf" in at or bool(fv.get("etf_type")) or bool(fv.get("aum"))


def build_universe(finviz):
    """returns {sym: (asset_class, sub_class)} for stocks + ETF wrappers, and the keep-set for the bar loader."""
    uni = {}
    for tk, fv in finviz.items():
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
            uni[sym] = ("etf", cls)
        else:
            mcap = fnum(fv.get("market_cap"))
            if mcap is not None and mcap < 1e8:
                mcap *= 1e6
            if mcap is not None and mcap < P["min_mcap"]:
                continue
            ind = str(fv.get("industry") or "")
            if ind.startswith("Shell Companies") or ind.startswith("Exchange Traded") or ind.startswith("Closed-End"):
                continue
            uni[sym] = ("stock", "stock")
    # the named asset classes are always on the board (the wrappers are the tradable, volume-bearing instruments)
    for cls, syms in CORE_WRAPPERS.items():
        for s in syms:
            if s not in uni or uni[s][0] == "etf":
                uni[s] = ("etf", cls)
    keep = set(uni) | set(BENCH)
    return uni, keep


# ---- fleet joins (evidence on the row; never gates) ------------------------------------------------------------------
def load_feeds():
    F = {"asof": {}}
    fv = s3_json("data/finviz-universe.json", {}) or {}
    F["finviz"] = fv.get("by_ticker") or {}
    F["asof"]["finviz"] = fv.get("generated_at")
    ar = s3_json("data/accumulation-radar.json", {}) or {}
    acc = {}
    # producer shape (justhodl-accumulation-radar): bottoms/accumulating = {stocks|etfs|countries: [rows]}, confirmed_bottoms = [rows]
    for grp in ("bottoms", "accumulating"):
        for cl in ("stocks", "etfs", "countries"):
            for r in ((ar.get(grp) or {}).get(cl) or []):
                if isinstance(r, dict) and r.get("ticker"):
                    acc.setdefault(to_poly(r["ticker"]), {"bottom_score": fnum(r.get("bottom_score")), "phase": r.get("phase"), "signal": r.get("flag") or r.get("signal"), "confirm_n": r.get("confirm_n")})
    for r in (ar.get("confirmed_bottoms") or []):
        if isinstance(r, dict) and r.get("ticker"):
            e = acc.setdefault(to_poly(r["ticker"]), {"bottom_score": fnum(r.get("bottom_score")), "phase": r.get("phase"), "signal": r.get("flag")})
            e["confirmed"] = True
            e["confirm_n"] = r.get("confirm_n")
    F["accum"] = acc
    F["asof"]["accumulation_radar"] = ar.get("generated_at") or ar.get("as_of")
    pd = s3_json("data/phase-detector.json", {}) or {}
    ph = {}
    # producer shape (justhodl-phase-detector): phases_all = {ticker: {p: phase, b: begin}}, tickers = {ticker: full row} for the kept names
    for t, v in (pd.get("phases_all") or {}).items():
        if isinstance(v, dict):
            ph[to_poly(t)] = {"phase": v.get("p"), "begin": v.get("b"), "end": None, "pressure": None}
    for t, v in (pd.get("tickers") or {}).items():
        if isinstance(v, dict):
            e = ph.setdefault(to_poly(t), {"phase": v.get("phase"), "begin": v.get("begin"), "end": None, "pressure": None})
            e["phase"] = v.get("phase") or e.get("phase"); e["begin"] = v.get("begin") or e.get("begin"); e["end"] = v.get("end"); e["pressure"] = fnum(v.get("pressure"))
    F["phase"] = ph
    F["asof"]["phase_detector"] = pd.get("generated_at") or pd.get("as_of")
    fo = s3_json("data/fortress.json", {}) or {}
    F["fortress"] = {r.get("ticker"): {"tier": r.get("tier"), "composite": fnum(r.get("composite")), "dump_capture": fnum(r.get("dump_capture"))}
                     for key in ("board", "etfs") for r in (fo.get(key) or []) if isinstance(r, dict) and r.get("ticker")}
    F["asof"]["fortress"] = fo.get("as_of") or fo.get("generated_at")
    ka = s3_json("data/katlin.json", {}) or {}
    F["katlin"] = {r.get("ticker"): {"tier": r.get("tier"), "composite": fnum(r.get("composite")), "structure_state": r.get("structure_state")}
                   for r in (ka.get("picks") or []) if isinstance(r, dict) and r.get("ticker")}
    F["katlin_posture"] = (ka.get("war_room") or {}).get("posture")
    F["asof"]["katlin"] = ka.get("generated_at")
    f13 = s3_json("data/13f-flows-by-ticker.json", {}) or {}
    F["f13"] = f13.get("t") or {}
    F["asof"]["f13"] = f13.get("as_of")
    dp = s3_json("data/dark-pool.json", {}) or {}
    F["dark"] = dp.get("xray_map") if isinstance(dp.get("xray_map"), dict) else {}
    F["asof"]["dark_pool"] = dp.get("generated_at") or dp.get("as_of")
    ins = s3_json("data/insider-radar.json", {}) or {}
    ib = {}
    for r in ins.get("latest_buys") or []:
        if isinstance(r, dict) and r.get("ticker"):
            e = ib.setdefault(to_poly(r["ticker"]), {"n_buys": 0, "usd": 0.0, "cluster": False})
            e["n_buys"] += 1
            e["usd"] += fnum(r.get("value")) or 0.0
    for r in ins.get("clusters") or []:
        if isinstance(r, dict) and r.get("ticker"):
            ib.setdefault(to_poly(r["ticker"]), {"n_buys": 0, "usd": 0.0, "cluster": False})["cluster"] = True
    F["insider"] = ib
    F["asof"]["insider"] = ins.get("generated_at")
    ef = s3_json("etf-flows/daily.json", {}) or {}
    F["flows"] = {m.get("ticker"): m for m in (ef.get("metrics") or []) if isinstance(m, dict) and m.get("ticker") and not m.get("error")}
    F["asof"]["etf_flows"] = ef.get("generated_at") or ef.get("as_of")
    kr = s3_json("data/khalid-risk.json", {}) or {}
    pol = kr.get("policy") if isinstance(kr.get("policy"), dict) else {}
    F["authority"] = {"mode": pol.get("mode") or kr.get("mode"), "allows_new_entries": pol.get("allows_new_entries"), "status": kr.get("status"),
                      "cap_pct": fnum(kr.get("exposure_cap_pct")), "policy_cap_pct": fnum(pol.get("exposure_cap_pct")), "generated_at": kr.get("generated_at")}
    rg = s3_json("data/risk-gate.json", {}) or {}
    F["risk_gate"] = {"posture": rg.get("posture") or rg.get("regime"), "sizing_multiplier": fnum(rg.get("sizing_multiplier")), "generated_at": rg.get("generated_at")}
    log("feeds: finviz=%d accum=%d phase=%d fortress=%d katlin=%d f13=%d dark=%d insider=%d flows=%d" % (
        len(F["finviz"]), len(acc), len(ph), len(F["fortress"]), len(F["katlin"]), len(F["f13"]), len(F["dark"]), len(ib), len(F["flows"])))
    return F


def confirmations(sym, asset_class, F):
    """fleet evidence for the row -- each leg named with its source; absent legs are absent, never zero."""
    legs, score = [], 0
    a = F["accum"].get(sym)
    if a:
        if a.get("confirmed"):
            legs.append({"src": "accumulation-radar", "read": "confirmed bottom (%s fleet confirmations)" % a.get("confirm_n"), "bull": True}); score += 1
        elif (a.get("bottom_score") or 0) >= 60 or str(a.get("signal") or "").upper().startswith("LIKELY_BOTTOM"):
            legs.append({"src": "accumulation-radar", "read": "bottom score %s / phase %s" % (rnd(a.get("bottom_score"), 0), a.get("phase")), "bull": True}); score += 1
        elif a.get("phase") in ("ACCUMULATION", "MARKUP"):
            legs.append({"src": "accumulation-radar", "read": "phase %s" % a.get("phase"), "bull": True}); score += 1
        elif a.get("phase") in ("DISTRIBUTION", "MARKDOWN"):
            legs.append({"src": "accumulation-radar", "read": "phase %s" % a.get("phase"), "bull": False})
    p = F["phase"].get(sym)
    if p and str(p.get("phase") or "").upper().startswith("ACCUM"):
        legs.append({"src": "phase-detector", "read": "accumulation range since %s" % (p.get("begin") or "?"), "bull": True}); score += 1
    fo = F["fortress"].get(sym)
    if fo and fo.get("tier") in ("FORTRESS_COIL", "COILED", "ACCUMULATING"):
        legs.append({"src": "fortress", "read": "%s (dump capture %s)" % (fo.get("tier"), rnd(fo.get("dump_capture"))), "bull": True}); score += 1
    ka = F["katlin"].get(sym)
    if ka and ka.get("tier") in ("KATLIN_PRIME", "READY", "BASING"):
        legs.append({"src": "katlin", "read": "%s / structure %s" % (ka.get("tier"), ka.get("structure_state")), "bull": True}); score += 1
    if asset_class == "stock":
        f = F["f13"].get(sym) or F["f13"].get(to_fv(sym))
        if isinstance(f, dict):
            net = fnum(f.get("net_usd_m") or f.get("net") or f.get("net_flow_usd_m"))
            if net is not None and net > 0:
                legs.append({"src": "13F", "read": "institutions net +$%.0fM last quarter" % net, "bull": True}); score += 1
            elif net is not None and net < 0:
                legs.append({"src": "13F", "read": "institutions net -$%.0fM last quarter" % -net, "bull": False})
        ins = F["insider"].get(sym)
        if ins and (ins.get("cluster") or ins.get("n_buys", 0) >= 2):
            legs.append({"src": "insiders", "read": "%s%d open-market buys ($%.1fM)" % ("cluster, " if ins.get("cluster") else "", ins.get("n_buys", 0), ins.get("usd", 0) / 1e6), "bull": True}); score += 1
        d = F["dark"].get(sym) or F["dark"].get(to_fv(sym))
        if isinstance(d, dict):
            stt = str(d.get("state") or d.get("signal") or "").upper()
            if "ACCUM" in stt or "BUY" in stt:
                legs.append({"src": "dark pool", "read": stt.lower().replace("_", " "), "bull": True}); score += 1
            elif "DISTRIB" in stt or "SELL" in stt:
                legs.append({"src": "dark pool", "read": stt.lower().replace("_", " "), "bull": False})
    else:
        fl = F["flows"].get(sym)
        if isinstance(fl, dict):
            z = fnum(fl.get("z90") or fl.get("flow_z90") or fl.get("z_score_90d"))
            pa = fnum(fl.get("pct_aum_21d"))
            if (z is not None and z >= 1.0) or (pa is not None and pa >= 2.0):
                legs.append({"src": "etf-flows", "read": "inflows z90 %s / %s%% of AUM 21d" % (rnd(z, 1), rnd(pa, 1)), "bull": True}); score += 1
            elif (z is not None and z <= -1.0) or (pa is not None and pa <= -2.0):
                legs.append({"src": "etf-flows", "read": "outflows z90 %s / %s%% of AUM 21d" % (rnd(z, 1), rnd(pa, 1)), "bull": False})
    return legs, score


# ---- rows -------------------------------------------------------------------------------------------------------------
def _event_view(E, dates, b_d, frame, prm):
    """serialise an event with dates instead of indices (b_d maps bar position -> session index)."""
    def dt(i):
        return dates[b_d[i]] if i is not None and 0 <= i < len(b_d) else None
    tests = [{"date": dt(t.get("i")), "low": rnd(t.get("low"), 4), "vol_ratio_sc": rnd(t.get("vol_ratio_sc"), 2), "depth_class": t.get("depth_class"),
              "lower_vol_than_prev": t.get("lower_vol_than_prev"), "kind": t.get("kind")} for t in (E.get("tests") or [])]
    return {"frame": frame, "state": E.get("state"), "why_closed": E.get("why_closed"),
            "sc": {"date": dt(E.get("sc_i")), "end": dt(E.get("sc_end")), "low": rnd(E.get("sc_low"), 4), "vol_x": rnd(E.get("sc_vol_x"), 2), "range_x": rnd(E.get("sc_range_x"), 2),
                   "clv": rnd(E.get("sc_clv"), 2), "decline_pct": rnd(E.get("sc_decline_pct"), 1), "pre_high": rnd(E.get("pre_high"), 4), "pre_high_date": dt(E.get("pre_high_i")),
                   "bars_down": E.get("sc_bars_down"), "pos_range_pct": rnd(E.get("sc_pos_range_pct"), 0), "vol_vs_regime": rnd(E.get("sc_vol_vs_regime"), 2),
                   "prelim_vol_x": rnd(E.get("sc_prelim_vol_x"), 2), "post_vol_ratio": rnd(E.get("post_vol_ratio"), 2)},
            "ar": {"date": dt(E.get("ar_i")), "high": rnd(E.get("ar_high"), 4), "rally_pct": rnd(E.get("ar_rally_pct"), 1), "rally_atr": rnd(E.get("ar_rally_atr"), 2),
                   "crowd_buy_date": dt(E.get("ar_valid_i")), "crowd_buy_px": rnd(E.get("ar_buy_px"), 4)},
            "st": {"date": dt(E.get("st_i")), "low": rnd(E.get("st_low"), 4), "bar_high": rnd(E.get("st_bar_high"), 4), "vol_ratio_sc": rnd(E.get("st_vol_ratio_sc"), 2),
                   "vol_ratio_avg": rnd(E.get("st_vol_ratio_avg"), 2), "depth_atr": rnd(E.get("st_depth_atr"), 2), "depth_class": E.get("st_depth_class"),
                   "retrace_frac": rnd(E.get("st_retrace_frac"), 2), "approach_vol_slope": rnd(E.get("approach_vol_slope"), 3), "approach_range_x": rnd(E.get("approach_range_x"), 2),
                   "spring": bool(E.get("spring")), "tests": tests, "n_tests": len([t for t in tests if t.get("kind") == "TEST"])},
            "trigger": {"date": dt(E.get("trig_i")), "level": rnd(E.get("trig_level"), 4), "fill": rnd(E.get("trig_fill"), 4), "vol_x": rnd(E.get("trig_vol_x"), 2),
                        "sos_date": dt(E.get("sos_i")), "stop_date": dt(E.get("stop_i"))},
            "outcome": {"ret_21": rnd(E.get("ret_21"), 1), "ret_63": rnd(E.get("ret_63"), 1), "mret_21": rnd(E.get("mret_21"), 1), "mret_63": rnd(E.get("mret_63"), 1),
                        "mfe_pct": rnd(E.get("mfe_pct"), 1), "mae_pct": rnd(E.get("mae_pct"), 1), "t1_hit": E.get("t1_hit"), "stop_hit": E.get("stop_hit"), "end": dt(E.get("end_i"))},
            "crowd": {"ar_ret_21": rnd(E.get("ar_ret_21"), 1), "ar_ret_63": rnd(E.get("ar_ret_63"), 1), "ar_mae_pct": rnd(E.get("ar_mae_pct"), 1), "ar_hole": E.get("ar_hole")},
            "atr": rnd(E.get("atr"), 4)}


def plan_for(E, last, prm):
    if E.get("st_i") is None:
        return None
    entry = E.get("trig_level") or E.get("st_bar_high")
    stop = E.get("stop") if E.get("stop") is not None else (E["st_low"] - prm["stop_buffer_atr"] * E["atr"])
    t1 = E.get("ar_high")
    t2 = E.get("pre_high")
    risk = (entry - stop) / entry * 100.0 if entry and stop and entry > stop else None
    rr1 = ((t1 - entry) / (entry - stop)) if (t1 and entry and stop and entry > stop and t1 > entry) else None
    rr2 = ((t2 - entry) / (entry - stop)) if (t2 and entry and stop and entry > stop and t2 > entry) else None
    stop_climax = min(E["sc_low"], E["st_low"]) - 0.25 * E["atr"] if E.get("sc_low") else None
    stop_wide = E["st_low"] - 1.0 * E["atr"]
    return {"entry": rnd(entry, 4), "stop": rnd(stop, 4), "risk_pct": rnd(risk, 2), "target_1": rnd(t1, 4), "target_2": rnd(t2, 4),
            "rr_1": rnd(rr1, 2), "rr_2": rnd(rr2, 2), "last_vs_entry_pct": rnd(100.0 * (last / entry - 1.0), 2) if entry else None,
            "stop_climax": rnd(stop_climax, 4), "risk_climax_pct": rnd((entry - stop_climax) / entry * 100.0, 2) if (entry and stop_climax and entry > stop_climax) else None,
            "stop_wide": rnd(stop_wide, 4), "risk_wide_pct": rnd((entry - stop_wide) / entry * 100.0, 2) if (entry and stop_wide and entry > stop_wide) else None,
            "rule": "buy the reaction away from the test (close above the test candle's high); the paper's stop sits just under the test low; the structural stop sits under the climax low; first target the rally high, second the pre-climax swing high -- the base-rates tab shows how often each stop was hit and what each rule earned"}


def why_text(r, ev, plan, confirm):
    st = r["state"]
    sc, ar, stt = ev["sc"], ev["ar"], ev["st"]
    s = []
    name = r.get("company") or r["ticker"]
    if st == "CLIMAX":
        s.append("%s just printed a selling climax on %s: %.1fx normal volume on a %.1fx-ATR bar after a %.0f%% decline. Sellers panicked; someone absorbed it. The next thing to watch is the bounce -- do not buy the climax itself." % (name, sc["date"], sc["vol_x"] or 0, sc["range_x"] or 0, -(sc["decline_pct"] or 0)))
    elif st == "TESTING":
        s.append("%s bounced %.1f%% off its %s climax low (%s) and is now drifting back toward it. This is the moment that separates amateurs from professionals: the question is whether the return to %.2f comes on silence. Watch the volume, not the price." % (name, ar["rally_pct"] or 0, sc["date"], sc["low"], sc["low"]))
    elif st == "ST_CONFIRMED":
        s.append("%s tested its %s climax low on %s at %.0f%% of the climax volume%s. The door was knocked on again and only silence answered. The trigger is a close above %.2f (the high of the test candle); the stop sits under %.2f." % (
            name, sc["date"], stt["date"], 100 * (stt["vol_ratio_sc"] or 0), " and held ABOVE the old low (a higher low -- buyers did not wait)" if stt["depth_class"] == "HIGHER_LOW" else (" with a light-volume undercut that was recovered (a spring)" if stt["depth_class"] == "SPRING" else ""), stt["bar_high"] or 0, stt["low"] or 0))
    elif st == "TRIGGERED":
        s.append("%s triggered on %s: it closed above the high of the candle that made the test low, after a %s climax (%.1fx volume) and a test on %.0f%% of that volume. This is a confirmed bottom by the paper's rules -- an entry on evidence, not a guess. Stop under %.2f, first target the rally high %.2f." % (
            name, ev["trigger"]["date"], sc["date"], sc["vol_x"] or 0, 100 * (stt["vol_ratio_sc"] or 0), plan["stop"] if plan else 0, ar["high"] or 0))
    elif st == "MARKUP":
        s.append("%s completed the whole sequence (climax %s, test %s, trigger %s) and has now closed above the rally high -- a sign of strength; the range is resolving upward." % (name, sc["date"], stt["date"], ev["trigger"]["date"]))
    elif st == "FAILED":
        s.append("%s's bottom attempt FAILED (%s): %s. The floor vanished -- this is the pattern that funds capitulations. Stand aside; a fresh climax must print before there is anything to test." % (name, ev["outcome"]["end"], ev["why_closed"]))
    elif st == "STOPPED":
        s.append("%s triggered on %s but closed below the protective stop on %s. The thesis (supply exhausted at %.2f) was proven wrong and the loss was defined in advance -- that is the point of the stop." % (name, ev["trigger"]["date"], ev["trigger"]["stop_date"], stt["low"] or 0))
    elif st == "COMPLETED":
        s.append("%s ran the full sequence; the outcome window closed on %s (%s%% after 63 bars from the trigger)." % (name, ev["outcome"]["end"], ev["outcome"]["ret_63"]))
    elif st == "NO_TEST_BREAKOUT":
        s.append("%s left the range upward before ever testing the low (a V-bottom). Real, but not a Wyckoff entry -- there was no low-volume test to lean on." % name)
    elif st in ("NO_RALLY", "EXPIRED"):
        s.append("%s: %s." % (name, ev["why_closed"]))
    if confirm:
        bull = [c for c in confirm if c.get("bull")]
        bear = [c for c in confirm if not c.get("bull")]
        if bull:
            s.append("Fleet confirmation: " + "; ".join("%s -- %s" % (c["src"], c["read"]) for c in bull[:4]) + ".")
        if bear:
            s.append("Against it: " + "; ".join("%s -- %s" % (c["src"], c["read"]) for c in bear[:2]) + ".")
    return " ".join(s)


def chart_payload(b, dates, E, n_bars=70):
    """the 'theater': the bars around the sequence for the evidence panel (bounded)."""
    end = len(b.c) - 1
    start = max(0, min(E.get("sc_i", end) - 12, end - n_bars + 1))
    idx = list(range(start, end + 1))
    if len(idx) > 110:
        idx = idx[-110:]
    def g(a, k):
        return round(a[k], 4)
    marks = {}
    for key, name in (("sc_i", "SC"), ("ar_i", "AR"), ("st_i", "ST"), ("trig_i", "TRIG"), ("sos_i", "SOS"), ("stop_i", "STOP")):
        i = E.get(key)
        if i is not None and idx[0] <= i <= idx[-1]:
            marks[name] = i - idx[0]
    return {"d": [dates[b.d[k]] for k in idx], "o": [g(b.o, k) for k in idx], "h": [g(b.h, k) for k in idx], "l": [g(b.l, k) for k in idx],
            "c": [g(b.c, k) for k in idx], "v": [round(b.v[k]) for k in idx], "marks": marks}


GATE_DIAG = {"D": {}, "W": {}}


def run_frame(b, dates, frame, desk="stocks"):
    prm = P[frame]
    if frame == "D":
        o, h, l, c, v, d = b.o, b.h, b.l, b.c, b.v, b.d
        dmin = P["decline_min_pct_by_desk"].get(desk, prm["decline_min_pct"])
    else:
        W = resample_weekly(b, dates)
        o, h, l, c, v, d = W["o"], W["h"], W["l"], W["c"], W["v"], W["d"]
        if len(c) < P["min_weeks"]:
            return None, [], None, None
        dmin = P["decline_min_pct_by_desk_W"].get(desk, prm["decline_min_pct"])
    events, active = detect(o, h, l, c, v, prm, frame, decline_min_pct=dmin, diag=GATE_DIAG[frame])
    n = len(c)
    cur = active
    if cur is None and events:
        last = events[-1]
        if last.get("end_i") is not None and n - 1 - last["end_i"] <= P["recent_closed_bars"][frame]:
            cur = last
    return cur, events, d, (o, h, l, c, v)


def build_row(sym, asset_class, sub_class, b, dates, F, session_idx):
    last = b.c[-1]
    fv = F["finviz"].get(to_fv(sym)) or {}
    adv = mean([b.v[k] * b.c[k] for k in range(max(0, len(b.c) - 20), len(b.c))]) if asset_class != "crypto" else mean([b.v[k] for k in range(max(0, len(b.c) - 20), len(b.c))])
    desk = DESK.get(sub_class, "stocks")
    curD, evD, dD, arrD = run_frame(b, dates, "D", desk)
    curW, evW, dW, arrW = run_frame(b, dates, "W", desk)
    if curD is None and curW is None:
        return None, evD, evW
    mcap = fnum(fv.get("market_cap"))
    if mcap is not None and mcap < 1e8:
        mcap *= 1e6
    aum = fnum(fv.get("aum"))
    if aum is not None and aum < 1e8:
        aum *= 1e6
    confirm, n_conf = confirmations(sym, asset_class, F)
    # the row's headline frame: the daily sequence unless only the weekly is live, or the weekly is further along
    rank = {"MARKUP": 7, "TRIGGERED": 6, "ST_CONFIRMED": 5, "TESTING": 4, "CLIMAX": 3, "COMPLETED": 2, "STOPPED": 1, "FAILED": 1, "NO_TEST_BREAKOUT": 1, "EXPIRED": 0, "NO_RALLY": 0}
    head_frame = "D"
    if curD is None or (curW is not None and rank.get(curW["state"], 0) > rank.get(curD["state"], 0) and curW["state"] in ("ST_CONFIRMED", "TRIGGERED", "MARKUP")):
        head_frame = "W"
    E = curD if head_frame == "D" else curW
    prm = P[head_frame]
    score, grade, parts, reasons, risks = score_event(E, prm)
    if n_conf:
        score = clamp(score + min(8, 2 * n_conf))
    if curW is not None and head_frame == "D" and curW["state"] in ("ST_CONFIRMED", "TRIGGERED", "MARKUP"):
        score = clamp(score + 6)
        reasons.append("weekly frame agrees (%s)" % curW["state"].lower().replace("_", " "))
    sma200 = sma_series(list(b.c), 200)[-1] if len(b.c) >= 200 else None
    hi252 = max(b.h[max(0, len(b.h) - 252):]); lo252 = min(b.l[max(0, len(b.l) - 252):])
    pos_52w = 100.0 * (last - lo252) / (hi252 - lo252) if hi252 > lo252 else None
    bd = b.d if head_frame == "D" else dW
    ev = _event_view(E, dates, bd, head_frame, prm)
    plan = plan_for(E, last, prm)
    state = E["state"]
    since_i = {"CLIMAX": E.get("sc_i"), "TESTING": E.get("test_start_i"), "ST_CONFIRMED": E.get("st_confirm_i"), "TRIGGERED": E.get("trig_i"), "MARKUP": E.get("sos_i")}.get(state, E.get("end_i"))
    bars_in_state = (len(arrD[3]) - 1 - since_i) if (head_frame == "D" and since_i is not None) else ((len(arrW[3]) - 1 - since_i) if (since_i is not None and arrW) else None)
    # actionable = the paper's entry is still available: a confirmed test awaiting its trigger, or a fresh trigger with price
    # still inside the range (not a name that already ran to the top of the chart)
    trig_lvl = E.get("trig_level") or E.get("st_bar_high")
    rng_w = (E["ar_high"] - E["sc_low"]) if (E.get("ar_high") and E.get("sc_low") and E["ar_high"] > E["sc_low"]) else None
    still_low = bool(trig_lvl and rng_w and last <= trig_lvl + 0.6 * rng_w)
    actionable = (state == "ST_CONFIRMED") or (state == "TRIGGERED" and (bars_in_state if bars_in_state is not None else 99) <= 10 and still_low)
    r = {"ticker": sym, "company": (fv.get("company") or sym)[:60], "asset_class": asset_class, "sub_class": sub_class, "desk": DESK.get(sub_class, "stocks"),
         "sector": fv.get("sector") or None, "industry": fv.get("industry") or None, "country": fv.get("country") or None,
         "last": rnd(last, 4 if last < 1 else 2), "mcap": mcap, "aum": aum, "adv_usd": rnd(adv, 0),
         "frame": head_frame, "state": state, "grade": grade, "score": round(score, 1), "score_parts": parts, "bars_in_state": bars_in_state,
         "session_date": dates[b.d[-1]],
         "sc_date": ev["sc"]["date"], "sc_low": ev["sc"]["low"], "sc_vol_x": ev["sc"]["vol_x"], "sc_range_x": ev["sc"]["range_x"], "sc_decline_pct": ev["sc"]["decline_pct"],
         "ar_high": ev["ar"]["high"], "ar_rally_pct": ev["ar"]["rally_pct"],
         "st_date": ev["st"]["date"], "st_low": ev["st"]["low"], "st_vol_ratio_sc": ev["st"]["vol_ratio_sc"], "st_vol_ratio_avg": ev["st"]["vol_ratio_avg"],
         "st_depth_class": ev["st"]["depth_class"], "n_tests": ev["st"]["n_tests"], "approach_vol_slope": ev["st"]["approach_vol_slope"], "approach_range_x": ev["st"]["approach_range_x"],
         "trigger_date": ev["trigger"]["date"], "trigger_level": ev["trigger"]["level"],
         "dist_sc_low_pct": rnd(100.0 * (last / E["sc_low"] - 1.0), 1) if E.get("sc_low") else None,
         "dist_sma200_pct": rnd(100.0 * (last / sma200 - 1.0), 1) if sma200 else None,
         "pos_52w_pct": rnd(pos_52w, 0), "dist_52w_high_pct": rnd(100.0 * (last / hi252 - 1.0), 1) if hi252 else None,
         "actionable": bool(actionable), "still_in_range": still_low, "sc_pos_range_pct": ev["sc"]["pos_range_pct"], "sc_bars_down": ev["sc"]["bars_down"],
         "plan": plan, "event": ev, "weekly_state": curW["state"] if curW else None, "daily_state": curD["state"] if curD else None,
         "weekly": _event_view(curW, dates, dW, "W", P["W"]) if (curW is not None and head_frame == "D") else None,
         "confirm": confirm, "n_confirm": n_conf, "reasons": reasons, "risks": risks,
         "n_events_5y": len(evD), "n_triggers_5y": sum(1 for e in evD if e.get("trig_i") is not None)}
    r["why"] = why_text(r, ev, plan, confirm)
    r["what_next"] = {"CLIMAX": "wait for the automatic rally to set the range top; never buy the climax bar",
                      "TESTING": "watch the volume as price returns to %s -- silence confirms, rising volume fails" % ev["sc"]["low"],
                      "ST_CONFIRMED": "buy only a close above %s (the test candle's high); stop %s" % (ev["st"]["bar_high"], plan["stop"] if plan else None),
                      "TRIGGERED": "hold with the stop at %s; first target %s (rally high); a close above it is a sign of strength" % (plan["stop"] if plan else None, plan["target_1"] if plan else None),
                      "MARKUP": "range resolved upward; trail the stop under the test low / rally high",
                      "FAILED": "stand aside until a new climax prints", "STOPPED": "thesis invalidated; wait for a new sequence",
                      "NO_TEST_BREAKOUT": "no low-volume test to lean on; not a Wyckoff entry", "COMPLETED": "sequence graded; watch for a new climax",
                      "EXPIRED": "sequence expired without a test/trigger", "NO_RALLY": "the climax did not hold"}.get(state)
    # bounded chart for the panel (the headline frame's bars)
    if head_frame == "D":
        r["chart"] = chart_payload(b, dates, E, 70)
    else:
        Wb = Bars()
        for k in range(len(arrW[3])):
            Wb.d.append(dW[k]); Wb.o.append(arrW[0][k]); Wb.h.append(arrW[1][k]); Wb.l.append(arrW[2][k]); Wb.c.append(arrW[3][k]); Wb.v.append(arrW[4][k])
        r["chart"] = chart_payload(Wb, dates, E, 60)
    return r, evD, evW


# ---- base rates: every completed sequence in the window, graded from the tape --------------------------------------
def _stats(vals):
    vs = [x for x in vals if x is not None]
    if not vs:
        return {"n": 0}
    return {"n": len(vs), "median": rnd(median(vs), 2), "mean": rnd(mean(vs), 2), "hit": rnd(100.0 * sum(1 for x in vs if x > 0) / len(vs), 0)}


def base_rates(hist):
    """hist rows: {desk, asset_class, frame, score, grade, depth_class, state/outcome, ret_21, ret_63, mret_21, mret_63, stop_hit, t1_hit,
    ar_ret_21, ar_ret_63, ar_mae_pct, ar_hole, sc_vol_x, st_vol_ratio_sc}"""
    out = {}
    trig = [e for e in hist if e.get("triggered")]
    seq = [e for e in hist if e.get("had_ar")]
    out["n_sequences"] = len(hist)
    out["n_with_rally"] = len(seq)
    out["n_triggered"] = len(trig)
    out["outcome_mix"] = {}
    for e in hist:
        out["outcome_mix"][e["outcome"]] = out["outcome_mix"].get(e["outcome"], 0) + 1

    def stop_block(rows):
        out = {}
        for rule in ("paper", "climax", "wide"):
            rs = [e["stops"][rule] for e in rows if isinstance(e.get("stops"), dict) and e["stops"].get(rule)]
            if not rs:
                continue
            out[rule] = {"n": len(rs), "stop_hit_pct": rnd(100.0 * sum(1 for x in rs if x.get("hit")) / len(rs), 0),
                         "managed_21": _stats([x.get("mret_21") for x in rs]), "managed_63": _stats([x.get("mret_63") for x in rs]),
                         "median_bars_to_stop": rnd(median([x.get("hit_bar") for x in rs if x.get("hit_bar") is not None]), 0)}
        best = None
        cands = [(k, v) for k, v in out.items() if (v.get("managed_63") or {}).get("n", 0) >= 100]
        if cands:
            best = max(cands, key=lambda kv: (kv[1]["managed_63"].get("median") or -1e9))[0]
        return {"rules": out, "best_rule_by_median_63": best,
                "levels": {"paper": "0.1 ATR under the test low (the paper)", "climax": "0.25 ATR under the lower of the climax low and the test low (structural)", "wide": "1.0 ATR under the test low"}}

    def block(rows):
        return {"n": len(rows), "ret_21": _stats([e.get("ret_21") for e in rows]), "ret_63": _stats([e.get("ret_63") for e in rows]),
                "managed_21": _stats([e.get("mret_21") for e in rows]), "managed_63": _stats([e.get("mret_63") for e in rows]),
                "stop_hit_pct": rnd(100.0 * sum(1 for e in rows if e.get("stop_hit")) / len(rows), 0) if rows else None,
                "target1_hit_pct": rnd(100.0 * sum(1 for e in rows if e.get("t1_hit")) / len([e for e in rows if e.get("t1_hit") is not None]), 0) if [e for e in rows if e.get("t1_hit") is not None] else None,
                "mae_median": rnd(median([e.get("mae_pct") for e in rows if e.get("mae_pct") is not None]), 1) if rows else None,
                "mfe_median": rnd(median([e.get("mfe_pct") for e in rows if e.get("mfe_pct") is not None]), 1) if rows else None,
                "target1_before_stop_pct": rnd(100.0 * sum(1 for e in rows if e.get("t1_before_stop")) / len([e for e in rows if e.get("t1_before_stop") is not None]), 0) if [e for e in rows if e.get("t1_before_stop") is not None] else None,
                "stops": stop_block(rows)}
    out["triggered"] = {"all": block(trig)}
    for fr in ("D", "W"):
        out["triggered"]["frame_" + fr] = block([e for e in trig if e.get("frame") == fr])
    out["by_desk"] = {k: block([e for e in trig if e.get("desk") == k]) for k in sorted(set(e.get("desk") for e in trig if e.get("desk")))}
    out["by_grade"] = {g: block([e for e in trig if e.get("grade") == g]) for g in ("A", "B", "C", "D")}
    out["by_depth_class"] = {d: block([e for e in trig if e.get("depth_class") == d]) for d in ("HIGHER_LOW", "EQUAL_LOW", "SPRING")}
    vb = {"quiet (<=0.4x climax)": [e for e in trig if (e.get("st_vol_ratio_sc") or 9) <= 0.4], "diminished (0.4-0.7x)": [e for e in trig if 0.4 < (e.get("st_vol_ratio_sc") or 9) <= 0.7],
          "loud (>0.7x)": [e for e in trig if (e.get("st_vol_ratio_sc") or 0) > 0.7]}
    out["by_test_volume"] = {k: block(v) for k, v in vb.items()}
    # the paper's counterfactual: the crowd buys the automatic rally; the professional waits for the trigger
    crowd = [e for e in seq if e.get("ar_ret_21") is not None]
    out["crowd_vs_pro"] = {
        "crowd_buys_the_bounce": {"n": len(crowd), "ret_21": _stats([e.get("ar_ret_21") for e in crowd]), "ret_63": _stats([e.get("ar_ret_63") for e in crowd]),
                                  "worst_drawdown_median_pct": rnd(median([e.get("ar_mae_pct") for e in crowd if e.get("ar_mae_pct") is not None]), 1) if crowd else None,
                                  "later_undercut_climax_low_pct": rnd(100.0 * sum(1 for e in seq if e.get("ar_hole")) / len(seq), 0) if seq else None,
                                  "sequence_failed_pct": rnd(100.0 * sum(1 for e in seq if e.get("outcome") == "FAILED") / len(seq), 0) if seq else None},
        "pro_waits_for_the_trigger": {"n": len(trig), "ret_21": _stats([e.get("mret_21") for e in trig]), "ret_63": _stats([e.get("mret_63") for e in trig]),
                                      "worst_drawdown_median_pct": rnd(median([e.get("mae_pct") for e in trig if e.get("mae_pct") is not None]), 1) if trig else None,
                                      "stopped_out_pct": rnd(100.0 * sum(1 for e in trig if e.get("stop_hit")) / len(trig), 0) if trig else None,
                                      "share_of_sequences_that_ever_trigger_pct": rnd(100.0 * len(trig) / len(seq), 0) if seq else None},
        "note": "measured from every selling-climax sequence in the 5-year window; crowd entry = close of the bar the automatic rally reached 2 ATR off the low; pro entry = close above the test candle's high; managed returns exit at the stop when it hits first"}
    out["findings"] = findings(out, seq, trig)
    return out


def findings(out, seq, trig):
    """plain-English verdicts derived from the measured numbers -- the engine argues with the paper where the tape disagrees."""
    f = []
    t = (out.get("triggered") or {}).get("all") or {}
    if not t.get("n"):
        return f
    r63 = t.get("ret_63") or {}
    st = (t.get("stops") or {}).get("rules") or {}
    pap, cli, wid = st.get("paper") or {}, st.get("climax") or {}, st.get("wide") or {}
    f.append("%d sequences reached the trigger. Unmanaged, the trigger entry made a median %+.1f%% after 63 bars (hit %.0f%%) and reached the rally high (target 1) in %.0f%% of cases%s." % (
        t["n"], r63.get("median") or 0, r63.get("hit") or 0, t.get("target1_hit_pct") or 0,
        (", %.0f%% of them before the paper's stop was touched" % t["target1_before_stop_pct"]) if t.get("target1_before_stop_pct") is not None else ""))
    if pap:
        hp = pap.get("stop_hit_pct") or 0
        f.append("The paper's stop (0.1 ATR under the test low) was hit in %.0f%% of triggered sequences%s; managed by that stop the 63-bar outcome is a median %+.1f%% (hit %.0f%%).%s" % (
            hp, (" (median %.0f bars after entry)" % pap["median_bars_to_stop"]) if pap.get("median_bars_to_stop") is not None else "",
            (pap.get("managed_63") or {}).get("median") or 0, (pap.get("managed_63") or {}).get("hit") or 0,
            " A stop that tight is whipsawed by the range itself: the tape revisits the test low far more often than it fails." if hp >= 50 else ""))
    if cli and wid:
        f.append("Alternatives measured on the same trades: a stop 0.25 ATR under the climax low is hit %.0f%% of the time (managed 63-bar median %+.1f%%); a stop 1 ATR under the test low is hit %.0f%% (median %+.1f%%). Best rule by median managed return: %s." % (
            cli.get("stop_hit_pct") or 0, (cli.get("managed_63") or {}).get("median") or 0, wid.get("stop_hit_pct") or 0, (wid.get("managed_63") or {}).get("median") or 0,
            (t.get("stops") or {}).get("best_rule_by_median_63") or "n/a"))
    cv = out.get("crowd_vs_pro") or {}
    c = cv.get("crowd_buys_the_bounce") or {}
    if c.get("n"):
        mix = out.get("outcome_mix") or {}
        vshare = 100.0 * (mix.get("NO_TEST_BREAKOUT") or 0) / max(1, out.get("n_sequences") or 1)
        f.append("The crowd's bounce entry (n %d) made a median %+.1f%% after 63 bars (hit %.0f%%) but with a median worst drawdown of %.1f%%; in %.0f%% of sequences the tape later undercut the climax low and %.0f%% failed outright. Waiting for the test forfeits the V-bottoms: %.0f%% of all sequences left the range upward without ever testing the low." % (
            c["n"], (c.get("ret_63") or {}).get("median") or 0, (c.get("ret_63") or {}).get("hit") or 0, c.get("worst_drawdown_median_pct") or 0, c.get("later_undercut_climax_low_pct") or 0, c.get("sequence_failed_pct") or 0, vshare))
    tv = out.get("by_test_volume") or {}
    q, l = tv.get("quiet (<=0.4x climax)") or {}, tv.get("loud (>0.7x)") or {}
    if q.get("n") and l.get("n"):
        f.append("Test volume: quiet tests (<=0.4x climax volume, n %d) made a median %+.1f%% at 63 bars vs %+.1f%% for loud tests (n %d) -- %s." % (
            q["n"], (q.get("ret_63") or {}).get("median") or 0, (l.get("ret_63") or {}).get("median") or 0, l["n"],
            "the paper's variable carries an edge in this window" if ((q.get("ret_63") or {}).get("median") or 0) > ((l.get("ret_63") or {}).get("median") or 0) + 0.5 else "no return edge from the volume ratio alone in this window; use it as a risk filter, not a return forecast"))
    bg = out.get("by_grade") or {}
    if (bg.get("A") or {}).get("n") and (bg.get("C") or {}).get("n"):
        f.append("Grade A sequences (n %d) made a median %+.1f%% at 63 bars vs %+.1f%% for grade C (n %d)." % ((bg["A"]["n"]), (bg["A"].get("ret_63") or {}).get("median") or 0, (bg["C"].get("ret_63") or {}).get("median") or 0, bg["C"]["n"]))
    return f


def hist_row(sym, desk, asset_class, E, frame, prm):
    score, grade, _, _, _ = score_event(E, prm)
    return {"ticker": sym, "desk": desk, "asset_class": asset_class, "frame": frame, "score": score, "grade": grade, "depth_class": E.get("st_depth_class"),
            "outcome": E.get("outcome") or E.get("state"), "triggered": E.get("trig_i") is not None, "had_ar": E.get("ar_valid_i") is not None,
            "ret_21": E.get("ret_21"), "ret_63": E.get("ret_63"), "mret_21": E.get("mret_21"), "mret_63": E.get("mret_63"),
            "stop_hit": E.get("stop_hit"), "t1_hit": E.get("t1_hit"), "mae_pct": E.get("mae_pct"), "mfe_pct": E.get("mfe_pct"),
            "ar_ret_21": E.get("ar_ret_21"), "ar_ret_63": E.get("ar_ret_63"), "ar_mae_pct": E.get("ar_mae_pct"), "ar_hole": E.get("ar_hole"),
            "sc_vol_x": E.get("sc_vol_x"), "st_vol_ratio_sc": E.get("st_vol_ratio_sc"), "stops": E.get("stops"), "t1_before_stop": E.get("t1_before_stop")}


# ---- market context ---------------------------------------------------------------------------------------------------
def market_context(rows, bench_rows, F, n_scored):
    st_counts = {}
    for r in rows:
        st_counts[r["state"]] = st_counts.get(r["state"], 0) + 1
    live = [r for r in rows if r["state"] in ("CLIMAX", "TESTING", "ST_CONFIRMED", "TRIGGERED", "MARKUP")]
    trig5 = [r for r in rows if r["state"] in ("TRIGGERED", "MARKUP") and (r.get("bars_in_state") or 99) <= 5]
    fails10 = [r for r in rows if r["state"] in ("FAILED", "STOPPED") and (r.get("bars_in_state") or 99) <= 10]
    breadth = {"in_bottom_process": len(live), "share_of_universe_pct": rnd(100.0 * len(live) / n_scored, 1) if n_scored else None,
               "climax_5d": sum(1 for r in rows if r["state"] == "CLIMAX" and (r.get("bars_in_state") or 99) <= 5),
               "testing": st_counts.get("TESTING", 0), "test_confirmed": st_counts.get("ST_CONFIRMED", 0),
               "triggered_5d": len(trig5), "failed_10d": len(fails10), "weekly_live": sum(1 for r in rows if r.get("weekly_state") in ("ST_CONFIRMED", "TRIGGERED", "MARKUP"))}
    read = "quiet tape" if len(live) < 0.03 * max(n_scored, 1) else ("bottoming processes are widespread -- a market-level climax/test cycle may be in play" if len(live) > 0.12 * max(n_scored, 1) else "selective bottoming processes")
    if breadth["climax_5d"] >= 0.04 * max(n_scored, 1):
        read = "capitulation cluster: %d names printed selling climaxes in the last 5 sessions" % breadth["climax_5d"]
    return {"benchmarks": bench_rows, "breadth": breadth, "read": read, "by_state": st_counts,
            "capital_authority": F.get("authority"), "risk_gate": F.get("risk_gate"), "katlin_posture": F.get("katlin_posture"),
            "note": "detection is pure tape (price and volume); the capital authority / risk gate decide whether NEW risk may be taken at all -- a confirmed bottom is an opportunity, not a permission"}


DEFINITIONS = {
    "selling_climax": "The highest intensity of supply within a downtrend, only after the move has been in effect for some time (Wyckoff/SMI). The engine requires all of: a real decline (>= 12% for stocks / 6% bonds / 5% currencies / 10% wrappers / 18% crypto, and >= 6 ATR, under the 120-bar high), a prolonged one (that high printed >= 15 bars earlier, or a >= 25% crash), the bar makes a NEW 120-bar LOW under the 50-bar average with the close in the bottom 35% of the yearly range, climactic volume (>= 2x the prior 50-bar average, the heaviest of the prior 60 bars, and >= 2x the 250-bar average so a rising volume regime does not qualify) and a wide spread (>= 1.5 ATR). Weekly frame: 52-week high, 26-week low, 26-week average, >= 6 weeks down. A pullback to a 3-week low in an uptrend is NOT a climax.",
    "actionable": "The paper's entry is still available: a confirmed secondary test awaiting its trigger, or a trigger no older than 10 bars with price still inside the range (no more than 60% of the range width above the trigger level). Names that already ran to the top of the chart are shown as resolved (MARKUP), never as bottoms to buy.",
    "automatic_rally": "The sharp bounce that follows a climax because sellers are exhausted (>=2 ATR off the climax low within 15 bars). Its high and the climax low define the trading range. The crowd buys this bounce; the engine records the crowd's entry to measure it.",
    "secondary_test": "Price drifts back toward the climax low. The signal is the VOLUME: the test candle's volume vs the climax volume (vol_ratio_sc), vs the 50-bar average (vol_ratio_avg), the slope of volume on the way down (approach_vol_slope) and bar narrowing (approach_range_x). A quiet test says the supply is gone.",
    "depth_class": "Where the test low sits vs the climax low in ATR units: HIGHER_LOW (buyers stepped in early -- the strongest), EQUAL_LOW (within 0.25 ATR), SPRING (light-volume undercut that recovered), UNDERCUT.",
    "trigger": "The paper's rule: never buy the level, buy the reaction away from it -- a CLOSE above the high of the candle that made the test low. Entry = that high; the engine records the trigger-bar close as the fill.",
    "stop": "0.1 ATR under the absolute low of the test. If it prints, the exhaustion thesis is proven wrong -- the loss was defined before entry.",
    "targets": "Target 1 = the automatic-rally high (top of the range); target 2 = the pre-climax swing high (the 60-bar high before the decline).",
    "failed": "The test arrived on RISING volume (>=70% of climax volume or >=1.5x average) and broke the low, or the undercut was not recovered within 3 bars. The floor vanished -- stand aside.",
    "markup": "After the trigger, a close above the rally high: a sign of strength; the range resolved upward.",
    "score": "0-100 evidence ladder: climax quality (<=25) + rally quality (<=15) + test quality (<=40) + trigger/confirmation (<=20), plus up to +8 for fleet confirmations and +6 when the weekly frame agrees. A/B/C/D = >=75 / >=60 / >=45 / below.",
    "frame": "D = daily bars (the trading bottom), W = weekly bars resampled from the same warehouse (the major bottom). A row's headline frame is the daily one unless only the weekly sequence is live or the weekly is further along.",
    "base_rates": "Every sequence detected in the ~5-year window is graded from the tape: +21/+63-bar returns from the trigger fill (raw and stop-managed), stop-hit and target-1-hit rates, three stop rules measured on the same trades (paper / climax-low / 1 ATR), and the paper's counterfactual -- the crowd buying the automatic rally vs the professional waiting for the trigger. The findings block states, in words, where the tape agrees with the paper and where it does not.",
    "stop_rules": "paper = 0.1 ATR under the test low (what the paper prescribes); climax = 0.25 ATR under the lower of the climax low and the test low (the structural floor); wide = 1 ATR under the test low. Each is managed independently on every historical trigger so their stop-hit rates and managed returns are comparable.",
    "confirmations": "Fleet evidence joined by ticker: accumulation-radar, phase-detector, fortress, katlin, 13F institutional flow, insider clusters, dark-pool state, ETF flows. Evidence on the row, never a gate: the tape decides, the fleet confirms.",
}


# ---- handler ------------------------------------------------------------------------------------------------------
def _run(event=None):
    t0 = time.time()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    F = load_feeds()
    uni, keep = build_universe(F["finviz"])
    log("universe: %d instruments (%d stocks, %d etf wrappers)" % (len(uni), sum(1 for v in uni.values() if v[0] == "stock"), sum(1 for v in uni.values() if v[0] == "etf")))
    keys = session_keys(P["sessions"])
    if len(keys) < P["min_sessions"]:
        raise RuntimeError("bar warehouse has %d sessions (<%d)" % (len(keys), P["min_sessions"]))
    dates, bars = load_bars(keys, keep)
    session = dates[-1]
    log("bars loaded: %d sessions %s..%s, %d tickers in %.0fs" % (len(dates), dates[0], dates[-1], len(bars), time.time() - t0))
    rows, hist = [], []
    n_scored = 0
    bench_rows = {}
    for sym, (cls, sub) in uni.items():
        b = bars.get(sym)
        if b is None or len(b.c) < P["min_sessions"]:
            continue
        last = b.c[-1]
        if last < P["min_price"]:
            continue
        adv = mean([b.v[k] * b.c[k] for k in range(max(0, len(b.c) - 20), len(b.c))])
        if adv is None or adv < P["min_adv_usd"]:
            continue
        n_scored += 1
        try:
            r, evD, evW = build_row(sym, cls, sub, b, dates, F, len(dates) - 1)
        except Exception as e:
            row_error(cls, sym, e)
            continue
        desk = DESK.get(sub, "stocks")
        for E in evD:
            hist.append(hist_row(sym, desk, cls, E, "D", P["D"]))
        for E in evW:
            hist.append(hist_row(sym, desk, cls, E, "W", P["W"]))
        if r is not None:
            rows.append(r)
    log("equities/wrappers: %d scored, %d rows with a live or recent sequence, %d historical sequences in %.0fs" % (n_scored, len(rows), len(hist), time.time() - t0))
    # benchmarks (state of the market's own bottom process)
    for bsym in BENCH:
        b = bars.get(bsym)
        if b is None or len(b.c) < P["min_sessions"]:
            continue
        curD, evD, _, _ = run_frame(b, dates, "D", "equity_etfs")
        curW, evW, _, _ = run_frame(b, dates, "W", "equity_etfs")
        bench_rows[bsym] = {"last": rnd(b.c[-1], 2), "daily": (curD or {}).get("state") or "NONE", "weekly": (curW or {}).get("state") or "NONE",
                            "daily_sc_date": dates[b.d[curD["sc_i"]]] if curD else None, "n_sequences_5y": len(evD)}
    # crypto lane (spot, dollar volume, its own calendar)
    try:
        cdates, cbars = load_crypto(today, list(dict.fromkeys(P["crypto_symbols"])))
        for sym, b in cbars.items():
            n_scored += 1
            try:
                r, evD, evW = build_row(sym, "crypto", "crypto", b, cdates, F, len(cdates) - 1)
            except Exception as e:
                row_error("crypto", sym, e)
                continue
            for E in evD:
                hist.append(hist_row(sym, "crypto", "crypto", E, "D", P["D"]))
            for E in evW:
                hist.append(hist_row(sym, "crypto", "crypto", E, "W", P["W"]))
            if r is not None:
                rows.append(r)
        if "BTC" in cbars:
            b = cbars["BTC"]
            curD, evD, _, _ = run_frame(b, cdates, "D", "crypto")
            bench_rows["BTC"] = {"last": rnd(b.c[-1], 0), "daily": (curD or {}).get("state") or "NONE", "weekly": None, "daily_sc_date": cdates[b.d[curD["sc_i"]]] if curD else None, "n_sequences_5y": len(evD)}
    except Exception as e:
        DEGRADED.append("crypto lane failed: %s" % str(e)[:120])
        log("crypto lane failed: %s" % traceback.format_exc()[-400:])
    # ranking: live states first, then score
    order = {"TRIGGERED": 1, "ST_CONFIRMED": 2, "TESTING": 3, "CLIMAX": 4, "MARKUP": 5, "COMPLETED": 6, "STOPPED": 7, "FAILED": 7, "NO_TEST_BREAKOUT": 8, "EXPIRED": 9, "NO_RALLY": 9}
    rows.sort(key=lambda r: (0 if r.get("actionable") else 1, order.get(r["state"], 9), -r["score"], -(r.get("adv_usd") or 0)))
    # the page carries: full board (bounded, charts on the top rows), per-desk boards, weekly board
    board = rows[:P["board_rows"]]
    for r in rows[P["board_rows"]:]:
        r.pop("chart", None)
    by_desk = {}
    for r in rows:
        by_desk.setdefault(r["desk"], []).append(r["ticker"])
    by_desk = {k: v[:P["class_rows"]] for k, v in by_desk.items()}
    weekly_live = [r["ticker"] for r in rows if r.get("weekly_state") in ("ST_CONFIRMED", "TRIGGERED", "MARKUP") or (r["frame"] == "W" and r["state"] in ("ST_CONFIRMED", "TRIGGERED", "MARKUP"))][:120]
    # harvester contract: fresh triggers (<=5 bars) with a real score
    top_picks = [{"ticker": (r["ticker"] + "-USD") if r["asset_class"] == "crypto" else r["ticker"], "score": r["score"], "grade": r["grade"], "state": r["state"], "frame": r["frame"], "asset_class": r["asset_class"], "desk": r["desk"],
                  "entry": (r.get("plan") or {}).get("entry"), "stop": (r.get("plan") or {}).get("stop"), "target_1": (r.get("plan") or {}).get("target_1"), "last": r["last"]}
                 for r in rows if r.get("actionable") and r["state"] == "TRIGGERED" and (r.get("bars_in_state") or 99) <= 5 and r["score"] >= 55][:60]
    # changes vs the prior snapshot
    prev = s3_json(OUT_KEY, {}, quiet=True) or {}
    prev_state = {r.get("ticker"): r.get("state") for r in (prev.get("board_all") or []) if isinstance(r, dict)}
    changes = {"new_triggered": [r["ticker"] for r in rows if r["state"] in ("TRIGGERED", "MARKUP") and prev_state.get(r["ticker"]) not in ("TRIGGERED", "MARKUP")][:40],
               "new_test_confirmed": [r["ticker"] for r in rows if r["state"] == "ST_CONFIRMED" and prev_state.get(r["ticker"]) != "ST_CONFIRMED"][:40],
               "new_climax": [r["ticker"] for r in rows if r["state"] == "CLIMAX" and prev_state.get(r["ticker"]) != "CLIMAX"][:40],
               "new_failed": [r["ticker"] for r in rows if r["state"] in ("FAILED", "STOPPED") and prev_state.get(r["ticker"]) not in ("FAILED", "STOPPED")][:40],
               "prior_session": prev.get("session")}
    br = base_rates(hist)
    out = {"engine": ENGINE, "version": VERSION, "generated_at": now_iso(), "session": session, "elapsed_s": round(time.time() - t0, 1),
           "method": {"source": "Khalid's paper 'How to spot a bottom' (Wyckoff: selling climax -> automatic rally -> secondary test on diminished volume -> trigger above the test candle's high -> stop under the test low)",
                      "frames": ["D", "W"], "params": {"D": P["D"], "W": P["W"]}, "hygiene": {k: P[k] for k in ("min_price", "min_adv_usd", "min_mcap", "min_etf_aum", "min_sessions", "min_weeks")}},
           "universe": {"instruments": len(uni), "scored": n_scored, "rows": len(rows), "sessions": len(dates), "window": [dates[0], dates[-1]], "crypto_symbols": len(P["crypto_symbols"])},
           "market": market_context(rows, bench_rows, F, n_scored),
           "counts": {"by_state": {}, "by_desk": {}, "actionable": sum(1 for r in rows if r.get("actionable"))},
           "climax_gates": {"D": dict(GATE_DIAG["D"]), "W": dict(GATE_DIAG["W"]),
                            "note": "how many candidate bars (>= climax_vol_x volume) each gate rejected over the whole window: range, not a new low of the move, shallow decline, not prolonged, above the average, not in the bottom of the yearly range, not the heaviest volume of the decline, heavy only vs a rising volume regime"},
           "board": board, "board_all": [{k: r.get(k) for k in ("ticker", "company", "asset_class", "sub_class", "desk", "frame", "state", "grade", "score", "last", "sc_date", "st_date", "trigger_date", "st_vol_ratio_sc", "st_depth_class", "bars_in_state", "weekly_state", "daily_state", "n_confirm", "dist_sc_low_pct")} for r in rows],
           "by_desk": by_desk, "desk_labels": DESK_LABEL, "weekly_live": weekly_live, "top_picks": top_picks, "changes": changes,
           "base_rates": br, "definitions": DEFINITIONS, "feeds_asof": F["asof"], "degraded": DEGRADED, "row_errors": ROW_ERRS, "log": LOG[-60:]}
    for r in rows:
        out["counts"]["by_state"][r["state"]] = out["counts"]["by_state"].get(r["state"], 0) + 1
        out["counts"]["by_desk"][r["desk"]] = out["counts"]["by_desk"].get(r["desk"], 0) + 1
    n = s3_put_json(OUT_KEY, out)
    snap = {"session": session, "generated_at": out["generated_at"], "version": VERSION, "board_all": out["board_all"], "top_picks": top_picks, "market": out["market"]["breadth"], "changes": changes}
    s3_put_json(HIST_PREFIX + session + ".json.gz", snap, gz=True)
    log("wrote %s (%d bytes) rows=%d triggered=%d test_confirmed=%d testing=%d climax=%d failed=%d elapsed=%.0fs" % (
        OUT_KEY, n, len(rows), out["counts"]["by_state"].get("TRIGGERED", 0) + out["counts"]["by_state"].get("MARKUP", 0), out["counts"]["by_state"].get("ST_CONFIRMED", 0),
        out["counts"]["by_state"].get("TESTING", 0), out["counts"]["by_state"].get("CLIMAX", 0), out["counts"]["by_state"].get("FAILED", 0), time.time() - t0))
    return {"ok": True, "session": session, "rows": len(rows), "bytes": n, "elapsed_s": out["elapsed_s"], "degraded": DEGRADED}


def lambda_handler(event=None, context=None):
    LOG.clear(); DEGRADED.clear(); ROW_ERRS.clear(); GATE_DIAG["D"].clear(); GATE_DIAG["W"].clear()
    try:
        return _run(event or {})
    except Exception as e:
        log("FATAL %s" % traceback.format_exc()[-1500:])
        raise
