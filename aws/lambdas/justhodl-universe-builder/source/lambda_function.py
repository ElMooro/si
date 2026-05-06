"""
justhodl-universe-builder v3 — comprehensive multi-cap universe.

Pulls stocks across ALL 6 cap buckets via FMP /company-screener:
  • nano    $5M-$50M       (typically 300-500 real stocks)
  • micro   $50M-$300M     (~480 real stocks)
  • small   $300M-$2B      (~470 real stocks)
  • mid     $2B-$10B       (~550 real stocks)
  • large   $10B-$200B     (~510 real stocks)
  • mega    >$200B         (~65 real stocks)

Filters out:
  • Asset management / mutual funds / ETFs / closed-end funds
  • Shell companies / blank checks / SPACs
  • Preferred shares / warrants / units (symbols with -, /, .)
  • Foreign-domiciled (only US country)
  • Non-listed exchanges (must be NYSE / NASDAQ / AMEX)

Tags each stock with cap_bucket for downstream cap-aware filtering.

Also seeds a curated AI-supply-chain microcap list manually (so even if
the screener misses some, we keep coverage of names like AAOI, AXTI etc).

OUTPUT: data/universe.json with shape:
  {
    "generated_at": "...",
    "stats": {
      "total_stocks": 2400+,
      "by_cap_bucket": {"nano": ..., "micro": ..., ...}
    },
    "stocks": [
      {"symbol": "X", "name": "...", "sector": "...", "industry": "...",
       "exchange": "...", "market_cap": ..., "price": ..., 
       "cap_bucket": "small", "source": "screener"},
      ...
    ]
  }
"""
import io, json, os, time, urllib.request, urllib.error, re
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/universe.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

S3 = boto3.client("s3", region_name=REGION)

# Cap buckets (low_inclusive, high_exclusive)
CAP_BUCKETS = [
    ("nano",    5_000_000,         50_000_000),
    ("micro",   50_000_000,        300_000_000),
    ("small",   300_000_000,       2_000_000_000),
    ("mid",     2_000_000_000,     10_000_000_000),
    ("large",   10_000_000_000,    200_000_000_000),
    ("mega",    200_000_000_000,   100_000_000_000_000),  # 100T as upper
]

# Industries to EXCLUDE (these are funds, asset managers, shells, ETFs)
EXCLUDE_INDUSTRIES = {
    "asset management", "asset management - bonds",
    "asset management - global", "asset management - leveraged",
    "asset management - income", "asset management - growth",
    "asset management - hybrid",
    "shell companies",
    "closed-end fund - equity", "closed-end fund - bond",
    "closed-end fund - debt", "closed-end fund - foreign",
    "closed-end fund - global", "closed-end fund - hybrid",
    "exchange traded fund",
}

# Symbols ending in these are typically preferreds/warrants/units (skip)
EXCLUDE_SYMBOL_SUFFIX = (
    "-PA", "-PB", "-PC", "-PD", "-PE", "-PF", "-PG", "-PH", "-PI", "-PJ", "-PK",
    "-PR", "-PRA", "-PRB", "-PRC", "-PRD", "-PRE", "-PRF",
    "-WS", "-WT", "-W", "-U", "-UN",
)

# Curated AI supply-chain microcap seed (so we never miss these)
CURATED_SEED = [
    # Optical / interconnect / AI substrate
    "AAOI", "LITE", "COHR", "VIAV", "FN", "INFN", "OCC", "POET",
    # AI memory / storage
    "MU", "SNDK", "WDC", "STX",
    # Semi test / inspection / tools (microcap to small)
    "AEHR", "ONTO", "FORM", "ACLS", "ICHR", "UCTT", "ENTG", "CAMT", "AMBA",
    # Compound / specialty semis
    "AXTI", "WOLF", "QRVO", "SWKS", "MTSI", "INDI", "ALGM",
    # AI silicon / chips
    "AVGO", "NVDA", "AMD", "QCOM", "MRVL", "ARM", "INTC", "CRDO",
    # AI infrastructure / DC / connectivity
    "ANET", "VRT", "PSTG", "NTAP", "DELL", "SMCI", "HPE",
    # Cybersecurity (often AI-driven now)
    "CRWD", "ZS", "PANW", "FTNT", "OKTA", "NET",
    # Hydrogen / energy infra
    "FCEL", "PLUG", "BE", "BLDP",
    # Biotech inflection candidates
    "AGIO", "BCRX", "BPMC", "AGEN", "CDNA", "EXAS", "INCY",
    # Renewables / EV picks-and-shovels
    "ALB", "ENPH", "FSLR", "SEDG", "RUN",
    # AI-adjacent industrials / robotics
    "ISRG", "TER", "FIX",
    # Crypto-equity
    "COIN", "MSTR", "HOOD", "MARA", "RIOT", "CLSK",
    # Defense / govtech
    "PLTR", "RKLB", "AVAV", "KTOS",
]


def fetch_url(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Universe/3.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def fetch_screener_bucket(bucket_name, low, high):
    """Fetch all stocks for a cap bucket from FMP screener."""
    url = ("https://financialmodelingprep.com/stable/company-screener?"
           "marketCapMoreThan=" + str(low) +
           "&marketCapLowerThan=" + str(high) +
           "&isActivelyTrading=true"
           "&country=US"
           "&exchange=NYSE,NASDAQ,AMEX"
           "&limit=1000"
           "&apikey=" + FMP_KEY)
    try:
        data = fetch_url(url, timeout=30)
        if not isinstance(data, list):
            return []
        return [{**d, "_cap_bucket": bucket_name} for d in data]
    except Exception as e:
        print("[universe] bucket " + bucket_name + " failed: " + str(e))
        return []


def fetch_quote_for_seed(symbol):
    """For curated seed, fetch quote to get current mcap + price."""
    try:
        url = "https://financialmodelingprep.com/stable/quote?symbol=" + symbol + "&apikey=" + FMP_KEY
        data = fetch_url(url, timeout=10)
        if isinstance(data, list) and data:
            return data[0]
    except Exception:
        pass
    return None


def fetch_profile_for_seed(symbol):
    """For curated seed, fetch profile to get sector/industry."""
    try:
        url = "https://financialmodelingprep.com/stable/profile?symbol=" + symbol + "&apikey=" + FMP_KEY
        data = fetch_url(url, timeout=10)
        if isinstance(data, list) and data:
            return data[0]
    except Exception:
        pass
    return None


def is_valid_common_stock(stock):
    """Return True if this is a real common stock (not fund/preferred/warrant)."""
    sym = (stock.get("symbol") or "").upper()
    if not sym:
        return False
    
    # Filter symbols with special suffixes (preferreds, warrants, units)
    for suf in EXCLUDE_SYMBOL_SUFFIX:
        if sym.endswith(suf):
            return False
    
    # Filter symbols with dots/slashes (rare common shares but most are warrants)
    if "." in sym or "/" in sym:
        # Allow BRK.B, BRK.A, RDS.A type structures (real common with dual class)
        if not (len(sym) <= 5 and sym.split(".")[0] in ("BRK", "BF", "GEF", "MOG", "BIO")):
            return False
    
    industry = (stock.get("industry") or "").lower().strip()
    if industry in EXCLUDE_INDUSTRIES:
        return False
    if "fund" in industry and "fundamental" not in industry:
        return False
    if industry.startswith("etf"):
        return False
    
    # Filter "Trust" / "Fund" in company name (common for closed-end funds)
    name = (stock.get("companyName") or "").lower()
    if any(kw in name for kw in [" fund", "etf", "trust series", "spdr", "ishares", "vanguard", "invesco bullet", "schwab "]):
        # Allow REITs which have "Trust" 
        if not any(reit_kw in (industry or "") for reit_kw in ["reit", "real estate"]):
            return False
    
    # Must have valid market cap
    mc = stock.get("marketCap")
    if not mc or mc <= 0:
        return False
    
    return True


def lambda_handler(event=None, context=None):
    started = time.time()
    print("[universe] v3.0 starting — full multi-cap")

    # Fetch all 6 cap buckets in parallel
    all_raw = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(fetch_screener_bucket, name, lo, hi): (name, lo, hi)
                   for name, lo, hi in CAP_BUCKETS}
        for f in as_completed(futures):
            name, lo, hi = futures[f]
            data = f.result()
            all_raw[name] = data
            print("[universe] " + name + " ($" + str(lo // 1_000_000) +
                  "M-): raw=" + str(len(data)))

    # Combine + dedupe (mid/large can return same stock if mcap crosses bucket boundary)
    by_symbol = {}
    for bucket_name, stocks in all_raw.items():
        for s in stocks:
            sym = (s.get("symbol") or "").upper()
            if not sym:
                continue
            if sym in by_symbol:
                # Keep the one with higher mcap (more recent/accurate)
                if (s.get("marketCap") or 0) > (by_symbol[sym].get("marketCap") or 0):
                    by_symbol[sym] = s
            else:
                by_symbol[sym] = s

    print("[universe] combined: " + str(len(by_symbol)) + " unique symbols")

    # Filter to real common stocks
    valid_stocks = []
    n_filtered_industry = 0
    n_filtered_symbol = 0
    n_filtered_other = 0
    for sym, s in by_symbol.items():
        if not is_valid_common_stock(s):
            sym_lower = sym.lower()
            industry_lower = (s.get("industry") or "").lower().strip()
            if any(suf for suf in EXCLUDE_SYMBOL_SUFFIX if sym.endswith(suf)) or "." in sym or "/" in sym:
                n_filtered_symbol += 1
            elif industry_lower in EXCLUDE_INDUSTRIES or "fund" in industry_lower:
                n_filtered_industry += 1
            else:
                n_filtered_other += 1
            continue
        valid_stocks.append(s)

    print("[universe] valid: " + str(len(valid_stocks)) + 
          " (filtered industry: " + str(n_filtered_industry) +
          ", symbol: " + str(n_filtered_symbol) +
          ", other: " + str(n_filtered_other) + ")")

    # Add curated seed (with deduplication)
    existing_syms = {(s.get("symbol") or "").upper() for s in valid_stocks}
    seed_to_add = [s for s in CURATED_SEED if s not in existing_syms]
    print("[universe] curated seed missing from screener: " + str(len(seed_to_add)) + 
          " of " + str(len(CURATED_SEED)))

    seed_added = 0
    if seed_to_add:
        with ThreadPoolExecutor(max_workers=10) as pool:
            quote_futures = {pool.submit(fetch_quote_for_seed, sym): sym for sym in seed_to_add}
            profile_futures = {pool.submit(fetch_profile_for_seed, sym): sym for sym in seed_to_add}
            quotes = {sym: f.result() for f, sym in quote_futures.items()}
            profiles = {sym: f.result() for f, sym in profile_futures.items()}
        for sym in seed_to_add:
            q = quotes.get(sym)
            p = profiles.get(sym)
            if not q:
                continue
            mc = q.get("marketCap") or 0
            # Determine bucket
            cap_bucket = "unknown"
            for name, lo, hi in CAP_BUCKETS:
                if lo <= mc < hi:
                    cap_bucket = name
                    break
            stock = {
                "symbol": sym,
                "companyName": q.get("name") or (p or {}).get("companyName"),
                "marketCap": mc,
                "price": q.get("price"),
                "exchange": q.get("exchange"),
                "industry": (p or {}).get("industry"),
                "sector": (p or {}).get("sector"),
                "_cap_bucket": cap_bucket,
                "_source": "curated_seed",
            }
            valid_stocks.append(stock)
            seed_added += 1

    print("[universe] curated seed added: " + str(seed_added))

    # Build final stock objects
    final_stocks = []
    by_bucket = {name: 0 for name, _, _ in CAP_BUCKETS}
    by_bucket["unknown"] = 0
    by_sector = {}
    
    for s in valid_stocks:
        sym = (s.get("symbol") or "").upper()
        mc = s.get("marketCap") or 0
        cap_bucket = s.get("_cap_bucket") or "unknown"
        # Re-verify bucket from market cap (to override any stale tagging)
        for name, lo, hi in CAP_BUCKETS:
            if lo <= mc < hi:
                cap_bucket = name
                break
        
        sector = s.get("sector") or "Unknown"
        by_sector[sector] = by_sector.get(sector, 0) + 1
        by_bucket[cap_bucket] = by_bucket.get(cap_bucket, 0) + 1
        
        final_stocks.append({
            "symbol": sym,
            "name": s.get("companyName") or "",
            "sector": sector,
            "industry": s.get("industry") or "Unknown",
            "exchange": s.get("exchange") or "",
            "market_cap": mc,
            "price": s.get("price"),
            "cap_bucket": cap_bucket,
            "source": s.get("_source") or "screener",
        })

    # Sort by mcap descending so consumers get most-liquid first
    final_stocks.sort(key=lambda x: -(x.get("market_cap") or 0))

    out = {
        "schema_version": 3,
        "method": "universe_builder_v3_multicap",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "total_stocks": len(final_stocks),
            "by_cap_bucket": by_bucket,
            "by_sector_top_10": dict(sorted(by_sector.items(), key=lambda x: -x[1])[:10]),
            "n_curated_seed_added": seed_added,
        },
        "cap_buckets": [
            {"name": name, "low": lo, "high": hi}
            for name, lo, hi in CAP_BUCKETS
        ],
        "stocks": final_stocks,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[universe] wrote " + str(len(body)) + "b")
    print("[universe] by_bucket: " + json.dumps(by_bucket))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "total_stocks": len(final_stocks),
            "by_bucket": by_bucket,
            "duration_s": out["duration_s"],
        }),
    }
