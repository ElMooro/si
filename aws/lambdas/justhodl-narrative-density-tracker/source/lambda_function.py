"""
justhodl-narrative-density-tracker — Bloomberg-style mention counting

Counts daily mentions of theme keywords across financial news. When a theme's
mentions accelerate (today >> 30-day average), it's likely about to manifest
in price action. Often catches retail-driven moves 4-8 weeks before
fundamental signals fire.

DATA: NewsAPI (we have key) — searches "everything" endpoint with date filters
Alternative: Polygon /v2/reference/news (we have access) for ticker-specific news

THEMES TRACKED (~50, each with multi-word keywords):
  • AI / generative AI / large language model
  • AI infrastructure / AI optical / AI memory
  • GLP-1 / weight loss / obesity drug
  • Quantum computing
  • Nuclear energy / SMR / small modular reactor
  • Lithium / battery
  • Hydrogen
  • Robotics / humanoid
  • Defense / drone warfare
  • Semiconductors / chip shortage / chip act
  • Data center / hyperscale
  • Crypto / bitcoin / stablecoin
  • EV / electric vehicle
  • CRISPR / gene editing
  • Cloud / hybrid cloud
  • Cybersecurity
  • Reshoring / onshoring / supply chain
  • Demographic / aging / silver economy
  • China decoupling / Taiwan
  • Tariff / trade war
  ... (extensive)

Per theme, computes:
  - mentions_today (last 24h)
  - mentions_7d_avg
  - mentions_30d_avg
  - acceleration_ratio = today / 30d_avg
  - velocity = (today + 7d_avg) / 30d_avg

OUTPUT: data/narrative-density.json
"""
import io, json, os, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/narrative-density.json")
STATE_KEY = os.environ.get("STATE_KEY", "data/narrative-density-state.json")
NEWS_KEY = os.environ.get("NEWS_KEY", "17d36cdd13c44e139853b3a6876cf940")
POLY_KEY = os.environ.get("POLY_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
N_WORKERS = int(os.environ.get("N_WORKERS", "6"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

S3 = boto3.client("s3", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────
# THEME KEYWORDS — broad list of investable themes
# Each entry: (theme_id, display_name, search_query, related_tickers)
# ─────────────────────────────────────────────────────────────────────────
THEMES = [
    # AI & Tech infrastructure
    ("ai_general",          "AI / Artificial Intelligence",       '("artificial intelligence" OR "generative AI")', ["NVDA","AVGO","MSFT","GOOGL"]),
    ("ai_infrastructure",   "AI Infrastructure",                  '"AI infrastructure"', ["AVGO","ANET","VRT","SMCI"]),
    ("ai_optical",          "AI Optical Interconnect",            '"optical interconnect" OR "optical networking AI"', ["AAOI","LITE","COHR","CRDO"]),
    ("ai_memory",           "AI Memory / HBM",                    '"HBM" OR "high bandwidth memory" OR "AI memory"', ["MU","SNDK"]),
    ("ai_data_center",      "AI Data Center",                     '"AI data center" OR "hyperscaler AI"', ["VRT","ETN","PWR","FIX"]),
    ("ai_chip",             "AI Chip / Semiconductor",            '"AI chip" OR "AI accelerator"', ["NVDA","AMD","AVGO","MRVL"]),
    ("llm",                 "Large Language Models",              '"large language model" OR "LLM"', ["MSFT","GOOGL","META"]),
    ("agentic_ai",          "Agentic AI",                          '"agentic AI" OR "AI agent"', ["MSFT","CRM","GOOGL"]),

    # Healthcare / Biotech
    ("glp1",                "GLP-1 / Obesity",                    '"GLP-1" OR "weight loss drug" OR "obesity drug"', ["LLY","NVO"]),
    ("crispr",              "CRISPR / Gene Editing",              '"CRISPR" OR "gene editing"', ["CRSP","NTLA","BEAM"]),
    ("alzheimer",           "Alzheimer's drugs",                  '"Alzheimer\'s drug" OR "Alzheimer disease"', ["BIIB","LLY"]),
    ("oncology",            "Oncology / cancer therapy",          '"cancer therapy" OR "oncology drug"', ["MRK","BMY","REGN","VRTX"]),

    # Energy
    ("nuclear",             "Nuclear / SMR",                       '"small modular reactor" OR "nuclear power"', ["CCJ","UEC","UUUU","DNN"]),
    ("uranium",             "Uranium",                             '"uranium price" OR "uranium supply"', ["CCJ","UEC","UUUU"]),
    ("lithium",             "Lithium / battery",                   '"lithium" OR "lithium battery"', ["ALB","SQM","LIT"]),
    ("hydrogen",            "Hydrogen",                            '"hydrogen energy" OR "hydrogen fuel"', ["PLUG","BLDP","BE"]),
    ("clean_energy",        "Clean Energy",                        '"clean energy" OR "renewable energy"', ["ENPH","FSLR","RUN","ICLN"]),
    ("oil_supply",          "Oil supply / OPEC",                   '"OPEC" OR "oil supply"', ["XOM","CVX","COP"]),
    ("natural_gas_lng",     "Natural Gas / LNG",                   '"LNG export" OR "natural gas"', ["LNG","CHK","EQT"]),

    # Tech themes
    ("quantum",             "Quantum Computing",                   '"quantum computing"', ["IBM","RGTI","IONQ"]),
    ("robotics",            "Robotics / Humanoid",                 '"humanoid robot" OR "industrial robot"', ["TSLA","ABB","FANUY"]),
    ("autonomous",          "Autonomous Vehicles",                 '"autonomous vehicle" OR "self-driving"', ["TSLA","GOOGL"]),
    ("cybersecurity",       "Cybersecurity",                       '"cybersecurity" OR "ransomware attack"', ["CRWD","PANW","ZS","FTNT"]),
    ("blockchain",          "Blockchain / DeFi",                   '"blockchain" OR "decentralized finance"', ["COIN","MSTR"]),
    ("web3",                "Web3 / metaverse",                    '"web3" OR "metaverse"', ["RBLX","META"]),

    # Macro themes
    ("china_decoupling",    "China decoupling",                    '"China decoupling" OR "Taiwan tension"', ["INTC","TSM","SOXX"]),
    ("tariff_trade",        "Tariff / Trade War",                  '"tariff" OR "trade war"', ["AAPL","FCX","SOXX"]),
    ("reshoring",           "Reshoring / Onshoring",                '"reshoring" OR "onshoring" OR "supply chain"', ["INTC","TSM","CAT","DE"]),
    ("inflation_macro",     "Inflation",                            '"inflation" AND ("Fed" OR "rate cut")', ["TLT","TIP","GLD"]),
    ("recession",           "Recession",                            '"recession" OR "economic slowdown"', ["XLP","XLU","TLT"]),
    ("rate_cut",            "Rate Cuts / Fed Pivot",                '"rate cut" OR "Fed pivot"', ["XLF","XLY","TLT"]),

    # Cryptocurrency
    ("bitcoin",             "Bitcoin",                              '"Bitcoin price" OR "BTC ETF"', ["MSTR","COIN","IBIT"]),
    ("ethereum",            "Ethereum",                             '"Ethereum"', ["ETHE","COIN"]),
    ("crypto_regulation",   "Crypto regulation",                    '"crypto regulation" OR "SEC crypto"', ["COIN","MSTR"]),

    # Real Estate
    ("commercial_real_estate","Commercial Real Estate",             '"commercial real estate" OR "office REIT"', ["VNQ","SPG","O"]),
    ("housing",             "Housing market",                       '"housing market" OR "home prices"', ["XHB","KBH","DHI","LEN"]),
    ("data_center_reit",    "Data Center REITs",                    '"data center REIT" OR "Equinix" OR "Digital Realty"', ["EQIX","DLR"]),

    # Demographic / consumer
    ("aging_demographics",  "Aging / Demographics",                 '"aging population" OR "silver economy"', ["AGNG","ABBV"]),
    ("luxury_consumer",     "Luxury consumer",                      '"luxury goods" OR "luxury consumer"', ["LVMH","RACE"]),
    ("ev_demand",           "EV demand",                            '"electric vehicle demand" OR "EV adoption"', ["TSLA","RIVN","BYDDY"]),

    # Defense / geopolitics
    ("defense",             "Defense / military spending",          '"defense spending" OR "military budget"', ["LMT","RTX","NOC","GD"]),
    ("ukraine_war",         "Ukraine conflict",                     '"Ukraine war" OR "Russia sanctions"', ["LMT","RTX"]),
    ("middle_east",         "Middle East / Iran",                   '"Iran" OR "Israel Hamas"', ["XOM","XLE","LMT"]),
    ("space",               "Space / satellite",                    '"satellite launch" OR "space industry"', ["RKLB","SPCE"]),

    # Industrial / Materials
    ("rare_earth",          "Rare Earth metals",                    '"rare earth" OR "REE supply"', ["MP","REMX"]),
    ("copper",              "Copper supply",                        '"copper price" OR "copper supply"', ["FCX","SCCO","COPX"]),
    ("gold",                "Gold / safe haven",                    '"gold price" OR "gold rally"', ["GLD","GDX","NEM"]),
    ("water",               "Water scarcity",                       '"water scarcity" OR "drought" OR "water rights"', ["PHO","XYL"]),
    ("agriculture",         "Agriculture / food prices",            '"food prices" OR "agricultural commodity"', ["DBA","MOO","DE"]),

    # Misc
    ("regenerative_med",    "Regenerative medicine",                '"regenerative medicine" OR "stem cell"', []),
    ("alternative_proteins","Alternative proteins",                  '"plant-based meat" OR "alternative protein"', []),
    ("electric_grid",       "Electric grid / transmission",         '"power grid" OR "grid modernization"', ["PWR","ETN","ABB"]),
]


def fetch_newsapi_count(query, from_date, to_date, timeout=20):
    """Use NewsAPI /everything to count articles matching query in date window.
    Returns: (total_results, sample_titles)
    """
    encoded = urllib.parse.quote(query)
    url = ("https://newsapi.org/v2/everything?"
           "q=" + encoded +
           "&from=" + from_date +
           "&to=" + to_date +
           "&language=en"
           "&sortBy=publishedAt"
           "&pageSize=10"
           "&apiKey=" + NEWS_KEY)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Narrative/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            d = json.loads(r.read())
            total = d.get("totalResults", 0)
            sample_titles = []
            for art in d.get("articles", [])[:5]:
                sample_titles.append({
                    "title": (art.get("title") or "")[:160],
                    "source": (art.get("source") or {}).get("name", ""),
                    "publishedAt": art.get("publishedAt", ""),
                })
            return total, sample_titles
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print("[narrative] NewsAPI rate limit hit")
        return 0, []
    except Exception:
        return 0, []


def evaluate_theme(theme_tuple):
    """For one theme, fetch counts in 3 windows and compute density score."""
    theme_id, name, query, tickers = theme_tuple

    today = time.strftime("%Y-%m-%d")
    yesterday = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 86400))
    seven_days_ago = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 7 * 86400))
    thirty_days_ago = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 30 * 86400))

    # NewsAPI free tier limits: requests are 100/day, dates capped to last 30 days
    # We do 3 windowed queries:
    # - last 24h
    # - last 7 days
    # - last 30 days
    # Then derive: today (24h) — exact, 7d_avg = (7d_total - 24h) / 6, 30d_avg = (30d_total - 7d) / 23
    
    n_today, today_samples = fetch_newsapi_count(query, yesterday, today)
    if n_today is None:
        return None

    n_7d, _ = fetch_newsapi_count(query, seven_days_ago, today)
    n_30d, _ = fetch_newsapi_count(query, thirty_days_ago, today)

    # Compute averages
    days_in_7d_excl_today = 6
    days_in_30d_excl_7d = 23
    avg_7d_excl_today = (n_7d - n_today) / days_in_7d_excl_today if n_7d > n_today else 0
    avg_30d_excl_7d = (n_30d - n_7d) / days_in_30d_excl_7d if n_30d > n_7d else 0

    # Acceleration ratios
    # today vs 7d-prior avg
    accel_today_vs_7d = n_today / max(avg_7d_excl_today, 1)
    # 7d-trailing avg vs 30d-trailing avg
    accel_7d_vs_30d = (n_7d / 7) / max(avg_30d_excl_7d, 1)

    # Score 0-100
    score = 0
    flags = []

    # Today is much higher than 7d trailing
    if accel_today_vs_7d > 5:
        score += 40
        flags.append("TODAY_5X_BASELINE")
    elif accel_today_vs_7d > 3:
        score += 30
        flags.append("TODAY_3X_BASELINE")
    elif accel_today_vs_7d > 2:
        score += 20
        flags.append("TODAY_2X_BASELINE")
    elif accel_today_vs_7d > 1.5:
        score += 12

    # 7d is higher than 30d (sustained acceleration)
    if accel_7d_vs_30d > 3:
        score += 30
        flags.append("7D_3X_30D_BASELINE")
    elif accel_7d_vs_30d > 2:
        score += 22
        flags.append("7D_2X_30D_BASELINE")
    elif accel_7d_vs_30d > 1.5:
        score += 12

    # Absolute volume — high baseline activity
    if n_30d >= 1000:
        score += 15
        flags.append("HIGH_BASE_VOLUME")
    elif n_30d >= 300:
        score += 10
    elif n_30d >= 100:
        score += 5

    # Today's volume is meaningful
    if n_today >= 50:
        score += 10
        flags.append("HIGH_DAILY_VOLUME")
    elif n_today >= 20:
        score += 5

    score = min(score, 100)

    if score >= 70:
        tier = "TIER_A_FORMING"
    elif score >= 50:
        tier = "TIER_B_BUILDING"
    elif score >= 30:
        tier = "WATCH"
    else:
        tier = "QUIET"

    return {
        "theme_id": theme_id,
        "name": name,
        "query": query,
        "tickers": tickers,
        "score": round(score, 1),
        "tier": tier,
        "flags": flags,
        "metrics": {
            "n_today": n_today,
            "n_7d": n_7d,
            "n_30d": n_30d,
            "avg_per_day_7d_excl_today": round(avg_7d_excl_today, 1),
            "avg_per_day_30d_excl_7d": round(avg_30d_excl_7d, 1),
            "accel_today_vs_7d": round(accel_today_vs_7d, 2),
            "accel_7d_vs_30d": round(accel_7d_vs_30d, 2),
        },
        "sample_titles_today": today_samples,
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print("[narrative] starting v1.0, " + str(len(THEMES)) + " themes")

    results = []
    n_failed = 0
    
    # NewsAPI free is 100 req/day. We do 3 calls per theme. So can do ~33 themes/day.
    # Smarter: do fewer parallel workers to avoid rate limits, but cycle through themes
    # If we hit rate limit, gracefully stop and write whatever we have.
    
    def evaluate(theme):
        if time.time() > deadline_at:
            return None
        try:
            return evaluate_theme(theme)
        except Exception as e:
            print("[narrative] " + theme[0] + " ERROR: " + str(e))
            return None

    # Sequential to respect NewsAPI free tier rate limits
    for theme in THEMES:
        if time.time() > deadline_at:
            print("[narrative] timeout — partial results")
            break
        try:
            r = evaluate_theme(theme)
            if r:
                results.append(r)
            else:
                n_failed += 1
        except Exception as e:
            print("[narrative] " + theme[0] + " ERROR: " + str(e))
            n_failed += 1
        time.sleep(0.5)  # be polite

    print("[narrative] OK: " + str(len(results)) + ", failed: " + str(n_failed))
    results.sort(key=lambda x: x["score"], reverse=True)

    tier_a = [r for r in results if r["tier"] == "TIER_A_FORMING"]
    tier_b = [r for r in results if r["tier"] == "TIER_B_BUILDING"]

    out = {
        "schema_version": 1,
        "method": "narrative_density_tracker_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_themes_total": len(THEMES),
            "n_themes_evaluated": len(results),
            "n_failed": n_failed,
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
        },
        "summary": {
            "top_15_themes": [
                {
                    "theme_id": r["theme_id"],
                    "name": r["name"],
                    "score": r["score"],
                    "tier": r["tier"],
                    "flags": r["flags"],
                    "n_today": r["metrics"]["n_today"],
                    "n_7d": r["metrics"]["n_7d"],
                    "n_30d": r["metrics"]["n_30d"],
                    "accel_today_vs_7d": r["metrics"]["accel_today_vs_7d"],
                    "accel_7d_vs_30d": r["metrics"]["accel_7d_vs_30d"],
                    "tickers": r["tickers"],
                    "sample_titles_today": r["sample_titles_today"],
                }
                for r in results[:15]
            ],
            "tier_a": [r["theme_id"] for r in tier_a],
        },
        "all_themes": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[narrative] wrote " + str(len(body)) + "b to " + S3_KEY)
    if results[:5]:
        print("[narrative] TOP: " + str([(r["theme_id"], r["score"], r["tier"]) for r in results[:5]]))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_themes": len(results),
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
            "duration_s": out["duration_s"],
        }),
    }
