"""
justhodl-narrative-density-tracker v2 — Polygon news edition

The free NewsAPI tier blocked us at 100 requests/day. Polygon news gives us:
  - Up to 200 articles per call
  - Date filtering (published_utc.gte / lte)
  - Ticker filtering
  - Keyword arrays per article
  - Much higher rate limits

STRATEGY:
  1. ONE bulk fetch of last 30 days of news (paginate to get a big sample)
  2. Per article, scan title + description + keywords for theme keywords
  3. Aggregate per-theme counts by date bucket (today / 7d / 30d)
  4. Compute acceleration ratios + density score

This is far more efficient than per-theme search (50 themes × 3 calls = 150 calls
becomes ~5 paginated bulk calls) AND it gives us ticker linkage per article.

OUTPUT: data/narrative-density.json
"""
import io, json, os, time, urllib.request, urllib.error, urllib.parse
from collections import defaultdict
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/narrative-density.json")
POLY_KEY = os.environ.get("POLY_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))
MAX_ARTICLES_PER_PAGE = 200
MAX_PAGES = 30  # ~6000 articles max

S3 = boto3.client("s3", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────
# THEME KEYWORDS — each theme has multiple keyword variants for matching
# Format: (theme_id, display_name, [keyword variants in lowercase], [related tickers])
# ─────────────────────────────────────────────────────────────────────────
THEMES = [
    # AI & Tech infrastructure
    ("ai_general",          "AI / Artificial Intelligence",
     ["artificial intelligence", "generative ai", "ai boom", "ai revolution"],
     ["NVDA","AVGO","MSFT","GOOGL"]),
    ("ai_infrastructure",   "AI Infrastructure",
     ["ai infrastructure", "ai capex", "ai spending"],
     ["AVGO","ANET","VRT","SMCI"]),
    ("ai_optical",          "AI Optical Interconnect",
     ["optical interconnect", "optical networking", "ai optical", "data center networking"],
     ["AAOI","LITE","COHR","CRDO"]),
    ("ai_memory",           "AI Memory / HBM",
     ["hbm memory", "high bandwidth memory", "ai memory", "memory chip"],
     ["MU","SNDK"]),
    ("ai_data_center",      "AI Data Center",
     ["ai data center", "hyperscaler", "data center capacity"],
     ["VRT","ETN","PWR","FIX"]),
    ("ai_chip",             "AI Chip / Semiconductor",
     ["ai chip", "ai accelerator", "ai gpu"],
     ["NVDA","AMD","AVGO","MRVL"]),
    ("llm",                 "Large Language Models",
     ["large language model", "chatgpt", "claude ai", "gemini ai"],
     ["MSFT","GOOGL","META"]),
    ("agentic_ai",          "Agentic AI",
     ["agentic ai", "ai agent", "autonomous ai"],
     ["MSFT","CRM","GOOGL"]),

    # Healthcare / Biotech
    ("glp1",                "GLP-1 / Obesity",
     ["glp-1", "glp1", "weight loss drug", "obesity drug", "ozempic", "wegovy", "mounjaro", "zepbound"],
     ["LLY","NVO"]),
    ("crispr",              "CRISPR / Gene Editing",
     ["crispr", "gene editing", "casgevy"],
     ["CRSP","NTLA","BEAM"]),
    ("alzheimer",           "Alzheimer's drugs",
     ["alzheimer", "leqembi", "kisunla", "donanemab"],
     ["BIIB","LLY"]),
    ("oncology",            "Oncology / cancer therapy",
     ["cancer therapy", "oncology drug", "cancer immunotherapy", "keytruda"],
     ["MRK","BMY","REGN","VRTX"]),

    # Energy
    ("nuclear",             "Nuclear / SMR",
     ["small modular reactor", "smr nuclear", "nuclear power", "nuclear plant"],
     ["CCJ","UEC","UUUU","DNN"]),
    ("uranium",             "Uranium",
     ["uranium price", "uranium supply", "uranium mining"],
     ["CCJ","UEC","UUUU"]),
    ("lithium",             "Lithium / battery",
     ["lithium price", "lithium battery", "lithium supply", "battery metals"],
     ["ALB","SQM","LIT"]),
    ("hydrogen",            "Hydrogen",
     ["hydrogen energy", "hydrogen fuel", "hydrogen production", "green hydrogen"],
     ["PLUG","BLDP","BE"]),
    ("clean_energy",        "Clean Energy",
     ["clean energy", "renewable energy", "solar power", "wind power"],
     ["ENPH","FSLR","RUN"]),
    ("oil_supply",          "Oil supply / OPEC",
     ["opec", "oil supply", "oil production cut"],
     ["XOM","CVX","COP"]),
    ("natural_gas_lng",     "Natural Gas / LNG",
     ["lng export", "natural gas", "gas liquefied"],
     ["LNG","CHK","EQT"]),

    # Tech themes
    ("quantum",             "Quantum Computing",
     ["quantum computing", "quantum computer", "quantum supremacy"],
     ["IBM","RGTI","IONQ"]),
    ("robotics",            "Robotics / Humanoid",
     ["humanoid robot", "industrial robot", "robotic"],
     ["TSLA","ABB","FANUY"]),
    ("autonomous",          "Autonomous Vehicles",
     ["autonomous vehicle", "self-driving", "robotaxi", "waymo"],
     ["TSLA","GOOGL"]),
    ("cybersecurity",       "Cybersecurity",
     ["cybersecurity", "ransomware attack", "data breach", "cyber attack"],
     ["CRWD","PANW","ZS","FTNT"]),
    ("blockchain",          "Blockchain / DeFi",
     ["blockchain", "decentralized finance", "defi"],
     ["COIN","MSTR"]),
    ("crypto_general",      "Cryptocurrency",
     ["bitcoin", "btc etf", "ethereum", "crypto rally", "crypto regulation"],
     ["COIN","MSTR","IBIT"]),

    # Macro themes
    ("china_decoupling",    "China decoupling / Taiwan",
     ["china decoupling", "taiwan tension", "chip ban china"],
     ["INTC","TSM"]),
    ("tariff_trade",        "Tariff / Trade War",
     ["tariff", "trade war", "trade dispute"],
     ["AAPL","FCX"]),
    ("reshoring",           "Reshoring / Onshoring",
     ["reshoring", "onshoring", "supply chain reshore"],
     ["INTC","TSM","CAT","DE"]),
    ("rate_cut",            "Rate Cuts / Fed Pivot",
     ["rate cut", "fed pivot", "fed easing", "fomc"],
     ["XLF","XLY","TLT"]),
    ("recession",           "Recession",
     ["recession", "economic slowdown", "yield curve inversion"],
     ["XLP","XLU","TLT"]),
    ("inflation_macro",     "Inflation",
     ["inflation report", "cpi report", "core inflation"],
     ["TLT","TIP","GLD"]),

    # Defense / geopolitics
    ("defense",             "Defense / military spending",
     ["defense spending", "military budget", "pentagon contract", "weapons procurement"],
     ["LMT","RTX","NOC","GD"]),
    ("ukraine_war",         "Ukraine conflict",
     ["ukraine war", "russia sanctions", "ukraine military aid"],
     ["LMT","RTX"]),
    ("middle_east",         "Middle East tensions",
     ["israel hamas", "iran tension", "middle east conflict", "houthi"],
     ["XOM","XLE","LMT"]),
    ("space",               "Space / satellite",
     ["satellite launch", "space industry", "spacex", "rocket launch"],
     ["RKLB","SPCE"]),

    # Industrial / Materials
    ("rare_earth",          "Rare Earth metals",
     ["rare earth", "ree supply", "rare earth mining"],
     ["MP","REMX"]),
    ("copper",              "Copper supply",
     ["copper price", "copper supply", "copper mining"],
     ["FCX","SCCO","COPX"]),
    ("gold",                "Gold / safe haven",
     ["gold price", "gold rally", "gold record high"],
     ["GLD","GDX","NEM"]),
    ("water",               "Water scarcity",
     ["water scarcity", "drought", "water rights"],
     ["PHO","XYL"]),
    ("electric_grid",       "Electric grid / transmission",
     ["power grid", "grid modernization", "transmission line", "electricity demand"],
     ["PWR","ETN","ABB"]),

    # Real Estate / Housing
    ("housing",             "Housing market",
     ["housing market", "home prices", "mortgage rate"],
     ["XHB","KBH","DHI","LEN"]),
    ("data_center_reit",    "Data Center REITs",
     ["data center reit", "equinix", "digital realty"],
     ["EQIX","DLR"]),
    ("commercial_real_estate","Commercial Real Estate",
     ["commercial real estate", "office reit", "office vacancy"],
     ["VNQ","SPG"]),

    # Consumer / Demographic
    ("aging_demographics",  "Aging / Demographics",
     ["aging population", "silver economy", "boomer retirement"],
     ["AGNG"]),
    ("luxury_consumer",     "Luxury consumer",
     ["luxury goods", "lvmh", "hermes"],
     ["RACE"]),
    ("ev_demand",           "EV demand",
     ["electric vehicle demand", "ev adoption", "ev sales"],
     ["TSLA","RIVN"]),
    ("ai_layoffs",          "AI Layoffs",
     ["ai layoff", "ai job cut", "ai automation jobs"],
     []),

    # Misc / themed
    ("alternative_proteins","Alternative proteins",
     ["plant-based meat", "alternative protein", "lab grown"],
     []),
    ("regenerative_med",    "Regenerative medicine",
     ["regenerative medicine", "stem cell therapy"],
     []),
    ("fintech_disrupt",     "Fintech disruption",
     ["fintech disruption", "neobank", "embedded finance"],
     ["SQ","HOOD","SOFI"]),
    ("gaming",              "Gaming",
     ["video game", "gaming industry", "esports"],
     ["EA","TTWO","ATVI"]),
    ("water_treatment",     "Water treatment infrastructure",
     ["water treatment", "wastewater", "water utility"],
     ["AWK","WTRG","PHO"]),
    ("supply_chain_software","Supply Chain software",
     ["supply chain software", "logistics tech", "scm platform"],
     ["MANH"]),
]


def fetch_news_page(published_gte=None, cursor=None):
    """Fetch one page of Polygon news. Returns (articles, next_cursor)."""
    if cursor:
        # cursor is a full URL, just append apiKey
        url = cursor + "&apiKey=" + POLY_KEY
    else:
        params = ["limit=" + str(MAX_ARTICLES_PER_PAGE), "order=desc", "sort=published_utc"]
        if published_gte:
            params.append("published_utc.gte=" + published_gte)
        url = "https://api.polygon.io/v2/reference/news?" + "&".join(params) + "&apiKey=" + POLY_KEY
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Narrative/2.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
            articles = d.get("results", []) or []
            next_url = d.get("next_url")
            return articles, next_url
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print("[narrative] Polygon rate limit hit")
        return [], None
    except Exception as e:
        print("[narrative] fetch error: " + str(e))
        return [], None


def article_text(article):
    """Concatenate searchable text from one article."""
    parts = [
        (article.get("title") or "").lower(),
        (article.get("description") or "").lower(),
    ]
    keywords = article.get("keywords") or []
    if isinstance(keywords, list):
        parts.append(" ".join((k or "").lower() for k in keywords))
    insights = article.get("insights") or []
    if isinstance(insights, list):
        for ins in insights:
            if isinstance(ins, dict):
                parts.append((ins.get("sentiment_reasoning") or "").lower())
    return " ".join(parts)


def article_date_str(article):
    """Get YYYY-MM-DD from published_utc."""
    pub = article.get("published_utc") or ""
    return pub[:10]


def keyword_match(text, variants):
    """Return number of variants that appear in text."""
    matches = 0
    for v in variants:
        if v in text:
            matches += 1
    return matches


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print("[narrative-v2] starting v2.0, " + str(len(THEMES)) + " themes")

    # Fetch news from last 30 days
    thirty_days_ago = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 30 * 86400))
    articles_all = []
    cursor = None
    page = 0
    while page < MAX_PAGES:
        if time.time() > deadline_at:
            break
        if cursor:
            articles, cursor = fetch_news_page(cursor=cursor)
        else:
            articles, cursor = fetch_news_page(published_gte=thirty_days_ago)
        if not articles:
            break
        articles_all.extend(articles)
        page += 1
        if not cursor:
            break
    print("[narrative-v2] fetched " + str(len(articles_all)) + " articles across " + str(page) + " pages")

    if not articles_all:
        return {"statusCode": 200, "body": json.dumps({"n": 0, "reason": "no articles"})}

    # Build date buckets
    today_str = time.strftime("%Y-%m-%d")
    yesterday_str = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 86400))
    seven_days_ago_str = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 7 * 86400))

    # Aggregate per theme with date bucketing
    theme_counts = {}
    theme_samples = defaultdict(list)
    for theme_id, name, variants, tickers in THEMES:
        theme_counts[theme_id] = {
            "today": 0,
            "yesterday": 0,
            "last_7d": 0,
            "last_30d": 0,
            "ticker_co_mentions": defaultdict(int),
        }

    for art in articles_all:
        text = article_text(art)
        if not text:
            continue
        date_str = article_date_str(art)
        art_tickers = art.get("tickers") or []

        for theme_id, name, variants, _tickers in THEMES:
            if keyword_match(text, variants) > 0:
                tc = theme_counts[theme_id]
                tc["last_30d"] += 1
                if date_str >= seven_days_ago_str:
                    tc["last_7d"] += 1
                if date_str == today_str or date_str == yesterday_str:
                    tc["today"] += 1
                # Co-mention with tickers
                for tk in art_tickers:
                    tc["ticker_co_mentions"][tk] += 1
                if len(theme_samples[theme_id]) < 5:
                    theme_samples[theme_id].append({
                        "title": (art.get("title") or "")[:160],
                        "publisher": ((art.get("publisher") or {}).get("name") or "")[:60],
                        "published": date_str,
                        "tickers": art_tickers[:6],
                    })

    # Compute density score per theme
    results = []
    for theme_id, name, variants, tickers in THEMES:
        tc = theme_counts[theme_id]
        n_today = tc["today"]
        n_7d = tc["last_7d"]
        n_30d = tc["last_30d"]

        # Average per day
        avg_per_day_30d = n_30d / 30
        avg_per_day_7d_excl_today = (n_7d - n_today) / 6 if n_7d > n_today else 0

        # Acceleration ratios
        accel_today_vs_7d = n_today / max(avg_per_day_7d_excl_today, 0.5)
        accel_7d_vs_30d = (n_7d / 7) / max(avg_per_day_30d, 0.3) if avg_per_day_30d > 0 else 0

        # Score 0-100
        score = 0
        flags = []

        if accel_today_vs_7d > 5:
            score += 35
            flags.append("TODAY_5X_BASELINE")
        elif accel_today_vs_7d > 3:
            score += 25
            flags.append("TODAY_3X_BASELINE")
        elif accel_today_vs_7d > 2:
            score += 15
            flags.append("TODAY_2X_BASELINE")
        elif accel_today_vs_7d > 1.5:
            score += 8

        if accel_7d_vs_30d > 3:
            score += 25
            flags.append("7D_3X_30D_BASELINE")
        elif accel_7d_vs_30d > 2:
            score += 18
            flags.append("7D_2X_30D_BASELINE")
        elif accel_7d_vs_30d > 1.5:
            score += 10

        if n_30d >= 200:
            score += 20
            flags.append("HIGH_VOLUME_30D")
        elif n_30d >= 80:
            score += 12
            flags.append("MED_VOLUME_30D")
        elif n_30d >= 30:
            score += 6
        elif n_30d >= 10:
            score += 3

        if n_today >= 20:
            score += 15
            flags.append("HIGH_TODAY")
        elif n_today >= 8:
            score += 8

        score = min(score, 100)

        if score >= 65:
            tier = "TIER_A_HOT"
        elif score >= 45:
            tier = "TIER_B_BUILDING"
        elif score >= 25:
            tier = "WATCH"
        else:
            tier = "QUIET"

        # Top tickers co-mentioned
        top_tickers = sorted(tc["ticker_co_mentions"].items(), key=lambda x: -x[1])[:8]

        results.append({
            "theme_id": theme_id,
            "name": name,
            "tickers": tickers,
            "score": round(score, 1),
            "tier": tier,
            "flags": flags,
            "metrics": {
                "n_today": n_today,
                "n_7d": n_7d,
                "n_30d": n_30d,
                "avg_per_day_7d_excl_today": round(avg_per_day_7d_excl_today, 2),
                "avg_per_day_30d": round(avg_per_day_30d, 2),
                "accel_today_vs_7d": round(accel_today_vs_7d, 2),
                "accel_7d_vs_30d": round(accel_7d_vs_30d, 2),
            },
            "top_co_mentioned_tickers": [{"ticker": t, "n": n} for t, n in top_tickers],
            "sample_titles": theme_samples[theme_id][:3],
        })

    results.sort(key=lambda x: -x["score"])

    tier_a = [r for r in results if r["tier"] == "TIER_A_HOT"]
    tier_b = [r for r in results if r["tier"] == "TIER_B_BUILDING"]

    out = {
        "schema_version": 2,
        "method": "narrative_density_polygon_v2",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_articles_total": len(articles_all),
            "n_themes_total": len(THEMES),
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
            "pages_fetched": page,
        },
        "summary": {
            "top_15_themes": results[:15],
            "tier_a": [r["theme_id"] for r in tier_a],
        },
        "all_themes": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[narrative-v2] wrote " + str(len(body)) + "b")
    if results[:5]:
        print("[narrative-v2] TOP: " + str([(r["theme_id"], r["score"], r["tier"]) for r in results[:5]]))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_articles": len(articles_all),
            "n_themes": len(results),
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
            "duration_s": out["duration_s"],
        }),
    }
