"""
justhodl-theme-classifier
══════════════════════════
Auto-discovers ACTIVE THEMES from the momentum-leaders universe.

THE INSIGHT
═══════════
Themes can't be hardcoded — they rotate. AI semis are hot now; 6 months
ago it was obesity pharma; 18 months ago it was nuclear/SMR. A static
list goes stale.

Instead, we derive themes DYNAMICALLY from co-movement:
  1. Take top 30 by momentum_score from momentum-leaders
  2. Fetch industry classification for each (FMP /stable/profile)
  3. Group by industry
  4. An industry is an ACTIVE THEME if ≥3 momentum leaders share it
  5. Label themes by industry name + dominant sub-themes

When the cluster composition shifts (MRVL drops, KLAC enters), themes
update automatically without code changes.

WHY INDUSTRY (not correlation)
══════════════════════════════
Correlation-based clustering needs 90+ days of overlapping price data
for every pair — expensive and noisy. Industry classification from FMP
is one API call per ticker, gives clean, interpretable buckets, and
correlation within a hot industry is almost always high anyway. We get
90% of the value at 5% of the cost.

OUTPUT
══════
data/themes.json
{
  "schema_version": "1.0",
  "generated_at":   "...",
  "n_momentum_leaders": 30,
  "n_active_themes":    4,
  "themes": {
    "Semiconductors": {
      "tickers":         ["NVDA","AMD","AVGO","MRVL","MU","ARM","SNDK"],
      "n_leaders":       7,
      "avg_momentum":    84.3,
      "label":           "AI semis",
      "is_active":       true
    },
    "Software—Application": {...},
    "Pharmaceuticals—Major":  {...},
  },
  "ticker_to_theme": {
    "NVDA": "Semiconductors",
    "AMD":  "Semiconductors",
    ...
  },
  "all_industries_seen": [...]
}

SCHEDULE
════════
cron(0 */6 * * ? *) — every 6 hours. Themes change slowly; no need to
                       re-cluster every minute.
"""
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET   = "justhodl-dashboard-live"
MOMENTUM_KEY = "data/momentum-leaders.json"
PROFILE_CACHE_KEY = "data/_cache/ticker-profiles.json"
OUTPUT_KEY  = "data/themes.json"
FMP_KEY     = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

MIN_TICKERS_FOR_THEME = 3   # need ≥3 momentum leaders in an industry to call it a theme
TOP_N_LEADERS          = 30  # how many momentum leaders to classify
PROFILE_CACHE_TTL_DAYS = 7   # industry classifications change slowly

s3 = boto3.client("s3", region_name="us-east-1")

# Human-readable theme aliases for common industries
THEME_ALIASES = {
    "Semiconductors":               "AI semis",
    "Software—Application":         "AI software",
    "Software—Infrastructure":      "AI infrastructure",
    "Information Technology Services": "IT services",
    "Computer Hardware":            "Compute hardware",
    "Drug Manufacturers—General":   "Big pharma",
    "Drug Manufacturers—Specialty & Generic": "Specialty pharma",
    "Biotechnology":                "Biotech",
    "Medical Devices":              "Medtech",
    "Healthcare Plans":             "Health insurance",
    "Banks—Diversified":            "Big banks",
    "Banks—Regional":               "Regional banks",
    "Capital Markets":              "Investment banks",
    "Insurance—Diversified":        "Insurance",
    "Asset Management":             "Asset managers",
    "Copper":                       "Copper miners",
    "Gold":                         "Gold miners",
    "Silver":                       "Silver miners",
    "Other Industrial Metals & Mining": "Industrial metals",
    "Steel":                        "Steel",
    "Oil & Gas E&P":                "Oil & gas",
    "Oil & Gas Integrated":         "Oil majors",
    "Oil & Gas Refining & Marketing": "Refiners",
    "Uranium":                      "Uranium",
    "Solar":                        "Solar",
    "Utilities—Renewable":          "Renewables",
    "Utilities—Regulated Electric": "Utilities",
    "Aerospace & Defense":          "Aerospace & defense",
    "Auto Manufacturers":           "Autos",
    "Auto Parts":                   "Auto parts",
    "REIT—Industrial":              "Industrial REITs",
    "REIT—Specialty":               "Specialty REITs",
    "REIT—Office":                  "Office REITs",
    "Entertainment":                "Entertainment / streaming",
    "Internet Retail":              "E-commerce",
    "Internet Content & Information": "Internet platforms",
    "Telecom Services":             "Telecom",
    "Communication Equipment":      "Comms equipment",
    "Restaurants":                  "Restaurants",
    "Apparel Retail":               "Apparel retail",
    "Specialty Retail":             "Specialty retail",
    "Lodging":                      "Hotels / lodging",
    "Travel Services":              "Travel",
    "Airlines":                     "Airlines",
    "Cybersecurity":                "Cybersecurity",
    "Security & Protection Services": "Security",
}


def load_s3_json(key: str) -> Optional[dict]:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[load] {key}: {str(e)[:120]}")
        return None


def load_profile_cache() -> Dict[str, dict]:
    """Load cached industry classifications (refreshed weekly)."""
    cached = load_s3_json(PROFILE_CACHE_KEY) or {}
    profiles = cached.get("profiles", {})
    cache_ts = cached.get("generated_at", "")
    # Check freshness — if older than 7 days, treat as empty (forces refetch)
    try:
        cache_age_days = (datetime.now(timezone.utc) -
                            datetime.fromisoformat(cache_ts.replace("Z", "+00:00"))).days
        if cache_age_days > PROFILE_CACHE_TTL_DAYS:
            print(f"[cache] {cache_age_days} days old — refreshing")
            return {}
    except Exception:
        return {}
    return profiles


def save_profile_cache(profiles: Dict[str, dict]) -> None:
    body = json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profiles": profiles,
    }, default=str)
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=PROFILE_CACHE_KEY, Body=body,
                        ContentType="application/json")
    except Exception as e:
        print(f"[cache-save] {e}")


def fetch_profile(ticker: str) -> Optional[dict]:
    """Fetch sector + industry from FMP /stable/profile."""
    try:
        url = f"https://financialmodelingprep.com/stable/profile?symbol={ticker}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/themes"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        item = data[0] if isinstance(data, list) and data else data
        if not isinstance(item, dict):
            return None
        return {
            "sector":    item.get("sector") or "Unknown",
            "industry":  item.get("industry") or "Unknown",
            "company":   item.get("companyName") or "",
            "market_cap": item.get("marketCap"),
        }
    except Exception as e:
        print(f"[profile] {ticker}: {str(e)[:80]}")
        return None


def derive_theme_label(industry: str, tickers: List[str]) -> str:
    """Map raw industry name to a human-friendly theme label."""
    alias = THEME_ALIASES.get(industry)
    if alias:
        return alias
    # Strip "—" suffixes for cleaner display
    if "—" in industry:
        return industry.split("—")[0].strip()
    return industry


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[themes] start {datetime.now(timezone.utc).isoformat()}")

    # Load momentum leaders
    mom_doc = load_s3_json(MOMENTUM_KEY)
    if not mom_doc:
        return _write_error("No momentum-leaders.json available")

    leaders_raw = (mom_doc.get("leaders") or mom_doc.get("all_scored") or [])[:TOP_N_LEADERS]
    if not leaders_raw:
        return _write_error("No leaders in momentum data")

    leader_tickers = [l["ticker"] for l in leaders_raw if l.get("ticker")]
    leader_scores  = {l["ticker"]: l.get("momentum_score", 0) for l in leaders_raw}
    print(f"[themes] {len(leader_tickers)} momentum leaders to classify")

    # Load profile cache + identify which need fetching
    cache = load_profile_cache()
    to_fetch = [t for t in leader_tickers if t not in cache]
    print(f"[themes] cache hit: {len(leader_tickers) - len(to_fetch)} · need fetch: {len(to_fetch)}")

    # Fetch missing profiles in parallel
    if to_fetch:
        with ThreadPoolExecutor(max_workers=6) as ex:
            futures = {ex.submit(fetch_profile, t): t for t in to_fetch}
            for fut in as_completed(futures, timeout=60):
                t = futures[fut]
                try:
                    p = fut.result()
                    if p:
                        cache[t] = p
                except Exception as e:
                    print(f"[fetch] {t}: {e}")
        # Save updated cache
        save_profile_cache(cache)

    # Group by industry
    industry_buckets: Dict[str, List[str]] = {}
    industry_scores: Dict[str, List[float]] = {}
    ticker_to_industry: Dict[str, str] = {}
    all_industries = set()
    unknown_tickers = []

    for t in leader_tickers:
        p = cache.get(t, {})
        ind = p.get("industry", "Unknown")
        all_industries.add(ind)
        if ind == "Unknown":
            unknown_tickers.append(t)
            continue
        industry_buckets.setdefault(ind, []).append(t)
        industry_scores.setdefault(ind, []).append(leader_scores.get(t, 0))
        ticker_to_industry[t] = ind

    # Build themes (industries with ≥3 leaders)
    themes = {}
    ticker_to_theme: Dict[str, str] = {}
    for ind, tickers in industry_buckets.items():
        n = len(tickers)
        is_active = n >= MIN_TICKERS_FOR_THEME
        if not is_active:
            continue  # not a theme yet
        avg_score = sum(industry_scores[ind]) / max(1, n)
        # Sort tickers within theme by momentum_score desc
        sorted_tickers = sorted(tickers, key=lambda t: -leader_scores.get(t, 0))
        label = derive_theme_label(ind, sorted_tickers)
        themes[ind] = {
            "tickers":      sorted_tickers,
            "n_leaders":    n,
            "avg_momentum": round(avg_score, 2),
            "label":        label,
            "is_active":    True,
            "top_ticker":   sorted_tickers[0] if sorted_tickers else None,
            "top_score":    round(leader_scores.get(sorted_tickers[0], 0), 1) if sorted_tickers else 0,
        }
        for t in sorted_tickers:
            ticker_to_theme[t] = ind

    # Sort themes by n_leaders × avg_momentum (strongest themes first)
    sorted_theme_keys = sorted(themes.keys(),
                                key=lambda k: -(themes[k]["n_leaders"] * themes[k]["avg_momentum"]))
    ordered_themes = {k: themes[k] for k in sorted_theme_keys}

    output = {
        "schema_version":     "1.0",
        "generated_at":       datetime.now(timezone.utc).isoformat(),
        "elapsed_sec":        round(time.time() - t0, 2),
        "n_momentum_leaders": len(leader_tickers),
        "n_active_themes":    len(themes),
        "n_classified":       len(ticker_to_industry),
        "n_unknown":          len(unknown_tickers),
        "min_tickers_for_theme": MIN_TICKERS_FOR_THEME,
        "themes":             ordered_themes,
        "ticker_to_theme":    ticker_to_theme,
        "unclassified":       sorted(unknown_tickers),
        "all_industries_seen": sorted(all_industries),
        "metadata": {
            "top_n_leaders_used":   TOP_N_LEADERS,
            "cache_ttl_days":       PROFILE_CACHE_TTL_DAYS,
            "n_profiles_fetched":   len(to_fetch),
            "n_profiles_cached":    len(leader_tickers) - len(to_fetch),
        },
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
                    ContentType="application/json", CacheControl="max-age=900")

    summary = {
        "status":          "ok",
        "elapsed_sec":     output["elapsed_sec"],
        "n_classified":    output["n_classified"],
        "n_active_themes": output["n_active_themes"],
        "themes_summary":  [
            f"{themes[k]['label']} ({themes[k]['n_leaders']} names, avg mom {themes[k]['avg_momentum']})"
            for k in sorted_theme_keys[:5]
        ],
    }
    print(f"[themes] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {"schema_version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "error", "error": message, **extras}
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception: pass
    print(f"[themes] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
