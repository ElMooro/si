"""
justhodl-gdelt-sentiment — GDELT 2.0 global news + geopolitical events

GDELT 2.0 is a free, open data project that monitors print, broadcast, and
web news from nearly every country every 15 minutes. We pull the latest
"GKG" (Global Knowledge Graph) and "Events" feeds, filter for
financially-relevant entries (themes like ECON_*, MARKETS_*, US_FED, etc.),
extract sentiment + entity mentions, and roll into per-asset signals.

Endpoints used (all free, no key required):
  https://data.gdeltproject.org/gdeltv2/lastupdate.txt
  https://data.gdeltproject.org/gdeltv2/{ts}.gkg.csv.zip      (every 15 min)
  https://data.gdeltproject.org/gdeltv2/{ts}.export.CSV.zip   (events)

GKG schema reference: https://www.gdeltproject.org/data.html#documentation

Output (data/gdelt-news.json):
  {
    "generated_at": ISO8601,
    "gdelt_timestamp": "20260427183000",   (last GDELT 15-min batch)
    "stats": {
       "articles": int,                    (total in batch)
       "financial_articles": int,          (filtered)
       "avg_tone": float,                  (-100 to +100, GDELT scale)
       "extreme_negative": int,            (tone < -5, count)
       "extreme_positive": int,            (tone > +5, count)
    },
    "themes": {                            (top mentioned themes)
       "ECON_INFLATION": {count, avg_tone},
       "US_FED": {count, avg_tone}, ...
    },
    "asset_sentiment": {
       "SPY":  {mentions, avg_tone, headlines: [...]},
       "BTC":  {...},
       "GOLD": {...}, ...
    },
    "geopolitical_events": [
       {actor, action, country, headline, tone, ts}, ...
    ],
    "headlines_by_tone": {
       "most_negative": [{headline, source, tone}, ...],
       "most_positive": [...],
    }
  }
"""
from __future__ import annotations
import csv
import io
import json
import os
import ssl
import time
import urllib.request
import urllib.error
import zipfile
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/gdelt-news.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")

# GDELT serves over http (no auth); the https cert often has hostname
# mismatch errors from CDN edges. Use http to avoid SSL issues; data is
# public and unauthenticated either way.
GDELT_BASE = "http://data.gdeltproject.org/gdeltv2"

# Themes we consider financially relevant (subset of GDELT taxonomy)
FINANCIAL_THEMES = frozenset([
    "ECON_INFLATION", "ECON_INTEREST_RATES", "ECON_RECESSION", "ECON_DEBT",
    "ECON_FINANCIAL_CRISIS", "ECON_BANKRUPTCY", "ECON_HOUSING", "ECON_BANK",
    "ECON_CENTRAL_BANK", "ECON_QUANTITATIVE_EASING", "ECON_TRADE",
    "ECON_TARIFF", "ECON_SANCTIONS", "ECON_DEFLATION", "ECON_STAGFLATION",
    "MARKETS_BULL", "MARKETS_BEAR", "MARKETS_VOLATILITY",
    "US_FED", "US_TREASURY", "EU_ECB", "JAPAN_BOJ", "UK_BOE",
    "CRYPTOCURRENCY", "BITCOIN", "BLOCKCHAIN", "STABLECOIN",
    "EARNINGS", "IPO", "MERGER_ACQUISITION", "STOCK_BUYBACK",
    "ENERGY_OIL", "ENERGY_GAS", "ENERGY_RENEWABLE",
    "GEOPOLITICAL_TENSION", "TRADE_WAR", "WAR_DECLARATION",
])

# Asset → keyword/theme regex tags
ASSET_TAGS = {
    "SPY":  ["S&P", "S&P 500", "SP500", "SPY", "WALL STREET", "STOCK MARKET"],
    "QQQ":  ["NASDAQ", "TECH STOCKS", "TECHNOLOGY SECTOR"],
    "DJI":  ["DOW JONES", "DOW INDUSTRIAL"],
    "BTC":  ["BITCOIN", "BTC", "CRYPTOCURRENCY"],
    "ETH":  ["ETHEREUM", "ETH "],
    "GOLD": ["GOLD PRICE", "GOLD MARKET", "PRECIOUS METAL"],
    "OIL":  ["CRUDE OIL", "OIL PRICE", "WTI", "BRENT"],
    "USD":  ["DOLLAR INDEX", "DXY", "U.S. DOLLAR"],
    "BONDS": ["TREASURY", "10-YEAR YIELD", "BOND MARKET", "FIXED INCOME"],
}


def _fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def _latest_batch_timestamp() -> str:
    """Read GDELT's lastupdate.txt → most recent 15-min batch timestamp."""
    text = _fetch(f"{GDELT_BASE}/lastupdate.txt").decode("utf-8")
    # Each line: SIZE_BYTES MD5 URL
    # We want the GKG (knowledge graph) entry
    for line in text.splitlines():
        parts = line.strip().split()
        if len(parts) >= 3 and "gkg.csv.zip" in parts[-1]:
            url = parts[-1]
            # extract timestamp like 20260427183000
            ts = url.split("/")[-1].split(".")[0]
            return ts
    raise RuntimeError("Could not parse GDELT lastupdate.txt")


def _fetch_csv_zip(url: str):
    """Download a GDELT zipped CSV and yield decoded rows."""
    raw = _fetch(url)
    z = zipfile.ZipFile(io.BytesIO(raw))
    name = z.namelist()[0]
    text = z.read(name).decode("utf-8", errors="ignore")
    return list(csv.reader(io.StringIO(text), delimiter="\t"))


# ─── GKG (Global Knowledge Graph) parsing ─────────────────────────────────
# GKG 2.0 schema (as of 2015+): tab-delimited, 27 columns
# 0:  GKGRECORDID
# 1:  V2.1DATE (yyyymmddhhmmss)
# 2:  V2SOURCECOLLECTIONIDENTIFIER
# 3:  V2SOURCECOMMONNAME
# 4:  V2DOCUMENTIDENTIFIER (URL)
# 5:  V1COUNTS (count of mentions of various themes)
# 6:  V2.1COUNTS
# 7:  V1THEMES (semicolons-separated)
# 8:  V2ENHANCEDTHEMES
# 9:  V1LOCATIONS
# 10: V2ENHANCEDLOCATIONS
# 11: V1PERSONS
# 12: V2ENHANCEDPERSONS
# 13: V1ORGANIZATIONS
# 14: V2ENHANCEDORGANIZATIONS
# 15: V1.5TONE  (avgtone, posscore, negscore, polarity, ardActiv, selfRef, wordcount)
# 16: V2.1ENHANCEDDATES
# ...
# 26: V2.1QUOTATIONS

def parse_gkg(rows):
    """Yields parsed GKG records."""
    for row in rows:
        if len(row) < 16:
            continue
        try:
            ts_raw = row[1]
            url = row[4]
            source = row[3]
            themes = [t for t in (row[7] or "").split(";") if t]
            persons = [p for p in (row[11] or "").split(";") if p]
            orgs = [o for o in (row[13] or "").split(";") if o]
            locations = (row[9] or "")
            tone_parts = (row[15] or "").split(",")
            tone = float(tone_parts[0]) if tone_parts and tone_parts[0] else 0.0
            yield {
                "ts": ts_raw,
                "url": url,
                "source": source,
                "themes": themes,
                "persons": persons,
                "orgs": orgs,
                "locations_raw": locations,
                "tone": tone,
                "headline": _headline_from_url(url),
            }
        except Exception:
            continue


def _headline_from_url(url: str) -> str:
    """Best-effort headline extraction from URL slug."""
    if not url:
        return ""
    slug = url.rstrip("/").split("/")[-1]
    # Strip extensions and IDs
    slug = slug.split("?")[0].split("#")[0]
    for ext in (".html", ".htm", ".php", ".aspx"):
        if slug.endswith(ext):
            slug = slug[:-len(ext)]
    # Replace separators with spaces
    slug = slug.replace("-", " ").replace("_", " ")
    # Drop pure numeric IDs
    parts = [w for w in slug.split() if not w.isdigit()]
    return " ".join(parts).strip()[:160]


def filter_financial(records):
    """Keep only records with at least one financial theme."""
    out = []
    for r in records:
        if any(t in FINANCIAL_THEMES for t in r["themes"]):
            out.append(r)
    return out


def aggregate_themes(records):
    counts = Counter()
    tones = defaultdict(list)
    for r in records:
        for t in r["themes"]:
            if t in FINANCIAL_THEMES:
                counts[t] += 1
                tones[t].append(r["tone"])
    return {
        t: {
            "count": counts[t],
            "avg_tone": round(sum(tones[t]) / len(tones[t]), 2) if tones[t] else 0.0,
        }
        for t in counts
    }


def aggregate_assets(records):
    out = {}
    for asset, kws in ASSET_TAGS.items():
        matched = []
        for r in records:
            blob = " ".join([
                r["headline"].upper(),
                " ".join(r["orgs"])[:500].upper(),
                r["url"].upper(),
            ])
            if any(kw in blob for kw in kws):
                matched.append(r)
        if not matched:
            continue
        tones = [m["tone"] for m in matched]
        out[asset] = {
            "mentions": len(matched),
            "avg_tone": round(sum(tones) / len(tones), 2),
            "headlines": [
                {"headline": m["headline"][:140], "source": m["source"], "tone": round(m["tone"], 2), "url": m["url"]}
                for m in sorted(matched, key=lambda x: -abs(x["tone"]))[:5]
            ],
        }
    return out


def extreme_headlines(records, n=8):
    sorted_records = sorted(records, key=lambda r: r["tone"])
    return {
        "most_negative": [
            {"headline": r["headline"][:140], "source": r["source"], "tone": round(r["tone"], 2), "url": r["url"]}
            for r in sorted_records[:n] if r["headline"]
        ],
        "most_positive": [
            {"headline": r["headline"][:140], "source": r["source"], "tone": round(r["tone"], 2), "url": r["url"]}
            for r in sorted_records[-n:][::-1] if r["headline"]
        ],
    }


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    try:
        ts = _latest_batch_timestamp()
        gkg_url = f"{GDELT_BASE}/{ts}.gkg.csv.zip"
        rows = _fetch_csv_zip(gkg_url)
    except Exception as e:
        return {"statusCode": 502, "body": json.dumps({"error": f"GDELT fetch failed: {e}"})}

    all_records = list(parse_gkg(rows))
    fin_records = filter_financial(all_records)

    tones = [r["tone"] for r in fin_records if r["tone"] is not None]
    avg_tone = round(sum(tones) / len(tones), 2) if tones else 0.0
    extreme_neg = sum(1 for t in tones if t < -5)
    extreme_pos = sum(1 for t in tones if t > 5)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "gdelt_timestamp": ts,
        "stats": {
            "articles_total": len(all_records),
            "financial_articles": len(fin_records),
            "avg_tone": avg_tone,
            "extreme_negative": extreme_neg,
            "extreme_positive": extreme_pos,
            "fetch_duration_s": round(time.time() - started, 1),
        },
        "themes": aggregate_themes(fin_records),
        "asset_sentiment": aggregate_assets(fin_records),
        "headlines_by_tone": extreme_headlines(fin_records),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY,
        Body=json.dumps(output).encode(),
        ContentType="application/json", CacheControl="no-cache",
    )
    print(f"Wrote GDELT batch {ts} → {len(fin_records)} financial articles, avg tone {avg_tone}")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "stats": output["stats"]}),
    }
