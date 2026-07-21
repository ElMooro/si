"""justhodl-geopolitical-risk v1.0 — a country geopolitical-stress engine.

Inspired by World Monitor's country stress index (koala73/worldmonitor,
AGPL-3.0), reimplemented JustHodl-native. We do NOT run their app or lift
code — we reuse the *facts* (a curated list of public RSS URLs) as a corpus,
and compute an original news-flow risk score wired into our macro fleet.

Per country: news-flow VELOCITY (articles/day across the geopolitics corpus
mentioning the country) + CRISIS-TERM intensity (conflict/sanction/strike
lexicon) + 20d z-score of both vs the country's own history → a 0-100
geopolitical stress score, escalation deltas, and top headlines. Feeds
data/geopolitical-risk.json. Escalation emits to macro-leads + defcon +
morning-intelligence. stdlib-only; caches history; never fabricates.
"""
import io
import json
import os
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.0.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/geopolitical-risk.json"
HIST_KEY = "geo/geopolitical-risk-history.json"
UA = {"User-Agent": "JustHodl research admin@justhodl.ai"}
S3 = boto3.client("s3", region_name="us-east-1")

_HERE = os.path.dirname(__file__)
with open(os.path.join(_HERE, "geo_feeds.json")) as _f:
    CORPUS = json.load(_f)

# Countries we track (aligned with GSSI sovereign set + macro relevance),
# each with match aliases. Score is per-country news-flow risk.
COUNTRIES = {
    "US": ["united states", "u.s.", "washington", "white house", "pentagon"],
    "China": ["china", "beijing", "chinese", "xi jinping"],
    "Russia": ["russia", "moscow", "kremlin", "putin"],
    "Iran": ["iran", "tehran", "iranian"],
    "Israel": ["israel", "israeli", "netanyahu", "gaza", "idf"],
    "Ukraine": ["ukraine", "kyiv", "kiev", "zelensky"],
    "Taiwan": ["taiwan", "taipei", "taiwanese"],
    "N.Korea": ["north korea", "pyongyang", "dprk", "kim jong"],
    "S.Korea": ["south korea", "seoul"],
    "Japan": ["japan", "tokyo", "japanese"],
    "India": ["india", "new delhi", "modi"],
    "Pakistan": ["pakistan", "islamabad"],
    "Saudi": ["saudi", "riyadh"],
    "Turkey": ["turkey", "turkiye", "ankara", "erdogan"],
    "Germany": ["germany", "berlin", "german"],
    "France": ["france", "paris", "french", "macron"],
    "UK": ["united kingdom", "britain", "london", "u.k.", "downing street"],
    "Venezuela": ["venezuela", "caracas", "maduro"],
    "Syria": ["syria", "damascus", "syrian"],
    "Lebanon": ["lebanon", "beirut", "hezbollah"],
    "Egypt": ["egypt", "cairo"],
    "Yemen": ["yemen", "houthi", "sanaa"],
}

CRISIS_TERMS = re.compile(
    r"\b(war|invasion|invade|strike|airstrike|missile|attack|sanction|"
    r"conflict|troops|military|nuclear|coup|assassinat|escalat|ceasefire|"
    r"casualt|killed|bombing|offensive|retaliat|blockade|incursion|"
    r"warhead|deployment|mobiliz|shelling|drone strike)\b", re.I)

MARKET_TERMS = re.compile(
    r"\b(default|devalu|capital control|bond|yield|currency|inflation|"
    r"central bank|rate hike|recession|debt crisis|imf|bailout)\b", re.I)


def _fetch(url, timeout=12):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=UA),
                                   timeout=timeout)
        return r.read(600_000)
    except Exception:
        return b""


def _parse_items(xml_bytes):
    """RSS/Atom → [(title, pubdate_epoch_or_None)]. Tolerant."""
    out = []
    if not xml_bytes:
        return out
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        # strip bad bytes and retry once
        try:
            root = ET.fromstring(re.sub(rb"[\x00-\x08\x0b\x0c\x0e-\x1f]", b"",
                                        xml_bytes))
        except Exception:
            return out
    for it in root.iter():
        tag = it.tag.lower().rsplit("}", 1)[-1]
        if tag not in ("item", "entry"):
            continue
        title = None
        dt = None
        for ch in it:
            ct = ch.tag.lower().rsplit("}", 1)[-1]
            if ct == "title" and ch.text:
                title = ch.text.strip()
            elif ct in ("pubdate", "published", "updated") and ch.text:
                dt = _parse_date(ch.text.strip())
        if title:
            out.append((title, dt))
    return out


def _parse_date(s):
    for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            d = datetime.strptime(s.replace("GMT", "+0000"), fmt)
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            return d.timestamp()
        except Exception:
            continue
    return None


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=2)).timestamp()

    # 1) pull the geopolitics + analysis + crisis + security corpus
    feeds = []
    for bucket in ("geopolitics", "analysis", "crisis", "security",
                   "official", "policy"):
        feeds += CORPUS.get(bucket, [])
    # dedupe by url, cap for runtime
    seen = set()
    feeds = [f for f in feeds if not (f["url"] in seen or seen.add(f["url"]))]

    all_titles = []
    feeds_ok = 0
    for f in feeds[:120]:
        items = _parse_items(_fetch(f["url"]))
        if items:
            feeds_ok += 1
        for title, dt in items:
            if dt is None or dt >= cutoff:
                all_titles.append((title, dt if dt else now.timestamp(),
                                   f["name"]))
    n_articles = len(all_titles)

    # 2) score each country by mentions + crisis intensity in the last 24h/48h
    day_ago = (now - timedelta(hours=24)).timestamp()
    countries = {}
    for cc, aliases in COUNTRIES.items():
        pat = re.compile(r"\b(" + "|".join(re.escape(a) for a in aliases) + r")\b", re.I)
        hits_24 = hits_48 = crisis_hits = market_hits = 0
        heads = []
        for title, ts, src in all_titles:
            if not pat.search(title):
                continue
            hits_48 += 1
            if ts >= day_ago:
                hits_24 += 1
            crisis = bool(CRISIS_TERMS.search(title))
            if crisis:
                crisis_hits += 1
            if MARKET_TERMS.search(title):
                market_hits += 1
            if len(heads) < 6 and (crisis or ts >= day_ago):
                heads.append({"title": title[:150], "src": src,
                              "crisis": crisis})
        # raw velocity (per day) + crisis share
        velocity = hits_48 / 2.0
        crisis_share = (crisis_hits / hits_48) if hits_48 else 0.0
        countries[cc] = {
            "mentions_24h": hits_24, "mentions_48h": hits_48,
            "velocity_per_day": round(velocity, 1),
            "crisis_hits": crisis_hits, "market_hits": market_hits,
            "crisis_share": round(crisis_share, 2),
            "headlines": heads,
        }

    # 3) history → 20d z-scores + raw 0-100 composite
    try:
        hist = json.loads(S3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
    except Exception:
        hist = {"days": {}}
    today = now.strftime("%Y-%m-%d")
    hist["days"][today] = {cc: {"v": countries[cc]["velocity_per_day"],
                                "c": countries[cc]["crisis_hits"]}
                           for cc in countries}
    # keep 90d
    keys = sorted(hist["days"])[-90:]
    hist["days"] = {k: hist["days"][k] for k in keys}

    def _z(series, x):
        if len(series) < 5:
            return 0.0
        m = sum(series) / len(series)
        var = sum((s - m) ** 2 for s in series) / len(series)
        sd = var ** 0.5
        return round((x - m) / sd, 2) if sd > 1e-9 else 0.0

    for cc in countries:
        vser = [hist["days"][k].get(cc, {}).get("v", 0) for k in keys[:-1]]
        cser = [hist["days"][k].get(cc, {}).get("c", 0) for k in keys[:-1]]
        vz = _z(vser, countries[cc]["velocity_per_day"])
        cz = _z(cser, countries[cc]["crisis_hits"])
        countries[cc]["velocity_z"] = vz
        countries[cc]["crisis_z"] = cz
        # composite 0-100: base on absolute crisis flow + z escalation
        base = min(60, countries[cc]["crisis_hits"] * 4
                   + countries[cc]["mentions_48h"] * 0.6)
        esc = max(0, vz) * 8 + max(0, cz) * 12
        countries[cc]["stress_score"] = round(min(100, base + esc), 1)
        # yesterday delta
        if len(keys) >= 2:
            y = hist["days"][keys[-2]].get(cc, {})
            countries[cc]["velocity_delta"] = round(
                countries[cc]["velocity_per_day"] - y.get("v", 0), 1)

    ranked = sorted(countries.items(), key=lambda x: -x[1]["stress_score"])
    escalating = sorted(
        [(cc, d) for cc, d in countries.items()
         if d.get("velocity_z", 0) >= 1.5 and d["crisis_hits"] >= 2],
        key=lambda x: -x[1]["velocity_z"])

    doc = {
        "ok": True, "version": VERSION, "generated_at": now.isoformat(),
        "sources": {"feeds_in_corpus": len(feeds), "feeds_responding": feeds_ok,
                    "articles_scanned": n_articles,
                    "corpus_attribution": "curated public-RSS list adapted from "
                    "koala73/worldmonitor (AGPL-3.0); scores are JustHodl-original"},
        "top_country": ranked[0][0] if ranked else None,
        "global_temp": round(sum(d["stress_score"] for _, d in ranked)
                             / max(1, len(ranked)), 1),
        "rankings": [{"country": cc, **d} for cc, d in ranked],
        "escalating": [{"country": cc, "velocity_z": d["velocity_z"],
                        "crisis_hits": d["crisis_hits"],
                        "stress_score": d["stress_score"],
                        "top": (d["headlines"][0]["title"]
                                if d["headlines"] else None)}
                       for cc, d in escalating],
        "methodology": {
            "velocity": "articles/day mentioning the country across the "
                        "geopolitics/analysis/crisis/security corpus (48h window)",
            "crisis_intensity": "share of those matching a conflict/sanction/"
                                "strike lexicon",
            "stress_score": "0-100: absolute crisis flow (cap 60) + escalation "
                            "from 20d z-scores of velocity & crisis counts",
            "escalating": "velocity z>=1.5 AND >=2 crisis headlines"},
    }
    S3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    S3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                  Body=json.dumps(hist).encode(), ContentType="application/json")
    print(f"[geo-risk] {feeds_ok} feeds, {n_articles} articles, "
          f"top={doc['top_country']}, temp={doc['global_temp']}, "
          f"escalating={len(escalating)}")
    return {"ok": True, "top": doc["top_country"], "temp": doc["global_temp"],
            "escalating": len(escalating), "articles": n_articles}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
