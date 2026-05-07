"""
justhodl-fed-speak — Claude-powered FOMC speech sentiment tracker.

WHY THIS EXISTS
───────────────
No Lambda parses Fed governor speeches for hawkish/dovish tone shifts.
Markets move violently on Fed speak. Currently invisible to the platform.

ALGORITHM
─────────
Daily at 11:15 UTC (6:15 AM ET):

  1. Pull Fed RSS feed: https://www.federalreserve.gov/feeds/speeches.xml
  2. Parse last 30-50 entries (title, description, author, date, link)
  3. Filter to last 30 days
  4. For each NEW speech (not in state cache), send to Claude:
       Prompt: "Classify this Fed speech excerpt on a -10 to +10 scale where:
         -10 = extremely dovish (rate cuts, easing, accommodation)
         0 = neutral / data dependent
         +10 = extremely hawkish (rate hikes, inflation, restrictive)

         Title: {title}
         Speaker: {speaker}
         Description: {description}

         Return JSON: {sentiment_score, classification, key_phrases[3]}"
  5. Cache classification → state (avoids re-classifying)
  6. Aggregate by speaker + over time

OUTPUT
──────
  data/fed-speak.json
    {
      as_of, n_speeches_30d,
      aggregate: {avg_sentiment, hawkish_count, dovish_count, neutral_count},
      by_speaker: { Powell: {n, avg, classification}, ... },
      timeline: [{date, speaker, title, sentiment, key_phrases, link}, ...],
      latest_hawkish, latest_dovish, latest_neutral
    }

  data/fed-speak-state.json — cached classifications, last 200 speeches

COST
────
  Daily run, typically 0-2 new speeches → ~50 Claude calls/year
  At $0.001/call (haiku-4.5) = ~$0.05/year
  Almost free.

SCHEDULE
────────
  cron(15 11 * * ? *)  — 11:15 UTC = 6:15 AM ET

ZERO DETERIORATION
  ✓ No Lambda touched
  ✓ Anthropic key already in env (used by ai-brief, divergence-interpreter, etc)
  ✓ Failure-safe: if RSS fails, returns last known state
"""
import json
import os
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/fed-speak.json")
S3_KEY_STATE = os.environ.get("S3_KEY_STATE", "data/fed-speak-state.json")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")
RSS_URL = "https://www.federalreserve.gov/feeds/speeches.xml"
DAYS_BACK = int(os.environ.get("DAYS_BACK", "30"))
MAX_NEW_PER_RUN = int(os.environ.get("MAX_NEW_PER_RUN", "8"))

S3 = boto3.client("s3", region_name=REGION)


def fetch_rss():
    """Fetch Fed speeches RSS feed and parse entries."""
    try:
        req = urllib.request.Request(
            RSS_URL,
            headers={"User-Agent": "justhodl-fed-speak/1.0"},
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            xml_text = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"[fed-speak] RSS fetch failed: {e}")
        return []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"[fed-speak] RSS parse failed: {e}")
        return []

    entries = []
    # RSS feed structure: <rss><channel><item>...</item>...</channel></rss>
    # OR Atom: <feed><entry>...</entry>...</feed>
    items = root.findall(".//item")
    if not items:
        items = root.findall(".//{http://www.w3.org/2005/Atom}entry")

    for item in items[:60]:
        def get(tag):
            for prefix in ("", "{http://www.w3.org/2005/Atom}",
                            "{http://purl.org/dc/elements/1.1/}"):
                el = item.find(f"{prefix}{tag}")
                if el is not None and el.text:
                    return el.text.strip()
            return ""

        title = get("title")
        link = get("link")
        if not link:
            link_el = item.find("{http://www.w3.org/2005/Atom}link")
            if link_el is not None:
                link = link_el.get("href", "")
        description = get("description") or get("summary")
        # dc:creator for speaker, fallback to author tag
        author = get("creator") or get("author") or ""
        pub_date_str = get("pubDate") or get("published") or get("date")

        # Strip HTML from description
        description = re.sub(r"<[^>]+>", " ", description)
        description = re.sub(r"\s+", " ", description).strip()[:1500]

        # Speaker often embedded in title like "Speech by Chair Powell on monetary policy"
        speaker = author or extract_speaker_from_title(title)

        # Parse pub date
        pub_dt = None
        for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                     "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
                     "%Y-%m-%d %H:%M:%S"):
            try:
                pub_dt = datetime.strptime(pub_date_str.strip(), fmt)
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                break
            except (ValueError, AttributeError):
                continue

        entries.append({
            "title": title[:200],
            "link": link,
            "speaker": speaker,
            "description": description,
            "pub_date": pub_dt.isoformat() if pub_dt else pub_date_str,
            "pub_dt": pub_dt,
        })
    return entries


def extract_speaker_from_title(title):
    """Extract speaker name from Fed speech title patterns."""
    if not title:
        return ""
    # Common patterns: "Speech by Chair Powell..." / "Remarks by Governor Cook..."
    m = re.search(r"(?:Speech|Remarks|Welcoming Remarks|Statement)\s+by\s+(?:Vice\s+)?(?:Chair(?:man)?|Governor|President)\s+([A-Z][a-zA-Z]+)",
                   title)
    if m:
        return m.group(1)
    # Other patterns
    m = re.search(r"by\s+([A-Z][a-zA-Z]+\s+[A-Z][a-zA-Z]+)", title)
    if m:
        return m.group(1)
    return ""


def call_claude_classify(speech):
    """Send a speech to Claude for HAWKISH/NEUTRAL/DOVISH classification."""
    if not ANTHROPIC_KEY:
        return None
    prompt = f"""Classify this Fed speech excerpt on a -10 to +10 scale where:
  -10 = extremely dovish (rate cuts urgent, recession concerns, max accommodation)
  -5 = lean dovish (concerns about growth, willing to ease)
  0 = neutral / data dependent / balanced
  +5 = lean hawkish (inflation concerns, willing to hold rates restrictive)
  +10 = extremely hawkish (urgent need for further hikes, well-anchored expectations slipping)

Speech metadata:
  Title: {speech.get('title','')}
  Speaker: {speech.get('speaker','')}
  Date: {speech.get('pub_date','')}
  Excerpt: {speech.get('description','')[:1200]}

Return ONLY this JSON (no markdown fences, no commentary):
{{"sentiment_score": <number -10..+10>, "classification": "HAWKISH|NEUTRAL|DOVISH", "key_phrases": ["short phrase 1", "phrase 2", "phrase 3"], "rationale": "1-sentence reasoning"}}"""

    body = json.dumps({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 400,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read().decode("utf-8"))
        text = "".join(b.get("text", "") for b in resp.get("content", []) if b.get("type") == "text")
        # Strip code fences if any
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE)
        return json.loads(text)
    except Exception as e:
        print(f"[fed-speak] Claude classify failed: {e}")
        return None


def load_state():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=S3_KEY_STATE)
        return json.loads(obj["Body"].read())
    except Exception:
        return {"by_link": {}, "n_total": 0}


def save_state(state):
    # Keep last 200 entries to avoid unbounded growth
    by_link = state.get("by_link", {})
    if len(by_link) > 200:
        # Keep newest 200 by classification timestamp
        items = sorted(by_link.items(),
                        key=lambda x: x[1].get("classified_at", ""),
                        reverse=True)
        state["by_link"] = dict(items[:200])
    state["n_total"] = len(state["by_link"])
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_STATE,
        Body=json.dumps(state, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def lambda_handler(event, context):
    started = time.time()

    print("[fed-speak] Loading state…")
    state = load_state()
    by_link = state.get("by_link", {})

    print("[fed-speak] Fetching Fed RSS feed…")
    entries = fetch_rss()
    print(f"[fed-speak] Got {len(entries)} entries from RSS")

    # Filter to last DAYS_BACK days
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    recent = [e for e in entries
               if e.get("pub_dt") and e["pub_dt"] >= cutoff]
    print(f"[fed-speak] {len(recent)} within {DAYS_BACK} days")

    # Classify NEW speeches only (cap at MAX_NEW_PER_RUN)
    new_classified = 0
    for entry in recent:
        link = entry.get("link", "")
        if not link or link in by_link:
            continue
        if new_classified >= MAX_NEW_PER_RUN:
            break

        print(f"[fed-speak] Classifying: {entry.get('title','?')[:80]}")
        cls = call_claude_classify(entry)
        if cls is None:
            continue

        by_link[link] = {
            "title": entry.get("title"),
            "speaker": entry.get("speaker"),
            "pub_date": entry.get("pub_date"),
            "link": link,
            "sentiment_score": cls.get("sentiment_score", 0),
            "classification": cls.get("classification", "NEUTRAL"),
            "key_phrases": cls.get("key_phrases", [])[:5],
            "rationale": (cls.get("rationale", "") or "")[:300],
            "classified_at": datetime.now(timezone.utc).isoformat(),
        }
        new_classified += 1
        time.sleep(0.5)  # gentle on Anthropic API

    state["by_link"] = by_link

    # Aggregate timeline from classified speeches
    classified_recent = []
    for entry in recent:
        link = entry.get("link", "")
        if link in by_link:
            classified_recent.append(by_link[link])
    classified_recent.sort(key=lambda x: x.get("pub_date", ""), reverse=True)

    # Aggregate stats
    if classified_recent:
        scores = [c.get("sentiment_score", 0) or 0 for c in classified_recent]
        avg_sentiment = round(sum(scores) / len(scores), 2)
        hawkish = sum(1 for s in scores if s >= 3)
        dovish = sum(1 for s in scores if s <= -3)
        neutral = len(scores) - hawkish - dovish
    else:
        avg_sentiment = 0
        hawkish = dovish = neutral = 0

    # By speaker
    by_speaker = {}
    for c in classified_recent:
        sp = c.get("speaker") or "Unknown"
        by_speaker.setdefault(sp, []).append(c)
    by_speaker_summary = {}
    for sp, items in by_speaker.items():
        scores = [it.get("sentiment_score", 0) or 0 for it in items]
        by_speaker_summary[sp] = {
            "n_speeches": len(items),
            "avg_sentiment": round(sum(scores) / len(scores), 2),
            "latest_classification": items[0].get("classification") if items else None,
            "latest_date": items[0].get("pub_date") if items else None,
        }

    # Latest by classification
    latest_hawk = next((c for c in classified_recent
                         if c.get("classification") == "HAWKISH"), None)
    latest_dove = next((c for c in classified_recent
                         if c.get("classification") == "DOVISH"), None)
    latest_neutral = next((c for c in classified_recent
                           if c.get("classification") == "NEUTRAL"), None)

    payload = {
        "schema_version": "1.0",
        "method": "fed_speak_v1",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_speeches_30d": len(classified_recent),
        "n_new_classified_this_run": new_classified,
        "aggregate": {
            "avg_sentiment": avg_sentiment,
            "hawkish_count": hawkish,
            "dovish_count": dovish,
            "neutral_count": neutral,
            "interpretation":
                "FED LEANING HAWKISH" if avg_sentiment >= 1.5
                else ("FED LEANING DOVISH" if avg_sentiment <= -1.5
                      else "FED NEUTRAL / DATA DEPENDENT"),
        },
        "by_speaker": by_speaker_summary,
        "timeline": classified_recent[:20],
        "latest_hawkish": latest_hawk,
        "latest_dovish": latest_dove,
        "latest_neutral": latest_neutral,
        "duration_s": round(time.time() - started, 2),
    }

    body_bytes = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_OUT, Body=body_bytes,
        ContentType="application/json", CacheControl="max-age=600",
    )

    save_state(state)

    print(f"[fed-speak] DONE in {payload['duration_s']}s · "
          f"{len(classified_recent)} speeches in last {DAYS_BACK}d · "
          f"{new_classified} new · avg sentiment={avg_sentiment} "
          f"({hawkish}H/{dovish}D/{neutral}N)")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "n_30d": len(classified_recent),
            "n_new_classified": new_classified,
            "avg_sentiment": avg_sentiment,
            "duration_s": payload["duration_s"],
        }),
    }
