"""
justhodl-cb-stance — Central Bank Hawkish/Dovish Engine (BUILD 11/15)

WHY THIS EXISTS
===============
Fed/ECB/BoE/BoJ language shifts move markets within seconds of release.
Bloomberg "Fed Sentiment" is a flagship $24k/yr product. This builds the
primitive from the Fed's own RSS feed + Claude Haiku NLP.

THE ALPHA: DELTA-HAWKISH
========================
Absolute hawkish/dovish levels matter less than the SHIFT between meetings.
When the Jan statement says "remains highly attentive to inflation risks"
and March drops "highly" → dovish pivot worth ~50 bps in market pricing.

DATA SOURCES
============
Fed (operational this build):
  Monetary RSS: federalreserve.gov/feeds/press_monetary.xml
    Lists last ~10 monetary policy press releases with dates and links.
  Statement pages: federalreserve.gov/newsevents/pressreleases/monetaryYYYYMMDDa.htm
    Full FOMC statement body (~84 KB HTML, ~1500 word statement text).

ECB/BoE/BoJ — deferred (URL discovery in progress).

NLP SCORING (Claude Haiku 4.5)
==============================
Per statement:
  hawkish_score: -100 (extremely dovish) to +100 (extremely hawkish)
  policy_action: HIKE / HOLD / CUT
  forward_guidance: HAWKISH / NEUTRAL / DOVISH / DATA_DEPENDENT
  inflation_concern: HIGH / MEDIUM / LOW
  growth_concern: HIGH / MEDIUM / LOW
  labor_concern: HIGH / MEDIUM / LOW
  key_themes: [3 phrases]
  notable_language_changes: [phrases that differ from prior]
  summary: one-sentence

DELTA METRICS
=============
Per ticker statement vs prior:
  delta_hawkish_score (+ = hawkish shift)
  shift_classification:
    HAWKISH_PIVOT (+15)
    HAWKISH_DRIFT (+5..+15)
    STABLE (-5..+5)
    DOVISH_DRIFT (-15..-5)
    DOVISH_PIVOT (-15)

OUTPUT data/cb-stance.json
==========================
  generated_at, version
  fed.latest_statement: {date, score, action, ...}
  fed.recent_statements: list of last 8 with scores
  fed.delta_hawkish_score: latest - prior
  fed.shift_classification
  fed.regime: HAWKISH_STANCE / DOVISH_STANCE / NEUTRAL / TRANSITION
  market_implications: derived signal interpretation

SCHEDULE
========
cron(0 */6 * * ? *) — every 6 hours (cheap, captures new statement promptly)
"""
import io, json, os, time, re, urllib.request, urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/cb-stance.json"

FED_MONETARY_RSS = "https://www.federalreserve.gov/feeds/press_monetary.xml"
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HTTP_TIMEOUT = 25
ANTHROPIC_TIMEOUT = 60
MAX_STATEMENTS_TO_SCORE = 5  # cache makes subsequent runs cheap

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# HTTP & HTML PARSING
# ═══════════════════════════════════════════════════════════════════════════

def http_get(url, timeout=HTTP_TIMEOUT):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120 Safari/537.36",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


class TextExtractor(HTMLParser):
    """Strip HTML tags, return clean text."""
    def __init__(self):
        super().__init__()
        self.parts = []
        self.skip = False
        self.tag_stack = []
    def handle_starttag(self, tag, attrs):
        self.tag_stack.append(tag)
        if tag in ("script", "style", "head", "header", "footer", "nav"):
            self.skip = True
    def handle_endtag(self, tag):
        if self.tag_stack: self.tag_stack.pop()
        if tag in ("script", "style", "head", "header", "footer", "nav"):
            self.skip = False
    def handle_data(self, data):
        if not self.skip and data.strip():
            self.parts.append(data.strip())
    def text(self): return " ".join(self.parts)


def strip_html(html):
    p = TextExtractor()
    try:
        p.feed(html)
    except Exception: pass
    return p.text()


# ═══════════════════════════════════════════════════════════════════════════
# FED RSS PARSING
# ═══════════════════════════════════════════════════════════════════════════

ITEM_RE = re.compile(r"<item>(.*?)</item>", re.DOTALL)
TAG_RE = re.compile(r"<(\w+)(?:\s[^>]*)?>(.*?)</\1>", re.DOTALL)
CDATA_RE = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)


def parse_rss_items(xml):
    """Returns list of {title, link, pubDate, description} sorted desc."""
    items = []
    for m in ITEM_RE.finditer(xml):
        body = m.group(1)
        item = {}
        for fm in TAG_RE.finditer(body):
            tag = fm.group(1)
            val = fm.group(2)
            # CDATA strip
            cm = CDATA_RE.search(val)
            if cm: val = cm.group(1)
            item[tag] = val.strip()
        if item.get("title") and item.get("link"):
            items.append(item)
    return items


def is_fomc_statement(item):
    """Filter RSS items to FOMC statements only.
    FOMC statements have titles like 'Federal Reserve issues FOMC statement'
    or contain 'FOMC' and reference 'monetary policy'."""
    t = (item.get("title") or "").lower()
    if "fomc statement" in t: return True
    if "fomc issues" in t: return True
    if "federal open market committee" in t and "monetary policy" in t: return True
    if "fomc" in t and ("statement" in t or "issues" in t): return True
    return False


def extract_statement_body(html, max_len=12000):
    """Extract main statement text from FRB statement page.
    Statement body is in <div class="col-xs-12 col-sm-8 col-md-8"> or similar.
    Best-effort: find the longest paragraph block."""
    text = strip_html(html)
    # Find the "For release" anchor — statement typically starts after it
    # Or "For immediate release"
    m = re.search(r"For\s+(immediate\s+)?release", text, re.IGNORECASE)
    if m:
        text = text[m.start():]
    # Cut at "Implementation Note" or "Voting for"
    end_markers = [
        "Implementation Note",
        "Voting for the monetary policy action",
        "Voting against the monetary policy action",
        "Related Information",
        "Last Update:",
    ]
    for em in end_markers:
        idx = text.find(em)
        if idx > 200:
            text = text[:idx + 500]  # keep voting context but trim noise
            break
    return text[:max_len].strip()


# ═══════════════════════════════════════════════════════════════════════════
# ANTHROPIC NLP
# ═══════════════════════════════════════════════════════════════════════════

NLP_PROMPT = """Analyze this Federal Reserve FOMC statement and score its hawkish/dovish stance. Output ONLY valid JSON, no prose, no markdown fences.

Statement:
{text}

JSON schema:
{{
  "hawkish_score": -100 to +100 integer (-100=extremely dovish/aggressive easing, 0=neutral hold, +100=extremely hawkish/aggressive tightening),
  "policy_action": "HIKE" | "HOLD" | "CUT",
  "policy_action_size_bps": integer (0 if HOLD, positive for HIKE, negative for CUT),
  "forward_guidance": "HAWKISH" | "NEUTRAL" | "DOVISH" | "DATA_DEPENDENT",
  "inflation_concern": "HIGH" | "MEDIUM" | "LOW",
  "growth_concern": "HIGH" | "MEDIUM" | "LOW",
  "labor_concern": "HIGH" | "MEDIUM" | "LOW",
  "balance_sheet_stance": "TIGHTENING" | "STABLE" | "EXPANDING" | "NOT_DISCUSSED",
  "key_themes": ["theme1", "theme2", "theme3"],
  "notable_phrases": ["specific phrase 1", "specific phrase 2"] (verbatim language that hints at stance shift),
  "summary": "one-sentence executive summary"
}}"""


def anthropic_score(text):
    if not ANTHROPIC_API_KEY:
        return {"err": "no ANTHROPIC_API_KEY"}
    if not text or len(text) < 200:
        return {"err": "text too short"}
    if len(text) > 11000:
        text = text[:8000] + "\n\n[...]\n\n" + text[-2500:]

    prompt = NLP_PROMPT.format(text=text)
    body = json.dumps({
        "model": ANTHROPIC_MODEL,
        "max_tokens": 700,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")

    try:
        req = urllib.request.Request("https://api.anthropic.com/v1/messages",
            data=body, headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            })
        with urllib.request.urlopen(req, timeout=ANTHROPIC_TIMEOUT) as r:
            resp = json.loads(r.read().decode("utf-8"))
        txt = (resp.get("content") or [{}])[0].get("text", "").strip()
        if "```" in txt:
            parts = txt.split("```")
            for p in parts:
                ps = p.strip()
                if ps.startswith("json"): ps = ps[4:].strip()
                if ps.startswith("{"):
                    txt = ps; break
        if txt.startswith("json"): txt = txt[4:].strip()
        score = json.loads(txt)
        score["_usage"] = resp.get("usage", {})
        return score
    except urllib.error.HTTPError as e:
        return {"err": f"anthropic http {e.code}"}
    except json.JSONDecodeError as e:
        return {"err": f"parse: {e}", "raw": txt[:300]}
    except Exception as e:
        return {"err": str(e)[:200]}


# ═══════════════════════════════════════════════════════════════════════════
# PER-STATEMENT PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

def date_from_url(url):
    """Extract YYYYMMDD from .../monetary20260128a.htm"""
    m = re.search(r"monetary(\d{8})", url)
    return m.group(1) if m else None


def cache_key_for(date_str):
    return f"cb-cache/fomc_{date_str}.json"


def fetch_or_cache_statement(item):
    """Fetch statement and NLP-score, with S3 cache."""
    url = item.get("link", "")
    date_str = date_from_url(url) or item.get("pubDate", "?")[:10].replace("-","")
    title = item.get("title", "?")
    pubdate = item.get("pubDate", "")

    result = {"date": date_str, "title": title, "pubDate": pubdate, "url": url}

    # Check cache
    ck = cache_key_for(date_str)
    try:
        cached = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=ck)["Body"].read())
        if cached.get("score") and not cached.get("score", {}).get("err"):
            result.update(cached["score"])
            result["from_cache"] = True
            return result
    except Exception: pass

    # Fetch + score
    try:
        html = http_get(url)
        body_text = extract_statement_body(html)
        if not body_text or len(body_text) < 300:
            result["err"] = f"statement body too short ({len(body_text)} chars)"
            return result
        result["body_length"] = len(body_text)
        score = anthropic_score(body_text)
        if score.get("err"):
            result["err"] = score["err"]
            return result
        # Cache
        try:
            cache = {
                "date": date_str, "url": url, "title": title,
                "score": {k: v for k, v in score.items() if k != "_usage"},
                "body_length": len(body_text),
                "scored_at": datetime.now(timezone.utc).isoformat(),
                "_usage": score.get("_usage", {}),
            }
            s3.put_object(Bucket=S3_BUCKET, Key=ck,
                Body=json.dumps(cache, separators=(",", ":")).encode("utf-8"),
                ContentType="application/json",
                CacheControl="public, max-age=86400")
        except Exception as e:
            print(f"  cache put err: {str(e)[:80]}")
        result.update({k: v for k, v in score.items() if k != "_usage"})
        result["from_cache"] = False
        return result
    except Exception as e:
        result["err"] = str(e)[:200]
        return result


# ═══════════════════════════════════════════════════════════════════════════
# COMPOSITE
# ═══════════════════════════════════════════════════════════════════════════

def classify_shift(delta):
    if delta is None: return "NO_PRIOR"
    if delta >= 15: return "HAWKISH_PIVOT"
    if delta >= 5: return "HAWKISH_DRIFT"
    if delta >= -5: return "STABLE"
    if delta >= -15: return "DOVISH_DRIFT"
    return "DOVISH_PIVOT"


def classify_regime(score, shift_cls):
    if score is None: return "UNKNOWN", "No score available"
    if score >= 50:
        return "HAWKISH_STANCE", "Tight monetary policy; restrictive for risk assets"
    if score >= 20:
        if shift_cls in ("DOVISH_DRIFT", "DOVISH_PIVOT"):
            return "TRANSITION_DOVISH", "Still hawkish but pivoting; constructive for risk"
        return "MILD_HAWKISH", "Restrictive but moderating; mixed for risk"
    if score >= -20:
        return "NEUTRAL", "Balanced stance; data-dependent"
    if score >= -50:
        if shift_cls in ("HAWKISH_DRIFT", "HAWKISH_PIVOT"):
            return "TRANSITION_HAWKISH", "Easy but pivoting; caution for risk"
        return "MILD_DOVISH", "Accommodative; supportive for risk"
    return "DOVISH_STANCE", "Aggressive easing; very supportive for risk"


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception: return None


def send_telegram(text):
    if not TELEGRAM_TOKEN: return False
    chat = get_chat_id()
    if not chat: return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        body = json.dumps({"chat_id": chat, "text": text[:4096],
                            "parse_mode": "Markdown",
                            "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"  tg err: {str(e)[:80]}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# HANDLER
# ═══════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== cb-stance v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    # Load prior state for regime change detection
    try:
        prior = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY)["Body"].read())
        prior_regime = (prior.get("fed") or {}).get("regime")
        prior_latest_date = (prior.get("fed") or {}).get("latest_statement", {}).get("date")
    except Exception:
        prior, prior_regime, prior_latest_date = None, None, None

    # ─── Fetch Fed monetary RSS ───
    try:
        xml = http_get(FED_MONETARY_RSS)
        items = parse_rss_items(xml)
        fomc_items = [i for i in items if is_fomc_statement(i)]
        print(f"  RSS: {len(items)} items, {len(fomc_items)} FOMC statements")
    except Exception as e:
        print(f"  RSS err: {str(e)[:120]}")
        return {"statusCode": 500, "body": json.dumps({"err": f"rss: {e}"})}

    if not fomc_items:
        return {"statusCode": 500, "body": json.dumps({
            "err": "no FOMC statements found in RSS",
            "n_items": len(items),
            "sample_titles": [i.get("title", "?") for i in items[:5]],
        })}

    # ─── Score each in parallel (cache-aware) ───
    statements = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(fetch_or_cache_statement, item): item
                    for item in fomc_items[:MAX_STATEMENTS_TO_SCORE]}
        for f in as_completed(futures):
            r = f.result()
            statements.append(r)
            cached = "[cache]" if r.get("from_cache") else "[new]"
            if r.get("err"):
                print(f"  ✗ {r.get('date')} {cached} err: {r['err']}")
            else:
                print(f"  ✓ {r.get('date')} {cached} score={r.get('hawkish_score')} "
                      f"action={r.get('policy_action')} guidance={r.get('forward_guidance')}")

    # Sort by date desc (most recent first)
    statements.sort(key=lambda r: r.get("date", ""), reverse=True)
    scored = [r for r in statements if r.get("hawkish_score") is not None]

    if not scored:
        return {"statusCode": 500, "body": json.dumps({
            "err": "no successfully scored statements",
            "n_attempted": len(statements),
            "first_err": (statements[0].get("err") if statements else None),
        })}

    # ─── Composite metrics ───
    latest = scored[0]
    prior_scored = scored[1] if len(scored) > 1 else None
    delta = (latest["hawkish_score"] - prior_scored["hawkish_score"]) if prior_scored else None
    shift_cls = classify_shift(delta)
    regime, regime_signal = classify_regime(latest["hawkish_score"], shift_cls)

    # ─── Build payload ───
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "model": ANTHROPIC_MODEL,
        "elapsed_seconds": round(time.time() - started, 2),
        "n_fomc_statements_scored": len(scored),
        "fed": {
            "latest_statement": {
                "date": latest.get("date"),
                "title": latest.get("title"),
                "url": latest.get("url"),
                "pubDate": latest.get("pubDate"),
                "hawkish_score": latest.get("hawkish_score"),
                "policy_action": latest.get("policy_action"),
                "policy_action_size_bps": latest.get("policy_action_size_bps"),
                "forward_guidance": latest.get("forward_guidance"),
                "inflation_concern": latest.get("inflation_concern"),
                "growth_concern": latest.get("growth_concern"),
                "labor_concern": latest.get("labor_concern"),
                "balance_sheet_stance": latest.get("balance_sheet_stance"),
                "key_themes": latest.get("key_themes"),
                "notable_phrases": latest.get("notable_phrases"),
                "summary": latest.get("summary"),
            },
            "prior_statement_date": prior_scored.get("date") if prior_scored else None,
            "prior_hawkish_score": prior_scored.get("hawkish_score") if prior_scored else None,
            "delta_hawkish_score": delta,
            "shift_classification": shift_cls,
            "regime": regime,
            "regime_signal": regime_signal,
            "recent_statements": [
                {"date": s.get("date"),
                  "title": s.get("title"),
                  "hawkish_score": s.get("hawkish_score"),
                  "action": s.get("policy_action"),
                  "guidance": s.get("forward_guidance"),
                  "summary": (s.get("summary") or "")[:200]}
                for s in scored
            ],
        },
        "regime_changed_from_prior": (prior_regime and prior_regime != regime),
        "new_statement_since_last_run": (prior_latest_date and prior_latest_date != latest.get("date")),
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=900")
        print(f"  ✓ cb-stance.json written")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # ─── Telegram on regime change or new statement ───
    alert_sent = False
    if payload["regime_changed_from_prior"] or payload["new_statement_since_last_run"]:
        latest_info = payload["fed"]["latest_statement"]
        lines = [f"🏦 *Fed FOMC Stance · {datetime.now(timezone.utc).strftime('%b %d')}*\n",
                  f"⚡ {regime}",
                  f"_{regime_signal}_\n",
                  f"📊 Hawkish: *{latest.get('hawkish_score'):+d}* (Δ {delta:+d} from prior · {shift_cls})" if delta is not None else f"📊 Hawkish: *{latest.get('hawkish_score'):+d}*",
                  f"⚖️  Action: {latest.get('policy_action')} {latest.get('policy_action_size_bps','?')} bps",
                  f"🔮 Guidance: {latest.get('forward_guidance')}",
                  f"\n_{(latest_info.get('summary') or '')[:300]}_"]
        if prior_regime and prior_regime != regime:
            lines.insert(2, f"_(was {prior_regime})_")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print("  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "latest_fomc_date": latest.get("date"),
        "hawkish_score": latest.get("hawkish_score"),
        "policy_action": latest.get("policy_action"),
        "regime": regime,
        "delta_vs_prior": delta,
        "shift_classification": shift_cls,
        "n_scored": len(scored),
        "regime_changed": payload["regime_changed_from_prior"],
        "alert_sent": alert_sent,
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
