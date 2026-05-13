"""
justhodl-earnings-nlp — Earnings Call Transcript NLP Engine

WHAT IS THIS?
=============
Bloomberg Terminal includes proprietary earnings-call NLP scoring that costs
~$24k/yr. This builds the same primitive from public data:

  1. Fetch latest earnings transcripts from FMP Premium
  2. Score management tone via Claude Haiku (tone, guidance, confidence)
  3. Compute quarter-over-quarter tone shift (the alpha signal)
  4. Surface ranked list of biggest positive/negative tone changes

The "edge" in earnings NLP isn't the absolute tone — it's the SHIFT in
language. When a CFO who said "strong demand" last quarter starts saying
"choppy execution" this quarter, that's the signal.

DATA FLOW
=========
  FMP /v3/earning_call_transcript/{ticker}?quarter=N&year=Y
    → raw transcript JSON cached in S3 transcripts/{ticker}_{Y}Q{N}.json
  Anthropic Haiku /v1/messages
    → tone score + guidance + themes JSON
  S3 sidecar data/earnings-nlp.json
    → current per-ticker scores + ranked tone shifts + market summary

UNIVERSE
========
Top 30 names by market cap (S&P 500 leaders + watchlist priority).
Per Lambda run, only NEW transcripts (cached lookups skip Anthropic call).

SCHEDULE
========
cron(0 14 ? * MON-FRI *) — daily 14:00 UTC.
  US earnings season covers AMC (afternoon) + BMO (morning) releases.
  Most transcripts publish T+1 day after the call.

OUTPUT
======
data/earnings-nlp.json:
{
  "generated_at": ...,
  "n_tickers_analyzed": 30,
  "n_with_transcripts": 28,
  "n_with_recent": 24,
  "universe": [...],
  "by_ticker": {
    "AAPL": {
      "quarter": "2026-Q1",
      "filed_date": "2026-04-30",
      "management_tone": 65,
      "guidance_direction": "RAISED",
      "confidence": "HIGH",
      "key_themes": [...],
      "summary": "...",
      "prior_quarter_tone": 58,
      "tone_shift_pp": 7,
      "shift_classification": "IMPROVING"
    }
  },
  "ranked_tone_shifts": {
    "biggest_improvers": [...],  # ranked by tone_shift_pp desc
    "biggest_deteriorators": [...],
  },
  "market_summary": {
    "median_tone": 52,
    "n_raised_guidance": 18,
    "n_lowered_guidance": 4,
    "n_maintained_guidance": 6,
    "regime": "EARNINGS_STRENGTH"
  }
}
"""
import io, json, os, time, urllib.request, urllib.error, threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

VERSION = "1.1.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/earnings-nlp.json"
TRANSCRIPT_CACHE_PREFIX = "transcripts/"  # S3 path prefix for raw transcripts

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HTTP_TIMEOUT = 25
ANTHROPIC_TIMEOUT = 60
MAX_PARALLEL = 6

# Top 30 names by sector coverage — covers most institutional flow
UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "AVGO",
    "JPM", "WMT", "LLY", "V", "UNH", "XOM", "MA", "PG", "JNJ", "ORCL",
    "HD", "COST", "ABBV", "BAC", "KO", "CVX", "MRK", "PEP", "ADBE", "CSCO", "TMO"
]

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# HTTP HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def http_get_json(url, timeout=HTTP_TIMEOUT):
    """Fetch URL, return parsed JSON or raise."""
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15) Chrome/120",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def s3_get_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        print(f"  s3 get {key}: {str(e)[:100]}")
        return None


def s3_put_json(key, data, cache_control="public, max-age=900"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                    Body=json.dumps(data, separators=(",", ":"), default=str).encode("utf-8"),
                    ContentType="application/json", CacheControl=cache_control)


# ═══════════════════════════════════════════════════════════════════════════
# FMP TRANSCRIPT FETCH
# ═══════════════════════════════════════════════════════════════════════════

def get_latest_transcript_meta(ticker):
    """Returns most recent (year, quarter, date) or None.
    Uses FMP /stable/earning-call-transcript-dates which lists ALL transcripts
    in reverse-chronological order with {quarter, fiscalYear, date}.
    """
    try:
        url = (f"https://financialmodelingprep.com/stable/earning-call-transcript-dates"
                f"?symbol={ticker}&apikey={FMP_KEY}")
        data = http_get_json(url, timeout=15)
        if isinstance(data, list) and data:
            latest = data[0]  # already sorted desc by date
            q = latest.get("quarter")
            y = latest.get("fiscalYear")
            d = latest.get("date")
            if q is not None and y is not None:
                return {"quarter": int(q), "year": int(y), "date": d}
        return None
    except Exception as e:
        print(f"  {ticker} meta err: {str(e)[:80]}")
        return None


def fetch_transcript_content(ticker, year, quarter):
    """Fetch single transcript via FMP /stable/. Returns transcript object or None.
    Response shape: [{symbol, period, year, date, content}]
    """
    url = (f"https://financialmodelingprep.com/stable/earning-call-transcript"
            f"?symbol={ticker}&year={year}&quarter={quarter}&apikey={FMP_KEY}")
    try:
        data = http_get_json(url, timeout=25)
        if isinstance(data, list) and data:
            return data[0]
    except Exception as e:
        print(f"  {ticker} transcript Q{quarter}/{year} err: {str(e)[:80]}")
    return None


# ═══════════════════════════════════════════════════════════════════════════
# ANTHROPIC NLP SCORING
# ═══════════════════════════════════════════════════════════════════════════

NLP_PROMPT = """Analyze this earnings call transcript and score management tone. Output ONLY valid JSON, no prose, no markdown fences.

Transcript:
{text}

JSON schema:
{{
  "management_tone": -100 to +100 integer (-100=extreme bearish/crisis language, 0=neutral, +100=extreme bullish/euphoric),
  "guidance_direction": "RAISED" | "MAINTAINED" | "LOWERED" | "NONE",
  "confidence": "LOW" | "MEDIUM" | "HIGH",
  "key_themes": ["theme1", "theme2", "theme3"] (3 short phrases capturing main topics),
  "demand_signal": "STRONG" | "STEADY" | "WEAKENING" | "MIXED",
  "margin_signal": "EXPANDING" | "STABLE" | "COMPRESSING" | "MIXED",
  "summary": "one-sentence executive summary"
}}"""


def anthropic_score_transcript(text, ticker):
    """Score a transcript via Claude Haiku. Truncates to first 8000 chars + last 2000 for guidance section."""
    if not ANTHROPIC_API_KEY:
        return {"err": "no ANTHROPIC_API_KEY"}
    if not text:
        return {"err": "empty text"}

    # Smart truncation: first 8K (prepared remarks) + last 2K (guidance/Q&A)
    if len(text) > 11000:
        text = text[:8000] + "\n\n[...transcript continues...]\n\n" + text[-2000:]

    prompt = NLP_PROMPT.format(text=text)
    body = json.dumps({
        "model": ANTHROPIC_MODEL,
        "max_tokens": 600,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")

    try:
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=body,
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=ANTHROPIC_TIMEOUT) as r:
            resp = json.loads(r.read().decode("utf-8"))

        txt = (resp.get("content") or [{}])[0].get("text", "").strip()
        # Strip markdown fences
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
# PER-TICKER PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

def analyze_ticker(ticker):
    """For one ticker: fetch latest meta → check cache → fetch transcript → score → return."""
    result = {"ticker": ticker, "started_at": time.time()}
    try:
        # 1. Find latest transcript period
        meta = get_latest_transcript_meta(ticker)
        if not meta:
            result["err"] = "no transcript metadata"
            return result
        year, quarter = meta["year"], meta["quarter"]
        filed_date = meta.get("date")
        period = f"{year}-Q{quarter}"
        result["period"] = period
        result["filed_date"] = filed_date

        # 2. Check S3 cache
        cache_key = f"{TRANSCRIPT_CACHE_PREFIX}{ticker}_{year}Q{quarter}.json"
        cached = s3_get_json(cache_key)
        if cached and cached.get("score"):
            result["from_cache"] = True
            result.update(cached["score"])
            result["raw_transcript_len"] = cached.get("raw_transcript_len")
            return result

        # 3. Fetch full transcript
        transcript_obj = fetch_transcript_content(ticker, year, quarter)
        if not transcript_obj:
            result["err"] = "transcript fetch failed"
            return result
        content = transcript_obj.get("content", "")
        if not content or len(content) < 500:
            result["err"] = f"transcript too short ({len(content)} chars)"
            return result
        result["raw_transcript_len"] = len(content)

        # 4. NLP score
        score = anthropic_score_transcript(content, ticker)
        if score.get("err"):
            result["err"] = score["err"]
            return result

        # 5. Cache result
        cache_payload = {
            "ticker": ticker, "period": period, "filed_date": filed_date,
            "raw_transcript_len": len(content),
            "score": {k: v for k, v in score.items() if k != "_usage"},
            "scored_at": datetime.now(timezone.utc).isoformat(),
            "_usage": score.get("_usage", {}),
        }
        try:
            s3_put_json(cache_key, cache_payload, cache_control="public, max-age=86400")
        except Exception as e:
            print(f"  cache write err: {str(e)[:80]}")

        result.update({k: v for k, v in score.items() if k != "_usage"})
        result["from_cache"] = False
        return result
    except Exception as e:
        result["err"] = str(e)[:200]
        return result


def get_prior_quarter_tone(ticker, current_period):
    """Look up previous quarter's tone from cache (if exists)."""
    # current_period is e.g. "2026-Q1" — previous is "2025-Q4"
    try:
        y, q = current_period.split("-Q")
        y, q = int(y), int(q)
        if q == 1: pq, py = 4, y - 1
        else: pq, py = q - 1, y
        cache_key = f"{TRANSCRIPT_CACHE_PREFIX}{ticker}_{py}Q{pq}.json"
        cached = s3_get_json(cache_key)
        if cached and cached.get("score"):
            return cached["score"].get("management_tone"), f"{py}-Q{pq}"
        return None, None
    except Exception:
        return None, None


# ═══════════════════════════════════════════════════════════════════════════
# AGGREGATION
# ═══════════════════════════════════════════════════════════════════════════

def classify_tone_shift(shift_pp):
    if shift_pp is None: return "NO_PRIOR"
    if shift_pp >= 15: return "SHARPLY_IMPROVING"
    if shift_pp >= 5: return "IMPROVING"
    if shift_pp >= -5: return "STABLE"
    if shift_pp >= -15: return "DETERIORATING"
    return "SHARPLY_DETERIORATING"


def market_summary(results):
    valid = [r for r in results if r.get("management_tone") is not None]
    if not valid:
        return {"err": "no valid scores", "n": 0}

    tones = [r["management_tone"] for r in valid]
    tones_sorted = sorted(tones)
    median = tones_sorted[len(tones_sorted) // 2]
    mean = sum(tones) / len(tones)

    guidance = {}
    for r in valid:
        g = r.get("guidance_direction")
        if g: guidance[g] = guidance.get(g, 0) + 1

    confidence_dist = {}
    for r in valid:
        c = r.get("confidence")
        if c: confidence_dist[c] = confidence_dist.get(c, 0) + 1

    # Regime classification
    n_raised = guidance.get("RAISED", 0)
    n_lowered = guidance.get("LOWERED", 0)
    raised_lowered_ratio = n_raised / max(n_lowered, 1)

    if median >= 60 and raised_lowered_ratio >= 3:
        regime = "EARNINGS_STRENGTH"
        signal = "Strong management confidence; raise/lower ratio favorable"
    elif median <= 30 and n_lowered > n_raised:
        regime = "EARNINGS_WEAKNESS"
        signal = "Negative tone breadth; cuts outpacing raises"
    elif median >= 50:
        regime = "EARNINGS_CONSTRUCTIVE"
        signal = "Above-trend tone; mixed guidance"
    elif median <= 40:
        regime = "EARNINGS_CAUTIOUS"
        signal = "Below-trend tone; defensive language"
    else:
        regime = "EARNINGS_NEUTRAL"
        signal = "Balanced tone; mixed earnings season"

    return {
        "n_scored": len(valid),
        "median_tone": median,
        "mean_tone": round(mean, 1),
        "tone_distribution": {
            "p10": tones_sorted[max(0, len(tones_sorted)//10)],
            "p25": tones_sorted[len(tones_sorted)//4],
            "p75": tones_sorted[3*len(tones_sorted)//4],
            "p90": tones_sorted[min(len(tones_sorted)-1, 9*len(tones_sorted)//10)],
        },
        "guidance_breakdown": guidance,
        "confidence_breakdown": confidence_dist,
        "raised_to_lowered_ratio": round(raised_lowered_ratio, 2),
        "regime": regime,
        "signal": signal,
    }


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
    print(f"=== earnings-nlp v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")
    print(f"  universe: {len(UNIVERSE)} tickers")
    print(f"  ANTHROPIC_API_KEY present: {bool(ANTHROPIC_API_KEY)}")

    # Load prior state for regime comparison
    prior = s3_get_json(OUTPUT_KEY) or {}
    prior_regime = (prior.get("market_summary") or {}).get("regime")

    # ─── Parallel ticker analysis ───
    results = []
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
        futures = {ex.submit(analyze_ticker, t): t for t in UNIVERSE}
        for f in as_completed(futures):
            r = f.result()
            tkr = r.get("ticker")
            if r.get("err"):
                print(f"  ✗ {tkr}: {r['err']}")
            else:
                tone = r.get("management_tone")
                cached = "[cache]" if r.get("from_cache") else "[new]"
                print(f"  ✓ {tkr} {r.get('period')} {cached} tone={tone} guidance={r.get('guidance_direction')}")
            results.append(r)

    # ─── Compute QoQ tone shifts ───
    by_ticker = {}
    for r in results:
        if not r.get("management_tone") and r.get("management_tone") != 0:
            by_ticker[r["ticker"]] = {"err": r.get("err"), "period": r.get("period")}
            continue
        prior_tone, prior_period = get_prior_quarter_tone(r["ticker"], r["period"])
        shift = (r["management_tone"] - prior_tone) if prior_tone is not None else None
        entry = {k: v for k, v in r.items() if not k.startswith("_") and k != "started_at"}
        entry["prior_quarter_tone"] = prior_tone
        entry["prior_quarter_period"] = prior_period
        entry["tone_shift_pp"] = shift
        entry["shift_classification"] = classify_tone_shift(shift)
        by_ticker[r["ticker"]] = entry

    # ─── Ranked tone shifts ───
    has_shift = [(k, v) for k, v in by_ticker.items()
                  if v.get("tone_shift_pp") is not None]
    improvers = sorted(has_shift, key=lambda x: -x[1]["tone_shift_pp"])[:10]
    deteriorators = sorted(has_shift, key=lambda x: x[1]["tone_shift_pp"])[:10]

    ranked = {
        "biggest_improvers": [
            {"ticker": k, "tone_shift_pp": v["tone_shift_pp"],
              "current_tone": v.get("management_tone"),
              "prior_tone": v.get("prior_quarter_tone"),
              "guidance": v.get("guidance_direction"),
              "summary": v.get("summary", "")[:140]}
            for k, v in improvers if v["tone_shift_pp"] > 0
        ],
        "biggest_deteriorators": [
            {"ticker": k, "tone_shift_pp": v["tone_shift_pp"],
              "current_tone": v.get("management_tone"),
              "prior_tone": v.get("prior_quarter_tone"),
              "guidance": v.get("guidance_direction"),
              "summary": v.get("summary", "")[:140]}
            for k, v in deteriorators if v["tone_shift_pp"] < 0
        ],
    }

    # Ranked by current tone
    valid_tones = [(k, v) for k, v in by_ticker.items()
                    if v.get("management_tone") is not None]
    most_bullish = sorted(valid_tones, key=lambda x: -x[1]["management_tone"])[:10]
    most_bearish = sorted(valid_tones, key=lambda x: x[1]["management_tone"])[:10]
    ranked["most_bullish_tone"] = [
        {"ticker": k, "tone": v["management_tone"], "guidance": v.get("guidance_direction"),
          "period": v.get("period")} for k, v in most_bullish
    ]
    ranked["most_bearish_tone"] = [
        {"ticker": k, "tone": v["management_tone"], "guidance": v.get("guidance_direction"),
          "period": v.get("period")} for k, v in most_bearish
    ]

    # ─── Market summary ───
    summary = market_summary(list(by_ticker.values()))

    n_with_data = sum(1 for v in by_ticker.values() if v.get("management_tone") is not None)
    n_with_err = sum(1 for v in by_ticker.values() if v.get("err"))

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "model": ANTHROPIC_MODEL,
        "source": "FMP Premium /v3/earning_call_transcript",
        "elapsed_seconds": round(time.time() - started, 1),
        "universe": UNIVERSE,
        "n_tickers": len(UNIVERSE),
        "n_with_data": n_with_data,
        "n_with_err": n_with_err,
        "by_ticker": by_ticker,
        "ranked": ranked,
        "market_summary": summary,
        "regime_changed_from_prior": (prior_regime != summary.get("regime")) if prior_regime else False,
    }

    s3_put_json(OUTPUT_KEY, payload)
    print(f"  ✓ data/earnings-nlp.json written")

    # ─── Telegram on regime change or big shifts ───
    alert_sent = False
    big_shifts = [r for r in (ranked.get("biggest_improvers", []) + ranked.get("biggest_deteriorators", []))
                   if abs(r.get("tone_shift_pp", 0)) >= 15]
    if (prior_regime and prior_regime != summary.get("regime")) or big_shifts:
        lines = [f"🎙 *Earnings NLP · {datetime.now(timezone.utc).strftime('%b %d')}*\n",
                  f"📊 Regime: *{summary.get('regime')}*",
                  f"_{summary.get('signal')}_\n",
                  f"📈 Median tone: {summary.get('median_tone')}",
                  f"⚖️  Raises:Cuts = {summary.get('guidance_breakdown', {}).get('RAISED',0)}:{summary.get('guidance_breakdown', {}).get('LOWERED',0)}\n"]
        if prior_regime and prior_regime != summary.get("regime"):
            lines.insert(2, f"_(was {prior_regime})_")
        if big_shifts[:3]:
            lines.append("⚡ Big shifts:")
            for s in big_shifts[:5]:
                sign = "+" if s["tone_shift_pp"] > 0 else ""
                lines.append(f"  • {s['ticker']}: {sign}{s['tone_shift_pp']}pp ({s.get('guidance','?')})")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print("  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "n_tickers": len(UNIVERSE),
        "n_with_data": n_with_data,
        "n_with_err": n_with_err,
        "regime": summary.get("regime"),
        "median_tone": summary.get("median_tone"),
        "n_improvers": len(ranked.get("biggest_improvers", [])),
        "n_deteriorators": len(ranked.get("biggest_deteriorators", [])),
        "alert_sent": alert_sent,
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
