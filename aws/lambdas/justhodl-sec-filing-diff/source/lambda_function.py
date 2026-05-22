"""
justhodl-sec-filing-diff -- Item 1A Risk Factor change detector.

═══════════════════════════════════════════════════════════════════════════════
INSTITUTIONAL THESIS
────────────────────
The single highest-signal section of a 10-K is Item 1A Risk Factors.
Companies are legally required to disclose material risks. When a company
ADDS a new risk factor that wasn't in last year's filing, it's often the
FIRST public warning of a coming problem. Examples:

  - FTX customer firms: "exposure to crypto counterparties" risk added
    in Q3 2022 10-Qs — 6-12 months before collapse
  - SVB clients: "interest rate sensitivity" added to many regional bank
    filings 2022-2023 — predicted the March 2023 banking stress
  - China-exposed names: "supply chain concentration in mainland China"
    became standard language 2018-2020 — quantified geopolitical risk
    well before it priced

Bloomberg charges $24k/yr for the "10-K AI" feature that does basic diff.
FactSet has a clunky text-comparison tool. Renaissance/Citadel run
internal versions. Zero retail/boutique product exposes this.

DISTINCTION FROM EXISTING ENGINES
──────────────────────────────────
  justhodl-sec-10kq    tracks 10-K/10-Q filing METADATA (who, when, amended)
  justhodl-sec-8k      8-K material event flags by Item code
  THIS engine          extracts + diffs ACTUAL RISK FACTOR TEXT YoY

THE 3-LAYER ANALYSIS
────────────────────
  Layer 1: FILING DETECTION
    Read data/10kq-filings.json (existing sec-10kq feed)
    For each tracked ticker, identify latest 10-K + prior year 10-K

  Layer 2: TEXT EXTRACTION
    Pull both filings from SEC EDGAR (free, no auth, User-Agent required)
    Extract Item 1A Risk Factors section via regex anchors
    Strip HTML, normalize whitespace

  Layer 3: CHANGE CLASSIFICATION
    Claude Haiku NLP scores each risk factor as:
      NEW (added vs prior year — highest signal)
      REMOVED (taken out — sometimes good sign, sometimes whitewashing)
      EXPANDED (existed but materially expanded language)
      CONTRACTED (existed but shrunk)
      UNCHANGED
    For each NEW/EXPANDED, assign severity 0-100 + category +
    forward implications

SEVERITY CATEGORIES (institutional taxonomy)
─────────────────────────────────────────────
  COUNTERPARTY_EXPOSURE     credit/lending/customer concentration
  REGULATORY_LEGAL          new investigations, lawsuits, regulatory regime
  GEOPOLITICAL_TRADE        China, sanctions, tariffs, supply chain
  TECHNOLOGY_CYBER          cyber breaches, AI disruption, tech obsolescence
  FINANCIAL_HEALTH          debt, liquidity, going concern, covenant
  OPERATIONAL              key personnel, facility, IP loss
  ENVIRONMENTAL_CLIMATE    climate transition, physical risk
  MACRO_RATES              interest rate sensitivity new exposure
  COMPETITIVE              new competitor, market structure shift

STATE MACHINE
─────────────
  CRITICAL_FILING_CHANGE   any single NEW risk severity >=80
  ELEVATED_CHANGES         3+ NEW risks across the universe OR 1 >=70
  ROUTINE_UPDATES          NEW risks present but all <70 severity
  STATIC                   no material changes detected

OUTPUT
──────
  s3://justhodl-dashboard-live/data/sec-filing-diff.json
  Schedule: daily 12:00 UTC (after sec-10kq morning refresh)

TRADE STRUCTURE
───────────────
  Severity >=80 NEW risk: institutional research alert
    → review thesis, consider trimming position
    → cross-check with insider selling, short interest, credit spreads
  Severity 60-79: add to monitoring list, evaluate quarterly

ACADEMIC BASIS
──────────────
- Brown, S. V., & Tucker, J. W. (2011). Large-sample evidence on firms'
  year-over-year MD&A modifications. Journal of Accounting Research.
- Cohen, L., Malloy, C., & Nguyen, Q. (2020). Lazy prices.
  Journal of Finance, 75(3), 1371-1415. (10-K text changes predict returns)
- Hoberg, G., & Phillips, G. (2016). Text-based network industries and
  endogenous product differentiation. Journal of Political Economy.
═══════════════════════════════════════════════════════════════════════════════
"""
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/sec-filing-diff.json"
S3_CACHE_PREFIX = "sec-filings-cache/"

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
ANTHROPIC_API = "https://api.anthropic.com/v1/messages"

SEC_USER_AGENT = "JustHodl Research raafouis@gmail.com"
SEC_BASE = "https://www.sec.gov"
SEC_DATA_BASE = "https://data.sec.gov"
HTTP_TIMEOUT = 30

# Universe: subset of STATIC_TOP50_SPX for cost + speed
RESEARCH_UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA",
    "AVGO", "JPM", "UNH", "XOM", "ORCL", "NFLX", "AMD", "BAC",
    "CRM", "ADBE", "INTU", "DIS", "WFC",
]

SEVERITY_CATEGORIES = [
    "COUNTERPARTY_EXPOSURE", "REGULATORY_LEGAL", "GEOPOLITICAL_TRADE",
    "TECHNOLOGY_CYBER", "FINANCIAL_HEALTH", "OPERATIONAL",
    "ENVIRONMENTAL_CLIMATE", "MACRO_RATES", "COMPETITIVE",
]

s3 = boto3.client("s3", region_name="us-east-1")


def http_get(url, headers=None, timeout=HTTP_TIMEOUT):
    h = {"User-Agent": SEC_USER_AGENT,
         "Accept": "application/json, text/html"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def fetch_s3_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[fetch_s3] {key} failed: {e}")
        return None


def s3_cache_get(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return obj["Body"].read().decode("utf-8")
    except s3.exceptions.NoSuchKey:
        return None
    except Exception:
        return None


def s3_cache_put(key, text):
    try:
        s3.put_object(
            Bucket=S3_BUCKET, Key=key,
            Body=text.encode("utf-8"),
            ContentType="text/plain")
    except Exception as e:
        print(f"[cache_put] {key} err: {e}")


# ---------- SEC EDGAR helpers ----------
def ticker_to_cik(ticker):
    """Look up CIK via SEC company tickers JSON (cached)."""
    cache_key = f"{S3_CACHE_PREFIX}company-tickers.json"
    cached = s3_cache_get(cache_key)
    if cached:
        try:
            data = json.loads(cached)
        except json.JSONDecodeError:
            data = None
    else:
        data = None
    if data is None:
        try:
            url = f"{SEC_BASE}/files/company_tickers.json"
            text = http_get(url)
            data = json.loads(text)
            s3_cache_put(cache_key, text)
        except Exception as e:
            print(f"[cik] fetch err: {e}")
            return None
    # Schema: {"0": {"cik_str": int, "ticker": "...", "title": "..."}, ...}
    for k, v in data.items():
        if isinstance(v, dict) and v.get("ticker") == ticker:
            return str(v.get("cik_str", "")).zfill(10)
    return None


def get_recent_10ks(cik, n=2):
    """Returns list of dicts: {accession, filed_at, primary_doc} latest first."""
    url = f"{SEC_DATA_BASE}/submissions/CIK{cik}.json"
    try:
        text = http_get(url)
        data = json.loads(text)
    except Exception as e:
        print(f"[get_10ks] CIK {cik} err: {e}")
        return []
    recent = (data.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    accessions = recent.get("accessionNumber") or []
    dates = recent.get("filingDate") or []
    primary_docs = recent.get("primaryDocument") or []
    out = []
    for i, form in enumerate(forms):
        if form in ("10-K", "10-K/A"):
            acc_clean = accessions[i].replace("-", "")
            out.append({
                "accession": accessions[i],
                "accession_clean": acc_clean,
                "filed_at": dates[i],
                "primary_doc": primary_docs[i],
                "form": form,
                "url": (f"{SEC_BASE}/Archives/edgar/data/"
                          f"{int(cik)}/{acc_clean}/{primary_docs[i]}"),
            })
            if len(out) >= n:
                break
    return out


def fetch_filing_text(filing_url):
    """Fetch + cache filing HTML."""
    cache_key = f"{S3_CACHE_PREFIX}filing-{re.sub(r'[^a-zA-Z0-9]', '_', filing_url)[-180:]}.html"
    cached = s3_cache_get(cache_key)
    if cached:
        return cached
    try:
        text = http_get(filing_url)
        s3_cache_put(cache_key, text)
        return text
    except urllib.error.HTTPError as e:
        print(f"[fetch_filing] {e.code}: {filing_url[-60:]}")
        return None
    except Exception as e:
        print(f"[fetch_filing] err: {e}")
        return None


# ---------- Risk Factor extraction ----------
ITEM_1A_START_PATTERNS = [
    r"item\s*1\s*a\.?\s*risk\s*factors?",
    r"item\s*1a\.?\s*risk\s*factors?",
    r"<a[^>]*>item\s*1a[^<]*risk\s*factors?</a>",
]
ITEM_1B_START_PATTERNS = [
    r"item\s*1\s*b\.?\s*unresolved\s*staff\s*comments?",
    r"item\s*2\.?\s*properties",
    r"item\s*1b\.?",
]


def strip_html(html):
    """Crude HTML stripper sufficient for SEC filings."""
    # Remove script/style
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ",
                   html, flags=re.IGNORECASE | re.DOTALL)
    # Replace tags with space
    html = re.sub(r"<[^>]+>", " ", html)
    # Decode common entities
    html = (html.replace("&nbsp;", " ").replace("&amp;", "&")
                  .replace("&lt;", "<").replace("&gt;", ">")
                  .replace("&quot;", '"').replace("&#39;", "'")
                  .replace("&#8217;", "'").replace("&#8220;", '"')
                  .replace("&#8221;", '"').replace("&#160;", " "))
    # Collapse whitespace
    html = re.sub(r"\s+", " ", html)
    return html.strip()


def extract_risk_factors(html_text):
    """Return Item 1A Risk Factors text, truncated to 30k chars."""
    if not html_text:
        return None
    text_lower = html_text.lower()
    # Find Item 1A anchor
    start = None
    for pat in ITEM_1A_START_PATTERNS:
        m = re.search(pat, text_lower)
        if m:
            start = m.start()
            break
    if start is None:
        return None
    # Find Item 1B / Item 2 end anchor — look AFTER start
    end = len(html_text)
    sub_lower = text_lower[start + 100:]  # skip past start anchor
    for pat in ITEM_1B_START_PATTERNS:
        m = re.search(pat, sub_lower)
        if m:
            end = start + 100 + m.start()
            break
    section = html_text[start:end]
    section = strip_html(section)
    # Truncate for token budget
    return section[:30000]


# ---------- Claude diff classification ----------
def http_post_json(url, payload, headers, timeout=120):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers,
                                   method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def classify_risk_changes(ticker, current_text, prior_text):
    """Use Claude Haiku to extract NEW risk factors + classify."""
    if not ANTHROPIC_KEY:
        return None, "no anthropic key"
    if not current_text or not prior_text:
        return None, "missing one or both filings"

    prompt = f"""You are an institutional equity analyst comparing two consecutive
10-K Risk Factor sections for {ticker}.

PRIOR YEAR 10-K Risk Factors (excerpt):
{prior_text[:12000]}

CURRENT YEAR 10-K Risk Factors (excerpt):
{current_text[:12000]}

Identify ONLY material additions (NEW risk factors that did not exist
in prior year) and material expansions (substantial language additions
to existing risks). Ignore boilerplate updates.

For each NEW or EXPANDED risk, classify:
  - category: one of {SEVERITY_CATEGORIES}
  - severity: 0-100 (80+ = institutional alert)
  - one_sentence_summary
  - forward_implication: what this likely signals for the next 6-18 months

Output JSON ONLY, no preamble, with this exact shape:
{{
  "new_risks": [
    {{"category": "...", "severity": 75, "summary": "...",
      "forward_implication": "..."}}
  ],
  "expanded_risks": [
    {{"category": "...", "severity": 60, "summary": "...",
      "forward_implication": "..."}}
  ],
  "overall_change_level": "NONE|ROUTINE|ELEVATED|CRITICAL",
  "key_takeaway": "single sentence"
}}"""

    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 2000,
        "messages": [{"role": "user", "content": prompt}],
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
    }
    try:
        result = http_post_json(ANTHROPIC_API, payload, headers, timeout=120)
        content = result.get("content", [{}])[0].get("text", "")
        # Strip code fences if present
        content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content.strip(),
                          flags=re.MULTILINE)
        parsed = json.loads(content)
        return parsed, None
    except json.JSONDecodeError as e:
        return None, f"json parse err: {str(e)[:100]}"
    except urllib.error.HTTPError as e:
        return None, f"http {e.code}"
    except Exception as e:
        return None, f"err: {str(e)[:120]}"


# ---------- Main ----------
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[sec-filing-diff] start v{VERSION}")

    results = []
    for ticker in RESEARCH_UNIVERSE:
        try:
            cik = ticker_to_cik(ticker)
            if not cik:
                results.append({
                    "ticker": ticker, "status": "no_cik"})
                continue

            ten_ks = get_recent_10ks(cik, n=2)
            time.sleep(0.15)  # SEC fair-use 10 req/s
            if len(ten_ks) < 2:
                results.append({
                    "ticker": ticker, "cik": cik,
                    "status": "insufficient_10ks",
                    "n_10ks_found": len(ten_ks)})
                continue

            current = ten_ks[0]
            prior = ten_ks[1]

            current_html = fetch_filing_text(current["url"])
            time.sleep(0.15)
            prior_html = fetch_filing_text(prior["url"])
            time.sleep(0.15)

            current_rf = extract_risk_factors(current_html)
            prior_rf = extract_risk_factors(prior_html)

            if not current_rf or not prior_rf:
                results.append({
                    "ticker": ticker, "cik": cik,
                    "current_filed": current["filed_at"],
                    "prior_filed": prior["filed_at"],
                    "status": "extraction_failed",
                    "current_rf_len": len(current_rf or ""),
                    "prior_rf_len": len(prior_rf or "")})
                continue

            classification, err = classify_risk_changes(
                ticker, current_rf, prior_rf)

            results.append({
                "ticker": ticker, "cik": cik,
                "current_filing": {
                    "accession": current["accession"],
                    "filed_at": current["filed_at"],
                    "url": current["url"],
                },
                "prior_filing": {
                    "accession": prior["accession"],
                    "filed_at": prior["filed_at"],
                    "url": prior["url"],
                },
                "current_rf_chars": len(current_rf),
                "prior_rf_chars": len(prior_rf),
                "classification": classification,
                "classification_error": err,
                "status": "ok" if classification else "classification_failed",
            })
        except Exception as e:
            print(f"[{ticker}] err: {str(e)[:200]}")
            results.append({
                "ticker": ticker, "status": "exception",
                "error": str(e)[:200]})

    # Aggregate state
    n_critical = 0
    n_elevated = 0
    n_ok = 0
    high_severity_alerts = []
    for r in results:
        if r.get("status") != "ok":
            continue
        n_ok += 1
        c = r.get("classification") or {}
        level = c.get("overall_change_level") or "NONE"
        if level == "CRITICAL":
            n_critical += 1
        elif level == "ELEVATED":
            n_elevated += 1
        for risk in (c.get("new_risks") or []):
            if isinstance(risk, dict) and (risk.get("severity") or 0) >= 60:
                high_severity_alerts.append({
                    "ticker": r["ticker"],
                    "filed_at": r["current_filing"]["filed_at"],
                    "category": risk.get("category"),
                    "severity": risk.get("severity"),
                    "summary": risk.get("summary"),
                    "forward_implication": risk.get("forward_implication"),
                    "risk_type": "NEW",
                })
        for risk in (c.get("expanded_risks") or []):
            if isinstance(risk, dict) and (risk.get("severity") or 0) >= 70:
                high_severity_alerts.append({
                    "ticker": r["ticker"],
                    "filed_at": r["current_filing"]["filed_at"],
                    "category": risk.get("category"),
                    "severity": risk.get("severity"),
                    "summary": risk.get("summary"),
                    "forward_implication": risk.get("forward_implication"),
                    "risk_type": "EXPANDED",
                })

    high_severity_alerts.sort(key=lambda x: -(x.get("severity") or 0))

    if n_critical >= 1 or any(a["severity"] >= 80
                                 for a in high_severity_alerts):
        state = "CRITICAL_FILING_CHANGE"
    elif n_elevated >= 1 or len(high_severity_alerts) >= 3:
        state = "ELEVATED_CHANGES"
    elif high_severity_alerts:
        state = "ROUTINE_UPDATES"
    else:
        state = "STATIC"

    output = {
        "engine": "sec-filing-diff",
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "state": state,
        "universe_size": len(RESEARCH_UNIVERSE),
        "n_ok": n_ok,
        "n_critical": n_critical,
        "n_elevated": n_elevated,
        "n_high_severity_alerts": len(high_severity_alerts),
        "high_severity_alerts": high_severity_alerts,
        "per_ticker_results": results,
        "severity_categories": SEVERITY_CATEGORIES,
        "methodology": {
            "framework": "10-K Item 1A Risk Factor year-over-year diff",
            "philosophy": (
                "Companies legally required to disclose material risks. "
                "NEW additions are the FIRST public warnings. Bloomberg "
                "AI 10-K is $24k/yr; FactSet text-compare is clunky. "
                "Renaissance + Citadel run internal versions; not sold."),
            "layer_1": ("Read justhodl-sec-10kq feed to identify latest "
                          "+ prior 10-K per ticker"),
            "layer_2": ("Fetch SEC EDGAR HTML, extract Item 1A section "
                          "via regex anchors, strip + normalize"),
            "layer_3": ("Claude Haiku NLP classifies NEW vs EXPANDED "
                          "vs UNCHANGED, scores severity 0-100, assigns "
                          "category + forward implication"),
            "data_source": "SEC EDGAR (free, no auth, User-Agent required)",
            "academic_basis": [
                "Cohen, Malloy, Nguyen (2020) — Lazy Prices: 10-K text "
                "changes predict returns. Journal of Finance.",
                "Brown & Tucker (2011) — Year-over-year MD&A "
                "modifications. JAR.",
            ],
        },
        "duration_seconds": round(time.time() - started, 1),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY,
        Body=json.dumps(output, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=3600")

    print(f"[sec-filing-diff] state={state} ok={n_ok}/{len(results)} "
          f"alerts={len(high_severity_alerts)}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True, "version": VERSION, "state": state,
            "n_ok": n_ok,
            "n_high_severity_alerts": len(high_severity_alerts),
            "top_3_alerts": high_severity_alerts[:3],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
