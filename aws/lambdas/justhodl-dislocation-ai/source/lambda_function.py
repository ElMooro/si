"""justhodl-dislocation-ai — AI analyst layer for the dislocation detector

For each top buy-the-laggard candidate:
  1. THEME classification (rule-based + keyword) — AI Compute, Power/Energy,
     Defense, Biotech, Fintech, Cybersecurity, Robotics, Nuclear, etc.
  2. BACKLOG extraction from the latest earnings-call transcript (FMP),
     keyword-scan for "backlog/RPO/committed contracted" + a $ figure.
  3. AI SUMMARY (Claude): what the company does, why it's cheap vs its richer
     peer, whether it's genuinely cheap or a value trap, and a 12-mo PRICE
     TARGET derived from forward revenue × peer-justified EV/Sales multiple,
     cross-checked vs analyst PT.

Reads:  data/dislocations.json (the ranked list)
Writes: data/dislocation-ai.json   { by_ticker: {TICKER: {...}}, by_theme: {...} }
Schedule: daily 15:15 UTC (after dislocation-detector at 14:30).
"""
import json, os, re, time, urllib.request, urllib.parse
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/dislocation-ai.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
MODEL = "claude-haiku-4-5-20251001"
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def read_json(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def http_json(url, t=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=t) as r: return json.loads(r.read().decode())
    except Exception: return None


def anthropic_key():
    if os.environ.get("ANTHROPIC_API_KEY"): return os.environ["ANTHROPIC_API_KEY"]
    for p in ["/justhodl/anthropic/api_key", "/anthropic/api_key"]:
        try: return ssm.get_parameter(Name=p, WithDecryption=True)["Parameter"]["Value"]
        except Exception: continue
    return ""


def call_claude(prompt, system, max_tokens=700):
    key = anthropic_key()
    if not key: return ""
    body = {"model": MODEL, "max_tokens": max_tokens, "system": system,
            "messages": [{"role": "user", "content": prompt}]}
    req = urllib.request.Request("https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "anthropic-version": "2023-06-01", "x-api-key": key},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            data = json.loads(r.read().decode())
        return "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
    except Exception as e:
        print(f"[claude] {e}"); return ""


# ── Theme classification ──
THEMES = {
    "AI Compute / Data Center": ["gpu","data center","datacenter","ai infrastructure","accelerat","hyperscal","neocloud","inference","training cluster","cloud comput","colocation"],
    "Power / Energy Infrastructure": ["power","grid","electric","utility","transmission","renewable","solar","wind","turbine","transformer","energy storage","battery"],
    "Nuclear / SMR": ["nuclear","uranium","smr","reactor","enrichment"],
    "Semiconductors": ["semiconductor","chip","foundry","wafer","fabless","lithography","packaging"],
    "Defense / Aerospace": ["defense","aerospace","missile","radar","munition","military","drone","space"],
    "Cybersecurity": ["cybersecurity","security software","endpoint","zero trust","threat","firewall"],
    "Biotech / Pharma": ["biotech","pharmaceutical","therapeut","clinical","oncology","drug","fda","trial"],
    "Fintech / Payments": ["fintech","payment","neobank","lending","processing","merchant","card network"],
    "Robotics / Automation": ["robot","automation","autonomous","machine vision","industrial automation"],
    "Software / SaaS": ["saas","software","platform","subscription","cloud software","arr"],
    "Quantum": ["quantum"],
    "EV / Mobility": ["electric vehicle","ev ","charging","autonomous driving","mobility"],
}


def classify_theme(name, industry, sector, description=""):
    text = f"{name} {industry} {sector} {description}".lower()
    best, score = None, 0
    for theme, kws in THEMES.items():
        s = sum(1 for k in kws if k in text)
        if s > score: best, score = theme, s
    return best or (industry or sector or "Other")


def extract_backlog(ticker):
    """Scan the latest earnings transcript for backlog / RPO figures."""
    tr = http_json(f"{FMP_BASE}/earning-call-transcript-latest?symbol={ticker}&apikey={FMP_KEY}") \
         or http_json(f"{FMP_BASE}/earning-call-transcript?symbol={ticker}&limit=1&apikey={FMP_KEY}")
    if not tr: return None
    content = ""
    if isinstance(tr, list) and tr: content = tr[0].get("content") or tr[0].get("transcript") or ""
    elif isinstance(tr, dict): content = tr.get("content") or tr.get("transcript") or ""
    if not content: return None
    low = content.lower()
    hits = []
    for kw in ["backlog", "remaining performance obligation", "rpo", "committed", "contracted revenue", "bookings"]:
        for m in re.finditer(kw, low):
            seg = content[max(0, m.start()-90): m.start()+140]
            dollar = re.search(r"\$\s?[\d.,]+\s?(billion|million|bn|mn|b|m)?", seg, re.I)
            if dollar:
                hits.append({"term": kw, "snippet": seg.strip().replace("\n", " ")[:200], "figure": dollar.group(0)})
                break
    return hits[:3] if hits else None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    disl = read_json("data/dislocations.json") or {}
    candidates = (disl.get("buy_the_laggard") or [])[:25]
    print(f"[disloc-ai] analyzing {len(candidates)}")

    cache = read_json(OUT_KEY) or {"by_ticker": {}}
    by_ticker = cache.get("by_ticker") or {}
    cutoff = time.time() - 20 * 3600  # 20h cache

    system = ("You are a buy-side equity analyst. Given a relative-value dislocation "
              "(a company trading cheap vs a richer industry peer), write a crisp, "
              "honest read. Be decisive but flag value traps. Derive a 12-month price "
              "target from forward revenue × a peer-justified EV/Sales multiple, "
              "cross-checked against fundamentals (growth, margin, backlog) and the "
              "analyst consensus PT. Never hype. Output STRICT JSON only.")

    for c in candidates:
        tk = c["ticker"]
        cached = by_ticker.get(tk)
        if cached:
            try:
                if datetime.fromisoformat(cached.get("generated_at","")).timestamp() > cutoff:
                    continue
            except Exception: pass
        # enrich: fundamentals (fwd rev, analyst PT, profile), backlog
        fund = http_json(f"https://justhodl-data-proxy.raafouis.workers.dev/fundamentals?ticker={tk}") or {}
        backlog = extract_backlog(tk)
        theme = classify_theme(c.get("name",""), c.get("industry",""), c.get("sector",""), fund.get("description",""))
        vs = c.get("dislocated_vs") or {}

        ctx = {
            "ticker": tk, "name": c.get("name"), "industry": c.get("industry"), "theme": theme,
            "price": fund.get("price"), "market_cap": c.get("market_cap"),
            "ev_sales": c.get("ev_sales"), "rev_growth_pct": c.get("rev_growth_pct"),
            "gross_margin": c.get("gross_margin"), "op_margin": c.get("op_margin"),
            "rule_of_40": c.get("rule_of_40"), "roic": c.get("roic"),
            "dislocation_score": c.get("dislocation_score"),
            "cheaper_than": {"peer": vs.get("ticker"), "peer_ev_sales": vs.get("ev_sales"),
                              "peer_premium_pct": vs.get("ev_sales_premium_pct")},
            "forward_estimates": fund.get("estimates"), "analyst_pt": fund.get("analystPT"),
            "pe": fund.get("pe"), "beta": fund.get("beta"),
            "backlog_mentions": backlog,
            "caveats": c.get("caveats"),
        }
        prompt = (f"Dislocation candidate:\n{json.dumps(ctx, default=str)}\n\n"
                  "Return STRICT JSON with keys: "
                  '{"summary": "<2-3 sentences: what the company does + the dislocation>", '
                  '"cheap_verdict": "CHEAP|FAIR|VALUE_TRAP|EXPENSIVE", '
                  '"verdict_reason": "<1-2 sentences>", '
                  '"price_target_12m": <number or null>, '
                  '"pt_upside_pct": <number or null>, '
                  '"pt_basis": "<how you derived it: fwd rev x multiple, etc>", '
                  '"key_risks": "<1 sentence>", '
                  '"backlog_note": "<1 sentence on backlog/RPO if found, else null>"}')
        raw = call_claude(prompt, system, max_tokens=650)
        parsed = None
        if raw:
            m = re.search(r"\{.*\}", raw, re.DOTALL)
            if m:
                try: parsed = json.loads(m.group(0))
                except Exception: parsed = None
        if not parsed:
            continue
        by_ticker[tk] = {
            **parsed, "theme": theme, "industry": c.get("industry"),
            "dislocation_score": c.get("dislocation_score"), "price": fund.get("price"),
            "dislocated_vs": vs.get("ticker"), "backlog": backlog,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        print(f"  ✓ {tk}: {parsed.get('cheap_verdict')} PT={parsed.get('price_target_12m')}")

    # group by theme + industry
    by_theme, by_industry = {}, {}
    for tk, a in by_ticker.items():
        by_theme.setdefault(a.get("theme") or "Other", []).append(tk)
        by_industry.setdefault(a.get("industry") or "Other", []).append(tk)

    out = {
        "engine": "dislocation-ai", "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": MODEL, "n_analyzed": len(by_ticker),
        "by_ticker": by_ticker,
        "by_theme": {k: sorted(v, key=lambda x: -(by_ticker[x].get("dislocation_score") or 0)) for k, v in by_theme.items()},
        "by_industry": {k: v for k, v in by_industry.items()},
        "themes_ranked": sorted(by_theme.keys(), key=lambda k: -len(by_theme[k])),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[disloc-ai] DONE {round(time.time()-t0,1)}s — {len(by_ticker)} analyzed, {len(by_theme)} themes")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "analyzed": len(by_ticker),
                                                     "themes": len(by_theme)})}
