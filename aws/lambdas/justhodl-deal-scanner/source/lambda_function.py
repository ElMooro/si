"""
justhodl-deal-scanner — FRESH DEAL WINS NOT YET IN REVENUE
==========================================================
Scans company press releases (the internet, not filings) for fresh CONTRACT /
ORDER / SUPPLY WINS, parses the deal size, and cross-references the company's
revenue to flag forward revenue the tape hasn't priced in yet — weighted HARDEST
for small caps, where a single contract can be transformative (deal > annual rev).

Complements forward-orders (RPO/backlog from filings) + sec-8k (Item 1.01) with
the real-time NEWS angle: a $500M multi-year award announced today won't show in
reported revenue for several quarters, but the stock can move now.

SOURCE: FMP /stable/news/press-releases-latest (official PRs across all tickers)
        + /stable/income-statement (revenue) + universe / FMP profile (market cap)
OUTPUT data/deal-scanner.json   SCHEDULE every 3 hours (8 runs/day).
v2.0.0: full-market boards (by_sector / by_cap / coverage, all 11 sectors, nano→mega)
+ graded deal-win signals [5,21,63] vs SPY via shared signals_emit. Real data, research only.
"""
import json
import re
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime

import boto3

VERSION = "2.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/deal-scanner.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
s3 = boto3.client("s3", region_name="us-east-1")

CAP_BOOST = {"nano": 35, "micro": 28, "small": 20, "mid": 8, "large": 3, "mega": 0}
SMALL_BUCKETS = {"nano", "micro", "small"}

AI_RE = re.compile(r'\bAI\b')
AI_PHRASES = (
    "a.i.", "artificial intelligence", "gpu", "gpus", "data center", "datacenter", "data-center",
    "inference", "hyperscaler", "hyperscale", "nvidia", "large language model", "llm",
    "machine learning", "accelerator", "high-performance computing", " hpc ", "neocloud",
    "gpu cloud", "cloud compute", "compute capacity", "h100", "h200", "gb200", "blackwell",
    "model training", "ai cluster", "ai infrastructure", "ai chip", "ai compute", "generative ai",
    "ai server", "ai workload", "supercomputer", "foundation model", "ai data center",
)

PUB_BLOCK = ("motley fool", "seeking alpha", "investorplace", "zacks", "tipranks",
             "24/7 wall", "247wall", "247 wall", "insider monkey", "simply wall", "benzinga insights")

# strong deal-win language (must hit at least one)
# STRONG terms qualify on their own (high-confidence contract/order/supply wins)
STRONG_DEAL = (
    "awarded a contract", "awarded the contract", "awarded contract", "wins contract",
    "win contract", "wins a contract", "wins bid", "won bid", "wins order", "secures contract",
    "secures order", "secures a contract", "purchase order", "supply agreement", "supply contract",
    "contract worth", "contract valued", "order worth", "order valued", "design win",
    "receives order", "receives purchase order", "new contract", "contract award", "wins deal",
    "lands contract", "selected to supply", "selected as supplier", "selected as the supplier",
    "framework agreement", "procurement contract", "wins $", "awarded $", "bookings worth",
)
# WEAK terms qualify ONLY if a deal size is parsed (a sized partnership/MOU is real; a bare one is noise)
WEAK_DEAL = (
    "partnership", "collaboration", "agreement", "memorandum of understanding", " mou ",
    "letter of intent", "multi-year", "multiyear", "to supply", "to provide", "to deliver",
    "secures", "selected to", "joint venture", "supply", "strategic agreement",
)
# hard exclusions (financing/governance/earnings PRs are not deal wins)
DEAL_NEG = (
    "dividend", "quarterly results", "earnings", "to present at", "webcast", "conference call",
    "prices $", "pricing of", "offering of", "public offering", "private placement", "stock split",
    "annual meeting", "appoints", "names new", "names ceo", "share repurchase", "buyback",
    "investor day", "to report", "schedules", "to participate", "completes offering", "registered direct",
    "reverse split", "regains compliance", "files for", "prospectus",
    "to sell", "sell its", "sale of", "to acquire", "to be acquired", "divest", "to merge",
    "innovation award", "wins award", "awards for", "best places", "named to the", "brand ambassador",
    "sponsorship", "wins two", "ranked ", "recognized as", "voucher",
    "credit facility", "loan agreement", "term loan", "senior notes", "notes due", "convertible note",
    "acquires", "acquisition", "provides update", "update on", "ongoing operations", "training",
    "education program", "feasibility", "to evaluate", "explores",
    "report", "facility", "prepayment", "welcomes", "leadership", "summit", "survey",
    "white paper", "outlook", "research report", "index report", "ventures",
    "?", "can it", "could ", "why ", "selloff", "sell-off", "amasses", "cash reserve",
    "stock", "shares", "analyst", "upgrade", "downgrade", "price target", "should you",
    "is it ", "what to know", " vs ", "approval", "yesterday", "rating", "earnings",
    "here's what", "here's why", "means for", "could mean", "going public", " ipo", "to ipo",
    "investment", "closing of", "buying", "to buy", "what it means",
)
SIZE_RE = re.compile(r'\$\s?([\d][\d,]*(?:\.\d+)?)\s*(billion|bn|million|mn|b|m)\b', re.I)
MULT = {"billion": 1e9, "bn": 1e9, "b": 1e9, "million": 1e6, "mn": 1e6, "m": 1e6}


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _fmp(path, retries=2):
    url = f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
    for attempt in range(retries + 1):
        try:
            raw = urllib.request.urlopen(
                urllib.request.Request(url, headers={"User-Agent": "jh-deal"}), timeout=20).read()
            return json.loads(raw)
        except Exception:
            if attempt < retries:
                time.sleep(1.2 * (attempt + 1))
                continue
            return None


def fetch_news(pages=14, limit=100):
    """Scan BOTH official company PRs and third-party financial news (catches widely-reported
    billion-dollar deals that aren't self-announced)."""
    out = []

    def one(args):
        feed, p = args
        d = _fmp(f"news/{feed}?page={p}&limit={limit}")
        items = d if isinstance(d, list) else []
        tr = "pr" if feed == "press-releases-latest" else "news"
        org = "fmp_pr" if feed == "press-releases-latest" else "fmp_news"
        for it in items:
            it["trust"] = tr
            it["origin"] = org
        return items
    tasks = [("press-releases-latest", p) for p in range(pages)] + \
            [("stock-latest", p) for p in range(pages)]
    with ThreadPoolExecutor(max_workers=6) as ex:
        for r in ex.map(one, tasks):
            out.extend(r)
    return out


def _http_json(url, timeout=15):
    try:
        raw = urllib.request.urlopen(urllib.request.Request(
            url, headers={"User-Agent": "jh-deal", "Accept": "application/json"}), timeout=timeout).read()
        return json.loads(raw)
    except Exception:
        return None


def fetch_polygon(limit=100, pages=8):
    """Polygon news — wide third-party coverage, multi-ticker tagged."""
    out, url = [], (f"https://api.polygon.io/v2/reference/news?limit={limit}"
                    f"&order=desc&sort=published_utc&apiKey={POLYGON_KEY}")
    for _ in range(pages):
        j = _http_json(url)
        if not isinstance(j, dict):
            break
        for a in j.get("results", []) or []:
            tks = a.get("tickers") or []
            if not tks:
                continue
            out.append({"symbol": tks[0], "title": a.get("title"),
                        "text": a.get("description") or "",
                        "publishedDate": (a.get("published_utc") or "").replace("T", " ")[:19],
                        "publisher": (a.get("publisher") or {}).get("name") or "Polygon",
                        "url": a.get("article_url"), "trust": "news", "origin": "polygon"})
        nu = j.get("next_url")
        if not nu:
            break
        url = nu + f"&apiKey={POLYGON_KEY}"
    return out


def _largest(blob):
    best, bstr = 0.0, None
    for m in SIZE_RE.finditer(blob or ""):
        val = _num(m.group(1).replace(",", ""))
        if val is None:
            continue
        val *= MULT.get(m.group(2).lower(), 1)
        if val > 1e11:        # >$100B: macro/industry figure misparse, not a single deal
            continue
        if val > best:
            best, bstr = val, m.group(0).strip()
    return best, bstr


DEAL_CONTEXT = re.compile(
    r'(contract|order|award|deal|agreement|supply|valued|worth|purchase|booking|backlog|'
    r'grant|funding|program|revenue|subcontract|task order|delivery order|procure)', re.I)

_NAME_SUFFIX = re.compile(
    r'\b(inc|incorporated|corp|corporation|ltd|limited|plc|holdings|holding|company|co|group|'
    r'llc|sa|ag|nv|the|and|technologies|technology|industries|international|systems|solutions|'
    r'pharmaceuticals|therapeutics|biosciences|energy|capital|partners|enterprises)\b', re.I)


def name_tokens(name):
    if not name:
        return []
    n = _NAME_SUFFIX.sub(" ", name.lower())
    return [t for t in re.findall(r"[a-z]{4,}", n)]


DEAL_VERB = re.compile(
    r'\b(wins?|won|secures?|secured|awarded|awards?|receives?|received|lands?|signs?|signed|'
    r'forms?|announces?|announced|expands?|expanded|selected|enters?|entered|completes?|'
    r'closes?|gets?|gains?|inks?|bags?|clinches?|to\s+supply|to\s+provide|to\s+build)\b', re.I)


def lead_company_matches(name, symbol, title):
    """Match the ticker against the ANNOUNCER — the company named before the first deal verb.
    A title that merely contains a $ figure isn't enough (it may belong to a different company
    in the headline). Decisive gate against mis-tagged PRs whose ticker symbol or name
    coincidentally appears mid-headline (NASA 'CLPS' program, 'Joint Venture' → JYNT)."""
    toks = name_tokens(name or "")
    m = DEAL_VERB.search(title or "")
    lead = ((title or "")[:m.start()] if m else (title or "")[:46]).lower()
    if toks:
        return any(t in lead for t in toks)
    return bool(re.search(r'\b' + re.escape((symbol or "").lower()) + r'\b', lead))


def parse_value(title, text):
    tv, ts = _largest(title)             # prefer the figure in the headline
    if tv > 0:
        return tv, ts
    # text fallback — ONLY accept a figure that sits near deal-context language, so we don't
    # grab unrelated numbers (industry/ecosystem/market-size figures in the body)
    blob = (text or "")[:900]
    best_v, best_s = 0.0, None
    for m in SIZE_RE.finditer(blob):
        a, b = max(0, m.start() - 75), min(len(blob), m.end() + 75)
        if not DEAL_CONTEXT.search(blob[a:b]):
            continue
        val = _num(m.group(1).replace(",", ""))
        if val is None:
            continue
        val *= MULT.get(m.group(2).lower(), 1)
        if val > 1e11 or val <= 0:
            continue
        if val > best_v:
            best_v, best_s = val, m.group(0).strip()
    return (best_v if best_v > 0 else None), best_s


def is_deal(title, text, value, trust="pr"):
    t = (title or "").lower()
    if any(k in t for k in DEAL_NEG):
        return False
    blob = t + " " + (text or "")[:300].lower()
    if any(k in blob for k in STRONG_DEAL):
        return True
    # soft partnership/agreement language only trusted from actual PR wires, with a size
    if trust == "pr" and value and any(k in blob for k in WEAK_DEAL):
        return True
    return False


def revenue_and_cap(symbol, uni):
    inc = _fmp(f"income-statement?symbol={urllib.parse.quote(symbol)}&limit=1")
    rev = None
    if isinstance(inc, list) and inc:
        rev = _num(inc[0].get("revenue"))
    mu = uni.get(symbol, {})
    mc = mu.get("market_cap")
    if not mc:
        prof = _fmp(f"profile?symbol={urllib.parse.quote(symbol)}")
        if isinstance(prof, list) and prof:
            mc = _num(prof[0].get("marketCap"))
            if not mu.get("name"):
                mu = {"name": prof[0].get("companyName"), "industry": prof[0].get("industry"),
                      "sector": prof[0].get("sector")}
            elif not mu.get("sector"):
                mu = dict(mu, sector=prof[0].get("sector"))
    return rev, mc, mu


def bucket_of(mc):
    if not mc:
        return ""
    if mc < 5e7:
        return "nano"
    if mc < 3e8:
        return "micro"
    if mc < 2e9:
        return "small"
    if mc < 1e10:
        return "mid"
    if mc < 2e11:
        return "large"
    return "mega"


def highlight_tier(materiality_pct, vs_mc_pct, val):
    """GREEN = deal is BIG vs revenue or market cap (transformative). YELLOW = moderate."""
    if not val:
        return None
    big = (materiality_pct is not None and materiality_pct >= 25) or \
          (vs_mc_pct is not None and vs_mc_pct >= 10)
    mod = (materiality_pct is not None and materiality_pct >= 10) or \
          (vs_mc_pct is not None and vs_mc_pct >= 4)
    return "green" if big else "yellow" if mod else None


def load_ai_universe():
    try:
        d = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/ai-infra-stack.json")["Body"].read())
        return {n.get("symbol") for layer in d.get("stack", []) for n in layer.get("names", []) if n.get("symbol")}
    except Exception:
        return set()


def ai_tag(symbol, title, text, ai_universe):
    blob = (title or "") + " " + (text or "")
    low = blob.lower()
    kws = []
    if AI_RE.search(blob):
        kws.append("AI")
    for p in AI_PHRASES:
        if p.strip() in low:
            kws.append(p.strip())
    if symbol in ai_universe:
        kws.append("ai-infra-name")
    seen = []
    for k in kws:
        if k not in seen:
            seen.append(k)
    return (len(seen) > 0), seen[:5]


SECTOR_TO_SPDR = {
    "technology": "XLK", "information technology": "XLK",
    "financial services": "XLF", "financials": "XLF", "financial": "XLF",
    "healthcare": "XLV", "health care": "XLV",
    "industrials": "XLI", "industrial": "XLI", "energy": "XLE",
    "basic materials": "XLB", "materials": "XLB",
    "consumer defensive": "XLP", "consumer staples": "XLP",
    "consumer cyclical": "XLY", "consumer discretionary": "XLY",
    "utilities": "XLU", "real estate": "XLRE",
    "communication services": "XLC", "communications": "XLC",
}


def load_sector_signal():
    """Sector rotation scores + which SPDR sectors are rotating IN (early-momentum tailwind)."""
    try:
        d = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/sector-rotation.json")["Body"].read())
    except Exception:
        return {}, set()
    scores = {x.get("symbol"): x.get("rotation_score") for x in d.get("sectors", []) if x.get("symbol")}
    rin = set()
    ra = d.get("rotation_alerts") or {}
    if isinstance(ra, dict):
        for it in ra.get("rotating_in", []) or []:
            if isinstance(it, dict) and it.get("sym"):
                rin.add(it["sym"])
    conv_map, posture_map = {}, {}
    try:
        sfs = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/sector-flow-state.json")["Body"].read())
        for x in sfs.get("sectors", []):
            if x.get("symbol"):
                conv_map[x["symbol"]] = x.get("conviction")
                posture_map[x["symbol"]] = x.get("posture")
    except Exception:
        pass
    return scores, rin, conv_map, posture_map


def lambda_handler(event, context):
    t0 = time.time()
    try:
        universe = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/universe.json")["Body"].read())
        uni = {s["symbol"]: {"name": s.get("name"), "industry": s.get("industry"),
                             "sector": s.get("sector"), "market_cap": s.get("market_cap"),
                             "cap_bucket": s.get("cap_bucket")}
               for s in universe.get("stocks", []) if s.get("symbol")}
    except Exception:
        uni = {}

    prs = fetch_news(pages=14, limit=100)        # v2: full-market sweep, both FMP feeds, deeper pages
    prs += fetch_polygon(limit=100, pages=8)     # wider third-party coverage
    # Benzinga leg REMOVED 2026-07-15: Massive stopped serving Benzinga (403 NOT_AUTHORIZED)
    ai_universe = load_ai_universe()
    sector_scores, sector_rotating_in, sector_conv, sector_posture = load_sector_signal()
    now = datetime.now(timezone.utc)
    # dedupe + filter deals, keep freshest per (symbol,title)
    seen, deals_raw = set(), []
    for pr in prs:
        sym = pr.get("symbol")
        title = pr.get("title") or ""
        if not sym or not title:
            continue
        k = (sym, title[:80])
        if k in seen:
            continue
        seen.add(k)
        pub = (pr.get("publisher") or "").lower()
        if any(b in pub for b in PUB_BLOCK):
            continue
        val, vstr = parse_value(title, pr.get("text"))
        if not is_deal(title, pr.get("text"), val, pr.get("trust", "pr")):
            continue
        try:
            pub = datetime.fromisoformat(pr.get("publishedDate").replace(" ", "T")).replace(tzinfo=timezone.utc)
            age_h = round((now - pub).total_seconds() / 3600.0, 1)
        except Exception:
            age_h = None
        deals_raw.append({"symbol": sym, "title": title.strip(), "publisher": pr.get("publisher"),
                          "url": pr.get("url"), "published": pr.get("publishedDate"), "age_h": age_h,
                          "deal_value_usd": val, "deal_value_str": vstr,
                          "multi_year": ("multi-year" in title.lower() or "multiyear" in title.lower()),
                          "text_snippet": (pr.get("text") or "")[:300]})

    # cross-ref revenue + cap for unique tickers (bounded)
    tickers = list({d["symbol"] for d in deals_raw})[:450]
    info = {}
    with ThreadPoolExecutor(max_workers=20) as ex:
        fut = {ex.submit(revenue_and_cap, s, uni): s for s in tickers}
        for f in as_completed(fut):
            info[fut[f]] = f.result()

    deals = []
    sm_long = set()
    try:
        for _f in json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/smart-money-13f.json")["Body"].read()).get("funds", []) or []:
            for _h in _f.get("top_longs", []) or []:
                if _h.get("ticker"):
                    sm_long.add(_h["ticker"])
    except Exception:
        pass
    for d in deals_raw:
        txt = d.get("text_snippet", "")
        rev, mc, mu = info.get(d["symbol"], (None, None, uni.get(d["symbol"], {})))
        # announcer-match using the FMP-profile company name (works for tickers outside our
        # curated universe). Drops mis-tagged PRs whose deal belongs to a different company.
        _nm = (mu or {}).get("name") or (uni.get(d["symbol"], {}) or {}).get("name") or ""
        if _nm and not lead_company_matches(_nm, d["symbol"], d["title"]):
            continue
        bkt = uni.get(d["symbol"], {}).get("cap_bucket") or bucket_of(mc)
        small = bkt in SMALL_BUCKETS
        val = d["deal_value_usd"]
        materiality = None
        if val and rev and rev > 0:
            materiality = round(val / rev * 100, 1)
        elif val and (rev == 0 or rev is None):
            materiality = 9999.0  # pre-revenue / first major contract
        vs_mc = round(val / mc * 100, 2) if (val and mc) else None
        # sanity cap: a single deal can't be a huge multiple of market cap — that means the
        # parsed figure is an unrelated number or the ticker is mis-tagged. Strip the bad size
        # rather than green-highlighting garbage (e.g. "$27B / 668564% of market cap").
        if vs_mc is not None and vs_mc > 300:
            val = None
            vstr = None
            vs_mc = None
            materiality = None
            d["deal_value_usd"] = None
            d["deal_value_str"] = None
        hl = highlight_tier(materiality, vs_mc, val)
        ai_rel, ai_kws = ai_tag(d["symbol"], d["title"], txt, ai_universe)
        is_billion = bool(val and val >= 1e9)
        ai_mega = bool(ai_rel and ((vs_mc is not None and vs_mc >= 20) or is_billion))
        sector = (mu or {}).get("sector") or (uni.get(d["symbol"], {}) or {}).get("sector") or ""
        sec_etf = SECTOR_TO_SPDR.get(sector.lower()) if sector else None
        sec_score = sector_scores.get(sec_etf) if sec_etf else None
        sec_rot_in = bool(sec_etf and sec_etf in sector_rotating_in)
        sec_conv = sector_conv.get(sec_etf) if sec_etf else None
        sec_post = sector_posture.get(sec_etf) if sec_etf else None
        sector_tailwind = bool(sec_rot_in or (sec_score is not None and sec_score >= 65) or sec_post == "OVERWEIGHT")
        sec_boost = (30 if ai_rel else 18) if sector_tailwind else (8 if (sec_score is not None and sec_score >= 50) else 0)
        cb = CAP_BOOST.get(bkt, 5)
        rec = 0 if d["age_h"] is None else max(0, 30 - d["age_h"] / 8.0)
        mat_score = 0 if materiality is None else min(materiality, 300) / 3.0
        mc_score = 0 if vs_mc is None else min(vs_mc, 50) * 1.5
        size_score = 0 if not val else min(val / 1e7, 40)
        focus = 60 if hl == "green" else 25 if hl == "yellow" else 0   # spotlight big-vs-size deals
        ai_boost = 90 if ai_mega else 35 if ai_rel else 0              # AI thesis: float AI deals up
        bil_boost = 45 if is_billion else 0                           # billion-dollar deals
        smbk = d["symbol"] in sm_long
        sm_boost = 22 if smbk else 0                                  # proven thematic 13F money is long this name
        score = round(mat_score + mc_score + cb + rec + size_score + focus + ai_boost + bil_boost
                      + sec_boost + sm_boost + (8 if d["multi_year"] else 0), 1)
        why_bits = []
        if d["deal_value_str"]:
            why_bits.append(f"{d['deal_value_str']}{' multi-year' if d['multi_year'] else ''} deal")
        else:
            why_bits.append("deal announced (size not disclosed)")
        if materiality == 9999.0:
            why_bits.append("pre-revenue / first major contract — transformative")
        elif materiality is not None:
            why_bits.append(f"{materiality}% of annual revenue (${rev/1e6:.0f}M) — not yet in reported numbers")
        if vs_mc is not None:
            why_bits.append(f"{vs_mc}% of market cap")
        if ai_mega:
            why_bits.append("🤖 AI mega-deal — billions / large vs market cap")
        elif ai_rel:
            why_bits.append("🤖 AI-relevant")
        if sector_tailwind:
            why_bits.append(f"🌊 sector {sec_etf} rotating in" + (f" (score {round(sec_score)})" if sec_score is not None else ""))
        if small:
            why_bits.append(f"{bkt}-cap — single contract moves the needle")
        if smbk:
            why_bits.append("★ smart money long (13F)")
        deals.append({k: v for k, v in d.items() if k != "text_snippet"} | {
            "name": (mu or {}).get("name"), "cap_bucket": bkt, "market_cap": mc,
            "is_small_cap": small, "revenue_fy": rev, "materiality_pct": materiality,
            "vs_market_cap_pct": vs_mc, "highlight": hl, "ai_relevant": ai_rel,
            "ai_keywords": ai_kws, "is_billion": is_billion, "ai_megadeal": ai_mega,
            "sector": sector, "sector_etf": sec_etf, "sector_rotation_score": sec_score,
            "sector_conviction": sec_conv, "sector_posture": sec_post,
            "sector_rotating_in": sec_rot_in, "sector_tailwind": sector_tailwind,
            "smart_money_backed": smbk,
            "score": score, "why": "; ".join(why_bits)})

    deals.sort(key=lambda x: x["score"], reverse=True)
    green = [d for d in deals if d["highlight"] == "green"]
    yellow = [d for d in deals if d["highlight"] == "yellow"]
    ai_deals = [d for d in deals if d["ai_relevant"]]
    ai_mega = sorted([d for d in deals if d["ai_megadeal"]],
                     key=lambda x: (x["vs_market_cap_pct"] or 0, x["deal_value_usd"] or 0), reverse=True)
    small_deals = [d for d in deals if d["is_small_cap"]]
    sized = [d for d in deals if d["deal_value_usd"]]
    high_mat = [d for d in deals if d["materiality_pct"] and d["materiality_pct"] >= 25]

    # ── v2.0.0: full-market boards — every sector, every cap tier ──────────
    SECTORS_ALL = ["Technology", "Financial Services", "Healthcare", "Industrials",
                   "Energy", "Basic Materials", "Consumer Defensive", "Consumer Cyclical",
                   "Utilities", "Real Estate", "Communication Services"]
    CAPS_ALL = ["nano", "micro", "small", "mid", "large", "mega"]

    def _slim(d):
        return {k: d.get(k) for k in ("symbol", "name", "title", "deal_value_str",
                                      "vs_market_cap_pct", "materiality_pct", "highlight",
                                      "ai_relevant", "ai_megadeal", "score", "age_h", "url")}

    def _zero():
        return {"n": 0, "n_sized": 0, "total_value_usd": 0.0, "n_green": 0, "n_ai": 0, "top": []}
    by_sector = {sn: _zero() for sn in SECTORS_ALL}
    by_cap = {c: _zero() for c in CAPS_ALL}
    for d in deals:                                   # deals already score-sorted
        for board, key in ((by_sector, (d.get("sector") or "Unclassified")),
                           (by_cap, (d.get("cap_bucket") or "unknown"))):
            b = board.setdefault(key, _zero())
            b["n"] += 1
            if d.get("deal_value_usd"):
                b["n_sized"] += 1
                b["total_value_usd"] = round(b["total_value_usd"] + d["deal_value_usd"], 0)
            if d.get("highlight") == "green":
                b["n_green"] += 1
            if d.get("ai_relevant"):
                b["n_ai"] += 1
            if len(b["top"]) < 3:
                b["top"].append(_slim(d))
    src_counts = {}
    for p in prs:
        _o = p.get("origin") or "other"
        src_counts[_o] = src_counts.get(_o, 0) + 1
    coverage = {
        "sources": src_counts, "n_items": len(prs),
        "n_unique_tickers_in_tape": len({p.get("symbol") for p in prs if p.get("symbol")}),
        "n_tickers_crossref": len(tickers),
        "sectors_with_deals": sum(1 for v in by_sector.values() if v["n"] > 0),
        "n_sectors_tracked": len(SECTORS_ALL),
        "caps_with_deals": sum(1 for v in by_cap.values() if v["n"] > 0),
        "runs_per_day": 8,
        "note": ("full-market: every ticker on the PR/news tape is eligible — no universe "
                 "filter, all caps nano→mega, all 11 GICS sectors"),
    }

    # ── v2.0.0: graded signals — material fresh deals enter the fleet loop ─
    logged = []
    try:
        from signals_emit import log_signal, yprice
        _tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
        _cands = [d for d in deals
                  if d.get("age_h") is not None and d["age_h"] <= 30
                  and (d.get("highlight") == "green" or d.get("ai_megadeal")
                       or (d.get("is_billion") and (d.get("vs_market_cap_pct") or 0) >= 5))]
        for d in _cands[:10]:
            _px = yprice(d["symbol"])
            if not _px:
                continue
            _conf = (0.66 if (d.get("ai_megadeal") and d.get("highlight") == "green")
                     else 0.62 if d.get("highlight") == "green" else 0.58)
            _mat = d.get("materiality_pct")
            _mat_s = ("pre-revenue" if _mat == 9999.0
                      else f"{_mat}% of revenue" if _mat is not None else "rev n/a")
            if log_signal(
                    _tbl, "deal-win", d["symbol"], "UP", [5, 21, 63], _px,
                    confidence=_conf,
                    rationale=(f"deal-win: {d.get('deal_value_str') or 'sized deal'}"
                               f"{' AI-mega' if d.get('ai_megadeal') else ''} = "
                               f"{d.get('vs_market_cap_pct')}% of mcap / {_mat_s} "
                               f"→ UP vs SPY at announcement"),
                    benchmark="SPY",
                    metadata={"engine": "deal-scanner",
                              "deal_value_usd": d.get("deal_value_usd"),
                              "vs_mc_pct": d.get("vs_market_cap_pct"),
                              "materiality_pct": _mat,
                              "cap_bucket": d.get("cap_bucket"), "sector": d.get("sector"),
                              "highlight": d.get("highlight"),
                              "ai_megadeal": bool(d.get("ai_megadeal")),
                              "age_h": d.get("age_h")}):
                logged.append({"ticker": d["symbol"], "conf": _conf,
                               "value": d.get("deal_value_str"),
                               "vs_mc_pct": d.get("vs_market_cap_pct")})
    except Exception as _e:
        print(f"[deal-scanner] signal emit skipped: {str(_e)[:100]}")

    out = {
        "engine": "deal-scanner", "version": VERSION,
        "generated_at": now.isoformat(),
        "window": "rolling PR + news tape (~last 24-72h), rescanned every 3h",
        "summary": {
            "n_prs_scanned": len(prs), "n_deals": len(deals), "n_with_size": len(sized),
            "n_small_cap": len(small_deals), "n_high_materiality": len(high_mat),
            "n_green": len(green), "n_yellow": len(yellow),
            "n_ai": len(ai_deals), "n_ai_mega": len(ai_mega),
            "signals_logged": len(logged), "signals": logged,
            "sectors_with_deals": coverage["sectors_with_deals"],
            "caps_with_deals": coverage["caps_with_deals"],
            "ai_megadeals": ai_mega, "ai_deals": ai_deals[:25],
            "green_highlights": green, "yellow_highlights": yellow[:20],
            "top_deals": deals[:20],
            "top_smallcap_deals": small_deals[:15],
            "top_high_materiality": sorted(high_mat, key=lambda x: x["materiality_pct"], reverse=True)[:15],
        },
        "deals": deals[:200],
        "by_sector": by_sector, "by_cap": by_cap, "coverage": coverage,
        "methodology": {
            "source": "FMP press-releases-latest + stock-latest + Polygon news — full-market, no universe filter",
            "coverage": "8 scans/day (every 3h); every ticker on the PR/news tape is eligible — all sectors, all cap tiers (nano→mega)",
            "signals": "material fresh deals (green / AI-mega / $1B+ ≥5% mcap, ≤30h old) → family deal-win, UP [5,21,63] vs SPY at announcement price; graded by the fleet loop, PROVEN gate applies",
            "deal_filter": "strong deal-win language (awarded/wins/secures/contract/order/supply/"
                           "multi-year/LOI/MOU/design-win) minus financing/governance/earnings PRs",
            "deal_value": "largest $ figure parsed from title+text",
            "materiality": "deal_value / latest-FY revenue; pre-revenue flagged as transformative",
            "score": "materiality + cap_boost (nano+35..mega+0) + recency + size + multi-year; small-cap tilted",
            "caveat": "size parsed from PR text (may misparse); 'not yet in revenue' inferred from announcement "
                      "freshness — a fresh award lags reported revenue by quarters",
        },
        "sources": ["FMP news/press-releases-latest", "FMP news/stock-latest", "Polygon news", "FMP income-statement", "universe", "FMP profile", "ai-infra-stack (AI universe)", "sector-rotation (sector tailwind)", "smart-money-13f", "signals: justhodl-signals family deal-win"],
        "disclaimer": "Announcement-driven forward-revenue signal. Real data, research only — not advice.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[deal-scanner] prs={len(prs)} deals={len(deals)} sized={len(sized)} "
          f"small={len(small_deals)} highmat={len(high_mat)} logged={len(logged)} "
          f"sectors={coverage['sectors_with_deals']}/11 {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_prs": len(prs), "n_deals": len(deals),
            "n_small_cap": len(small_deals), "n_high_materiality": len(high_mat), "logged": len(logged)})}
