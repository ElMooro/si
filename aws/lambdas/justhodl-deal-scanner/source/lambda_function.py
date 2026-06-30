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
OUTPUT data/deal-scanner.json   SCHEDULE daily 22:00 UTC. Real data, research only.
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

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/deal-scanner.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
BENZINGA_KEY = "bzMJ62WO2YP2OKVIE2YSF4ZWVSVOJ6CTNP"
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


def fetch_news(pages=8, limit=100):
    """Scan BOTH official company PRs and third-party financial news (catches widely-reported
    billion-dollar deals that aren't self-announced)."""
    out = []

    def one(args):
        feed, p = args
        d = _fmp(f"news/{feed}?page={p}&limit={limit}")
        items = d if isinstance(d, list) else []
        tr = "pr" if feed == "press-releases-latest" else "news"
        for it in items:
            it["trust"] = tr
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


def fetch_polygon(limit=100, pages=3):
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
                        "url": a.get("article_url"), "trust": "news"})
        nu = j.get("next_url")
        if not nu:
            break
        url = nu + f"&apiKey={POLYGON_KEY}"
    return out


def fetch_benzinga(pagesize=100, pages=2):
    """Benzinga news — fast deal/PR coverage, ticker-tagged."""
    out = []
    for p in range(pages):
        j = _http_json(f"https://api.benzinga.com/api/v2/news?token={BENZINGA_KEY}"
                       f"&pageSize={pagesize}&page={p}&displayOutput=full")
        items = j if isinstance(j, list) else (j.get("data") if isinstance(j, dict) else None)
        if not isinstance(items, list) or not items:
            break
        for a in items:
            stocks = a.get("stocks") or []
            sym = stocks[0].get("name") if (stocks and isinstance(stocks[0], dict)) else None
            if not sym:
                continue
            try:
                pub = parsedate_to_datetime(a.get("created")).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pub = ""
            out.append({"symbol": sym, "title": a.get("title"),
                        "text": a.get("teaser") or a.get("body") or "",
                        "publishedDate": pub, "publisher": "Benzinga", "url": a.get("url"),
                        "trust": "news"})
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


def name_in_lead(symbol, title, uni, lead=32):
    """The ticker's company name must appear in the HEADLINE LEAD (announcer position). This
    rejects coincidental mid-headline token overlaps that anywhere-matching let through — e.g.
    'Realty Income Forms ... Joint Venture' mis-tagged JYNT ('Joint'), or a NASA 'CLPS' program
    award mis-tagged the CLPS ticker. Recipient-style headlines ('Agency Awards XYZ $14M') are
    kept separately via a title-parsed deal size."""
    name = (uni.get(symbol, {}) or {}).get("name") or ""
    toks = name_tokens(name)
    if not toks:
        return False                      # unknown ticker → rely on a title-parsed size instead
    head = (title or "")[:lead].lower()
    return any(t in head for t in toks)


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

    prs = fetch_news(pages=8, limit=100)
    prs += fetch_polygon(limit=100, pages=3)   # wider third-party AI-deal coverage
    prs += fetch_benzinga(pagesize=100, pages=2)
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
        # keep only if the company is named in the headline lead OR the deal size is in the title
        # (recipient-style awards). Kills mis-tagged tickers (JYNT/CLPS/GTLL).
        if not (_largest(title)[0] > 0 or name_in_lead(sym, title, uni)):
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
    tickers = list({d["symbol"] for d in deals_raw})[:300]
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

    out = {
        "engine": "deal-scanner", "version": VERSION,
        "generated_at": now.isoformat(),
        "window": "rolling latest press releases (~last 24-48h)",
        "summary": {
            "n_prs_scanned": len(prs), "n_deals": len(deals), "n_with_size": len(sized),
            "n_small_cap": len(small_deals), "n_high_materiality": len(high_mat),
            "n_green": len(green), "n_yellow": len(yellow),
            "n_ai": len(ai_deals), "n_ai_mega": len(ai_mega),
            "ai_megadeals": ai_mega, "ai_deals": ai_deals[:25],
            "green_highlights": green, "yellow_highlights": yellow[:20],
            "top_deals": deals[:20],
            "top_smallcap_deals": small_deals[:15],
            "top_high_materiality": sorted(high_mat, key=lambda x: x["materiality_pct"], reverse=True)[:15],
        },
        "deals": deals[:120],
        "methodology": {
            "source": "FMP press-releases-latest (official company PRs)",
            "deal_filter": "strong deal-win language (awarded/wins/secures/contract/order/supply/"
                           "multi-year/LOI/MOU/design-win) minus financing/governance/earnings PRs",
            "deal_value": "largest $ figure parsed from title+text",
            "materiality": "deal_value / latest-FY revenue; pre-revenue flagged as transformative",
            "score": "materiality + cap_boost (nano+35..mega+0) + recency + size + multi-year; small-cap tilted",
            "caveat": "size parsed from PR text (may misparse); 'not yet in revenue' inferred from announcement "
                      "freshness — a fresh award lags reported revenue by quarters",
        },
        "sources": ["FMP news/press-releases-latest", "FMP news/stock-latest", "Polygon news", "Benzinga news", "FMP income-statement", "universe", "FMP profile", "ai-infra-stack (AI universe)", "sector-rotation (sector tailwind)"],
        "disclaimer": "Announcement-driven forward-revenue signal. Real data, research only — not advice.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[deal-scanner] prs={len(prs)} deals={len(deals)} sized={len(sized)} "
          f"small={len(small_deals)} highmat={len(high_mat)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_prs": len(prs), "n_deals": len(deals),
            "n_small_cap": len(small_deals), "n_high_materiality": len(high_mat)})}
