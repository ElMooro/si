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
v3.0.0: institutional layer — ALL US-listed universe join (exhaustive universe-builder v4,
        ADRs included; OTC/unlisted tape names flagged + barred from signals), event
        taxonomy (M&A target/acquirer, gov contract, licensing, partnership, equity inv),
        counterparty extraction + quality (GOV / HYPERSCALER / MEGACAP), promo-risk guard
        (LOI/MOU/non-binding + nano-cap unnamed-counterparty pumps), SEC EDGAR 8-K Item 1.01
        confirmation, chase-guard (event-study house finding: chasing loses), census +
        13F dollar-flow overlays, and a persistent deal-history ledger with base-rate
        event study (population fwd returns vs SPY by event type).
+ graded deal-win signals [5,21,63] vs SPY via shared signals_emit. Real data, research only.
"""
import hashlib
import json
import re
import time
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime

import boto3

VERSION = "3.2.1"
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


def fetch_news(pages=14, limit=200):
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


def fetch_polygon_window(days, limit=1000, max_pages=250):
    """v3.1.1 HISTORICAL mode: Polygon news date-windowed paging — the only leg
    that reaches WEEKS back (FMP latest-feeds cap out at ~3-7 days; ops 3579
    proved 100 pages = 22k items but zero matured entries). limit=1000 requested;
    tier gracefully serves what it allows."""
    now = datetime.now(timezone.utc)
    gte = (now - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
    out, url = [], (f"https://api.polygon.io/v2/reference/news?limit={limit}"
                    f"&published_utc.gte={gte}&order=desc&sort=published_utc"
                    f"&apiKey={POLYGON_KEY}")
    for _ in range(max_pages):
        j = _http_json(url, timeout=25)
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
                        "url": a.get("article_url"), "trust": "news", "origin": "polygon_hist"})
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
    inc = _fmp(f"income-statement?symbol={urllib.parse.quote(symbol)}&limit=2")
    rev = None
    if isinstance(inc, list) and inc:
        rev = _num(inc[0].get("revenue"))
        rev_prev = _num(inc[1].get("revenue")) if len(inc) > 1 else None
        rev_g = (round((rev / rev_prev - 1) * 100, 1)
                 if (rev and rev_prev and rev_prev > 0) else None)
    mu = uni.get(symbol, {})
    mc = mu.get("market_cap")
    if not mc:
        prof = _fmp(f"profile?symbol={urllib.parse.quote(symbol)}")
        if isinstance(prof, list) and prof:
            mc = _num(prof[0].get("marketCap"))
            if not mu.get("name"):
                mu = {"name": prof[0].get("companyName"), "industry": prof[0].get("industry"),
                      "rev_growth_pct": rev_g,
                      "sector": prof[0].get("sector")}
            elif not mu.get("sector"):
                mu = dict(mu, sector=prof[0].get("sector"))
    mu = dict(mu or {}, rev_growth_pct=rev_g)
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


# ── v3 institutional layer ──────────────────────────────────────────────
EVENTS_ALL = ["contract_win", "govt_contract", "ma_target", "ma_acquirer",
              "partnership", "licensing_supply", "equity_investment",
              "capital_structure", "other"]

_EV_PATTERNS = [
    # capital-structure / financing events are NOT deal wins — classified first,
    # stripped of green/AI-mega highlighting, barred from signals (v3.1: EPR
    # credit agreement, LASE warrant exercise, MRAI debt restructuring class)
    ("capital_structure", re.compile(r"credit (?:agreement|facility)|revolving credit|term loan|warrants?\b|equity line|at[- ]the[- ]market|controlled equity offering|open market sale|public offering|private placement|registered direct|convertible (?:notes?|senior|debt)|notes offering|debt (?:restructuring|refinanc\w*|repayment|reduction|service)|cost savings|share repurchase|buyback program|reverse (?:stock )?split|shelf registration|equity offering|unit offering|pricing of .{0,30}offering", re.I)),
    ("ma_target", re.compile(r"to be acquired|going.private|take[- ]private|receives?\s+(?:an?\s+)?(?:unsolicited\s+)?(?:acquisition|takeover|buyout)\s+(?:proposal|offer|bid)|merger agreement (?:with|under which)|per share in cash", re.I)),
    ("ma_acquirer", re.compile(r"\b(?:to acquire|acquires|agreement to acquire|completes? (?:the )?acquisition of|to merge with|definitive (?:merger|acquisition) agreement)\b", re.I)),
    ("govt_contract", re.compile(r"department of defense|\bdod\b|u\.?s\.? (?:army|navy|air force|space force)|\bdarpa\b|\bpentagon\b|\bgsa\b|\bnasa\b|\bidiq\b|task order|federal contract|government contract|department of energy|homeland security|defense contract", re.I)),
    ("equity_investment", re.compile(r"strategic (?:equity )?investment (?:in|from)|equity stake|\bpipe\b financing|takes? .{0,20} stake", re.I)),
    ("licensing_supply", re.compile(r"licens(?:e|ing) agreement|royalt(?:y|ies)|long[- ]term supply agreement|supply agreement", re.I)),
    ("partnership", re.compile(r"strategic (?:partnership|collaboration|alliance)|joint venture|teams? up with|partners? with", re.I)),
    ("contract_win", re.compile(r"award(?:ed|s)?|\bwins?\b|secures?|purchase order|order for|contracts? (?:to|for|with|valued)|design win|selected (?:by|to)|deploy|\\bcontracts?\\b", re.I)),
]
_NONBIND_RE = re.compile(r"letter of intent|\bloi\b|memorandum of understanding|\bmou\b|non[- ]binding", re.I)
_PROMO_RE = re.compile(r"revolutionar|game[- ]chang|poised to|first[- ]of[- ]its[- ]kind|paradigm|disrupt(?:s|ive)", re.I)

_CP_GOV = ["department of defense", "dod", "u.s. army", "u.s. navy", "air force",
           "space force", "darpa", "pentagon", "gsa", "nasa", "department of energy",
           "homeland security", "federal government", "u.s. government"]
_CP_HYPER = ["microsoft", "azure", "google", "alphabet", "amazon", "aws", "meta ",
             "oracle", "nvidia", "openai", "anthropic", "xai", "coreweave", "tesla"]
_CP_MEGA = ["apple", "boeing", "lockheed", "raytheon", "rtx", "northrop", "general dynamics",
            "jpmorgan", "goldman", "berkshire", "exxon", "chevron", "walmart", "unitedhealth",
            "johnson & johnson", "eli lilly", "pfizer", "broadcom", "qualcomm", "intel",
            "samsung", "tsmc", "taiwan semiconductor", "at&t", "verizon", "comcast",
            "general motors", "ford", "caterpillar", "honeywell", "ge aerospace", "siemens",
            "airbus", "toyota", "volkswagen", "salesforce", "ibm", "dell", "cisco"]


def classify_event(title, text):
    blob = (title or "") + " " + (text or "")[:400]
    for ev, rx in _EV_PATTERNS:
        if rx.search(blob):
            return ev
    return "other"


def counterparty(title, text, self_name):
    """Who is the deal WITH — curated megacap/gov list scan → quality tier."""
    blob = ((title or "") + " " + (text or "")[:400]).lower()
    sn = (self_name or "").lower()[:18]
    hits, q = [], None
    for lst, tier in ((_CP_GOV, "GOV"), (_CP_HYPER, "HYPERSCALER"), (_CP_MEGA, "MEGACAP")):
        for nm in lst:
            nm = nm.strip()
            if re.search(r"(?<![a-z])" + re.escape(nm) + r"(?![a-z])", blob) \
                    and (not sn or nm not in sn):
                hits.append(nm)
                q = q or tier
    if not q:
        q = "NAMED" if re.search(r"(?:with|from|for|by)\s+[A-Z][A-Za-z&.\-]{2,}", title or "") else "UNNAMED"
    return hits[:3], q


def promo_guard(title, text, cap_bucket, cp_quality, val):
    """Institutional pump filter: non-binding paper + nano-cap unnamed-counterparty
    sizeless 'deals' + heavy promo language never reach the signal book."""
    blob = (title or "") + " " + (text or "")[:400]
    nonbind = bool(_NONBIND_RE.search(blob))
    promo_lang = len(_PROMO_RE.findall(blob)) >= 2
    pump = (cap_bucket in ("nano", "micro") and cp_quality == "UNNAMED" and not val)
    return (nonbind or promo_lang or pump), nonbind


def load_universe_meta():
    """Exhaustive universe-builder v4 feed → listed-set + per-symbol exchange."""
    try:
        u = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/universe.json")["Body"].read())
        ex = {s0["symbol"]: (s0.get("exchange") or "") for s0 in u.get("stocks", []) if s0.get("symbol")}
        return ex, (u.get("stats") or {}), u.get("generated_at")
    except Exception:
        return {}, {}, None


def load_census():
    """Nervous-system overlay (fundamental-census matrix) — same fields best-setups uses."""
    out = {}
    try:
        mx = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/fundamental-census-matrix.json")["Body"].read())
        C = mx.get("cols") or {}
        col = lambda k: C.get(k) or [None] * len(mx.get("tickers") or [])
        for i, t in enumerate(mx.get("tickers") or []):
            out[t] = {"conviction": col("conviction_score")[i], "combo": col("combo_score")[i],
                      "risk": col("risk_score")[i], "whale_usd_m": col("whale_net_usd_m")[i]}
    except Exception as _e:
        print("[deal-scanner] census overlay skipped:", str(_e)[:80])
    return out


def load_shareflows():
    """Dilution truth from share-flows: a 'transformative' deal at a serial
    diluter is a trap — join sh_yoy_pct + forensic flags per ticker."""
    try:
        sf = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/share-flows.json")["Body"].read())
        return {t: {"sh_yoy_pct": v.get("sh_yoy_pct"), "flags": v.get("flags") or []}
                for t, v in (sf.get("tickers") or {}).items() if not v.get("data_suspect")}
    except Exception:
        return {}


def load_13f_flows():
    """Per-ticker institutional dollar flows (13F complex): b/s/n $, whale wb/ws/wn, nf funds."""
    try:
        tf = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/13f-flows-by-ticker.json")["Body"].read())
        return tf.get("t") or {}
    except Exception:
        return {}


def load_options_flow():
    """v3.2: informed-flow confirmation — merge the LIVE options fleet's four
    per-ticker views (flow-scanner tiers, analytics most-unusual, polygon call
    flow, confluence posture). A deal + elevated call flow = the tape agreeing.
    Coverage is the options universes' (large/liquid names); absence is honest."""
    out = {}

    def _g(key):
        try:
            return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
        except Exception:
            return {}

    def _sym(x):
        return ((x.get("ticker") or x.get("symbol") or "") or "").upper()

    fs = _g("data/options-flow-scanner.json")
    for x in fs.get("all_qualifying") or []:
        t = _sym(x)
        if not t:
            continue
        m = x.get("metrics") or {}
        out.setdefault(t, {}).update({"tier": x.get("tier"),
                                      "call_vol_surge": m.get("call_vol_surge"),
                                      "cpr_change_pct": m.get("cpr_change_pct")})
        out[t].setdefault("sources", []).append("flow-scanner")
    oa = _g("data/options-analytics.json")
    for x in oa.get("most_unusual") or []:
        t = _sym(x)
        if not t:
            continue
        out.setdefault(t, {}).update({"n_unusual": x.get("n_unusual"),
                                      "net_premium_usd": x.get("net_premium_usd"),
                                      "pcr_vol": x.get("pcr_vol")})
        out[t].setdefault("sources", []).append("analytics-unusual")
    pf = _g("data/polygon-options-flow.json")
    for lst, tag in ((pf.get("extreme") or [], "extreme"),
                     (pf.get("bullish_call_flow") or [], "bullish")):
        for x in lst:
            t = _sym(x)
            if not t:
                continue
            out.setdefault(t, {}).setdefault("poly_flow", tag)
            out[t].setdefault("sources", []).append("polygon-flow-" + tag)
    oc = _g("data/options-confluence.json")
    for x in oc.get("multi_engine_confluence") or []:
        t = _sym(x)
        if not t:
            continue
        out.setdefault(t, {}).update({"posture": x.get("posture")})
        out[t].setdefault("sources", []).append("confluence")
    for t, v in out.items():
        v["bullish_confirm"] = bool(
            (v.get("tier") or "").startswith("TIER_A")
            or (v.get("tier") or "").startswith("TIER_B")
            or v.get("poly_flow") in ("extreme", "bullish")
            or v.get("posture") in ("BULLISH_FLOW", "SQUEEZE_FUEL")
            or ((v.get("n_unusual") or 0) >= 3 and (v.get("net_premium_usd") or 0) > 0))
    return out


def load_8k_set(now):
    """SEC EDGAR full-text search: 8-Ks citing Item 1.01 (material definitive agreement)
    in the last 3 days → set of tickers with a filed confirmation. A PR without an 8-K
    is unconfirmed paper; institutions check the filing. Fully non-fatal."""
    try:
        try:
            ct = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="sec/company-tickers.json")["Body"].read())
            if (now - datetime.fromisoformat(ct.get("_cached_at"))).days > 7:
                raise ValueError("stale")
            cmap = ct["map"]
        except Exception:
            raw = urllib.request.urlopen(urllib.request.Request(
                "https://www.sec.gov/files/company_tickers.json",
                headers={"User-Agent": "JustHodl research contact@justhodl.ai"}), timeout=25).read()
            j = json.loads(raw)
            cmap = {str(v["cik_str"]): v["ticker"] for v in j.values() if v.get("ticker")}
            s3.put_object(Bucket=S3_BUCKET, Key="sec/company-tickers.json",
                          Body=json.dumps({"_cached_at": now.isoformat(), "map": cmap}).encode(),
                          ContentType="application/json")
        d0 = (now - timedelta(days=3)).strftime("%Y-%m-%d")
        d1 = now.strftime("%Y-%m-%d")
        tks = {}
        for frm in (0, 100, 200):
            q = (f"https://efts.sec.gov/LATEST/search-index?q=%22Item%201.01%22&forms=8-K"
                 f"&startdt={d0}&enddt={d1}&from={frm}")
            raw = urllib.request.urlopen(urllib.request.Request(
                q, headers={"User-Agent": "JustHodl research contact@justhodl.ai",
                            "Accept": "application/json"}), timeout=20).read()
            hits = (json.loads(raw).get("hits") or {}).get("hits") or []
            if not hits:
                break
            for h in hits:
                src0 = h.get("_source") or {}
                adsh = src0.get("adsh") or (h.get("_id") or "").split(":")[0]
                fname = (h.get("_id") or "").split(":")[-1]
                for cik in src0.get("ciks") or []:
                    tk = cmap.get(str(int(cik)))
                    if tk and tk not in tks:
                        doc = (f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
                               f"{adsh.replace('-', '')}/{fname}") if adsh and fname else None
                        tks[tk] = {"adsh": adsh, "cik": int(cik), "doc": doc}
        return tks
    except Exception as _e:
        print("[deal-scanner] 8-K join skipped:", str(_e)[:80])
        return None


_TERM_RE = re.compile(r"(\d{1,2})[- ]year", re.I)
_THROUGH_RE = re.compile(r"through (?:fiscal |calendar )?(20\d\d)", re.I)
_TFC_RE = re.compile(r"terminat\w* (?:.{0,40})?for convenience", re.I)


def fetch_8k_terms(meta, pr_value, now):
    """v3.2: read the ACTUAL 8-K primary document and extract contract terms —
    filed value, term, non-binding language, termination-for-convenience — and
    verdict them against the PR's number. Institutions read the exhibit, not
    the press release. Sequential, polite, capped upstream, fully non-fatal."""
    try:
        if not (meta and meta.get("doc")):
            return None
        raw = urllib.request.urlopen(urllib.request.Request(
            meta["doc"], headers={"User-Agent": "JustHodl research contact@justhodl.ai"}),
            timeout=20).read(900_000)
        txt = re.sub(r"<[^>]+>", " ", raw.decode("utf-8", "replace"))
        txt = re.sub(r"&nbsp;|&#160;", " ", txt)
        txt = re.sub(r"\s+", " ", txt)
        # focus on Item 1.01 section when locatable
        m = re.search(r"Item\s*1\.01(.{0,12000}?)(?:Item\s*[2-9]\.|SIGNATURE)", txt, re.I | re.S)
        section = m.group(1) if m else txt[:12000]
        f_val, f_str = parse_value("", section)
        term_y = None
        tm = _TERM_RE.search(section)
        if tm:
            term_y = int(tm.group(1))
        else:
            th = _THROUGH_RE.search(section)
            if th:
                term_y = max(1, int(th.group(1)) - now.year)
        nonbind = bool(_NONBIND_RE.search(section))
        tfc = bool(_TFC_RE.search(section))
        if f_val and pr_value:
            r = pr_value / f_val
            match = "CONFIRMS" if 0.6 <= r <= 1.6 else ("PR_LARGER" if r > 1.6 else "FILING_LARGER")
        elif f_val:
            match = "FILED_VALUE"
        else:
            match = "NO_VALUE_IN_FILING"
        return {"value_usd": f_val, "value_str": f_str, "term_years": term_y,
                "non_binding": nonbind, "termination_for_convenience": tfc,
                "match": match, "url": meta["doc"]}
    except Exception as _e:
        print("[deal-scanner] 8-K terms skipped:", str(_e)[:80])
        return None


def _poly_closes(sym, d_from, d_to):
    j = _http_json(f"https://api.polygon.io/v2/aggs/ticker/{urllib.parse.quote(sym)}/range/1/day/"
                   f"{d_from}/{d_to}?adjusted=true&sort=asc&limit=150&apiKey={POLYGON_KEY}")
    if not isinstance(j, dict):
        return []
    return [(datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d"), r.get("c"))
            for r in (j.get("results") or []) if r.get("c")]


def pop_since_announce(sym, announce_date, now):
    """Chase-guard input: % move since the close before the announcement. House
    event-study (ops 3344-47): CHASING LOSES — hit21 16.7%, median −406bps."""
    try:
        d_from = (datetime.strptime(announce_date, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")
        cl = _poly_closes(sym, d_from, now.strftime("%Y-%m-%d"))
        if len(cl) < 2:
            return None
        base = None
        for dt_, c in cl:
            if dt_ < announce_date:
                base = c
        base = base or cl[0][1]
        return round((cl[-1][1] / base - 1) * 100, 1) if base else None
    except Exception:
        return None


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


def load_industry_boom():
    """v3.2.1: join the Industry Boom League — per-deal industry rank/score so
    every card can show growth-vs-industry-peers context."""
    try:
        j = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/industry-boom.json")["Body"].read())
        L = j.get("league") or []
        return {r["industry"]: {"score": r.get("boom_score"), "rank": i + 1,
                                "n": len(L), "delta_20d": r.get("score_delta_20d")}
                for i, r in enumerate(L)}
    except Exception as e:
        print("[deal-scanner] industry-boom join skip", str(e)[:70])
        return {}


def lambda_handler(event, context):
    t0 = time.time()
    # v3.1 BACKFILL MODE: event {"backfill_pages": N} sweeps the tape N pages
    # deep (days/weeks of PR history), classifies + ledgers everything with the
    # SAME pipeline, skips signals + the live feed write — matured entries get
    # forward returns filled immediately, so base rates are born populated.
    bf = 0
    bf_days = 0
    try:
        bf = int((event or {}).get("backfill_pages") or 0)
        bf_days = int((event or {}).get("backfill_days") or 0)
    except Exception:
        bf, bf_days = 0, 0
    try:
        universe = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/universe.json")["Body"].read())
        uni = {s["symbol"]: {"name": s.get("name"), "industry": s.get("industry"),
                             "sector": s.get("sector"), "market_cap": s.get("market_cap"),
                             "cap_bucket": s.get("cap_bucket")}
               for s in universe.get("stocks", []) if s.get("symbol")}
    except Exception:
        uni = {}

    if bf_days:
        # historical ledger backfill: Polygon windowed tape only (reaches weeks back)
        prs = fetch_polygon_window(bf_days)
        bf = bf or bf_days
    else:
        prs = fetch_news(pages=(bf or 14), limit=100)   # v2: full-market sweep; v3.1 backfill goes deep
        prs += fetch_polygon(limit=100, pages=(20 if bf else 8))
    # Benzinga leg REMOVED 2026-07-15: Massive stopped serving Benzinga (403 NOT_AUTHORIZED)
    ai_universe = load_ai_universe()
    sector_scores, sector_rotating_in, sector_conv, sector_posture = load_sector_signal()
    now = datetime.now(timezone.utc)
    # v3 institutional joins (each cached / non-fatal)
    exch_map, uni_stats, uni_gen = load_universe_meta()
    census = load_census()
    f13 = load_13f_flows()
    sfm = load_shareflows()
    oflow = load_options_flow()
    sec8k = load_8k_set(now)
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

    ind_boom = load_industry_boom()
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
        # ── v3: taxonomy, counterparty, listing, promo, 8-K, overlays ──
        ev = classify_event(d["title"], txt)
        cp_names, cp_q = counterparty(d["title"], txt, (mu or {}).get("name"))
        listed = (d["symbol"] in exch_map) if exch_map else None
        exch = exch_map.get(d["symbol"]) if exch_map else None
        promo, nonbind = promo_guard(d["title"], txt, bkt, cp_q, val)
        c8k = (d["symbol"] in sec8k) if sec8k is not None else None
        cen = census.get(d["symbol"])
        _f = f13.get(d["symbol"]) or {}
        inst_flow = ({"net_usd": _f.get("n"), "whale_net_usd": _f.get("wn"),
                      "n_funds": _f.get("nf")} if _f else None)
        _of = oflow.get(d["symbol"])
        opt_confirm = bool(_of and _of.get("bullish_confirm"))
        _sf = sfm.get(d["symbol"]) or {}
        diluting = bool((_sf.get("sh_yoy_pct") or 0) >= 3)
        dilution = ({"sh_yoy_pct": _sf.get("sh_yoy_pct"), "flags": _sf.get("flags")}
                    if _sf else None)
        if ev == "capital_structure":
            # financing / balance-sheet events are not revenue deals — never
            # green, never AI-mega, never signaled; shown in their own section
            hl = None
            ai_mega = False
        cp_boost = 25 if cp_q in ("GOV", "HYPERSCALER") else 8 if cp_q == "MEGACAP" else 0
        v3_adj = cp_boost + (12 if c8k else 0) - (60 if promo else 0) \
                 - (40 if ev == "ma_target" else 0) - (25 if listed is False else 0) \
                 - (70 if ev == "capital_structure" else 0) - (10 if diluting else 0) \
                 + (10 if opt_confirm else 0)
        score = round(mat_score + mc_score + cb + rec + size_score + focus + ai_boost + bil_boost
                      + sec_boost + sm_boost + (8 if d["multi_year"] else 0) + v3_adj, 1)
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
        if cp_q in ("GOV", "HYPERSCALER"):
            why_bits.append(f"🏛 {cp_q.lower()} counterparty" + (f" ({cp_names[0]})" if cp_names else ""))
        if c8k:
            why_bits.append("📄 8-K Item 1.01 on file (SEC-confirmed)")
        if ev == "ma_target":
            why_bits.append("⚖️ M&A target — price pins to deal terms (arb, not drift)")
        if promo:
            why_bits.append("⚠️ promo-risk" + (" (non-binding LOI/MOU)" if nonbind else ""))
        if listed is False:
            why_bits.append("⚠️ not on NYSE/NASDAQ/AMEX (OTC/unlisted)")
        if ev == "capital_structure":
            why_bits.append("🏦 capital-structure event (financing) — not a revenue deal")
        if diluting:
            why_bits.append(f"⚠️ diluting {_sf.get('sh_yoy_pct')}%/yr (share-flows)")
        if opt_confirm:
            why_bits.append("📈 call flow confirms (options fleet: "
                            + ",".join((_of.get("sources") or [])[:2]) + ")")
        deals.append({k: v for k, v in d.items() if k != "text_snippet"} | {
            "name": (mu or {}).get("name"), "cap_bucket": bkt, "market_cap": mc,
            "is_small_cap": small, "revenue_fy": rev, "materiality_pct": materiality,
            "vs_market_cap_pct": vs_mc, "highlight": hl, "ai_relevant": ai_rel,
            "ai_keywords": ai_kws, "is_billion": is_billion, "ai_megadeal": ai_mega,
            "industry": (mu or {}).get("industry") or (uni.get(d["symbol"], {}) or {}).get("industry"),
            "rev_growth_pct": (mu or {}).get("rev_growth_pct"),
            "industry_boom": ind_boom.get((mu or {}).get("industry")
                                          or (uni.get(d["symbol"], {}) or {}).get("industry")),
            "sector": sector, "sector_etf": sec_etf, "sector_rotation_score": sec_score,
            "sector_conviction": sec_conv, "sector_posture": sec_post,
            "sector_rotating_in": sec_rot_in, "sector_tailwind": sector_tailwind,
            "smart_money_backed": smbk,
            "event_type": ev, "counterparties": cp_names, "counterparty_quality": cp_q,
            "listed": listed, "exchange": exch, "promo_risk": promo, "non_binding": nonbind,
            "confirmed_8k": c8k, "census": cen, "inst_flow": inst_flow,
            "dilution": dilution, "options_flow": _of, "options_confirm": opt_confirm,
            "filing": None,
            "score": score, "why": "; ".join(why_bits)})

    # v3.2: read the actual filings for material, 8-K-confirmed deals (cap 8/run)
    _n_terms = 0
    for d in deals:
        if _n_terms >= 8:
            break
        if not d.get("confirmed_8k"):
            continue
        if not (d.get("highlight") or d.get("ai_megadeal") or d.get("is_billion")):
            continue
        fil = fetch_8k_terms((sec8k or {}).get(d["symbol"]) if isinstance(sec8k, dict) else None,
                             d.get("deal_value_usd"), now)
        if fil:
            d["filing"] = fil
            _n_terms += 1
            if fil["match"] == "CONFIRMS":
                d["score"] = round(d["score"] + 8, 1)
                d["why"] += f"; 📄 filing terms confirm ({fil.get('value_str') or 'filed'})"
            elif fil["match"] == "PR_LARGER":
                d["score"] = round(d["score"] - 15, 1)
                d["why"] += "; ⚠️ PR number LARGER than filed value — spin risk"
            if fil.get("term_years"):
                d["why"] += f"; {fil['term_years']}-year term (filed)"
            time.sleep(0.2)

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
    by_event = {e: _zero() for e in EVENTS_ALL}
    for d in deals:                                   # deals already score-sorted
        for board, key in ((by_sector, (d.get("sector") or "Unclassified")),
                           (by_cap, (d.get("cap_bucket") or "unknown")),
                           (by_event, (d.get("event_type") or "other"))):
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
        "events_with_deals": sum(1 for v in by_event.values() if v["n"] > 0),
        "universe": {"n_listed": (uni_stats or {}).get("total_stocks"),
                     "n_adr": (uni_stats or {}).get("n_adr"),
                     "by_exchange": (uni_stats or {}).get("by_exchange"),
                     "generated_at": uni_gen,
                     "note": "exhaustive universe-builder v4 — every NYSE/NASDAQ/AMEX common stock incl. ADRs; tape names outside it are OTC/unlisted (flagged, barred from signals)"},
        "n_8k_item101_3d": (len(sec8k) if sec8k is not None else None),
        "n_options_confirmed": sum(1 for d in deals if d.get("options_confirm")),
        "n_filings_parsed": sum(1 for d in deals if d.get("filing")),
        "runs_per_day": 8,
        "note": ("full-market: every ticker on the PR/news tape is eligible — no universe "
                 "filter, all caps nano→mega, all 11 GICS sectors"),
    }

    # ── v3: persistent deal-history ledger + base-rate event study ─────────
    # Institutions demand the POPULATION study, not just the signaled subset:
    # every detected deal is ledgered; matured entries get fwd 5d/21d excess vs
    # SPY filled from Polygon closes; base_rates aggregates by event type.
    base_rates, hist_n, hist_filled = {}, 0, 0
    try:
        try:
            _hist = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/deal-history.json")["Body"].read())
            entries = _hist.get("entries") or {}
        except Exception:
            entries = {}
        for d in deals:
            if not d.get("published"):
                continue
            _ad = d["published"][:10]
            _id = hashlib.sha1(f"{d['symbol']}|{_ad}|{(d['title'] or '')[:60]}".encode()).hexdigest()[:16]
            if _id not in entries:
                entries[_id] = {"sym": d["symbol"], "announce": _ad, "event_type": d.get("event_type"),
                                "cap": d.get("cap_bucket"), "sector": d.get("sector"),
                                "val": d.get("deal_value_usd"), "vs_mc": d.get("vs_market_cap_pct"),
                                "highlight": d.get("highlight"), "promo": d.get("promo_risk"),
                                "listed": d.get("listed"), "fwd5_ex": None, "fwd21_ex": None}
        _cut = (now - timedelta(days=120)).strftime("%Y-%m-%d")
        entries = {k: v for k, v in entries.items() if (v.get("announce") or "9999") >= _cut}
        _spy = dict(_poly_closes("SPY", (now - timedelta(days=135)).strftime("%Y-%m-%d"),
                                 now.strftime("%Y-%m-%d")))
        _spy_days = sorted(_spy)

        def _fwd(closes, spy_map, ad, n_td):
            days = [x for x in closes if x[0] >= ad]
            if len(days) <= n_td:
                return None
            b, e = days[0], days[n_td]
            sb, se = spy_map.get(b[0]), spy_map.get(e[0])
            if not (b[1] and e[1] and sb and se):
                return None
            return round(((e[1] / b[1]) - (se / sb)) * 100, 2)

        _fills = 0
        _fill_cap = 300 if bf else 40
        for _id, v in entries.items():
            if _fills >= _fill_cap or v.get("listed") is False:
                continue
            ad = v.get("announce")
            need5 = v.get("fwd5_ex") is None and ad and len([x for x in _spy_days if x >= ad]) > 6
            need21 = v.get("fwd21_ex") is None and ad and len([x for x in _spy_days if x >= ad]) > 22
            if not (need5 or need21):
                continue
            cl = _poly_closes(v["sym"], ad, now.strftime("%Y-%m-%d"))
            if not cl:
                continue
            if need5:
                v["fwd5_ex"] = _fwd(cl, _spy, ad, 5)
            if need21:
                v["fwd21_ex"] = _fwd(cl, _spy, ad, 21)
            _fills += 1
        hist_filled = _fills
        hist_n = len(entries)
        for ev0 in EVENTS_ALL:
            xs5 = sorted(v["fwd5_ex"] for v in entries.values()
                         if v.get("event_type") == ev0 and v.get("fwd5_ex") is not None)
            xs21 = sorted(v["fwd21_ex"] for v in entries.values()
                          if v.get("event_type") == ev0 and v.get("fwd21_ex") is not None)
            if xs5 or xs21:
                base_rates[ev0] = {
                    "n5": len(xs5), "med_fwd5_ex": (xs5[len(xs5) // 2] if xs5 else None),
                    "n21": len(xs21), "med_fwd21_ex": (xs21[len(xs21) // 2] if xs21 else None),
                    "hit21": (round(100 * sum(1 for x in xs21 if x > 0) / len(xs21), 1) if xs21 else None)}
        s3.put_object(Bucket=S3_BUCKET, Key="data/deal-history.json",
                      Body=json.dumps({"generated_at": now.isoformat(), "n": hist_n,
                                       "base_rates": base_rates, "entries": entries}).encode(),
                      ContentType="application/json")
    except Exception as _e:
        print("[deal-scanner] history ledger skipped:", str(_e)[:100])

    # ── v2.0.0: graded signals — material fresh deals enter the fleet loop ─
    logged = []
    if bf:
        print(f"[deal-scanner] BACKFILL pages={bf}: prs={len(prs)} deals={len(deals)} "
              f"ledger={hist_n}(+{hist_filled} filled) base_rate_types={list(base_rates)} "
              f"{round(time.time()-t0,1)}s — signals + feed write skipped")
        return {"statusCode": 200, "body": json.dumps(
            {"ok": True, "backfill": bf, "n_prs": len(prs), "n_deals": len(deals),
             "ledger_n": hist_n, "filled": hist_filled,
             "base_rate_types": list(base_rates)})}
    try:
        from signals_emit import log_signal, yprice
        _tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
        _cands = [d for d in deals
                  if d.get("age_h") is not None and d["age_h"] <= 30
                  and (d.get("highlight") == "green" or d.get("ai_megadeal")
                       or (d.get("is_billion") and (d.get("vs_market_cap_pct") or 0) >= 5))
                  # v3 institutional bar: listed only, no M&A-pinned targets, no promo paper
                  and d.get("listed") is not False
                  and d.get("event_type") not in ("ma_target", "capital_structure")
                  and not d.get("promo_risk")]
        for d in _cands[:10]:
            _px = yprice(d["symbol"])
            if not _px:
                continue
            _conf = (0.66 if (d.get("ai_megadeal") and d.get("highlight") == "green")
                     else 0.62 if d.get("highlight") == "green" else 0.58)
            # v3 conf adjustments + chase-guard (house event-study: chasing loses)
            _pop = pop_since_announce(d["symbol"], (d.get("published") or "")[:10], now)
            d["pop_since_announce_pct"] = _pop
            _chased = _pop is not None and _pop >= 25
            if _chased and not (d.get("ai_megadeal") and d.get("highlight") == "green"):
                print(f"[deal-scanner] chase-guard skip {d['symbol']} pop={_pop}%")
                continue
            if d.get("counterparty_quality") in ("GOV", "HYPERSCALER"):
                _conf += 0.04
            if d.get("confirmed_8k"):
                _conf += 0.03
            if d.get("options_confirm"):
                _conf += 0.03
            if (d.get("filing") or {}).get("match") == "CONFIRMS":
                _conf += 0.02
            if _chased:
                _conf -= 0.06
            _conf = round(max(0.50, min(0.74, _conf)), 2)
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
                              "event_type": d.get("event_type"),
                              "counterparty_quality": d.get("counterparty_quality"),
                              "confirmed_8k": d.get("confirmed_8k"),
                              "pop_since_announce_pct": d.get("pop_since_announce_pct"),
                              "options_confirm": bool(d.get("options_confirm")),
                              "filing_match": (d.get("filing") or {}).get("match"),
                              "exchange": d.get("exchange"),
                              "age_h": d.get("age_h")}):
                logged.append({"ticker": d["symbol"], "conf": _conf,
                               "value": d.get("deal_value_str"),
                               "vs_mc_pct": d.get("vs_market_cap_pct"),
                               "event_type": d.get("event_type"),
                               "confirmed_8k": d.get("confirmed_8k"),
                               "pop_pct": d.get("pop_since_announce_pct")})
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
        "by_sector": by_sector, "by_cap": by_cap, "by_event": by_event,
        "base_rates": base_rates, "history": {"n_entries": hist_n, "n_filled_this_run": hist_filled},
        "coverage": coverage,
        "methodology": {
            "source": "FMP press-releases-latest + stock-latest + Polygon news — full-market, no universe filter",
            "coverage": "8 scans/day (every 3h); every ticker on the PR/news tape is eligible — all sectors, all cap tiers (nano→mega)",
            "signals": "material fresh deals (green / AI-mega / $1B+ ≥5% mcap, ≤30h old) → family deal-win, UP [5,21,63] vs SPY at announcement price; graded by the fleet loop, PROVEN gate applies",
            "institutional_bar": "signals require: US-listed (NYSE/NASDAQ/AMEX incl. ADRs — OTC barred), not an M&A target (arb-pinned, no drift), not promo-risk (LOI/MOU/non-binding + nano-cap unnamed-counterparty pumps filtered); chase-guard skips names already +25% since announcement (house event-study: chasing loses); conf +0.04 GOV/HYPERSCALER counterparty, +0.03 SEC 8-K Item 1.01 confirmed",
            "informed_flow": "deal tickers cross-checked against the live options fleet (Tier-A/B flow, most-unusual premium, bullish call flow, confluence posture) — elevated calls at announcement = the tape agreeing; conf +0.03",
            "filed_terms": "material 8-K-confirmed deals get the ACTUAL filing parsed (Item 1.01 section): filed value vs PR value (CONFIRMS / PR_LARGER spin flag), term-years, non-binding + termination-for-convenience language; conf +0.02 on CONFIRMS",
            "event_taxonomy": "every deal classified: contract_win / govt_contract / ma_target / ma_acquirer / partnership / licensing_supply / equity_investment / other — base-rate fwd 5d/21d excess vs SPY tracked per type in data/deal-history.json (population study, not just signaled subset)",
            "deal_filter": "strong deal-win language (awarded/wins/secures/contract/order/supply/"
                           "multi-year/LOI/MOU/design-win) minus financing/governance/earnings PRs",
            "deal_value": "largest $ figure parsed from title+text",
            "materiality": "deal_value / latest-FY revenue; pre-revenue flagged as transformative",
            "score": "materiality + cap_boost (nano+35..mega+0) + recency + size + multi-year; small-cap tilted",
            "caveat": "size parsed from PR text (may misparse); 'not yet in revenue' inferred from announcement "
                      "freshness — a fresh award lags reported revenue by quarters",
        },
        "sources": ["FMP news/press-releases-latest", "FMP news/stock-latest", "Polygon news", "FMP income-statement", "universe-builder v4 (exhaustive US-listed incl. ADRs)", "FMP profile", "ai-infra-stack (AI universe)", "sector-rotation (sector tailwind)", "smart-money-13f", "SEC EDGAR 8-K Item 1.01 (efts full-text)", "fundamental-census matrix (overlay)", "13f-flows-by-ticker (inst $)", "Polygon aggs (base-rate study + chase-guard)", "options fleet: flow-scanner + analytics most-unusual + polygon-flow + confluence (informed-flow confirm)", "SEC 8-K primary documents (filed contract terms)", "signals: justhodl-signals family deal-win"],
        "disclaimer": "Announcement-driven forward-revenue signal. Real data, research only — not advice.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[deal-scanner] prs={len(prs)} deals={len(deals)} sized={len(sized)} "
          f"small={len(small_deals)} highmat={len(high_mat)} logged={len(logged)} "
          f"sectors={coverage['sectors_with_deals']}/11 universe={coverage['universe'].get('n_listed')} "
          f"8k={coverage.get('n_8k_item101_3d')} hist={hist_n}(+{hist_filled}) {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_prs": len(prs), "n_deals": len(deals),
            "n_small_cap": len(small_deals), "n_high_materiality": len(high_mat), "logged": len(logged)})}
