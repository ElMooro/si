"""
justhodl-readthrough v1.0.0 — CATALYST READ-THROUGH & DIFFUSION ENGINE ("Opportunities")
════════════════════════════════════════════════════════════════════════════════════════
THE GAP THIS CLOSES
  When SMCI printed >$60B of new orders after the close, the alpha was never SMCI —
  it was the names that MUST receive that spend and had not repriced yet by the open.
  The fleet had no engine for that:
    • bottleneck-boom      = quarterly fundamentals (backlog/shipments z + revenue accel).
                             Slow lens. Cannot see an 8pm order print.
    • supply-chain-graph   = 30-DAY lead-lag on a curated edge map. Right graph,
                             wrong clock, and no catalyst object.
    • catalyst-classifier  = names the catalyst ON the mover. Does not PROPAGATE it.
  This engine is the propagation layer: EVENT → GRAPH → DIFFUSION → WHAT HASN'T MOVED.

THE DISCIPLINE (why this is not a sympathy-chase list)
  Every beneficiary is tiered by MECHANISM, not by vibe:
    T1_DIRECT_SUPPLIER      sells into the order      → revenue read-through   (NVDA)
    T2_TIER2_INPUT          supplies the supplier     → BOM read-through       (MU, MKSI)
    T3_INFRASTRUCTURE       enables the buildout      → facility read-through  (VRT, ETN)
    T4_THEMATIC_SYMPATHY    same theme, unconfirmed   → multiple re-rate only  (NOK, ANET)
    T5_COMPETITOR_VALIDATION competitor, TAM proof    → multiple only, no rev  (DELL, HPE)
  A T4/T5 name can NEVER be labelled a supplier. The engine says so in the row.

THE MATH (event clock, beta-stripped, materiality-weighted)
  expected_move = catalyst_move × tier_weight × edge_confidence × materiality
  realized_ex_beta = move_since_catalyst − beta × SPY_move_since_catalyst
  residual = expected − realized_ex_beta      → the un-priced gap, in points
  capture  = realized_ex_beta / expected      → <0.33 UNPRICED · >1.5 OVERSHOT (chase guard)
  materiality = f(order_value_usd × tier_capture_share / beneficiary_revenue)
    — a $60B order is transformative for a $9B-revenue name and noise for a $4T one.
      Nobody prices that difference at the open. That is the edge.

GUARDS
  earnings ≤3d (the Nokia-reports-Thursday trap) · 5d run >15% (already chased) ·
  immaterial read-through (<1% of revenue) · illiquid (<$3M/day) · unconfirmed edge.

DATA — 100% real, zero synthetic. Degrades loudly, never fabricates.
  Polygon full-market snapshot (premarket/last) + grouped-daily (whole-market history)
  FMP /stable news + earnings-calendar + profile (shortlist only)
  S3 siblings: supply-chain-graph (named edges) · polygon-related-graph (market-inferred)
              8k-filings (SEC confirmation) · universe (mcap/sector/industry) · industry-boom

OUTPUT   data/readthrough.json      (top_picks → signal-harvester → truth ledger)
SCHEDULE 21:20 UTC (after-hours catch) · 11:20 UTC (premarket) · 13:20 UTC (pre-open) M-F
PAGE     readthrough.html
Research/education only — not investment advice.
"""
import json
import math
import os
import re
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import boto3

VERSION = "1.0.4"
REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/readthrough.json"
STATE_KEY = "data/readthrough-state.json"
S3 = boto3.client("s3", region_name=REGION)

POLY = os.environ.get("POLYGON_KEY") or os.environ.get("POLYGON_API_KEY", "")
FMP = os.environ.get("FMP_KEY") or os.environ.get("FMP_API_KEY", "")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ.get("TELEGRAM_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")

UA = {"User-Agent": "Mozilla/5.0 (JustHodl readthrough)"}

# ── Tuning (all overridable by the invoke event) ────────────────────────────────
GAP_MIN_PCT = 6.0          # a catalyst must move its own name at least this much
MIN_PRICE = 2.0            # no sub-$2 tape
MIN_DOLLAR_VOL = 3_000_000  # $3M/day floor for a tradeable beneficiary
EXTREME_MOVE_PCT = 45.0    # above this, require corroboration (8-K or a $ figure)
MAX_CATALYSTS = 12         # deepest-moving, most material events only
MAX_BENEF_PER_EVENT = 22
SHORTLIST_CAP = 140        # per-ticker FMP enrichment budget

# ── Mechanism tiers ─────────────────────────────────────────────────────────────
# tier_weight  = how much of the catalyst's own move should rationally transmit
# capture_share = fraction of the ORDER VALUE that plausibly lands as this tier's revenue
TIERS = {
    "T1_DIRECT_SUPPLIER":       {"w": 1.00, "capture": 0.55, "label": "Direct supplier — sells into the order"},
    "T2_TIER2_INPUT":           {"w": 0.65, "capture": 0.15, "label": "Tier-2 input — supplies the supplier"},
    "T3_INFRASTRUCTURE":        {"w": 0.50, "capture": 0.08, "label": "Infrastructure — enables the buildout"},
    "T4_THEMATIC_SYMPATHY":     {"w": 0.35, "capture": 0.02, "label": "Thematic sympathy — unconfirmed supplier"},
    "T5_COMPETITOR_VALIDATION": {"w": 0.20, "capture": 0.00, "label": "Competitor — TAM validated, no revenue"},
}
TIER_ORDER = list(TIERS.keys())

# Per-event tier caps (ops 3701). Without these an industry with 60 members
# floods the board with identical-score peers — 22 rows of T5 software names
# for a datacenter capex story, all scoring exactly the same. The cap is
# tightest where the mechanism is weakest.
TIER_CAPS = {"T1_DIRECT_SUPPLIER": 12, "T2_TIER2_INPUT": 8,
             "T3_INFRASTRUCTURE": 8, "T4_THEMATIC_SYMPATHY": 6,
             "T5_COMPETITOR_VALIDATION": 5}

EDGE_CONF = {"curated_mutual": 1.00, "curated_one_way": 0.85,
             "curated_none": 0.70, "polygon_related": 0.60, "industry_peer": 0.45}

# ── Catalyst taxonomy. propagate = how strongly this event type implies SPEND ───
CATALYST_TYPES = [
    ("BACKLOG_ORDERS", 1.00, ("backlog", "new orders", "order book", "bookings",
                              "record orders", "orders worth", "order intake", "orders totaling",
                              "orders topped", "orders exceeded", "orders reached",
                              "backlog of", "record backlog", "order backlog",
                              "books $", "booked $", "preliminary business update")),
    ("MEGA_CONTRACT", 0.95, ("awarded", "wins contract", "contract worth", "purchase order",
                             "agreement to supply", "supply agreement", "multi-year deal",
                             "signs deal", "selects", "partnership with")),
    ("CAPACITY_EXPANSION", 0.85, ("expand capacity", "new fab", "gigawatt", "capex",
                                  "capital expenditure", "build out", "data center build",
                                  "breaks ground", "new plant", "megawatt")),
    ("GUIDANCE_RAISE", 0.70, ("raises guidance", "raised guidance", "boosts outlook",
                              "lifts outlook", "raises forecast", "raises outlook",
                              "above consensus guidance", "sees revenue above")),
    ("EARNINGS_BEAT", 0.50, ("beats", "tops estimates", "earnings beat", "revenue beat",
                             "results beat", "smashes")),
    ("PRODUCT_LAUNCH", 0.40, ("launches", "unveils", "introduces", "announces new")),
    ("MA", 0.30, ("to acquire", "acquisition of", "merger", "takeover", "agrees to buy")),
    ("REGULATORY", 0.25, ("fda", "approval", "clearance", "authorization")),
]
# Capital-structure / mechanical events. These move a tape hard and mean NOTHING
# for a supply chain — a second-step conversion, a raise, a split. Hard-excluded
# before classification so they can never become a propagating catalyst.
STRUCTURAL_EXCLUDE = (
    "second step conversion", "second-step conversion", "stock offering",
    "public offering", "secondary offering", "rights offering", "at-the-market",
    "shelf registration", "reverse split", "reverse stock split", "forward split",
    "stock split", "share consolidation", "conversion and", "direct offering",
    "registered direct", "warrant exercise", "convertible notes offering",
    "special dividend", "spin-off completion", "dividend declaration",
)

MONEY_RE = re.compile(
    r"\$\s?([\d][\d,]*\.?\d*)\s*(trillion|billion|bn\b|b\b|million|mm\b|m\b)", re.I)
MONEY_MULT = {"trillion": 1e12, "billion": 1e9, "bn": 1e9, "b": 1e9,
              "million": 1e6, "mm": 1e6, "m": 1e6}


# ═══════════════════════════ HTTP / S3 plumbing ═════════════════════════════════
def http(url, timeout=25, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(0.6 * (i + 1))
    raise RuntimeError(f"http failed {url[:80]}: {last}")


def jget(url, timeout=25, retries=2):
    return json.loads(http(url, timeout, retries))


def s3_json(key):
    """Read a sibling engine's published output. None (never fake) on failure."""
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:  # noqa: BLE001
        print(f"[s3] MISS {key}: {str(e)[:90]}")
        return None


def num(v):
    try:
        f = float(v)
        return f if f == f and abs(f) != float("inf") else None
    except Exception:  # noqa: BLE001
        return None


def pick(d, *keys):
    for k in keys:
        if isinstance(d, dict) and d.get(k) not in (None, "", 0):
            v = num(d.get(k))
            if v is not None:
                return v
    return None


# ═══════════════════════════ Market tape (whole market, few calls) ══════════════
def snapshot():
    """One call → every US ticker's last price incl. extended hours + prev close."""
    if not POLY:
        return {}, "no POLYGON_KEY"
    try:
        d = jget("https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
                 f"?apiKey={POLY}", timeout=50)
    except Exception as e:  # noqa: BLE001
        return {}, f"snapshot failed: {str(e)[:80]}"
    out = {}
    for t in d.get("tickers", []) or []:
        sym = t.get("ticker")
        if not sym:
            continue
        day, prev, mn = t.get("day") or {}, t.get("prevDay") or {}, t.get("min") or {}
        lt = t.get("lastTrade") or {}
        pc = num(prev.get("c"))
        # EXTENDED-HOURS AWARE (ops 3705). min.c is the most recent minute bar in
        # ANY session — premarket, regular, after-hours. day.c freezes at the 4pm
        # close, so preferring it blinds the engine to after-hours order prints,
        # which is the single case this engine exists to catch.
        last = num(lt.get("p")) or num(mn.get("c")) or num(day.get("c")) or pc
        if not pc or not last or pc <= 0:
            continue
        reg = num(day.get("c"))
        poly_chg = num(t.get("todaysChangePerc"))   # Polygon's own extended-aware calc
        chg = poly_chg if poly_chg is not None else (last / pc - 1.0) * 100.0
        reg_chg = ((reg / pc - 1.0) * 100.0) if reg else None
        out[sym] = {
            "last": last, "prev_close": pc, "chg_pct": chg,
            "regular_close": reg, "chg_regular_pct": reg_chg,
            "chg_extended_pct": (chg - reg_chg) if reg_chg is not None else None,
            "dollar_vol": (num(prev.get("v")) or 0) * pc,
            "session_vol": num(day.get("v")) or 0,
        }
    return out, None


def grouped_day(ds):
    try:
        d = jget("https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
                 f"{ds}?adjusted=true&apiKey={POLY}", timeout=40)
        return ds, {r["T"]: r["c"] for r in (d.get("results") or []) if r.get("c")}
    except Exception:  # noqa: BLE001
        return ds, {}


def market_history(days_back=9):
    """Whole-market daily closes for the last ~9 calendar days in <=9 calls."""
    cal = [(date.today() - timedelta(days=i)).isoformat() for i in range(1, days_back + 1)]
    hist = {}
    with ThreadPoolExecutor(max_workers=9) as ex:
        for ds, mp in ex.map(grouped_day, cal):
            if mp:
                hist[ds] = mp
    return dict(sorted(hist.items()))  # oldest → newest


def close_on_or_before(hist, sym, iso_day):
    """Last real close at or before iso_day — the pre-catalyst anchor."""
    best = None
    for ds in hist:
        if ds <= iso_day and sym in hist[ds]:
            best = hist[ds][sym]
    return best


# ═══════════════════════════ Catalyst detection ═════════════════════════════════
def fmp_news(sym):
    if not FMP:
        return []
    q = urllib.parse.quote(sym)
    for path in (f"news/stock?symbols={q}&limit=12",
                 f"news/press-releases?symbols={q}&limit=8"):
        try:
            d = jget(f"https://financialmodelingprep.com/stable/{path}&apikey={FMP}", timeout=18)
            if isinstance(d, list) and d:
                return d
        except Exception:  # noqa: BLE001
            continue
    return []


def classify(text):
    t = (text or "").lower()
    for name, prop, keys in CATALYST_TYPES:
        if any(k in t for k in keys):
            return name, prop
    return "UNCLASSIFIED", 0.30


def extract_order_value(text):
    """Largest $ figure in the headline/body → the size of the spend."""
    best = None
    for amt, unit in MONEY_RE.findall(text or ""):
        try:
            v = float(amt.replace(",", "")) * MONEY_MULT[unit.lower().strip()]
        except Exception:  # noqa: BLE001
            continue
        if best is None or v > best:
            best = v
    return best


def anchor_day(published, fallback):
    """Pre-catalyst anchor session. FMP stamps ET: >=16:00 means the release
    landed after the close, so that day's close is still 'before' the news."""
    try:
        d = (published or "")[:10]
        hh = int((published or "")[11:13])
        if not d:
            return fallback
        if hh >= 16:
            return d
        return (date.fromisoformat(d) - timedelta(days=1)).isoformat()
    except Exception:  # noqa: BLE001
        return (published or "")[:10] or fallback


def hours_since(iso):
    try:
        s = (iso or "").replace("Z", "+00:00").replace(" ", "T")
        dt = datetime.fromisoformat(s[:25])
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    except Exception:  # noqa: BLE001
        return None


def find_catalysts(tape, univ_meta, filings_by_ticker):
    """Gappers with a NAMEABLE, SPEND-IMPLYING catalyst in the last 48h."""
    cands = []
    for sym, q in tape.items():
        if len(sym) > 5 or not sym.isalpha():
            continue
        if q["prev_close"] < MIN_PRICE or q["dollar_vol"] < MIN_DOLLAR_VOL:
            continue
        # POSITIVE ONLY: a surge is a claim on the chain. A collapse is
        # contagion — a different engine's job, not a beneficiary board.
        if q["chg_pct"] < GAP_MIN_PCT:
            continue
        cands.append((q["chg_pct"] * math.log1p(q["dollar_vol"]), sym, q))
    cands.sort(reverse=True)
    by_liq = sorted(cands, key=lambda x: -x[2]["dollar_vol"])[:25]
    seen, merged = set(), []
    for it in cands[:40] + by_liq:
        if it[1] not in seen:
            seen.add(it[1])
            merged.append(it)
    cands = merged

    def enrich(item):
        _, sym, q = item
        news = fmp_news(sym)
        best = None
        for n in news:
            title = n.get("title") or n.get("text") or ""
            body = (n.get("text") or "")[:600]
            pub = n.get("publishedDate") or n.get("date") or ""
            age = hours_since(pub)
            if age is None or age > 48:
                continue
            blob = f"{title} {body}".lower()
            if any(x in blob for x in STRUCTURAL_EXCLUDE):
                continue  # capital-structure event — never propagates
            ctype, prop = classify(f"{title} {body}")
            if ctype == "UNCLASSIFIED":
                continue
            val = extract_order_value(f"{title} {body}")
            rank = (prop, val or 0)
            if best is None or rank > best["rank"]:
                best = {"rank": rank, "type": ctype, "propagation": prop,
                        "order_value_usd": val, "headline": title[:240],
                        "published": pub, "age_h": round(age, 1),
                        "source": n.get("site") or n.get("publisher") or "fmp"}
        if best is None:
            return None
        # Extreme-move corroboration gate: a >45% move needs either an SEC 8-K on
        # the tape or an explicit dollar figure in the release. Otherwise it is
        # unverified and does not get to move 22 other tickers.
        f8k = filings_by_ticker.get(sym)
        if q["chg_pct"] >= EXTREME_MOVE_PCT and not (f8k or best.get("order_value_usd")):
            print(f"[skip] {sym} {q['chg_pct']:.1f}% — extreme move, no 8-K and no $ figure")
            return None
        best.update({
            "ticker": sym, "move_pct": round(q["chg_pct"], 2),
            "move_regular_pct": (round(q["chg_regular_pct"], 2)
                                 if q.get("chg_regular_pct") is not None else None),
            "move_extended_pct": (round(q["chg_extended_pct"], 2)
                                  if q.get("chg_extended_pct") is not None else None),
            "price": q["last"], "prev_close": q["prev_close"],
            "dollar_vol": int(q["dollar_vol"]),
            "market_cap": (univ_meta.get(sym) or {}).get("mcap"),
            "sector": (univ_meta.get(sym) or {}).get("sector"),
            "industry": (univ_meta.get(sym) or {}).get("industry"),
            "sec_8k_confirm": bool(f8k), "sec_8k": f8k,
        })
        best.pop("rank", None)
        return best

    out = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        for r in ex.map(enrich, cands):
            if r:
                out.append(r)
    out.sort(key=lambda x: (x["propagation"], x.get("order_value_usd") or 0,
                            x["move_pct"]), reverse=True)
    return out[:MAX_CATALYSTS]


# ═══════════════════════════ Beneficiary graph ══════════════════════════════════
def build_graph(scg, related, univ_meta):
    """suppliers_of / customers_of / peers / theme — from REAL published siblings."""
    suppliers_of, customers_of, conf = {}, {}, {}
    theme = {}
    for e in ((scg or {}).get("edges") or []):
        s, c = e.get("supplier"), e.get("customer")
        if not s or not c:
            continue
        src = e.get("source") or "curated"
        cf = ("polygon_related" if src == "polygon"
              else f"curated_{e.get('confirm') or 'none'}")
        suppliers_of.setdefault(c, []).append((s, e.get("relationship") or "", cf))
        customers_of.setdefault(s, []).append((c, e.get("relationship") or "", cf))
        conf[(s, c)] = cf
    for n in ((scg or {}).get("nodes") or []):
        if n.get("ticker"):
            theme[n["ticker"]] = n.get("theme")

    peers = {}
    rel = (related or {}).get("related") or (related or {}).get("graph") or {}
    if isinstance(rel, dict):
        for k, v in rel.items():
            if isinstance(v, list):
                peers[k] = [x for x in v if isinstance(x, str)][:12]

    by_industry = {}
    for sym, m in univ_meta.items():
        ind = (m or {}).get("industry")
        if ind:
            by_industry.setdefault(ind, []).append(sym)
    return suppliers_of, customers_of, conf, theme, peers, by_industry


def beneficiaries_for(cat, g, tape, univ_meta):
    """Tiered candidate set for ONE catalyst. Dedupe keeps the STRONGEST tier."""
    suppliers_of, customers_of, _conf, theme, peers, by_industry = g
    sym = cat["ticker"]
    found = {}

    def add(t, tier, why, econf, named):
        if t == sym or t not in tape:
            return
        q = tape[t]
        if q["prev_close"] < MIN_PRICE or q["dollar_vol"] < MIN_DOLLAR_VOL:
            return
        cur = found.get(t)
        if cur and TIER_ORDER.index(cur["tier"]) <= TIER_ORDER.index(tier):
            return
        found[t] = {"ticker": t, "tier": tier, "why": why,
                    "edge_confidence": EDGE_CONF.get(econf, 0.5),
                    "edge_source": econf, "named_edge": named}

    # T1 — its own NAMED suppliers (they sell into the order). A Polygon-inferred
    # edge is co-movement, not a vendor relationship, so it can never enter a
    # revenue tier — it is demoted to sympathy and labelled as such.
    for s, rel, cf in suppliers_of.get(sym, []):
        if cf == "polygon_related":
            add(s, "T4_THEMATIC_SYMPATHY",
                "market-inferred co-movement link — NOT a named supplier", cf, False)
        else:
            add(s, "T1_DIRECT_SUPPLIER", f"named supplier to {sym} ({rel or 'supply'})", cf, True)
    # T2 — suppliers of those suppliers (BOM tier). Curated edges only, both hops:
    # an inferred link at either hop makes the chain a guess, not a bill of materials.
    for s, _rel, cf in suppliers_of.get(sym, []):
        if cf == "polygon_related":
            continue
        for s2, rel2, cf2 in suppliers_of.get(s, []):
            if cf2 == "polygon_related":
                continue
            add(s2, "T2_TIER2_INPUT", f"supplies {s}, a named supplier to {sym} ({rel2 or 'input'})",
                cf2, True)
    # T3 — same-theme infrastructure hubs on the curated graph
    th = theme.get(sym)
    if th:
        for t, mtheme in theme.items():
            if mtheme == th and t != sym and t not in found:
                add(t, "T3_INFRASTRUCTURE", f"same buildout theme: {th}", "curated_none", True)
    # T4 — market-inferred related names (co-movement / co-mention, NOT suppliers)
    for t in peers.get(sym, []):
        add(t, "T4_THEMATIC_SYMPATHY",
            "market-inferred peer (Polygon related-companies) — no confirmed supply link",
            "polygon_related", False)
    # T5 — same-industry competitors: TAM validated, no revenue transfer
    ind = (univ_meta.get(sym) or {}).get("industry")
    if ind:
        for t in by_industry.get(ind, [])[:60]:
            add(t, "T5_COMPETITOR_VALIDATION",
                f"same industry ({ind}) — demand validated, not supplied", "industry_peer", False)

    rows = list(found.values())
    cat_mc = cat.get("market_cap") or 0

    def _rank(r):
        t = r["ticker"]
        if r["tier"] in ("T4_THEMATIC_SYMPATHY", "T5_COMPETITOR_VALIDATION"):
            # No revenue transfer in these tiers, so liquidity is the wrong sort —
            # it just surfaces the biggest names in the industry. A comparable is
            # useful when it is comparable IN SIZE: a $4T mega-cap does not re-rate
            # on a mid-cap's capex print. Closest-in-size first.
            mc = (univ_meta.get(t) or {}).get("mcap")
            if mc and cat_mc:
                return abs(math.log(mc / cat_mc))
            return 99.0
        return -tape[t]["dollar_vol"]   # revenue tiers: most tradeable first

    out_rows = []
    for tier in TIER_ORDER:
        tier_rows = sorted([r for r in rows if r["tier"] == tier], key=_rank)
        out_rows.extend(tier_rows[:TIER_CAPS.get(tier, 99)])
    return out_rows[:MAX_BENEF_PER_EVENT]


# ═══════════════════════════ Shortlist enrichment ═══════════════════════════════
def fmp_profile(sym):
    if not FMP:
        return sym, {}
    try:
        d = jget("https://financialmodelingprep.com/stable/profile"
                 f"?symbol={urllib.parse.quote(sym)}&apikey={FMP}", timeout=15)
        r = d[0] if isinstance(d, list) and d else {}
        return sym, {"mcap": pick(r, "marketCap", "mktCap"), "beta": pick(r, "beta"),
                     "sector": r.get("sector"), "industry": r.get("industry"),
                     "company": r.get("companyName")}
    except Exception:  # noqa: BLE001
        return sym, {}


def fmp_revenue(sym):
    if not FMP:
        return sym, None
    for path in (f"key-metrics-ttm?symbol={urllib.parse.quote(sym)}",
                 f"income-statement?symbol={urllib.parse.quote(sym)}&limit=1"):
        try:
            d = jget(f"https://financialmodelingprep.com/stable/{path}&apikey={FMP}", timeout=15)
            r = d[0] if isinstance(d, list) and d else {}
            v = pick(r, "revenue", "revenueTTM", "revenuePerShareTTM")
            if v and v > 1e6:
                return sym, v
        except Exception:  # noqa: BLE001
            continue
    return sym, None


def fmp_days_to_earnings(sym):
    if not FMP:
        return sym, None
    try:
        d = jget("https://financialmodelingprep.com/stable/earnings-calendar"
                 f"?symbol={urllib.parse.quote(sym)}&apikey={FMP}", timeout=15)
        today = date.today()
        best = None
        for r in (d or []):
            ds = (r.get("date") or "")[:10]
            try:
                dd = (date.fromisoformat(ds) - today).days
            except Exception:  # noqa: BLE001
                continue
            if 0 <= dd <= 60 and (best is None or dd < best):
                best = dd
        return sym, best
    except Exception:  # noqa: BLE001
        return sym, None


# ═══════════════════════════ Diffusion scoring ══════════════════════════════════
def materiality(order_value, tier, revenue):
    """How much could this order actually MOVE this company's P&L?"""
    if not order_value or not revenue or revenue <= 0:
        return 0.50, "unknown — order size or revenue unavailable"
    share = (order_value * TIERS[tier]["capture"]) / revenue
    m = min(1.0, 0.20 + 0.80 * min(1.0, share / 0.50))
    return round(m, 3), f"~{share * 100:.1f}% of TTM revenue at this tier's capture share"


def score_row(row, cat, tape, hist, meta, spy_move, cat_day):
    t = row["ticker"]
    q = tape[t]
    tier = row["tier"]
    tw = TIERS[tier]["w"]
    anchor = close_on_or_before(hist, t, cat_day)
    move = ((q["last"] / anchor - 1.0) * 100.0) if anchor else q["chg_pct"]
    beta = (meta.get(t) or {}).get("beta") or 1.0
    realized = move - beta * spy_move

    mat, mat_note = materiality(cat.get("order_value_usd"), tier, (meta.get(t) or {}).get("revenue"))
    expected = cat["move_pct"] * tw * row["edge_confidence"] * mat * cat["propagation"]
    residual = expected - realized
    capture = (realized / expected) if expected and abs(expected) > 0.01 else None

    if capture is None:
        status = "NO_SIGNAL"
    elif capture < 0.33 and residual >= 1.5:
        status = "UNPRICED"
    elif capture < 0.90:
        status = "PARTIAL"
    elif capture <= 1.50:
        status = "PRICED"
    else:
        status = "OVERSHOT"

    # 5-day run — have they already chased it?
    closes = [hist[d][t] for d in hist if t in hist[d]]
    run5 = ((q["last"] / closes[0] - 1.0) * 100.0) if len(closes) >= 4 else None

    flags = []
    if not row["named_edge"]:
        flags.append("UNCONFIRMED_EDGE — sympathy/validation only, not a proven supplier")
    d2e = (meta.get(t) or {}).get("days_to_earnings")
    if d2e is not None and d2e <= 3:
        flags.append(f"EVENT_RISK — reports in {d2e}d; the print, not the read-through, sets the next move")
    if run5 is not None and run5 > 15:
        flags.append(f"ALREADY_RAN — +{run5:.1f}% over 5 sessions")
    if mat < 0.30:
        flags.append("IMMATERIAL_READ_THROUGH — order is small vs this company's revenue")
    if q["dollar_vol"] < MIN_DOLLAR_VOL * 2:
        flags.append("THIN_LIQUIDITY")
    if status == "OVERSHOT":
        flags.append("CHASE_GUARD — already moved more than the mechanism justifies")

    base = 100.0 * (0.40 * tw
                    + 0.25 * max(0.0, min(1.0, 1.0 - (capture if capture is not None else 1.0)))
                    + 0.20 * mat
                    + 0.15 * row["edge_confidence"])
    for f in flags:
        head = f.split(" ")[0]
        base *= {"EVENT_RISK": 0.85, "ALREADY_RAN": 0.85, "IMMATERIAL_READ_THROUGH": 0.80,
                 "THIN_LIQUIDITY": 0.70, "CHASE_GUARD": 0.45, "UNCONFIRMED_EDGE": 0.90}.get(head, 1.0)
    if status in ("PRICED", "OVERSHOT", "NO_SIGNAL"):
        base *= 0.5

    row.update({
        "company": (meta.get(t) or {}).get("company"),
        "tier_label": TIERS[tier]["label"],
        "price": round(q["last"], 2),
        "move_since_catalyst_pct": round(move, 2),
        "beta": round(beta, 2),
        "realized_ex_beta_pct": round(realized, 2),
        "expected_move_pct": round(expected, 2),
        "residual_pct": round(residual, 2),
        "capture_ratio": (round(capture, 2) if capture is not None else None),
        "status": status,
        "materiality": mat, "materiality_note": mat_note,
        "run_5d_pct": (round(run5, 1) if run5 is not None else None),
        "days_to_earnings": d2e,
        "dollar_vol": int(q["dollar_vol"]),
        "market_cap": (meta.get(t) or {}).get("mcap"),
        "revenue_ttm": (meta.get(t) or {}).get("revenue"),
        "flags": flags,
        "catch_up_score": round(max(0.0, min(100.0, base)), 1),
        "catalyst_ticker": cat["ticker"],
        "catalyst_type": cat["type"],
        "anchor_close": anchor,
    })
    return row


def plain_english(r, cat):
    ov = cat.get("order_value_usd")
    size = f"${ov / 1e9:.0f}B " if ov and ov >= 1e9 else ""
    lead = {
        "T1_DIRECT_SUPPLIER": f"{r['ticker']} sells into that {size}order",
        "T2_TIER2_INPUT": f"{r['ticker']} supplies the parts that go into it",
        "T3_INFRASTRUCTURE": f"{r['ticker']} sells the power/cooling/interconnect the buildout needs",
        "T4_THEMATIC_SYMPATHY": f"{r['ticker']} is a same-theme name with no confirmed supply link",
        "T5_COMPETITOR_VALIDATION": f"{r['ticker']} competes with {cat['ticker']} — the print validates demand, it does not send revenue",
    }[r["tier"]]
    if r["status"] == "UNPRICED":
        tail = (f"mechanism says ~{r['expected_move_pct']:+.1f}%, tape has delivered "
                f"{r['realized_ex_beta_pct']:+.1f}% ex-beta — {r['residual_pct']:+.1f}pts unclaimed")
    elif r["status"] == "PARTIAL":
        tail = f"about {int((r['capture_ratio'] or 0) * 100)}% of the read-through is in the price"
    elif r["status"] == "OVERSHOT":
        tail = "already moved more than the mechanism justifies — this is the chase, not the edge"
    else:
        tail = "the read-through is in the price"
    return f"{lead}; {tail}."


# ═══════════════════════════ Telegram ═══════════════════════════════════════════
def telegram(text):
    if not (TG_TOKEN and TG_CHAT):
        return False
    try:
        body = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": text,
                                       "parse_mode": "HTML",
                                       "disable_web_page_preview": "true"}).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                                     data=body, headers=UA)
        urllib.request.urlopen(req, timeout=12).read()
        return True
    except Exception as e:  # noqa: BLE001
        print(f"[tg] {str(e)[:90]}")
        return False


# ═══════════════════════════ Handler ════════════════════════════════════════════
def lambda_handler(event=None, context=None):
    t0 = time.time()
    ev = event or {}
    global GAP_MIN_PCT
    GAP_MIN_PCT = float(ev.get("gap_min_pct", GAP_MIN_PCT))
    degraded = []

    tape, err = snapshot()
    if err:
        degraded.append(err)
    if not tape:
        payload = {"engine": "justhodl-readthrough", "version": VERSION, "ok": False,
                   "generated_at": datetime.now(timezone.utc).isoformat(),
                   "status": "DEGRADED", "degraded": degraded or ["no market tape"],
                   "events": [], "beneficiaries": [], "unpriced": [], "top_picks": []}
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload).encode(),
                      ContentType="application/json", CacheControl="max-age=60")
        return {"statusCode": 500, "body": json.dumps({"ok": False, "degraded": degraded})}

    hist = market_history()
    if len(hist) < 3:
        degraded.append(f"thin grouped-daily history ({len(hist)} sessions)")

    # Siblings — real published state, never invented
    scg = s3_json("data/supply-chain-graph.json")
    related = s3_json("data/polygon-related-graph.json")
    universe = s3_json("data/universe.json")
    f8k = s3_json("data/8k-filings.json")
    boom = s3_json("data/industry-boom.json")
    for nm, v in (("supply-chain-graph", scg), ("polygon-related-graph", related),
                  ("universe", universe), ("8k-filings", f8k)):
        if not v:
            degraded.append(f"sibling missing: {nm}")

    univ_meta = {}
    for row in ((universe or {}).get("stocks") or (universe or {}).get("symbols") or []):
        if isinstance(row, dict) and row.get("symbol") or row.get("ticker"):
            s = row.get("symbol") or row.get("ticker")
            univ_meta[s] = {"mcap": pick(row, "marketCap", "mcap", "market_cap"),
                            "sector": row.get("sector"), "industry": row.get("industry"),
                            "company": row.get("companyName") or row.get("name")}

    filings_by_ticker = {}
    for f in ((f8k or {}).get("filings") or []):
        if f.get("ticker"):
            filings_by_ticker.setdefault(f["ticker"], {
                "items": f.get("items"), "filed_at": f.get("filed_at"),
                "url": f.get("primary_url")})

    cats = find_catalysts(tape, univ_meta, filings_by_ticker)
    if not cats:
        payload = {"engine": "justhodl-readthrough", "version": VERSION, "ok": True,
                   "generated_at": datetime.now(timezone.utc).isoformat(),
                   "status": "QUIET", "degraded": degraded,
                   "note": (f"No name gapped >={GAP_MIN_PCT}% on a spend-implying catalyst "
                            "in the last 48h. Quiet tape is a real answer."),
                   "events": [], "beneficiaries": [], "unpriced": [], "top_picks": [],
                   "elapsed_s": round(time.time() - t0, 1)}
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload).encode(),
                      ContentType="application/json", CacheControl="max-age=120")
        print("[readthrough] QUIET — no qualifying catalysts")
        return {"statusCode": 200, "body": json.dumps({"ok": True, "events": 0})}

    g = build_graph(scg, related, univ_meta)

    # candidate sets per event
    per_event = {c["ticker"]: beneficiaries_for(c, g, tape, univ_meta) for c in cats}
    shortlist = sorted({r["ticker"] for rows in per_event.values() for r in rows}
                       | {c["ticker"] for c in cats})[:SHORTLIST_CAP]

    meta = {s: dict(univ_meta.get(s) or {}) for s in shortlist}
    with ThreadPoolExecutor(max_workers=12) as ex:
        for s, p in ex.map(fmp_profile, [s for s in shortlist if not meta.get(s, {}).get("mcap")]):
            meta.setdefault(s, {}).update({k: v for k, v in p.items() if v is not None})
        for s, rev in ex.map(fmp_revenue, shortlist):
            if rev:
                meta.setdefault(s, {})["revenue"] = rev
        for s, d2e in ex.map(fmp_days_to_earnings, shortlist):
            meta.setdefault(s, {})["days_to_earnings"] = d2e

    spy_now = tape.get("SPY", {}).get("last")
    all_rows, events = [], []
    for cat in cats:
        cat_day = anchor_day(cat.get("published"),
                             (date.today() - timedelta(days=1)).isoformat())
        spy_anchor = close_on_or_before(hist, "SPY", cat_day)
        spy_move = ((spy_now / spy_anchor - 1.0) * 100.0) if (spy_now and spy_anchor) else 0.0
        cat["spy_move_since_pct"] = round(spy_move, 2)
        cat["anchor_day"] = cat_day

        rows = []
        for r in per_event[cat["ticker"]]:
            try:
                sr = score_row(r, cat, tape, hist, meta, spy_move, cat_day)
                sr["thesis"] = plain_english(sr, cat)
                rows.append(sr)
            except Exception as e:  # noqa: BLE001
                print(f"[score] {r.get('ticker')}: {str(e)[:110]}")
        rows.sort(key=lambda x: -x["catch_up_score"])
        cat["n_beneficiaries"] = len(rows)
        cat["n_unpriced"] = sum(1 for x in rows if x["status"] == "UNPRICED")
        cat["order_value_str"] = (f"${cat['order_value_usd'] / 1e9:.1f}B"
                                  if cat.get("order_value_usd") else None)
        events.append(cat)
        all_rows.extend(rows)

    all_rows.sort(key=lambda x: -x["catch_up_score"])
    unpriced = [r for r in all_rows if r["status"] == "UNPRICED"]
    overshot = [r for r in all_rows if r["status"] == "OVERSHOT"][:12]

    top_picks = [{"ticker": r["ticker"], "score": r["catch_up_score"], "tier": r["tier"],
                  "status": r["status"], "catalyst": r["catalyst_ticker"],
                  "residual_pct": r["residual_pct"], "note": r["thesis"][:220]}
                 for r in unpriced
                 if r["tier"] in ("T1_DIRECT_SUPPLIER", "T2_TIER2_INPUT", "T3_INFRASTRUCTURE")
                 and r["catch_up_score"] >= 55][:15]

    ind_boom = {}
    for row in ((boom or {}).get("industries") or (boom or {}).get("league") or []):
        if isinstance(row, dict) and row.get("industry"):
            ind_boom[row["industry"]] = row.get("boom_score")

    payload = {
        "engine": "justhodl-readthrough", "version": VERSION, "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "DEGRADED" if degraded else "OK",
        "degraded": degraded,
        "thesis": ("A spend-implying catalyst on one name is a cash-flow claim on its chain. "
                   "This engine tiers every beneficiary by MECHANISM (supplier vs sympathy vs "
                   "competitor), sizes the claim against each company's own revenue, strips "
                   "market beta, and ranks what the tape has NOT yet paid for."),
        "params": {"gap_min_pct": GAP_MIN_PCT, "min_price": MIN_PRICE,
                   "min_dollar_vol": MIN_DOLLAR_VOL, "history_sessions": len(hist)},
        "n_events": len(events), "n_beneficiaries": len(all_rows), "n_unpriced": len(unpriced),
        "events": events,
        "unpriced": unpriced[:60],
        "beneficiaries": all_rows[:200],
        "chase_guard": overshot,
        "top_picks": top_picks,
        "tier_caps": TIER_CAPS,
        "tiers": {k: {"weight": v["w"], "capture_share": v["capture"], "label": v["label"]}
                  for k, v in TIERS.items()},
        "industry_boom_context": ind_boom,
        "data_source": ("Polygon snapshot + grouped-daily · FMP news/earnings/profile · "
                        "S3 siblings: supply-chain-graph, polygon-related-graph, 8k-filings, "
                        "universe, industry-boom"),
        "caveats": [
            "Polygon-inferred edges are demoted to T4 and can never enter a revenue tier. "
            "T4/T5 rows are NOT supplier claims. Sympathy and competitor-validation moves are "
            "multiple re-rates and mean-revert far more often than revenue read-throughs.",
            "order_value_usd is parsed from the headline/body. Announced orders are not "
            "recognised revenue — they can be delayed, re-cut or cancelled.",
            "MEASURE-BEFORE-TRUST: top_picks → signal-harvester (eng:justhodl-readthrough) → "
            "outcome-checker. This engine earns its scorecard slot like every other.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=120")

    # ── Telegram: only NEW, high-conviction, revenue-mechanism un-priced names ──
    state = s3_json(STATE_KEY) or {"sent": {}}
    now_iso = datetime.now(timezone.utc).isoformat()
    fresh = []
    for r in top_picks:
        key = f"{r['catalyst']}:{r['ticker']}"
        prev = state["sent"].get(key)
        if prev and (hours_since(prev) or 999) < 36:
            continue
        if r["score"] >= 70:
            state["sent"][key] = now_iso
            fresh.append(r)
    if fresh:
        head = fresh[0]
        ev0 = next((e for e in events if e["ticker"] == head["catalyst"]), {})
        lines = [f"<b>🔗 READ-THROUGH — un-priced beneficiaries</b>",
                 f"Catalyst: <b>{ev0.get('ticker')}</b> {ev0.get('move_pct'):+.1f}% · "
                 f"{ev0.get('type')}" + (f" · {ev0.get('order_value_str')}" if ev0.get("order_value_str") else ""),
                 f"<i>{(ev0.get('headline') or '')[:150]}</i>", ""]
        for r in fresh[:6]:
            lines.append(f"<b>{r['ticker']}</b> {r['tier'].split('_', 1)[1].replace('_', ' ').title()} "
                         f"· gap {r['residual_pct']:+.1f}pts · score {r['score']}")
        lines.append("")
        lines.append("justhodl.ai/readthrough.html")
        telegram("\n".join(lines))
        state["sent"] = {k: v for k, v in state["sent"].items()
                         if (hours_since(v) or 0) < 240}
        S3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps(state).encode(),
                      ContentType="application/json")

    print(f"[readthrough] events={len(events)} benef={len(all_rows)} "
          f"unpriced={len(unpriced)} picks={len(top_picks)} tg={len(fresh)} "
          f"in {payload['elapsed_s']}s degraded={degraded}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "events": [(e["ticker"], e["type"], e["move_pct"], e.get("order_value_str"))
                               for e in events],
        "unpriced_top": [(r["ticker"], r["tier"], r["residual_pct"], r["catch_up_score"])
                         for r in unpriced[:10]],
        "degraded": degraded})}
