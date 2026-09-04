"""justhodl-auction-desk -- the Treasury auctions desk engine (ops 5180, 2026-09-03).

Khalid: "auctions.html needs a major overhaul -- make sure it gets data from the
Treasury API daily, and give me a daily analysis of each auction; today's
$12.5B buyback should be flagged as easy monetary policy and a pump in risk
assets."

Sources (all public, no keys):
  * TreasuryDirect TA_WS securities/auctioned  -- SAME-DAY results (minutes after
    the 1:00pm ET close): high yield/rate, bid-to-cover, dealer/direct/indirect
    takedown, allotted-at-high, SOMA add-on, offering size.
  * TreasuryDirect TA_WS securities/announced + upcoming -- the forward calendar.
  * FiscalData accounting/od/auctions_query -- deep history (bank backfill).
  * FiscalData accounting/od/buybacks_operations + buybacks_security_details --
    every Treasury debt buyback operation (max par, offered, accepted, bucket,
    type) and the CUSIPs bought.
  * data/warm/treasury-par/curve.json.gz (the fleet's Treasury par-curve bank) --
    prior-close par yield of the matching tenor, for the tail proxy.

Analysis (deterministic, every number shown comes from the sources above):
  * per auction: z-scores vs the trailing 12 auctions of the same type+term
    for bid-to-cover, indirect share, dealer share, allotted-at-high and the
    tail proxy (high yield minus prior-close par yield of that tenor, bp --
    a true WI tail needs dealer WI quotes, which are not public); a demand
    score -> grade A..F and a verdict sentence.
  * per buyback: fill vs maximum, offered/accepted coverage, size percentile
    within the program, program pace; large max-fill operations are flagged
    as a liquidity injection ("easy-policy signal"), risk-asset supportive.
  * per day: headline + tags + liquidity / rates / risk-asset implications
    built from the day's operations; an optional Claude note (Haiku) written
    ONLY from those computed facts, cached per date.

Outputs:
  data/auction-desk.json                          latest (page feed)
  data/warm/treasury-auctions/history.json.gz     auction bank (3y+, keyed cusip|date)
  data/warm/treasury-auctions/buybacks.json       buyback bank
  data/warm/treasury-auctions/daily/{date}.json   day archive (incl. AI note)
"""
import gzip
import json
import os
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import bisect

import boto3

try:
    import crisis_scoring  # vendored justhodl-auction-crisis-detector scoring (same math for the 1996-> composite)
except Exception:  # pragma: no cover
    crisis_scoring = None

VERSION = "1.2.1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/auction-desk.json"
HIST_KEY = "data/warm/treasury-auctions/history.json.gz"
BUY_KEY = "data/warm/treasury-auctions/buybacks.json"
DAILY_PREFIX = "data/warm/treasury-auctions/daily/"
PAR_KEY = "data/warm/treasury-par/curve.json.gz"
FULL_KEY = "data/warm/treasury-auctions/history-full.json.gz"     # raw FiscalData rows since 1996 (composite history + reactions)
ASSETS_KEY = "data/warm/treasury-auctions/assets.json.gz"
COMPOSITE_KEY = "data/warm/treasury-auctions/composite-history.json"
REACT_KEY = "data/warm/treasury-auctions/reactions.json"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
FULL_FIELDS = ("cusip", "security_type", "security_term", "auction_date", "issue_date", "maturity_date", "high_rate", "high_yield",
               "high_discnt_rate", "high_investment_rate", "low_rate", "low_yield", "low_discnt_rate", "median_rate", "median_yield",
               "median_discnt_rate", "bid_to_cover_ratio", "primary_dealer_accepted", "direct_bidder_accepted", "indirect_bidder_accepted",
               "allocation_pctage", "total_accepted", "comp_accepted", "noncomp_accepted", "soma_accepted", "offering_amt", "reopening", "int_rate")
ASSETS = [("SPY", "S&P 500", "stocks"), ("QQQ", "Nasdaq 100", "stocks"), ("IWM", "Small caps", "stocks"),
          ("TLT", "Long Treasuries", "bonds"), ("IEF", "7-10Y Treasuries", "bonds"), ("HYG", "High-yield credit", "credit"),
          ("BTC-USD", "Bitcoin", "crypto"), ("ETH-USD", "Ether", "crypto"), ("GLD", "Gold", "gold"), ("SLV", "Silver", "silver"),
          ("VNQ", "Real estate (REITs)", "real estate"), ("UUP", "US dollar", "fx"), ("USO", "Oil", "commodities"), ("EEM", "Emerging markets", "stocks")]
HORIZONS = (("same_day", 0), ("d1", 1), ("d5", 5), ("d20", 20))
TD = "https://www.treasurydirect.gov/TA_WS/securities/"
FD = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/"
UA = "justhodl-auction-desk/" + VERSION
s3 = boto3.client("s3", region_name="us-east-1")

TERM_TENOR = {"4-Week": "1M", "8-Week": "2M", "13-Week": "3M", "17-Week": "4M", "26-Week": "6M", "52-Week": "1Y",
              "2-Year": "2Y", "3-Year": "3Y", "5-Year": "5Y", "7-Year": "7Y", "10-Year": "10Y", "20-Year": "20Y", "30-Year": "30Y",
              "9-Year 10-Month": "10Y", "9-Year 11-Month": "10Y", "19-Year 10-Month": "20Y", "19-Year 11-Month": "20Y",
              "29-Year 10-Month": "30Y", "29-Year 11-Month": "30Y", "4-Year 10-Month": "5Y", "4-Year 11-Month": "5Y",
              "6-Year 10-Month": "7Y", "6-Year 11-Month": "7Y", "1-Year 11-Month": "2Y", "2-Year 11-Month": "3Y"}


# ─────────────────────────── io helpers ──────────────────────────────
def _now():
    return datetime.now(timezone.utc)


def _iso():
    return _now().isoformat(timespec="seconds")


def _get_json(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _s3_json(key, default=None):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


def _put_json(key, obj, gz=False):
    body = json.dumps(obj, separators=(",", ":"), default=str).encode()
    kw = {"Bucket": BUCKET, "Key": key, "ContentType": "application/json", "CacheControl": "no-cache"}
    if gz:
        body = gzip.compress(body)
        kw["ContentEncoding"] = "gzip"
    s3.put_object(Body=body, **kw)


def _f(v):
    try:
        if v is None or v == "" or v == "null":
            return None
        return float(str(v).replace(",", ""))
    except Exception:
        return None


def _d(v):
    return str(v or "")[:10] or None


# ─────────────────────────── ingestion ───────────────────────────────
def norm_td(r):
    """TreasuryDirect auctioned/announced row -> canonical record."""
    ttype = str(r.get("securityType") or "")
    term = str(r.get("securityTerm") or "")
    hy = _f(r.get("highYield"))
    hir = _f(r.get("highInvestmentRate"))
    hdr = _f(r.get("highDiscountRate"))
    return {
        "cusip": r.get("cusip"), "type": ttype, "term": term, "tenor": TERM_TENOR.get(term),
        "reopening": str(r.get("reopening") or "").lower() == "yes",
        "auction_date": _d(r.get("auctionDate")), "issue_date": _d(r.get("issueDate")), "maturity_date": _d(r.get("maturityDate")),
        "announcement_date": _d(r.get("announcementDate")),
        "high_yield": hy if hy is not None else hir,          # bond-equivalent for bills (investment rate)
        "high_discount_rate": hdr, "interest_rate": _f(r.get("interestRate")),
        "btc": _f(r.get("bidToCoverRatio")),
        "pd": _f(r.get("primaryDealerAccepted")), "direct": _f(r.get("directBidderAccepted")), "indirect": _f(r.get("indirectBidderAccepted")),
        "total_accepted": _f(r.get("totalAccepted")), "total_tendered": _f(r.get("totalTendered")),
        "competitive_accepted": _f(r.get("competitiveAccepted")), "noncompetitive_accepted": _f(r.get("noncompetitiveAccepted")),
        "allocation_pct": _f(r.get("allocationPercentage")), "soma": _f(r.get("somaAccepted")),
        "offering": _f(r.get("offeringAmount")), "src": "treasurydirect",
    }


def norm_fd(r):
    """FiscalData auctions_query row -> canonical record."""
    term = str(r.get("security_term") or "")
    hy = _f(r.get("high_yield"))
    hir = _f(r.get("high_investment_rate"))
    return {
        "cusip": r.get("cusip"), "type": str(r.get("security_type") or ""), "term": term, "tenor": TERM_TENOR.get(term),
        "reopening": str(r.get("reopening") or "").lower() == "yes",
        "auction_date": _d(r.get("auction_date")), "issue_date": _d(r.get("issue_date")), "maturity_date": _d(r.get("maturity_date")),
        "announcement_date": _d(r.get("announcemt_date") or r.get("announcement_date")),
        "high_yield": hy if hy is not None else hir, "high_discount_rate": _f(r.get("high_discnt_rate")),
        "interest_rate": _f(r.get("int_rate")), "btc": _f(r.get("bid_to_cover_ratio")),
        "pd": _f(r.get("primary_dealer_accepted")), "direct": _f(r.get("direct_bidder_accepted")), "indirect": _f(r.get("indirect_bidder_accepted")),
        "total_accepted": _f(r.get("total_accepted")), "total_tendered": _f(r.get("total_tendered")),
        "competitive_accepted": _f(r.get("comp_accepted")), "noncompetitive_accepted": _f(r.get("noncomp_accepted")),
        "allocation_pct": _f(r.get("allocation_pctage")), "soma": _f(r.get("soma_accepted")),
        "offering": _f(r.get("offering_amt")), "src": "fiscaldata",
    }


def fetch_td(path, days=None):
    url = TD + path + "?format=json" + ("&days=%d" % days if days else "")
    rows = _get_json(url)
    return rows if isinstance(rows, list) else []


def fetch_fd(dataset, params, max_pages=40):
    out, page = [], 1
    while page <= max_pages:
        q = dict(params)
        q["page[number]"] = page
        q.setdefault("page[size]", 1000)
        q.setdefault("format", "json")
        url = FD + dataset + "?" + urllib.parse.urlencode(q, safe=":,-[]")
        body = _get_json(url)
        data = body.get("data") or []
        out.extend(data)
        if page >= int((body.get("meta") or {}).get("total-pages") or 1) or not data:
            break
        page += 1
    return out


def rec_key(r):
    return "%s|%s" % (r.get("cusip"), r.get("auction_date"))


# ─────────────────────────── par curve (tail proxy) ──────────────────
_PAR_DATES = {}


def par_prev_close(par_rows, auction_date, tenor):
    """Par yield of `tenor` on the last curve date strictly before the auction date."""
    if not par_rows or not tenor or not auction_date:
        return None, None
    key = id(par_rows)
    if key not in _PAR_DATES:
        _PAR_DATES[key] = sorted(par_rows)
    dates = _PAR_DATES[key]
    i = bisect.bisect_left(dates, auction_date) - 1
    if i < 0:
        return None, None
    d = dates[i]
    v = (par_rows.get(d) or {}).get(tenor)
    return (_f(v), d) if v is not None else (None, d)


# ─────────────────────────── analysis ────────────────────────────────
def shares(r):
    tot = r.get("total_accepted") or 0
    comp = r.get("competitive_accepted") or tot
    base = comp if comp else tot
    def pct(x):
        return round(100.0 * x / base, 1) if (x is not None and base) else None
    return {"pd_pct": pct(r.get("pd")), "direct_pct": pct(r.get("direct")), "indirect_pct": pct(r.get("indirect"))}


def z(value, hist):
    vals = [v for v in hist if v is not None]
    if value is None or len(vals) < 4:
        return None
    mu = statistics.fmean(vals)
    sd = statistics.pstdev(vals)
    if sd < 1e-9:
        return 0.0
    return round((value - mu) / sd, 2)


def analyze_bank(bank_sorted, par_rows, today):
    """Grade every settled auction in the bank (O(n): trailing-12 via per-tenor groups)."""
    groups = {}
    out = []
    for r in bank_sorted:
        if r.get("btc") is None or r["auction_date"] > today:
            continue
        g = groups.setdefault((r["type"], r["term"]), [])
        out.append(analyze_auction(r, None, par_rows, prior=g[-12:]))
        g.append(r)
    return out


def analyze_auction(r, bank_sorted, par_rows, prior=None):
    """Grade one auction against the trailing 12 same type+term auctions before it."""
    a = dict(r)
    a.update(shares(r))
    if prior is None:
        prior = [b for b in bank_sorted if b["type"] == r["type"] and b["term"] == r["term"] and b["auction_date"] < r["auction_date"]][-12:]
    for b in prior:
        b.update(shares(b))
    par, par_date = par_prev_close(par_rows, r["auction_date"], r.get("tenor"))
    a["par_prev_close"], a["par_prev_date"] = par, par_date
    a["tail_bp"] = round((r["high_yield"] - par) * 100, 1) if (r.get("high_yield") is not None and par is not None) else None
    tails = []
    for b in prior:
        bp, _ = par_prev_close(par_rows, b["auction_date"], b.get("tenor"))
        tails.append(round((b["high_yield"] - bp) * 100, 1) if (b.get("high_yield") is not None and bp is not None) else None)
    a["z"] = {
        "btc": z(r.get("btc"), [b.get("btc") for b in prior]),
        "indirect": z(a.get("indirect_pct"), [b.get("indirect_pct") for b in prior]),
        "pd": z(a.get("pd_pct"), [b.get("pd_pct") for b in prior]),
        "allocation": z(r.get("allocation_pct"), [b.get("allocation_pct") for b in prior]),
        "tail": z(a["tail_bp"], tails),
        "size": z(r.get("total_accepted"), [b.get("total_accepted") for b in prior]),
    }
    a["trailing12"] = {
        "n": len(prior),
        "btc": round(statistics.fmean([b["btc"] for b in prior if b.get("btc") is not None]), 2) if any(b.get("btc") is not None for b in prior) else None,
        "indirect_pct": round(statistics.fmean([b["indirect_pct"] for b in prior if b.get("indirect_pct") is not None]), 1) if any(b.get("indirect_pct") is not None for b in prior) else None,
        "pd_pct": round(statistics.fmean([b["pd_pct"] for b in prior if b.get("pd_pct") is not None]), 1) if any(b.get("pd_pct") is not None for b in prior) else None,
        "tail_bp": round(statistics.fmean([t for t in tails if t is not None]), 1) if any(t is not None for t in tails) else None,
    }
    # demand score: strong bid-to-cover and indirect demand, weak dealer absorption, no tail
    zz = a["z"]
    parts = []
    if zz["btc"] is not None:
        parts.append(1.0 * zz["btc"])
    if zz["indirect"] is not None:
        parts.append(0.8 * zz["indirect"])
    if zz["pd"] is not None:
        parts.append(-0.8 * zz["pd"])
    if zz["tail"] is not None:
        parts.append(-1.0 * zz["tail"])
    score = round(sum(parts) / max(len(parts), 1), 2) if parts else None
    a["demand_score"] = score
    a["grade"] = ("A" if score >= 1.0 else "B" if score >= 0.4 else "C" if score >= -0.4 else "D" if score >= -1.0 else "F") if score is not None else "n/a"
    a["score_parts"] = [
        {"metric": "Bid-to-cover", "z": zz["btc"], "weight": 1.0, "contribution": round(1.0 * zz["btc"], 2) if zz["btc"] is not None else None,
         "why": "more bids per dollar sold = more demand"},
        {"metric": "Indirect bidders", "z": zz["indirect"], "weight": 0.8, "contribution": round(0.8 * zz["indirect"], 2) if zz["indirect"] is not None else None,
         "why": "foreign central banks and big funds taking more = real-money demand"},
        {"metric": "Dealers", "z": zz["pd"], "weight": -0.8, "contribution": round(-0.8 * zz["pd"], 2) if zz["pd"] is not None else None,
         "why": "dealers forced to absorb more = weaker demand, so this counts against"},
        {"metric": "Tail", "z": zz["tail"], "weight": -1.0, "contribution": round(-1.0 * zz["tail"], 2) if zz["tail"] is not None else None,
         "why": "paying up vs the market (positive tail) = weaker; stopping through (negative) = stronger"},
    ]
    a["explain"] = explain_auction(a)
    a["verdict"] = auction_verdict(a)
    return a


def explain_auction(a):
    """Plain-English lines for people who do not read auction tapes. Every number is the auction's own."""
    t = a.get("trailing12") or {}
    is_bill = a["type"] == "Bill"
    unit = "bills" if is_bill else a["type"].lower() + "s"
    lines = []
    if a.get("btc") is not None:
        lines.append({"metric": "Bid-to-cover %.2f" % a["btc"],
                      "text": "Investors asked for $%.2f of %s for every $1 Treasury sold%s. Higher means more demand." % (
                          a["btc"], unit, (" (usual for this tenor lately: %.2f)" % t["btc"]) if t.get("btc") else "")})
    if a.get("indirect_pct") is not None:
        lines.append({"metric": "Indirect bidders %.0f%%" % a["indirect_pct"],
                      "text": "Share bought by foreign central banks and large funds bidding through dealers -- the 'real money' crowd%s. More is stronger." % (
                          (" (usual: %.0f%%)" % t["indirect_pct"]) if t.get("indirect_pct") else "")})
    if a.get("pd_pct") is not None:
        lines.append({"metric": "Dealers %.0f%%" % a["pd_pct"],
                      "text": "Share the primary dealers had to take onto their own books because nobody else bid for it%s. Less is better -- it means investors, not dealers, absorbed the supply." % (
                          (" (usual: %.0f%%)" % t["pd_pct"]) if t.get("pd_pct") else "")})
    if a.get("direct_pct") is not None:
        lines.append({"metric": "Direct bidders %.0f%%" % a["direct_pct"], "text": "Institutions (banks, funds, insurers) bidding for their own account, not via a dealer."})
    if a.get("tail_bp") is not None:
        tb = a["tail_bp"]
        if tb < -0.3:
            txt = "The auction cleared %.1f basis points BELOW where this tenor traded the day before -- buyers accepted a lower yield than the market offered. That is a strong auction ('stopped through')." % -tb
        elif tb > 0.3:
            txt = "The auction cleared %.1f basis points ABOVE where this tenor traded the day before -- Treasury had to pay a higher yield to find buyers. That is a weak auction ('tailed')." % tb
        else:
            txt = "The auction cleared right where the market traded the day before -- no concession needed either way."
        lines.append({"metric": "Tail %+.1fbp" % tb, "text": txt + (" (recent average for this tenor: %+.1fbp)" % t["tail_bp"] if t.get("tail_bp") is not None else "")})
    if a.get("allocation_pct") is not None:
        ap = a["allocation_pct"]
        lines.append({"metric": "Allotted at high %.0f%%" % ap,
                      "text": ("Of the bids placed at the highest accepted yield, %.0f%% were filled. A high number means the auction only just cleared at that level; a low number means demand was deep and most high-yield bids were unnecessary." % ap)})
    if a.get("high_yield") is not None:
        lines.append({"metric": ("High investment rate %.3f%%" % a["high_yield"]) if is_bill else ("High yield %.3f%%" % a["high_yield"]),
                      "text": ("The annualised yield the last accepted bidder got%s." % ((" (discount rate %.3f%%)" % a["high_discount_rate"]) if a.get("high_discount_rate") is not None else "")) if is_bill else
                              "The yield the last accepted bidder got -- everyone is filled at this single yield."})
    if a.get("soma"):
        lines.append({"metric": "SOMA %s" % fmt_bn(a["soma"]), "text": "The Federal Reserve rolled over maturing holdings by this amount -- an add-on outside the competitive bidding, not a sign of demand."})
    z_txt = ", ".join("%s %s" % (p["metric"].lower(), ("%+.1f" % p["z"]) + "σ") for p in a.get("score_parts", []) if p.get("z") is not None)
    if a.get("demand_score") is not None:
        lines.append({"metric": "Grade %s (score %+.2f)" % (a["grade"], a["demand_score"]),
                      "text": "Each metric is compared with the last 12 auctions of the same tenor (σ = how unusual it is). Demand score averages them: bid-to-cover and indirects count for, dealers and tail count against (%s). A ≥ +1.0, B ≥ +0.4, C ≥ -0.4, D ≥ -1.0, else F." % z_txt})
    return lines


def ttm_label(years):
    """Time to maturity as people say it: '3m', '1y 2m', '8y'."""
    if years is None:
        return None
    months = int(round(years * 12))
    y, m = divmod(months, 12)
    if y == 0:
        return "%dm" % max(m, 1)
    return "%dy" % y if m == 0 else "%dy %dm" % (y, m)


def fmt_bn(x):
    return "$%.1fB" % (x / 1e9) if x is not None else "n/a"


def auction_verdict(a):
    zz = a["z"]
    bits = []
    if a.get("btc") is not None:
        bits.append("bid-to-cover %.2f%s" % (a["btc"], (" (z %+.1f vs trailing 12)" % zz["btc"]) if zz.get("btc") is not None else ""))
    if a.get("indirect_pct") is not None:
        bits.append("indirects %.0f%%%s" % (a["indirect_pct"], (" (z %+.1f)" % zz["indirect"]) if zz.get("indirect") is not None else ""))
    if a.get("pd_pct") is not None:
        bits.append("dealers %.0f%%" % a["pd_pct"])
    if a.get("tail_bp") is not None:
        bits.append(("stopped through %.1fbp" % -a["tail_bp"]) if a["tail_bp"] < -0.3 else ("tailed %.1fbp" % a["tail_bp"]) if a["tail_bp"] > 0.3 else "on the screws")
    label = {"A": "STRONG demand", "B": "solid demand", "C": "average demand", "D": "soft demand", "F": "WEAK demand"}.get(a["grade"], "demand")
    return "%s %s %s -- %s: %s." % (a["term"], a["type"].lower(), fmt_bn(a.get("total_accepted")), label, ", ".join(bits) or "no takedown detail yet")


def implication_for_auction(a):
    coupon = a["type"] in ("Note", "Bond", "TIPS", "FRN")
    g = a["grade"]
    if coupon:
        if g in ("A", "B"):
            return {"rates": "duration well bid -> supports lower/stable yields", "risk": "supportive (term premium contained)", "tone": "bullish"}
        if g in ("D", "F"):
            return {"rates": "buyers demanded concession -> yields under upward pressure", "risk": "headwind if tails persist (higher discount rate for equities)", "tone": "bearish"}
        return {"rates": "no signal", "risk": "neutral", "tone": "neutral"}
    return {"rates": "front-end absorbed", "risk": "neutral", "tone": "neutral"} if g in ("A", "B", "C") else {"rates": "bill demand thinner than usual", "risk": "watch funding/collateral", "tone": "cautious"}


def analyze_buyback(op, program):
    """op: canonical buyback op. program: all ops (for percentiles/pace)."""
    b = dict(op)
    mx, off, acc = op.get("max_par"), op.get("offered"), op.get("accepted")
    b["fill_pct"] = round(100.0 * acc / mx, 1) if (acc is not None and mx) else None
    b["coverage"] = round(off / acc, 2) if (off is not None and acc) else None
    accs = sorted(x["accepted"] for x in program if x.get("accepted") is not None)
    b["size_pctile"] = round(100.0 * sum(1 for x in accs if x <= acc) / len(accs)) if (acc is not None and accs) else None
    tags = []
    if mx and mx >= 10e9:
        tags.append("LARGE OPERATION")
    if b["fill_pct"] is not None and b["fill_pct"] >= 99:
        tags.append("MAX FILL")
    if b["coverage"] is not None and b["coverage"] >= 2:
        tags.append("HEAVY OFFERS %.1fx" % b["coverage"])
    liq = "strong" if (b["fill_pct"] or 0) >= 90 and (mx or 0) >= 5e9 else "moderate" if (acc or 0) >= 2e9 else "light"
    b["liquidity_signal"] = liq
    if liq == "strong":
        tags.append("LIQUIDITY INJECTION")
        tags.append("EASY-POLICY SIGNAL")
        tags.append("RISK-ASSET BULLISH")
    b["tags"] = tags
    what = "Treasury bought back %s of %s (%s bucket%s)" % (
        fmt_bn(acc), (op.get("security_type") or "coupons").lower(),
        op.get("maturity_bucket") or "off-the-run", (", " + op["operation_type"].lower()) if op.get("operation_type") else "")
    why = ("dealers offered %s, %.1fx what Treasury took, and Treasury accepted the full maximum -- it removed that supply from the market and paid dealers cash for it. "
           "That is an easing impulse in effect: less duration for the street to warehouse, more liquidity in the system, the same direction as easy monetary policy."
           % (fmt_bn(off), b["coverage"] or 0)) if liq == "strong" else (
           "accepted %s of a %s maximum (%s%% fill); a routine liquidity-support operation with a modest liquidity effect." % (fmt_bn(acc), fmt_bn(mx), b["fill_pct"]))
    b["verdict"] = what + ". " + why
    b["implication"] = {
        "liquidity": "+%s of cash returned to the street" % fmt_bn(acc),
        "rates": "off-the-run supply removed in the %s bucket -> supportive for that part of the curve" % (op.get("maturity_bucket") or "target"),
        "risk": "supportive for risk assets" if liq == "strong" else "neutral",
        "tone": "bullish" if liq == "strong" else "neutral",
    }
    return b


def norm_buyback(r):
    """FiscalData buybacks_operations row -> canonical op (field names discovered by suffix)."""
    def pick(*cands):
        for k in r:
            lk = k.lower()
            if all(c in lk for c in cands):
                return _f(r.get(k))
        return None
    return {
        "operation_date": _d(r.get("operation_date")), "settlement_date": _d(r.get("settlement_date")),
        "start_time_est": r.get("operation_start_time_est"), "close_time_est": r.get("operation_close_time_est"),
        "operation_type": r.get("operation_type"), "security_type": r.get("security_type"), "maturity_bucket": r.get("maturity_bucket"),
        "max_par": pick("max", "par") or pick("maximum"),
        "offered": pick("offered", "par") or pick("offered"),
        "accepted": pick("accepted", "par") or pick("accepted"),
        "n_issues_eligible": pick("nbr_issues", "eligible") or pick("issues", "eligible"),
        "n_issues_accepted": pick("nbr_issues", "accepted") or pick("issues", "accepted"),
        "results_pdf": r.get("results_pdf"), "results_xml": r.get("results_xml"), "raw_fields": sorted(r.keys())[:40],
    }


def day_verdict(date, auctions, buybacks):
    tags, bullets = [], []
    strong = [a for a in auctions if a["grade"] in ("A", "B")]
    weak = [a for a in auctions if a["grade"] in ("D", "F")]
    coupons = [a for a in auctions if a["type"] in ("Note", "Bond", "TIPS", "FRN")]
    liq_ops = [b for b in buybacks if b["liquidity_signal"] == "strong"]
    tot_bills = sum(a.get("total_accepted") or 0 for a in auctions if a["type"] == "Bill")
    tot_coupons = sum(a.get("total_accepted") or 0 for a in coupons)
    buy_total = sum(b.get("accepted") or 0 for b in buybacks)
    risk = "neutral"
    if liq_ops:
        tags += ["LIQUIDITY EASY", "EASY-POLICY SIGNAL", "RISK-ASSET BULLISH"]
        risk = "bullish"
        for b in liq_ops:
            bullets.append("Buyback: %s accepted (%s%% of max, %.1fx offered) in the %s bucket -> supply removed, cash to dealers. Easing impulse; risk-asset supportive."
                           % (fmt_bn(b["accepted"]), b["fill_pct"], b["coverage"] or 0, b.get("maturity_bucket") or "target"))
    elif buybacks:
        tags.append("BUYBACK")
        bullets.append("Buyback: %s accepted; modest liquidity effect." % fmt_bn(buy_total))
    if coupons:
        if weak and not strong:
            tags += ["DEMAND WEAK", "TAIL RISK"]
            risk = "bearish" if risk != "bullish" else "mixed"
        elif strong and not weak:
            tags.append("DEMAND STRONG")
        else:
            tags.append("DEMAND MIXED")
        for a in coupons:
            bullets.append("%s (%s): %s" % (a["term"], a["grade"], a["verdict"]))
    if tot_bills:
        n = sum(1 for a in auctions if a["type"] == "Bill")
        bills = [a for a in auctions if a["type"] == "Bill"]
        g = "absorbed cleanly" if all(a["grade"] in ("A", "B", "C") for a in bills) else "met thinner demand"
        bullets.append("Bills: %s across %d auction%s %s (bid-to-cover %s)." % (
            fmt_bn(tot_bills), n, "s" if n != 1 else "", g, ", ".join("%.2f" % a["btc"] for a in bills if a.get("btc") is not None)))
    headline_parts = []
    if liq_ops:
        headline_parts.append("%s buyback at max" % fmt_bn(sum(b["accepted"] for b in liq_ops)))
    elif buybacks:
        headline_parts.append("%s buyback" % fmt_bn(buy_total))
    if coupons:
        headline_parts.append("%s coupons %s" % (fmt_bn(tot_coupons), "well bid" if strong and not weak else "tailed" if weak and not strong else "mixed"))
    if tot_bills:
        headline_parts.append("%s bills" % fmt_bn(tot_bills))
    headline = (" · ".join(headline_parts) or "no operations") + (" -> %s" % {"bullish": "risk-on supportive", "bearish": "risk-off pressure", "mixed": "mixed", "neutral": "neutral"}[risk])
    return {"date": date, "headline": headline, "tags": tags or ["QUIET"], "risk_assets": risk,
            "liquidity": "easy" if liq_ops else ("neutral" if not weak else "tightening bias"),
            "rates": ("duration well bid" if strong and not weak else "concession demanded" if weak and not strong else "no strong signal") if coupons else "front-end only",
            "bullets": bullets, "n_auctions": len(auctions), "n_buybacks": len(buybacks),
            "buyback_accepted": buy_total, "bills_accepted": tot_bills, "coupons_accepted": tot_coupons}


# ─────────────────────────── AI note (grounded, cached per day) ──────
def ai_note(facts):
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return None
    prompt = ("You are the JustHodl Treasury desk. Using ONLY the facts below (do not invent numbers), write a JSON object with keys "
              "\"what_happened\" (2 sentences), \"what_it_means\" (3 sentences on liquidity, rates and risk assets), \"watch_next\" (1-2 sentences). "
              "Plain institutional English, no hedging boilerplate.\n\nFACTS:\n" + json.dumps(facts, default=str)[:6000])
    body = {"model": "claude-haiku-4-5-20251001", "max_tokens": 600, "temperature": 0.2,
            "messages": [{"role": "user", "content": prompt + "\n\nRespond ONLY with valid JSON."}]}
    req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=json.dumps(body).encode(),
                                 headers={"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"})
    try:
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                data = json.loads(r.read())
        except urllib.error.HTTPError as he:
            detail = ""
            try:
                detail = he.read().decode("utf-8", "ignore")[:300]
            except Exception:
                pass
            raise RuntimeError("HTTP %s %s" % (he.code, detail))
        text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
        text = text[text.find("{"):text.rfind("}") + 1]
        note = json.loads(text)
        note["model"] = body["model"]
        note["generated_at"] = _iso()
        return note
    except Exception as e:
        print("[auction-desk] ai note failed:", str(e)[:200])
        return {"error": str(e)[:200]}


# ─────────────────────────── full history (1996->) ───────────────────
def load_full_bank(force=False):
    doc = _s3_json(FULL_KEY) or {}
    rows = doc.get("rows") or {}
    if rows and not force:
        # incremental: everything since the newest banked auction minus 10 days
        start = (datetime.fromisoformat(max(rows)[:10]) - timedelta(days=10)).date().isoformat()
    else:
        rows, start = {}, "1996-01-01"
    try:
        for r in fetch_fd("auctions_query", {"filter": "auction_date:gte:%s" % start, "sort": "-auction_date"}, max_pages=60):
            if not r.get("cusip") or not r.get("auction_date") or _f(r.get("bid_to_cover_ratio")) is None:
                continue
            rows["%s|%s" % (r["auction_date"][:10], r["cusip"])] = {k: r.get(k) for k in FULL_FIELDS if r.get(k) not in (None, "", "null")}
        doc = {"version": VERSION, "rows": rows, "as_of": _iso(), "n": len(rows), "first": min(rows)[:10] if rows else None, "last": max(rows)[:10] if rows else None}
        _put_json(FULL_KEY, doc, gz=True)
    except Exception as e:
        doc.setdefault("rows", rows)
        doc["error"] = str(e)[:160]
    return doc


def fetch_fred_daily(series, obs=12000):
    try:
        body = _get_json("%s/fred?series=%s&obs=%d" % (PROXY, series, obs), timeout=40)
        out = {}
        for b in body.get("bars") or []:
            d = b.get("date") or (datetime.fromtimestamp(int(b["time"]), tz=timezone.utc).date().isoformat() if b.get("time") else None)
            if d and _f(b.get("value")) is not None:
                out[d[:10]] = _f(b.get("value"))
        return out
    except Exception:
        return {}


def build_composite_history(full_rows, ff_by_date, live_point=None):
    """Detector-faithful composite (0.7*max + 0.3*avg per auction, size-weighted over a 14-day window,
    + up to 15 points of bill-issuance overlay), sampled weekly from 1996 and daily for the last 60 days."""
    if not crisis_scoring or not full_rows:
        return {"series": [], "note": "scoring module unavailable"}
    scored = []
    ff_dates = sorted(ff_by_date)
    for key in sorted(full_rows):
        r = full_rows[key]
        d = r.get("auction_date", "")[:10]
        try:
            m = crisis_scoring.compute_record_metrics(r)
        except Exception:
            continue
        if not m.get("btc"):
            continue
        i = bisect.bisect_right(ff_dates, d) - 1
        ff = ff_by_date[ff_dates[i]] if i >= 0 else 0.0
        sc = crisis_scoring.score_indicators(m, ff)
        if not sc:
            continue
        mx, av = max(sc.values()), sum(sc.values()) / len(sc)
        scored.append((d, 0.7 * mx + 0.3 * av, m.get("accepted_billions") or 1.0, "BILL" in str(r.get("security_type", "")).upper(), _f(r.get("total_accepted")) or 0.0))
    scored.sort()
    dates = [x[0] for x in scored]
    today = _now().date()
    first = datetime.fromisoformat(min(dates)).date() if dates else today
    samples = []
    d = first
    while d <= today - timedelta(days=60):
        samples.append(d)
        d += timedelta(days=7)
    d = max(d, today - timedelta(days=60))
    while d <= today:
        samples.append(d)
        d += timedelta(days=1)
    series = []
    for D in samples:
        lo, hi = (D - timedelta(days=14)).isoformat(), D.isoformat()
        a, b = bisect.bisect_left(dates, lo), bisect.bisect_right(dates, hi)
        win = scored[a:b]
        if not win:
            continue
        tot = sum(w[2] for w in win)
        comp = sum(w[1] * w[2] for w in win) / tot if tot else 0.0
        # issuance overlay: bills 4w average per auction day vs trailing 1y
        a1 = bisect.bisect_left(dates, (D - timedelta(days=365)).isoformat())
        a4 = bisect.bisect_left(dates, (D - timedelta(days=28)).isoformat())
        by_day_1y, by_day_4w = {}, {}
        for w in scored[a1:b]:
            if w[3] and w[4]:
                by_day_1y[w[0]] = by_day_1y.get(w[0], 0) + w[4]
        for w in scored[a4:b]:
            if w[3] and w[4]:
                by_day_4w[w[0]] = by_day_4w.get(w[0], 0) + w[4]
        if by_day_1y and by_day_4w:
            avg4 = sum(by_day_4w.values()) / len(by_day_4w)
            avg1 = sum(by_day_1y.values()) / len(by_day_1y)
            pct = (avg4 - avg1) / avg1 * 100 if avg1 else 0
            iss = 90 if pct > 50 else 60 if pct > 30 else 30 if pct > 15 else 0
            comp = min(100, comp + iss * 0.15)
        series.append({"date": hi, "composite": round(comp, 1), "n": len(win)})
    if live_point and live_point.get("date") and live_point.get("composite") is not None:
        series = [p for p in series if p["date"] != live_point["date"]]
        series.append({"date": live_point["date"], "composite": round(float(live_point["composite"]), 1), "n": live_point.get("n") or 0, "live": True})
        series.sort(key=lambda p: p["date"])
    return {"series": series, "sampled": "weekly to T-60d, daily after", "first": series[0]["date"] if series else None,
            "last": series[-1]["date"] if series else None, "n_auctions_scored": len(scored), "method": "detector composite: 0.7*max+0.3*avg per auction, size-weighted 14d window, +0-15 issuance overlay"}


# ─────────────────────────── cross-asset reactions ───────────────────
def load_assets(force=False):
    doc = _s3_json(ASSETS_KEY) or {}
    fresh = doc.get("as_of") and (Date_parse(doc["as_of"]) > _now() - timedelta(hours=20))
    if fresh and not force and doc.get("series"):
        return doc
    series = dict(doc.get("series") or {})
    for sym, name, cls in ASSETS:
        try:
            body = _get_json("%s/yf-ohlc?symbol=%s&range=max&interval=1d" % (PROXY, urllib.parse.quote(sym)), timeout=40)
            bars = body.get("bars") or []
            dates, closes = [], []
            for b in bars:
                t = b.get("time")
                c = _f(b.get("close"))
                if t is None or c is None:
                    continue
                d = datetime.fromtimestamp(int(t), tz=timezone.utc).date().isoformat() if isinstance(t, (int, float)) else str(t)[:10]
                if d >= "1995-01-01":
                    dates.append(d)
                    closes.append(c)
            if len(dates) > 100:
                series[sym] = {"name": name, "class": cls, "dates": dates, "closes": closes}
        except Exception as e:
            print("[auction-desk] asset %s failed: %s" % (sym, str(e)[:80]))
    doc = {"version": VERSION, "as_of": _iso(), "series": series}
    _put_json(ASSETS_KEY, doc, gz=True)
    return doc


def Date_parse(iso):
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except Exception:
        return _now() - timedelta(days=9)


def fwd_returns(ser, day):
    """same-day (auction-day close vs prior close), +1/+5/+20 trading days after the auction day.
    Requires a bar ON the auction day (a stale series must not borrow the previous close); bars dated
    today (UTC) are in progress and flagged partial."""
    dates, closes = ser["dates"], ser["closes"]
    i = bisect.bisect_left(dates, day)
    if i >= len(dates) or dates[i] != day or i < 1:
        return None
    today_utc = _now().date().isoformat()
    out = {"same_day": (closes[i] / closes[i - 1] - 1) if closes[i - 1] else None, "partial": []}
    if dates[i] >= today_utc:
        out["partial"].append("same_day")
    for label, n in HORIZONS[1:]:
        j = i + n
        out[label] = (closes[j] / closes[i] - 1) if j < len(closes) and closes[i] else None
        if j < len(closes) and dates[j] >= today_utc:
            out["partial"].append(label)
    return out


def day_class(auctions, buybacks):
    coupons = [a for a in auctions if a["type"] in ("Note", "Bond", "TIPS", "FRN")]
    strong_bb = any(b.get("liquidity_signal") == "strong" for b in buybacks)
    cls = []
    if strong_bb:
        cls.append("buyback_strong")
    if coupons:
        grades = [a["grade"] for a in coupons if a["grade"] in "ABCDF" and a["grade"] != "n/a"]
        if grades and all(g in ("A", "B") for g in grades):
            cls.append("coupon_strong")
        elif grades and any(g in ("D", "F") for g in grades):
            cls.append("coupon_weak")
        elif grades:
            cls.append("coupon_mixed")
    elif auctions:
        cls.append("bills_only")
    return cls or ["none"]


CLASS_LABEL = {"buyback_strong": "large max-fill buyback day", "coupon_strong": "coupon auctions well bid (A/B)",
               "coupon_weak": "a coupon auction tailed / graded D-F", "coupon_mixed": "coupon auctions mixed (C)", "bills_only": "bills only", "none": "no operations"}


def build_reactions(day_ops, assets, today_key):
    """day_ops: {date: {"auctions": [...analyzed], "buybacks": [...analyzed]}} over the analysed history.
    Returns conditional forward-return stats per class x asset x horizon, today's prediction, and the realised scoreboard."""
    events = []
    for d, ops in day_ops.items():
        if d >= today_key:
            continue
        for c in day_class(ops["auctions"], ops["buybacks"]):
            events.append((d, c))
    stats = {}
    base = {}
    for sym, ser in assets.items():
        # unconditional baseline: every auction day
        rets = [fwd_returns(ser, d) for d in day_ops if d < today_key]
        rets = [r for r in rets if r]
        base[sym] = {h: _dist([r[h] for r in rets if r.get(h) is not None]) for h, _ in HORIZONS}
        for cls in set(c for _, c in events):
            days = [d for d, c in events if c == cls]
            rr = [fwd_returns(ser, d) for d in days]
            rr = [r for r in rr if r]
            stats.setdefault(cls, {})[sym] = {h: _dist([r[h] for r in rr if r.get(h) is not None]) for h, _ in HORIZONS}
    return {"stats": stats, "baseline": base, "n_events": {c: sum(1 for _, x in events if x == c) for c in set(c for _, c in events)},
            "classes": CLASS_LABEL, "horizons": [h for h, _ in HORIZONS]}


def _dist(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 3:
        return {"n": len(xs), "median": None, "mean": None, "hit": None}
    xs_sorted = sorted(xs)
    med = xs_sorted[len(xs) // 2] if len(xs) % 2 else (xs_sorted[len(xs) // 2 - 1] + xs_sorted[len(xs) // 2]) / 2
    return {"n": len(xs), "median": round(med * 100, 2), "mean": round(sum(xs) / len(xs) * 100, 2), "hit": round(100.0 * sum(1 for x in xs if x > 0) / len(xs))}


def predict_today(classes, reactions, assets):
    """Per asset: the conditional distribution for today's class (prefer the rarest informative class), the call, and the edge vs baseline."""
    order = ["buyback_strong", "coupon_weak", "coupon_strong", "coupon_mixed", "bills_only"]
    use = [c for c in order if c in classes and c in reactions["stats"]]
    rows = []
    for sym, name, cls_name in ASSETS:
        if sym not in assets:
            continue
        chosen, st = None, None
        for c in use:
            cand = reactions["stats"].get(c, {}).get(sym)
            if cand and (cand.get("d1", {}).get("n") or 0) >= 8:
                chosen, st = c, cand
                break
        if not st:
            continue
        b = reactions["baseline"].get(sym, {})
        h1, h5, h20 = st.get("d1", {}), st.get("d5", {}), st.get("d20", {})
        med1, hit1 = h1.get("median"), h1.get("hit")
        call = "→"
        if med1 is not None and hit1 is not None:
            if med1 >= 0.15 and hit1 >= 58:
                call = "↑"
            elif med1 <= -0.15 and hit1 <= 42:
                call = "↓"
        conf = "high" if (hit1 is not None and abs(hit1 - 50) >= 15 and h1.get("n", 0) >= 30) else "medium" if (hit1 is not None and abs(hit1 - 50) >= 10 and h1.get("n", 0) >= 15) else "low"
        rows.append({"symbol": sym, "name": name, "asset_class": cls_name, "basis": chosen, "basis_label": CLASS_LABEL.get(chosen, chosen), "n": h1.get("n"),
                     "same_day": st.get("same_day"), "d1": h1, "d5": h5, "d20": h20, "baseline_d1": b.get("d1"), "call": call, "confidence": conf,
                     "edge_d1": round((med1 or 0) - ((b.get("d1") or {}).get("median") or 0), 2) if med1 is not None else None})
    return rows


def realised_scoreboard(day_ops, assets, day_list, verdicts, n=6):
    board = []
    for d in day_list[:n + 1]:
        ops = day_ops.get(d) or {"auctions": [], "buybacks": []}
        cls = day_class(ops["auctions"], ops["buybacks"])
        moves = {}
        for sym in ("SPY", "QQQ", "TLT", "HYG", "BTC-USD", "GLD", "SLV", "VNQ"):
            ser = assets.get(sym)
            if not ser:
                continue
            r = fwd_returns(ser, d)
            if r:
                moves[sym] = {"same_day": round(r["same_day"] * 100, 2) if r.get("same_day") is not None else None, "d1": round(r["d1"] * 100, 2) if r.get("d1") is not None else None,
                              "partial": r.get("partial") or []}
            else:
                moves[sym] = {"same_day": None, "d1": None, "partial": [], "pending": True}
        v = verdicts.get(d) or {}
        board.append({"date": d, "classes": cls, "labels": [CLASS_LABEL.get(c, c) for c in cls], "headline": v.get("headline"), "risk_assets": v.get("risk_assets"),
                      "grades": [a["grade"] for a in ops["auctions"]], "buyback_accepted": sum(b.get("accepted") or 0 for b in ops["buybacks"]), "moves": moves})
    return board


# ─────────────────────────── main ────────────────────────────────────
def lambda_handler(event, ctx):
    t0 = time.time()
    event = event or {}
    notes = []
    today = _now().date().isoformat()

    # 1) history bank (backfill 3y from FiscalData once; TD refreshes the tail)
    hist = _s3_json(HIST_KEY) or {"version": VERSION, "records": {}}
    records = hist.get("records") or {}
    if not records or event.get("backfill"):
        start = (_now() - timedelta(days=int(event.get("backfill_days") or 1100))).date().isoformat()
        try:
            for r in fetch_fd("auctions_query", {"filter": "auction_date:gte:%s" % start, "sort": "-auction_date"}):
                rec = norm_fd(r)
                # announced-but-not-yet-auctioned rows carry no results: they belong to the calendar, not the bank
                if rec["cusip"] and rec["auction_date"] and rec.get("btc") is not None:
                    records.setdefault(rec_key(rec), rec)
            notes.append("backfill %d records since %s" % (len(records), start))
        except Exception as e:
            notes.append("backfill failed: %s" % str(e)[:120])
    td_rows = []
    try:
        td_rows = fetch_td("auctioned", days=int(event.get("days") or 120))
        for r in td_rows:
            rec = norm_td(r)
            if rec["cusip"] and rec["auction_date"] and rec.get("btc") is not None:
                records[rec_key(rec)] = rec           # same-day TD row wins over the FiscalData copy
    except Exception as e:
        notes.append("treasurydirect auctioned failed: %s" % str(e)[:120])
    hist.update({"version": VERSION, "records": records, "as_of": _iso(), "n": len(records)})
    _put_json(HIST_KEY, hist, gz=True)

    # 2) buybacks
    buy = _s3_json(BUY_KEY) or {"operations": {}}
    ops = buy.get("operations") or {}
    try:
        for r in fetch_fd("buybacks_operations", {"sort": "-operation_date"}):
            op = norm_buyback(r)
            if op["operation_date"]:
                ops["%s|%s" % (op["operation_date"], op.get("maturity_bucket") or "")] = op
        notes.append("buybacks %d operations" % len(ops))
    except Exception as e:
        notes.append("buybacks failed: %s" % str(e)[:120])
    details = []
    try:
        since = (_now() - timedelta(days=45)).date().isoformat()
        details = fetch_fd("buybacks_security_details", {"filter": "operation_date:gte:%s" % since, "sort": "-operation_date", "page[size]": 500}, max_pages=3)
    except Exception as e:
        notes.append("buyback details failed: %s" % str(e)[:120])
    buy.update({"version": VERSION, "operations": ops, "as_of": _iso(), "n": len(ops)})
    _put_json(BUY_KEY, buy)

    # 3) calendar
    calendar_auctions, calendar_note = [], ""
    try:
        seen = set()
        for path, days in (("upcoming", None), ("announced", 21)):
            for r in fetch_td(path, days=days):
                rec = norm_td(r)
                if rec["cusip"] and rec["auction_date"] and rec["auction_date"] >= today and rec_key(rec) not in seen:
                    seen.add(rec_key(rec))
                    calendar_auctions.append({k: rec[k] for k in ("cusip", "type", "term", "tenor", "auction_date", "issue_date", "maturity_date", "offering", "reopening", "announcement_date")})
        calendar_auctions.sort(key=lambda x: (x["auction_date"], x["term"]))
    except Exception as e:
        calendar_note = "calendar failed: %s" % str(e)[:120]

    # 4) analysis
    par_rows = (_s3_json(PAR_KEY) or {}).get("rows") or {}
    bank_sorted = sorted(records.values(), key=lambda r: r["auction_date"])
    analyzed_all = analyze_bank(bank_sorted, par_rows, today)
    cutoff = (_now() - timedelta(days=400)).date().isoformat()
    analyzed = [a for a in analyzed_all if a["auction_date"] >= cutoff]
    analyzed.sort(key=lambda a: (a["auction_date"], a["term"]), reverse=True)
    program = [op for op in ops.values() if op.get("accepted") is not None]
    program.sort(key=lambda o: o["operation_date"])
    bb_analyzed = [analyze_buyback(op, program) for op in program]
    bb_analyzed.sort(key=lambda b: b["operation_date"], reverse=True)
    term_by_cusip = {}
    for r in records.values():
        if r.get("cusip") and r.get("term"):
            term_by_cusip.setdefault(r["cusip"], "%s %s" % (r["term"], r["type"]))
    for b in bb_analyzed[:12]:
        secs = []
        for d in details:
            if _d(d.get("operation_date")) != b["operation_date"] or not _f(d.get("par_amt_accepted")):
                continue
            mat = _d(d.get("maturity_date"))
            ttm = None
            try:
                ttm = (datetime.fromisoformat(mat) - datetime.fromisoformat(b["operation_date"])).days / 365.25
            except Exception:
                pass
            secs.append({"cusip": d.get("cusip_nbr"), "coupon": _f(d.get("coupon_rate_pct")), "maturity": mat,
                         "par_accepted": _f(d.get("par_amt_accepted")), "price": _f(d.get("weighted_avg_accepted_price")),
                         "ttm_years": round(ttm, 2) if ttm is not None else None, "ttm_label": ttm_label(ttm),
                         "orig_term": term_by_cusip.get(d.get("cusip_nbr"))})
        secs.sort(key=lambda x: x.get("ttm_years") or 0)
        b["securities"] = secs[:80]

    # day buckets: the full analysed history feeds the reaction model; the desk shows the last 30 operation days
    days_all = {}
    for a in analyzed_all:
        days_all.setdefault(a["auction_date"], {"auctions": [], "buybacks": []})["auctions"].append(a)
    for b in bb_analyzed:
        days_all.setdefault(b["operation_date"], {"auctions": [], "buybacks": []})["buybacks"].append(b)
    days = {d: v for d, v in days_all.items() if d >= cutoff}
    day_list = sorted(days, reverse=True)[:30]
    verdicts = {d: day_verdict(d, days[d]["auctions"], days[d]["buybacks"]) for d in day_list}
    latest_day = day_list[0] if day_list else None

    # cross-asset reactions (history from the same graded bank; prices via the fleet proxy, cached ~daily)
    reactions, prediction, scoreboard, assets_note = None, [], [], ""
    try:
        assets = (load_assets(force=bool(event.get("assets"))) or {}).get("series") or {}
        reactions = build_reactions(days_all, assets, latest_day or today)
        today_classes = day_class(days[latest_day]["auctions"], days[latest_day]["buybacks"]) if latest_day else ["none"]
        prediction = predict_today(today_classes, reactions, assets)
        scoreboard = realised_scoreboard(days_all, assets, day_list, verdicts)
        assets_note = "%d assets, %d graded auction days" % (len(assets), len([d for d in days_all if d < (latest_day or today)]))
    except Exception as e:
        assets_note = "reactions failed: %s" % str(e)[:140]
        today_classes = ["none"]

    # composite history 1996-> (weekly; daily last 60d) with the detector's own live point appended
    composite = None
    try:
        full = load_full_bank(force=bool(event.get("history")))
        live = None
        try:
            ac = _s3_json("data/auction-crisis.json") or {}
            live = {"date": today, "composite": ac.get("composite_score"), "n": ac.get("n_recent_auctions_14d")}
        except Exception:
            pass
        composite = build_composite_history(full.get("rows") or {}, fetch_fred_daily("DFF"), live)
        composite["bank"] = {"first": full.get("first"), "last": full.get("last"), "n": full.get("n"), "error": full.get("error")}
        _put_json(COMPOSITE_KEY, composite)
    except Exception as e:
        composite = {"series": [], "error": str(e)[:160]}

    # program stats
    fy_start = ("%d-10-01" % (_now().year - 1)) if _now().month < 10 else ("%d-10-01" % _now().year)
    last4w = (_now() - timedelta(days=28)).date().isoformat()
    program_stats = {
        "n_ops": len(program), "since": program[0]["operation_date"] if program else None,
        "total_accepted": sum(o["accepted"] for o in program),
        "fy_accepted": sum(o["accepted"] for o in program if o["operation_date"] >= fy_start), "fy_start": fy_start,
        "last_4w_accepted": sum(o["accepted"] for o in program if o["operation_date"] >= last4w),
        "avg_fill_pct": round(statistics.fmean([b["fill_pct"] for b in bb_analyzed if b.get("fill_pct") is not None]), 1) if bb_analyzed else None,
        "by_bucket": {},
    }
    for o in program:
        k = o.get("maturity_bucket") or "n/a"
        program_stats["by_bucket"][k] = program_stats["by_bucket"].get(k, 0) + o["accepted"]

    # per-term trailing stats (for charts)
    by_term = {}
    for a in analyzed:
        k = "%s %s" % (a["type"], a["term"])
        by_term.setdefault(k, []).append({"d": a["auction_date"], "btc": a.get("btc"), "indirect": a.get("indirect_pct"), "pd": a.get("pd_pct"),
                                          "tail": a.get("tail_bp"), "hy": a.get("high_yield"), "size": a.get("total_accepted"), "grade": a["grade"]})

    # 5) AI note for the latest day (cached in the archive)
    ai = None
    if latest_day:
        arch_key = DAILY_PREFIX + latest_day + ".json"
        prev = _s3_json(arch_key) or {}
        fingerprint = json.dumps([(a["cusip"], a.get("btc"), a["grade"]) for a in days[latest_day]["auctions"]] +
                                 [(b["operation_date"], b.get("accepted")) for b in days[latest_day]["buybacks"]], sort_keys=True)
        if prev.get("fingerprint") == fingerprint and prev.get("ai_note") and not prev["ai_note"].get("error"):
            ai = prev["ai_note"]
        elif not event.get("no_ai"):
            ai = ai_note({"date": latest_day, "verdict": verdicts[latest_day],
                          "auctions": [{k: a.get(k) for k in ("term", "type", "total_accepted", "btc", "indirect_pct", "pd_pct", "tail_bp", "grade", "z", "trailing12")} for a in days[latest_day]["auctions"]],
                          "buybacks": [{k: b.get(k) for k in ("operation_date", "maturity_bucket", "operation_type", "max_par", "offered", "accepted", "fill_pct", "coverage", "tags")} for b in days[latest_day]["buybacks"]],
                          "program": program_stats})
        if ai and ai.get("error"):
            notes.append("ai note: " + ai["error"])
            ai = None
        _put_json(arch_key, {"date": latest_day, "fingerprint": fingerprint, "verdict": verdicts[latest_day],
                             "auctions": days[latest_day]["auctions"], "buybacks": days[latest_day]["buybacks"], "ai_note": ai, "written_at": _iso()})

    out = {
        "version": VERSION, "generated_at": _iso(), "elapsed_s": round(time.time() - t0, 1), "notes": notes + ([calendar_note] if calendar_note else []),
        "sources": {"same_day": "treasurydirect.gov TA_WS securities/auctioned", "history": "api.fiscaldata.treasury.gov auctions_query",
                    "buybacks": "api.fiscaldata.treasury.gov buybacks_operations + buybacks_security_details",
                    "calendar": "treasurydirect.gov TA_WS securities/upcoming + announced", "par_curve": "home.treasury.gov daily par yield curve (fleet bank)"},
        "freshness": {"newest_auction": analyzed[0]["auction_date"] if analyzed else None, "newest_buyback": bb_analyzed[0]["operation_date"] if bb_analyzed else None,
                      "par_curve_last": max(par_rows) if par_rows else None, "td_rows": len(td_rows), "bank_records": len(records)},
        "today": {"date": latest_day, "verdict": verdicts.get(latest_day), "auctions": days[latest_day]["auctions"] if latest_day else [],
                  "buybacks": days[latest_day]["buybacks"] if latest_day else [], "ai_note": ai,
                  "implications": {a["cusip"]: implication_for_auction(a) for a in (days[latest_day]["auctions"] if latest_day else [])}},
        "recent_days": [verdicts[d] for d in day_list],
        "auctions": analyzed[:200],
        "buybacks": {"operations": bb_analyzed, "program": program_stats},
        "calendar": {"auctions": calendar_auctions[:40], "buybacks": [dict(b, securities=None) for b in bb_analyzed if b["operation_date"] > today][:10]},
        "by_term": by_term,
        "composite_history": composite,
        "reactions": {"prediction": prediction, "today_classes": today_classes, "class_labels": CLASS_LABEL, "scoreboard": scoreboard,
                      "stats": (reactions or {}).get("stats"), "baseline": (reactions or {}).get("baseline"), "n_events": (reactions or {}).get("n_events"),
                      "note": assets_note, "horizons": "same_day = auction-day close vs prior close (results land at 1pm ET); d1/d5/d20 = trading days after the auction day; crypto trades 7 days"},
        "methodology": {
            "tail_proxy": "high yield (bills: investment rate) minus the prior-close Treasury par yield of the matching tenor, in bp; a true when-issued tail needs dealer WI quotes",
            "z_scores": "vs the trailing 12 auctions of the same type and term", "grade": "demand score = z(bid-to-cover) + 0.8 z(indirect) - 0.8 z(dealer) - z(tail); A >= 1.0, B >= 0.4, C >= -0.4, D >= -1.0, else F",
            "buyback_signal": "strong = accepted >= 90% of maximum and maximum >= $5B (tags LIQUIDITY INJECTION / EASY-POLICY SIGNAL / RISK-ASSET BULLISH)",
            "reactions": "for every graded auction day since the bank starts, forward returns of each asset are bucketed by the day's class (buyback_strong, coupon_weak, coupon_strong, coupon_mixed, bills_only); today's prediction shows the median and hit-rate of those buckets -- conditional history, not a forecast model"},
    }
    _put_json(OUT_KEY, out)
    return {"ok": True, "elapsed_s": out["elapsed_s"], "newest_auction": out["freshness"]["newest_auction"], "newest_buyback": out["freshness"]["newest_buyback"],
            "today": latest_day, "headline": (verdicts.get(latest_day) or {}).get("headline"), "notes": out["notes"], "ai": bool(ai)}
