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
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/auction-desk.json"
HIST_KEY = "data/warm/treasury-auctions/history.json.gz"
BUY_KEY = "data/warm/treasury-auctions/buybacks.json"
DAILY_PREFIX = "data/warm/treasury-auctions/daily/"
PAR_KEY = "data/warm/treasury-par/curve.json.gz"
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
def par_prev_close(par_rows, auction_date, tenor):
    """Par yield of `tenor` on the last curve date strictly before the auction date."""
    if not par_rows or not tenor or not auction_date:
        return None, None
    dates = [d for d in par_rows if d < auction_date]
    if not dates:
        return None, None
    d = max(dates)
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


def analyze_auction(r, bank_sorted, par_rows):
    """Grade one auction against the trailing 12 same type+term auctions before it."""
    a = dict(r)
    a.update(shares(r))
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
    a["verdict"] = auction_verdict(a)
    return a


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
        with urllib.request.urlopen(req, timeout=60) as r:
            data = json.loads(r.read())
        text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
        text = text[text.find("{"):text.rfind("}") + 1]
        note = json.loads(text)
        note["model"] = body["model"]
        note["generated_at"] = _iso()
        return note
    except Exception as e:
        print("[auction-desk] ai note failed:", str(e)[:120])
        return None


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
                if rec["cusip"] and rec["auction_date"]:
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
    recent = [r for r in bank_sorted if r["auction_date"] >= (_now() - timedelta(days=400)).date().isoformat()]
    analyzed = [analyze_auction(r, bank_sorted, par_rows) for r in recent]
    analyzed.sort(key=lambda a: (a["auction_date"], a["term"]), reverse=True)
    program = [op for op in ops.values() if op.get("accepted") is not None]
    program.sort(key=lambda o: o["operation_date"])
    bb_analyzed = [analyze_buyback(op, program) for op in program]
    bb_analyzed.sort(key=lambda b: b["operation_date"], reverse=True)
    for b in bb_analyzed[:12]:
        b["securities"] = [{"cusip": d.get("cusip_nbr"), "coupon": _f(d.get("coupon_rate_pct")), "maturity": _d(d.get("maturity_date")),
                            "par_accepted": _f(d.get("par_amt_accepted")), "price": _f(d.get("weighted_avg_accepted_price"))}
                           for d in details if _d(d.get("operation_date")) == b["operation_date"] and _f(d.get("par_amt_accepted"))][:60]

    # day buckets (most recent 30 operation days)
    days = {}
    for a in analyzed:
        days.setdefault(a["auction_date"], {"auctions": [], "buybacks": []})["auctions"].append(a)
    for b in bb_analyzed:
        days.setdefault(b["operation_date"], {"auctions": [], "buybacks": []})["buybacks"].append(b)
    day_list = sorted(days, reverse=True)[:30]
    verdicts = {d: day_verdict(d, days[d]["auctions"], days[d]["buybacks"]) for d in day_list}
    latest_day = day_list[0] if day_list else None

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
        if prev.get("fingerprint") == fingerprint and prev.get("ai_note"):
            ai = prev["ai_note"]
        elif not event.get("no_ai"):
            ai = ai_note({"date": latest_day, "verdict": verdicts[latest_day],
                          "auctions": [{k: a.get(k) for k in ("term", "type", "total_accepted", "btc", "indirect_pct", "pd_pct", "tail_bp", "grade", "z", "trailing12")} for a in days[latest_day]["auctions"]],
                          "buybacks": [{k: b.get(k) for k in ("operation_date", "maturity_bucket", "operation_type", "max_par", "offered", "accepted", "fill_pct", "coverage", "tags")} for b in days[latest_day]["buybacks"]],
                          "program": program_stats})
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
        "methodology": {
            "tail_proxy": "high yield (bills: investment rate) minus the prior-close Treasury par yield of the matching tenor, in bp; a true when-issued tail needs dealer WI quotes",
            "z_scores": "vs the trailing 12 auctions of the same type and term", "grade": "demand score = z(bid-to-cover) + 0.8 z(indirect) - 0.8 z(dealer) - z(tail); A >= 1.0, B >= 0.4, C >= -0.4, D >= -1.0, else F",
            "buyback_signal": "strong = accepted >= 90% of maximum and maximum >= $5B (tags LIQUIDITY INJECTION / EASY-POLICY SIGNAL / RISK-ASSET BULLISH)"},
    }
    _put_json(OUT_KEY, out)
    return {"ok": True, "elapsed_s": out["elapsed_s"], "newest_auction": out["freshness"]["newest_auction"], "newest_buyback": out["freshness"]["newest_buyback"],
            "today": latest_day, "headline": (verdicts.get(latest_day) or {}).get("headline"), "notes": out["notes"], "ai": bool(ai)}
