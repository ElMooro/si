"""
justhodl-merger-arb - Merger-Arbitrage Spread Desk

A real risk-arb desk: takes announced M&A deals (from justhodl-ma-tracker's
screener/ma-latest.json feed), parses the actual deal consideration out of
the SEC S-4 each deal links to, prices the gross spread vs the target's live
market price, annualizes it over an estimated close horizon, and scores deal
completion risk.

Pipeline per run:
  1. Load screener/ma-latest.json (ma-tracker keeps it fresh hourly).
  2. Keep deals with a target ticker + S-4 link announced within ~300 days.
  3. Quote target + acquirer on FMP /stable/quote. Drop delisted targets
     (no volume) and sub-$1 shells. Resolve the acquirer's COMMON ticker
     (the feed often hands back a preferred series like PSA-PL).
  4. For the most recent ~45 survivors, fetch the S-4 and extract the
     merger consideration with an anchored scan:
        "converted ... into the right to receive (i) $X in cash and
         (ii) 0.NNNN shares of <acquirer> common stock"
     Handles all-cash, all-stock and cash+stock (mixed) deals.
  5. deal_value = cash + exchange_ratio * acquirer_price
     gross_spread = (deal_value - target_price) / target_price
     annualized   = gross_spread * 365 / est_close_days
  6. Estimate downside-to-unaffected (target's pre-announcement price) and
     a deal-risk score (acquirer can absorb target, cash vs stock, deal age).
  7. Tier: TIGHT CARRY / WIDE SPREAD / BUMP WATCH / UNVERIFIED.
  8. Write data/merger-arb.json.

No fake data. SEC is accessed with a compliant User-Agent and throttled.
"""
import json
import os
import re
import time
import gzip
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

import boto3

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/merger-arb.json"
FEED_KEY = "screener/ma-latest.json"
SEC_UA = "JustHodl Research raafouis@gmail.com"

MAX_DEAL_AGE_DAYS = 300        # ignore stale deals
MAX_S4_DEALS = 45              # cap S-4 fetches per run
S4_CAP_BYTES = 4_200_000       # read at most ~4MB of each S-4
SANE_SPREAD_LO = -0.30         # accept spreads in this band only
SANE_SPREAD_HI = 0.55

s3 = boto3.client("s3", region_name="us-east-1")

# ----- regex toolbox ---------------------------------------------------------
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
ENT_RE = re.compile(r"&nbsp;|&#160;|&#xa0;|&#8203;|&#8201;|&#8202;", re.IGNORECASE)
# the canonical consideration anchor
ANCHOR_RE = re.compile(
    r"converted\s+(?:automatically\s+)?into\s+"
    r"(?:and\s+thereafter\s+represent\s+)?the\s+right\s+to\s+receive",
    re.IGNORECASE)
CASH_RE = re.compile(
    r"\$\s?([\d,]+(?:\.\d{1,2})?)\s+(?:in\s+cash|per\s+share)", re.IGNORECASE)
RATIO_RE = re.compile(
    r"(\d{1,2}\.\d{3,6})\s+(?:newly\s+issued\s+)?"
    r"(?:validly\s+issued[^.]{0,70}?)?shares\s+of", re.IGNORECASE)


# ----- http helpers ----------------------------------------------------------
def fmp(path, params="", retries=2):
    url = f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}"
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(0.6 * (attempt + 1))
                continue
            return None
        except Exception:
            if attempt < retries:
                time.sleep(0.6 * (attempt + 1))
                continue
            return None
    return None


def fetch_sec(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": SEC_UA,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"})
        with urllib.request.urlopen(req, timeout=35) as r:
            raw = r.read(S4_CAP_BYTES)
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return raw.decode("utf-8", errors="ignore")
    except Exception:
        return None


# ----- quotes ----------------------------------------------------------------
def quote(sym):
    """Return {price, volume, mcap, name, avg200, yearLow} or None."""
    d = fmp("quote", f"&symbol={sym}")
    if isinstance(d, list) and d:
        q = d[0]
        return {
            "sym": sym,
            "price": q.get("price"),
            "volume": q.get("volume"),
            "mcap": q.get("marketCap"),
            "name": q.get("name"),
            "avg200": q.get("priceAvg200"),
            "yearLow": q.get("yearLow"),
        }
    return None


def resolve_acquirer(sym):
    """The M&A feed often returns a preferred series (PSA-PL). Resolve to the
    common-stock ticker by comparing the raw symbol with its de-suffixed base
    and keeping whichever trades with real volume / higher price."""
    cand = [sym]
    if "-" in sym:
        base = sym.split("-")[0]
        if base and base != sym:
            cand.append(base)
    best = None
    for c in cand:
        q = quote(c)
        if not q or q.get("price") is None:
            continue
        if best is None:
            best = q
            continue
        # prefer the one with materially more volume (common >> preferred)
        bv = best.get("volume") or 0
        cv = q.get("volume") or 0
        if cv > bv * 1.5:
            best = q
    return best


# ----- S-4 consideration extraction -----------------------------------------
def detag(html):
    t = ENT_RE.sub(" ", html)
    t = TAG_RE.sub(" ", t)
    return WS_RE.sub(" ", t)


def is_spac(flat):
    """SPAC business combinations are a different instrument - exclude."""
    head = flat[:80000].lower()
    return ("trust account" in head and "business combination" in head
            and "merger consideration" not in head[:40000])


def extract_consideration(flat):
    """Scan anchored windows for the first one carrying numeric deal terms.
    Returns {cash, ratio, deal_type, snippet} or None."""
    for m in ANCHOR_RE.finditer(flat):
        win = flat[m.end():m.end() + 480]
        cash = None
        cm = CASH_RE.search(win)
        if cm:
            try:
                v = float(cm.group(1).replace(",", ""))
                if 0.10 <= v <= 5000:
                    cash = v
            except ValueError:
                pass
        ratio = None
        rm = RATIO_RE.search(win)
        if rm:
            try:
                v = float(rm.group(1))
                if 0.001 <= v <= 50:
                    ratio = v
            except ValueError:
                pass
        if cash is None and ratio is None:
            continue  # procedural "right to receive" - keep scanning
        if cash is not None and ratio is not None:
            dtype = "MIXED"
        elif cash is not None:
            dtype = "ALL-CASH"
        else:
            dtype = "ALL-STOCK"
        return {"cash": cash, "ratio": ratio, "deal_type": dtype,
                "snippet": win.strip()[:240]}
    return None


# ----- downside-to-unaffected ------------------------------------------------
def unaffected_price(sym, announce_date):
    """Target close ~1-2 weeks before announcement = unaffected price."""
    try:
        d0 = datetime.strptime(announce_date[:10], "%Y-%m-%d")
    except Exception:
        return None
    frm = (d0 - timedelta(days=24)).strftime("%Y-%m-%d")
    to = (d0 - timedelta(days=3)).strftime("%Y-%m-%d")
    data = fmp("historical-price-eod/light",
               f"&symbol={sym}&from={frm}&to={to}")
    rows = data if isinstance(data, list) else (data or {}).get("historical")
    if not isinstance(rows, list) or not rows:
        return None
    closes = [r.get("price") if r.get("price") is not None else r.get("close")
              for r in rows]
    closes = [c for c in closes if isinstance(c, (int, float)) and c > 0]
    if not closes:
        return None
    # the feed is newest-first; pre-announce window -> take the median
    closes.sort()
    return round(closes[len(closes) // 2], 2)


# ----- scoring ---------------------------------------------------------------
def deal_risk(deal_type, acq_mcap, tgt_mcap, days_out):
    """0 (clean) .. 100 (fragile). Lower is safer carry."""
    risk = 18.0
    # acquirer absorption capacity
    if acq_mcap and tgt_mcap and tgt_mcap > 0:
        ratio = acq_mcap / tgt_mcap
        if ratio < 1.3:
            risk += 34          # merger-of-equals / strained
        elif ratio < 3:
            risk += 18
        elif ratio < 8:
            risk += 6
    else:
        risk += 12              # unknown size = unknown
    # cash deals carry no stub market risk
    if deal_type == "ALL-CASH":
        risk -= 6
    elif deal_type == "ALL-STOCK":
        risk += 8
    # a deal dragging on past ~7 months is a regulatory tell
    if days_out > 240:
        risk += 26
    elif days_out > 150:
        risk += 12
    return max(0, min(100, round(risk, 1)))


def classify(spread, risk, deal_type):
    if spread < -0.012:
        return "BUMP WATCH"
    if spread <= 0.04 and risk <= 55:
        return "TIGHT CARRY"
    return "WIDE SPREAD"


# ----- main ------------------------------------------------------------------
def lambda_handler(event, context):
    started = time.time()
    now = datetime.now(timezone.utc)

    # 1. load ma-tracker feed
    try:
        feed = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=FEED_KEY)["Body"].read())
    except Exception as e:
        return _fail(f"could not load {FEED_KEY}: {e}")

    raw_deals = feed.get("deals", [])
    cutoff = (now - timedelta(days=MAX_DEAL_AGE_DAYS)).strftime("%Y-%m-%d")

    # 2. dedupe + recency filter
    seen = set()
    cand = []
    for d in raw_deals:
        tsym = d.get("targetedSymbol")
        link = d.get("link")
        tdate = (d.get("transactionDate") or "")[:10]
        if not tsym or not link or not tdate or tdate < cutoff:
            continue
        key = (tsym, d.get("symbol"), tdate)
        if key in seen:
            continue
        seen.add(key)
        cand.append(d)
    cand.sort(key=lambda d: d.get("acceptedDate", ""), reverse=True)

    # 3. quote targets, drop delisted / shell
    def tq(d):
        return d, quote(d.get("targetedSymbol"))
    pending = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for d, q in ex.map(tq, cand[:120]):
            if not q or q.get("price") is None:
                continue
            if (q.get("volume") or 0) <= 0:        # delisted -> closed/dead
                continue
            if q["price"] < 1.0:                    # penny shell / SPAC unit
                continue
            d["_tq"] = q
            pending.append(d)

    # 4. work the most recent N with S-4 parsing
    work = pending[:MAX_S4_DEALS]
    deals_out = []
    n_spac = 0
    n_parsed = 0
    for d in work:
        tq_ = d["_tq"]
        tsym = d["targetedSymbol"]
        tprice = tq_["price"]
        asym_raw = d.get("symbol") or ""
        tdate = (d.get("transactionDate") or "")[:10]
        try:
            days_out = (now - datetime.strptime(
                tdate, "%Y-%m-%d").replace(tzinfo=timezone.utc)).days
        except Exception:
            days_out = 90

        html = fetch_sec(d.get("link", ""))
        time.sleep(0.35)            # SEC politeness
        rec = {
            "target": tsym,
            "target_name": d.get("targetedCompanyName"),
            "acquirer_raw": asym_raw,
            "acquirer_name": d.get("companyName"),
            "announced": tdate,
            "days_outstanding": days_out,
            "target_price": round(tprice, 2),
            "target_mcap": tq_.get("mcap"),
            "s4_link": d.get("link"),
        }
        if not html:
            rec.update(tier="UNVERIFIED", note="S-4 fetch failed")
            deals_out.append(rec)
            continue
        flat = detag(html)
        if is_spac(flat):
            n_spac += 1
            continue
        terms = extract_consideration(flat)
        if not terms:
            rec.update(tier="UNVERIFIED", note="deal terms not located in S-4")
            deals_out.append(rec)
            continue

        # resolve acquirer common-stock price for stock/mixed deals
        acq = None
        if terms["ratio"] is not None:
            acq = resolve_acquirer(asym_raw)
        cash = terms["cash"] or 0.0
        ratio = terms["ratio"]
        if ratio is not None and acq is None:
            rec.update(tier="UNVERIFIED",
                       note="stock deal but acquirer price unresolved",
                       deal_type=terms["deal_type"])
            deals_out.append(rec)
            continue

        deal_value = cash + (ratio * acq["price"] if ratio else 0.0)
        gross = (deal_value - tprice) / tprice
        rec["deal_type"] = terms["deal_type"]
        rec["cash_per_share"] = round(cash, 4) if cash else None
        rec["exchange_ratio"] = round(ratio, 5) if ratio else None
        rec["acquirer"] = acq["sym"] if acq else None
        rec["acquirer_price"] = round(acq["price"], 2) if acq else None
        rec["deal_value"] = round(deal_value, 2)
        rec["terms_snippet"] = terms["snippet"]

        # sanity gate - reject mis-parses
        if not (SANE_SPREAD_LO <= gross <= SANE_SPREAD_HI):
            rec.update(tier="UNVERIFIED",
                       note=f"parsed spread {gross*100:.0f}% outside sane band")
            deals_out.append(rec)
            continue

        n_parsed += 1
        est_close = max(45, 165 - days_out)
        annualized = gross * 365.0 / est_close
        # downside if the deal breaks
        unaff = unaffected_price(tsym, tdate)
        downside = None
        if unaff and tprice > 0:
            downside = round((unaff - tprice) / tprice * 100, 1)
        risk = deal_risk(terms["deal_type"], (acq or {}).get("mcap"),
                         tq_.get("mcap"), days_out)
        tier = classify(gross, risk, terms["deal_type"])

        # capture / risk ratio - reward per unit of break risk
        rr = None
        if downside is not None and downside < 0:
            rr = round((gross * 100) / abs(downside), 2)

        rec.update(
            gross_spread_pct=round(gross * 100, 2),
            est_close_days=est_close,
            annualized_return_pct=round(annualized * 100, 1),
            downside_to_unaffected_pct=downside,
            unaffected_price=unaff,
            reward_risk=rr,
            deal_risk=risk,
            tier=tier,
        )
        if terms["deal_type"] == "ALL-CASH":
            rec["trade"] = f"Long {tsym}; collect ${deal_value:.2f} cash at close"
        elif terms["deal_type"] == "ALL-STOCK":
            rec["trade"] = (f"Long {tsym} / short {acq['sym']} at "
                            f"{ratio:.4f}x to lock the spread")
        else:
            rec["trade"] = (f"Long {tsym}; hedge {ratio:.4f}x {acq['sym']} for "
                            f"the stock leg, ${cash:.2f}/sh cash at close")
        deals_out.append(rec)

    # 5. rank + tier
    def sort_key(r):
        return -(r.get("annualized_return_pct") or -999)
    priced = [r for r in deals_out if r.get("tier") != "UNVERIFIED"]
    unverified = [r for r in deals_out if r.get("tier") == "UNVERIFIED"]
    priced.sort(key=sort_key)

    tight = [r for r in priced if r["tier"] == "TIGHT CARRY"]
    wide = [r for r in priced if r["tier"] == "WIDE SPREAD"]
    bump = [r for r in priced if r["tier"] == "BUMP WATCH"]

    spreads = [r["gross_spread_pct"] for r in priced]
    summary = {
        "n_feed_deals": len(raw_deals),
        "n_recent_pending": len(pending),
        "n_priced": len(priced),
        "n_unverified": len(unverified),
        "n_spac_skipped": n_spac,
        "median_spread_pct": round(sorted(spreads)[len(spreads) // 2], 2)
        if spreads else None,
        "best_annualized_pct": priced[0]["annualized_return_pct"]
        if priced else None,
    }
    if priced:
        headline = (f"{len(priced)} live merger-arb spreads priced - "
                    f"{len(tight)} tight carry, {len(wide)} wide. "
                    f"Best annualized {summary['best_annualized_pct']}%.")
    else:
        headline = "No verifiable merger-arb spreads in the current deal feed."

    payload = {
        "generated_at": now.isoformat(),
        "elapsed_seconds": round(time.time() - started, 1),
        "headline": headline,
        "summary": summary,
        "tight_carry": tight,
        "wide_spread": wide,
        "bump_watch": bump,
        "all_priced": priced,
        "unverified": unverified,
        "method": ("Deal terms parsed from the SEC S-4 each deal links to; "
                   "spread vs live FMP target price; annualized over an "
                   "estimated close horizon. Every spread passes a sanity "
                   "gate - unparseable deals are quarantined, not guessed."),
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(payload, default=str),
                      ContentType="application/json",
                      CacheControl="public, max-age=600")
    except Exception as e:
        return _fail(f"S3 write failed: {e}")

    return {"statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "n_priced": len(priced),
                                "n_unverified": len(unverified),
                                "headline": headline})}


def _fail(msg):
    print(f"[merger-arb] ERROR: {msg}")
    return {"statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": False, "error": msg})}


if __name__ == "__main__":
    print(lambda_handler({}, None))
