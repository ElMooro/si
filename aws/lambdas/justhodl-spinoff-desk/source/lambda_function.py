"""
justhodl-spinoff-desk - The Spin-Off & Special-Situations Desk

The documented edge (Greenblatt, "You Can Be a Stock Market Genius"; and a
deep academic literature on post-spin-off drift): newly spun-off companies
are systematically mispriced for 12-24 months because

  1. index funds and institutions RECEIVE shares of a company that does not
     fit their mandate and dump them indiscriminately - forced, price-
     insensitive selling that overshoots fair value;
  2. the SpinCo launches with little or no sell-side analyst coverage - an
     information vacuum;
  3. management is handed fresh equity incentives and, tellingly, often buys.

The richest sub-edge is the ORPHAN: a small SpinCo carved out of a large
parent - too small to matter to the parent's holders, so it gets dumped
hardest and covered least.

Pipeline per run:
  1. Pull the most recent Form 10-12B registrations from SEC EDGAR full-text
     search (EFTS). Form 10-12B is the canonical spin-off registration form
     (registration of a class of securities under Exchange Act 12(b)).
  2. Parse ticker + CIK + filing date straight out of EFTS display_names.
  3. Resolve each on FMP /stable/quote:
       - quotes with a real price + volume  -> a TRADING spin-off
       - no quote / no ticker yet           -> a PENDING registration
  4. For trading SpinCos: fetch the registration's primary document once
     (hard-capped read) to confirm spin-off language and lift the parent
     name when it can be read cleanly - never guessed.
  5. Enrich with FMP profile + ratios + analyst-coverage count, and cross-
     reference the platform's own insider-aggregate feed for post-spin
     cluster buys.
  6. Score the opportunity - timing in the forced-selling window, orphan
     size, analyst neglect, post-spin drawdown depth, insider confirmation,
     fundamental health - and tier it.
  7. Section into fresh (0-6mo), seasoned (6-18mo) and pending, and write
     data/spinoff-desk.json.

Real data only. SEC is accessed with a compliant User-Agent and throttled.
"""
import json
import os
import re
import time
import gzip
import urllib.request
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import boto3

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/spinoff-desk.json"
INSIDER_KEY = "data/insider-aggregate.json"
SEC_UA = "JustHodl Research raafouis@gmail.com"

EFTS_URL = "https://efts.sec.gov/LATEST/search-index?forms=10-12B&q="
MAX_FILINGS = 100             # EFTS returns up to 100 most-recent hits
LOOKBACK_DAYS = 540           # keep registrations filed within ~18 months
MAX_DOC_FETCH = 48            # cap primary-document fetches per run
DOC_CAP_BYTES = 650_000       # read at most ~650KB of each Form 10

s3 = boto3.client("s3", region_name="us-east-1")

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
ENT_RE = re.compile(r"&nbsp;|&#160;|&#xa0;|&#8203;|&amp;", re.IGNORECASE)
# "ABC Company  (ABCD)  (CIK 0001234567)"
TICK_RE = re.compile(r"\(([A-Z][A-Z0-9.\-]{0,6})\)\s*\(CIK")
SPIN_WORDS = ("spin-off", "spinoff", "spun off", "spun-off",
              "the distribution", "the separation", "remainco")


# ----- http helpers ----------------------------------------------------------
def http_json(url, headers, timeout=30, retries=2):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8", "ignore"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(0.7 * (attempt + 1))
                continue
            return None
        except Exception:
            if attempt < retries:
                time.sleep(0.7 * (attempt + 1))
                continue
            return None
    return None


def fmp(path, params=""):
    return http_json(f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}",
                     {"User-Agent": "JustHodl/1.0"})


def fetch_sec(url, cap=DOC_CAP_BYTES):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": SEC_UA,
            "Accept-Encoding": "gzip, deflate"})
        with urllib.request.urlopen(req, timeout=35) as r:
            raw = r.read(cap)
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return raw.decode("utf-8", errors="ignore")
    except Exception:
        return None


# ----- EDGAR ingest ----------------------------------------------------------
def pull_registrations():
    """Return recent 10-12B registrations parsed from EFTS."""
    j = http_json(EFTS_URL, {"User-Agent": SEC_UA, "Accept": "application/json"})
    if not j:
        return []
    hits = j.get("hits", {}).get("hits", [])
    cutoff = (datetime.now(timezone.utc).timestamp() - LOOKBACK_DAYS * 86400)
    out = []
    for h in hits[:MAX_FILINGS]:
        src = h.get("_source", {}) or {}
        fdate = src.get("file_date")
        if not fdate:
            continue
        try:
            ts = datetime.strptime(fdate, "%Y-%m-%d").replace(
                tzinfo=timezone.utc).timestamp()
        except Exception:
            continue
        if ts < cutoff:
            continue
        names = src.get("display_names") or []
        name0 = names[0] if names else ""
        tm = TICK_RE.search(name0)
        ticker = tm.group(1) if tm else None
        # strip the trailing "(TICK) (CIK ...)" decoration from the name
        clean = re.sub(r"\s*\([A-Z0-9.\-]+\)\s*\(CIK.*$", "", name0).strip()
        cik = (src.get("cik") or "").lstrip("0") or None
        adsh = src.get("_id") or h.get("_id") or ""
        accession = adsh.split(":")[0]
        primary = adsh.split(":")[1] if ":" in adsh else ""
        out.append({
            "name": clean or name0,
            "ticker": ticker,
            "cik": cik,
            "filed_date": fdate,
            "filed_ts": ts,
            "accession": accession,
            "primary_doc": primary,
        })
    return out


def doc_url(cik, accession, primary):
    if not (cik and accession):
        return None
    nodash = accession.replace("-", "")
    if primary:
        return (f"https://www.sec.gov/Archives/edgar/data/"
                f"{cik}/{nodash}/{primary}")
    return (f"https://www.sec.gov/Archives/edgar/data/"
            f"{cik}/{nodash}/{accession}.txt")


PARENT_RE = re.compile(
    r"(?:distribution by|wholly[- ]owned subsidiary of|"
    r"separation from|spin-?off (?:by|from))\s+"
    r"([A-Z][A-Za-z0-9&.,'\- ]{2,55}?)"
    r"(?:[,.]|\s+(?:of|will|intends|plc|inc|corp|company|ltd|holdings))",
    re.IGNORECASE)


def inspect_filing(reg):
    """Fetch the primary doc once: confirm spin-off language, lift parent."""
    url = doc_url(reg["cik"], reg["accession"], reg["primary_doc"])
    reg["filing_url"] = url or ""
    if not url:
        return
    html = fetch_sec(url)
    if not html:
        return
    flat = WS_RE.sub(" ", TAG_RE.sub(" ", ENT_RE.sub(" ", html)))
    low = flat.lower()
    reg["is_spinoff"] = any(w in low for w in SPIN_WORDS)
    pm = PARENT_RE.search(flat[:60000])
    if pm:
        cand = pm.group(1).strip(" ,.")
        # reject obvious non-names
        if 3 <= len(cand) <= 55 and not cand.lower().startswith(("the ", "this ")):
            reg["parent"] = cand


# ----- FMP enrichment --------------------------------------------------------
def quote(sym):
    d = fmp("quote", f"&symbol={sym}")
    if isinstance(d, list) and d:
        return d[0]
    return None


def profile(sym):
    d = fmp("profile", f"&symbol={sym}")
    if isinstance(d, list) and d:
        return d[0]
    return None


def analyst_count(sym):
    d = fmp("price-target-summary", f"&symbol={sym}")
    if isinstance(d, list) and d:
        s = d[0]
        for k in ("allTime", "allTimeCount", "lastYear", "lastYearCount"):
            v = s.get(k)
            if isinstance(v, (int, float)):
                return int(v)
    return None


def ratios(sym):
    d = fmp("ratios-ttm", f"&symbol={sym}")
    if isinstance(d, list) and d:
        return d[0]
    return None


# ----- scoring ---------------------------------------------------------------
def cap_label(mc):
    if not mc:
        return "unknown"
    if mc < 300e6:
        return "micro"
    if mc < 2e9:
        return "small"
    if mc < 10e9:
        return "mid"
    if mc < 200e9:
        return "large"
    return "mega"


def score_spin(s):
    """Composite 0-100. Forced-selling + neglect + timing is the edge."""
    pts = 0.0
    sig = []
    dt = s.get("days_trading")

    # timing in the forced-selling window
    if dt is not None:
        if dt <= 90:
            pts += 28
            sig.append("0-3mo - peak forced selling")
        elif dt <= 180:
            pts += 22
            sig.append("3-6mo window")
        elif dt <= 365:
            pts += 14
            sig.append("6-12mo - drift continues")
        else:
            pts += 7
            sig.append("12-18mo - late window")
    else:
        pts += 10

    # orphan size - small SpinCos get dumped hardest, covered least
    cl = s.get("market_cap_label")
    if cl == "micro":
        pts += 22
        sig.append("micro-cap orphan")
    elif cl == "small":
        pts += 17
        sig.append("small-cap orphan")
    elif cl == "mid":
        pts += 9
    elif cl == "large":
        pts += 3

    # analyst neglect
    ac = s.get("analyst_coverage")
    if ac is not None:
        if ac <= 1:
            pts += 16
            sig.append("uncovered by analysts")
        elif ac <= 4:
            pts += 10
            sig.append("thinly covered")
        elif ac <= 8:
            pts += 5
    else:
        pts += 6

    # post-spin drawdown - selling already absorbed = the entry
    dh = s.get("from_52w_high_pct")
    if dh is not None:
        if dh <= -30:
            pts += 16
            sig.append("down 30%+ from post-spin high")
        elif dh <= -15:
            pts += 11
            sig.append("washed out from highs")
        elif dh <= -5:
            pts += 6
        else:
            pts += 2

    # insider confirmation - the highest-conviction post-spin tell
    if s.get("insider_cluster_buy"):
        pts += 10
        sig.append("insider cluster buying")

    # fundamental health
    f = s.get("fundamentals", {}) or {}
    if f.get("fcf_positive") is True:
        pts += 4
        sig.append("positive free cash flow")
    if isinstance(f.get("net_margin"), (int, float)) and f["net_margin"] > 5:
        pts += 2
    de = f.get("debt_to_equity")
    if isinstance(de, (int, float)) and 0 <= de < 1.5:
        pts += 2

    s["spinoff_score"] = round(min(pts, 100.0), 1)
    s["signals"] = sig
    sc = s["spinoff_score"]
    s["tier"] = ("PRIME SPIN" if sc >= 70 else "STRONG" if sc >= 52
                 else "WATCH" if sc >= 35 else "MONITOR")
    return s


def build_thesis(s):
    cl = s.get("market_cap_label", "")
    parent = s.get("parent")
    bits = []
    if parent:
        bits.append(f"Spun out of {parent}")
    if cl in ("micro", "small"):
        bits.append(f"a {cl}-cap orphan likely hit by indiscriminate "
                     "index/institutional selling")
    ac = s.get("analyst_coverage")
    if ac is not None and ac <= 4:
        bits.append("with little to no sell-side coverage to anchor price")
    dh = s.get("from_52w_high_pct")
    if dh is not None and dh <= -15:
        bits.append(f"already down {abs(round(dh))}% from its post-spin high")
    if s.get("insider_cluster_buy"):
        bits.append("and insiders are buying the dislocation")
    if not bits:
        bits.append("A recent spin-off inside the post-separation drift window")
    return ". ".join(b[0].upper() + b[1:] for b in [bits[0]] ) + (
        ((", " + ", ".join(bits[1:])) if len(bits) > 1 else "")) + "."


def build_risk(s):
    f = s.get("fundamentals", {}) or {}
    r = []
    if f.get("fcf_positive") is False:
        r.append("SpinCo is not yet free-cash-flow positive")
    de = f.get("debt_to_equity")
    if isinstance(de, (int, float)) and de >= 2:
        r.append("parent loaded the SpinCo with debt at separation")
    if s.get("market_cap_label") == "micro":
        r.append("micro-cap liquidity - size positions accordingly")
    if not r:
        r.append("forced-selling overhang can persist for months before it "
                 "clears - this is a patient, multi-month setup")
    return "; ".join(r) + "."


# ----- main ------------------------------------------------------------------
def lambda_handler(event, context):
    started = time.time()
    now = datetime.now(timezone.utc)

    regs = pull_registrations()
    if not regs:
        return _fail("EDGAR EFTS returned no 10-12B registrations")

    # insider cross-reference (free S3 read)
    insider_syms = set()
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=INSIDER_KEY)
        idoc = json.loads(obj["Body"].read())
        for row in idoc.get("notable_cluster_buys", []) or []:
            sym = row.get("symbol") or row.get("ticker")
            if sym:
                insider_syms.add(sym.upper())
    except Exception:
        pass

    # split into ticker-bearing (candidate trading) vs pending
    with_ticker = [r for r in regs if r.get("ticker")]
    pending = []
    trading = []

    # quote the ticker-bearing ones
    def _q(r):
        q = quote(r["ticker"])
        return r, q

    with ThreadPoolExecutor(max_workers=8) as ex:
        for r, q in ex.map(_q, with_ticker):
            px = (q or {}).get("price")
            vol = (q or {}).get("volume") or 0
            if q and px and px >= 1 and vol > 0:
                r["_quote"] = q
                trading.append(r)
            else:
                pending.append(r)
    pending.extend(r for r in regs if not r.get("ticker"))

    # inspect filings for the trading set (confirm spin-off + parent)
    for r in trading[:MAX_DOC_FETCH]:
        inspect_filing(r)
        time.sleep(0.18)

    # enrich + score the trading set
    def _enrich(r):
        sym = r["ticker"]
        q = r["_quote"]
        prof = profile(sym) or {}
        rat = ratios(sym) or {}
        ac = analyst_count(sym)

        px = q.get("price")
        yhi = q.get("yearHigh")
        ylo = q.get("yearLow")
        mc = q.get("marketCap") or prof.get("marketCap")

        ipo = prof.get("ipoDate")
        days_trading = None
        ipo_recent = False
        if ipo:
            try:
                idt = datetime.strptime(ipo[:10], "%Y-%m-%d").replace(
                    tzinfo=timezone.utc)
                days_trading = int((now - idt).total_seconds() / 86400)
                ipo_recent = days_trading >= 0
            except Exception:
                pass
        # if FMP has no recent ipoDate, fall back to the 10-12B filing date
        if not ipo_recent or days_trading is None or days_trading > 900:
            days_trading = int((now.timestamp() - r["filed_ts"]) / 86400)
            r["days_trading_approx"] = True

        fcf = rat.get("freeCashFlowPerShareTTM")
        nm = rat.get("netProfitMarginTTM")
        de = rat.get("debtToEquityRatioTTM")
        if de is None:
            de = rat.get("debtToEquityTTM")
        if isinstance(nm, (int, float)) and abs(nm) <= 1.5:
            nm = nm * 100.0  # FMP sometimes returns a fraction

        s = {
            "symbol": sym,
            "name": prof.get("companyName") or r["name"],
            "cik": r["cik"],
            "sector": prof.get("sector") or "",
            "industry": prof.get("industry") or "",
            "price": round(px, 2) if isinstance(px, (int, float)) else None,
            "market_cap": mc,
            "market_cap_label": cap_label(mc),
            "filed_date": r["filed_date"],
            "ipo_date": ipo if ipo_recent else None,
            "days_trading": days_trading,
            "days_trading_approx": r.get("days_trading_approx", False),
            "parent": r.get("parent"),
            "is_spinoff": r.get("is_spinoff"),
            "analyst_coverage": ac,
            "neglect": (ac is not None and ac <= 4),
            "orphan": cap_label(mc) in ("micro", "small"),
            "insider_cluster_buy": sym.upper() in insider_syms,
            "filing_url": r.get("filing_url", ""),
            "from_52w_high_pct": (
                round((px - yhi) / yhi * 100, 1)
                if isinstance(px, (int, float)) and yhi else None),
            "from_52w_low_pct": (
                round((px - ylo) / ylo * 100, 1)
                if isinstance(px, (int, float)) and ylo else None),
            "fundamentals": {
                "fcf_positive": (fcf > 0) if isinstance(fcf, (int, float))
                else None,
                "net_margin": round(nm, 1) if isinstance(nm, (int, float))
                else None,
                "debt_to_equity": round(de, 2) if isinstance(de, (int, float))
                else None,
            },
        }
        s["window"] = ("fresh 0-6mo" if (days_trading is not None
                                         and days_trading <= 180)
                       else "seasoned 6-18mo")
        score_spin(s)
        s["thesis"] = build_thesis(s)
        s["risk"] = build_risk(s)
        return s

    scored = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for s in ex.map(_enrich, trading):
            if s:
                scored.append(s)

    # drop registrations that the filing clearly shows are NOT spin-offs
    # (direct listings etc.) - keep where the flag is True or unknown
    spins = [s for s in scored if s.get("is_spinoff") is not False]
    spins.sort(key=lambda x: x["spinoff_score"], reverse=True)

    fresh = [s for s in spins if s["window"].startswith("fresh")]
    seasoned = [s for s in spins if s["window"].startswith("seasoned")]
    top = spins[:20]

    pend_out = []
    for r in pending[:40]:
        pend_out.append({
            "name": r["name"],
            "symbol": r.get("ticker"),
            "cik": r["cik"],
            "filed_date": r["filed_date"],
            "filing_url": doc_url(r["cik"], r["accession"],
                                  r.get("primary_doc", "")) or "",
            "note": "Form 10-12B registered with the SEC - not yet trading. "
                    "Watch for the when-issued listing.",
        })
    pend_out.sort(key=lambda x: x["filed_date"], reverse=True)

    scores = [s["spinoff_score"] for s in spins]
    summary = {
        "n_filings_scanned": len(regs),
        "n_trading": len(spins),
        "n_fresh": len(fresh),
        "n_seasoned": len(seasoned),
        "n_pending": len(pend_out),
        "best_score": max(scores) if scores else None,
        "median_score": (round(sorted(scores)[len(scores) // 2], 1)
                         if scores else None),
        "n_prime": sum(1 for s in spins if s["tier"] == "PRIME SPIN"),
    }
    if spins:
        headline = (f"{len(spins)} tradeable spin-offs in the post-separation "
                    f"drift window - {len(fresh)} fresh (0-6mo), "
                    f"{summary['n_prime']} rated PRIME. "
                    f"{len(pend_out)} more registered and pending.")
    else:
        headline = ("No tradeable spin-offs resolved from the current 10-12B "
                    f"feed; {len(pend_out)} registrations pending.")

    payload = {
        "generated_at": now.isoformat(),
        "elapsed_seconds": round(time.time() - started, 1),
        "headline": headline,
        "summary": summary,
        "top_setups": top,
        "fresh_spinoffs": fresh,
        "seasoned_spinoffs": seasoned,
        "pending_registrations": pend_out,
        "method": ("Spin-offs sourced from SEC EDGAR Form 10-12B "
                   "registrations; tickers parsed from the filing index, "
                   "priced and screened on FMP, parent lifted from the "
                   "registration document. Scored on the documented spin-off "
                   "edge - timing inside the forced-selling window, orphan "
                   "size, analyst neglect, post-spin drawdown, insider "
                   "confirmation and fundamental health. No fabricated data; "
                   "filings whose ticker cannot be confirmed are listed as "
                   "pending, never guessed."),
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(payload, default=str),
                      ContentType="application/json",
                      CacheControl="public, max-age=900")
    except Exception as e:
        return _fail(f"S3 write failed: {e}")

    return {"statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": True, "n_trading": len(spins),
                                "n_fresh": len(fresh),
                                "n_prime": summary["n_prime"],
                                "n_pending": len(pend_out)})}


def _fail(msg):
    print(f"[spinoff-desk] ERROR: {msg}")
    return {"statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"ok": False, "error": msg})}


if __name__ == "__main__":
    print(lambda_handler({}, None))
