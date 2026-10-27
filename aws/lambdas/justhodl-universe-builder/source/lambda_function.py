"""
justhodl-universe-builder v4 — EXHAUSTIVE all-US-listed universe.

v4.1.0 (2026-10-27): a provider outage can no longer publish an empty universe.
  On 2026-10-16 and 2026-10-26 every /company-screener range answered HTTP 429
  (the FMP bandwidth block); each failed range returned [] and the run wrote a
  0-stock data/universe.json over the good one, so every engine that selects
  from it (earnings-pead, revenue-acceleration, momentum-breakout,
  volatility-squeeze-hunter, microcap-float-squeeze, options-flow-scanner ...)
  raised "No selected universe" for a day.  Now:
  • a failed range (an exception or a non-list answer) is RECORDED, not
    swallowed; its market-cap span is filled from the FMP bank's profile-bulk
    (FMP's own whole-market profile file, banked verbatim each night by
    justhodl-fmp-bank), filtered like the screener request (NYSE/NASDAQ/AMEX,
    actively trading, not ETF/fund, >= $1M) and limited to the previous
    publication's members (the bank refreshes membership, it does not invent
    it); those rows say source "bank_profile_bulk" and carry the bank's as_of,
    and the document's `sources` block says which ranges came from where.  The
    bank is used only while it is <= UNIVERSE_BANK_MAX_AGE_DAYS old (default 10).
  • the listing rule (ListingRule, both paths): the screener returned an
    issuer's notes, preferreds, warrants, rights and units with the ISSUER's
    market cap - 172 of 5,230 live rows on 2026-10-27, AT&T's 5.35% notes as a
    $140B large cap - and v4's suffix filter could not see them.  Polygon's
    reference type (joined by the factor book) now decides, with a second
    signal required for the types Polygon occasionally mis-assigns.
  • a publication guard: fewer than 1,000 stocks, < 85% of the previous
    publication's total, or a cap bucket < 50% of its previous count -> nothing
    is written, the previous publication stays, and the run fails loudly.
  • event {"dry_run": true[, "force_bank": true]} builds without writing and
    returns the comparison with the live publication (the verification path).
  Rows keep the v4 shape; the document gains engine_version, sources
  (screener / bank_profile_bulk / listing_rule) and stats.by_source.

v4 (2026-07-20): institutional coverage guarantee —
  • adaptive mcap-range BISECTION: FMP screener caps every response at 1000
    rows; when a range saturates we split it at the geometric midpoint and
    recurse until every sub-range returns <1000 → ZERO truncation.
  • ADRs INCLUDED: country filter removed — "US-listed" = NYSE/NASDAQ/AMEX,
    which includes TSM, BABA, ASML-class ADRs institutions trade all day.
  • floor lowered $5M → $1M so living nano-caps are not dropped.
  • per-stock exchange/country/is_adr + stats.by_exchange/n_adr (shape is
    otherwise unchanged — 13+ downstream consumers unaffected, additive only).

Pulls stocks across ALL 6 cap buckets via FMP /company-screener:
  • nano    $5M-$50M       (typically 300-500 real stocks)
  • micro   $50M-$300M     (~480 real stocks)
  • small   $300M-$2B      (~470 real stocks)
  • mid     $2B-$10B       (~550 real stocks)
  • large   $10B-$200B     (~510 real stocks)
  • mega    >$200B         (~65 real stocks)

Filters out:
  • Asset management / mutual funds / ETFs / closed-end funds
  • Shell companies / blank checks / SPACs
  • Preferred shares / warrants / units (symbols with -, /, .)
  • Foreign-domiciled (only US country)
  • Non-listed exchanges (must be NYSE / NASDAQ / AMEX)

Tags each stock with cap_bucket for downstream cap-aware filtering.

Also seeds a curated AI-supply-chain microcap list manually (so even if
the screener misses some, we keep coverage of names like AAOI, AXTI etc).

OUTPUT: data/universe.json with shape:
  {
    "generated_at": "...",
    "stats": {
      "total_stocks": 2400+,
      "by_cap_bucket": {"nano": ..., "micro": ..., ...}
    },
    "stocks": [
      {"symbol": "X", "name": "...", "sector": "...", "industry": "...",
       "exchange": "...", "market_cap": ..., "price": ..., 
       "cap_bucket": "small", "source": "screener"},
      ...
    ]
  }
"""

try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass
import csv, gzip, io, json, os, time, urllib.request, urllib.error, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

ENGINE_VERSION = "4.1.0"
REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/universe.json")
FMP_KEY = managed_secret(('FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))

S3 = boto3.client("s3", region_name=REGION)

# v4.1 bank fill + publication guard
BANK_PREFIX = "data/warm/fmp-bank/profile-bulk/"          # part-N/latest.csv.gz, written by justhodl-fmp-bank
BOOK_UNIVERSE_KEY = "data/warm/factor-book/universe.json.gz"  # symbol -> identity flags (Polygon-typed), justhodl-bank-factors
BANK_MAX_AGE_DAYS = float(os.environ.get("UNIVERSE_BANK_MAX_AGE_DAYS") or "10")
SCREEN_EXCHANGES = ("NYSE", "NASDAQ", "AMEX")             # the screener request's exchange filter
MIN_PUBLISH = 1000                                        # a US-listed universe below this is never true
GUARD_TOTAL_RATIO = 0.85                                  # vs the previous publication's total
GUARD_BUCKET_RATIO = 0.5                                  # vs the previous publication's bucket count (buckets >= 50)

# Cap buckets (low_inclusive, high_exclusive)
CAP_BUCKETS = [
    ("nano",    1_000_000,         50_000_000),
    ("micro",   50_000_000,        300_000_000),
    ("small",   300_000_000,       2_000_000_000),
    ("mid",     2_000_000_000,     10_000_000_000),
    ("large",   10_000_000_000,    200_000_000_000),
    ("mega",    200_000_000_000,   100_000_000_000_000),  # 100T as upper
]

# Industries to EXCLUDE (these are funds, asset managers, shells, ETFs)
EXCLUDE_INDUSTRIES = {
    "asset management", "asset management - bonds",
    "asset management - global", "asset management - leveraged",
    "asset management - income", "asset management - growth",
    "asset management - hybrid",
    "shell companies",
    "closed-end fund - equity", "closed-end fund - bond",
    "closed-end fund - debt", "closed-end fund - foreign",
    "closed-end fund - global", "closed-end fund - hybrid",
    "exchange traded fund",
}

# Symbols ending in these are typically preferreds/warrants/units (skip)
EXCLUDE_SYMBOL_SUFFIX = (
    "-PA", "-PB", "-PC", "-PD", "-PE", "-PF", "-PG", "-PH", "-PI", "-PJ", "-PK",
    "-PR", "-PRA", "-PRB", "-PRC", "-PRD", "-PRE", "-PRF",
    "-WS", "-WT", "-W", "-U", "-UN",
)

# Curated AI supply-chain microcap seed (so we never miss these)
CURATED_SEED = [
    # Optical / interconnect / AI substrate
    "AAOI", "LITE", "COHR", "VIAV", "FN", "INFN", "OCC", "POET",
    # AI memory / storage
    "MU", "SNDK", "WDC", "STX",
    # Semi test / inspection / tools (microcap to small)
    "AEHR", "ONTO", "FORM", "ACLS", "ICHR", "UCTT", "ENTG", "CAMT", "AMBA",
    # Compound / specialty semis
    "AXTI", "WOLF", "QRVO", "SWKS", "MTSI", "INDI", "ALGM",
    # AI silicon / chips
    "AVGO", "NVDA", "AMD", "QCOM", "MRVL", "ARM", "INTC", "CRDO",
    # AI infrastructure / DC / connectivity
    "ANET", "VRT", "PSTG", "NTAP", "DELL", "SMCI", "HPE",
    # Neoclouds / AI compute / data-center buildout (newly public, high-interest)
    "NBIS", "CRWV", "APLD", "IREN", "CIFR", "WULF", "GLXY", "CORZ", "SMR", "OKLO",
    "NNE", "VST", "CEG", "TLN", "GEV", "ETN", "PWR", "NVT", "POWL",
    # Cybersecurity (often AI-driven now)
    "CRWD", "ZS", "PANW", "FTNT", "OKTA", "NET",
    # Hydrogen / energy infra
    "FCEL", "PLUG", "BE", "BLDP",
    # Biotech inflection candidates
    "AGIO", "BCRX", "BPMC", "AGEN", "CDNA", "EXAS", "INCY",
    # Renewables / EV picks-and-shovels
    "ALB", "ENPH", "FSLR", "SEDG", "RUN",
    # AI-adjacent industrials / robotics
    "ISRG", "TER", "FIX",
    # Crypto-equity
    "COIN", "MSTR", "HOOD", "MARA", "RIOT", "CLSK",
    # Defense / govtech
    "PLTR", "RKLB", "AVAV", "KTOS",
]


def fetch_url(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Universe/3.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _screener_call(low, high):
    url = ("https://financialmodelingprep.com/stable/company-screener?"
           "marketCapMoreThan=" + str(low) +
           "&marketCapLowerThan=" + str(high) +
           "&isActivelyTrading=true"
           "&exchange=NYSE,NASDAQ,AMEX"      # v4: no country filter — ADRs are US-listed
           "&limit=1000"
           "&apikey=" + FMP_KEY)
    data = fetch_url(url, timeout=30)
    if not isinstance(data, list):
        # an error object ({"Error Message": ...}) is a failed range, never an empty one (v4.1)
        raise ValueError("screener answered a %s, not a list%s" % (type(data).__name__, (": " + ",".join(sorted(map(str, data))[:4])) if isinstance(data, dict) else ""))
    return data


def fetch_screener_bucket(bucket_name, low, high, depth=0, failed=None):
    """v4 EXHAUSTIVE: FMP caps responses at 1000 rows. When a range saturates,
    bisect at the geometric midpoint (mcap is log-distributed) and recurse until
    every sub-range returns <1000 — guarantees zero truncation of the listed
    universe. Depth-capped.  v4.1: a failed (sub-)range is appended to `failed`
    as {bucket, low, high, error} so the run can fill exactly that span from the
    bank instead of publishing a hole."""
    try:
        rows = _screener_call(low, high)
    except Exception as e:
        print("[universe] range " + bucket_name + " " + str(low) + "-" + str(high)
              + " failed: " + str(e))
        if failed is not None:
            failed.append({"bucket": bucket_name, "low": low, "high": high, "error": str(e)[:160]})
        return []
    if len(rows) >= 1000 and depth < 10 and (high - low) > 2_000_000:
        mid = int((low * high) ** 0.5)
        if mid <= low or mid >= high:
            mid = (low + high) // 2
        print("[universe] " + bucket_name + " saturated at 1000 (" + str(low) + "-"
              + str(high) + ") → bisect @ " + str(mid))
        return (fetch_screener_bucket(bucket_name, low, mid, depth + 1, failed)
                + fetch_screener_bucket(bucket_name, mid, high, depth + 1, failed))
    return [{**d, "_cap_bucket": bucket_name} for d in rows]


# ------------------------------------------------------------------ v4.1: the bank fill and the publication guard --
def _truthy(v):
    return str(v).strip().lower() in ("true", "1", "yes")


def _num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x == x and abs(x) != float("inf") else None


def bucket_of(mc):
    for name, lo, hi in CAP_BUCKETS:
        if lo <= mc < hi:
            return name
    return "unknown"


def bank_profile_rows(s3=None, now=None):
    """FMP's whole-market profile file as banked by justhodl-fmp-bank (profile-bulk/part-N/latest.csv.gz), STREAMED
    (93k rows, ~38 MB gzip - never held in memory) and filtered like the screener request: exchange NYSE/NASDAQ/AMEX,
    actively trading, not an ETF or fund, market cap >= the nano floor.  -> (rows in the screener's field names,
    provenance {status, as_of = the oldest part's write time, age_days, objects, rows}).  Older than
    BANK_MAX_AGE_DAYS -> no rows, status "stale"."""
    s3 = s3 or S3
    now = now or datetime.now(timezone.utc)
    objs, kw = [], {"Bucket": BUCKET, "Prefix": BANK_PREFIX}
    while True:
        resp = s3.list_objects_v2(**kw)
        objs += [o for o in resp.get("Contents", []) if o["Key"].endswith("/latest.csv.gz")]
        if not resp.get("IsTruncated"):
            break
        kw["ContinuationToken"] = resp["NextContinuationToken"]
    if not objs:
        return [], {"status": "not_banked", "prefix": BANK_PREFIX}
    as_of = min(o["LastModified"] for o in objs)
    age = (now - as_of).total_seconds() / 86400.0
    prov = {"status": "read", "objects": sorted(o["Key"] for o in objs), "as_of": as_of.isoformat(), "age_days": round(age, 2),
            "max_age_days": BANK_MAX_AGE_DAYS}
    if age > BANK_MAX_AGE_DAYS:
        prov["status"] = "stale"
        return [], prov
    rows, stamp = [], as_of.isoformat()
    for o in sorted(objs, key=lambda o: o["Key"]):
        body = s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"]
        with gzip.GzipFile(fileobj=body) as gz:
            reader = csv.reader(io.TextIOWrapper(gz, encoding="utf-8", errors="replace", newline=""))
            header = next(reader, None) or []
            col = {h.strip(): i for i, h in enumerate(header)}
            if any(k not in col for k in ("symbol", "exchange", "marketCap")):
                prov.setdefault("unreadable_objects", []).append(o["Key"])
                continue

            def g(row, name):
                i = col.get(name)
                return row[i] if i is not None and i < len(row) else None
            for row in reader:
                ex = (g(row, "exchange") or "").strip().upper()
                if ex not in SCREEN_EXCHANGES:
                    continue
                if "isActivelyTrading" in col and not _truthy(g(row, "isActivelyTrading")):
                    continue
                if _truthy(g(row, "isEtf")) or _truthy(g(row, "isFund")):
                    continue
                mc = _num(g(row, "marketCap"))
                sym = (g(row, "symbol") or "").strip().upper()
                if not sym or not mc or mc < CAP_BUCKETS[0][1]:
                    continue
                rows.append({"symbol": sym, "companyName": g(row, "companyName"), "marketCap": mc, "price": _num(g(row, "price")),
                             "exchange": ex, "industry": g(row, "industry"), "sector": g(row, "sector"), "country": g(row, "country"),
                             "_source": "bank_profile_bulk", "_as_of": stamp})
    prov["rows"] = len(rows)
    return rows, prov


def book_identity(s3=None):
    """symbol -> identity flags from the factor book's universe (Polygon reference types folded into is_common):
    -> (flags, provenance).  {} when the book is not there - the builder's own filter then stands alone."""
    try:
        doc = json.loads(gzip.decompress((s3 or S3).get_object(Bucket=BUCKET, Key=BOOK_UNIVERSE_KEY)["Body"].read()))
        syms = doc.get("symbols") if isinstance(doc, dict) else None
        if not isinstance(syms, dict):
            return {}, {"status": "malformed", "key": BOOK_UNIVERSE_KEY}
        return syms, {"status": "read", "key": BOOK_UNIVERSE_KEY, "generated_at": doc.get("generated_at"), "symbols": len(syms)}
    except Exception as e:  # noqa: BLE001
        return {}, {"status": "unavailable", "key": BOOK_UNIVERSE_KEY, "error": type(e).__name__}


COMMON_TYPES = {"CS", "ADRC", "OS", "GDR", "NYRS", "LT"}          # Polygon reference types that are a company's equity
NEVER_TYPES = {"WARRANT", "RIGHT", "ADRW", "ADRR", "BOND", "AGEN", "EQLK", "BASKET", "ETN", "ETV", "ETS"}
CHECKED_TYPES = {"SP", "PFD", "ADRP", "UNIT"}                       # dropped only when a second signal agrees
NOTE_NAME = re.compile(r"\d(?:\.\d+)?\s?%[^,]*?\b(notes?|nts?|debentures?|bonds?|subordinated|sub)\b", re.I)
PREF_NAME = re.compile(r"\bpfd\b|\bpreferred (stock|shares?|securities)\b|depositary shares?,? (each )?represent|\bdepository shs\b"
                       r"|\bperpetual\b|\bcumulative (redeemable|preferred|perpetual)\b|\bconv(ertible)? pfd\b", re.I)
DEBT_NAME = re.compile(r"\d\s?%|\b\d{1,2}\.\d{1,3}\b|\b(limited|ltd|inc|corp|corporation|company|co|llc|plc|lp)\.?,?\s+\d+(\.\d*)?\s*$|"
                       r"\b(notes?|nts?|nt|debentures?|bonds?|bd|preferred|pfd|perpetual|cumulative|depositary|"
                       r"depository|subordinated|junior|jr|senior|sr|collateral|parrs|zones|series|units?|warrants?|rights?|"
                       r"tangible equity|capital trust|trust preferred|acquisition|llc|ulc|finance (co|corp|llc|ulc))\b", re.I)
WARRANT_NAME = re.compile(r"\bwarrants?\b|\brights?\b|\bunits?\b", re.I)
NAME_STOP = {"inc", "corp", "corporation", "company", "co", "ltd", "limited", "plc", "the", "incorporated", "llc", "lp", "l", "p"}


def _norm_name(name):
    return " ".join(w for w in re.findall(r"[a-z0-9]+", (name or "").lower()) if w not in NAME_STOP)


class ListingRule:
    """Is a listing a company's equity?  The screener and the profile file both list an issuer's notes, preferreds,
    warrants, rights and units under their own tickers WITH THE ISSUER'S MARKET CAP (2026-10-27, ops 6694: the live
    universe carried 172 of them - AT&T's 5.35% notes TBB as a $140B large cap, Southern Co / Duke / Prudential notes,
    Strategy's preferreds, SPAC warrants and rights).  Polygon's reference type, joined by the factor book, decides:
      * WARRANT / RIGHT / bond-like types -> out;
      * SP / PFD / ADRP / UNIT -> out when a second signal agrees: the name (coupon, notes, series, preferred, units,
        LLC ...), an issuer sibling (a common-typed listing whose name this one's starts with - "Prudential Financial,
        Inc. 5.95" vs PRU) or a secondary ticker (a dash, or a common ticker + one letter).  Polygon mis-types a few
        plain commons (Tutor Perini TPC as PFD, Balchem BCPC as SP): with no second signal they stay;
      * any type, a name like "6.25 % Notes" / "Junior Subordinated Notes" -> out (notes Polygon types CS);
        a CS whose name is a preferred share -> out;
      * untyped -> out only with a warrant/rights/units/preferred name or a warrant-style suffix on a common ticker.
    The factor book's own is_common flag is NOT used: its symbol/name heuristics flag W.W. Grainger (GWW), News Corp
    (NWS/NWSA) and every "American Depositary Shares" ADR (ARM)."""

    def __init__(self, ident):
        self.ident = ident or {}
        self.common_syms, self.common_names = set(), {}
        for sym, f in self.ident.items():
            if isinstance(f, dict) and f.get("security_type") in COMMON_TYPES:
                self.common_syms.add(sym)
                n = _norm_name(f.get("name"))
                if n:
                    self.common_names.setdefault(n, set()).add(sym)

    def _type(self, sym):
        f = self.ident.get(sym) or self.ident.get(sym.replace(".", "-")) or self.ident.get(sym.replace("-", ".")) or {}
        return f.get("security_type") if isinstance(f, dict) else None

    def _sibling(self, sym, name):
        words = _norm_name(name).split()
        for k in range(len(words), 0, -1):
            prefix = " ".join(words[:k])
            if len(prefix) >= 5 and (self.common_names.get(prefix) or set()) - {sym}:
                return True
        return False

    def verdict(self, sym, name):
        """-> (keep, reason)"""
        t, name = self._type(sym), name or ""
        if NOTE_NAME.search(name):
            return False, "note_by_name"
        if t in COMMON_TYPES:
            return (False, "preferred_by_name") if t == "CS" and PREF_NAME.search(name) else (True, None)
        if t in NEVER_TYPES:
            return False, "type_" + t.lower()
        if t in CHECKED_TYPES:
            if DEBT_NAME.search(name):
                return False, "type_%s_name" % t.lower()
            if self._sibling(sym, name):
                return False, "type_%s_issuer_sibling" % t.lower()
            if "-" in sym or (len(sym) >= 4 and sym[:-1] in self.common_syms):
                return False, "type_%s_secondary_ticker" % t.lower()
            return True, None
        if t is None and (WARRANT_NAME.search(name) or PREF_NAME.search(name) or re.search(r"-P[A-Z]{0,2}$", sym)
                          or (re.search(r"(W|WS|U|R|RT)$", sym) and (sym[:-1] in self.common_syms or sym[:-2] in self.common_syms))):
            return False, "untyped_by_name_or_ticker"
        return True, None


def bank_fill(failed, s3=None, now=None, members=None, rule=None):
    """Rows for exactly the failed market-cap spans, from the bank -> (rows, provenance).  `members` (the previous
    publication's symbols) keeps a fill to names the screener itself listed - the bank refreshes membership it does not
    invent; None (no usable previous publication) -> every profile row, with the listing rule only."""
    rows, prov = bank_profile_rows(s3, now)
    if not rows:
        return [], prov
    out, outside, not_member, not_equity = [], 0, 0, 0
    for r in rows:
        mc = r["marketCap"]
        if not any(f["low"] <= mc < f["high"] for f in failed):
            outside += 1
            continue
        if members is not None and r["symbol"] not in members:
            not_member += 1
            continue
        if rule is not None and not rule.verdict(r["symbol"], r.get("companyName"))[0]:
            not_equity += 1
            continue
        out.append({**r, "_cap_bucket": bucket_of(mc)})
    prov.update(membership="previous_publication" if members is not None else "listing_rule_only",
                in_failed_spans=len(out) + not_member + not_equity, dropped_not_member=not_member, dropped_not_equity=not_equity,
                outside_failed_spans=outside)
    return out, prov


def previous_publication(s3=None):
    """The live data/universe.json (dict) or None."""
    try:
        doc = json.loads((s3 or S3).get_object(Bucket=BUCKET, Key=S3_KEY)["Body"].read())
        return doc if isinstance(doc, dict) else None
    except Exception:  # noqa: BLE001
        return None


def guard_violations(stocks, by_bucket, previous):
    """Why this universe must NOT replace the live one ([] = publish)."""
    out = []
    if len(stocks) < MIN_PUBLISH:
        out.append("only %d stocks (a US-listed universe is never below %d)" % (len(stocks), MIN_PUBLISH))
    pst = (previous or {}).get("stats") or {}
    prev_total = pst.get("total_stocks") if isinstance(pst.get("total_stocks"), int) else 0
    if prev_total >= MIN_PUBLISH and len(stocks) < GUARD_TOTAL_RATIO * prev_total:
        out.append("%d stocks < %d%% of the previous publication's %d" % (len(stocks), round(GUARD_TOTAL_RATIO * 100), prev_total))
    for name, n in sorted((pst.get("by_cap_bucket") or {}).items()):
        if isinstance(n, int) and n >= 50 and by_bucket.get(name, 0) < GUARD_BUCKET_RATIO * n:
            out.append("bucket %s %d < %d%% of the previous %d" % (name, by_bucket.get(name, 0), round(GUARD_BUCKET_RATIO * 100), n))
    return out


def compare(stocks, previous):
    """This build vs the live publication: membership overlap and bucket agreement (the dry-run verification)."""
    prev = {s.get("symbol"): s for s in ((previous or {}).get("stocks") or []) if isinstance(s, dict)}
    new = {s["symbol"]: s for s in stocks}
    both = set(prev) & set(new)
    return {"previous_total": len(prev), "new_total": len(new), "both": len(both),
            "both_pct_of_previous": round(100.0 * len(both) / len(prev), 2) if prev else None,
            "only_previous": len(set(prev) - set(new)), "only_new": len(set(new) - set(prev)),
            "only_previous_sample": sorted(set(prev) - set(new))[:40], "only_new_sample": sorted(set(new) - set(prev))[:40],
            "same_bucket": sum(1 for s in both if prev[s].get("cap_bucket") == new[s].get("cap_bucket")),
            "previous_generated_at": (previous or {}).get("generated_at")}


def fetch_quote_for_seed(symbol):
    """For curated seed, fetch quote to get current mcap + price."""
    try:
        url = "https://financialmodelingprep.com/stable/quote?symbol=" + symbol + "&apikey=" + FMP_KEY
        data = fetch_url(url, timeout=10)
        if isinstance(data, list) and data:
            return data[0]
    except Exception:
        pass
    return None


def fetch_profile_for_seed(symbol):
    """For curated seed, fetch profile to get sector/industry."""
    try:
        url = "https://financialmodelingprep.com/stable/profile?symbol=" + symbol + "&apikey=" + FMP_KEY
        data = fetch_url(url, timeout=10)
        if isinstance(data, list) and data:
            return data[0]
    except Exception:
        pass
    return None


def is_valid_common_stock(stock):
    """Return True if this is a real common stock (not fund/preferred/warrant)."""
    sym = (stock.get("symbol") or "").upper()
    if not sym:
        return False
    
    # Filter symbols with special suffixes (preferreds, warrants, units)
    for suf in EXCLUDE_SYMBOL_SUFFIX:
        if sym.endswith(suf):
            return False
    
    # Filter symbols with dots/slashes (rare common shares but most are warrants)
    if "." in sym or "/" in sym:
        # Allow BRK.B, BRK.A, RDS.A type structures (real common with dual class)
        if not (len(sym) <= 5 and sym.split(".")[0] in ("BRK", "BF", "GEF", "MOG", "BIO")):
            return False
    
    industry = (stock.get("industry") or "").lower().strip()
    if industry in EXCLUDE_INDUSTRIES:
        return False
    if "fund" in industry and "fundamental" not in industry:
        return False
    if industry.startswith("etf"):
        return False
    
    # Filter "Trust" / "Fund" in company name (common for closed-end funds)
    name = (stock.get("companyName") or "").lower()
    if any(kw in name for kw in [" fund", "etf", "trust series", "spdr", "ishares", "vanguard", "invesco bullet", "schwab "]):
        # Allow REITs which have "Trust" 
        if not any(reit_kw in (industry or "") for reit_kw in ["reit", "real estate"]):
            return False
    
    # Must have valid market cap
    mc = stock.get("marketCap")
    if not mc or mc <= 0:
        return False
    
    return True


def lambda_handler(event=None, context=None):
    started = time.time()
    event = event if isinstance(event, dict) else {}
    dry_run, force_bank = bool(event.get("dry_run")), bool(event.get("force_bank"))
    print("[universe] v" + ENGINE_VERSION + " starting — exhaustive all-US-listed (bisection + ADRs)"
          + (" [dry_run]" if dry_run else "") + (" [force_bank]" if force_bank else ""))

    # Fetch all 6 cap buckets in parallel; v4.1: failed (sub-)ranges are recorded
    all_raw, failed = {}, []
    if force_bank:
        failed = [{"bucket": name, "low": lo, "high": hi, "error": "forced by event.force_bank"} for name, lo, hi in CAP_BUCKETS]
        all_raw = {name: [] for name, _, _ in CAP_BUCKETS}
    else:
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = {pool.submit(fetch_screener_bucket, name, lo, hi, 0, failed): (name, lo, hi)
                       for name, lo, hi in CAP_BUCKETS}
            for f in as_completed(futures):
                name, lo, hi = futures[f]
                data = f.result()
                all_raw[name] = data
                print("[universe] " + name + " ($" + str(lo // 1_000_000) +
                      "M-): raw=" + str(len(data)))
    n_screener_rows = sum(len(v) for v in all_raw.values())

    # v4.1: fill exactly the failed spans from the FMP bank's profile-bulk (labelled, dated)
    previous = previous_publication()
    ident, ident_prov = book_identity()
    rule = ListingRule(ident) if ident else None          # no book -> the v4 filter alone (said in sources.listing_rule)
    prev_stocks = (previous or {}).get("stocks") if isinstance((previous or {}).get("stocks"), list) else []
    members = {s.get("symbol") for s in prev_stocks if isinstance(s, dict)} if len(prev_stocks) >= MIN_PUBLISH else None
    fill_rows, fill = [], {"status": "not_needed"}
    if failed:
        print("[universe] " + str(len(failed)) + " screener range(s) failed - filling those market-cap spans from the FMP bank's profile-bulk")
        try:
            fill_rows, fill = bank_fill(failed, members=members, rule=rule)
        except Exception as e:  # noqa: BLE001 - the guard below decides what an unfilled hole means
            fill = {"status": "error", "error": type(e).__name__ + ": " + str(e)[:200]}
        print("[universe] bank fill: " + json.dumps({k: fill.get(k) for k in ("status", "as_of", "age_days", "rows", "in_failed_spans", "dropped_not_common", "error")}))

    # Combine + dedupe (mid/large can return same stock if mcap crosses bucket boundary)
    by_symbol = {}
    for bucket_name, stocks in all_raw.items():
        for s in stocks:
            sym = (s.get("symbol") or "").upper()
            if not sym:
                continue
            if sym in by_symbol:
                # Keep the one with higher mcap (more recent/accurate)
                if (s.get("marketCap") or 0) > (by_symbol[sym].get("marketCap") or 0):
                    by_symbol[sym] = s
            else:
                by_symbol[sym] = s
    n_bank_added = 0
    for s in fill_rows:                      # the screener's own answer always wins over the bank's
        if s["symbol"] not in by_symbol:
            by_symbol[s["symbol"]] = s
            n_bank_added += 1

    print("[universe] combined: " + str(len(by_symbol)) + " unique symbols" + (" (" + str(n_bank_added) + " from the bank)" if fill_rows else ""))

    # Filter to real common stocks
    valid_stocks = []
    n_filtered_industry = 0
    n_filtered_symbol = 0
    n_filtered_other = 0
    listing_drops = {}
    for sym, s in by_symbol.items():
        if rule is not None:
            keep, why = rule.verdict(sym, s.get("companyName"))
            if not keep:
                listing_drops.setdefault(why, []).append(sym)
                continue
        if not is_valid_common_stock(s):
            sym_lower = sym.lower()
            industry_lower = (s.get("industry") or "").lower().strip()
            if any(suf for suf in EXCLUDE_SYMBOL_SUFFIX if sym.endswith(suf)) or "." in sym or "/" in sym:
                n_filtered_symbol += 1
            elif industry_lower in EXCLUDE_INDUSTRIES or "fund" in industry_lower:
                n_filtered_industry += 1
            else:
                n_filtered_other += 1
            continue
        valid_stocks.append(s)

    print("[universe] valid: " + str(len(valid_stocks)) + 
          " (filtered industry: " + str(n_filtered_industry) +
          ", symbol: " + str(n_filtered_symbol) +
          ", other: " + str(n_filtered_other) +
          ", not a company's equity: " + str(sum(len(v) for v in listing_drops.values())) + ")")

    # Add curated seed (with deduplication)
    existing_syms = {(s.get("symbol") or "").upper() for s in valid_stocks}
    seed_to_add = [s for s in CURATED_SEED if s not in existing_syms]
    print("[universe] curated seed missing from screener: " + str(len(seed_to_add)) + 
          " of " + str(len(CURATED_SEED)))

    seed_added = 0
    if seed_to_add:
        with ThreadPoolExecutor(max_workers=10) as pool:
            quote_futures = {pool.submit(fetch_quote_for_seed, sym): sym for sym in seed_to_add}
            profile_futures = {pool.submit(fetch_profile_for_seed, sym): sym for sym in seed_to_add}
            quotes = {sym: f.result() for f, sym in quote_futures.items()}
            profiles = {sym: f.result() for f, sym in profile_futures.items()}
        for sym in seed_to_add:
            q = quotes.get(sym)
            p = profiles.get(sym)
            if not q:
                continue
            mc = q.get("marketCap") or 0
            # Determine bucket
            cap_bucket = "unknown"
            for name, lo, hi in CAP_BUCKETS:
                if lo <= mc < hi:
                    cap_bucket = name
                    break
            stock = {
                "symbol": sym,
                "companyName": q.get("name") or (p or {}).get("companyName"),
                "marketCap": mc,
                "price": q.get("price"),
                "exchange": q.get("exchange"),
                "industry": (p or {}).get("industry"),
                "sector": (p or {}).get("sector"),
                "_cap_bucket": cap_bucket,
                "_source": "curated_seed",
            }
            valid_stocks.append(stock)
            seed_added += 1

    print("[universe] curated seed added: " + str(seed_added))

    # Build final stock objects
    final_stocks = []
    by_bucket = {name: 0 for name, _, _ in CAP_BUCKETS}
    by_bucket["unknown"] = 0
    by_sector = {}
    by_exchange = {}
    n_adr = 0
    
    for s in valid_stocks:
        sym = (s.get("symbol") or "").upper()
        mc = s.get("marketCap") or 0
        cap_bucket = s.get("_cap_bucket") or "unknown"
        # Re-verify bucket from market cap (to override any stale tagging)
        for name, lo, hi in CAP_BUCKETS:
            if lo <= mc < hi:
                cap_bucket = name
                break
        
        sector = s.get("sector") or "Unknown"
        by_sector[sector] = by_sector.get(sector, 0) + 1
        by_bucket[cap_bucket] = by_bucket.get(cap_bucket, 0) + 1
        _exch = s.get("exchange") or s.get("exchangeShortName") or ""
        _ctry = s.get("country") or ""
        by_exchange[_exch or "?"] = by_exchange.get(_exch or "?", 0) + 1
        _adr = bool(_ctry and _ctry.upper() not in ("US", "USA"))
        if _adr:
            n_adr += 1

        row = {
            "symbol": sym,
            "name": s.get("companyName") or "",
            "sector": sector,
            "industry": s.get("industry") or "Unknown",
            "exchange": _exch,
            "country": _ctry,
            "is_adr": _adr,
            "market_cap": mc,
            "price": s.get("price"),
            "cap_bucket": cap_bucket,
            "source": s.get("_source") or "screener",
        }
        if s.get("_as_of"):
            row["as_of"] = s["_as_of"]          # v4.1: a bank row's market cap / price are as of the bank's pull
        final_stocks.append(row)

    # Sort by mcap descending so consumers get most-liquid first
    final_stocks.sort(key=lambda x: -(x.get("market_cap") or 0))
    by_source = {}
    for s in final_stocks:
        by_source[s["source"]] = by_source.get(s["source"], 0) + 1

    out = {
        "schema_version": 4,
        "method": "universe_builder_v4_exhaustive_bisection",
        "engine_version": ENGINE_VERSION,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "total_stocks": len(final_stocks),
            "by_cap_bucket": by_bucket,
            "by_sector_top_10": dict(sorted(by_sector.items(), key=lambda x: -x[1])[:10]),
            "by_exchange": by_exchange,
            "n_adr": n_adr,
            "n_curated_seed_added": seed_added,
            "by_source": by_source,
        },
        "sources": {
            "screener": {"endpoint": "/stable/company-screener", "rows": n_screener_rows, "ranges_failed": len(failed),
                         "failed_ranges": failed[:60]},
            "bank_profile_bulk": dict(fill, added_before_filter=n_bank_added),
            "listing_rule": dict(ident_prov, applied=rule is not None,
                                 dropped=sum(len(v) for v in listing_drops.values()),
                                 by_reason={k: len(v) for k, v in sorted(listing_drops.items())},
                                 samples={k: sorted(v)[:12] for k, v in sorted(listing_drops.items())}),
        },
        "cap_buckets": [
            {"name": name, "low": lo, "high": hi}
            for name, lo, hi in CAP_BUCKETS
        ],
        "stocks": final_stocks,
    }

    # v4.1: the publication guard - a shrunken universe never replaces the live one
    violations = guard_violations(final_stocks, by_bucket, previous)
    if dry_run:
        cmp = compare(final_stocks, previous)
        print("[universe] dry run (nothing written): " + json.dumps({"violations": violations, "by_bucket": by_bucket, "by_source": by_source,
                                                                    "compare": {k: v for k, v in cmp.items() if not k.endswith("_sample")}}))
        return {"statusCode": 200, "body": json.dumps({"dry_run": True, "would_publish": not violations, "violations": violations,
                                                       "total_stocks": len(final_stocks), "by_bucket": by_bucket, "by_source": by_source,
                                                       "sources": out["sources"], "compare": cmp, "duration_s": round(time.time() - started, 1)}, default=str)}
    if violations:
        msg = ("universe NOT published: " + "; ".join(violations) + " - the previous publication (generated_at "
               + str((previous or {}).get("generated_at")) + ") stays live")
        print("[universe] " + msg)
        raise RuntimeError(msg)

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[universe] wrote " + str(len(body)) + "b")
    print("[universe] by_bucket: " + json.dumps(by_bucket))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "total_stocks": len(final_stocks),
            "by_bucket": by_bucket,
            "by_source": by_source,
            "ranges_failed": len(failed),
            "bank_fill": fill.get("status"),
            "duration_s": out["duration_s"],
        }),
    }
