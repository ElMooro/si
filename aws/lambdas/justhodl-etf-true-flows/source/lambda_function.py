"""justhodl-etf-true-flows v2.0 — TRUE ETF creation/redemption flows at NAV

ops-4559 rewrite. What changed and why:

  BUG-1  flow = Δshares × NAV (was × price). Baskets settle at NAV; using
         market price injects premium/discount as phantom flow — on a 40bp
         discount day for a $10B fund that fabricated $40M. Price is kept
         ONLY as the premium_discount_bps diagnostic (a real stress signal).
  BUG-2  Share counts + NAV now come from ISSUER sources first (iShares
         screener, SSGA fund finder, ProShares historical CSV — the last one
         carries FULL history, killing the cold-start problem for the entire
         ProShares complex). FMP is the fallback, not the primary.
  BUG-3  Distribution correction: on ex-dates, dist_per_share × shares is
         added back so an NAV drop from a payout doesn't read as an outflow.
         A TNA-residual cross-check (Morningstar method) runs in parallel;
         disagreement > 25bp of TNA lands in anomalies[].

Tiering (Part 0 discipline — facts vs estimates):
  tier_1_realtime_estimate : Δshares × NAV, T+1, this engine's live number
  tier_2_cross_check       : AUM÷NAV-derived shares, disagreement flag
  tier_3_ground_truth      : SEC N-PORT Item B.6 (T+60d) — reconciliation
                             target; wiring status stated honestly in payload.

OUTPUT: data/etf-true-flows.json   Daily 15:45 UTC.
"""
import csv, io, json, os, time
import urllib.request, urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/etf-true-flows.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP = "https://financialmodelingprep.com/stable"
UA = {"User-Agent": "JustHodl/2.0 (ops@justhodl.ai)"}
s3 = boto3.client("s3", region_name=REGION)

ISHARES_SCREENER = ("https://www.ishares.com/us/product-screener/product-screener-v3.1.jsn"
                    "?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/"
                    "ishares-product-screener-backend-config&siteEntryPassthrough=true")
SSGA_FUNDFINDER = ("https://www.ssga.com/bin/v1/ssmp/fund/fundfinder"
                   "?country=us&language=en&role=intermediary&product=etfs&ui=fund-finder")
PROSHARES_HIST = "https://accounts.profunds.com/etfdata/ByFund/{t}-historical_nav.csv"

ETFS = {
    "BROAD_EQUITY_US": ["SPY", "VOO", "IVV", "QQQ", "VTI", "IWM", "DIA", "RSP"],
    "SECTOR_EQUITY": ["XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE", "XLC", "SMH", "XBI", "KRE", "ITB", "XOP"],
    "THEMATIC": ["ARKK", "ICLN", "TAN", "BOTZ", "ROBO", "LIT", "URA", "NLR", "IGV", "HACK", "CIBR", "FINX", "DRIV"],
    "INTERNATIONAL": ["EFA", "VEA", "EEM", "VWO", "FXI", "EWZ", "EWJ", "INDA", "ASHR"],
    "COUNTRY": ["MCHI", "EWG", "EWY", "EWT", "EWU", "EWC", "EWA", "EWW", "EZA", "TUR",
                "EPOL", "ARGT", "EIDO", "VNM", "THD", "EWQ", "EWL", "EWI", "EWP", "EWS"],
    "RATES_TREASURIES": ["TLT", "IEF", "SHY", "GOVT", "BIL", "SGOV", "SHV", "VGSH", "IEI", "VGIT", "VGLT", "EDV"],
    "CREDIT": ["LQD", "HYG", "JNK", "AGG", "BND", "VCIT", "VCSH", "USHY", "SJNK", "BKLN", "SRLN", "EMB", "MUB"],
    "TIPS_INFLATION": ["TIP", "SCHP", "VTIP", "STIP"],
    "CRYPTO_ETF": ["IBIT", "FBTC", "ETHA", "BITO"],
    "COMMODITIES": ["GLD", "SLV", "USO", "DBC", "DBA", "UNG", "GDX"],
    "CRYPTO": ["IBIT", "FBTC", "ETHA", "BITO", "GBTC"],
    "VOLATILITY": ["VXX", "UVXY", "SVXY"],
    "DIVIDEND_VALUE": ["SCHD", "VYM", "VTV", "DVY", "VIG"],
    "GROWTH": ["VUG", "IWF", "MGK", "SCHG"],
    # ops-4559: leveraged/inverse — where flow says the most about positioning,
    # and where the ProShares historical CSVs give us instant deep history.
    "LEVERED_INVERSE": ["TQQQ", "SQQQ", "SSO", "SDS", "UPRO", "SPXU", "QLD", "QID", "TBT"],
}
PROSHARES = {"TQQQ", "SQQQ", "SSO", "SDS", "UPRO", "SPXU", "QLD", "QID", "TBT", "UVXY", "SVXY", "BITO"}
HIST_KEY = "data/etf-shares-history.json"
SNAP_KEY = "data/etf-shares-snapshots/latest.json"
HISTORY_DAYS = 90
ANOMALY_BP = 25.0


def _get(url, t=25, raw=False):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=t) as r:
            b = r.read()
            return b if raw else b.decode("utf-8", "replace")
    except Exception:
        return None


def probe(url, t=10):
    """ops-4559 first-fetch assertion: HEAD (fall back to ranged GET)."""
    try:
        req = urllib.request.Request(url, headers=UA, method="HEAD")
        with urllib.request.urlopen(req, timeout=t) as r:
            return {"url": url.split("?")[0], "ok": True, "status": r.status}
    except urllib.error.HTTPError as e:
        if e.code in (403, 405):  # HEAD-hostile hosts — try a real GET
            try:
                req = urllib.request.Request(url, headers={**UA, "Range": "bytes=0-512"})
                with urllib.request.urlopen(req, timeout=t) as r:
                    return {"url": url.split("?")[0], "ok": True, "status": r.status}
            except Exception as e2:
                return {"url": url.split("?")[0], "ok": False, "err": str(e2)[:80]}
        return {"url": url.split("?")[0], "ok": False, "err": "HTTP %s" % e.code}
    except Exception as e:
        return {"url": url.split("?")[0], "ok": False, "err": str(e)[:80]}


def http_json(url, t=25):
    b = _get(url, t)
    if b is None:
        return None
    try:
        return json.loads(b)
    except Exception:
        return None


def read_json(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def num(v):
    try:
        if isinstance(v, str):
            v = v.replace(",", "").replace("$", "").strip()
        f = float(v)
        return f if f == f else None
    except Exception:
        return None


# ── issuer sources ──────────────────────────────────────────────────────────

def ishares_map(gaps, probes):
    """iShares screener → {ticker: {nav, tna, shares}} for the whole complex."""
    p = probe(ISHARES_SCREENER); probes.append({"src": "ishares_screener", **p})
    if not p["ok"]:
        gaps.append("ishares screener probe failed: %s" % p.get("err")); return {}
    j = http_json(ISHARES_SCREENER, t=40)
    if not isinstance(j, dict):
        gaps.append("ishares screener unparseable"); return {}
    out = {}
    def walk(node):
        if isinstance(node, dict):
            tk = node.get("localExchangeTicker") or node.get("ticker")
            nav = num((node.get("navAmount") or {}).get("r") if isinstance(node.get("navAmount"), dict) else node.get("navAmount"))
            tna = num((node.get("totalNetAssets") or {}).get("r") if isinstance(node.get("totalNetAssets"), dict) else node.get("totalNetAssets"))
            if tk and nav and nav > 0:
                out[str(tk).upper()] = {"nav": nav, "tna": tna,
                                        "shares": (tna / nav) if tna else None,
                                        "src": "ISHARES_ISSUER"}
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)
    walk(j)
    if not out:
        gaps.append("ishares screener parsed 0 funds")
    return out


def ssga_map(gaps, probes):
    p = probe(SSGA_FUNDFINDER); probes.append({"src": "ssga_fundfinder", **p})
    if not p["ok"]:
        gaps.append("ssga fundfinder probe failed: %s" % p.get("err")); return {}
    j = http_json(SSGA_FUNDFINDER, t=40)
    if not isinstance(j, dict):
        gaps.append("ssga fundfinder unparseable"); return {}
    out = {}
    def walk(node):
        if isinstance(node, dict):
            tk = node.get("fundTicker") or node.get("ticker")
            nav = num(node.get("nav")); aum = num(node.get("aum") or node.get("totalNetAssets"))
            if tk and nav and nav > 0:
                out[str(tk).upper()] = {"nav": nav, "tna": aum,
                                        "shares": (aum / nav) if aum else None,
                                        "src": "SSGA_ISSUER"}
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)
    walk(j)
    if not out:
        gaps.append("ssga fundfinder parsed 0 funds")
    return out


def proshares_history(tk, gaps):
    """Full historical NAV + shares for one ProShares fund → [{date, nav, shares}]."""
    url = PROSHARES_HIST.format(t=tk)
    b = _get(url, t=30)
    if not b or "," not in b:
        gaps.append("proshares hist %s unavailable" % tk); return []
    try:
        rows = list(csv.reader(io.StringIO(b)))
        hdr = [h.strip().lower() for h in rows[0]]
        def col(*names):
            for n in names:
                for i, h in enumerate(hdr):
                    if n in h:
                        return i
            return None
        ci_d = col("date"); ci_n = col("nav"); ci_s = col("shares")
        if ci_d is None or ci_n is None:
            gaps.append("proshares hist %s: no nav/date col (hdr=%s)" % (tk, hdr[:6])); return []
        out = []
        for r in rows[1:]:
            if len(r) <= max(ci_d, ci_n):
                continue
            nav = num(r[ci_n]); sh = num(r[ci_s]) if ci_s is not None and len(r) > ci_s else None
            d = r[ci_d].strip()
            if nav and d:
                try:  # normalize m/d/Y → ISO
                    if "/" in d:
                        m, dd, y = d.split("/")
                        d = "%04d-%02d-%02d" % (int(y), int(m), int(dd))
                except Exception:
                    pass
                out.append({"date": d, "nav": nav, "shares": sh})
        return out
    except Exception as e:
        gaps.append("proshares hist %s parse: %s" % (tk, str(e)[:60])); return []


def fmp_quote_and_shares(sym):
    """Fallback path: FMP shares-float (latest-only) + market price + etf nav if present."""
    so = price = nav = None
    sf = http_json("%s/shares-float?symbol=%s&apikey=%s" % (FMP, sym, FMP_KEY), t=12)
    rec = (sf[0] if isinstance(sf, list) and sf else sf) if sf else None
    if rec:
        so = num(rec.get("outstandingShares")) or num(rec.get("floatShares"))
    q = http_json("%s/quote-short?symbol=%s&apikey=%s" % (FMP, sym, FMP_KEY), t=12)
    if isinstance(q, list) and q:
        price = num(q[0].get("price"))
    ei = http_json("%s/etf/info?symbol=%s&apikey=%s" % (FMP, sym, FMP_KEY), t=12)
    er = (ei[0] if isinstance(ei, list) and ei else ei) if ei else None
    if isinstance(er, dict):
        nav = num(er.get("nav")) or num(er.get("navPrice"))
    return so, price, nav


def dividend_map(tickers, gaps):
    """FMP dividends calendar around today → {ticker: dist_per_share} on ex-date."""
    today = datetime.now(timezone.utc).date().isoformat()
    j = http_json("%s/dividends-calendar?from=%s&to=%s&apikey=%s" % (FMP, today, today, FMP_KEY), t=20)
    out = {}
    if isinstance(j, list):
        want = set(tickers)
        for r in j:
            tk = str(r.get("symbol") or "").upper()
            if tk in want:
                d = num(r.get("dividend") or r.get("adjDividend"))
                if d:
                    out[tk] = d
    elif j is None:
        gaps.append("dividends-calendar unavailable — ex-date correction skipped today")
    return out


def lambda_handler(event=None, context=None):
    t0 = time.time()
    gaps, probes, anomalies = [], [], []
    tickers = sorted({s for syms in ETFS.values() for s in syms})
    cat_of = {}
    for cat, syms in ETFS.items():
        for s in syms:
            cat_of.setdefault(s, cat)

    # 1) issuer bulk maps (one fetch each, covers many funds)
    issuer = {}
    issuer.update(ishares_map(gaps, probes))
    for k, v in ssga_map(gaps, probes).items():
        issuer.setdefault(k, v)

    # 2) per-ticker resolution: issuer → FMP fallback (parallel)
    def resolve(sym):
        rec = {"ticker": sym, "category": cat_of.get(sym, "?")}
        iss = issuer.get(sym)
        so_f, price, nav_f = fmp_quote_and_shares(sym)
        rec["price"] = price
        if iss and iss.get("nav"):
            rec["nav"] = iss["nav"]; rec["tna"] = iss.get("tna")
            rec["shares_outstanding"] = iss.get("shares") or so_f
            rec["nav_source"] = iss["src"]
        elif nav_f:
            rec["nav"] = nav_f; rec["tna"] = (so_f * nav_f) if so_f else None
            rec["shares_outstanding"] = so_f; rec["nav_source"] = "FMP_ETF_INFO"
        else:
            rec["nav"] = None; rec["tna"] = (so_f * price) if (so_f and price) else None
            rec["shares_outstanding"] = so_f; rec["nav_source"] = "PRICE_FALLBACK_DEGRADED"
        if rec.get("nav") and price:
            rec["premium_discount_bps"] = round((price - rec["nav"]) / rec["nav"] * 1e4, 1)
        else:
            rec["premium_discount_bps"] = None
        return rec if rec.get("shares_outstanding") else None

    today = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        for fut in as_completed([ex.submit(resolve, s) for s in tickers]):
            r = fut.result()
            if r:
                today[r["ticker"]] = r

    # 3) history: migrate old shape ({shares}) → new ({shares, nav, tna})
    hist = read_json(HIST_KEY) or {"days": []}
    days = hist.get("days", [])
    prev = read_json(SNAP_KEY) or {}
    prev_shares = prev.get("shares", {})
    prev_nav = prev.get("nav", {})
    prev_tna = prev.get("tna", {})

    # 3b) ProShares full-history backfill on cold/short history (BUG-2 killer)
    if len(days) < 20:
        ps = [t for t in tickers if t in PROSHARES]
        filled = 0
        ph = {}
        for tk in ps:
            h = proshares_history(tk, gaps)
            if h:
                ph[tk] = {r["date"]: r for r in h}
                filled += 1
        if ph:
            all_dates = sorted({d for m in ph.values() for d in m})[-HISTORY_DAYS:]
            by_date = {d.get("date"): d for d in days}
            for d in all_dates:
                slot = by_date.setdefault(d, {"date": d, "shares": {}, "nav": {}})
                slot.setdefault("shares", {}); slot.setdefault("nav", {})
                for tk, m in ph.items():
                    if d in m:
                        if m[d].get("shares"):
                            slot["shares"][tk] = m[d]["shares"]
                        slot["nav"][tk] = m[d]["nav"]
            days = [by_date[d] for d in sorted(by_date)]
            print("[etf-true-flows] proshares backfill: %d funds, %d dates" % (filled, len(all_dates)))

    div_map = dividend_map(tickers, gaps)

    # 4) flows at NAV, distribution-corrected, with TNA cross-check
    results = []
    for tk, r in today.items():
        so_now = r["shares_outstanding"]
        nav = r.get("nav") or r.get("price")   # degraded path already labeled
        p_so = prev_shares.get(tk)
        dist = div_map.get(tk, 0.0)
        nf1 = None
        if p_so and nav:
            nf1 = (so_now - p_so) * nav
            if dist:
                nf1 += dist * so_now   # BUG-3: payout is not an outflow
            nf1 = round(nf1, 0)
        # TNA-residual cross-check (Morningstar): TNA_t − TNA_{t−1}×(1+r)
        tna_now, tna_prev = r.get("tna"), prev_tna.get(tk)
        nav_prev = prev_nav.get(tk)
        if tna_now and tna_prev and nav and nav_prev:
            r_tot = (nav + dist) / nav_prev - 1.0
            alt = tna_now - tna_prev * (1.0 + r_tot)
            r["net_flow_1d_tna_method_usd"] = round(alt, 0)
            if nf1 is not None and tna_now > 0:
                dis_bp = abs(nf1 - alt) / tna_now * 1e4
                if dis_bp > ANOMALY_BP:
                    anomalies.append({"ticker": tk, "shares_method": nf1,
                                      "tna_method": round(alt, 0),
                                      "disagreement_bp_of_tna": round(dis_bp, 1)})
        def flow_over(n):
            if len(days) < n:
                return None
            old_day = days[-n]
            old = (old_day.get("shares") or {}).get(tk)
            old_nav = (old_day.get("nav") or {}).get(tk)
            base = nav if nav else None
            if old and base:
                return round((so_now - old) * base, 0)
            return None
        nf5, nf20 = flow_over(5), flow_over(20)
        results.append({**r,
                        "aum_est_b": round((r.get("tna") or (so_now * (nav or 0))) / 1e9, 2) if nav else None,
                        "net_flow_1d_usd": nf1,
                        "net_flow_5d_usd": nf5 if nf5 is not None else nf1,
                        "net_flow_20d_usd": nf20,
                        "dist_per_share_today": dist or None,
                        "shares_chg_5d_pct": (round((so_now / p_so - 1) * 100, 2) if p_so else None)})

    def fm(r):
        v = r.get("net_flow_5d_usd")
        return v if v is not None else (r.get("net_flow_1d_usd") or 0)
    results.sort(key=lambda x: -(fm(x) or 0))
    inflows = [r for r in results if (fm(r) or 0) > 0][:25]
    outflows = sorted([r for r in results if (fm(r) or 0) < 0], key=lambda x: fm(x) or 0)[:20]
    by_cat = defaultdict(lambda: {"net_flow_5d_usd": 0.0, "n": 0})
    for r in results:
        v = fm(r)
        if v is not None:
            by_cat[r["category"]]["net_flow_5d_usd"] += v
            by_cat[r["category"]]["n"] += 1
    cat_rotation = sorted([{"category": k, "net_flow_5d_usd": round(v["net_flow_5d_usd"], 0), "n_etfs": v["n"]}
                           for k, v in by_cat.items()], key=lambda x: -x["net_flow_5d_usd"])

    # 5) persist snapshot + rolled history
    day_str = datetime.now(timezone.utc).date().isoformat()
    snap = {"date": day_str,
            "shares": {tk: r["shares_outstanding"] for tk, r in today.items()},
            "nav": {tk: r["nav"] for tk, r in today.items() if r.get("nav")},
            "tna": {tk: r["tna"] for tk, r in today.items() if r.get("tna")}}
    s3.put_object(Bucket=BUCKET, Key=SNAP_KEY, Body=json.dumps(snap).encode(),
                  ContentType="application/json")
    if not days or days[-1].get("date") != day_str:
        days.append({"date": day_str, "shares": snap["shares"], "nav": snap["nav"]})
    else:
        days[-1] = {"date": day_str, "shares": snap["shares"], "nav": snap["nav"]}
    days = days[-HISTORY_DAYS:]
    s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps({"days": days}).encode(),
                  ContentType="application/json")

    src_counts = defaultdict(int)
    for r in results:
        src_counts[r.get("nav_source", "?")] += 1
    degraded = src_counts.get("PRICE_FALLBACK_DEGRADED", 0)
    bootstrapping = not prev_shares and len(days) < 2
    out = {
        "engine": "etf-true-flows", "version": "2.0",
        "engine_class": "fund_flow_mechanical",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "n_etfs": len(results),
        "maturity": "BOOTSTRAPPING" if bootstrapping else ("BUILDING" if len(days) < 5 else "READY"),
        "evidence_tier": "tier_1_realtime_estimate",
        "method": ("flow_t = Δ(shares outstanding) × NAV_t, distribution-corrected on "
                   "ex-dates (ops-4559 BUG-1/3). Baskets settle at NAV; market price is "
                   "reported only as premium_discount_bps. Shares+NAV from issuer files "
                   "first (iShares/SSGA/ProShares-with-history), FMP fallback. TNA-residual "
                   "cross-check per Morningstar; >%sbp disagreement → anomalies[]." % int(ANOMALY_BP)),
        "ground_truth": {"source": "SEC N-PORT Item B.6 (monthly sales/redemptions, ~T+60d)",
                         "status": "PENDING_WIRE",
                         "note": ("reconciliation job not yet wired; until it is, tier-1 "
                                  "numbers carry no stated error bound — treat 5d/20d "
                                  "windows as more reliable than 1d")},
        "nav_source_counts": dict(src_counts),
        "n_price_fallback_degraded": degraded,
        "probes": probes, "gaps": gaps, "anomalies": anomalies,
        "inflows": inflows, "outflows": outflows,
        "category_rotation": cat_rotation,
        "by_etf": {r["ticker"]: r for r in results},
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print("[etf-true-flows v2] DONE %ss — %d ETFs, nav_sources=%s, degraded=%d, "
          "anomalies=%d, gaps=%d, history=%dd"
          % (round(time.time() - t0, 1), len(results), dict(src_counts), degraded,
             len(anomalies), len(gaps), len(days)))
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_etfs": len(results), "maturity": out["maturity"],
        "degraded": degraded, "anomalies": len(anomalies), "history_days": len(days)})}
