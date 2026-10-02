"""
ticker_360.py — Cross-engine enrichment hub.

Problem: ~900 engines each hand-wire 3-8 data sources with ad-hoc merge
logic. No shared per-ticker "full picture"; adding a source means touching
every consumer.

This module is the shared assembler: given a ticker (or list), it pulls
every registered data domain, runs each through its evidence-contract
context, extracts the per-ticker slice, and returns ONE enriched view.

Usage (any engine):
    from ticker_360 import enrich, enrich_many
    view = enrich("AAPL", s3)          # single ticker, full 360 view
    views = enrich_many(["AAPL","NVDA"], s3)  # batch, S3 reads cached

    view["domains"]["short-interest"]["ticker_data"]  # per-ticker slice
    view["confluence"]["domains_covering"]            # coverage count
    view["confluence"]["bullish_domains"]             # domains with bull signal

Fail-soft: a dead/missing source marks its domain unavailable; the view
still returns. Never raises on data problems.

To register a new domain, add one entry to SOURCES. Every engine picks it
up automatically — no per-engine edits.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

BUCKET = "justhodl-dashboard-live"

# domain -> {key, context module, kind}
# kind "packet": universe packet, extract per-ticker slice from decision_view
# kind "tkr": per-ticker artifact, read ticker entry directly
SOURCES = {
    "short-interest":      {"key": "data/short-interest.json",
                            "context": "short_interest_context", "kind": "packet"},
    "short-interest-tkr":  {"key": "data/short-interest-tickers.json",
                            "context": None, "kind": "tkr"},
    "finra-short-volume":  {"key": "data/finra-short.json",
                            "context": "short_volume_context", "kind": "packet"},
    "dark-pool":           {"key": "data/dark-pool.json",
                            "context": "offexchange_context", "kind": "packet"},
    "share-flows":         {"key": "data/share-flows.json",
                            "context": "capital_structure_context", "kind": "packet"},
    "squeeze-fuel-ftd":    {"key": "data/squeeze-fuel.json",
                            "context": "sec_ftd_context", "kind": "packet"},
    "forensic":            {"key": "data/forensic-screen.json",
                            "context": "statement_context", "kind": "packet"},
    "settlement-fails":    {"key": "data/settlement-fails.json",
                            "context": "pd_fails_context", "kind": "packet"},
    "dollar":              {"key": "data/dollar-radar.json",
                            "context": "dollar_research_context", "kind": "packet"},
    "futures":             {"key": "data/futures-research.json",
                            "context": "futures_research_context", "kind": "packet"},
    "fx":                  {"key": "data/fx-quote-research.json",
                            "context": "fx_research_context", "kind": "packet"},
    "gold-rotation":       {"key": "data/gold-equity-rotation.json",
                            "context": "gold_rotation_context", "kind": "packet"},
    "macro-regime":        {"key": "data/macro-regime.json",
                            "context": None, "kind": "packet"},
    "flow-confluence":     {"key": "data/flow-confluence.json",
                            "context": None, "kind": "packet"},
    "squeeze-pretrigger":  {"key": "data/squeeze-pretrigger.json",
                            "context": None, "kind": "packet"},
    "cboe-options":        {"key": "data/cboe-options-chain.json",
                            "context": None, "kind": "packet"},
    "xbrl-fundamentals":   {"key": "data/xbrl-fundamentals-index.json",
                            "context": None, "kind": "packet"},
    "sec-8k":              {"key": "data/8k-by-ticker.json",
                            "context": None, "kind": "tkr"},
    "corporate-actions":   {"key": "data/corporate-actions-index.json",
                            "context": None, "kind": "packet"},
    "etf-holdings":        {"key": "data/etf-issuer-holdings.json",
                            "context": None, "kind": "packet"},
    "13f-holdings":          {"key": "data/13f-by-ticker.json",
                            "context": None, "kind": "packet"},
    "insider-trading":       {"key": "data/insider-trades.json",
                            "context": None, "kind": "packet"},
    "earnings":              {"key": "data/earnings-tracker.json",
                            "context": None, "kind": "packet"},
}

# Domains that describe the market as a whole, not individual tickers.
# They enrich every ticker's view (regime, dollar, futures...).
MARKET_WIDE = {"macro-regime", "dollar", "futures", "fx", "gold-rotation",
               "flow-confluence", "cboe-options"}

# decision_view keys that may hold per-ticker rows
_TICKER_LIST_KEYS = ("by_ticker", "tickers", "stocks", "rows", "items",
                     "board", "squeeze_candidates", "top_squeeze",
                     "top_covering", "top_distribution", "top_crowded",
                     "top_accumulation", "candidates", "setups", "names",
                     "top_picks", "data", "tickers_list")


def _tk(x):
    if isinstance(x, dict):
        return str(x.get("ticker") or x.get("symbol") or "").upper()
    if isinstance(x, str):
        return x.strip().upper()
    return ""


def _extract_ticker(view, ticker):
    """Pull the per-ticker slice out of a decision_view dict."""
    t = ticker.upper()
    # direct by_ticker / tickers dict hit
    for k in ("by_ticker", "tickers"):
        d = view.get(k)
        if isinstance(d, dict):
            for tk, v in d.items():
                if str(tk).upper() == t:
                    return v
    # scan list keys for a row with this ticker
    for k in _TICKER_LIST_KEYS:
        v = view.get(k)
        if isinstance(v, list):
            for it in v:
                if _tk(it) == t:
                    return it
        elif isinstance(v, dict):
            for tk, vv in v.items():
                if str(tk).upper() == t:
                    return vv
    return None


def _read_packet(s3, key, cache):
    if key in cache:
        return cache[key]
    try:
        pkt = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        pkt = None
    cache[key] = pkt
    return pkt


def _domain_view(s3, domain, spec, ticker, cache):
    """Build the enriched view for one domain. Never raises."""
    out = {"domain": domain, "available": False, "as_of": None,
           "ticker_data": None, "summary": {}}
    try:
        pkt = _read_packet(s3, spec["key"], cache)
        if not isinstance(pkt, dict):
            out["reason"] = "packet missing/unreadable"
            return out
        out["as_of"] = pkt.get("generated_at") or pkt.get("as_of")
        if spec["kind"] == "tkr":
            # per-ticker artifact: direct lookup
            t = ticker.upper()
            data = None
            if isinstance(pkt.get("tickers"), dict):
                for tk, v in pkt["tickers"].items():
                    if str(tk).upper() == t:
                        data = v
                        break
            elif isinstance(pkt.get("by_ticker"), dict):
                for tk, v in pkt["by_ticker"].items():
                    if str(tk).upper() == t:
                        data = v
                        break
            else:
                for tk, v in pkt.items():
                    if str(tk).upper() == t and isinstance(v, dict):
                        data = v
                        break
            if data is not None:
                out["available"] = True
                out["ticker_data"] = data
            else:
                out["reason"] = "ticker not in artifact"
            return out
        # packet kind: run through context if one is registered
        ctx_mod = spec.get("context")
        if ctx_mod:
            try:
                view = __import__(ctx_mod).decision_view(pkt)
            except Exception:
                view = {}
        else:
            view = pkt
        if not isinstance(view, dict):
            view = {}
        out["summary"] = {k: view.get(k) for k in
                          ("call", "score", "signal", "state") if k in view}
        td = _extract_ticker(view, ticker)
        if td is None:
            # fall back to scanning the raw packet itself
            td = _extract_ticker(pkt, ticker)
        out["ticker_data"] = td
        # domain counts as available if the packet parsed, even when this
        # ticker has no row (coverage is reported separately)
        out["available"] = True
        # market-wide domains enrich every ticker (regime/dollar/futures...)
        out["ticker_covered"] = td is not None or domain in MARKET_WIDE
        if domain in MARKET_WIDE and td is None:
            out["ticker_data"] = {"market_wide": True,
                                  "summary": out.get("summary")}
        return out
    except Exception as e:  # noqa: BLE001 - fail-soft per domain
        out["reason"] = "error: %s" % str(e)[:80]
        return out


def _confluence(domains):
    covering = [d for d, v in domains.items()
                if v.get("available") and v.get("ticker_covered", v.get("ticker_data") is not None)]
    avail = [d for d, v in domains.items() if v.get("available")]
    return {
        "domains_available": len(avail),
        "domains_total": len(domains),
        "domains_covering": covering,
        "coverage_count": len(covering),
        "coverage_pct": round(100.0 * len(covering) / max(1, len(domains)), 1),
    }


def enrich(ticker, s3, cache=None, domains=None):
    """Full 360-degree enriched view for one ticker. Never raises on data.

    Returns {"ticker", "as_of", "domains": {domain: view}, "confluence": {...}}.
    Pass cache={} to share S3 reads across calls; pass domains=[...] to
    restrict to a subset.
    """
    cache = cache if cache is not None else {}
    t = str(ticker or "").upper().strip()
    want = domains or list(SOURCES)
    dom = {}
    for d in want:
        spec = SOURCES.get(d)
        if not spec:
            continue
        dom[d] = _domain_view(s3, d, spec, t, cache)
    return {
        "ticker": t,
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "domains": dom,
        "confluence": _confluence(dom),
    }


def enrich_many(tickers, s3, domains=None):
    """Enriched views for many tickers; S3 packets read once. Never raises."""
    cache: dict = {}
    out = {}
    for t in tickers:
        t = str(t or "").upper().strip()
        if not t or t in out:
            continue
        try:
            out[t] = enrich(t, s3, cache=cache, domains=domains)
        except Exception:  # noqa: BLE001
            out[t] = {"ticker": t, "domains": {}, "confluence": {},
                      "error": "enrich failed"}
    return out


def universe(s3, cache=None):
    """Collect the ticker universe from all packet sources. Never raises."""
    cache = cache if cache is not None else {}
    tickers = set()
    for domain, spec in SOURCES.items():
        try:
            pkt = _read_packet(s3, spec["key"], cache)
            if not isinstance(pkt, dict):
                continue
            for k in ("by_ticker", "tickers"):
                d = pkt.get(k)
                if isinstance(d, dict):
                    tickers.update(str(x).upper() for x in d.keys())
            for k in _TICKER_LIST_KEYS:
                v = pkt.get(k)
                if isinstance(v, list):
                    for it in v:
                        tk = _tk(it)
                        if tk and len(tk) <= 6:
                            tickers.add(tk)
        except Exception:  # noqa: BLE001
            continue
    return sorted(tickers)
