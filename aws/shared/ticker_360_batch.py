"""Invocation-local batch projection for the Ticker 360 publisher.

The single-ticker hub remains the behavioral oracle. Validate each complete
source through its registered context once, then index the *projected* rows.
Never index raw rows behind a context hold. No state survives an invocation.
Yielded views share read-only source/context objects; the publisher copies its
compact output. This avoids repeatedly hashing large packets for every symbol.
"""
from datetime import datetime, timezone

import ticker_360 as hub


def _first_rows(view):
    """Exact hub precedence, including first case-insensitive match and nulls."""
    index = {}
    # Direct-map precedence comes before any list, even a by_ticker list.
    for key in ("by_ticker", "tickers"):
        value = view.get(key)
        if isinstance(value, dict):
            for ticker, row in value.items():
                index.setdefault(str(ticker).upper(), row)
    for key in hub._TICKER_LIST_KEYS:
        value = view.get(key)
        if isinstance(value, list):
            for row in value:
                index.setdefault(hub._tk(row), row)
        elif isinstance(value, dict):
            for ticker, row in value.items():
                index.setdefault(str(ticker).upper(), row)
    return index


def _prepare(s3, domain, spec, cache):
    out = {"domain": domain, "available": False, "as_of": None,
           "ticker_data": None, "summary": {}, "packet_available": False,
           "source_key": spec.get("key"), "generated_at": None,
           "reported_as_of": None, "context_status": "not_evaluated",
           "research_context": None, "raw_fallback_used": False,
           "observation_freshness_verified": False,
           "independent_investment_votes": 0,
           "calls_eligible": False, "sizing_eligible": False,
           "execution_eligible": False, "forecast_qualified": False}
    try:
        packet = hub._read_packet(s3, spec["key"], cache, spec.get("fallback_key"))
        if not isinstance(packet, dict):
            out["reason"] = "packet missing/unreadable"
            return out, None
        out["packet_available"] = True
        out["configured_source_key"] = spec["key"]
        fallback = cache.get(spec["key"]) is None and bool(spec.get("fallback_key"))
        out["fallback_source_used"] = fallback
        out["source_key"] = spec["fallback_key"] if fallback else spec["key"]
        out["generated_at"] = packet.get("generated_at") if isinstance(packet.get("generated_at"), str) else None
        out["reported_as_of"] = packet.get("as_of") if isinstance(packet.get("as_of"), str) else None
        out["as_of"] = out["reported_as_of"]
        if spec["kind"] == "tkr":
            out["context_status"] = "unqualified_raw_inventory"
            if isinstance(packet.get("tickers"), dict):
                values = packet["tickers"].items()
            elif isinstance(packet.get("by_ticker"), dict):
                values = packet["by_ticker"].items()
            else:
                values = ((key, row) for key, row in packet.items() if isinstance(row, dict))
            rows = {}
            for ticker, row in values:
                rows.setdefault(str(ticker).upper(), row)
            return out, rows
        ctx = spec.get("context")
        if ctx:
            try:
                view = hub.CONTEXT_LOADERS[ctx](packet)
            except Exception:
                out["context_status"] = "unavailable"
                out["reason"] = "registered context failed"
                return out, None
            if not isinstance(view, dict):
                out["context_status"] = "invalid"
                out["reason"] = "registered context returned a non-object"
                return out, None
            out["context_status"] = "applied"
            research = view if ctx in hub.CONTEXT_FUNCTIONS else view.get("research_context")
            out["research_context"] = research if isinstance(research, dict) else None
        else:
            view = packet
            out["context_status"] = "unqualified_raw_inventory"
        out["summary"] = {key: view.get(key) for key in ("call", "score", "signal", "state") if key in view}
        rows = _first_rows(view)
        out["available"] = True
        return out, rows
    except Exception as exc:
        out["reason"] = "domain unavailable: " + type(exc).__name__
        return out, None


def iter_enriched(tickers, s3, cache=None):
    """Yield (original ticker, enriched view) using one preparation per domain."""
    cache = {} if cache is None else cache
    prepared = [(domain, spec, *_prepare(s3, domain, spec, cache))
                for domain, spec in hub.SOURCES.items() if spec]
    for original in tickers:
        try:
            ticker = str(original or "").upper().strip()
            domains = {}
            for domain, spec, template, rows in prepared:
                out = dict(template)
                if rows is not None:
                    data = rows.get(ticker)
                    if spec["kind"] == "tkr":
                        if data is None:
                            out["reason"] = "ticker not in artifact"
                        else:
                            out["available"] = True
                            out["ticker_data"] = data
                    else:
                        out["ticker_data"] = data
                        out["ticker_covered"] = data is not None or domain in hub.MARKET_WIDE
                        if data is None and domain in hub.MARKET_WIDE:
                            out["ticker_data"] = {"market_wide": True, "summary": out["summary"]}
                domains[domain] = out
            yield original, {"ticker": ticker,
                             "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                             "domains": domains, "confluence": hub._confluence(domains)}
        except Exception:
            # Preserve the publisher's per-ticker failure boundary.
            continue
