"""FMP harvest book — S3 first, live API never for names already harvested.

data/fmp-ratios.json is the Ultimate EOD/TTM tape (ops 5576). 238 engines
hitting financialmodelingprep.com for AAPL is why the hour opens with 429s.
Read this book first. Fall through to fmp_stable.fmp_get only on a miss.

Never logs the key. Bundle via deploy-lambdas (aws/shared).
"""
import json
import time

import boto3

_S3 = boto3.client("s3", region_name="us-east-1")
_BUCKET = "justhodl-dashboard-live"
_KEY = "data/fmp-ratios.json"
_CACHE = {"t": 0.0, "book": None}

HARVEST_PATHS = {
    "ratios-ttm", "ratios", "key-metrics-ttm", "key-metrics",
    "income-statement", "income-statement-ttm",
}


def _load():
    if _CACHE["book"] is not None and time.time() - _CACHE["t"] < 300:
        return _CACHE["book"]
    try:
        d = json.loads(_S3.get_object(Bucket=_BUCKET, Key=_KEY)["Body"].read())
        _CACHE["book"] = d if isinstance(d, dict) else {}
        _CACHE["t"] = time.time()
    except Exception:
        _CACHE["book"] = {}
        _CACHE["t"] = time.time()
    return _CACHE["book"]


def _norm(tk):
    return str(tk or "").upper().replace(".", "-").strip()


def row(ticker):
    """Return the harvest row or None. Never raises."""
    book = _load()
    tickers = book.get("tickers") or {}
    if not isinstance(tickers, dict):
        return None
    tk = _norm(ticker)
    r = tickers.get(tk) or tickers.get(tk.replace("-", "."))
    if isinstance(r, dict) and not r.get("error"):
        return r
    return None


def as_of():
    return (_load() or {}).get("generated_at")


def key_status():
    return ((_load() or {}).get("key_status") or {}).get("status")


def as_ratios(r):
    """FMP /stable/ratios-ttm shaped list-of-one from the harvest row."""
    if not r:
        return None
    rec = {
        "symbol": r.get("ticker"),
        "peRatioTTM": r.get("pe"),
        "pegRatioTTM": r.get("peg"),
        "evToEBITDATTM": r.get("ev_ebitda"),
        "returnOnEquityTTM": r.get("roe"),
        "returnOnCapitalEmployedTTM": r.get("roic"),
        "currentRatioTTM": r.get("current"),
        "netDebtToEBITDATTM": r.get("net_debt_ebitda"),
        "freeCashFlowYieldTTM": r.get("fcf_yield"),
        "pe": r.get("pe"),
        "peg": r.get("peg"),
        "ev_ebitda": r.get("ev_ebitda"),
        "roe": r.get("roe"),
        "roic": r.get("roic"),
        "current": r.get("current"),
        "net_debt_ebitda": r.get("net_debt_ebitda"),
        "fcf_yield": r.get("fcf_yield"),
        "cash_conv": r.get("cash_conv"),
        "source": "FMP EOD/TTM harvest",
        "cadence": r.get("cadence") or "EOD_TTM",
    }
    return [rec]


def as_income(r):
    """FMP income-statement shaped list from harvest financials[]."""
    fins = (r or {}).get("financials") or []
    if not fins:
        return None
    out = []
    for y in fins:
        out.append({
            "calendarYear": y.get("year"),
            "date": str(y.get("year") or ""),
            "revenue": y.get("rev") if y.get("rev") is not None else y.get("revenue"),
            "grossProfit": y.get("gp"),
            "operatingIncome": y.get("ebit"),
            "netIncome": y.get("ni"),
            "eps": y.get("eps"),
            "epsdiluted": y.get("eps"),
            "operatingCashFlow": y.get("ocf"),
            "freeCashFlow": y.get("fcf"),
            "weightedAverageShsOutDil": y.get("shares"),
            "source": "FMP FILING harvest",
        })
    return out


def symbol_from(path, qs=""):
    for part in str(qs or "").split("&"):
        if part.lower().startswith("symbol="):
            return part.split("=", 1)[1].split("&")[0].upper()
    p = (path or "").lstrip("/")
    if "/" in p:
        tail = p.split("/")[-1]
        if tail and tail.upper() == tail.replace(".", "-").upper() and len(tail) <= 8:
            return tail.upper()
    return None


def path_head(path):
    p = (path or "").lstrip("/")
    return p.split("/")[0].split("?")[0]


def _wants_history(head, qs):
    """A quarter or a deep limit asks for history the TTM harvest cannot honestly answer (audit 2026-10-23: the book
    was answering period=quarter with one TTM row)."""
    q = str(qs or "").lower()
    if "period=quarter" in q or "period=q" in q:
        return True
    for part in q.split("&"):
        if part.startswith("limit="):
            try:
                return int(part.split("=", 1)[1]) > 3 and head not in ("ratios-ttm", "key-metrics-ttm", "income-statement-ttm")
            except ValueError:
                return False
    return False


def try_harvest(path, qs=""):
    """Return FMP-shaped payload for this path+symbol: the FMP bank (full history, verbatim bulk rows) first, then the
    TTM harvest for the single-row shapes it can honestly answer, else None (caller goes live)."""
    head = path_head(path)
    try:
        import fmp_bank
        hit = fmp_bank.try_bank(head, qs if "symbol=" in str(qs or "") else "symbol=%s&%s" % (symbol_from(path, qs) or "", qs or ""))
        if hit:
            return hit
    except Exception:
        pass
    if head not in HARVEST_PATHS:
        return None
    tk = symbol_from(path, qs)
    if not tk:
        return None
    if _wants_history(head, qs):
        return None
    r = row(tk)
    if not r:
        return None
    if head in ("income-statement", "income-statement-ttm"):
        return as_income(r)
    return as_ratios(r)
