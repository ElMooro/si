"""Resolve published IDs before considering on-demand market banking."""
import gzip
import hashlib
import json
import math
import re
import time
from datetime import datetime, timezone

# 2026-11-10 (Khalid: "SPY is delayed almost 24 hours ... I want all the stocks and ETFs data to be accurate"): every bank is
# end-of-day, so during the session the last daily bar was yesterday's close.  justhodl-polygon-today writes the forming
# day bar of every US stock / ETF from ONE Polygon all-tickers snapshot every 5 minutes (15-minute delayed on the Stocks
# Starter plan); a daily read appends it as the LAST bar when the bank's last session is older.  Read once a minute per
# resident process; never applied over a bank that already holds the session (the nightly append and the 03:10 UTC fold).
TODAY_KEY = "data/warm/polygon-full/today.json.gz"
TODAY_TTL_S = 60
TODAY_US_NS = {"US", "NASDAQ", "NYSE", "AMEX", "NYSEARCA", "ARCA", "BATS", "CBOE"}
_TODAY = {"at": 0.0, "doc": None, "modified": None}


def today_overlay(s3, bucket, now=None):
    """The forming-session overlay, cached TODAY_TTL_S seconds: {session, t, as_of, delay_minutes, rows{T:[o,h,l,c,v,vw,updated_ms]}} or None."""
    now = time.time() if now is None else now
    if _TODAY["doc"] is not None and now - _TODAY["at"] < TODAY_TTL_S:
        return _TODAY["doc"]
    if _TODAY["doc"] is None and now - _TODAY["at"] < TODAY_TTL_S:
        return None                                                     # a miss is cached too: no S3 call per request
    doc = None
    try:
        obj = s3.get_object(Bucket=bucket, Key=TODAY_KEY)
        raw = obj["Body"].read()
        doc = json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
        if not isinstance(doc, dict) or not isinstance(doc.get("rows"), dict) or not doc.get("session") or not doc.get("t"):
            doc = None
        else:
            _TODAY["modified"] = obj.get("LastModified")
    except Exception:  # noqa: BLE001 - missing overlay, access denied, bad gzip: the bank is served as banked
        doc = None
    _TODAY["at"], _TODAY["doc"] = now, doc
    return doc


def us_ticker(symbol, banks):
    """The bare US ticker an overlay row is keyed by, or None for crypto / FX / indices / foreign venues."""
    symbol = str(symbol).upper()
    if ":" in symbol:
        ns, _, rest = symbol.partition(":")
        return rest if ns in TODAY_US_NS and re.fullmatch(r"[A-Z0-9.\-]{1,12}", rest) else None
    if symbol.startswith("^") or symbol.endswith("=X") or symbol.endswith("-USD") or symbol.startswith("X:"):
        return None
    return symbol if re.fullmatch(r"[A-Z0-9.\-]{1,12}", symbol) and any(b.startswith("US:") for b in banks) else None


def with_forming_bar(result, symbol, banks, overlay, span, mult):
    """Append the overlay's forming day bar to a daily bank result when the bank's last session is older. Pure."""
    if not overlay or span != "day" or mult != 1 or not result or result.get("warehouse_empty") or result.get("source_span") != "day" or int(result.get("source_mult") or 1) != 1:
        return result
    t = us_ticker(symbol, banks)
    row = (overlay.get("rows") or {}).get(t) if t else None
    if not row or len(row) < 5 or any(v is None for v in row[:4]):
        return result
    bars = result.get("bars") or []
    try:
        session_day = datetime.fromisoformat(str(overlay["session"])).date()
        t_open = int(overlay["t"])
    except (TypeError, ValueError):
        return result
    if bars:
        last_day = datetime.fromtimestamp(int(bars[-1][0]), tz=timezone.utc).date()
        if last_day >= session_day:
            return result                                               # the bank already holds this session
    vol = row[4] if len(row) > 4 and row[4] is not None and math.isfinite(float(row[4])) else None
    result = dict(result)
    result["bars"] = bars + [[t_open, float(row[0]), float(row[1]), float(row[2]), float(row[3]), vol]]
    result["bar_sources"] = list(result.get("bar_sources") or []) + ["polygon-snapshot:forming-day (%s-minute delayed)" % overlay.get("delay_minutes", 15)]
    result["forming_bar"] = {"session": overlay["session"], "as_of": overlay.get("as_of"), "delay_minutes": overlay.get("delay_minutes", 15), "updated_ms": row[6] if len(row) > 6 else None, "vwap": row[5] if len(row) > 5 else None}
    return result


def banked_ohlc(s3, bucket, symbol, span="day", mult=1):
    """A daily read of a US stock / ETF carries today's forming bar from the snapshot overlay (see with_forming_bar)."""
    result = _banked_ohlc(s3, bucket, symbol, span, mult)
    if span == "day" and mult == 1 and not result.get("warehouse_empty"):
        banks = _banks_for(str(symbol).upper())
        overlay = today_overlay(s3, bucket)
        return with_forming_bar(result, symbol, banks, overlay, span, mult)
    return result


def _banks_for(symbol):
    aliases = {"^VIX":["I:VIX","TVC:VIX"],"^GSPC":["I:SPX"],"^NDX":["I:NDX"],"^DJI":["I:DJI"],
               "DX-Y.NYB":["TVC:DXY"],"GC=F":["TVC:GOLD"],"CL=F":["TVC:USOIL"]}
    if ":" in symbol:
        return [symbol]
    if symbol in aliases:
        return aliases[symbol]
    if symbol.endswith("=X"):
        return [ex+":"+symbol[:-2] for ex in ("C","FX","OANDA")]
    if symbol.endswith("-USD"):
        return [ex+":"+symbol.replace("-", "") for ex in ("X","COINBASE","BITSTAMP")]
    return [ex+":"+symbol for ex in ("US","NASDAQ","NYSE","AMEX","ARCA","BATS","CBOE","OTC")]


def _banked_ohlc(s3, bucket, symbol, span="day", mult=1):
    """Authenticated S3 existence check. No vendor, index build, or write.

    Public S3 cannot distinguish a missing key from AccessDenied. Here only
    an authenticated NoSuchKey/404 is accepted as an empty warehouse bank.
    """
    symbol = str(symbol).upper()
    if not re.fullmatch(r"[A-Z0-9_.=^:!\-]{1,40}", symbol):
        raise ValueError("Invalid warehouse symbol")
    rank = {"minute":1,"hour":2,"day":3,"week":4,"month":5}
    if span not in rank or not 1 <= mult <= 60:
        raise ValueError("Invalid span or multiplier")
    aliases = {"^VIX":["I:VIX","TVC:VIX"],"^GSPC":["I:SPX"],"^NDX":["I:NDX"],"^DJI":["I:DJI"],
               "DX-Y.NYB":["TVC:DXY"],"GC=F":["TVC:GOLD"],"CL=F":["TVC:USOIL"]}
    if ":" in symbol:
        banks = [symbol]
    elif symbol in aliases:
        banks = aliases[symbol]
    elif symbol.endswith("=X"):
        banks = [ex+":"+symbol[:-2] for ex in ("C","FX","OANDA")]
    elif symbol.endswith("-USD"):
        banks = [ex+":"+symbol.replace("-", "") for ex in ("X","COINBASE","BITSTAMP")]
    else:
        banks = [ex+":"+symbol for ex in ("US","NASDAQ","NYSE","AMEX","ARCA","BATS","CBOE","OTC")]
    keys = []
    crypto = re.fullmatch(r"(?:X:)?([A-Z0-9]+)-?USD",symbol) if (symbol.startswith("X:") or symbol.endswith("-USD")) else None
    if crypto:
        keys.append("data/warm/katlin/crypto-bars/"+crypto.group(1)+".json.gz")
    if re.fullmatch(r"[A-Z0-9.\-]+", symbol):
        keys.extend("data/warm/us-equities-daily/"+symbol+ext for ext in (".json.gz",".json"))
    keys.extend("data/warm/tv-bars/universe/"+re.sub(r"[^A-Za-z0-9_.\-!]", "__", s)+".json.gz" for s in banks)
    for sid in dict.fromkeys([symbol]+banks+["tv:"+s for s in banks]):
        h = hashlib.sha1(sid.encode()).hexdigest()
        keys.append("data/series-cache/"+h[:2]+"/"+h+".json")
    for key in keys:
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
        except Exception as exc:
            if getattr(exc,"response",{}).get("Error",{}).get("Code") in ("404","NoSuchKey"):
                continue
            raise
        raw = obj["Body"].read()
        doc = json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
        if not isinstance(doc,dict):
            continue
        source_span = doc.get("span") or {"D":"day","W":"week","M":"month"}.get(doc.get("freq"),"day")
        source_mult = int(doc.get("mult") or 1)
        if (source_span not in rank or rank[span] < rank[source_span]
                or (span == source_span and (mult < source_mult or mult % source_mult))
                or (source_span == "week" and span == "month")
                or (source_mult > 1 and source_span != span)):
            continue
        bars, labels = [], []
        original_labels = doc.get("bar_sources") or []
        source_rows = doc.get("bars") or doc.get("ohlc") or doc.get("rows") or []
        for index, row in enumerate(source_rows):
            try:
                a = row if isinstance(row,list) else [row.get("time",row.get("date",row.get("t"))),
                    row.get("open",row.get("o")),row.get("high",row.get("h")),row.get("low",row.get("l")),row.get("close",row.get("c")),row.get("volume",row.get("value",row.get("v"))) ]
                prices = [float(v) for v in a[1:5]]
                if len(prices) != 4 or not all(math.isfinite(v) for v in prices):
                    continue
                if isinstance(a[0],(int,float)):
                    if not math.isfinite(a[0]): continue
                    ts = a[0]/1000 if a[0] > 1e12 else a[0]
                else:
                    dt = datetime.fromisoformat(str(a[0]).replace("Z","+00:00"))
                    ts = dt.replace(tzinfo=dt.tzinfo or timezone.utc).timestamp()
                volume = float(a[5]) if len(a)>5 and a[5] is not None else None
                bars.append([int(ts)]+prices+[volume if volume is not None and math.isfinite(volume) else None])
                labels.append(original_labels[index] if isinstance(original_labels,list) and len(original_labels)==len(source_rows) and isinstance(original_labels[index],str) else "legacy:unverified")
            except (TypeError,ValueError,IndexError):
                continue
        if bars:
            return {"warehouse_empty":False,"source":"warehouse","warehouse_key":key,
                    "last_modified":obj["LastModified"].isoformat(),"bars":bars,
                    "source_span":source_span,"source_mult":source_mult,"vendor_requests":0,
                    "market_history_quality":doc.get("market_history_quality"),
                    "bar_sources":labels}
    return {"warehouse_empty":True,"bars":[],"vendor_requests":0}


def resolved_id(sym, read, extra=None):
    from reviewed_symbol_config import reviewed_resolver
    resolver = reviewed_resolver()
    symbol_map = read("data/symbol-map.json")
    if not isinstance(symbol_map, dict) or not isinstance(symbol_map.get("map"), dict):
        raise ValueError("Runtime symbol map unavailable; refusing TV fallback")
    if sym in resolver.get("licensed_econ_skip", []):
        return "SKIP"
    exact = resolver.get("exact", {}).get(sym, {})
    mapped = symbol_map.get("map", {}).get(sym, {})
    if exact.get("id"):
        identity, engine = exact["id"], exact.get("engine")
    elif mapped.get("id"):
        identity, engine = mapped["id"], str(mapped.get("source", "")).lower()
    else:
        prefix, _, rest = sym.partition(":")
        p = resolver.get("prefix", {}).get(prefix, {})
        if p.get("strip"):
            return rest
        if p.get("engine") == "fred":
            return "fred:" + rest
        if p.get("engine") == "yahoo" and len(rest) == 6 and rest.isalpha():
            return "C:" + rest
        ex = extra or {}
        if ex.get("src") and ex.get("sid"):
            source = str(ex["src"]).lower()
            identity = str(ex["sid"])
            return identity if ":" in identity else source + ":" + identity
        return None
    if engine == "fred":
        return "fred:" + str(identity).split(":")[-1]
    if engine == "yahoo":
        if identity.startswith("^"):
            return "I:" + {"^GSPC":"SPX", "^IXIC":"COMP"}.get(identity, identity[1:])
        if identity.endswith("=X"):
            return "C:" + identity[:-2]
        if identity.endswith("-USD"):
            return "X:" + identity.replace("-", "")
    return str(identity)
