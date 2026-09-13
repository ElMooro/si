"""Resolve published IDs before considering on-demand market banking."""
import gzip
import hashlib
import json
import math
import re
from datetime import datetime, timezone


def banked_ohlc(s3, bucket, symbol, span="day", mult=1):
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
        bars = []
        for row in doc.get("bars") or doc.get("ohlc") or doc.get("rows") or []:
            try:
                a = row if isinstance(row,list) else [row.get("time",row.get("date",row.get("t"))),
                    row.get("open",row.get("o")),row.get("high",row.get("h")),row.get("low",row.get("l")),row.get("close",row.get("c")),row.get("volume",row.get("value",row.get("v",0))) ]
                prices = [float(v) for v in a[1:5]]
                if len(prices) != 4 or not all(math.isfinite(v) for v in prices):
                    continue
                if isinstance(a[0],(int,float)):
                    if not math.isfinite(a[0]): continue
                    ts = a[0]/1000 if a[0] > 1e12 else a[0]
                else:
                    dt = datetime.fromisoformat(str(a[0]).replace("Z","+00:00"))
                    ts = dt.replace(tzinfo=dt.tzinfo or timezone.utc).timestamp()
                volume = float(a[5]) if len(a)>5 and a[5] is not None else 0
                bars.append([int(ts)]+prices+[volume if math.isfinite(volume) else 0])
            except (TypeError,ValueError,IndexError):
                continue
        if bars:
            return {"warehouse_empty":False,"source":"warehouse","warehouse_key":key,
                    "last_modified":obj["LastModified"].isoformat(),"bars":bars,
                    "source_span":source_span,"source_mult":source_mult,"vendor_requests":0}
    return {"warehouse_empty":True,"bars":[],"vendor_requests":0}


def resolved_id(sym, read, extra=None):
    resolver = read("data/tv-symbol-resolver.json")
    symbol_map = read("data/symbol-map.json")
    if not isinstance(resolver, dict) or not isinstance(symbol_map, dict):
        raise ValueError("Symbol resolution unavailable; refusing TV fallback")
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
