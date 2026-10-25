"""justhodl-tv-bars — TradingView chart history, server-side.

Khalid: "TradingView has all ICE data since inception — I can see it in
my account." True, and the reason the vault never held it is that TV
streams CHART BARS over a WebSocket (wss://data.tradingview.com), while
justhodl-tradingview only ever RESOLVED symbols to other providers
(FRED/Yahoo/ECB) — so for ICE it re-fetched the same truncated window.

The browser-extension route worked but required Khalid to reload an
unpacked extension by hand, which violates the autonomy contract. This
engine does it with no browser at all: the TV session cookie is ALREADY
in SSM (used by justhodl-tv-notes-crawler), so we speak TV's own socket
protocol directly and pull full history in cursored tranches.

Protocol (observed, not guessed — every frame is length-prefixed
"~m~<len>~m~<json>"):
  set_auth_token -> chart_create_session -> resolve_symbol ->
  create_series(countback) -> read timescale_update frames -> bank.

Worklist: the ICE ids whose banked docs carry a history_gap marker
(2017-2023) plus any explicitly requested. Bars land append-only in
data/warm/tv-bars/; merging into banked FRED docs stays a separate
audited ops pass — a puller never rewrites history unattended.
"""
import base64
import gzip
import json
import os
import random
import re
import socket
import ssl
import string
import struct
import time
from datetime import datetime, timezone

import boto3
from market_history_integrity import ProviderDenied, stop_on_denial, validate_upgrade, read_bank, merged_document, valid_bar

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
TRANCHE = int(os.environ.get("TRANCHE", "12"))
s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
STATE_KEY = "data/warm/tv-bars/_state.json"
IDX_KEY = "data/warm/tv-bars/_index.json"
WS_HOST = "data.tradingview.com"


def _p(name, dec=True):
    try:
        return ssm.get_parameter(Name=name,
                                 WithDecryption=dec)["Parameter"]["Value"]
    except Exception:
        return ""


def _rand(n=12):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(n))


class WS:
    """Minimal RFC6455 client — no third-party deps in the Lambda zip."""

    def __init__(self, host, origin, cookie, path=None):
        raw = socket.create_connection((host, 443), timeout=25)
        ctx = ssl.create_default_context()
        self.s = ctx.wrap_socket(raw, server_hostname=host)
        # rev-2: the handshake 400'd because Sec-WebSocket-Key was the
        # static RFC6455 EXAMPLE nonce. Servers that validate the
        # accept-hash reject a reused/known nonce outright. It must be
        # 16 fresh random bytes, base64'd, per connection.
        key = base64.b64encode(os.urandom(16)).decode()
        req = (
            "GET " + (path or "/socket.io/websocket?from=chart%2F"
                              "&type=chart") + " HTTP/1.1\r\n"
            "Host: %s\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: %s\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "Origin: %s\r\n"
            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 "
            "Safari/537.36\r\n"
            "Cookie: %s\r\n\r\n" % (host, key, origin, cookie))
        try:
            self.s.sendall(req.encode())
            buf = b""
            while b"\r\n\r\n" not in buf:
                c = self.s.recv(4096)
                if not c:
                    raise RuntimeError("handshake closed")
                buf += c
                if len(buf) > 65536:
                    raise ValueError("WebSocket upgrade exceeds bound")
            header, self.buf = buf.split(b"\r\n\r\n", 1)
            validate_upgrade(header, key)
        except Exception:
            self.close()
            raise

    def send(self, msg):
        data = msg.encode()
        hdr = bytearray([0x81])
        n = len(data)
        mask = os.urandom(4)
        if n < 126:
            hdr.append(0x80 | n)
        elif n < 65536:
            hdr.append(0x80 | 126)
            hdr += struct.pack(">H", n)
        else:
            hdr.append(0x80 | 127)
            hdr += struct.pack(">Q", n)
        hdr += mask
        self.s.sendall(bytes(hdr) + bytes(b ^ mask[i % 4]
                                          for i, b in enumerate(data)))

    def recv(self, timeout=20):
        self.s.settimeout(timeout)
        while True:
            if len(self.buf) >= 2:
                b0, b1 = self.buf[0], self.buf[1]
                op = b0 & 0x0F
                ln = b1 & 0x7F
                off = 2
                if ln == 126:
                    if len(self.buf) < 4:
                        ln = None
                    else:
                        ln = struct.unpack(">H", self.buf[2:4])[0]
                        off = 4
                elif ln == 127:
                    if len(self.buf) < 10:
                        ln = None
                    else:
                        ln = struct.unpack(">Q", self.buf[2:10])[0]
                        off = 10
                if ln is not None and len(self.buf) >= off + ln:
                    payload = self.buf[off:off + ln]
                    self.buf = self.buf[off + ln:]
                    if op == 0x8:
                        raise RuntimeError("closed by peer")
                    if op == 0x9:            # ping -> pong
                        self.send("")
                        continue
                    return payload.decode("utf-8", "replace")
            chunk = self.s.recv(65536)
            if not chunk:
                raise RuntimeError("socket eof")
            self.buf += chunk

    def close(self):
        try:
            self.s.close()
        except Exception:
            pass


def frame(func, args):
    body = json.dumps({"m": func, "p": args}, separators=(",", ":"))
    return "~m~%d~m~%s" % (len(body), body)


def unframe(raw):
    return re.findall(r"~m~\d+~m~(\{.*?\})(?=~m~|\Z)", raw)


ENDPOINTS = [
    ("data.tradingview.com", "/socket.io/websocket?from=chart%2F"
                             "&type=chart"),
    ("prodata.tradingview.com", "/socket.io/websocket?from=chart%2F"
                                "&type=chart"),
    ("widgetdata.tradingview.com", "/socket.io/websocket?from=widget"
                                   "embed%2F&type=chart"),
    ("data.tradingview.com", "/socket.io/websocket"),
]


def _connect(cookie):
    """rev-2: try each known socket endpoint; report every rejection so
    a future failure is diagnosable instead of a bare 400."""
    errs = []
    for host, path in ENDPOINTS:
        try:
            return WS(host, "https://www.tradingview.com", cookie,
                      path), host
        except Exception as e:
            stop_on_denial(e, "TradingView")
            errs.append("%s%s -> %s" % (host, path[:26], str(e)[:70]))
    raise RuntimeError("all endpoints refused: " + " | ".join(errs))


def pull(symbol, token, cookie, countback=20000, budget=45):
    """One symbol -> [[unix_ts, o, h, l, c], ...] oldest-first."""
    ws, _host = _connect(cookie)
    bars, t0 = {}, time.time()
    try:
        cs, sid = "cs_" + _rand(), "sds_sym_1"
        ws.send(frame("set_auth_token", [token or "unauthorized_user_token"]))
        ws.send(frame("chart_create_session", [cs, ""]))
        ws.send(frame("resolve_symbol", [
            cs, sid,
            '={"symbol":"%s","adjustment":"splits"}' % symbol]))
        ws.send(frame("create_series", [
            cs, "s1", "s1", sid, "1D", countback, ""]))
        done = False
        while not done and time.time() - t0 < budget:
            try:
                raw = ws.recv(timeout=15)
            except Exception as error:
                stop_on_denial(error, "TradingView")
                break
            if raw.startswith("~h~") or "~h~" in raw[:12]:
                ws.send(raw)                     # heartbeat echo
                continue
            for js in unframe(raw):
                try:
                    msg = json.loads(js)
                except Exception:
                    continue
                m = msg.get("m")
                if m in ("timescale_update", "du"):
                    for blk in msg.get("p", []):
                        if not isinstance(blk, dict):
                            continue
                        for v in blk.values():
                            if isinstance(v, dict) and \
                                    isinstance(v.get("s"), list):
                                for row in v["s"]:
                                    a = row.get("v") if isinstance(
                                        row, dict) else None
                                    if a and len(a) >= 5:
                                        candidate = [a[0], a[1], a[2], a[3], a[4], a[5] if len(a)>5 else None]
                                        if not valid_bar(candidate):
                                            raise ValueError("Invalid TradingView market row")
                                        bars[int(a[0])] = candidate[1:]
                elif m in ("series_completed", "critical_error",
                           "symbol_error", "series_error"):
                    if m != "series_completed":
                        raise ProviderDenied("TradingView " + m + "; no alternate provider attempted")
                    done = True
    finally:
        ws.close()
    return [[k] + bars[k] for k in sorted(bars)]


def _gj(key, dflt):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception as error:
        if getattr(error, "response", {}).get("Error", {}).get("Code") in ("404", "NoSuchKey"):
            return dflt
        raise


def build_worklist():
    """ICE ids whose banked docs carry a history_gap, newest first."""
    ids, tok = [], None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": "data/warm/fred-scoped/",
              "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents") or []:
            bn = o["Key"].rsplit("/", 1)[-1]
            if bn.startswith("BAML") and bn.endswith(".json"):
                ids.append(bn[:-5])
        if not resp.get("IsTruncated"):
            break
        tok = resp.get("NextContinuationToken")
    return sorted(set(ids))


UNIV_IDX_KEY = "data/warm/tv-bars/universe/_index.json"


def safe_name(sym):
    return re.sub(r"[^A-Za-z0-9_.\-!]", "__", sym)


Y_SUFFIX = {"SSE": ".SS", "SZSE": ".SZ", "NSE": ".NS", "BSE": ".BO", "TSE": ".T", "HKEX": ".HK", "LSE": ".L", "XETR": ".DE", "FWB": ".DE",
            "TSX": ".TO", "TSXV": ".V", "ASX": ".AX", "SGX": ".SI", "KRX": ".KS", "TWSE": ".TW", "MIL": ".MI", "BME": ".MC", "SIX": ".SW",
            "OMXSTO": ".ST", "OMXCOP": ".CO", "OMXHEX": ".HE", "OSL": ".OL", "MOEX": ".ME", "BMFBOVESPA": ".SA", "BVMF": ".SA", "BMV": ".MX",
            "JSE": ".JO", "TADAWUL": ".SR", "EURONEXT": ".PA", "NZX": ".NZ", "IDX": ".JK", "SET": ".BK", "PSE": ".PS", "TASE": ".TA",
            "WSE": ".WA", "BIST": ".IS", "VIE": ".VI", "EGX": ".CA", "BCBA": ".BA", "BVL": ".LM", "BCS": ".SN", "BVC": ".CL"}
Y_INDEX = {"TVC:VIX": "^VIX", "CBOE:VIX": "^VIX", "TVC:SPX": "^GSPC", "SP:SPX": "^GSPC", "TVC:NDX": "^NDX", "NASDAQ:NDX": "^NDX", "TVC:DJI": "^DJI",
           "DJ:DJI": "^DJI", "TVC:RUT": "^RUT", "TVC:DXY": "DX-Y.NYB", "TVC:US10Y": "^TNX", "TVC:US30Y": "^TYX", "TVC:US05Y": "^FVX", "TVC:US13W": "^IRX",
           "TVC:GOLD": "GC=F", "TVC:SILVER": "SI=F", "TVC:USOIL": "CL=F", "TVC:UKOIL": "BZ=F", "TVC:NI225": "^N225", "TVC:HSI": "^HSI", "HSI:HSI": "^HSI",
           "FTSE:UKX": "^FTSE", "XETR:DAX": "^GDAXI", "TVC:DEU40": "^GDAXI", "TVC:CAC40": "^FCHI", "TVC:STOXX50E": "^STOXX50E", "TVC:SX5E": "^STOXX50E",
           "TVC:KOSPI": "^KS11", "TVC:SENSEX": "^BSESN", "TVC:NIFTY": "^NSEI", "TVC:SSEC": "000001.SS", "SSE:000001": "000001.SS", "TVC:SHCOMP": "000001.SS",
           "TVC:MOVE": "^MOVE", "TVC:VXN": "^VXN", "CBOE:VXN": "^VXN", "TVC:SPGSCI": "^SPGSCI", "TVC:BDI": "^BDIY", "INDEX:BTCUSD": "BTC-USD", "CRYPTOCAP:BTC": "BTC-USD",
           "NASDAQ:NDX100": "^NDX", "TVC:RUI": "^RUI", "TVC:SP500": "^GSPC", "AMEX:SPY": "SPY", "TVC:NYA": "^NYA", "TVC:XAU": "^XAU", "TVC:HUI": "^HUI"}
Y_FUT = {"ES": "ES=F", "NQ": "NQ=F", "YM": "YM=F", "RTY": "RTY=F", "ZN": "ZN=F", "ZB": "ZB=F", "ZF": "ZF=F", "ZT": "ZT=F", "GC": "GC=F", "SI": "SI=F",
         "HG": "HG=F", "CL": "CL=F", "NG": "NG=F", "BZ": "BZ=F", "ZC": "ZC=F", "ZS": "ZS=F", "ZW": "ZW=F", "6E": "6E=F", "6J": "6J=F", "6B": "6B=F",
         "DX": "DX=F", "VX": "^VIX", "PL": "PL=F", "PA": "PA=F", "KC": "KC=F", "SB": "SB=F", "CC": "CC=F", "CT": "CT=F", "LE": "LE=F", "HE": "HE=F"}
_FEED = {}


def tv_to_yahoo(sym):
    """TradingView EXCHANGE:SYMBOL -> Yahoo symbol candidates (the fleet's symbol-feed resolutions first)."""
    if not _FEED:
        try:
            fd = _gj("data/symbol-feed.json", {})
            # `resolved` is a COUNT in symbol-feed.json; the per-symbol Yahoo symbol lives in prices[sym].ysym
            _FEED["r"] = fd.get("resolved") if isinstance(fd.get("resolved"), dict) else {}
            _FEED["p"] = fd.get("prices") if isinstance(fd.get("prices"), dict) else {}
        except Exception:
            # Unknown/read-denied metadata must not activate guessed fallbacks.
            raise
    out = []
    rs = _FEED["r"].get(sym) if isinstance(_FEED.get("r"), dict) else None
    if isinstance(rs, str):
        out.append(rs)
    elif isinstance(rs, dict) and rs.get("ysym"):
        out.append(rs["ysym"])
    pr = _FEED["p"].get(sym) if isinstance(_FEED.get("p"), dict) else None
    if isinstance(pr, dict) and pr.get("ysym"):
        out.append(pr["ysym"])
    if sym in Y_INDEX:
        out.append(Y_INDEX[sym])
    if ":" in sym:
        ex, bare = sym.split(":", 1)
        ex = ex.upper()
        # symdir's deterministic bank keys for bare tickers and Polygon-style ids
        if ex == "US":
            out.append(bare.replace(".", "-").replace("/", "-"))
        elif ex == "X":
            m = re.match(r"^([A-Z0-9]{2,10})(USDT|USDC|USD)$", bare)
            out.append((m.group(1) + "-USD") if m else bare)
        elif ex == "C":
            out.append(bare + "=X")
        elif ex == "I":
            out.append({"SPX": "^GSPC", "NDX": "^NDX", "DJI": "^DJI", "VIX": "^VIX", "RUT": "^RUT", "NYA": "^NYA", "XAU": "^XAU", "HUI": "^HUI", "SOX": "^SOX",
                        "OEX": "^OEX", "RUI": "^RUI", "TNX": "^TNX", "TYX": "^TYX", "IRX": "^IRX", "FVX": "^FVX", "COMP": "^IXIC", "IXIC": "^IXIC"}.get(bare, "^" + bare))
        if ex in ("NASDAQ", "NYSE", "AMEX", "CBOE", "OTC", "ARCA", "BATS", "NYSEARCA"):
            out.append(bare.replace(".", "-").replace("/", "-"))
        elif ex in ("FX", "FX_IDC", "OANDA", "FOREXCOM", "SAXO", "PEPPERSTONE", "CAPITALCOM", "FXCM", "ICEEUR", "ICEUS") and re.fullmatch(r"[A-Z]{6}", bare):
            out.append(bare + "=X")
        elif ex in ("COINBASE", "BINANCE", "BITSTAMP", "KRAKEN", "CRYPTO", "BITFINEX", "GEMINI", "OKX", "BYBIT", "CRYPTOCAP", "INDEX"):
            m = re.match(r"^([A-Z0-9]{2,10})(USDT|USDC|USD|BUSD)$", bare)
            if m:
                out.append(m.group(1) + "-USD")
        elif ex in ("CME_MINI", "CME", "CBOT", "COMEX", "NYMEX", "CBOT_MINI", "COMEX_MINI", "NYMEX_MINI", "ICE", "ICEEUR", "ICEUS", "EUREX", "CBOE_FUT"):
            m = re.match(r"^([A-Z0-9]{1,4}?)[0-9]?!$", bare) or re.match(r"^([A-Z0-9]{1,4})$", bare)
            if m and m.group(1) in Y_FUT:
                out.append(Y_FUT[m.group(1)])
        elif ex in Y_SUFFIX:
            b = bare
            if ex == "HKEX" and b.isdigit():
                b = b.zfill(4)
            out.append(b + Y_SUFFIX[ex])
        elif ex == "TVC":
            out.append("^" + bare)
        elif ex in ("ECONOMICS", "FRED", "QUANDL"):
            pass
    return list(dict.fromkeys(x for x in out if x))


def yahoo_bars(ysym, rng="max"):
    """Full daily history from Yahoo's chart API (the same endpoint the fleet's symbol-feed uses from Lambda)."""
    import urllib.request
    if rng == "max":
        # range=max answers with MONTHLY bars (5126: AAPL 169 bars since 1984); an explicit epoch window returns daily
        url = ("https://query1.finance.yahoo.com/v8/finance/chart/%s?period1=-2208988800&period2=%d&interval=1d&events=div%%2Csplit"
               % (urllib.request.quote(ysym), int(time.time()) + 86400))
    else:
        url = ("https://query1.finance.yahoo.com/v8/finance/chart/%s?range=%s&interval=1d&events=div%%2Csplit"
               % (urllib.request.quote(ysym), rng))
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=25) as r:
        j = json.loads(r.read().decode("utf-8", "ignore"))
    res = ((j.get("chart") or {}).get("result") or [None])[0]
    if not res:
        raise RuntimeError(str(((j.get("chart") or {}).get("error") or {}).get("description") or "no result")[:80])
    ts = res.get("timestamp") or []
    q = ((res.get("indicators") or {}).get("quote") or [{}])[0]
    rows = []
    for i, t in enumerate(ts):
        try:
            o, h, l, c = q["open"][i], q["high"][i], q["low"][i], q["close"][i]
        except Exception:
            continue
        if c is None or o is None:
            continue
        v = (q.get("volume") or [None] * len(ts))[i]
        candidate = [t, o, h, l, c, v]
        if valid_bar(candidate):
            rows.append(candidate)
    return rows


def bank_symbol(sym, token, cookie, countback=20000, budget=45, ysym=None, refresh=False):
    """v1.1 (ops 5124/5125): bank ANY TradingView symbol (EXCHANGE:SYMBOL) under
    data/warm/tv-bars/universe/{safe}.json.gz -- append-only union with what is
    already banked. Source order: TradingView chart socket (session) when it
    answers, else Yahoo's chart API resolved through the fleet's symbol map
    (full history since inception, volume included). Returns (doc, key)."""
    key = "data/warm/tv-bars/universe/%s.json.gz" % safe_name(sym)
    old = read_bank(s3, BUCKET, key)
    rows, src, err = [], None, []
    if cookie:
        try:
            rows = pull(sym, token, cookie, countback=countback, budget=min(budget, 12))
            src = "tradingview-ws (session-auth, server-side)"
        except Exception as e:
            stop_on_denial(e, "TradingView")
            err.append("tv:" + str(e)[:70])
    if not rows:
        for y in ([ysym] if ysym else []) + tv_to_yahoo(sym):
            try:
                rows = yahoo_bars(y, rng=("6mo" if refresh else "max"))
                if rows:
                    src = "yahoo-chart:%s" % y
                    break
            except Exception as e:
                stop_on_denial(e, "Yahoo")
                err.append("yahoo %s: %s" % (y, str(e)[:60]))
    if not rows:
        raise RuntimeError("no bars for %s (%s)" % (sym, "; ".join(err)[:200] or "no yahoo mapping"))
    doc = merged_document(old, rows, sym, sym, src)
    s3.put_object(Bucket=BUCKET, Key=key, Body=gzip.compress(json.dumps(doc).encode()),
                  ContentType="application/gzip", CacheControl="public, max-age=900")
    return doc, key


def _session():
    session = _p("/justhodl/tradingview/sessionid")
    sign = _p("/justhodl/tradingview/sessionid_sign")
    dev = _p("/justhodl/tradingview/device_t")
    token = _p("/justhodl/tradingview/auth_token") or "unauthorized_user_token"
    if not session:
        return None, None
    cookie = "sessionid=%s" % session
    if sign:
        cookie += "; sessionid_sign=%s" % sign
    if dev:
        cookie += "; device_t=%s" % dev
    return token, cookie


def universe_pull(event, context):
    """mode=pull: on-demand banking for chart-pro (symdir calls this synchronously the first time a
    TradingView symbol is opened). No lease, no catalog state; touches only universe/."""
    token, cookie = _session()
    syms = [str(x) for x in (event.get("tv_symbols") or []) if x][:8]
    ymap = event.get("ysym") or {}
    countback = int(event.get("countback") or 20000)
    out, idx = {}, _gj(UNIV_IDX_KEY, {"symbols": {}})
    for sym in syms:
        if context and context.get_remaining_time_in_millis() < 20000:
            out[sym] = {"ok": False, "error": "budget"}
            break
        try:
            doc, key = bank_symbol(sym, token, cookie, countback=(120 if event.get("refresh") else countback), budget=int(event.get("budget") or 40), ysym=ymap.get(sym), refresh=bool(event.get("refresh")))
            out[sym] = {"ok": True, "key": key, "n": doc["n"], "first": doc["first_date"], "last": doc["last_date"], "source": doc["source"]}
            idx["symbols"][sym] = {"key": key, "n": doc["n"], "first": doc["first_date"], "last": doc["last_date"],
                                   "as_of": doc["as_of"]}
        except Exception as e:
            out[sym] = {"ok": False, "error": "%s: %s" % (type(e).__name__, str(e)[:120])}
            idx.setdefault("failures", {})[sym] = {"err": str(e)[:100], "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            if isinstance(e, ProviderDenied):
                break
        time.sleep(0.4)
    idx["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    idx["n_symbols"] = len(idx["symbols"])
    s3.put_object(Bucket=BUCKET, Key=UNIV_IDX_KEY, Body=json.dumps(idx, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    return {"ok": True, "results": out, "universe": idx["n_symbols"]}


def universe_refresh(event, context):
    """mode=refresh: nightly, fan-out shards over the banked universe with a short countback
    (append-only union) so every opened symbol stays current without re-pulling its history."""
    token, cookie = _session()
    idx = _gj(UNIV_IDX_KEY, {"symbols": {}})
    syms = sorted(idx.get("symbols") or {})
    shard, nshards = int(event.get("shard") or 0), int(event.get("nshards") or 1)
    if event.get("fanout"):
        n = int(event["fanout"])
        lc = boto3.client("lambda", region_name="us-east-1")
        for i in range(n):
            lc.invoke(FunctionName=context.function_name, InvocationType="Event",
                      Payload=json.dumps({"mode": "refresh", "shard": i, "nshards": n}).encode())
        return {"ok": True, "fanned_out": n, "universe": len(syms)}
    import hashlib
    mine = [x for x in syms if int(hashlib.md5(x.encode()).hexdigest(), 16) % nshards == shard]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    done = fail = skipped = 0
    for sym in mine:
        if context and context.get_remaining_time_in_millis() < 25000:
            break
        meta = idx["symbols"].get(sym) or {}
        if (meta.get("as_of") or "")[:10] == today:
            skipped += 1
            continue
        try:
            doc, key = bank_symbol(sym, token, cookie, countback=int(event.get("countback") or 120), budget=25, refresh=True)
            idx["symbols"][sym] = {"key": key, "n": doc["n"], "first": doc["first_date"], "last": doc["last_date"], "as_of": doc["as_of"]}
            done += 1
        except Exception as e:
            idx.setdefault("failures", {})[sym] = {"err": str(e)[:100], "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            fail += 1
            if isinstance(e, ProviderDenied):
                break
        time.sleep(0.4)
    # per-shard write of the shared index: merge with the latest copy to keep other shards' updates
    cur = _gj(UNIV_IDX_KEY, {"symbols": {}})
    cur.setdefault("symbols", {}).update({k: v for k, v in idx["symbols"].items() if k in mine})
    cur["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cur["n_symbols"] = len(cur["symbols"])
    s3.put_object(Bucket=BUCKET, Key=UNIV_IDX_KEY, Body=json.dumps(cur, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    return {"ok": True, "shard": shard, "nshards": nshards, "mine": len(mine), "refreshed": done, "failed": fail, "skipped_today": skipped}


# ---------------------------------------------------------------------------------------------------------------------
# v1.2 (2026-10-21, Khalid: "do Polygon US append"): mode=polygon-append.  TradingView has refused the websocket
# handshake since ~2026-10-12 and this engine's rule is "no fallback on a refusal", so the bank's US names stopped at
# the last session TradingView served (288 of 309 at 2026-10-19).  Polygon (licensed, the fleet's paid tape) appends
# the sessions the bank is missing: ONE grouped-daily call per missing session date (every US stock/ETF in one
# response), split-adjusted, bars stamped at the session open 09:30 America/New_York exactly like the TradingView
# and Yahoo rows already banked, written through merged_document (per-bar producer label "polygon-aggs:daily-adjusted",
# append-only: dates the bank already holds are never touched).  Only completed sessions are banked (today's only
# after 16:15 ET).  Paced to stay inside the Basic plan's 5 calls/minute.
# ---------------------------------------------------------------------------------------------------------------------
POLY_SRC = "polygon-aggs:daily-adjusted"
POLY_US_NS = {"US", "NASDAQ", "NYSE", "AMEX", "NYSEARCA", "BATS", "CBOE"}
POLY_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]{0,5}([.\-][A-Z]{1,2})?$")
POLY_MAX_DATES = int(os.environ.get("POLY_MAX_DATES", "15"))
POLY_CALL_GAP_S = float(os.environ.get("POLY_CALL_GAP_S", "13"))   # <= 4.6 calls/min (Polygon Basic allows 5)


def _ny_offset_seconds(d):
    """UTC offset of America/New_York on date d (zoneinfo when the runtime has tzdata, else the US DST rule)."""
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime as _dt
        return int(_dt(d.year, d.month, d.day, 12, tzinfo=ZoneInfo("America/New_York")).utcoffset().total_seconds())
    except Exception:
        from datetime import date as _date, timedelta as _td
        mar = _date(d.year, 3, 1)
        dst_start = mar + _td(days=(6 - mar.weekday()) % 7 + 7)      # second Sunday of March
        nov = _date(d.year, 11, 1)
        dst_end = nov + _td(days=(6 - nov.weekday()) % 7)            # first Sunday of November
        return -4 * 3600 if dst_start <= d < dst_end else -5 * 3600


def session_open_ts(d):
    """Bank convention: a daily bar is stamped at the regular-session open, 09:30 America/New_York (TradingView/Yahoo rows)."""
    from datetime import datetime as _dt
    return int(_dt(d.year, d.month, d.day, 9, 30, tzinfo=timezone.utc).timestamp()) - _ny_offset_seconds(d)


def ny_now():
    from datetime import timedelta as _td
    now = datetime.now(timezone.utc)
    return now + _td(seconds=_ny_offset_seconds(now.date()))


def last_completed_session(now_ny=None):
    """Most recent weekday whose regular session has closed (16:00 ET + 15 min settle). Holidays are detected by an empty
    grouped-daily answer, not by a calendar."""
    from datetime import timedelta as _td
    n = now_ny or ny_now()
    d = n.date()
    if n.hour * 60 + n.minute < 16 * 60 + 15:
        d = d - _td(days=1)
    while d.weekday() >= 5:
        d = d - _td(days=1)
    return d


def polygon_grouped(day, key):
    """Every US stock/ETF daily bar for one date: {ticker: [ts, o, h, l, c, v]} (split-adjusted). [] on a holiday."""
    import urllib.request
    url = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/%s?adjusted=true&apiKey=%s" % (day.isoformat(), key)
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-tv-bars/1.2"})
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            doc = json.loads(r.read(64 * 1024 * 1024).decode("utf-8", "ignore"))
    except Exception as e:
        stop_on_denial(e, "Polygon")
        raise
    if doc.get("status") not in ("OK", "DELAYED"):
        raise RuntimeError("polygon grouped %s: status %s" % (day, doc.get("status")))
    ts = session_open_ts(day)
    out = {}
    for row in doc.get("results") or []:
        t = row.get("T")
        if not isinstance(t, str):
            continue
        cand = [ts, row.get("o"), row.get("h"), row.get("l"), row.get("c"), row.get("v")]
        if valid_bar(cand):
            out[t.upper()] = cand
    return out


def poly_ticker(sym):
    """'US:BRK.B' / 'NYSE:BRK.B' -> 'BRK.B'; None for anything that is not a plain US listing (futures, funds, FX...)."""
    if ":" not in str(sym):
        return None
    ns, tk = str(sym).split(":", 1)
    tk = tk.strip().upper()
    if ns.upper() not in POLY_US_NS or not POLY_TICKER_RE.match(tk):
        return None
    return tk


def polygon_append(event, context):
    """mode=polygon-append: append the sessions missing from each banked US name from Polygon grouped daily aggregates."""
    from datetime import date as _date, timedelta as _td
    from managed_secret import managed_secret
    key = managed_secret(("POLYGON_KEY", "POLYGON_API_KEY", "POLY_KEY"), ("/justhodl/polygon/api-key",))
    if not key:
        raise RuntimeError("no Polygon key (env POLYGON_KEY or SSM /justhodl/polygon/api-key)")
    idx = _gj(UNIV_IDX_KEY, {"symbols": {}})
    syms = idx.get("symbols") or {}
    last_ok = last_completed_session()
    if event.get("through"):
        last_ok = min(last_ok, _date.fromisoformat(event["through"]))
    # which names need which dates; a name whose bank ends before the window (POLY_MAX_DATES sessions) is left for a
    # wider pull or TradingView rather than given a hole, and never drives daily full-window fetches
    window_start = last_ok
    n_sessions = 1
    while n_sessions < POLY_MAX_DATES:
        window_start = window_start - _td(days=1)
        if window_start.weekday() < 5:
            n_sessions += 1
    todo, too_old = {}, 0
    for sym, meta in syms.items():
        tk = poly_ticker(sym)
        if not tk or not isinstance(meta, dict) or not meta.get("key"):
            continue
        last = (meta.get("last") or "")[:10]
        try:
            last_d = _date.fromisoformat(last) if last else None
        except ValueError:
            last_d = None
        if last_d is not None and last_d >= last_ok:
            continue
        if last_d is None or last_d < window_start - _td(days=3):
            too_old += 1
            continue
        todo[sym] = (tk, last_d)
    if event.get("symbols"):
        todo = {k: v for k, v in todo.items() if k in set(event["symbols"])}
    if not todo:
        return {"ok": True, "mode": "polygon-append", "through": last_ok.isoformat(), "symbols": 0, "dates": [], "gap_older_than_window": too_old, "note": "every US name inside the window is current"}
    oldest = min(v[1] for v in todo.values())
    dates = []
    d = last_ok
    while d > oldest and len(dates) < POLY_MAX_DATES:
        if d.weekday() < 5:
            dates.append(d)
        d = d - _td(days=1)
    dates.sort()
    grouped, calls, holidays = {}, 0, []
    t_last = 0.0
    for day in dates:
        if context and context.get_remaining_time_in_millis() < 120000:
            break
        wait = t_last + POLY_CALL_GAP_S - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        t_last = time.monotonic()
        rows = polygon_grouped(day, key)
        calls += 1
        if rows:
            grouped[day] = rows
        else:
            holidays.append(day.isoformat())
    done = fail = skipped = 0
    appended_rows = 0
    per_sym = {}
    first_fetched = min(dates) if dates else None
    for sym, (tk, last_d) in sorted(todo.items()):
        if context and context.get_remaining_time_in_millis() < 25000:
            break
        # never leave a hole: the fetched window must reach the first session after the bank's last bar
        nxt = last_d + _td(days=1)
        while nxt.weekday() >= 5:
            nxt = nxt + _td(days=1)
        if first_fetched is None or nxt < first_fetched:
            too_old += 1
            continue
        rows = [grouped[day][tk] for day in sorted(grouped) if day > last_d and tk in grouped[day]]
        if not rows:
            skipped += 1
            continue
        try:
            key_s3 = syms[sym]["key"]
            old = read_bank(s3, BUCKET, key_s3)
            if not old:
                skipped += 1
                continue
            have = {r[0] for r in old.get("bars", [])}
            rows = [r for r in rows if r[0] not in have]
            if not rows:
                skipped += 1
                continue
            doc = merged_document(old, rows, old.get("symbol") or sym, old.get("tv_symbol") or sym, POLY_SRC)
            s3.put_object(Bucket=BUCKET, Key=key_s3, Body=gzip.compress(json.dumps(doc).encode()),
                          ContentType="application/gzip", CacheControl="public, max-age=900")
            syms[sym] = {"key": key_s3, "n": doc["n"], "first": doc["first_date"], "last": doc["last_date"], "as_of": doc["as_of"]}
            per_sym[sym] = {"appended": len(rows), "last": doc["last_date"]}
            appended_rows += len(rows)
            done += 1
        except Exception as e:
            idx.setdefault("failures", {})[sym] = {"err": ("polygon-append: " + str(e))[:100], "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            fail += 1
    cur = _gj(UNIV_IDX_KEY, {"symbols": {}})
    cur.setdefault("symbols", {}).update({k: syms[k] for k in per_sym})
    cur.setdefault("failures", {}).update({k: v for k, v in (idx.get("failures") or {}).items() if str(v.get("err", "")).startswith("polygon-append")})
    cur["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cur["n_symbols"] = len(cur["symbols"])
    cur["polygon_append"] = {"at": cur["updated_at"], "through": last_ok.isoformat(), "dates": [x.isoformat() for x in sorted(grouped)], "holidays_or_unavailable": holidays,
                            "calls": calls, "symbols_needing": len(todo), "appended_symbols": done, "appended_rows": appended_rows, "failed": fail, "skipped": skipped,
                            "gap_older_than_window": too_old}
    s3.put_object(Bucket=BUCKET, Key=UNIV_IDX_KEY, Body=json.dumps(cur, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    return {"ok": True, "mode": "polygon-append", **cur["polygon_append"], "sample": dict(list(per_sym.items())[:5])}


# ---------------------------------------------------------------------------------------------------------------------
# v1.3 (2026-10-25, Khalid: "100% of the data pulled and used"): mode=fmp-append.  The names outside Polygon's US tape
# (indexes, forex, crypto, non-US listings) read the FMP warehouse - justhodl-fmp-walker's historical-price-eod-full,
# refreshed every run by its v1.2.0 daily price lane - never the provider, never a guess: a TradingView symbol is
# appended only when its FMP spelling is PROVEN (an FMP_MAP entry whose symbol is a member of the banked FMP list of the
# right kind) and the first new bar is continuous with the bank's last bar (a close jump beyond 25% + 5%/session of gap,
# capped at 100%, is recorded as a series/scale mismatch and nothing is written - this is what catches a yield quoted
# x10 or a different continuous futures series).  Only sessions after the bank's last bar and no later than the last
# completed session are appended; US-namespace names are stamped 09:30 America/New_York like every row before them,
# other names inherit their own series' time of day.  Per-bar label fmp-eod-full:bank.  Append-only, never a hole:
# FMP's file is the complete history, so every missing session is present or the name is skipped.
# ---------------------------------------------------------------------------------------------------------------------
FMP_SRC = "fmp-eod-full:bank"
FMP_MAP = {   # TradingView symbol -> (FMP symbol, the banked FMP list that must contain it)
    "I:SPX": ("^GSPC", "index-list"), "I:NDX": ("^NDX", "index-list"), "XETR:DAX": ("^GDAXI", "index-list"),
    "SSE:000001": ("000001.SS", "index-list"), "US:^MOVE": ("^MOVE", "index-list"), "US:^TNX": ("^TNX", "index-list"),
    "US:^IRX": ("^IRX", "index-list"), "US:^TYX": ("^TYX", "index-list"), "TVC:US10Y": ("^TNX", "index-list"),
    "FX:EURUSD": ("EURUSD", "forex-list"), "C:BRLUSD": ("BRLUSD", "forex-list"),
    "X:BTCUSD": ("BTCUSD", "cryptocurrency-list"), "COINBASE:BTCUSD": ("BTCUSD", "cryptocurrency-list"),
    "COINBASE:ETHUSD": ("ETHUSD", "cryptocurrency-list"), "X:PEPEUSD": ("PEPEUSD", "cryptocurrency-list"),
    "HKEX:700": ("0700.HK", "stock-list"),
    # CME_MINI:ES1! is deliberately absent: FMP's ESUSD is a different continuous series (its own roll convention).
}
FMP_BASE_JUMP, FMP_JUMP_PER_SESSION, FMP_JUMP_CAP = 0.25, 0.05, 1.0


def _fmp_list_members(fmp_bank, name, cache):
    if name not in cache:
        rows = fmp_bank.walker(name)
        cache[name] = {str(r.get("symbol")).upper() for r in rows if isinstance(r, dict) and r.get("symbol")} if isinstance(rows, list) else None
    return cache[name]


def _sessions_between(a, b):
    """Weekday count strictly after a and up to b."""
    from datetime import timedelta as _td
    n, d = 0, a
    while d < b:
        d = d + _td(days=1)
        if d.weekday() < 5:
            n += 1
    return n


def fmp_append(event, context):
    """mode=fmp-append: append the sessions missing from each mapped non-tape name from the FMP warehouse (bank only)."""
    from datetime import date as _date, timedelta as _td
    import fmp_bank
    idx = _gj(UNIV_IDX_KEY, {"symbols": {}})
    syms = idx.get("symbols") or {}
    last_ok = last_completed_session()
    if event.get("through"):
        last_ok = min(last_ok, _date.fromisoformat(event["through"]))
    only = set(event.get("symbols") or [])
    lists = {}
    done = fail = skipped = unproven = stale_bank = 0
    appended_rows = 0
    per_sym, verdicts = {}, {}
    failures = {}
    for sym, (fsym, lname) in sorted(FMP_MAP.items()):
        if only and sym not in only:
            continue
        meta = syms.get(sym)
        if not isinstance(meta, dict) or not meta.get("key"):
            verdicts[sym] = "not_banked_by_chart"
            continue
        if context and context.get_remaining_time_in_millis() < 25000:
            break
        last = (meta.get("last") or "")[:10]
        try:
            last_d = _date.fromisoformat(last) if last else None
        except ValueError:
            last_d = None
        if last_d is not None and last_d >= last_ok:
            verdicts[sym] = "current"
            continue
        members = _fmp_list_members(fmp_bank, lname, lists)
        if members is None:
            verdicts[sym] = "fmp_list_not_banked:%s" % lname
            unproven += 1
            continue
        if fsym.upper() not in members:
            verdicts[sym] = "fmp_symbol_not_in_%s:%s" % (lname, fsym)
            failures[sym] = {"err": ("fmp-append: %s is not in the banked %s" % (fsym, lname))[:100], "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            unproven += 1
            continue
        hist = fmp_bank.walker("historical-price-eod-full", fsym)
        if not isinstance(hist, list) or not hist:
            verdicts[sym] = "fmp_history_not_banked:%s" % fsym
            stale_bank += 1
            continue
        try:
            key_s3 = meta["key"]
            old = read_bank(s3, BUCKET, key_s3)
            if not old or not old.get("bars"):
                skipped += 1
                verdicts[sym] = "chart_bank_unreadable"
                continue
            prev = old["bars"][-1]
            prev_dt = datetime.fromtimestamp(prev[0], tz=timezone.utc)
            prev_date = prev_dt.date()
            us_ns = str(sym).split(":")[0].upper() in POLY_US_NS
            tod = prev[0] - int(datetime(prev_date.year, prev_date.month, prev_date.day, tzinfo=timezone.utc).timestamp())
            rows = []
            for r in hist:
                if not isinstance(r, dict):
                    continue
                try:
                    d = _date.fromisoformat(str(r.get("date"))[:10])
                    o, h, l, c = float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])
                except (KeyError, TypeError, ValueError):
                    continue
                if d <= prev_date or d > last_ok:
                    continue
                v = r.get("volume")
                v = float(v) if isinstance(v, (int, float)) and v >= 0 else None
                ts = session_open_ts(d) if us_ns else int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()) + tod
                cand = [ts, o, h, l, c, v]
                if valid_bar(cand):
                    rows.append(cand)
            rows.sort()
            if not rows:
                verdicts[sym] = "fmp_bank_has_no_newer_session (newest %s)" % max((str(r.get("date"))[:10] for r in hist if isinstance(r, dict)), default=None)
                stale_bank += 1
                continue
            # continuity proof: the first new close against the bank's last close, tolerance grows with the gap
            first_date = datetime.fromtimestamp(rows[0][0], tz=timezone.utc).date()
            gap = max(1, _sessions_between(prev_date, first_date))
            tol = min(FMP_JUMP_CAP, FMP_BASE_JUMP + FMP_JUMP_PER_SESSION * gap)
            if prev[4] and abs(rows[0][4] / prev[4] - 1.0) > tol:
                verdicts[sym] = "series_or_scale_mismatch: bank close %s -> fmp %s over %d sessions (tol %.0f%%)" % (prev[4], rows[0][4], gap, tol * 100)
                failures[sym] = {"err": ("fmp-append: " + verdicts[sym])[:100], "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
                fail += 1
                continue
            have = {r[0] for r in old.get("bars", [])}
            rows = [r for r in rows if r[0] not in have]
            if not rows:
                skipped += 1
                verdicts[sym] = "already_present"
                continue
            doc = merged_document(old, rows, old.get("symbol") or sym, old.get("tv_symbol") or sym, FMP_SRC)
            s3.put_object(Bucket=BUCKET, Key=key_s3, Body=gzip.compress(json.dumps(doc).encode()),
                          ContentType="application/gzip", CacheControl="public, max-age=900")
            syms[sym] = {"key": key_s3, "n": doc["n"], "first": doc["first_date"], "last": doc["last_date"], "as_of": doc["as_of"]}
            per_sym[sym] = {"appended": len(rows), "last": doc["last_date"], "fmp": fsym}
            verdicts[sym] = "appended %d through %s from %s" % (len(rows), doc["last_date"], fsym)
            appended_rows += len(rows)
            done += 1
        except Exception as e:
            failures[sym] = {"err": ("fmp-append: " + str(e))[:100], "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            verdicts[sym] = "error: %s" % str(e)[:80]
            fail += 1
    cur = _gj(UNIV_IDX_KEY, {"symbols": {}})
    cur.setdefault("symbols", {}).update({k: syms[k] for k in per_sym})
    cur.setdefault("failures", {}).update(failures)
    cur["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cur["n_symbols"] = len(cur["symbols"])
    cur["fmp_append"] = {"at": cur["updated_at"], "through": last_ok.isoformat(), "mapped": len(FMP_MAP), "appended_symbols": done, "appended_rows": appended_rows,
                        "failed": fail, "skipped": skipped, "unproven": unproven, "fmp_bank_not_ready": stale_bank, "verdicts": verdicts}
    s3.put_object(Bucket=BUCKET, Key=UNIV_IDX_KEY, Body=json.dumps(cur, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    return {"ok": True, "mode": "fmp-append", **{k: v for k, v in cur["fmp_append"].items() if k != "verdicts"}, "verdicts": verdicts}


def lambda_handler(event, context):
    event = event or {}
    if event.get("mode") == "pull":
        return universe_pull(event, context)
    if event.get("mode") == "refresh":
        return universe_refresh(event, context)
    if event.get("mode") == "polygon-append":
        return polygon_append(event, context)
    if event.get("mode") == "fmp-append":
        return fmp_append(event, context)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    session = _p("/justhodl/tradingview/sessionid")
    sign = _p("/justhodl/tradingview/sessionid_sign")
    dev = _p("/justhodl/tradingview/device_t")
    token = _p("/justhodl/tradingview/auth_token") or "unauthorized_user_token"
    if not session:
        out = {"ok": False, "error": "session_missing",
               "detail": "/justhodl/tradingview/sessionid empty"}
        s3.put_object(Bucket=BUCKET, Key="data/tv-bars-status.json",
                      Body=json.dumps(out).encode(),
                      ContentType="application/json")
        return {"statusCode": 500, "body": json.dumps(out)}
    cookie = "sessionid=%s" % session
    if sign:
        cookie += "; sessionid_sign=%s" % sign
    if dev:
        cookie += "; device_t=%s" % dev

    st = _gj(STATE_KEY, {})
    if (st.get("lease_until") or 0) > time.time():
        return {"statusCode": 200,
                "body": json.dumps({"skipped": "lease_held"})}
    st["lease_until"] = time.time() + 560
    if not st.get("catalog"):
        st["catalog"] = build_worklist()
    todo = [x for x in st["catalog"]
            if x not in set(st.get("done") or [])]
    req = (event or {}).get("symbols")
    if req:
        todo = list(req)
    todo = todo[:TRANCHE]
    idx = _gj(IDX_KEY, {"symbols": {}})
    got = failed = 0
    for sid in todo:
        sym = "FRED:%s" % sid
        try:
            rows = pull(sym, token, cookie)
        except Exception as e:
            st.setdefault("failures", {})[sid] = \
                "%s: %s" % (type(e).__name__, str(e)[:90])
            failed += 1
            if isinstance(e, ProviderDenied):
                break
            continue
        if not rows:
            st.setdefault("failures", {})[sid] = "no bars returned"
            failed += 1
            continue
        key = "data/warm/tv-bars/%s.json.gz" % sid
        try:
            old = read_bank(s3, BUCKET, key)
            doc = merged_document(old, rows, sid, sym, "tradingview-ws (session-auth, server-side)")
        except Exception as error:
            st.setdefault("failures", {})[sid] = type(error).__name__ + ": history preserved"
            failed += 1
            continue
        merged, d0, d1 = doc["bars"], doc["first_date"], doc["last_date"]
        s3.put_object(Bucket=BUCKET, Key=key,
                      Body=gzip.compress(json.dumps(doc).encode()),
                      ContentType="application/gzip")
        idx["symbols"][sid] = {"key": key, "n": len(merged),
                               "first": d0, "last": d1}
        st.setdefault("done", []).append(sid)
        st.get("failures", {}).pop(sid, None)
        got += 1
        time.sleep(0.6)
    idx["updated_at"] = now
    idx["n_symbols"] = len(idx["symbols"])
    s3.put_object(Bucket=BUCKET, Key=IDX_KEY,
                  Body=json.dumps(idx, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    n_cat = len(st.get("catalog") or [])
    n_done = len(set(st.get("done") or []))
    st["as_of"] = now
    st["progress_pct"] = round(100 * n_done / n_cat, 1) if n_cat else 0
    st["status"] = ("COMPLETE-maintaining" if n_done >= n_cat
                    else "converging")
    st["lease_until"] = 0
    s3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                  Body=json.dumps(st, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True, "pulled": got, "failed": failed,
           "done": n_done, "catalog": n_cat,
           "status": st["status"],
           "recent_failures": dict(list(
               (st.get("failures") or {}).items())[:5])}
    s3.put_object(Bucket=BUCKET, Key="data/tv-bars-status.json",
                  Body=json.dumps(res, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
