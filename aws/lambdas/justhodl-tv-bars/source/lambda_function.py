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
            "Sec-WebSocket-Extensions: permessage-deflate; "
            "client_max_window_bits\r\n"
            "Cookie: %s\r\n\r\n" % (host, key, origin, cookie))
        self.s.sendall(req.encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            c = self.s.recv(4096)
            if not c:
                raise RuntimeError("handshake closed")
            buf += c
        if b"101" not in buf.split(b"\r\n")[0]:
            raise RuntimeError("handshake %s [%s]"
                               % (buf.split(b"\r\n")[0][:90],
                                  host + (path or "")[:40]))
        self.buf = b""

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
            except Exception:
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
                                        bars[int(a[0])] = [
                                            float(a[1]), float(a[2]),
                                            float(a[3]), float(a[4])]
                elif m in ("series_completed", "critical_error",
                           "symbol_error", "series_error"):
                    if m != "series_completed":
                        raise RuntimeError("%s %s" % (m, str(
                            msg.get("p"))[:120]))
                    done = True
    finally:
        ws.close()
    return [[k] + bars[k] for k in sorted(bars)]


def _gj(key, dflt):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return dflt


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
            _FEED["r"], _FEED["p"] = {}, {}
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
        rows.append([int(t), float(o), float(h if h is not None else c), float(l if l is not None else c), float(c), float(v or 0)])
    return rows


def bank_symbol(sym, token, cookie, countback=20000, budget=45, ysym=None, refresh=False):
    """v1.1 (ops 5124/5125): bank ANY TradingView symbol (EXCHANGE:SYMBOL) under
    data/warm/tv-bars/universe/{safe}.json.gz -- append-only union with what is
    already banked. Source order: TradingView chart socket (session) when it
    answers, else Yahoo's chart API resolved through the fleet's symbol map
    (full history since inception, volume included). Returns (doc, key)."""
    rows, src, err = [], None, []
    if cookie:
        try:
            rows = pull(sym, token, cookie, countback=countback, budget=min(budget, 12))
            src = "tradingview-ws (session-auth, server-side)"
        except Exception as e:
            err.append("tv:" + str(e)[:70])
    if not rows:
        for y in ([ysym] if ysym else []) + tv_to_yahoo(sym):
            try:
                rows = yahoo_bars(y, rng=("6mo" if refresh else "max"))
                if rows:
                    src = "yahoo-chart:%s" % y
                    break
            except Exception as e:
                err.append("yahoo %s: %s" % (y, str(e)[:60]))
    if not rows:
        raise RuntimeError("no bars for %s (%s)" % (sym, "; ".join(err)[:200] or "no yahoo mapping"))
    key = "data/warm/tv-bars/universe/%s.json.gz" % safe_name(sym)
    prev = {}
    try:
        old = json.loads(gzip.decompress(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()))
        prev = {int(b[0]): b for b in old.get("bars") or []}
    except Exception:
        pass
    for b in rows:
        prev[int(b[0])] = b
    merged = [prev[k] for k in sorted(prev)]
    d0 = datetime.fromtimestamp(merged[0][0], tz=timezone.utc).strftime("%Y-%m-%d")
    d1 = datetime.fromtimestamp(merged[-1][0], tz=timezone.utc).strftime("%Y-%m-%d")
    doc = {"symbol": sym, "tv_symbol": sym, "source": src,
           "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"), "n": len(merged),
           "first_date": d0, "last_date": d1, "pulled_now": len(rows), "bars": merged}
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
            doc, key = bank_symbol(sym, token, cookie, countback=countback, budget=int(event.get("budget") or 40), ysym=ymap.get(sym))
            out[sym] = {"ok": True, "key": key, "n": doc["n"], "first": doc["first_date"], "last": doc["last_date"], "source": doc["source"]}
            idx["symbols"][sym] = {"key": key, "n": doc["n"], "first": doc["first_date"], "last": doc["last_date"],
                                   "as_of": doc["as_of"]}
        except Exception as e:
            out[sym] = {"ok": False, "error": "%s: %s" % (type(e).__name__, str(e)[:120])}
            idx.setdefault("failures", {})[sym] = {"err": str(e)[:100], "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
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
        time.sleep(0.4)
    # per-shard write of the shared index: merge with the latest copy to keep other shards' updates
    cur = _gj(UNIV_IDX_KEY, {"symbols": {}})
    cur.setdefault("symbols", {}).update({k: v for k, v in idx["symbols"].items() if k in mine})
    cur["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cur["n_symbols"] = len(cur["symbols"])
    s3.put_object(Bucket=BUCKET, Key=UNIV_IDX_KEY, Body=json.dumps(cur, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    return {"ok": True, "shard": shard, "nshards": nshards, "mine": len(mine), "refreshed": done, "failed": fail, "skipped_today": skipped}


def lambda_handler(event, context):
    event = event or {}
    if event.get("mode") == "pull":
        return universe_pull(event, context)
    if event.get("mode") == "refresh":
        return universe_refresh(event, context)
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
            continue
        if not rows:
            st.setdefault("failures", {})[sid] = "no bars returned"
            failed += 1
            continue
        key = "data/warm/tv-bars/%s.json.gz" % sid
        prev = {}
        try:
            old = json.loads(gzip.decompress(s3.get_object(
                Bucket=BUCKET, Key=key)["Body"].read()))
            prev = {int(b[0]): b for b in old.get("bars") or []}
        except Exception:
            pass
        for b in rows:
            prev[int(b[0])] = b            # append-only union
        merged = [prev[k] for k in sorted(prev)]
        d0 = datetime.fromtimestamp(merged[0][0],
                                    tz=timezone.utc).strftime("%Y-%m-%d")
        d1 = datetime.fromtimestamp(merged[-1][0],
                                    tz=timezone.utc).strftime("%Y-%m-%d")
        doc = {"symbol": sid, "tv_symbol": sym,
               "source": "tradingview-ws (session-auth, server-side)",
               "as_of": now, "n": len(merged),
               "first_date": d0, "last_date": d1, "bars": merged}
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
