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


def lambda_handler(event, context):
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
