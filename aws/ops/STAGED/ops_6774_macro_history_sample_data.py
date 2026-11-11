"""ops 6774 - READ-ONLY data pull for the Macro desk "history strip" sample (Khalid 2026-11-10 19:42 CT: "add a historical
chart under every single data point starting from 1996 highlighting dotcom top, 2006 top, every market top and bottom,
top highlighted in green and bottom in red. Show me a sample of what it would look like first").

Real data only: this fetches, through the same Worker routes the Macro desk uses, the full history of a handful of the
desk's own series (/series?id=fred:...) plus the S&P 500 (several candidate routes; the first that answers since 1996
wins) and embeds them in the report as gzip+base64 JSON so the sample can be rendered offline with real observations.
Nothing is written anywhere else.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import sys
import time

import requests

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
SERIES = ["fred:BAMLH0A0HYM2", "fred:DGS10", "fred:UNRATE", "fred:T10Y2Y", "fred:FEDFUNDS", "fred:VIXCLS", "fred:DCOILWTICO", "fred:CPIAUCSL", "fred:WALCL", "fred:ICSA"]
SPX_ROUTES = [
    ("series", PROXY + "/series?id=TVC:SPX"),
    ("series", PROXY + "/series?id=SP:SPX"),
    ("ohlc", PROXY + "/ohlc?ticker=%5EGSPC&span=day&mult=1&days=12000"),
    ("ohlc", PROXY + "/ohlc?ticker=SPX&span=day&mult=1&days=12000"),
    ("ohlc", PROXY + "/ohlc?ticker=SPY&span=day&mult=1&days=12000"),
    ("series", PROXY + "/series?id=fred:SP500"),
]


def obs_of(payload):
    rows = payload.get("obs") or payload.get("bars") or []
    out = []
    for row in rows:
        if isinstance(row, (list, tuple)):
            d, v = row[0], row[1]
        else:
            d = row.get("time") or row.get("date")
            v = row.get("close", row.get("value"))
            if isinstance(d, (int, float)):
                d = datetime.fromtimestamp(float(d), tz=timezone.utc).strftime("%Y-%m-%d")
        if v in (None, ""):
            continue
        try:
            v = float(v)
        except (TypeError, ValueError):
            continue
        d = str(d)[:10]
        if len(d) == 10:
            out.append((d, v))
    out.sort()
    return out


def main():
    with report("ops_6774_macro_history_sample_data") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6774 - Macro desk history-strip sample: real data pull %s" % now.isoformat(timespec="minutes"))
        data = {}
        for sid in SERIES:
            try:
                t0 = time.time()
                resp = requests.get(PROXY + "/series?id=" + sid, timeout=60)
                payload = resp.json() if resp.ok else {}
                obs = [o for o in obs_of(payload) if o[0] >= "1990-01-01"]
                r.log("%-20s HTTP %s | %d obs since 1990 | first %s | last %s = %s | name %s | %.1fs" % (
                    sid, resp.status_code, len(obs), obs[0][0] if obs else None, obs[-1][0] if obs else None, obs[-1][1] if obs else None, str(payload.get("name") or payload.get("title") or "")[:60], time.time() - t0))
                if obs:
                    data[sid] = {"name": payload.get("name") or payload.get("title") or sid, "unit": payload.get("unit") or payload.get("units") or "", "obs": obs}
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s %s" % (sid, type(e).__name__, str(e)[:160]))
        spx = None
        for kind, url in SPX_ROUTES:
            try:
                t0 = time.time()
                resp = requests.get(url, timeout=90)
                payload = resp.json() if resp.ok else {}
                obs = [o for o in obs_of(payload) if o[0] >= "1990-01-01"]
                r.log("S&P route %-7s %s -> HTTP %s | %d obs | first %s | last %s | %.1fs" % (kind, url.replace(PROXY, ""), resp.status_code, len(obs), obs[0][0] if obs else None, obs[-1][0] if obs else None, time.time() - t0))
                if obs and obs[0][0] <= "1996-01-05" and not spx:
                    spx = {"name": "S&P 500 (%s)" % url.replace(PROXY, ""), "obs": obs}
            except Exception as e:  # noqa: BLE001
                r.warn("S&P route %s: %s %s" % (url.replace(PROXY, ""), type(e).__name__, str(e)[:160]))
        if spx:
            data["spx"] = spx
        blob = base64.b64encode(gzip.compress(json.dumps(data, separators=(",", ":")).encode())).decode()
        r.log("payload: %d series, %d observations, %d KB gz+b64" % (len(data), sum(len(v["obs"]) for v in data.values()), len(blob) // 1024))
        r.section("data (gzip+base64 JSON)")
        r.log("<!-- MACRO_HISTORY_SAMPLE_GZB64 -->\n%s\n<!-- /MACRO_HISTORY_SAMPLE_GZB64 -->" % blob)
        if spx and len(data) > 3:
            r.ok("ops 6774 complete (read-only): real history in hand for the sample")
        else:
            r.fail("ops 6774: S&P route missing (%s) or too few series (%d)" % (bool(spx), len(data)))


if __name__ == "__main__":
    main()
    sys.exit(0)
