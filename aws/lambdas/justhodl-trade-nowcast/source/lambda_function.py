"""justhodl-trade-nowcast v1.0 — the trade-rate layer of the canary chain.

Completes the physical-economy stack: gateways (portwatch) -> chokepoints ->
FREIGHT RATES (here) -> inland freight (freight-pulse) -> economy. Rates are
the price of moving goods: they spike when demand outruns capacity and
collapse when trade volume dies, so they lead reported trade data.

Sources (all proven live in ops 3689/3690):
  * Baltic Dry Index — dry-bulk shipping rates (tradingeconomics public page;
    three independent regex patterns, cross-checked before accepting)
  * FRED PPI Deep Sea Freight (PCU4831114831115) — ocean container/bulk rates
  * FRED PPI Long-Distance Trucking (PCU484121484121) — inland rate pressure
  * FRED Import/Export price indices (IR / IQ) — traded-goods price pressure
  * CPB World Trade Monitor — monthly world trade VOLUME (cpb.nl publication
    pages discovered via sitemap; volume is the true global demand read)

Emits data/trade-nowcast.json with per-series yoy + a composite rate-pressure
score and a plain read. stdlib only; nothing is fabricated — a source that
fails is reported as null with its error.
"""
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/trade-nowcast.json"
FRED_KEY = "2f057499936072679d8843d7fce99989"
UA = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 Chrome/126.0 Safari/537.36")}
S3 = boto3.client("s3", region_name="us-east-1")

FRED_SERIES = {
    "ocean_ppi": ("PCU4831114831115", "PPI: Deep Sea Freight Transportation"),
    "truck_ppi": ("PCU484121484121", "PPI: Long-Distance Trucking"),
    "import_prices": ("IR", "Import Price Index (all commodities)"),
    "export_prices": ("IQ", "Export Price Index (all commodities)"),
}


def _get(url, timeout=30, cap=600_000):
    try:
        return urllib.request.urlopen(
            urllib.request.Request(url, headers=UA),
            timeout=timeout).read(cap), None
    except Exception as e:
        return b"", str(e)[:110]


def _fred(sid):
    u = ("https://api.stlouisfed.org/fred/series/observations?"
         f"series_id={sid}&api_key={FRED_KEY}&file_type=json"
         "&sort_order=desc&limit=30")
    b, e = _get(u, 20)
    if e:
        return None
    try:
        obs = [x for x in (json.loads(b).get("observations") or [])
               if x.get("value") not in (".", "", None)]
        if not obs:
            return None
        cur = float(obs[0]["value"])
        d = {"level": round(cur, 2), "date": obs[0]["date"]}
        if len(obs) > 12:
            prior = float(obs[12]["value"])
            if prior:
                d["yoy_pct"] = round(100 * (cur / prior - 1), 1)
        if len(obs) > 3:
            p3 = float(obs[3]["value"])
            if p3:
                d["q_ann_pct"] = round(100 * ((cur / p3) ** 4 - 1), 1)
        return d
    except Exception:
        return None


def _bdi():
    """Baltic Dry Index — accept only if two patterns agree."""
    b, e = _get("https://tradingeconomics.com/commodity/baltic", 25, 400_000)
    if e or not b:
        return {"err": e or "empty"}
    h = b.decode("utf-8", "replace")
    cands = []
    m = re.search(r'id="p"[^>]*>\s*([\d,\.]+)', h)
    if m:
        cands.append(float(m.group(1).replace(",", "")))
    m2 = re.search(r'"last"\s*:\s*([\d\.]+)', h)
    if m2:
        cands.append(float(m2.group(1)))
    m3 = re.search(r'Baltic[^<]{0,60}?([\d,]{3,6}(?:\.\d+)?)\s*'
                   r'(?:points|index)', h, re.I)
    if m3:
        cands.append(float(m3.group(1).replace(",", "")))
    good = [c for c in cands if 200 <= c <= 20000]
    if len(good) < 2:
        return {"err": "patterns disagree", "cands": cands[:3]}
    # agreement check: two values within 1%
    good.sort()
    for i in range(len(good) - 1):
        if good[i] and abs(good[i + 1] - good[i]) / good[i] <= 0.01:
            v = round(good[i + 1], 1)
            return {"level": v,
                    "read": ("TIGHT — dry-bulk rates elevated, "
                             "commodity shipping demand strong" if v >= 2000
                             else "SOFT — dry-bulk rates depressed, "
                                  "weak bulk trade" if v <= 1200
                             else "NEUTRAL")}
    return {"err": "no agreeing pair", "cands": good[:3]}


def _cpb():
    """CPB World Trade Monitor — latest monthly publication page."""
    b, e = _get("https://www.cpb.nl/sitemap.xml", 30, 3_000_000)
    if e or not b:
        return {"err": e or "sitemap empty"}
    locs = re.findall(r"<loc>([^<]+)</loc>", b.decode("utf-8", "replace"))
    wtm = [u for u in locs if "wereldhandelsmonitor" in u.lower()
           or "world-trade-monitor" in u.lower()]
    if not wtm:
        return {"err": "no WTM pages in sitemap", "sitemap_n": len(locs)}
    MO = ("januari februari maart april mei juni juli augustus september "
          "oktober november december").split()

    def key(u):
        ym = re.search(r"-(" + "|".join(MO) + r")-(\d{4})", u.lower())
        if ym:
            return (int(ym.group(2)), MO.index(ym.group(1)) + 1)
        y2 = re.search(r"(20\d\d)", u)
        return (int(y2.group(1)) if y2 else 0, 0)

    wtm.sort(key=key, reverse=True)
    latest = wtm[0]
    page, e2 = _get(latest, 30, 500_000)
    d = {"latest_url": latest[-72:], "pages_n": len(wtm),
         "period": "-".join(str(x) for x in key(latest))}
    if not e2 and page:
        txt = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ",
                                         page.decode("utf-8", "replace")))
        m = re.search(r"wereldhandel[^.]{0,120}?([+-]?\d+[,.]\d)\s*(?:%|procent)",
                      txt, re.I)
        if not m:
            m = re.search(r"([+-]?\d+[,.]\d)\s*(?:%|procent)[^.]{0,80}?"
                          r"wereldhandel", txt, re.I)
        if m:
            d["trade_volume_chg_pct"] = float(m.group(1).replace(",", "."))
        d["excerpt"] = txt[:260]
    else:
        d["err"] = e2
    return d


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    out = {"ok": False, "version": VERSION, "generated_at": now.isoformat(),
           "series": {}, "errors": []}

    for k, (sid, name) in FRED_SERIES.items():
        r = _fred(sid)
        if r:
            r["name"] = name
            r["series_id"] = sid
            out["series"][k] = r
        else:
            out["errors"].append(f"{k}: fetch failed")

    out["bdi"] = _bdi()
    out["cpb_wtm"] = _cpb()

    # composite: rate pressure (positive = shipping/trade costs rising)
    parts = []
    for k in ("ocean_ppi", "truck_ppi", "import_prices"):
        y = (out["series"].get(k) or {}).get("yoy_pct")
        if y is not None:
            parts.append(y)
    bl = (out["bdi"] or {}).get("level")
    if bl:
        parts.append(round((bl - 1500) / 25, 1))
    if parts:
        comp = round(sum(parts) / len(parts), 1)
        out["rate_pressure"] = comp
        out["verdict"] = ("RISING" if comp >= 8 else
                          "FALLING" if comp <= -8 else "STABLE")
        out["plain"] = (
            "Freight rates are "
            + ("RISING — moving goods is getting more expensive, which "
               "feeds inflation and signals capacity tightness"
               if comp >= 8 else
               "FALLING — cheap shipping usually means weak trade volume, "
               "a slowdown tell" if comp <= -8 else
               "STABLE — no strong cost signal from shipping right now")
            + ("; Baltic Dry at " + str(bl) + " ("
               + str((out["bdi"] or {}).get("read", "")) + ")"
               if bl else "") + ".")
        out["ok"] = len(parts) >= 3

    S3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[trade-nowcast] ok={out['ok']} comp={out.get('rate_pressure')} "
          f"verdict={out.get('verdict')} bdi={(out.get('bdi') or {}).get('level')} "
          f"cpb={(out.get('cpb_wtm') or {}).get('period')} "
          f"errs={out['errors']}")
    return {"ok": out["ok"], "rate_pressure": out.get("rate_pressure"),
            "verdict": out.get("verdict")}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
