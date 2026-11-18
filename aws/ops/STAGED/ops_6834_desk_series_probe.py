"""ops 6834 - every series of the three new chart desks, read through the desks' own route (re-runnable).

Khalid 2026-11-17: three icons beside 13F on the chart - all dollar indexes from FRED, all securities held outright,
all ICE BofA indices - each with a history chart and week / month / quarter / year percentage change.

The catalogs (aws/ops/STAGED/ops_6834_desk_catalogs.json, written from ops 6833's lists: 43 + 222 + 192 series) are
read one by one through /series - the request each card will make.  Reports, per series: observations, first and
last print, last value, the week / month / quarter / year percentage change computed here (the page must print the
same figures), and every series that does not answer.  Also:
  - the ICE dollar index rebuilt from FRED's six H.10 exchange rates with ICE's published weights, checked against
    the market's own DXY closes on the same days (the page computes the same thing in the browser);
  - a wider look through the FRED catalog for dollar-index titles the first audit's wording could have missed;
  - a pack of real observations (thinned) for the offline browser test.
Opening a series that the warehouse has not banked yet banks it (the resolver's normal behaviour on first open).
Nothing else is written.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import base64
import gzip
import json
import re
import sys
import time
import urllib.parse
import urllib.request

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=48)
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = {"User-Agent": "Mozilla/5.0 jh-ops"}
CATALOGS = json.loads((Path(__file__).with_name("ops_6834_desk_catalogs.json")).read_text(encoding="utf-8"))
DXY = {"k": 50.14348112, "legs": [("DEXUSEU", -0.576), ("DEXJPUS", 0.136), ("DEXUSUK", -0.119), ("DEXCAUS", 0.091), ("DEXSDUS", 0.042), ("DEXSZUS", 0.036)]}
TOL = {"W": 5, "M": 16, "Q": 45, "Y": 95}


def http(url, timeout=90):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as resp:
        return resp.status, resp.read()


def series(sid):
    last = None
    for attempt in range(3):
        try:
            _, body = http(PROXY + "/series?id=" + urllib.parse.quote(sid))
            doc = json.loads(body)
            obs = []
            for row in doc.get("obs") or doc.get("bars") or []:
                d, v = (row[0], row[1]) if isinstance(row, list) else (row.get("time") or row.get("date"), row.get("close", row.get("value")))
                try:
                    v = float(v)
                except (TypeError, ValueError):
                    continue
                if d and v == v:
                    obs.append((str(d)[:10], v))
            obs.sort()
            return {"obs": obs, "name": doc.get("name") or doc.get("title"), "source": str(doc.get("source") or "")[:70], "keys": sorted(doc.keys())}
        except Exception as exc:  # noqa: BLE001
            last = exc
            body = getattr(exc, "read", lambda: b"")()
            if getattr(exc, "code", None) in (400, 404):
                return {"error": "HTTP %s %s" % (exc.code, body[:160].decode("utf-8", "replace"))}
            time.sleep(2 + 3 * attempt)
    return {"error": str(last)[:160]}


def shift(day, hz):
    d = datetime.strptime(day, "%Y-%m-%d")
    if hz == "W":
        return d - timedelta(days=7)
    months = {"M": 1, "Q": 3, "Y": 12}[hz]
    y, m = d.year, d.month - months
    while m <= 0:
        y, m = y - 1, m + 12
    dd = d.day
    while True:
        try:
            return datetime(y, m, dd)
        except ValueError:
            dd -= 1


def changes(obs):
    out = {}
    last_day, last = obs[-1]
    for hz in ("W", "M", "Q", "Y"):
        cut = shift(last_day, hz)
        cut_s = cut.strftime("%Y-%m-%d")
        prior = next((o for o in reversed(obs) if o[0] <= cut_s), None)
        ok = prior and (cut - datetime.strptime(prior[0], "%Y-%m-%d")).days <= TOL[hz] and prior[1] != 0
        out[hz] = round((last - prior[1]) / abs(prior[1]) * 100, 4) if ok else None
    return out


def thin(obs):
    if len(obs) <= 420:
        return obs
    head, tail = obs[:-300], obs[-300:]
    keep, seen = [], set()
    for d, v in head:
        if d[:7] not in seen:
            seen.add(d[:7])
            keep.append((d, v))
    return keep + tail


def main():
    now = datetime.now(timezone.utc)
    pack = {"generated_at": now.isoformat(timespec="seconds"), "series": {}, "status": {}}
    with report("ops_6834_desk_series_probe") as r:
        r.heading("ops 6834 - the three desks' 457 series through /series %s" % now.isoformat(timespec="minutes"))
        todo = []
        for desk, cats in CATALOGS.items():
            for ci, cat in enumerate(cats):
                for it in cat["items"]:
                    if not it[0].startswith("calc:"):
                        todo.append((desk, ci, it[0], it[1]))
        ids = sorted({t[2] for t in todo})
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=4) as pool:
            got = dict(zip(ids, pool.map(lambda s: series("FRED:" + s), ids)))
        r.log("read %d series in %.0f s" % (len(ids), time.time() - t0))
        keys = next((g["keys"] for g in got.values() if "keys" in g), None)
        r.log("a /series answer carries: %s" % keys)

        for desk, cats in CATALOGS.items():
            r.section("%s desk" % desk)
            bad, short, stale_live = [], [], []
            for ci, cat in enumerate(cats):
                r.log("**%s** (%d)" % (cat["cat"], len(cat["items"])))
                for it in cat["items"]:
                    sid = it[0]
                    if sid.startswith("calc:"):
                        continue
                    g = got[sid]
                    if "error" in g or not g.get("obs"):
                        bad.append(sid)
                        pack["status"][sid] = {"error": g.get("error") or "no observations"}
                        r.log("  %-26s FAILED %s" % (sid, g.get("error") or "no observations"))
                        continue
                    obs = g["obs"]
                    ch = changes(obs)
                    pack["status"][sid] = {"n": len(obs), "first": obs[0][0], "last": obs[-1][0], "value": obs[-1][1], "chg": ch}
                    if len(obs) < 3:
                        short.append(sid)
                    ended = "Ended" in cat["cat"]
                    if not ended and obs[-1][0] < "2026-07-01":
                        stale_live.append("%s %s" % (sid, obs[-1][0]))
                    r.log("  %-26s %6d obs %s..%s last %s | W %s M %s Q %s Y %s | %s" % (sid, len(obs), obs[0][0], obs[-1][0], ("%.6g" % obs[-1][1]),
                                                                                    ch["W"], ch["M"], ch["Q"], ch["Y"], it[1][:58]))
                    keep = ci < 2 or it is cat["items"][0] or sid in dict(DXY["legs"])
                    if keep:
                        pack["series"]["FRED:" + sid] = thin(obs)
            n = sum(len(c["items"]) for c in cats)
            r.log("%s: %d series, %d failed, %d with fewer than 3 prints, live series without a print since July: %s" % (desk, n, len(bad), len(short), stale_live or "none"))
            if bad:
                r.fail("%s desk: %d series did not answer: %s" % (desk, len(bad), ", ".join(bad[:40])))
            else:
                r.ok("%s desk: every series answers" % desk)

        r.section("DXY rebuilt from FRED's six exchange rates")
        legs = {sid: dict(got[sid]["obs"]) for sid, _ in DXY["legs"] if got.get(sid, {}).get("obs")}
        if len(legs) == 6:
            days = sorted(set.intersection(*[set(v) for v in legs.values()]))
            calc = []
            for d in days:
                val = DXY["k"]
                for sid, w in DXY["legs"]:
                    val *= legs[sid][d] ** w
                calc.append((d, val))
            r.log("%d days where all six rates printed, %s .. %s; latest %.3f on %s; a year earlier %s" % (len(calc), calc[0][0], calc[-1][0], calc[-1][1], calc[-1][0], changes(calc)))
            pack["dxy_calc"] = {"n": len(calc), "first": calc[0], "last": calc[-1], "chg": changes(calc), "tail": calc[-12:]}
            for url in ("https://justhodl.ai/api/yahoo?ticker=DX-Y.NYB&range=6mo&interval=1d", "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?range=6mo&interval=1d"):
                try:
                    status, body = http(url, 40)
                    doc = json.loads(body)
                    res = ((doc.get("chart") or {}).get("result") or [None])[0] or doc
                    ts = res.get("timestamp") or []
                    closes = (((res.get("indicators") or {}).get("quote") or [{}])[0]).get("close") or []
                    mkt = {datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d"): c for t, c in zip(ts, closes) if c}
                    if not mkt and isinstance(doc.get("bars"), list):
                        mkt = {datetime.fromtimestamp(b["time"], timezone.utc).strftime("%Y-%m-%d"): b.get("close") for b in doc["bars"] if b.get("close")}
                    both = [(d, v, mkt[d]) for d, v in calc if d in mkt]
                    if not both:
                        r.log("market DXY from %s: %d days, none in common (%s)" % (url.split("/")[2], len(mkt), str(body[:140])))
                        continue
                    diffs = [abs(v / m - 1) * 100 for _, v, m in both]
                    r.log("market DXY from %s: %d common days; rebuilt vs market close: mean gap %.3f%%, worst %.3f%%; latest common day %s rebuilt %.3f market %.3f"
                          % (url.split("/")[2], len(both), sum(diffs) / len(diffs), max(diffs), both[-1][0], both[-1][1], both[-1][2]))
                    pack["dxy_market"] = {"source": url.split("/")[2], "n": len(both), "mean_gap_pct": round(sum(diffs) / len(diffs), 4), "worst_gap_pct": round(max(diffs), 4), "latest": both[-1]}
                    if max(diffs) < 1.0:
                        r.ok("the rebuilt DXY stays within 1% of the market's close on every common day")
                    else:
                        r.fail("the rebuilt DXY is more than 1% away from the market's close on some day")
                    break
                except Exception as exc:  # noqa: BLE001
                    r.log("market DXY from %s unavailable: %s" % (url.split("/")[2], str(exc)[:120]))
        else:
            r.fail("only %d of the six exchange rates answered" % len(legs))
        for sid in ("TVC:DXY",):
            g = series(sid)
            r.log("/series %s -> %s" % (sid, g.get("error") or ("%d obs" % len(g["obs"]))))

        r.section("A wider look for dollar-index titles in the FRED catalog")
        s3 = boto3.client("s3", region_name=REGION, config=CFG)

        def listing(prefix):
            out, token = [], None
            while True:
                kw = {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
                if token:
                    kw["ContinuationToken"] = token
                resp = s3.list_objects_v2(**kw)
                out.extend(resp.get("Contents") or [])
                token = resp.get("NextContinuationToken")
                if not token:
                    return out

        def get(key):
            try:
                body = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
                return json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)
            except Exception:  # noqa: BLE001
                return None

        pages = [o["Key"] for o in listing("data/warm/fred-catalog/series-meta/") if o["Key"].endswith(".json")]
        with ThreadPoolExecutor(max_workers=32) as pool:
            docs = list(pool.map(get, pages))
        have = {t[2] for t in todo}
        wide = re.compile(r"trade[- ]weighted.*dollar|dollar.*trade[- ]weighted|value of the dollar|dollar index|effective exchange rate.*(united states|u\.s\.)|exchange rate.*broad.*united states|ICE BofA|BofA|held outright", re.I)
        extra = {}
        for d in docs:
            for row in (d or {}).get("rows") or []:
                sid, title = row.get("id"), row.get("title") or ""
                if sid and sid not in have and not sid.startswith("NASDAQ") and wide.search(title):
                    extra[sid] = row
        r.log("%d catalog titles match the wider wording and are not on a desk" % len(extra))
        for sid, row in sorted(extra.items(), key=lambda kv: -int(kv[1].get("popularity") or 0))[:80]:
            r.log("  %-24s pop %3s %s %s..%s | %s" % (sid, row.get("popularity"), row.get("freq"), row.get("obs_start"), row.get("obs_end"), (row.get("title") or "")[:120]))
        pack["extra"] = [{"id": k, "title": v.get("title"), "freq": v.get("freq"), "first": v.get("obs_start"), "last": v.get("obs_end"), "units": v.get("units")} for k, v in extra.items()]

        blob = base64.b64encode(gzip.compress(json.dumps(pack, separators=(",", ":")).encode("utf-8"), 9)).decode("ascii")
        r.log("pack: %d series with observations, %d chars" % (len(pack["series"]), len(blob)))
        r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("FEED-PACK-GZ-B64-END -->")
        failed = [k for k, v in pack["status"].items() if "error" in v]
        r.kv(series=str(len(ids)), answered=str(len(ids) - len(failed)), failed=str(len(failed)), dxy_days=str((pack.get("dxy_calc") or {}).get("n")))


if __name__ == "__main__":
    main()
    sys.exit(0)
