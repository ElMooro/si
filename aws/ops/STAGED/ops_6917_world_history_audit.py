"""ops 6917 - READ-ONLY: what history the site already holds for each country on the Sovereign desk's World tab.

Khalid 2026-11-22 12:57 ET: "add a chart to world cds and yields and have it show all and also have it the chart show
only the ones i tagged same as you have on crisis peak".  The World tab's table is one day's provider-reported figures
(data/global-sovereign.json, 44 countries); a chart needs a history per country.  Before building, this lists what is
already stored, per country and per measure:

  1. the CDS desk's own measured 5Y CDS per sovereign name (data/cds-desk-history.json, DTCC public prints);
  2. the official 10-year yield banks (data/warm/official-yields/*.json) and the Treasury par bank;
  3. monthly 10-year yields in the warehouse (OECD long-term rates through the site's series resolver), asked one at
     a time with a pause;
  4. the world document's own retained daily publications (the engine keeps every predecessor), which carry the very
     figures the table shows.

Nothing is written, no engine is run.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
BUCKET = "justhodl-dashboard-live"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
RETAINED = "audit-private/20260909-originals/global-sovereign-research/"
# ISO-2 for the OECD monthly long-term rate ids (IRLTLT01<cc>M156N); the ECB's monthly yields already cover the EU
OECD = {"United States": "US", "Japan": "JP", "United Kingdom": "GB", "Canada": "CA", "Australia": "AU", "New Zealand": "NZ", "Switzerland": "CH", "Norway": "NO",
        "South Korea": "KR", "Mexico": "MX", "South Africa": "ZA", "Israel": "IL", "Chile": "CL", "Colombia": "CO", "Russia": "RU", "India": "IN", "Indonesia": "ID",
        "Brazil": "BR", "China": "CN", "Turkey": "TR", "Sweden": "SE", "Denmark": "DK", "Poland": "PL", "Hungary": "HU", "Czech Republic": "CZ", "Germany": "DE"}


def shape(value, depth=0, limit=2):
    """A short description of a JSON value's layout."""
    if isinstance(value, dict):
        keys = list(value)
        if depth >= limit:
            return "{%d keys: %s}" % (len(keys), ", ".join(map(str, keys[:8])))
        return "{" + ", ".join("%s: %s" % (k, shape(value[k], depth + 1, limit)) for k in keys[:10]) + (", ..." if len(keys) > 10 else "") + "}"
    if isinstance(value, list):
        return "[%d x %s]" % (len(value), shape(value[0], depth + 1, limit) if value else "")
    return type(value).__name__ if not isinstance(value, str) else "str(%s)" % value[:24]


def points(series):
    """(n, first date, last date, last value) of a series in any of the layouts the site stores."""
    if isinstance(series, dict):
        for a, b in (("d", "v"), ("dates", "values"), ("t", "v")):
            if isinstance(series.get(a), list) and isinstance(series.get(b), list) and series[a]:
                return len(series[a]), str(series[a][0]), str(series[a][-1]), series[b][-1]
        for key in ("observations", "obs", "series", "points", "rows", "history", "daily"):
            if isinstance(series.get(key), list) and series[key]:
                return points(series[key])
        return 0, None, None, None
    if isinstance(series, list) and series:
        first, last = series[0], series[-1]
        if isinstance(first, dict):
            dk = next((k for k in ("date", "d", "t", "day") if k in first), None)
            vk = next((k for k in ("value", "v", "spread_bp", "bp", "close", "y") if k in last), None)
            return len(series), str(first.get(dk)), str(last.get(dk)), last.get(vk)
        if isinstance(first, (list, tuple)) and len(first) >= 2:
            return len(series), str(first[0]), str(last[0]), last[1]
    return 0, None, None, None


def selftest():
    assert points({"d": ["2020-01-01", "2020-01-02"], "v": [1, 2]}) == (2, "2020-01-01", "2020-01-02", 2)
    assert points([{"date": "2020-01-01", "value": 1.5}]) == (1, "2020-01-01", "2020-01-01", 1.5)
    assert points({"observations": [["2020-01-01", 3]]}) == (1, "2020-01-01", "2020-01-01", 3)
    assert points({}) == (0, None, None, None) and shape({"a": [1]}).startswith("{a: [1 x int]")


def main():
    selftest()
    import boto3
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1")

    def get(key):
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        raw = obj["Body"].read()
        if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw), len(raw), obj["LastModified"]

    with report("ops_6917_world_history_audit") as r:
        r.heading("ops 6917 - history already stored for the World tab's countries (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        world, _, wmod = get("data/global-sovereign.json")
        countries = [c["country"] for c in world["countries"]]
        r.log("world document %s, written %s, %d countries" % (world.get("version"), wmod.strftime("%Y-%m-%d %H:%M"), len(countries)))
        found = {c: {} for c in countries}

        r.section("1. The CDS desk's measured 5Y CDS per sovereign")
        try:
            packet, _, pmod = get("data/cds-desk.json")
            hist, hbytes, hmod = get("data/cds-desk-history.json")
            sov = packet["groups"]["sovereign"]
            names = {}
            for part in ("rows", "unpriced", "dormant", "universe"):
                for row in sov.get(part) or []:
                    if row.get("name"):
                        names.setdefault(row["name"], {}).update({k: row.get(k) for k in ("key", "iso3", "status", "tier") if row.get(k) is not None})
            r.log("packet %s as of %s (written %s) | sovereign names known %d | history file %d bytes, written %s, top keys %s" % (
                packet.get("version"), packet.get("as_of"), pmod.strftime("%Y-%m-%d %H:%M"), len(names), hbytes, hmod.strftime("%Y-%m-%d %H:%M"), list(hist)[:12]))
            hn = hist.get("names") or {}
            r.log("history.names: %d names | one record: %s" % (len(hn), shape(next(iter(hn.values())), 0, 2) if hn else "-"))
            for extra in ("meta", "as_of", "first_day", "last_day", "generated_at", "schema", "fields", "note"):
                if extra in hist and not isinstance(hist[extra], (dict, list)):
                    r.log("  history.%s = %s" % (extra, str(hist[extra])[:160]))
            alias = {"Czech Republic": ("Czech Republic", "Czechia"), "South Korea": ("South Korea", "Korea"), "United States": ("United States", "USA"), "United Kingdom": ("United Kingdom", "UK")}
            n_series = 0
            for c in countries:
                info = next((names[a] for a in alias.get(c, (c,)) if a in names), None)
                if not info:
                    r.log("  %-16s not a name on the CDS desk" % c)
                    continue
                rec = hn.get(info.get("key")) if info.get("key") else None
                n, d0, d1, last = points(rec) if rec is not None else (0, None, None, None)
                if n:
                    n_series += 1
                    found[c]["cds"] = "%d points %s..%s" % (n, d0, d1)
                r.log("  %-16s %-10s key %-34s history %s" % (c, info.get("status"), str(info.get("key"))[:34], ("%d points, %s .. %s, last %s" % (n, d0, d1, last)) if n else "none"))
            r.log("countries of the World tab with a measured CDS history: %d of %d" % (n_series, len(countries)))
            long = (hist.get("long") or {}).get("series") or {}
            r.log("long records (not single names): %s" % ", ".join("%s %s" % (k, "%d pts %s..%s" % points(v)[:3]) for k, v in list(long.items())[:14]))
        except Exception as e:  # noqa: BLE001
            r.warn("CDS desk files: %s: %s" % (type(e).__name__, str(e)[:200]))

        r.section("2. Official 10-year yield banks")
        try:
            state, _, smod = get("data/warm/official-yields/_state.json")
            catalog = state.get("catalog") or state
            r.log("lane state written %s | layout %s" % (smod.strftime("%Y-%m-%d %H:%M"), shape(state, 0, 1)[:300]))
            slugs = sorted(k for k in (catalog if isinstance(catalog, dict) else {}) ) or sorted(x.get("slug") or x.get("id") for x in catalog if isinstance(x, dict))
            for slug in slugs:
                if "10y" not in str(slug) and "long" not in str(slug):
                    continue
                key = "data/warm/official-yields/%s.json" % str(slug).split(":")[-1]
                try:
                    doc, size, mod = get(key)
                    n, d0, d1, last = points(doc)
                    r.log("  %-18s %6d points %s .. %s last %s | %s | written %s" % (slug, n, d0, d1, last, str(doc.get("source") or doc.get("title"))[:60], mod.strftime("%Y-%m-%d")))
                except Exception as e:  # noqa: BLE001
                    r.log("  %-18s %s" % (slug, type(e).__name__))
        except Exception as e:  # noqa: BLE001
            r.warn("official yields: %s: %s" % (type(e).__name__, str(e)[:200]))
        try:
            par, size, mod = get("data/warm/treasury-par/curve.json.gz")
            r.log("Treasury par bank: %d bytes, written %s, layout %s" % (size, mod.strftime("%Y-%m-%d"), shape(par, 0, 1)[:300]))
        except Exception as e:  # noqa: BLE001
            r.log("Treasury par bank: %s" % type(e).__name__)

        r.section("3. Monthly 10-year yields through the site's series resolver, one at a time")
        ok = 0
        for c, cc in OECD.items():
            if c not in found:
                continue
            sid = "fred:IRLTLT01%sM156N" % cc
            try:
                with urllib.request.urlopen(urllib.request.Request(PROXY + "/series?id=" + urllib.parse.quote(sid), headers={"User-Agent": "jh-ops-probe"}), timeout=40) as resp:
                    doc = json.loads(resp.read())
                n, d0, d1, last = points(doc)
                extra = {k: str(doc[k])[:50] for k in ("source", "store", "from", "error", "title", "origin") if k in doc}
                if n:
                    ok += 1
                    found[c]["oecd"] = "%d points %s..%s" % (n, d0, d1)
                r.log("  %-16s %-22s %s %s" % (c, sid, ("%d points, %s .. %s, last %s" % (n, d0, d1, last)) if n else "nothing", extra))
            except Exception as e:  # noqa: BLE001
                r.log("  %-16s %-22s %s: %s" % (c, sid, type(e).__name__, str(e)[:80]))
            time.sleep(0.8)
        r.log("resolved with history: %d of %d asked" % (ok, sum(1 for c in OECD if c in found)))

        r.section("4. The world document's own retained publications")
        try:
            keys, pages = [], s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=RETAINED)
            for page in pages:
                keys += page.get("Contents") or []
            total = sum(k["Size"] for k in keys)
            r.log("%d retained objects, %.1f MB, %s .. %s" % (len(keys), total / 1e6, min(k["LastModified"] for k in keys).strftime("%Y-%m-%d") if keys else "-", max(k["LastModified"] for k in keys).strftime("%Y-%m-%d") if keys else "-"))
            heads = {}
            if keys and total < 400e6:
                for k in sorted(keys, key=lambda k: k["LastModified"]):
                    if not 20000 < k["Size"] < 400000:
                        continue
                    try:
                        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=k["Key"])["Body"].read())
                    except Exception:  # noqa: BLE001
                        continue
                    if isinstance(doc, dict) and isinstance(doc.get("countries"), list) and doc.get("generated_at"):
                        heads[doc["generated_at"]] = doc
                days = sorted({g[:10] for g in heads})
                r.log("publications among them: %d, on %d distinct days, %s .. %s" % (len(heads), len(days), days[0] if days else "-", days[-1] if days else "-"))
                if heads:
                    first = heads[min(heads)]
                    r.log("earliest: version %s, %d countries, a row: %s" % (first.get("version"), len(first["countries"]), json.dumps(first["countries"][0])[:260]))
                    per = {}
                    for g, doc in heads.items():
                        for c in doc["countries"]:
                            if isinstance(c, dict) and c.get("country"):
                                slot = per.setdefault(c["country"], {"y": set(), "cds": set()})
                                if isinstance(c.get("yield_10y_pct"), (int, float)):
                                    slot["y"].add(g[:10])
                                if isinstance(c.get("cds_bp"), (int, float)):
                                    slot["cds"].add(g[:10])
                    r.log("days with a yield / with a CDS, per country: %s" % " | ".join("%s %d/%d" % (c, len(per.get(c, {"y": ()})["y"]), len(per.get(c, {"cds": ()})["cds"])) for c in countries))
                    gaps = [days[i] for i in range(1, len(days)) if (datetime.fromisoformat(days[i]) - datetime.fromisoformat(days[i - 1])).days > 3]
                    r.log("gaps longer than three days begin before: %s" % (gaps[:12] or "none"))
        except Exception as e:  # noqa: BLE001
            r.warn("retained publications: %s: %s" % (type(e).__name__, str(e)[:200]))

        r.section("5. Per country")
        for c in countries:
            r.log("  %-16s %s" % (c, json.dumps(found[c]) if found[c] else "-"))
        r.kv(countries=len(countries), with_cds_history=sum(1 for c in found.values() if "cds" in c), with_oecd_monthly=sum(1 for c in found.values() if "oecd" in c))
        r.ok("audit complete; nothing written")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        print("selftest ok")
    else:
        main()
