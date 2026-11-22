"""ops 6923 - READ-ONLY: (A) the Eurosystem's foreign-currency claims, verified again at the ECB; (B) what else the
site already holds that measures a dollar shortage.

Khalid 2026-11-22 16:20 ET: "are you sure about Claims on non-euro-area residents in foreign currency and
Foreign-currency claims abroad charts and data? verify again and add whatever dollar shortage metrics such as dxy or
other".

A. Two fresh requests to the ECB Data Portal, one after the other with a pause: every item of the Eurosystem's weekly
   statement that is a claim or liability in foreign currency on non-euro-area residents, and on euro-area residents.
   The published series (data/ciss-dollar-shortage.json) is compared with the fresh one observation by observation;
   item 2 is checked against its two sub-items (2.1 receivables from the IMF, 2.2 balances, securities and loans); the
   largest weekly moves are split by sub-item so the steps in the chart can be explained.
B. The documents of the site's own dollar engines (their layout and what they measure), and whether the warehouse
   holds the Federal Reserve's series that show dollars being lent to, or withdrawn by, foreign central banks -
   asked of the series resolver one at a time with a pause - and a title search of the warehouse's FRED catalog.

Nothing is written, no engine is run.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import re
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-ciss-stress" / "source")]
BUCKET = "justhodl-dashboard-live"
ECB = "https://data-api.ecb.europa.eu/service/data/"
ECB_HEADERS = {"User-Agent": "JustHodl-SourceEvidence/1.0", "Accept": "text/csv"}
RESOLVER = "https://justhodl-data-proxy.raafouis.workers.dev/series?id="
DOCS = ("data/eurodollar-plumbing.json", "data/eurodollar-stress.json", "data/usd-funding.json", "data/dollar-radar.json", "data/crisis-plumbing.json", "data/fx-decomposition.json")
WORDS = re.compile(r"swap|fima|basis|custody|foreign|rrp|reverse|dxy|broad|dollar|xccy|libor|ted|sofr|eurodollar|shortage", re.I)
# the Federal Reserve's and the market's dollar series, as FRED names them
FRED = ("DTWEXBGS", "DTWEXAFEGS", "DTWEXEMEGS", "DEXUSEU", "DEXJPUS", "DEXUSUK", "DEXCAUS", "DEXSDUS", "DEXSZUS",
        "SWPT", "WLRRAFOIAL", "WSEFINTL1", "WMTSECL1", "WFASECL1", "H41RESPPALGTRFNWW", "WLFOL", "WDFOL", "SOFR", "IORB", "EFFR", "OBFR", "TEDRATE", "DGS3MO", "RRPONTSYD")
TITLES = re.compile(r"liquidity swaps|foreign official|held in custody|foreign and international|FIMA", re.I)


def shape(value, depth=0, limit=2):
    if isinstance(value, dict):
        keys = list(value)
        if depth >= limit:
            return "{%d: %s}" % (len(keys), ", ".join(map(str, keys[:10])))
        return "{" + ", ".join("%s: %s" % (k, shape(value[k], depth + 1, limit)) for k in keys[:14]) + (", ..." if len(keys) > 14 else "") + "}"
    if isinstance(value, list):
        return "[%d x %s]" % (len(value), shape(value[0], depth + 1, limit) if value else "")
    return type(value).__name__ if not isinstance(value, str) else "str(%s)" % value[:20]


def leaves(value, path="", out=None, cap=4000):
    """(path, dict) for every dict that looks like one measurement: it has a value and a date or a label."""
    out = [] if out is None else out
    if len(out) >= cap:
        return out
    if isinstance(value, dict):
        keys = set(value)
        if (keys & {"value", "latest", "last", "level"}) and (keys & {"date", "as_of", "label", "name", "title", "unit", "series_id", "id"}):
            out.append((path, value))
        for k, v in value.items():
            if isinstance(v, (dict, list)):
                leaves(v, path + "/" + str(k), out, cap)
    elif isinstance(value, list):
        for i, v in enumerate(value[:400]):
            if isinstance(v, (dict, list)):
                leaves(v, path + "[%d]" % i, out, cap)
    return out


def points(doc):
    obs = (doc or {}).get("obs") if isinstance(doc, dict) else None
    rows = [(r[0], r[1]) for r in obs or [] if isinstance(r, (list, tuple)) and len(r) >= 2 and r[1] is not None]
    return (len(rows), rows[0][0], rows[-1][0], rows[-1][1]) if rows else (0, None, None, None)


def selftest():
    assert points({"obs": [["2020-01-01", 1], ["2020-01-02", None], ["2020-01-03", 3]]}) == (2, "2020-01-01", "2020-01-03", 3)
    assert points({}) == (0, None, None, None)
    found = leaves({"a": {"value": 1, "date": "x"}, "b": [{"latest": 2, "label": "y"}, {"other": 1}]})
    assert [p for p, _ in found] == ["/a", "/b[0]"]
    assert shape({"a": [1, 2]}) == "{a: [2 x int]}"
    assert TITLES.search("Assets: Central Bank Liquidity Swaps: Wednesday Level") and WORDS.search("fima_repo")


def main():
    selftest()
    import boto3
    from decimal import Decimal
    from ops_report import report
    import sovereign_desk as desk
    s3 = boto3.client("s3", region_name="us-east-1")
    today = datetime.now(timezone.utc).date().isoformat()

    def get(key):
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        raw = obj["Body"].read()
        if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw), len(raw), obj["LastModified"]

    with report("ops_6923_dollar_shortage_verify_and_audit") as r:
        r.heading("ops 6923 - the foreign-currency claims verified at the ECB; the site's other dollar measures (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        fails = []

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.fail(msg)
            return cond

        r.section("A. The Eurosystem's foreign-currency items, fresh from the ECB")
        published, _, pmod = get("data/ciss-dollar-shortage.json")
        fresh = {}
        for n, path in enumerate(("ILM/W.U2.C..U4.Z06", "ILM/W.U2.C..U2.Z06")):
            if n:
                time.sleep(4)
            try:
                with urllib.request.urlopen(urllib.request.Request(ECB + path + "?format=csvdata", headers=ECB_HEADERS), timeout=120) as resp:
                    raw = resp.read()
                series = desk.flow_series(raw)
                r.log("%s -> %d bytes, %d series" % (path, len(raw), len(series)))
                for key, entry in sorted(series.items()):
                    scale = Decimal(10) ** (int(entry["meta"]["unit_mult"]) - 9)
                    obs, info = desk.observations(entry, today, scale)
                    fresh[key] = obs
                    title = str(entry["meta"].get("title_compl") or entry["meta"].get("title") or "")
                    r.log("  %-30s %5d obs %s .. %s last %10.3f bn | unit %s x10^%s | %s" % (key, len(obs), obs[0][0] if obs else "-", obs[-1][0] if obs else "-", float(obs[-1][1]) if obs else float("nan"),
                                                                                         entry["meta"].get("unit"), entry["meta"].get("unit_mult"), title[:150]))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s: %s" % (path, type(e).__name__, str(e)[:160]))
        for mid, key in (("AUX_FX_NONEA", "ILM.W.U2.C.A020000.U4.Z06"), ("AUX_FX_EA", "ILM.W.U2.C.A030000.U2.Z06")):
            ours, theirs = published["series"][mid], fresh.get(key)
            if not ck(theirs is not None, "the ECB did not return %s" % key):
                continue
            theirs = {d: float(v) for d, v in theirs}
            mine = dict(zip(ours["d"], ours["v"]))
            common = sorted(set(mine) & set(theirs))
            differ = [(d, mine[d], theirs[d]) for d in common if abs(mine[d] - round(theirs[d], 3)) > 0.0005]
            only_mine, only_theirs = sorted(set(mine) - set(theirs)), sorted(set(theirs) - set(mine))
            r.log("%s (%s): published %d observations (written %s), fresh %d | same date and value: %d | differ: %d %s | only published: %s | only fresh: %s" % (
                mid, key, len(mine), pmod.strftime("%H:%M"), len(theirs), len(common) - len(differ), len(differ), differ[:3], only_mine[:3], only_theirs[:3]))
            ck(not differ and not only_mine and len(only_theirs) <= 1, "%s is not the ECB's series observation for observation" % mid)
        total, imf, other = fresh.get("ILM.W.U2.C.A020000.U4.Z06"), fresh.get("ILM.W.U2.C.A020100.U4.Z06"), fresh.get("ILM.W.U2.C.A020200.U4.Z06")
        if total and imf and other:
            T, I, O = dict(total), dict(imf), dict(other)
            days = sorted(set(T) & set(I) & set(O))
            worst = max(days, key=lambda d: abs(T[d] - I[d] - O[d]))
            r.log("item 2 against 2.1 + 2.2 on %d weeks (%s .. %s): largest difference %.3f bn on %s | latest: 2 = %.3f, 2.1 receivables from the IMF = %.3f, 2.2 balances, securities, loans = %.3f" % (
                len(days), days[0], days[-1], float(abs(T[worst] - I[worst] - O[worst])), worst, float(T[days[-1]]), float(I[days[-1]]), float(O[days[-1]])))
            ck(abs(T[worst] - I[worst] - O[worst]) < Decimal("0.01"), "item 2 is not the sum of its two sub-items")
            moves = sorted(((T[days[i]] - T[days[i - 1]], days[i], I[days[i]] - I[days[i - 1]], O[days[i]] - O[days[i - 1]]) for i in range(1, len(days))), key=lambda x: -abs(x[0]))[:10]
            for dt, day, di, do in moves:
                r.log("  week to %s: item 2 %+8.1f bn = IMF receivables %+8.1f + balances / securities / loans %+8.1f" % (day, float(dt), float(di), float(do)))
        else:
            r.warn("the sub-items of item 2 were not returned under the expected keys: %s" % sorted(k for k in fresh if "A02" in k))

        r.section("B1. The site's own dollar documents")
        for key in DOCS:
            try:
                doc, size, mod = get(key)
                found = leaves(doc)
                hits = [(p, v) for p, v in found if WORDS.search(p + " " + str(v.get("label") or v.get("name") or v.get("title") or v.get("series_id") or v.get("id") or ""))]
                r.log("%s | %d bytes, written %s | %d measurements, %d naming a dollar-shortage word | top level: %s" % (key, size, mod.strftime("%Y-%m-%d %H:%M"), len(found), len(hits), shape(doc, 0, 0)[:260]))
                for p, v in hits[:22]:
                    label = v.get("label") or v.get("name") or v.get("title") or v.get("series_id") or v.get("id") or ""
                    val = next((v[k] for k in ("value", "latest", "last", "level") if k in v), None)
                    hist = next((len(v[k]) for k in ("history", "series", "obs", "points", "spark") if isinstance(v.get(k), list)), 0)
                    r.log("    %-62s %-46s %s %s %s%s" % (p[-62:], str(label)[:46], str(val)[:14], str(v.get("unit") or "")[:10], str(v.get("date") or v.get("as_of") or "")[:10], (" | history %d" % hist) if hist else ""))
            except Exception as e:  # noqa: BLE001
                r.log("%s | %s: %s" % (key, type(e).__name__, str(e)[:120]))

        r.section("B2. The Federal Reserve's dollar series in the warehouse, one at a time")
        ok = 0
        for sid in FRED:
            try:
                with urllib.request.urlopen(urllib.request.Request(RESOLVER + urllib.parse.quote("fred:" + sid, safe=""), headers={"User-Agent": "jh-ops-probe"}), timeout=45) as resp:
                    doc = json.loads(resp.read())
                n, d0, d1, last = points(doc)
                ok += bool(n)
                r.log("  %-20s %s | %s" % (sid, ("%6d obs %s .. %s last %s" % (n, d0, d1, last)) if n else "nothing", str(doc.get("source") or doc.get("title") or "")[:70]))
            except Exception as e:  # noqa: BLE001
                r.log("  %-20s %s: %s" % (sid, type(e).__name__, str(e)[:60]))
            time.sleep(0.8)
        r.log("resolved with history: %d of %d" % (ok, len(FRED)))

        r.section("B3. Titles in the warehouse's FRED catalog")
        try:
            keys = []
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="data/warm/fred-catalog/"):
                keys += page.get("Contents") or []
            r.log("%d objects under data/warm/fred-catalog/, %.1f MB: %s" % (len(keys), sum(k["Size"] for k in keys) / 1e6, [k["Key"].split("fred-catalog/")[-1] for k in keys[:8]]))
            hits, scanned = [], 0
            for k in sorted(keys, key=lambda k: k["Key"]):
                if k["Size"] > 120e6 or not re.search(r"\.(json|jsonl|ndjson)(\.gz)?$", k["Key"]):
                    continue
                raw = s3.get_object(Bucket=BUCKET, Key=k["Key"])["Body"].read()
                if raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                text = raw.decode("utf-8", "replace")
                scanned += 1
                for m in TITLES.finditer(text):
                    lo, hi = text.rfind("{", 0, m.start()), text.find("}", m.end())
                    if 0 <= lo and 0 < hi - lo < 1500:
                        hits.append(text[lo:hi + 1])
                if len(hits) > 4000:
                    break
            seen, shown = set(), 0
            r.log("scanned %d catalog files; %d title hits" % (scanned, len(hits)))
            for h in hits:
                try:
                    row = json.loads(h)
                except Exception:  # noqa: BLE001
                    continue
                sid, title = str(row.get("id") or row.get("series_id") or ""), str(row.get("title") or "")
                if not sid or sid in seen or not re.search(r"wednesday level|week average", title, re.I) and not re.search(r"swap", title, re.I):
                    continue
                seen.add(sid)
                if shown < 45:
                    r.log("  %-22s %s | %s | %s..%s" % (sid, title[:110], row.get("frequency_short") or row.get("frequency") or "", str(row.get("observation_start") or "")[:10], str(row.get("observation_end") or "")[:10]))
                    shown += 1
        except Exception as e:  # noqa: BLE001
            r.warn("catalog: %s: %s" % (type(e).__name__, str(e)[:160]))

        r.kv(failed_checks=len(fails), fresh_ecb_series=len(fresh), fred_resolved=ok)
        if fails:
            raise SystemExit(1)
        r.ok("verification and audit complete; nothing written")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        print("selftest ok")
    else:
        main()
