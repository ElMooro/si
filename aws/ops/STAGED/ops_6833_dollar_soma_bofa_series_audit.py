"""ops 6833 - READ-ONLY audit before building three chart desks (re-runnable).

Khalid 2026-11-17: "add another icon on chart.html next to 13F and it should have all the DXY and dollar index from
FRED and it should also show their historical chart: week, month, quarter, yearly percentage change; and another icon
for all the securities held outright index from FRED and another one for all ICE BofA indices from FRED, and both
should have historical charts and week, month, quarter, year percentage change."

Audit before building: which series are those, exactly?  Reads the warehouse's own FRED catalog (the pages under
data/warm/fred-catalog/series-meta/ the symbol directory is built from - every FRED series with its title, units,
frequency, first and last observation and popularity) and the map of banked observation files, then
  A. lists every series of the three families by title (dollar indexes and effective exchange rates of the dollar;
     Federal Reserve securities held outright; ICE BofA indices), with units, frequency, span and whether it is banked;
  B. reads a sample of each family through the same /series route the desks use (history length, first and last
     print) and looks for the ICE dollar index itself in the directory;
  C. embeds the lists so the desks' catalogs can be written from them, not from memory.
Writes nothing.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import re
import sys
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
FAMILIES = {
    "dollar": re.compile(r"dollar index|effective exchange rate.*united states|exchange value of the (u\.?s\.? )?dollar", re.I),
    "soma": re.compile(r"held outright", re.I),
    "bofa": re.compile(r"\bICE BofA\b|\bBofA\b.*\b(index|spread|yield)\b|merrill lynch", re.I),
}
DXY_PARTS = ["DEXUSEU", "DEXJPUS", "DEXUSUK", "DEXCAUS", "DEXSDUS", "DEXSZUS"]


def http_json(path, timeout=60):
    with urllib.request.urlopen(urllib.request.Request(PROXY + path, headers=UA), timeout=timeout) as resp:
        return json.loads(resp.read())


def main():
    now = datetime.now(timezone.utc)
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

    pack = {}
    with report("ops_6833_dollar_soma_bofa_series_audit") as r:
        r.heading("ops 6833 - dollar indexes, securities held outright, ICE BofA: what the warehouse holds %s (read-only)" % now.isoformat(timespec="minutes"))
        r.section("A. the FRED catalog")
        pages = [o["Key"] for o in listing("data/warm/fred-catalog/series-meta/") if o["Key"].endswith(".json")]
        roots = [c["Prefix"] for c in s3.list_objects_v2(Bucket=LIVE, Prefix="data/warm/fred-scoped/", Delimiter="/").get("CommonPrefixes", [])]
        with ThreadPoolExecutor(max_workers=32) as pool:
            docs = list(pool.map(get, pages))
            banked = {}
            for objs in pool.map(listing, roots):
                for o in objs:
                    parts = o["Key"].split("/")
                    if len(parts) == 5 and parts[4].endswith(".json") and not parts[4].startswith("_"):
                        banked[parts[4][:-5]] = parts[3]
        best = {}
        for d in docs:
            for row in (d or {}).get("rows") or []:
                sid = row.get("id")
                if sid and (sid not in best or int(row.get("popularity") or 0) > int(best[sid].get("popularity") or 0)):
                    best[sid] = row
        r.log("catalog pages %d, series %d, banked observation files %d in %d roots" % (len(pages), len(best), len(banked), len(roots)))
        if best:
            r.log("catalog row fields: %s" % sorted(next(iter(best.values())).keys()))
        for fam, rx in FAMILIES.items():
            rows = [{"id": sid, "title": row.get("title") or "", "units": row.get("units") or "", "freq": row.get("freq") or "", "first": row.get("obs_start"), "last": row.get("obs_end"),
                     "pop": int(row.get("popularity") or 0), "sa": row.get("seasonal_adj") or "", "banked": banked.get(sid)} for sid, row in best.items() if rx.search(row.get("title") or "")]
            rows.sort(key=lambda x: (-x["pop"], x["id"]))
            pack[fam] = rows
            live = [x for x in rows if (x["last"] or "") >= "2026-01-01"]
            r.log("**%s**: %d series by title, %d with a print in 2026, %d banked, %d discontinued in the title" % (fam, len(rows), len(live), sum(1 for x in rows if x["banked"]), sum(1 for x in rows if "DISCONTINUED" in x["title"].upper())))
            freq = {}
            for x in rows:
                freq[x["freq"]] = freq.get(x["freq"], 0) + 1
            r.log("  by frequency: %s" % json.dumps(freq))
            for x in rows[:70]:
                r.log("  %-22s pop %3d %-2s %s..%s %s | %s | %s" % (x["id"], x["pop"], (x["freq"] or "?")[:2], x["first"], x["last"], "banked" if x["banked"] else "NOT BANKED", x["title"][:110], x["units"][:40]))
            if len(rows) > 70:
                r.log("  ... and %d more (all in the pack)" % (len(rows) - 70))
        parts = {sid: best.get(sid) for sid in DXY_PARTS}
        r.log("the six currencies inside the ICE dollar index on FRED: %s" % ", ".join("%s %s (%s)" % (k, "banked" if banked.get(k) else "not banked", (v or {}).get("obs_end")) for k, v in parts.items()))
        pack["dxy_parts"] = [{"id": k, "title": (v or {}).get("title"), "units": (v or {}).get("units"), "first": (v or {}).get("obs_start"), "last": (v or {}).get("obs_end"), "banked": banked.get(k)} for k, v in parts.items()]

        r.section("B. the same series through the desks' own route")
        for q in ("dxy", "dollar index"):
            try:
                res = http_json("/symsearch?q=%s&limit=12" % urllib.parse.quote(q))
                rows = res.get("rows") or res.get("results") or []
                r.log("directory search %r: %s" % (q, " | ".join("%s (%s)" % (x.get("id") or x.get("symbol"), (x.get("name") or x.get("title") or "")[:38]) for x in rows[:10])))
                pack.setdefault("search", {})[q] = [{k: x.get(k) for k in ("id", "symbol", "name", "title", "provider", "kind", "sources") if x.get(k) is not None} for x in rows[:12]]
            except Exception as exc:  # noqa: BLE001
                r.log("directory search %r failed: %s" % (q, str(exc)[:120]))
        probes = ["TVC:DXY", "ICEUS:DXY", "TVC:DX1!"] + ["fred:" + x["id"] for x in pack["dollar"][:8]] + ["fred:" + x["id"] for x in pack["soma"][:8]] + ["fred:" + x["id"] for x in pack["bofa"][:8]]
        for fam in ("dollar", "soma", "bofa"):
            cold = [x for x in pack[fam] if not x["banked"] and (x["last"] or "") >= "2026-01-01"][:3]
            probes += ["fred:" + x["id"] for x in cold]
        seen = set()
        samples = {}
        for sid in probes:
            if sid in seen:
                continue
            seen.add(sid)
            try:
                res = http_json("/series?id=%s" % urllib.parse.quote(sid), timeout=90)
                obs = res.get("obs") or res.get("bars") or []
                first = obs[0] if obs else None
                last = obs[-1] if obs else None
                d0 = (first[0] if isinstance(first, list) else (first or {}).get("date") or (first or {}).get("time")) if first else None
                d1 = (last[0] if isinstance(last, list) else (last or {}).get("date") or (last or {}).get("time")) if last else None
                samples[sid] = {"n": len(obs), "first": d0, "last": d1, "name": res.get("name") or res.get("title"), "source": str(res.get("source") or res.get("src") or "")[:80]}
                r.log("  /series %-28s %6d obs %s .. %s | %s | %s" % (sid, len(obs), str(d0)[:10], str(d1)[:10], str(samples[sid]["name"])[:60], samples[sid]["source"]))
            except Exception as exc:  # noqa: BLE001
                samples[sid] = {"error": str(exc)[:120]}
                r.log("  /series %-28s FAILED %s" % (sid, str(exc)[:120]))
        pack["samples"] = samples
        blob = base64.b64encode(gzip.compress(json.dumps(pack, separators=(",", ":"), default=str).encode("utf-8"), 9)).decode("ascii")
        r.log("pack: %d chars" % len(blob))
        r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("FEED-PACK-GZ-B64-END -->")
        r.kv(catalog_series=str(len(best)), dollar=str(len(pack["dollar"])), soma=str(len(pack["soma"])), bofa=str(len(pack["bofa"])), banked=str(len(banked)))
        r.ok("AUDIT: %d dollar-index, %d held-outright and %d ICE BofA series by title in a catalog of %d" % (len(pack["dollar"]), len(pack["soma"]), len(pack["bofa"]), len(best)))


if __name__ == "__main__":
    main()
    sys.exit(0)
