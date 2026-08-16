"""ops/4775 -- PD splice v3: the 1e6 factor + semantic fails map.
4774 proved: bank = OFR / 1e6 exactly (dollars vs $ millions), the pd
bank already spans SBN2013..SBN2024 (fails floor 2013-04-03 in hand),
and the fails semantics are UST=TIPS, USTET=ex-TIPS, FGM/FGEM=agency
MBS/ex-MBS, CS=corporate, OM=other-MBS. v3 therefore:
  * fails: EXPLICIT map -- 12 direct rows (TIPS,T_eTIPS,AG_MBS,
    AG_eMBS,CORS,OMBS x FtD/FtR) + 6 composites (T=UST+USTET,
    AG=FGM+FGEM, TOT=all six) -- every one re-verified by value
    (factor-scaled) before banking; composites summed per-date only
    where ALL components exist
  * financing: brute value-match rerun over all PDS* kids with the
    full factor set {1,1e-3,1e-6,1e3,1e6}
  * splice: older breaks SBP2013 + SBP2001 fetched per kid (the bank
    already holds SBN2013+), merged strictly-before, banked to
    pd-spliced/{kid or composite-id}.json.gz + pd-splice-map.json
    (factor recorded; engine v1.3 already consumes this map/format)
"""
import gzip
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
NYF = "https://markets.newyorkfed.org/api"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com"}
s3 = boto3.client("s3", region_name="us-east-1")
T0 = time.time()
OLD_BREAKS = ["SBP2013", "SBP2001"]
FACTORS = (1.0, 1e-3, 1e-6, 1e3, 1e6)

DIRECT = {"TIPS": "UST", "T_eTIPS": "USTET", "AG_MBS": "FGM",
           "AG_eMBS": "FGEM", "CORS": "CS", "OMBS": "OM"}
COMPOSITE = {"T": ["UST", "USTET"], "AG": ["FGM", "FGEM"],
              "TOT": ["UST", "USTET", "FGM", "FGEM", "CS", "OM"]}


def iso(d):
    d = (d or "").strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}", d):
        return d[:10]
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", d)
    if m:
        return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return None


def pairs_of(doc):
    out = {}
    def walk(o, depth=0):
        if depth > 7:
            return
        if isinstance(o, list):
            for row in o:
                if isinstance(row, dict):
                    d0 = iso(str(row.get("asofdate") or row.get("asOfDate")
                                  or row.get("date") or ""))
                    if d0:
                        try:
                            out[d0] = float(str(row.get("value")
                                                 ).replace(",", ""))
                        except Exception:
                            pass
                walk(row, depth + 1)
        elif isinstance(o, dict):
            for v in o.values():
                walk(v, depth + 1)
    walk(doc)
    return dict(sorted(out.items()))


def sread(key, as_json=True):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw) if as_json else raw


def fetch(url, timeout=40):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw


def vmatch(bank, ofr):
    common = sorted(set(bank) & set(ofr))[-24:]
    if len(common) < 10:
        return None
    for f in FACTORS:
        ok = sum(1 for d0 in common
                  if abs(bank[d0] - ofr[d0] * f) <=
                  max(0.005 * abs(ofr[d0] * f), 0.51))
        if ok / len(common) >= 0.9:
            return {"factor": f, "agree": ok, "overlap": len(common)}
    return None


def main():
    with report("4775_pd_splice_v3") as rep:
        rep.heading("ops 4775 -- splice v3 (1e6 factor + semantic fails)")
        board = sread("data/repo.json")
        rows = {s0["id"]: s0 for g in board["groups"] for s0 in g["series"]
                 if s0["id"].startswith("NYPD-")}
        hist = {}
        for rid, r0 in rows.items():
            h = sread(f"data/repo-history/{r0['sid']}.json")
            hist[rid] = dict(zip(h["dates"], h["values"]))

        bank_cache = {}
        def bank(kid):
            if kid not in bank_cache:
                try:
                    bank_cache[kid] = pairs_of(sread(
                        f"data/warm/nyfed-markets/pd/{kid}.json.gz"))
                except Exception:
                    bank_cache[kid] = {}
            return bank_cache[kid]

        def spliced_pairs(kid):
            cur = dict(bank(kid))
            used = ["bank(SBN2013..SBN2024)"]
            for bid in OLD_BREAKS:
                try:
                    older = pairs_of(json.loads(fetch(
                        f"{NYF}/pd/get/{bid}/timeseries/{kid}.json")))
                except Exception:
                    continue
                add = {d0: x for d0, x in older.items()
                        if cur and d0 < min(cur)}
                if add:
                    cur.update(add)
                    used.append(bid)
                time.sleep(0.1)
            return dict(sorted(cur.items())), used

        map_out = {}
        floors = []

        rep.section("fails: explicit map, verified, spliced")
        for direction, prefix in (("AFtD", "PDFTD-"), ("AFtR", "PDFTR-")):
            for leg, kid_leg in DIRECT.items():
                m = f"NYPD-PD_{direction}_{leg}-A"
                if m not in hist:
                    continue
                kid = prefix + kid_leg
                res = vmatch(bank(kid), hist[m])
                if not res:
                    rep.warn(f"  {m} vs {kid}: verify FAILED")
                    continue
                merged, used = spliced_pairs(kid)
                s3.put_object(Bucket=B,
                    Key=f"data/warm/nyfed-markets/pd-spliced/{kid}.json.gz",
                    Body=gzip.compress(json.dumps(
                        {"keyid": kid, "mnemonic": m, "breaks_used": used,
                          "factor": res["factor"],
                          "built_at":
                              datetime.now(timezone.utc).isoformat(),
                          "dates": list(merged.keys()),
                          "values": list(merged.values())},
                         separators=(",", ":")).encode()),
                    ContentType="application/json",
                    ContentEncoding="gzip")
                map_out[m] = {"keyid": kid, "breaks_used": used,
                               "factor": res["factor"],
                               "first": min(merged), "last": max(merged),
                               "n": len(merged)}
                floors.append((m, min(merged)))
            for leg, comps in COMPOSITE.items():
                m = f"NYPD-PD_{direction}_{leg}-A"
                if m not in hist:
                    continue
                comp_pairs = []
                for c in comps:
                    mp, _ = spliced_pairs(prefix + c)
                    comp_pairs.append(mp)
                common = set(comp_pairs[0])
                for cp in comp_pairs[1:]:
                    common &= set(cp)
                merged = {d0: sum(cp[d0] for cp in comp_pairs)
                           for d0 in sorted(common)}
                res = vmatch(merged, hist[m])
                if not res:
                    rep.warn(f"  {m} composite({'+'.join(comps)}): "
                             "verify FAILED")
                    continue
                cid = f"COMPOSITE-{direction}-{leg}"
                s3.put_object(Bucket=B,
                    Key=f"data/warm/nyfed-markets/pd-spliced/{cid}.json.gz",
                    Body=gzip.compress(json.dumps(
                        {"keyid": cid, "mnemonic": m,
                          "components": [prefix + c for c in comps],
                          "factor": res["factor"],
                          "built_at":
                              datetime.now(timezone.utc).isoformat(),
                          "dates": list(merged.keys()),
                          "values": list(merged.values())},
                         separators=(",", ":")).encode()),
                    ContentType="application/json",
                    ContentEncoding="gzip")
                map_out[m] = {"keyid": cid,
                               "components": [prefix + c for c in comps],
                               "factor": res["factor"],
                               "first": min(merged), "last": max(merged),
                               "n": len(merged)}
                floors.append((m, min(merged)))
        rep.kv(check="fails_mapped", value=sum(
            1 for m in map_out if "AFt" in m))

        rep.section("financing: brute rerun with full factor set")
        tsraw = sread("data/warm/nyfed-markets/pd/_meta/timeseries.csv.gz",
                       as_json=False).decode("utf-8", "replace")
        fin_kids = sorted({x.group(0) for x in re.finditer(
            r"PDS[A-Z]+-[A-Z0-9_]+", tsraw)})
        rep.kv(check="fin_kids", value=len(fin_kids))
        taken = set()
        n_fin = 0
        for m, o in hist.items():
            if "AFt" in m or m in map_out:
                continue
            if time.time() - T0 > 60 * 50:
                rep.warn("financing time cap")
                break
            best = None
            for kid in fin_kids:
                if kid in taken:
                    continue
                res = vmatch(bank(kid), o)
                if res and (best is None or res["agree"] > best[1]["agree"]):
                    best = (kid, res)
                    if res["agree"] == res["overlap"]:
                        break
            if not best:
                continue
            kid, res = best
            taken.add(kid)
            merged, used = spliced_pairs(kid)
            s3.put_object(Bucket=B,
                Key=f"data/warm/nyfed-markets/pd-spliced/{kid}.json.gz",
                Body=gzip.compress(json.dumps(
                    {"keyid": kid, "mnemonic": m, "breaks_used": used,
                      "factor": res["factor"],
                      "built_at": datetime.now(timezone.utc).isoformat(),
                      "dates": list(merged.keys()),
                      "values": list(merged.values())},
                     separators=(",", ":")).encode()),
                ContentType="application/json", ContentEncoding="gzip")
            map_out[m] = {"keyid": kid, "breaks_used": used,
                           "factor": res["factor"], "first": min(merged),
                           "last": max(merged), "n": len(merged)}
            floors.append((m, min(merged)))
            n_fin += 1
        rep.kv(check="financing_mapped", value=n_fin)

        s3.put_object(Bucket=B,
            Key="data/warm/nyfed-markets/pd-splice-map.json",
            Body=json.dumps({"built_at":
                              datetime.now(timezone.utc).isoformat(),
                              "verified": map_out},
                             separators=(",", ":")).encode(),
            ContentType="application/json")
        rep.kv(check="total_mapped", value=len(map_out))
        for m, f0 in sorted(floors, key=lambda x: x[1])[:20]:
            rep.ok(f"  {m}: floor {f0}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
