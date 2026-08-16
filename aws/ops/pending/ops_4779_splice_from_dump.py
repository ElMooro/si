"""ops/4779 -- splice v4 FROM THE DUMP (correct columns this time:
'As Of Date','Time Series','Value (millions)'; no break column -- the
dump may already be NY Fed's own continuous concordance per current
keyid). Stages:
  A. census the banked dump: distinct kids, spans; kids reaching
     <2005 / <2013; the 12 fails kids' spans specifically.
  B. if modern fails kids reach pre-2013 here: REBUILD every mapped
     pd-spliced doc (direct + composites + financing 19) straight from
     the dump -- authoritative, includes the 1998-2013 era -- and
     update pd-splice-map floors.
  C. financing expansion: for each still-unmapped NYPD row, single-kid
     value-verify against ALL dump kids (in-memory, factor set incl
     1e6); newly verified rows join the map with dump-built docs.
Everything value-verified before banking, as always."""
import csv
import gzip
import io
import json
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
T0 = time.time()
FACTORS = (1.0, 1e-3, 1e-6, 1e3, 1e6)
DIRECT = {"TIPS": "UST", "T_eTIPS": "USTET", "AG_MBS": "FGM",
           "AG_eMBS": "FGEM", "CORS": "CS", "OMBS": "OM"}
COMPOSITE = {"T": ["UST", "USTET"], "AG": ["FGM", "FGEM"],
              "TOT": ["UST", "USTET", "FGM", "FGEM", "CS", "OM"]}


def sread(key, as_json=True):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw) if as_json else raw


def iso(d):
    d = (d or "").strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}", d):
        return d[:10]
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", d)
    if m:
        return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return None


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


def put_spliced(kid_or_cid, m, pairs, factor, extra=None):
    body = {"keyid": kid_or_cid, "mnemonic": m, "factor": factor,
             "source": "pd/get/all/timeseries.csv (banked)",
             "built_at": datetime.now(timezone.utc).isoformat(),
             "dates": list(pairs.keys()), "values": list(pairs.values())}
    if extra:
        body.update(extra)
    s3.put_object(Bucket=B,
        Key=f"data/warm/nyfed-markets/pd-spliced/{kid_or_cid}.json.gz",
        Body=gzip.compress(json.dumps(body, separators=(",", ":"))
                            .encode()),
        ContentType="application/json", ContentEncoding="gzip")


def main():
    with report("4779_splice_from_dump") as rep:
        rep.heading("ops 4779 -- splice v4 from the banked master dump")

        rep.section("A. dump census (correct columns)")
        text = sread("data/warm/nyfed-markets/pd/all-timeseries.csv.gz",
                      as_json=False).decode("utf-8", "replace")
        rdr = csv.reader(io.StringIO(text))
        next(rdr)
        series = defaultdict(dict)
        for row in rdr:
            if len(row) < 3:
                continue
            d0 = iso(row[0])
            kid = row[1].strip()
            if not d0 or not kid:
                continue
            try:
                series[kid][d0] = float(row[2].replace(",", ""))
            except Exception:
                pass
        rep.kv(check="distinct_kids", value=len(series))
        firsts = {k: min(v) for k, v in series.items() if v}
        rep.kv(check="kids_before_2005",
                value=sum(1 for f in firsts.values() if f < "2005"))
        rep.kv(check="kids_before_2013",
                value=sum(1 for f in firsts.values() if f < "2013"))
        for leg in ("UST", "USTET", "FGM", "FGEM", "CS", "OM"):
            k = "PDFTD-" + leg
            if k in series:
                rep.log(f"  {k}: {firsts[k]} -> {max(series[k])} "
                        f"n={len(series[k])}")

        board = sread("data/repo.json")
        hist = {}
        rows = {}
        for g in board["groups"]:
            for s0 in g["series"]:
                if s0["id"].startswith("NYPD-"):
                    rows[s0["id"]] = s0
                    h = sread(f"data/repo-history/{s0['sid']}.json")
                    hist[s0["id"]] = dict(zip(h["dates"], h["values"]))

        old_map = sread("data/warm/nyfed-markets/pd-splice-map.json"
                         ).get("verified") or {}
        map_out = {}
        floors = []

        rep.section("B. rebuild mapped docs from the dump")
        for direction, prefix in (("AFtD", "PDFTD-"), ("AFtR", "PDFTR-")):
            for leg, kid_leg in DIRECT.items():
                m = f"NYPD-PD_{direction}_{leg}-A"
                kid = prefix + kid_leg
                if m not in hist or kid not in series:
                    continue
                res = vmatch(series[kid], hist[m])
                if not res:
                    rep.warn(f"  {m} vs dump {kid}: verify FAILED")
                    continue
                pairs = dict(sorted(series[kid].items()))
                put_spliced(kid, m, pairs, res["factor"])
                map_out[m] = {"keyid": kid, "factor": res["factor"],
                               "first": min(pairs), "last": max(pairs),
                               "n": len(pairs), "src": "dump"}
                floors.append((m, min(pairs)))
            for leg, comps in COMPOSITE.items():
                m = f"NYPD-PD_{direction}_{leg}-A"
                if m not in hist:
                    continue
                cps = [series.get(prefix + c) or {} for c in comps]
                if not all(cps):
                    continue
                common = set(cps[0])
                for cp in cps[1:]:
                    common &= set(cp)
                pairs = {d0: sum(cp[d0] for cp in cps)
                          for d0 in sorted(common)}
                res = vmatch(pairs, hist[m])
                if not res:
                    rep.warn(f"  {m} composite: verify FAILED")
                    continue
                cid = f"COMPOSITE-{direction}-{leg}"
                put_spliced(cid, m, pairs, res["factor"],
                             {"components": [prefix + c for c in comps]})
                map_out[m] = {"keyid": cid, "factor": res["factor"],
                               "components": [prefix + c for c in comps],
                               "first": min(pairs), "last": max(pairs),
                               "n": len(pairs), "src": "dump"}
                floors.append((m, min(pairs)))
        for m, v in old_map.items():
            if m in map_out or "AFt" in m:
                continue
            kid = v["keyid"]
            if kid in series and m in hist:
                res = vmatch(series[kid], hist[m])
                if res:
                    pairs = dict(sorted(series[kid].items()))
                    put_spliced(kid, m, pairs, res["factor"])
                    map_out[m] = {"keyid": kid, "factor": res["factor"],
                                   "first": min(pairs), "last": max(pairs),
                                   "n": len(pairs), "src": "dump"}
                    floors.append((m, min(pairs)))

        rep.section("C. financing expansion (dump brute)")
        taken = {v["keyid"] for v in map_out.values()}
        n_new = 0
        for m, o in hist.items():
            if m in map_out or "AFt" in m:
                continue
            if time.time() - T0 > 60 * 45:
                rep.warn("time cap in financing brute")
                break
            best = None
            for kid, bp in series.items():
                if kid in taken:
                    continue
                res = vmatch(bp, o)
                if res and (best is None or res["agree"] > best[1]["agree"]):
                    best = (kid, res)
                    if res["agree"] == res["overlap"]:
                        break
            if not best:
                continue
            kid, res = best
            taken.add(kid)
            pairs = dict(sorted(series[kid].items()))
            put_spliced(kid, m, pairs, res["factor"])
            map_out[m] = {"keyid": kid, "factor": res["factor"],
                           "first": min(pairs), "last": max(pairs),
                           "n": len(pairs), "src": "dump"}
            floors.append((m, min(pairs)))
            n_new += 1
        rep.kv(check="financing_new", value=n_new)

        s3.put_object(Bucket=B,
            Key="data/warm/nyfed-markets/pd-splice-map.json",
            Body=json.dumps({"built_at":
                              datetime.now(timezone.utc).isoformat(),
                              "verified": map_out},
                             separators=(",", ":")).encode(),
            ContentType="application/json")
        rep.kv(check="total_mapped", value=len(map_out))
        rep.kv(check="floors_pre2005", value=sum(
            1 for _, f0 in floors if f0 < "2005"))
        for m, f0 in sorted(floors, key=lambda x: x[1])[:18]:
            rep.ok(f"  {m}: floor {f0}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
