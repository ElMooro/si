"""
ops/4772 -- PD splice v2: brute-force VALUE joins (4771 verified 0/106
because the pd bank's dates are MM/DD/YYYY -- ISO intersection was
empty -- and the timeseries labels look shifted, so token-mapping was
doomed anyway). v2 changes:

  * date normalization: MM/DD/YYYY -> ISO everywhere
  * mapping by values alone: every NYPD board row is compared against
    EVERY keyid in its candidate family (fails rows vs all PDFTD/PDFTR
    kids; financing rows vs all PDS* kids), bank pairs cached per kid;
    a join is accepted only if >=90% of the last 24 common dates agree
    within 0.5% AFTER scale-factor detection (factor tested at 1,
    1000, 0.001 -- units differ between mirrors sometimes); factor is
    recorded in the map
  * then unchanged from 4771's design: cross-break fetches per
    verified kid across SBP2001/SBP2013/SBN2013/SBN2015/SBN2022 (only
    dates strictly before the newer floor merge in), permanent bank at
    data/warm/nyfed-markets/pd-spliced/{kid}.json.gz + the engine map
    data/warm/nyfed-markets/pd-splice-map.json
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
BREAKS_OLD = ["SBN2022", "SBN2015", "SBN2013", "SBP2013", "SBP2001"]


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
                    v = row.get("value")
                    if d0:
                        try:
                            out[d0] = float(str(v).replace(",", ""))
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
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw) if as_json else raw


def fetch(url, timeout=40):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw


def match(bank, ofr):
    common = sorted(set(bank) & set(ofr))[-24:]
    if len(common) < 10:
        return None
    for f in (1.0, 1000.0, 0.001):
        ok = 0
        for d0 in common:
            bv, ov = bank[d0], ofr[d0] * f
            if abs(bv - ov) <= max(0.005 * abs(ov), 0.51):
                ok += 1
        if ok / len(common) >= 0.9:
            return {"factor": f, "agree": ok, "overlap": len(common)}
    return None


def main():
    with report("4772_pd_splice_v2") as rep:
        rep.heading("ops 4772 -- PD splice v2: value-brute joins + splice")

        # keyid universe from the banked meta (labels ignored)
        tsraw = sread("data/warm/nyfed-markets/pd/_meta/timeseries.csv.gz",
                       as_json=False).decode("utf-8", "replace")
        kids = sorted({m.group(0) for m in
                        re.finditer(r"PD[A-Z]+-[A-Z0-9_]+", tsraw)})
        fails_kids = [k for k in kids if k.startswith(("PDFTD-", "PDFTR-"))]
        fin_kids = [k for k in kids if k.startswith(
            ("PDSIRRA-", "PDSORA-", "PDSIOSB-", "PDSOOS-"))]
        rep.kv(check="keyids_total", value=len(kids))
        rep.kv(check="fails_kids", value=len(fails_kids))
        rep.kv(check="financing_kids", value=len(fin_kids))

        board = sread("data/repo.json")
        nypd = [s0 for g in board["groups"] for s0 in g["series"]
                 if s0["id"].startswith("NYPD-")]
        rep.kv(check="board_nypd_rows", value=len(nypd))

        rep.section("brute-force value joins")
        bank_cache = {}

        def bank_pairs(kid):
            if kid not in bank_cache:
                try:
                    bank_cache[kid] = pairs_of(sread(
                        f"data/warm/nyfed-markets/pd/{kid}.json.gz"))
                except Exception:
                    bank_cache[kid] = {}
            return bank_cache[kid]

        verified = {}
        taken = set()
        for r0 in nypd:
            if time.time() - T0 > 60 * 30:
                rep.warn("join-phase time cap")
                break
            fam = fails_kids if ("AFtD" in r0["id"] or "AFtR" in r0["id"]) \
                else fin_kids
            hist = sread(f"data/repo-history/{r0['sid']}.json")
            ofr = dict(zip(hist["dates"], hist["values"]))
            best = None
            for kid in fam:
                if kid in taken:
                    continue
                res = match(bank_pairs(kid), ofr)
                if res and (best is None or res["agree"] > best[1]["agree"]):
                    best = (kid, res)
                    if res["agree"] == res["overlap"]:
                        break
            if best:
                verified[r0["id"]] = {"keyid": best[0], "sid": r0["sid"],
                                        **best[1]}
                taken.add(best[0])
        rep.kv(check="verified_mappings", value=len(verified))
        facs = {}
        for m, v in verified.items():
            facs[v["factor"]] = facs.get(v["factor"], 0) + 1
        rep.kv(check="scale_factors", value=json.dumps(facs))
        for m, v in list(verified.items())[:10]:
            rep.log(f"  ✓ {m} <-> {v['keyid']} f={v['factor']} "
                    f"({v['agree']}/{v['overlap']})")
        unv = [r0["id"] for r0 in nypd if r0["id"] not in verified]
        rep.kv(check="unverified", value=len(unv))
        for m in unv[:10]:
            rep.log(f"  unverified: {m}")

        rep.section("older-break fetch + permanent splice bank")
        map_out = {}
        floors = []
        for m, v in verified.items():
            if time.time() - T0 > 60 * 55:
                rep.warn("splice time cap -- rest next run")
                break
            kid = v["keyid"]
            cur = bank_pairs(kid)
            if not cur:
                continue
            merged = dict(cur)
            used = ["current"]
            for bid in BREAKS_OLD:
                try:
                    older = pairs_of(json.loads(fetch(
                        f"{NYF}/pd/get/{bid}/timeseries/{kid}.json")))
                except Exception:
                    continue
                add = {d0: x for d0, x in older.items() if d0 < min(merged)}
                if add:
                    merged.update(add)
                    used.append(bid)
                time.sleep(0.1)
            merged = dict(sorted(merged.items()))
            if min(merged) < min(cur):
                floors.append((m, min(cur), min(merged), len(merged)))
            s3.put_object(
                Bucket=B,
                Key=f"data/warm/nyfed-markets/pd-spliced/{kid}.json.gz",
                Body=gzip.compress(json.dumps(
                    {"keyid": kid, "mnemonic": m, "breaks_used": used,
                      "factor_to_ofr_units": v["factor"],
                      "built_at": datetime.now(timezone.utc).isoformat(),
                      "dates": list(merged.keys()),
                      "values": list(merged.values())},
                     separators=(",", ":")).encode()),
                ContentType="application/json", ContentEncoding="gzip")
            map_out[m] = {"keyid": kid, "breaks_used": used,
                           "factor": v["factor"], "first": min(merged),
                           "last": max(merged), "n": len(merged)}
        s3.put_object(
            Bucket=B, Key="data/warm/nyfed-markets/pd-splice-map.json",
            Body=json.dumps({"built_at":
                              datetime.now(timezone.utc).isoformat(),
                              "verified": map_out},
                             separators=(",", ":")).encode(),
            ContentType="application/json")
        rep.kv(check="spliced_docs_banked", value=len(map_out))
        rep.kv(check="rows_with_deeper_floor", value=len(floors))
        for m, old, new, n in sorted(floors, key=lambda x: x[2])[:18]:
            rep.ok(f"  {m}: {old} -> {new} (n={n})")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
