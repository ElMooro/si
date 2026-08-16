"""ops/4781 -- fails legacy prepend, corrected family: PDFAS{asset}F{D|R}A
(A=amounts; C-variants are cumulative, excluded). Legacy asset classes:
U=Treasury(total), FA=agency ex-MBS, MB=agency MBS, C=corporate.
Mapping (per direction D/R):
  T   <- PDFASUF{D}A          AG_eMBS <- PDFASFAF{D}A
  AG_MBS <- PDFASMBF{D}A      CORS    <- PDFASCF{D}A
  AG  <- FA+MB                TOT     <- U+FA+MB+C
  TIPS / T_eTIPS / OMBS: no legacy split -> stay 2013 (honest).
Each prepend seam-validated (last-legacy vs first-modern within 3x
log-band); composites summed on shared legacy dates only."""
import csv
import gzip
import io
import json
import math
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
SEAM = "2013-04-03"


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


def seam_ok(leg, mod, k=8):
    lt = [v for d, v in sorted(leg.items()) if d < SEAM][-k:]
    mh = [v for d, v in sorted(mod.items())][:k]
    if len(lt) < 3 or len(mh) < 3:
        return False
    a, b = sum(lt) / len(lt), sum(mh) / len(mh)
    return a > 0 and b > 0 and abs(math.log(b / a)) <= math.log(3)


def main():
    with report("4781_fails_legacy_prepend") as rep:
        rep.heading("ops 4781 -- fails legacy prepend (PDFAS family)")
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
            if d0 and kid.startswith("PDFAS"):
                try:
                    series[kid][d0] = float(row[2].replace(",", ""))
                except Exception:
                    pass
        rep.kv(check="pdfas_kids", value=len(series))
        rep.log("kids: " + ", ".join(sorted(series)))
        for k in sorted(series)[:4]:
            rep.log(f"  {k}: {min(series[k])} -> {max(series[k])} "
                    f"n={len(series[k])}")

        modmap = sread("data/warm/nyfed-markets/pd-splice-map.json"
                        )["verified"]
        SINGLE = {"T": "U", "AG_eMBS": "FA", "AG_MBS": "MB", "CORS": "C"}
        COMPO = {"AG": ["FA", "MB"], "TOT": ["U", "FA", "MB", "C"]}
        done = []
        for direction, dl in (("AFtD", "D"), ("AFtR", "R")):
            def legkid(a):
                return f"PDFAS{a}F{dl}A"

            def legpairs(assets):
                cps = [series.get(legkid(a)) or {} for a in assets]
                if not all(cps):
                    return {}
                common = set(cps[0])
                for cp in cps[1:]:
                    common &= set(cp)
                return {d0: sum(cp[d0] for cp in cps)
                         for d0 in sorted(common)}

            plan = {**{leg: [a] for leg, a in SINGLE.items()}, **COMPO}
            for leg, assets in plan.items():
                m = f"NYPD-PD_{direction}_{leg}-A"
                v = modmap.get(m)
                if not v:
                    continue
                kid = v["keyid"]
                leg_pairs = legpairs(assets)
                if not leg_pairs:
                    rep.warn(f"  {m}: legacy assets {assets} absent")
                    continue
                doc = sread(
                    f"data/warm/nyfed-markets/pd-spliced/{kid}.json.gz")
                have = dict(zip(doc["dates"], doc["values"]))
                if not seam_ok(leg_pairs, have):
                    rep.warn(f"  {m}: seam check FAILED "
                             f"({'+'.join(assets)})")
                    continue
                pre = {d0: x for d0, x in leg_pairs.items()
                        if d0 < min(have)}
                merged = dict(sorted({**pre, **have}.items()))
                doc.update({"dates": list(merged.keys()),
                             "values": list(merged.values()),
                             "legacy_kids": [legkid(a) for a in assets],
                             "seam": "magnitude-checked (3x band)",
                             "built_at":
                                 datetime.now(timezone.utc).isoformat()})
                s3.put_object(Bucket=B,
                    Key=f"data/warm/nyfed-markets/pd-spliced/{kid}.json.gz",
                    Body=gzip.compress(json.dumps(
                        doc, separators=(",", ":")).encode()),
                    ContentType="application/json",
                    ContentEncoding="gzip")
                v.update({"first": min(merged), "n": len(merged),
                           "legacy_kids": [legkid(a) for a in assets]})
                done.append((m, min(merged), len(pre)))
        s3.put_object(Bucket=B,
            Key="data/warm/nyfed-markets/pd-splice-map.json",
            Body=json.dumps({"built_at":
                              datetime.now(timezone.utc).isoformat(),
                              "verified": modmap},
                             separators=(",", ":")).encode(),
            ContentType="application/json")
        rep.kv(check="fails_prepended", value=len(done))
        for m, f0, npre in done:
            rep.ok(f"  {m}: floor {f0} (+{npre} legacy obs)")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
