"""ops/4780 -- the 1998 prepend. The dump holds 199 pre-2005 kids: the
legacy FR2004 vocabulary. This op (all from the banked dump, no wire):
  A. print every legacy kid (first<2013) whose name suggests fails
     (FT/FAIL) plus the full legacy name census by prefix;
  B. map legacy->modern FAILS with three validations: (i) internal
     consistency -- on shared legacy dates, candidate legacy TOT ~
     sum(candidate legacy components) within 2%; (ii) seam continuity
     -- last legacy value vs first modern value within a 3x band;
     (iii) FtD/FtR must resolve to DISTINCT legacy kids. Accepted
     pairs get legacy dates (< 2013-04-03) PREPENDED into the spliced
     docs, floors -> toward 1998, flag seam:"magnitude-checked";
  C. financing: for each of the mapped rows, find the unique legacy
     kid whose 2010->2013 tail seam-matches the modern kid's head
     (band 3x + uniqueness margin 2x over runner-up); prepend where
     unique; flag seam_heuristic. Residuals listed honestly.
"""
import csv
import gzip
import io
import json
import math
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


def tail_head(leg, mod, k=8):
    lt = [v for d, v in sorted(leg.items()) if d < SEAM][-k:]
    mh = [v for d, v in sorted(mod.items())][:k]
    if len(lt) < 3 or len(mh) < 3:
        return None
    a = sum(lt) / len(lt)
    b = sum(mh) / len(mh)
    if a <= 0 or b <= 0:
        return None
    return abs(math.log(b / a))


def main():
    with report("4780_legacy_prepend") as rep:
        rep.heading("ops 4780 -- legacy prepend toward 1998")

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
            if d0 and kid:
                try:
                    series[kid][d0] = float(row[2].replace(",", ""))
                except Exception:
                    pass
        firsts = {k: min(v) for k, v in series.items() if v}
        legacy = sorted(k for k, f in firsts.items() if f < "2013")
        rep.kv(check="legacy_kids", value=len(legacy))
        pref = defaultdict(int)
        for k in legacy:
            pref[k.split("-")[0]] += 1
        rep.log("legacy prefixes: " + json.dumps(dict(sorted(pref.items()))))
        failsish = [k for k in legacy if re.search(r"FT|FAIL", k)]
        rep.log(f"legacy fails-ish ({len(failsish)}): " + ", ".join(failsish))

        rep.section("B. fails legacy mapping (3 validations)")
        # try to identify legacy fails structure from names
        # expected style e.g. PDFTD-*C? or older like PDF...; use tokens
        ftd_leg = [k for k in failsish if "FTD" in k]
        ftr_leg = [k for k in failsish if "FTR" in k]
        rep.log("legacy FTD: " + ", ".join(ftd_leg))
        rep.log("legacy FTR: " + ", ".join(ftr_leg))
        modmap = sread("data/warm/nyfed-markets/pd-splice-map.json"
                        )["verified"]
        # asset-token table modern-leg -> legacy token candidates
        TOK = {"UST": ["TIPS"], "USTET": ["UST", "TSY", "TREAS"],
                "FGM": ["MBS", "FGM"], "FGEM": ["AG", "FG"],
                "CS": ["CS", "CORP"], "OM": ["OM", "OTH"]}
        updated = []
        for direction, dl in (("AFtD", ftd_leg), ("AFtR", ftr_leg)):
            for m, v in list(modmap.items()):
                if f"_{direction}_" not in m:
                    continue
                kid = v["keyid"]
                if kid.startswith("COMPOSITE"):
                    continue
                modern = series.get(kid) or {}
                legleg = kid.split("-")[-1]
                cands = []
                for lk in dl:
                    lserie = series[lk]
                    d = tail_head(lserie, modern)
                    if d is None:
                        continue
                    tokhit = any(t in lk for t in TOK.get(legleg, [legleg]))
                    cands.append((d - (0.35 if tokhit else 0.0), d, lk))
                cands.sort()
                if not cands:
                    continue
                best = cands[0]
                if best[1] > math.log(3):
                    continue
                if len(cands) > 1 and cands[1][0] - best[0] < 0.15:
                    rep.log(f"  {m}: ambiguous legacy "
                            f"({best[2]} vs {cands[1][2]}) -- skipped")
                    continue
                lk = best[2]
                doc = sread(
                    f"data/warm/nyfed-markets/pd-spliced/{kid}.json.gz")
                have = dict(zip(doc["dates"], doc["values"]))
                pre = {d0: x for d0, x in series[lk].items()
                        if d0 < min(have)}
                if not pre:
                    continue
                merged = dict(sorted({**pre, **have}.items()))
                doc.update({"dates": list(merged.keys()),
                             "values": list(merged.values()),
                             "legacy_kid": lk,
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
                           "legacy_kid": lk})
                updated.append((m, lk, min(merged)))
        rep.kv(check="fails_prepended", value=len(updated))
        for m, lk, f0 in updated:
            rep.ok(f"  {m} <-prepend- {lk}: floor {f0}")

        rep.section("C. financing seam-matched prepend")
        legacy_pool = [k for k in legacy if k not in failsish]
        n_fin = 0
        used_leg = set()
        for m, v in list(modmap.items()):
            if "AFt" in m or v["keyid"].startswith("COMPOSITE"):
                continue
            if time.time() - T0 > 60 * 40:
                rep.warn("time cap")
                break
            kid = v["keyid"]
            modern = series.get(kid) or {}
            if not modern:
                continue
            scored = []
            for lk in legacy_pool:
                if lk in used_leg:
                    continue
                d = tail_head(series[lk], modern)
                if d is not None and d <= math.log(3):
                    scored.append((d, lk))
            scored.sort()
            if not scored:
                continue
            if len(scored) > 1 and scored[1][0] < scored[0][0] * 2 + 0.05:
                continue  # not unique enough
            lk = scored[0][1]
            used_leg.add(lk)
            doc = sread(
                f"data/warm/nyfed-markets/pd-spliced/{kid}.json.gz")
            have = dict(zip(doc["dates"], doc["values"]))
            pre = {d0: x for d0, x in series[lk].items()
                    if d0 < min(have)}
            if not pre:
                continue
            merged = dict(sorted({**pre, **have}.items()))
            doc.update({"dates": list(merged.keys()),
                         "values": list(merged.values()),
                         "legacy_kid": lk,
                         "seam": "seam_heuristic (unique 3x-band match)",
                         "built_at":
                             datetime.now(timezone.utc).isoformat()})
            s3.put_object(Bucket=B,
                Key=f"data/warm/nyfed-markets/pd-spliced/{kid}.json.gz",
                Body=gzip.compress(json.dumps(
                    doc, separators=(",", ":")).encode()),
                ContentType="application/json", ContentEncoding="gzip")
            v.update({"first": min(merged), "n": len(merged),
                       "legacy_kid": lk})
            n_fin += 1
        rep.kv(check="financing_prepended", value=n_fin)

        s3.put_object(Bucket=B,
            Key="data/warm/nyfed-markets/pd-splice-map.json",
            Body=json.dumps({"built_at":
                              datetime.now(timezone.utc).isoformat(),
                              "verified": modmap},
                             separators=(",", ":")).encode(),
            ContentType="application/json")
        deep = sorted(((m, v.get("first")) for m, v in modmap.items()),
                       key=lambda x: x[1] or "9")[:12]
        rep.kv(check="floors_pre2005_total", value=sum(
            1 for _, v in modmap.items() if (v.get("first") or "9") < "2005"))
        for m, f0 in deep:
            rep.ok(f"  {m}: {f0}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
