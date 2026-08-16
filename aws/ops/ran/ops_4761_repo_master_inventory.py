"""
ops/4761 -- the combined REPO MASTER INVENTORY, built from the bank.

The doc's closing ask, and the natural index for the overnight-repo
monitor: one machine-readable file covering the entire repo stack --
tri-party (TRI + TRIV1), DVP, GCF, reference rates + volumes (FNYR),
primary-dealer fails + repo/reverse-repo financing (NYPD), and the two
NY Fed haircut workbooks -- each entry carrying:

  mnemonic, OFR-official name (fetched live from metadata, per-dataset),
  decoded family/tenor/collateral/vintage, REAL banked span
  (first/last/n_obs computed from the warm files, not asserted),
  direct API URL, S3 key, and a crisis-utility tier:
    T1 = core stress dials (overnight rate/volume, venue spreads
         inputs, total+Treasury fails, haircut p90 files)
    T2 = decomposition (tenor buckets, collateral splits, FtR grid,
         PD financing tenor shares)
    T3 = context/archive (TRI legacy vs TRIV1, -F finals, cumulative
         fails variants)
  (tiering is a labeled heuristic for dashboard ordering, not data.)

Also proves the Aug-2025 tenor transition FROM THE BANK: LE30's last
observation vs B27/B830's first -- the doc says 2025-08-13; the banked
files either confirm it or the report says otherwise.

Outputs: data/repo-master-inventory.json (hot, chartable index) +
data/warm/repo-master/inventory.json.gz (permanent snapshot).
Plus a fresh soma-cusip tick check (first 3h tick was due 18:41 UTC --
report whatever the hot doc shows now, absent included).
"""
import gzip
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OFR = "https://data.financialresearch.gov/v1"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com"}
s3 = boto3.client("s3", region_name=REGION)

TENOR = {"OO": "overnight/open", "B27": "2-7d", "B830": "8-30d",
          "LE30": "2-30d (retired 2025-08)", "G30": ">30d", "TOT": "total"}
COLL = {"T": "Treasury", "AG": "Agency/GSE", "CORD": "Corporate debt",
         "O": "Other", "ABS": "ABS", "EQT": "Equities",
         "OS": "Other securities", "TOT": "total"}


def fetch_json(url, timeout=45):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def sread(key):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def deep_dates(obj, out=None, depth=0):
    if out is None:
        out = []
    if depth > 7:
        return out
    if isinstance(obj, str):
        if len(obj) >= 10 and obj[:4].isdigit() and obj[4] == "-" \
                and obj[7] == "-":
            out.append(obj[:10])
    elif isinstance(obj, (list, tuple)):
        for x in obj[:60000]:
            deep_dates(x, out, depth + 1)
    elif isinstance(obj, dict):
        for v in obj.values():
            deep_dates(v, out, depth + 1)
    return out


def decode(m):
    """REPO-TRIV1_AR_OO-P -> family/measure/leg/vintage."""
    fam = meas = leg = vint = None
    body = m
    if "-" in m:
        head, tail = m.split("-", 1)
        if tail.endswith(("-P", "-F", "-A", "-W", "-M")):
            body, vint = tail.rsplit("-", 1)
        else:
            body = tail
        parts = body.split("_")
        fam = parts[0] if parts else None
        if len(parts) >= 2:
            meas = parts[1]
        if len(parts) >= 3:
            leg = "_".join(parts[2:])
    return fam, meas, leg, vint


def tier_of(m):
    u = m.upper()
    if u in ("REPO-TRIV1_AR_OO-P", "REPO-TRIV1_TV_OO-P",
              "REPO-TRIV1_AR_TOT-P", "REPO-TRIV1_TV_TOT-P",
              "REPO-DVP_AR_TOT-P", "REPO-GCF_AR_TOT-P",
              "FNYR-SOFR-A", "FNYR-TGCR-A", "FNYR-BGCR-A",
              "FNYR-SOFR_UV-A",
              "NYPD-PD_AFtD_TOT-A", "NYPD-PD_AFtR_TOT-A",
              "NYPD-PD_AFtD_T-A", "NYPD-PD_AFtR_T-A"):
        return 1
    if u.endswith("-F") or "-TRI_" in u or "_C" == u[-2:]:
        return 3
    return 2


def main():
    with report("4761_repo_master_inventory") as rep:
        rep.heading("ops 4761 -- combined repo master inventory (built from the bank)")

        rep.section("Names from OFR metadata (live, per-dataset)")
        names = {}
        for ds in ("repo", "nypd", "fnyr"):
            try:
                d = fetch_json(f"{OFR}/metadata/mnemonics?dataset={ds}")
                rows = d if isinstance(d, list) else \
                    (d.get("mnemonics") or d.get("results") or [])
                for x in rows:
                    if isinstance(x, dict) and x.get("mnemonic"):
                        names[str(x["mnemonic"])] = str(
                            x.get("name") or x.get("label") or "")[:160]
                rep.kv(**{f"names_{ds}": len(rows)})
            except Exception as e:
                rep.warn(f"metadata {ds}: {type(e).__name__}: {str(e)[:80]}")

        st = sread("data/warm/ofr/state.json")
        cat = sorted(map(str, st.get("catalog") or []))
        want = [m for m in cat if m.startswith(
            ("REPO-TRI_", "REPO-TRIV1_", "REPO-DVP_", "REPO-GCF_",
             "FNYR-")) or (m.startswith("NYPD-PD_") and any(
                 t in m for t in ("AFtD", "AFtR", "_RP_", "_RRP_")))]
        rep.kv(check="series_in_scope", value=len(want))

        rep.section("Depth scan (real spans from the warm bank)")
        entries = []
        t0 = time.time()
        for m in want:
            if time.time() - t0 > 60 * 45:
                rep.warn("depth-scan time cap")
                break
            first = last = None
            n = 0
            try:
                doc = sread(f"data/warm/ofr/series/{m}.json.gz")
                dd = sorted(set(deep_dates(doc)))
                if dd:
                    first, last, n = dd[0], dd[-1], len(dd)
            except Exception:
                pass
            fam, meas, leg, vint = decode(m)
            entries.append({
                "mnemonic": m, "name": names.get(m, ""),
                "family": fam, "measure": meas,
                "tenor": TENOR.get(leg or "", None),
                "collateral": COLL.get(leg or "", None),
                "vintage": vint, "first": first, "last": last,
                "n_obs": n,
                "api_url": f"{OFR}/series/full?mnemonic={m}",
                "s3_key": f"data/warm/ofr/series/{m}.json.gz",
                "tier": tier_of(m)})
        held = sum(1 for e in entries if e["n_obs"])
        rep.kv(check="entries_built", value=len(entries))
        rep.kv(check="entries_with_banked_depth", value=held)

        rep.section("Tenor-break proof from the bank (doc claims 2025-08-13)")
        by_m = {e["mnemonic"]: e for e in entries}
        for pair in (("REPO-TRIV1_TV_LE30-P", "REPO-TRIV1_TV_B27-P"),
                      ("REPO-TRIV1_AR_LE30-P", "REPO-TRIV1_AR_B27-P")):
            old, new = pair
            eo, en = by_m.get(old), by_m.get(new)
            rep.kv(**{f"{old}_last": eo["last"] if eo else None,
                       f"{new}_first": en["first"] if en else None})
            if eo and en and eo["last"] and en["first"]:
                rep.ok(f"{old} ends {eo['last']} -> {new} begins "
                        f"{en['first']} (doc: 2025-08-13)")

        rep.section("Haircut + metadata file entries")
        file_entries = []
        for label, key in (
                ("NY Fed tri-party haircuts (current, post-Nov-2025)",
                 "data/warm/nyfed-research/haircuts/"
                 "tri-party-repo_data_current.xlsx"),
                ("NY Fed tri-party haircuts (May 2010 - Oct 2025)",
                 "data/warm/nyfed-research/haircuts/"
                 "tri-party-repo_preNov25_history.xlsx"),
                ("PD series-break definitions",
                 "data/warm/nyfed-markets/pd/_meta/seriesbreaks.csv.gz")):
            try:
                h = s3.head_object(Bucket=BUCKET, Key=key)
                file_entries.append({
                    "label": label, "s3_key": key,
                    "bytes": h["ContentLength"], "tier": 1,
                    "note": ("sheet volume_haircut_concentration: "
                              "p10/median/p90, dispersion, concentration"
                              if "haircut" in key else
                              "join key for 1998->present PD history")})
                rep.ok(f"file entry: {label} ({h['ContentLength']} bytes)")
            except Exception as e:
                rep.warn(f"file entry missing: {label} ({type(e).__name__})")

        inv = {"built_at": datetime.now(timezone.utc).isoformat(),
                "built_by": "ops 4761",
                "note": ("tier = labeled dashboard-ordering heuristic "
                          "(1=core stress dials, 2=decomposition, "
                          "3=context/archive), not a data field"),
                "series": entries, "files": file_entries,
                "counts": {"series": len(entries),
                            "with_depth": held,
                            "tier1": sum(1 for e in entries
                                          if e["tier"] == 1),
                            "files": len(file_entries)}}
        body = json.dumps(inv, separators=(",", ":"))
        s3.put_object(Bucket=BUCKET, Key="data/repo-master-inventory.json",
                       Body=body.encode(), ContentType="application/json",
                       CacheControl="no-cache")
        s3.put_object(Bucket=BUCKET,
                       Key="data/warm/repo-master/inventory.json.gz",
                       Body=gzip.compress(body.encode()),
                       ContentType="application/json",
                       ContentEncoding="gzip")
        rep.ok(f"inventory written: {len(entries)} series + "
                f"{len(file_entries)} files -> data/repo-master-inventory.json "
                "(hot) + warm permanent copy")

        rep.section("soma-cusip fresh check (first tick due 18:41 UTC)")
        try:
            hot = sread("data/soma-cusip-status.json")
            rep.kv(check="soma_fired", value=True)
            for k in ("as_of", "banked_this_run", "done_pairs",
                       "total_pairs", "progress_pct"):
                rep.kv(**{f"soma_{k}": hot.get(k)})
        except Exception:
            rep.kv(check="soma_fired", value=False)
            rep.log("still pre-first-tick at op runtime")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
