"""ops/4777 -- legacy keyid recon (pre-2013 FR2004 vocabulary).
Reads the BANKED markets-api spec to find the per-break list endpoint,
then pulls the timeseries list for SBP2001 / SBP2013 (SBN2013 as
cross-check): keyid counts per break, the fails-ish and financing-ish
vocabularies with LABELS, plus a live data probe of two legacy keyids
(span + first raw rows). Also greps the spec for every pd path so no
list variant is missed. Read-only; the v4 splice map is built on this.
"""
import gzip
import io
import json
import re
import sys
import urllib.request
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
NYF = "https://markets.newyorkfed.org/api"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com"}
s3 = boto3.client("s3", region_name="us-east-1")


def fetch(url, timeout=40):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw


def main():
    with report("4777_legacy_keyid_recon") as rep:
        rep.heading("ops 4777 -- pre-2013 keyid vocabulary recon")

        rep.section("pd paths in the banked spec")
        spec = gzip.decompress(s3.get_object(
            Bucket=B, Key="data/warm/nyfed-markets/markets-api-spec.yml.gz"
        )["Body"].read()).decode("utf-8", "replace")
        pd_paths = sorted({m.group(0) for m in re.finditer(
            r"/api/pd/[^\s:'\"]+", spec)})
        for p0 in pd_paths:
            rep.log("  " + p0)

        rep.section("per-break timeseries lists")
        for bid in ("SBP2001", "SBP2013", "SBN2013"):
            got = None
            for cand in (f"{NYF}/pd/list/{bid}/timeseries.csv",
                          f"{NYF}/pd/get/{bid}/timeseries.csv",
                          f"{NYF}/pd/list/timeseries/{bid}.csv"):
                try:
                    raw = fetch(cand).decode("utf-8", "replace")
                    if "," in raw[:200]:
                        got = (cand, raw)
                        break
                except Exception:
                    continue
            if not got:
                rep.warn(f"{bid}: no list endpoint answered")
                continue
            url, raw = got
            import csv as _csv
            rows = list(_csv.DictReader(io.StringIO(raw)))
            kids = [(r0.get("Key Id") or r0.get("keyid") or "").strip()
                     for r0 in rows]
            kids = [k for k in kids if k]
            rep.kv(**{f"{bid}_endpoint": url.split('/api/')[-1],
                       f"{bid}_keyids": len(kids)})
            fails = [k for k in kids if re.search(
                r"FT|FAIL", k, re.I)]
            fin = [k for k in kids if re.search(
                r"^PDS|RP|REPO|SI|SO", k, re.I)][:400]
            rep.log(f"{bid} fails-ish ({len(fails)}): "
                    + ", ".join(fails[:24]))
            lab = {(r0.get("Key Id") or r0.get("keyid") or "").strip():
                    (r0.get("Label") or r0.get("label") or "")
                    for r0 in rows}
            for k in fails[:8]:
                rep.log(f"    {k}: {lab.get(k, '')[:90]}")
            finlab = [k for k in fin if any(w in lab.get(k, "").lower()
                       for w in ("repo", "reverse", "financ"))]
            rep.log(f"{bid} financing-labeled ({len(finlab)}): "
                    + ", ".join(finlab[:20]))
            for k in finlab[:8]:
                rep.log(f"    {k}: {lab.get(k, '')[:90]}")

        rep.section("legacy data probes")
        for bid, kid in (("SBP2001", None), ("SBP2013", None)):
            pass
        # probe two fails-ish legacy kids found above dynamically:
        # (re-fetch SBP2001 list quickly and take first two fails kids)
        try:
            raw = fetch(f"{NYF}/pd/list/SBP2001/timeseries.csv"
                         ).decode("utf-8", "replace")
            import csv as _csv
            rows = list(_csv.DictReader(io.StringIO(raw)))
            kids = [(r0.get("Key Id") or r0.get("keyid") or "").strip()
                     for r0 in rows]
            probes = [k for k in kids if re.search(r"FT|FAIL", k, re.I)][:2]
            for kid in probes:
                doc = json.loads(fetch(
                    f"{NYF}/pd/get/SBP2001/timeseries/{kid}.json"))
                blob = json.dumps(doc)
                dates = sorted(set(re.findall(r"\d{4}-\d{2}-\d{2}", blob)))
                rep.ok(f"SBP2001 {kid}: rows~{blob.count('asofdate')} "
                        f"span={dates[0] if dates else '?'} -> "
                        f"{dates[-1] if dates else '?'}")
                rep.log("  raw[:260]: " + blob[:260])
        except Exception as e:
            rep.warn(f"legacy probe: {type(e).__name__}: {str(e)[:100]}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
