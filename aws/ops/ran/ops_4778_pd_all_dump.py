"""ops/4778 -- the master dump: /api/pd/get/all/timeseries.csv.
One fetch = every series, every break, all history. Stream to disk,
gzip-bank permanently (data/warm/nyfed-markets/pd/all-timeseries.csv.gz),
then census: columns, rows, distinct seriesbreaks with keyid counts +
date spans, and the legacy (SBP2001/SBP2013) fails + financing
vocabularies with whatever label/description text the dump carries.
This is the substrate for the v4 splice -- no per-kid fetching needed.
"""
import csv
import gzip
import io
import sys
import time
import urllib.request
from collections import defaultdict
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
URL = "https://markets.newyorkfed.org/api/pd/get/all/timeseries.csv"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com"}
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("4778_pd_all_dump") as rep:
        rep.heading("ops 4778 -- pd master dump: fetch, bank, census")
        t0 = time.time()
        req = urllib.request.Request(URL, headers=UA)
        buf = io.BytesIO()
        with urllib.request.urlopen(req, timeout=300) as r:
            while True:
                chunk = r.read(1 << 20)
                if not chunk:
                    break
                buf.write(chunk)
        raw = buf.getvalue()
        rep.kv(check="bytes", value=len(raw))
        rep.kv(check="fetch_secs", value=round(time.time() - t0, 1))
        gz = gzip.compress(raw)
        s3.put_object(Bucket=B,
                       Key="data/warm/nyfed-markets/pd/all-timeseries.csv.gz",
                       Body=gz, ContentType="text/csv",
                       ContentEncoding="gzip")
        rep.ok(f"banked all-timeseries.csv.gz ({len(gz)} bytes gz)")

        text = raw.decode("utf-8", "replace")
        rdr = csv.reader(io.StringIO(text))
        header = next(rdr)
        rep.log("columns: " + ", ".join(header))
        idx = {h.lower().strip(): i for i, h in enumerate(header)}
        ib = next((idx[k] for k in idx if "break" in k), None)
        ik = next((idx[k] for k in idx if "key" in k), None)
        id_ = next((idx[k] for k in idx if "date" in k), None)
        il = next((idx[k] for k in idx if "label" in k or "desc" in k), None)
        per = defaultdict(lambda: {"kids": set(), "n": 0,
                                     "dmin": "9999", "dmax": "0000"})
        lab = {}
        n = 0
        for row in rdr:
            n += 1
            try:
                bkr = row[ib] if ib is not None else "?"
                kid = row[ik] if ik is not None else "?"
                d0 = row[id_] if id_ is not None else ""
            except Exception:
                continue
            p = per[bkr]
            p["kids"].add(kid)
            p["n"] += 1
            if len(d0) >= 8:
                p["dmin"] = min(p["dmin"], d0)
                p["dmax"] = max(p["dmax"], d0)
            if il is not None and kid not in lab:
                lab[kid] = row[il]
        rep.kv(check="data_rows", value=n)
        for bkr in sorted(per):
            p = per[bkr]
            rep.kv(**{f"break_{bkr}":
                       f"kids={len(p['kids'])} rows={p['n']} "
                       f"{p['dmin']}->{p['dmax']}"})
        for bkr in ("SBP2001", "SBP2013"):
            kids = sorted(per.get(bkr, {}).get("kids") or [])
            failsish = [k for k in kids if "FT" in k or "FAIL" in k.upper()]
            rep.log(f"{bkr} fails-ish ({len(failsish)}): "
                    + ", ".join(failsish[:30]))
            for k in failsish[:10]:
                rep.log(f"    {k}: {lab.get(k, '')[:90]}")
        # financing vocab sample for SBP2001
        kids01 = sorted(per.get("SBP2001", {}).get("kids") or [])
        rep.log("SBP2001 first 40 kids: " + ", ".join(kids01[:40]))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
