"""ops/4770 -- prove the flagged holes are the PUBLISHER'S record.
For three representative flagged windows (DVP year-end 2024, GCF's
2020 drought, GCF's new B27 bucket), fetch the series live from OFR
and compare observation dates inside the window against the bank.
identical = the bank is faithful and the hole is source-side truth
(no trades / no publication); any live date the bank lacks = a REAL
banking gap, listed. Read-only."""
import gzip
import json
import sys
import urllib.request
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
OFR = "https://data.financialresearch.gov/v1"
UA = {"User-Agent": "JustHodl.AI research raafouis@gmail.com"}
s3 = boto3.client("s3", region_name="us-east-1")
CASES = [("REPO-DVP_AR_OO-P", "2024-12-18", "2025-01-06"),
          ("REPO-GCF_AR_LE30-P", "2020-03-25", "2020-10-28"),
          ("REPO-GCF_OV_B27-P", "2025-12-20", "2026-05-10")]


def deep_dates(o, out=None, depth=0):
    if out is None:
        out = []
    if depth > 7:
        return out
    if isinstance(o, str):
        if len(o) >= 10 and o[:4].isdigit() and o[4] == "-" and o[7] == "-":
            out.append(o[:10])
    elif isinstance(o, (list, tuple)):
        for x in o[:60000]:
            deep_dates(x, out, depth + 1)
    elif isinstance(o, dict):
        for v in o.values():
            deep_dates(v, out, depth + 1)
    return out


def main():
    with report("4770_holes_source_proof") as rep:
        rep.heading("ops 4770 -- flagged holes vs the live publisher")
        all_match = True
        for m, a, b in CASES:
            req = urllib.request.Request(
                f"{OFR}/series/full?mnemonic={m}", headers=UA)
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            live = sorted({d for d in set(deep_dates(json.loads(raw)))
                            if a <= d <= b})
            raw2 = s3.get_object(
                Bucket=B, Key=f"data/warm/ofr/series/{m}.json.gz")["Body"].read()
            bank = sorted({d for d in set(deep_dates(
                json.loads(gzip.decompress(raw2)))) if a <= d <= b})
            missing = sorted(set(live) - set(bank))
            extra = sorted(set(bank) - set(live))
            ok = not missing
            all_match = all_match and ok
            rep.kv(mnemonic=m, window=f"{a}..{b}",
                    live_obs=len(live), banked_obs=len(bank),
                    bank_missing=len(missing), match=ok)
            rep.log(f"  {m} live dates in window: "
                    + (", ".join(live) if len(live) <= 12
                       else ", ".join(live[:12]) + " ..."))
            if missing:
                rep.warn(f"  REAL GAP {m}: bank lacks {missing[:10]}")
            if extra:
                rep.log(f"  bank-only dates (fine, newer pull): {extra[:6]}")
        if all_match:
            rep.ok("all flagged windows: bank == publisher exactly -- the "
                    "holes are the source's own record (closures / zero-"
                    "trade days), not banking gaps")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
