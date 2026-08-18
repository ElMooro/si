"""ops/4890 -- detail-layer shape probe (report-only).
 (a) fundamental-census key candidates: revenue / growth /
     margin field names for the S&P subset.
 (b) spx-beaters weekly-closes ledger: shape + one symbol row
     (drives per-member 12m growth-within-industry).
Bind only what prints.
"""
import gzip
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops 4890 -- detail shapes") as rep:
        found = 0
        for key in ("data/fundamental-census.json",
                    "data/fundamental-census/matrix.json",
                    "data/census-matrix.json",
                    "data/census.json"):
            try:
                j = g(key)
            except Exception:  # noqa: BLE001
                rep.log("[%s] absent" % key)
                continue
            found += 1
            rep.ok("[%s] top=%s" % (key, list(j)[:12]))
            for holder in ("companies", "members", "rows",
                           "matrix", "by_ticker"):
                v = j.get(holder)
                if not v:
                    continue
                it = (v.get("NVDA") or v.get("AAPL")
                      or list(v.values())[0]) \
                    if isinstance(v, dict) else v[0]
                ks = sorted(it) if isinstance(it, dict) \
                    else []
                rep.log("  %s n=%d" % (holder, len(v)))
                rep.log("  fields(rev/growth/margin-ish)=%s"
                        % [k for k in ks if any(
                            s in k.lower() for s in
                            ("rev", "growth", "margin",
                             "cagr", "income", "sales"))][:24])
                rep.log("  all fields head=%s" % ks[:40])
                samp = {k: it[k] for k in ks[:14]} \
                    if isinstance(it, dict) else it
                rep.log("  sample=%s" % json.dumps(
                    samp, default=str)[:420])
                break
            break
        try:
            j = g("spx-beaters/weekly-closes.json")
        except Exception:
            try:
                j = g("data/spx-beaters/weekly-closes.json")
            except Exception as e:  # noqa: BLE001
                rep.fail("closes ledger absent: %s"
                         % str(e)[:60])
                sys.exit(1)
        rep.ok("closes ledger top=%s" % list(j)[:8])
        rows = j.get("rows") or j.get("closes") or {}
        rep.log("  holder n=%d" % len(rows))
        it = rows.get("NVDA")
        if isinstance(rows, dict) and it is None:
            k0 = list(rows)[0]
            it = rows[k0]
            rep.log("  (NVDA absent; sample key=%s)" % k0)
        rep.log("  symbol row type=%s sample=%s"
                % (type(it).__name__,
                   json.dumps(it, default=str)[:300]))
        if not found:
            rep.warn("census absent under all candidates -- "
                     "revenue legs will be nulled honestly")
        rep.ok("shapes on record")


if __name__ == "__main__":
    main()
