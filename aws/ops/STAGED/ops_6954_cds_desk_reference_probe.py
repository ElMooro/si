"""ops 6954 - READ-ONLY probe for CDS desk 2.0: what the warehouse holds to put beside a name.

  1  the Global Sovereign desk's document: its fields, so a reference CDS quote can stand beside a sovereign the DTCC tape
     does not price
  2  the FMP bank's company profiles: is there a worldwide list of listed companies (exchange, country) to tie a CDS name to
Nothing is invoked and nothing is written.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import csv
import gzip
import io
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared")]
LIVE = "justhodl-dashboard-live"


def main():
    import boto3
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)
    with report("ops_6954_cds_desk_reference_probe") as r:
        r.heading("ops 6954 - what the warehouse holds to put beside a CDS name (read-only), %s" % now.isoformat(timespec="minutes"))
        r.section("1. The Global Sovereign desk's document")
        raw = s3.get_object(Bucket=LIVE, Key="data/global-sovereign.json")["Body"].read()
        doc = json.loads(raw)
        r.log("keys: %s" % {k: type(v).__name__ for k, v in doc.items()})
        cs = doc.get("countries")
        items = list(cs.values()) if isinstance(cs, dict) else (cs or [])
        r.log("countries: %s, %d entries; keys of the container: %s" % (type(cs).__name__, len(items), list(cs)[:6] if isinstance(cs, dict) else None))
        for c in items[:3]:
            r.log("  %s" % json.dumps(c)[:900])
        with_cds = [c for c in items if isinstance(c, dict) and any(isinstance(c.get(k), (int, float)) for k in c if "cds" in k.lower())]
        r.log("entries with a CDS figure: %d | fields that read like CDS: %s" % (len(with_cds), sorted({k for c in items if isinstance(c, dict) for k in c if "cds" in k.lower()})))
        r.log("every entry, name and CDS fields: %s" % json.dumps([[c.get("name") or c.get("country"), c.get("iso3") or c.get("iso") or c.get("code")] + [c.get(k) for k in sorted(c) if "cds" in k.lower()][:4] for c in items if isinstance(c, dict)])[:3500])

        r.section("2. The FMP bank: company profiles worldwide")
        tops = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix="data/warm/fmp-bank/", Delimiter="/"):
            tops += [p["Prefix"] for p in page.get("CommonPrefixes") or []] + [o["Key"] for o in page.get("Contents") or []]
        r.log("top of data/warm/fmp-bank/ (%d): %s" % (len(tops), [t.replace("data/warm/fmp-bank/", "") for t in tops][:80]))
        prof = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix="data/warm/fmp-bank/", PaginationConfig={"MaxItems": 4000}):
            for o in page.get("Contents") or []:
                if "profile" in o["Key"].lower():
                    prof.append((o["Key"], o["Size"], o["LastModified"].strftime("%Y-%m-%d")))
        r.log("objects with 'profile' in the key (%d): %s" % (len(prof), prof[:20]))
        ex, co, n, header, isin = Counter(), Counter(), 0, None, 0
        sample = []
        for key, size, _ in sorted(prof, key=lambda p: p[0]):
            if size > 400_000_000:
                continue
            body = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
            if body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
            txt = body.decode("utf-8", "replace")
            if txt.lstrip().startswith(("[", "{")):
                try:
                    rows = json.loads(txt)
                    rows = rows if isinstance(rows, list) else rows.get("rows") or rows.get("data") or []
                except Exception:
                    rows = []
            else:
                rows = list(csv.DictReader(io.StringIO(txt)))
            for x in rows:
                if not isinstance(x, dict):
                    continue
                header = header or sorted(x)
                n += 1
                ex[str(x.get("exchange") or x.get("exchangeShortName") or "")] += 1
                co[str(x.get("country") or "")] += 1
                isin += 1 if x.get("isin") else 0
                if len(sample) < 3:
                    sample.append({k: str(v)[:40] for k, v in x.items() if k not in ("description", "isin", "cusip", "cik", "image")})
        r.log("profile rows read: %d | fields: %s" % (n, header))
        r.log("by exchange (top 45): %s" % ex.most_common(45))
        r.log("by country (top 45): %s" % co.most_common(45))
        r.log("rows with an ISIN on file: %d | samples: %s" % (isin, json.dumps(sample)[:1500]))
        r.kv(sovereign_entries=len(items), sovereign_with_cds=len(with_cds), profile_rows=n, exchanges=len(ex), countries=len(co))
        r.ok("PROBE COMPLETE: nothing was invoked or written")


if __name__ == "__main__":
    main()
