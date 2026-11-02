"""ops 6713 - READ-ONLY: which Treasury FiscalData auctions query answers today.

treasury-api (released by ops 6712) asks
  /services/api/fiscal_service/v1/accounting/od/auctions_query?filter=bid_to_cover_ratio:not.is.null,
  auction_date:gte:<5y ago>&sort=-auction_date&limit=500
and FiscalData answers HTTP 400 - no data since 2026-05-28.  This fetches the public endpoint with candidate forms
(documented operators only: filter=<field>:gte:<v>, sort, page[size]) from the runner and reports status, the error
body, the row count and the fields of the newest row, so the fix uses a form proven live.  Public API, no key.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query"
START = (datetime.now(timezone.utc) - timedelta(days=5 * 365)).strftime("%Y-%m-%d")
CANDIDATES = {
    "current code": "?filter=bid_to_cover_ratio:not.is.null,auction_date:gte:%s&sort=-auction_date&limit=500" % START,
    "no null filter, limit": "?filter=auction_date:gte:%s&sort=-auction_date&limit=500" % START,
    "no null filter, page[size]": "?filter=auction_date:gte:%s&sort=-auction_date&page%%5Bsize%%5D=500" % START,
    "page[size] 10000": "?filter=auction_date:gte:%s&sort=-auction_date&page%%5Bsize%%5D=10000" % START,
}
REQUIRED = ["bid_to_cover_ratio", "total_accepted", "total_tendered", "high_yield", "avg_med_yield", "security_type", "security_term", "auction_date"]


def main():
    with report("ops_6713_fiscaldata_auctions_probe") as r:
        r.heading("ops 6713 - Treasury FiscalData auctions_query: which form answers (read-only, public)")
        for name, q in CANDIDATES.items():
            req = urllib.request.Request(BASE + q, headers={"User-Agent": "justhodl-ops/6713", "Accept": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    doc = json.loads(resp.read())
                rows = doc.get("data") or []
                meta = doc.get("meta") or {}
                newest = rows[0] if rows else {}
                complete = [x for x in rows if all(x.get(f) not in (None, "", "null") for f in REQUIRED[:5])]
                r.ok("%s: HTTP 200, %d rows (total-count %s, total-pages %s), %d with all five auction fields; newest %s" % (
                    name, len(rows), meta.get("total-count"), meta.get("total-pages"), len(complete),
                    json.dumps({k: newest.get(k) for k in ["auction_date", "security_type", "security_term", "high_yield", "bid_to_cover_ratio"]})))
                r.log("   fields present of the required: %s | missing %s" % ([f for f in REQUIRED if f in newest], [f for f in REQUIRED if f not in newest]))
                if complete:
                    c = complete[0]
                    r.log("   newest complete: %s" % json.dumps({k: c.get(k) for k in ["auction_date", "security_type", "security_term", "high_yield", "bid_to_cover_ratio", "total_tendered", "total_accepted", "avg_med_yield"]}))
            except urllib.error.HTTPError as e:
                r.warn("%s: HTTP %s %s" % (name, e.code, e.read()[:400].decode("utf-8", "replace")))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s %s" % (name, type(e).__name__, str(e)[:200]))
        r.ok("ops 6713 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
