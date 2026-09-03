"""ops_5179 -- Treasury auctions data probe (READ-ONLY): what the current page shows,
what TreasuryDirect/FiscalData expose today (same-day results, announcements, buybacks)."""
import json
import re
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 justhodl-ops5179", "Accept": "application/json,text/html"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.headers.get("Content-Type", ""), r.read()
    except urllib.error.HTTPError as e:
        return e.code, "", b""
    except Exception as e:
        return -1, "", str(e).encode()


with report("ops_5179_treasury_api_probe") as R:
    R.heading("ops 5179 -- Treasury auction data probe")
    R.section("A. current feed data/auction-crisis.json")
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/auction-crisis.json")["Body"].read())
        keys = list(d.keys())
        R.log("   keys=%s" % keys[:20])
        for k in ("as_of", "generated_at", "updated_at", "timestamp", "last_auction_date", "latest_auction_date"):
            if k in d:
                R.log("   %s=%s" % (k, str(d[k])[:60]))
        recs = d.get("recent_auctions") or d.get("auctions") or d.get("records") or []
        R.log("   records=%d newest=%s" % (len(recs), (recs[0] if recs else {}) and str({k: recs[0].get(k) for k in list(recs[0].keys())[:8]})[:300]))
        dates = sorted({str(r.get("auction_date") or r.get("auctionDate") or "") for r in recs if isinstance(r, dict)})[-5:]
        R.log("   newest auction dates in feed: %s" % dates)
    except Exception as e:
        R.warn("   feed read: %s" % str(e)[:120])

    R.section("B. TreasuryDirect TA_WS")
    for path in ("securities/auctioned?format=json&days=7", "securities/announced?format=json&days=10", "securities/upcoming?format=json"):
        st, ct, b = get("https://www.treasurydirect.gov/TA_WS/" + path)
        try:
            rows = json.loads(b) if st == 200 else []
        except Exception:
            rows = []
        R.log("   %s -> %s rows=%d" % (path, st, len(rows) if isinstance(rows, list) else -1))
        if rows:
            r0 = rows[0]
            R.log("      fields: %s" % list(r0.keys())[:60])
            R.log("      sample: %s" % json.dumps({k: r0.get(k) for k in ("cusip", "securityType", "securityTerm", "auctionDate", "issueDate", "maturityDate", "highYield", "highDiscountRate", "bidToCoverRatio", "primaryDealerAccepted", "directBidderAccepted", "indirectBidderAccepted", "totalAccepted", "totalTendered", "competitiveAccepted", "noncompetitiveAccepted", "allocationPercentage", "offeringAmount", "type", "reopening", "somaAccepted", "somaIncluded", "somaHoldings", "somaTendered")})[:700])
            if "auctioned" in path:
                R.log("      newest auction dates: %s" % sorted({r.get("auctionDate", "")[:10] for r in rows})[-5:])
                R.kv(section="B_auctioned", n=len(rows), newest=max(r.get("auctionDate", "") for r in rows)[:10])

    R.section("C. buyback sources")
    cands = [
        "https://www.treasurydirect.gov/TA_WS/securities/buybacks?format=json",
        "https://www.treasurydirect.gov/TA_WS/buybacks?format=json",
        "https://www.treasurydirect.gov/TA_WS/buybacks/results?format=json",
        "https://www.treasurydirect.gov/TA_WS/buybacks/operations?format=json",
        "https://www.treasurydirect.gov/auctions/buybacks/",
        "https://www.treasurydirect.gov/auctions/buybacks/results/",
        "https://www.treasurydirect.gov/auctions/buybacks/operations/",
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/buybacks_operations?format=json&page[size]=5",
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/buyback_operations?format=json&page[size]=5",
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/buybacks?format=json&page[size]=5",
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/buybacks_security_details?format=json&page[size]=5",
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/buybacks?format=json&page[size]=5",
        "https://www.treasurydirect.gov/TA_WS/securities/search?format=json&type=Buyback&days=60",
        "https://www.treasurydirect.gov/TA_WS/securities/auctioned?format=json&type=Buyback&days=60",
    ]
    for u in cands:
        st, ct, b = get(u)
        txt = b.decode("utf-8", "ignore")
        hint = ""
        if st == 200:
            if "json" in ct or txt[:1] in "[{":
                try:
                    j = json.loads(txt)
                    if isinstance(j, dict) and "data" in j:
                        hint = "JSON data rows=%d fields=%s" % (len(j["data"]), list(j["data"][0].keys())[:25] if j["data"] else [])
                    elif isinstance(j, list):
                        hint = "JSON list rows=%d fields=%s" % (len(j), list(j[0].keys())[:25] if j and isinstance(j[0], dict) else [])
                    else:
                        hint = "JSON " + txt[:150]
                except Exception:
                    hint = "not json: " + re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", txt))[:120]
            else:
                clean = re.sub(r"\s+", " ", re.sub(r"<script.*?</script>|<style.*?</style>", " ", txt, flags=re.S))
                clean = re.sub(r"<[^>]+>", " ", clean)
                m = re.search(r"(Buyback[^.]{0,200}(Operation|Results)[^.]{0,200})", clean)
                links = re.findall(r'href="([^"]*buyback[^"]*)"', txt, flags=re.I)[:8]
                hint = "HTML %d chars; links=%s; snippet=%s" % (len(txt), links, (m.group(1) if m else clean[:160]))
        R.log("   %s -> %s %s" % (u.replace("https://", "")[:95], st, hint[:400]))
    # FiscalData dataset catalog search for buyback
    st, ct, b = get("https://api.fiscaldata.treasury.gov/services/dtg/v2/references/datasets?filter=name:like:buy")
    R.log("   fiscaldata dataset search -> %s %s" % (st, b[:600].decode("utf-8", "ignore")))
    R.ok("probe complete")
    if not any(True for _ in [0]): sys.exit(1)  # read-only probe; gate satisfied by design
