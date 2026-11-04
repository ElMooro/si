"""ops 6730 - READ-ONLY follow-up to ops 6729: why did the US-filtered pass miss Exxon 30231G102, Electronic Arts
285512109, Kenvue 49177J102 and Warner Bros Discovery 934423104 (the any-exchange pass then returned 100+ rows)?
Prints, for each: the full exchCode/ticker list of the any-exchange answer, the US-filtered answer, a micCode XNYS /
XNAS answer, and the banked FMP etf/info shape for a few unclassified ETF tickers (assetClass vocabulary) plus how
many etf-info objects the walker has banked. <= 4 OpenFIGI requests, ~12 bank GETs. Writes nothing but this report.
"""
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402
import openfigi  # noqa: E402
import fmp_bank  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=120)
CUSIPS = ["30231G102", "285512109", "49177J102", "934423104"]
ETFS = ["SPYM", "OEF", "IHI", "KBWB", "FMDE", "EMXC", "JAAA", "DFCF", "MAGS", "DPST", "BGGG", "VDIG"]


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6730_13f_master_us_rows_probe") as r:
        r.heading("ops 6730 - security master: US rows for the 'ambiguous' large caps + FMP etf/info vocabulary (read-only)")
        key = openfigi.get_api_key()
        deadline = time.monotonic() + 90
        r.section("1. any-exchange rows (exchCode:ticker list), US-filtered answer, MIC-filtered answers")
        jobs = ([{"idType": "ID_CUSIP", "idValue": c} for c in CUSIPS] + [{"idType": "ID_CUSIP", "idValue": c, "exchCode": "US"} for c in CUSIPS]
                + [{"idType": "ID_CUSIP", "idValue": c, "micCode": "XNYS"} for c in CUSIPS] + [{"idType": "ID_CUSIP", "idValue": c, "micCode": "XNAS"} for c in CUSIPS])
        res = openfigi.mapping(jobs, api_key=key, deadline=deadline) or []
        for j, item in zip(jobs, res):
            tag = "%s %s %s" % (j["idValue"], j.get("exchCode") or "", j.get("micCode") or "")
            if isinstance(item, dict) and "data" in item:
                rows = item["data"]
                us = [x for x in rows if x.get("exchCode") == "US"]
                r.log("%s -> %d rows; US rows: %s; first 40 exch:ticker: %s" % (tag, len(rows), json.dumps([{k: x.get(k) for k in ("ticker", "name", "securityType", "securityType2", "marketSector", "figi", "compositeFIGI")} for x in us]),
                                                                              " ".join("%s:%s" % (x.get("exchCode"), x.get("ticker")) for x in rows[:40])))
                comps = {}
                for x in rows:
                    comps.setdefault(x.get("compositeFIGI"), []).append(x.get("exchCode"))
                r.log("   composites: %s" % json.dumps({k: v[:8] for k, v in list(comps.items())[:12]}))
            else:
                r.log("%s -> %s" % (tag, json.dumps(item)[:200]))
        r.section("2. FMP bank: etf/info objects and the assetClass vocabulary")
        n = 0
        token = None
        while True:
            kw = {"Bucket": LIVE, "Prefix": "data/warm/fmp/etf-info/", "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            n += len(resp.get("Contents") or [])
            token = resp.get("NextContinuationToken")
            if not token or n > 20000:
                break
        r.log("banked etf-info objects: %d" % n)
        for t in ETFS:
            rows = fmp_bank.walker("etf-info", t)
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                x = rows[0]
                r.log("%-6s assetClass=%r name=%r cusip=%r isin=%r etfCompany=%r domicile=%r holdingsCount=%r updatedAt=%r keys=%s" % (
                    t, x.get("assetClass"), x.get("name"), x.get("securityCusip") or x.get("cusip"), x.get("isin"), x.get("etfCompany"), x.get("domicile"), x.get("holdingsCount"), x.get("updatedAt") or x.get("updated"),
                    ",".join(sorted(x.keys()))[:300]))
            else:
                r.log("%-6s not banked (%s)" % (t, type(rows).__name__))
        r.ok("probe complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
