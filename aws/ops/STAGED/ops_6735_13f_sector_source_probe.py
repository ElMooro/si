"""ops 6735 - READ-ONLY probe for the 13F sector layer (Khalid 2026-11-04: "which specific sectors - semis, not just
tech - is smart money pouring money into and pulling out of").

Source candidate: the banked FMP profile bulk (data/warm/fmp-bank/profile-bulk, fmp_bank.snapshot("profile")) - FMP's
own `sector` and `industry` per symbol, plus its `cusip` / `cik` / isAdr / isEtf flags. Prints: whether the bulk is
banked and how old, row count, columns, the sector and industry vocabularies with counts, coverage of the book's
resolved stock tickers (sector known / unknown by count and by reported value), and how many of the identities OpenFIGI
left unresolved (no_match / ambiguous / typed-only) match a profile row by CUSIP (the sanctioned SEC/FMP-first bridge).
S3 reads only. Writes nothing but this report.
"""
from collections import Counter
from decimal import Decimal
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
import fmp_bank  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6735_13f_sector_source_probe") as r:
        r.heading("ops 6735 - 13F sector layer: FMP profile bulk as the sector/industry source (read-only)")
        r.section("1. the profile bulk")
        man = fmp_bank.manifest() or {}
        r.log("bank manifest as_of %s | endpoints served: %s" % (fmp_bank.as_of(), ", ".join((man.get("endpoints_served") or [])[:40])))
        t0 = time.time()
        rows = fmp_bank.snapshot("profile") or []
        r.log("profile rows: %d in %.1fs (last answer %s)" % (len(rows), time.time() - t0, json.dumps(fmp_bank.last_answer())))
        if not rows:
            r.fail("profile bulk not banked - the sector layer needs another source")
            raise SystemExit(1)
        cols = sorted(rows[0].keys())
        r.log("columns: %s" % ", ".join(cols))
        sectors = Counter(str(x.get("sector") or "").strip() for x in rows)
        inds = Counter(str(x.get("industry") or "").strip() for x in rows)
        r.log("sector vocabulary (%d): %s" % (len(sectors), json.dumps(sectors.most_common(40))))
        r.log("industry vocabulary (%d); most common 80: %s" % (len(inds), json.dumps(inds.most_common(80))))
        us = [x for x in rows if str(x.get("exchange") or "").upper() in ("NASDAQ", "NYSE", "AMEX", "NYSEARCA", "BATS", "OTC", "PNK", "NYSEAMERICAN", "CBOE")]
        r.log("US-exchange rows: %d; with cusip: %d; isEtf true: %d; isAdr true: %d; isFund true: %d" % (
            len(us), sum(1 for x in us if str(x.get("cusip") or "").strip()), sum(1 for x in us if str(x.get("isEtf")).lower() == "true"),
            sum(1 for x in us if str(x.get("isAdr")).lower() == "true"), sum(1 for x in us if str(x.get("isFund")).lower() == "true")))
        by_sym = {}
        by_cusip = {}
        for x in rows:
            sym = str(x.get("symbol") or "").upper().strip()
            if sym and sym not in by_sym:
                by_sym[sym] = x
            c = str(x.get("cusip") or "").upper().strip()
            if c and c not in by_cusip:
                by_cusip[c] = sym
        r.log("distinct symbols %d, distinct cusips %d" % (len(by_sym), len(by_cusip)))
        r.section("2. coverage of the book's identities")
        master = json.loads(s3.get_object(Bucket=LIVE, Key="data/13f-security-master.json")["Body"].read()).get("by_cusip") or {}
        book = json.loads(s3.get_object(Bucket=LIVE, Key="data/13f-book.json")["Body"].read())
        snap = json.loads(s3.get_object(Bucket=LIVE, Key="data/13f-book/periods/%s.json" % book["book"]["required_period"])["Body"].read())
        known = Counter()
        known_v = Counter()
        unknown_examples = []
        for pid, cols_ in (snap.get("by_identity") or {}).items():
            ticker, kind, bucket, n_holders, value, n_prior, cusip = cols_
            if kind != "stock" or not n_holders:
                continue
            v = Decimal(str(value or 0))
            prof = by_sym.get(str(ticker or "").upper()) if ticker else None
            if prof is None and cusip:
                sym = by_cusip.get(str(cusip).upper())
                prof = by_sym.get(sym) if sym else None
                key = "by_cusip" if prof else "none"
            else:
                key = "by_ticker" if prof else "none"
            if prof and str(prof.get("sector") or "").strip():
                known[key] += 1
                known_v[key] += v
            else:
                known["no_sector"] += 1
                known_v["no_sector"] += v
                if len(unknown_examples) < 40:
                    unknown_examples.append((ticker, cusip, float(v), (prof or {}).get("companyName")))
        tot_v = sum(known_v.values()) or Decimal(1)
        r.log("held stock identities: %s | by value: %s" % (json.dumps(known), json.dumps({k: "%.2f%%" % (100 * float(v / tot_v)) for k, v in known_v.items()})))
        r.log("largest without a sector: %s" % json.dumps(sorted(unknown_examples, key=lambda t: -t[2])[:40]))
        r.section("3. the identities OpenFIGI left unresolved: FMP cusip bridge")
        hit, miss, examples = 0, 0, []
        for c, row in master.items():
            if row.get("status") == "resolved" and row.get("ticker"):
                continue
            sym = by_cusip.get(c)
            if sym:
                hit += 1
                if len(examples) < 30:
                    examples.append((c, sym, (by_sym.get(sym) or {}).get("companyName"), (by_sym.get(sym) or {}).get("sector"), (by_sym.get(sym) or {}).get("industry"), row.get("status")))
            else:
                miss += 1
        r.log("unresolved/unticked master rows: %d match a profile cusip, %d do not" % (hit, miss))
        r.log("examples: %s" % json.dumps(examples))
        for c in ("30231G102", "285512109", "49177J102", "934423104", "25434V500", "25434V708"):
            sym = by_cusip.get(c)
            r.log("  %s -> %s %s" % (c, sym, json.dumps({k: (by_sym.get(sym) or {}).get(k) for k in ("companyName", "sector", "industry", "isEtf", "isAdr", "exchange")}) if sym else "-"))
        r.ok("probe complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
