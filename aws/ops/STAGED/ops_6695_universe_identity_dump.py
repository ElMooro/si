"""ops 6695 - read-only: the identity dataset behind the universe's membership rule, dumped for offline rule design.

ops 6694 found the LIVE screener universe carries ~190 listings that are not a company's equity (Polygon types: 77 SP
- notes such as TBB "AT&T 5.35% notes" ranked as a $140B mega cap because FMP gives them the parent's market cap -, 48
WARRANT, 20 UNIT, 16 PFD, 13 RIGHT), and that the factor book's name/symbol heuristics misfire on real common stocks
(GWW / NWSA / FLWS by the "ends in WS/WW" warrant regex, ARM and other ADRs by "depositary share", ASTS by " spac").
To design one membership rule for both universe paths, this dumps - for every symbol in the live universe or the
bank-built one - [symbol, polygon type, book flags, FMP name, industry, market cap $B, in live, in bank] as one
gzip+base64 JSON line (DATASET_B64).  Nothing is written.  The builder module is imported with FMP_KEY set to a
placeholder (no SSM read, no FMP call).
"""
from pathlib import Path
import base64
import gzip
import json
import os
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

LIVE = "justhodl-dashboard-live"


def main():
    os.environ.setdefault("FMP_KEY", "probe-placeholder-not-a-key")
    sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-universe-builder" / "source"))
    import lambda_function as ub  # noqa: E402
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 6, "mode": "standard"}, read_timeout=120))
    ub.S3 = s3
    with report("ops_6695_universe_identity_dump") as r:
        r.heading("ops 6695 - universe identity dataset for the membership rule (read-only)")
        live = {s["symbol"]: s for s in json.loads(s3.get_object(Bucket=LIVE, Key=ub.S3_KEY)["Body"].read()).get("stocks", [])}
        rows, prov = ub.bank_profile_rows(s3)
        ident, iprov = ub.book_identity(s3)
        stream = {x["symbol"]: x for x in rows if ub.is_valid_common_stock(x)}      # the bank path BEFORE any type rule
        out = []
        for sym in sorted(set(live) | set(stream)):
            f = ident.get(sym) or ident.get(sym.replace(".", "-")) or ident.get(sym.replace("-", ".")) or {}
            src = live.get(sym) or {}
            b = stream.get(sym) or {}
            flags = "".join(c for c, k in (("e", "is_etf"), ("f", "is_fund"), ("o", "is_otc"), ("p", "is_preferred"), ("w", "is_warrant_unit"),
                                           ("s", "is_shell"), ("d", "is_debt_or_structured")) if f.get(k))
            out.append([sym, f.get("security_type"), flags, (src.get("name") or b.get("companyName") or "")[:90],
                        (src.get("industry") or b.get("industry") or "")[:40], round(((src.get("market_cap") or b.get("marketCap") or 0) / 1e9), 3),
                        int(sym in live), int(sym in stream)])
        blob = base64.b64encode(gzip.compress(json.dumps(out, separators=(",", ":")).encode())).decode()
        r.log("rows %d (live %d, bank before type rule %d); book %s; bank as_of %s; blob %d chars" % (
            len(out), len(live), len(stream), iprov.get("generated_at"), prov.get("as_of"), len(blob)))
        r.log("DATASET_B64 " + blob)
        r.ok("ops 6695 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
