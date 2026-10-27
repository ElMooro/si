"""ops 6694 - read-only: WHY the bank-built universe (ops 6692: 5,078, 95.0% of the live 5,230) differs from the live
screener universe - for each of the 263 names only the live file has and the 111 only the bank path has, the first
step that separates them, using the builder's own deployed code (bank_profile_rows / book_identity / book_allows /
is_valid_common_stock) on the runner against the same S3 objects:

  live-only:  absent from profile-bulk | exchange | not actively trading | ETF/fund flag | market cap missing/< $1M |
              book: Polygon security_type (CS, WARRANT, RIGHT, PFD, SP ...) or a name/industry heuristic flag |
              the builder's filter
  bank-only:  the book's security_type / flags, the profile row (is it a common stock the screener did not return?)

Counted by reason and by Polygon security type, with samples.  Nothing is written.  The builder module is imported
with FMP_KEY set to a placeholder (no SSM read, no FMP call).
"""
from pathlib import Path
import csv
import gzip
import io
import json
import os
import sys
from collections import Counter, defaultdict

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def main():
    os.environ.setdefault("FMP_KEY", "probe-placeholder-not-a-key")
    sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-universe-builder" / "source"))
    import lambda_function as ub  # noqa: E402
    s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
    ub.S3 = s3
    with report("ops_6694_universe_membership_diff") as r:
        r.heading("ops 6694 - live screener universe vs the bank-built universe: the reason for every difference (read-only)")
        live = json.loads(s3.get_object(Bucket=LIVE, Key=ub.S3_KEY)["Body"].read())
        live_rows = {s["symbol"]: s for s in live.get("stocks", [])}
        r.log("live: %d stocks, generated %s, by source %s" % (len(live_rows), live.get("generated_at"),
                                                             dict(Counter(s.get("source") for s in live_rows.values()))))
        # every raw profile row (unfiltered) for the reasons
        raw = {}
        for o in s3.list_objects_v2(Bucket=LIVE, Prefix=ub.BANK_PREFIX).get("Contents", []):
            if not o["Key"].endswith("/latest.csv.gz"):
                continue
            text = gzip.decompress(s3.get_object(Bucket=LIVE, Key=o["Key"])["Body"].read()).decode("utf-8", "replace")
            for row in csv.DictReader(io.StringIO(text)):
                raw.setdefault((row.get("symbol") or "").strip().upper(), row)
        rows, prov = ub.bank_profile_rows(s3)
        ident, iprov = ub.book_identity(s3)
        r.log("profile rows (raw) %d; after the stream filter %d (%s); book %s" % (len(raw), len(rows), prov.get("as_of"), json.dumps(iprov)))
        stream = {x["symbol"]: x for x in rows}
        bank = {}
        for sym, x in stream.items():
            if ub.book_allows(sym, ident) and ub.is_valid_common_stock(x):
                bank[sym] = x

        def book(sym):
            f = ident.get(sym) or ident.get(sym.replace(".", "-")) or ident.get(sym.replace("-", ".")) or {}
            flags = [k for k in ("is_etf", "is_fund", "is_otc", "is_preferred", "is_warrant_unit", "is_shell", "is_debt_or_structured") if f.get(k)]
            return f, f.get("security_type"), flags

        def why_not_bank(sym):
            p = raw.get(sym)
            if p is None:
                return "absent from profile-bulk", None
            if (p.get("exchange") or "").strip().upper() not in ub.SCREEN_EXCHANGES:
                return "exchange %s" % p.get("exchange"), None
            if not ub._truthy(p.get("isActivelyTrading")):
                return "not actively trading", None
            if ub._truthy(p.get("isEtf")) or ub._truthy(p.get("isFund")):
                return "ETF/fund flag", None
            mc = ub._num(p.get("marketCap"))
            if not mc or mc < ub.CAP_BUCKETS[0][1]:
                return "market cap %s" % p.get("marketCap"), None
            f, t, flags = book(sym)
            if not ub.book_allows(sym, ident):
                return "book: type %s flags %s" % (t, flags), (t, tuple(flags))
            return "builder filter", None
        only_live = sorted(set(live_rows) - set(bank))
        only_bank = sorted(set(bank) - set(live_rows))
        r.section("live-only: %d" % len(only_live))
        reasons, by_type, samples = Counter(), Counter(), defaultdict(list)
        for sym in only_live:
            why, key = why_not_bank(sym)
            head = why.split(":")[0] if why.startswith("book") else why.split(" ")[0] if why.startswith(("exchange", "market")) else why
            reasons[head] += 1
            if key:
                by_type["%s %s" % key] += 1
                samples["%s %s" % key].append("%s(%s)" % (sym, (live_rows[sym].get("name") or "")[:28]))
            else:
                samples[head].append("%s(%s)" % (sym, (live_rows[sym].get("name") or "")[:28]))
        r.log("by reason: %s" % dict(reasons.most_common()))
        for k, n in by_type.most_common():
            r.log("   book drop - type/flags %s: %d e.g. %s" % (k, n, samples[k][:10]))
        for k in reasons:
            if k != "book":
                r.log("   %s: e.g. %s" % (k, samples[k][:12]))
        r.section("bank-only: %d" % len(only_bank))
        bt = Counter()
        bsamples = defaultdict(list)
        for sym in only_bank:
            f, t, flags = book(sym)
            key = "type %s%s" % (t, (" flags " + ",".join(flags)) if flags else "")
            bt[key] += 1
            bsamples[key].append("%s(%s, %s)" % (sym, (bank[sym].get("companyName") or "")[:28], bank[sym].get("industry")))
        for k, n in bt.most_common():
            r.log("   %s: %d e.g. %s" % (k, n, bsamples[k][:8]))
        r.section("live universe composition by Polygon security type (the screener path today)")
        lt = Counter()
        lsamples = defaultdict(list)
        for sym, row in live_rows.items():
            f, t, flags = book(sym)
            lt[t or "untyped"] += 1
            if t not in (None, "CS", "ADRC", "OS", "GDR", "NYRS", "LT"):
                lsamples[t].append("%s mcap %.1fB" % (sym, (row.get("market_cap") or 0) / 1e9))
        r.log("live by type: %s" % dict(lt.most_common()))
        for t, xs in lsamples.items():
            r.log("   %s in the live universe: %d e.g. %s" % (t, len(xs), xs[:8]))
        r.ok("ops 6694 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
