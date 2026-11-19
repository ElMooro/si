"""ops 6852 - READ-ONLY audit for the stock volatility engine: what are the quietest "stocks" really?

ops 6851's dry run put two kinds of name at the quiet end of the table:
  - exchange-traded notes and preferred shares that the factor book lists as common stock (ATLCL "6.125% Senior Notes
    due 2026", AGNCO, HBANL ...), and
  - companies whose price has not moved for weeks (TECH, MKTX, UTZ, DV, PTC after a +33% session) - a pending deal, or
    stale bars?
This reads, and writes nothing: the factor book's identity columns for those symbols and for the whole stock universe
(security_type counts), every stock that shares its name with a shorter ticker it extends, and the last banked bars of
the flat names with their volume.
"""
from pathlib import Path
from types import SimpleNamespace
import json
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-stock-volatility" / "source"))
from ops_report import report  # noqa: E402

sys.modules.setdefault("chain_guard", SimpleNamespace(begin=lambda event, context=None: 0))
import lambda_function as LF  # noqa: E402

NAMED = ["AGNCO", "AGNCL", "AGNC", "HBANL", "HBANM", "HBAN", "MBINL", "ACGLN", "TPGXL", "WAFDP", "ADAMH", "ATLCL", "ATLCZ", "XELLL", "EMP", "JSM", "CGABL", "DHCNI", "PFBC", "ARM",
         "GOOGL", "GOOG", "FOXA", "FOX", "BRK.B", "TXNM", "BPYPM", "PROV", "TECH", "MKTX", "UTZ", "DV", "PTC", "BRZE", "AES", "SLAB"]
FLAT = ["TECH", "MKTX", "UTZ", "DV", "PTC", "BRZE", "AES", "ATLCL", "AGNCO", "KO"]
FIELDS = ["mcap", "dollar_vol_20d", "ipo_date", "security_type", "is_adr", "exchange", "volatility_20d"]
FLAGS = ["is_common", "is_etf", "is_otc", "is_fund", "is_preferred", "is_warrant_unit", "is_shell", "is_adr", "is_debt_or_structured"]
DEBT_NAME = re.compile(r"\d(\.\d+)?\s*%|\bnotes?\b|\bdebentures?\b|\bsr\.?\s+nt\b|\b\dm\s+bd\b|\bbonds?\b", re.I)
PREF_NAME = re.compile(r"\bpreferred\s+(stock|shares?|units?|securities|l\.?p\.?)|\bdepositary\s+shares?\b.*\b(preferred|interest)\b|\bseries\s+[a-z]{1,2}\b|\bperpetual\b|\bcumulative\b", re.I)


def main():
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=5, read_timeout=30))
    with report("ops_6852_stock_volatility_identity_audit") as r:
        r.heading("ops 6852 - what the quietest stocks really are (read-only)")
        book = LF.bank_enrich.rows(fields=FIELDS)
        r.log("factor book rows: %d; columns on a row: %s" % (len(book), sorted(book[0].keys()) if book else None))
        by = {LF.canon(b.get("symbol")): b for b in book}

        r.section("1. the named symbols as the factor book describes them")
        for t in NAMED:
            b = by.get(t)
            if not b:
                r.log("  %-6s not in the book" % t)
                continue
            r.log("  %-6s type %-5s kind %-6s %s | mcap $%.0fM dv20 $%.1fM | %s | %s" % (
                t, b.get("security_type"), LF.kind_of(b)[0] or LF.kind_of(b)[1], " ".join(f[3:] for f in FLAGS if b.get(f)), (b.get("mcap") or 0) / 1e6, (b.get("dollar_vol_20d") or 0) / 1e6,
                b.get("exchange"), (b.get("name") or "")[:70]))

        r.section("2. the stock universe by Polygon security type")
        uni, stats = LF.select_universe(book)
        stocks = [by[t] for t, m in uni.items() if m["kind"] == "stock"]
        types = {}
        for b in stocks:
            types[b.get("security_type")] = types.get(b.get("security_type"), 0) + 1
        r.log("stocks in the universe: %d; by security_type: %s" % (len(stocks), json.dumps(types, default=str)))
        odd = [b for b in stocks if b.get("security_type") not in ("CS", "ADRC", "OS", "GDR", "NYRS", "LT")]
        r.log("stocks whose type is not a common-equity type: %d" % len(odd))
        for b in sorted(odd, key=lambda b: -(b.get("mcap") or 0))[:60]:
            r.log("  %-7s type %-5s $%.0fM  %s" % (b.get("symbol"), b.get("security_type"), (b.get("mcap") or 0) / 1e6, (b.get("name") or "")[:80]))

        r.section("3. names that read as debt or preferred")
        debt = [b for b in stocks if DEBT_NAME.search(b.get("name") or "")]
        pref = [b for b in stocks if PREF_NAME.search(b.get("name") or "") and b not in debt]
        r.log("debt by name: %d; preferred by name: %d" % (len(debt), len(pref)))
        for label, sub in (("debt", debt), ("pref", pref)):
            for b in sorted(sub, key=lambda b: str(b.get("symbol"))):
                r.log("  %s %-7s type %-5s $%.0fM  %s" % (label, b.get("symbol"), b.get("security_type"), (b.get("mcap") or 0) / 1e6, (b.get("name") or "")[:90]))

        r.section("4. stocks that share their name with a shorter ticker they extend")
        by_name = {}
        for b in stocks:
            by_name.setdefault((b.get("name") or "").strip().lower(), []).append(b)
        fam = []
        for name, group in by_name.items():
            if len(group) < 2 or not name:
                continue
            syms = sorted((str(b.get("symbol")) for b in group), key=len)
            for b in group:
                s = str(b.get("symbol"))
                base = next((x for x in syms if x != s and len(x) < len(s) and s.startswith(x)), None)
                if base:
                    fam.append((s, base, b))
        r.log("%d such lines" % len(fam))
        for s, base, b in sorted(fam):
            r.log("  %-7s extends %-5s type %-5s vol20 %s  $%.0fM dv20 $%.1fM  %s" % (s, base, b.get("security_type"), b.get("volatility_20d"), (b.get("mcap") or 0) / 1e6, (b.get("dollar_vol_20d") or 0) / 1e6, (b.get("name") or "")[:60]))

        r.section("5. the flat names: last banked bars with volume")
        for t in FLAT:
            bars = LF.read_bars(s3, t)
            r.log("%s: %d bars; last 12:" % (t, len(bars)))
            for b in bars[-12:]:
                r.log("    %s o %s h %s l %s c %s v %s" % (b["date"], b.get("o"), b.get("h"), b.get("l"), b.get("c"), b.get("v")))
            big = sorted(range(1, len(bars)), key=lambda i: -abs(bars[i]["c"] / bars[i - 1]["c"] - 1))[:1]
            for i in big:
                if len(bars) - i < 260:
                    r.log("    largest move of the series: %s %+.1f%% (%s -> %s), %d sessions ago" % (bars[i]["date"], (bars[i]["c"] / bars[i - 1]["c"] - 1) * 100, bars[i - 1]["c"], bars[i]["c"], len(bars) - 1 - i))
        r.ok("audit read: %d stocks, %d with a non-equity type, %d debt names, %d preferred names, %d extended tickers" % (len(stocks), len(odd), len(debt), len(pref), len(fam)))


if __name__ == "__main__":
    main()
    sys.exit(0)
