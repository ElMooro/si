"""ops 6941 - READ-ONLY: why some of the largest borrowers have no listed company to be tied to.

Credit Before Equity 2.0.0 ties a bond issuer to a company of the factor book. The cut of ops 6940 has no row for Energy
Transfer, Sunoco, Paramount Skydance, Leggett & Platt and a few more. Before the engine is written this says, for each of them,
what the factor book holds (which flag keeps it out, or that the row is absent) and what the price bank holds (a file, its last
bar). Nothing is invoked and nothing is written.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-regime-map" / "source")]
LIVE = "justhodl-dashboard-live"
ASK = ["ET", "SUN", "PSKY", "PARA", "WBD", "USAC", "LEG", "DHC", "KW", "K", "SEE", "CCO", "TEF", "HOUS", "ATUS", "SATS", "ECHO", "MPW", "MPT", "BK", "BNY", "MMC", "MRSH", "FI", "FISV",
       "HES", "CIVI", "SM", "LNW", "CYBR", "JWN", "BRK-B", "BRK.B", "BRK-A", "PK", "WSC", "HLF", "CLVT", "CODI", "CPS", "PRMB", "IQV", "ADT", "GFL", "NGG", "TU", "RCI", "E", "EQNR", "MT",
       "UL", "DEO", "SNY", "NVO", "RELX", "STLA", "TTE", "ORAN", "BNPQY", "LYG", "ING", "BBVA", "MFG", "NMR", "ENB", "CNQ", "SU", "CVE", "TRP", "BNS", "BMO", "RY", "TD", "CM"]
FLAGS = ("is_common", "is_adr", "is_etf", "is_fund", "is_preferred", "is_warrant_unit", "is_shell", "is_debt_or_structured", "is_otc")


def main():
    import boto3
    from ops_report import report
    import bank_enrich
    import lambda_function as RM
    s3 = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)

    with report("ops_6941_credit_before_equity_listing_probe") as r:
        r.heading("ops 6941 - which listed companies the factor book holds for the largest borrowers (read-only), %s" % now.isoformat(timespec="minutes"))
        idx = bank_enrich.index() or {}
        have = set(idx.get("columns") or [])
        cols = [c for c in ("name", "sector", "industry", "mcap", "price", "exchange", "country", "security_type") + FLAGS if c in have]
        rows = {str(x.get("symbol")): x for x in bank_enrich.rows(fields=cols) or []}
        r.log("factor book: %d rows, as of %s" % (len(rows), bank_enrich.as_of() if hasattr(bank_enrich, "as_of") else None))
        n_abs = 0
        for t in ASK:
            x = rows.get(t)
            try:
                raw = s3.get_object(Bucket=LIVE, Key=RM.bank_key(t))["Body"].read()
                bars = RM.fund_bars(json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw))
                bank = "%d bars, last %s" % (len(bars), bars[-1]["date"] if bars else None)
            except Exception as e:  # noqa: BLE001
                bank = "no file (%s)" % type(e).__name__
            if not x:
                n_abs += 1
                r.log("%-6s factor book: ABSENT | price bank: %s" % (t, bank))
                continue
            on = [f for f in FLAGS if x.get(f)]
            r.log("%-6s %-44s %-5s mcap %7.1fB %-9s flags %s | price bank: %s" % (t, str(x.get("name"))[:44], x.get("security_type"), (x.get("mcap") or 0) / 1e9, x.get("exchange"), on, bank))
        # names beginning with these words, whatever the ticker is today
        for word in ("ENERGY TRANSFER", "SUNOCO", "PARAMOUNT", "LEGGETT", "DIVERSIFIED HEALTHCARE", "WARNER BROS", "USA COMPRESSION", "KENNEDY", "CLEAR CHANNEL", "TELEFONICA", "SEALED AIR", "KELLANOVA",
                     "NORDSTROM", "ECHOSTAR", "LIGHT & WONDER", "CYBERARK", "CIVITAS", "HESS"):
            hit = [(s, x.get("name"), x.get("security_type"), [f for f in FLAGS if x.get(f)], round((x.get("mcap") or 0) / 1e9, 1)) for s, x in rows.items() if str(x.get("name") or "").upper().startswith(word)]
            r.log("name begins %-24s -> %s" % (word, hit[:6]))
        r.kv(asked=len(ASK), absent=n_abs, book_rows=len(rows))
        r.ok("PROBE COMPLETE: nothing was invoked or written")


if __name__ == "__main__":
    main()
