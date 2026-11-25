"""ops 6958 - CDS desk 2.0: the raw prints behind one stray day (read-only).

The fourth dry run of 2.0 still reads The Hertz Corporation at 49 bp on 2026-04-20, between days at 4,000 bp and more.
Six prints that day carry "49" as a quoted spread. This op reads the public DTCC files around that day and prints those
rows as the file wrote them (no reference-obligation identifier is read or printed), and how the staged 2.0 engine
reads each, so the rule that should catch them can be written from the rows and not from a guess.

Nothing is invoked or written.
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
STAGING = ROOT / "aws" / "ops" / "staging" / "cds2"
sys.path[:0] = ([str(STAGING)] if STAGING.exists() else []) + [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-cds-desk" / "source")]
FIELDS = ("Execution Timestamp", "Expiration Date", "Platform identifier", "Cleared", "Notional amount-Leg 1", "Notional currency-Leg 1", "Fixed rate-Leg 1", "Spread-Leg 1", "Spread notation-Leg 1",
          "Price", "Price notation", "Other payment amount", "Other payment type", "Package indicator", "UPI FISN", "Action type", "Event type")
WATCH = {"2026-04-20": ("HERTZ",), "2026-04-21": ("HERTZ",), "2026-05-26": ("HERTZ",), "2026-05-27": ("HERTZ",), "2026-03-20": ("DISH DBS",), "2026-03-23": ("DISH DBS",)}


def main():
    from ops_report import report
    import cds_desk as C
    import lambda_function as L
    L.DRY = True
    par = L.par_curve()
    with report("ops_6958_cds_desk_quoted_points_probe") as r:
        r.heading("6958 - the raw prints behind a stray day (read-only)")
        for day, names in WATCH.items():
            for source in ("sec", "cftc"):
                raw = L.fetch_zip(source, day)
                if raw is None:
                    r.log("%s %s: no file" % (day, source))
                    continue
                n = 0
                for row in C.read_dissemination_zip(raw):
                    under = (row.get("Underlying Asset Name") or "").upper()
                    if not any(w in under for w in names):
                        continue
                    t = C.normalize_trade(row, source)
                    if not t or (t["quoted_bp"] is None and n > 6):
                        continue
                    n += 1
                    rate = L.rate_for(t["date"], par)
                    cash = round(t["upfront"] / t["notional"] * 100.0, 3) if t.get("upfront") is not None and t.get("notional") else None
                    r.log("%s %s | %s | tenor %s bucket %s coupon %s quoted %s (%s) cash%% %s | status %s points_quote %s cands %s | raw %s" % (
                        day, source, (row.get("Underlying Asset Name") or "")[:30], t["tenor"], t["bucket"], t["coupon_bp"], t["quoted_bp"], t.get("quote_how"), cash,
                        C.quote_status(t, rate) if t["quoted_bp"] is not None else "-", C.points_quote(t), [(round(v, 1), b) for v, b in C.candidate_spreads(dict(t), rate)],
                        {k: row.get(k) for k in FIELDS if row.get(k) not in (None, "")}))
                r.log("%s %s: %d rows shown" % (day, source, n))
        r.ok("PROBE COMPLETE: nothing was invoked or written")


if __name__ == "__main__":
    main()
