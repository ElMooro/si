"""ops 6940 - READ-ONLY: a cut of everything Credit Before Equity 2.0.0 will read, so the engine is built on the real thing.

Nothing is invoked and nothing is written. The cut is embedded in the report (gzip, base64):

  bonds     every bond of the corporate bonds desk (data/corp-bonds.json), the columns the board needs
  cds       every corporate name of the CDS desk (data/cds-desk.json): priced rows and the unpriced lists
  book      the listed companies of the factor book: symbol, name, sector, industry, size
  par       the Treasury par curve since 2025-12-01
  closes    split-repaired closes of every listed company worth $100M or more, on the bond desk's reference days and the
            last sessions (the Risk Map engine's own reader and split rule)
  model     the structural model's rows (cds-monitor), today's board and its ledger
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import base64
import gzip
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-regime-map" / "source")]
LIVE = "justhodl-dashboard-live"
BOND_COLS = ["issuer", "sector", "country", "ccy", "coupon", "maturity", "rate", "chg", "chg_y", "price", "dpx", "dur", "demand", "book", "nf", "issued"]
BOOK_COLS = ["name", "sector", "industry", "mcap", "price", "exchange", "country", "is_common", "is_adr", "is_etf", "is_fund", "is_preferred", "is_warrant_unit", "is_shell",
             "is_debt_or_structured", "is_otc", "security_type", "cik", "ret_5d", "ret_21d", "ret_63d", "dollar_vol_20d", "debt_to_equity", "interest_coverage", "altman_z", "ev", "volatility_20d"]


def main():
    import boto3
    from ops_report import report
    import bank_enrich
    import lambda_function as RM
    s3 = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)

    def get(key):
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)

    with report("ops_6940_credit_before_equity_inputs") as r:
        r.heading("ops 6940 - a cut of what Credit Before Equity will read (read-only), %s" % now.isoformat(timespec="minutes"))
        cb = get("data/corp-bonds.json")
        iss = cb.get("issues") or {}
        bonds = iss.get("bonds") or []
        cut = {"cut_at": now.isoformat(timespec="seconds"),
               "bonds": {"as_of": iss.get("as_of"), "move_as_of": iss.get("move_as_of"), "ytd_as_of": iss.get("ytd_as_of"), "generated_at": cb.get("generated_at"), "version": cb.get("version"),
                         "funds": iss.get("funds"), "note": iss.get("note"), "cols": BOND_COLS, "rows": [[b.get(k) for k in BOND_COLS] for b in bonds]}}
        r.log("bonds: %d, as of %s, changes since %s, year to date since %s" % (len(bonds), iss.get("as_of"), iss.get("move_as_of"), iss.get("ytd_as_of")))

        cds = get("data/cds-desk.json")
        keep = lambda x: {k: v for k, v in x.items() if k not in ("spark", "curve_last", "read", "sign_sources_20d", "candidates")}
        cut["cds"] = {"as_of": cds.get("as_of"), "generated_at": cds.get("generated_at"), "version": cds.get("version"), "source": cds.get("source"), "groups": {}}
        for g in ("us_corp", "global_corp"):
            blk = (cds.get("groups") or {}).get(g) or {}
            cut["cds"]["groups"][g] = {"rows": [keep(x) for x in blk.get("rows") or []], "unpriced": [keep(x) for x in blk.get("unpriced") or []], "dormant": [keep(x) for x in blk.get("dormant") or []],
                                       **{k: blk.get(k) for k in ("n_tracked", "n_priced", "n_liquid", "n_unpriced", "n_dormant", "median_bp")}}
            r.log("cds %s: %d priced, %d unpriced, %d dormant" % (g, len(blk.get("rows") or []), len(blk.get("unpriced") or []), len(blk.get("dormant") or [])))

        idx = bank_enrich.index() or {}
        have = set(idx.get("columns") or [])
        cols = [c for c in BOOK_COLS if c in have]
        rows = bank_enrich.rows(fields=cols) or []
        bad = ("is_etf", "is_fund", "is_preferred", "is_warrant_unit", "is_shell", "is_debt_or_structured")
        listed = [x for x in rows if not any(x.get(k) for k in bad) and (x.get("mcap") or 0) >= 5e7 and x.get("name")]
        cut["book"] = {"as_of": bank_enrich.as_of() if hasattr(bank_enrich, "as_of") else None, "cols": ["symbol"] + cols, "rows": [[x.get("symbol")] + [x.get(c) for c in cols] for x in listed]}
        r.log("factor book: %d rows, %d listed companies kept (not a fund, preferred, warrant, shell or note; $50M or more); columns %s" % (len(rows), len(listed), cols))

        par = get("data/warm/treasury-par/curve.json.gz")
        prow = par.get("rows") or {}
        cut["par"] = {"as_of": par.get("as_of"), "source": par.get("source"), "tenors": par.get("tenors"), "rows": {d: v for d, v in prow.items() if d >= "2025-12-01"}}
        r.log("Treasury par curve: %s .. %s, %d days kept, tenors %s, last row %s" % (par.get("first"), par.get("last"), len(cut["par"]["rows"]), par.get("tenors"), json.dumps(prow.get(max(prow)) if prow else None)[:300]))

        cm = get("data/cds-monitor.json")
        snc = cm.get("single_name_cds") or {}
        cut["model"] = {"generated_at": cm.get("generated_at"), "single_name_cds": {k: snc.get(k) for k in ("banks", "corporates", "model", "risk_free_1y_pct", "recovery_assumption")},
                        "board": get("data/credit-before-equity.json"), "ledger": get("credit/credit-before-equity-history.json")}

        # split-repaired closes on the days that matter, by the Risk Map engine's own reader
        as_of = iss.get("as_of") or now.date().isoformat()
        first = "2025-11-15"
        want = sorted({x["symbol"] for x in listed if (x.get("mcap") or 0) >= 1e8})
        recorded, n_files = RM.recorded_splits(s3, first, now.date().isoformat(), set(want))
        start = (date.fromisoformat(as_of) - timedelta(days=75)).isoformat()
        keep_days = {iss.get("ytd_as_of"), iss.get("move_as_of"), "2025-12-31"}

        def one(t):
            try:
                doc = get(RM.bank_key(t))
                bars, events, jumps = RM.repair(RM.fund_bars(doc), recorded.get(t))
                pts = {b["date"]: round(b["c"], 4) for b in bars if b["date"] >= start or b["date"] in keep_days or "2025-12-24" <= b["date"] <= "2026-01-02"}
                return t, pts, [{"date": e.get("date"), "factor": e.get("factor")} for e in events if (e.get("date") or "") >= "2025-12-01"], len(jumps)
            except Exception as e:  # noqa: BLE001
                return t, None, type(e).__name__, 0
        with ThreadPoolExecutor(32) as pool:
            res = list(pool.map(one, want))
        days = sorted({d for _, pts, _, _ in res if pts for d in pts})
        cut["closes"] = {"days": days, "rows": {t: [pts.get(d) for d in days] for t, pts, _, _ in res if pts}, "splits": {t: ev for t, pts, ev, _ in res if pts and ev},
                         "missing": [t for t, pts, _, _ in res if not pts], "reader": "justhodl-regime-map fund_bars + repair (leaders_tape)"}
        r.log("closes: %d of %d companies read, %d days kept (%s .. %s), %d with a split rebased since December, %d not in the bank: %s" % (
            len(cut["closes"]["rows"]), len(want), len(days), days[0] if days else None, days[-1] if days else None, len(cut["closes"]["splits"]), len(cut["closes"]["missing"]), cut["closes"]["missing"][:30]))

        raw = gzip.compress(json.dumps(cut, separators=(",", ":"), default=str).encode(), 9)
        b64 = base64.b64encode(raw).decode("ascii")
        r.lines.append("<!-- FIXTURE cbe-inputs %d bytes" % len(raw))
        r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
        r.lines.append("FIXTURE-END -->")
        r.kv(bonds=len(bonds), cds_priced=sum(len(g["rows"]) for g in cut["cds"]["groups"].values()), listed=len(listed), closes=len(cut["closes"]["rows"]), fixture_kb=len(raw) // 1024)
        r.ok("CUT COMPLETE: nothing was invoked or written")


if __name__ == "__main__":
    main()
