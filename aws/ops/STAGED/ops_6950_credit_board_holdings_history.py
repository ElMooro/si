"""ops 6950 - the bond desk's month-end holdings, kept: what the credit board needs to say what a stock did after credit moved.

The board measures credit from the bonds four broad funds hold (the corporate bonds desk's own feed). That feed still serves
any earlier month-end (ops 6949: back to 2020 at least). The board's ledger begins 2026-11-05, so today it cannot answer the
question it exists for - when the bond market marked a company down and the stock had not followed, what did the stock do
next? This run reads the month-ends from 2021-07 on (the price bank begins 2021-08-26) and keeps, for each:

  credit/cbe-holdings/<day>.json.gz   every bond of that day as the bond desk itself assembles it (issuer, sector, country,
                                      currency, book, coupon, maturity, yield, price, duration, market value held) under the
                                      bond desk's own key - the same key the daily document uses; no ISIN or CUSIP is kept
  credit/cbe-spreads/<day>.json.gz    that day's spreads in the board's ledger (the engine's own arithmetic on that day's
                                      Treasury curve), a real day; it replaces a day the board had only worked back

One request at a time, 2.5 seconds apart, four funds a month-end (USIG, USHY, CEMB, EMHY: the broad US-dollar books).
It can be run again: a month-end already kept is not asked for again. A month-end the feed does not serve is named and
skipped. Nothing is invoked; the daily engine and its document are not touched.
"""
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import gzip
import json
import os
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-corp-bonds" / "source"),
                str(ROOT / "aws" / "lambdas" / "justhodl-credit-before-equity" / "source")]
LIVE = "justhodl-dashboard-live"
HOLD_ROOT = "credit/cbe-holdings/"
LEDGER_ROOT = "credit/cbe-spreads/"
FUNDS = ("USIG", "USHY", "CEMB", "EMHY")
FIRST = os.environ.get("CBE_FIRST", "2021-07")
LAST = os.environ.get("CBE_LAST", "2026-10")
PAUSE = 2.5
BUDGET_S = 50 * 60                       # the runner allows 60 minutes; stop cleanly before it and say where
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"       # the bond desk's own
COLS = ["k", "issuer", "sector", "country", "ccy", "book", "coupon", "maturity", "issued", "rate", "price", "dur", "demand", "nf"]


def month_ends(first, last):
    """The last weekday of every month from first to last (YYYY-MM), oldest first."""
    y, m = (int(x) for x in first.split("-"))
    ly, lm = (int(x) for x in last.split("-"))
    out = []
    while (y, m) <= (ly, lm):
        d = date(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1)
        while d.weekday() > 4:
            d -= timedelta(days=1)
        out.append(d)
        y, m = y + (m == 12), m % 12 + 1
    return out


def fetch(url, timeout=90):
    import urllib.request
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json,*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def put(s3, key, doc):
    body = gzip.compress(json.dumps(doc, separators=(",", ":")).encode("utf-8"), 6)
    s3.put_object(Bucket=LIVE, Key=key, Body=body, ContentType="application/json", ContentEncoding="gzip", CacheControl="no-store")
    return len(body)


def main():
    import boto3
    from ops_report import report
    import corp_bonds_issues as BI
    import cbe_core as C
    s3 = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)
    started = time.monotonic()
    funds = {f["id"]: f for f in BI.FUNDS}

    with report("ops_6950_credit_board_holdings_history") as r:
        r.heading("ops 6950 - month-end bond holdings kept for the credit board, %s to %s, %s" % (FIRST, LAST, now.isoformat(timespec="minutes")))
        raw = s3.get_object(Bucket=LIVE, Key="data/warm/treasury-par/curve.json.gz")["Body"].read()
        par_rows = json.loads(gzip.decompress(raw)).get("rows") or {}
        have = set()
        token = None
        while True:
            got = s3.list_objects_v2(Bucket=LIVE, Prefix=HOLD_ROOT, **({"ContinuationToken": token} if token else {}))
            have |= {o["Key"][len(HOLD_ROOT):len(HOLD_ROOT) + 10] for o in got.get("Contents") or []}
            token = got.get("NextContinuationToken")
            if not token:
                break
        ledger = {}
        got = s3.list_objects_v2(Bucket=LIVE, Prefix=LEDGER_ROOT)
        for o in got.get("Contents") or []:
            ledger[o["Key"][len(LEDGER_ROOT):len(LEDGER_ROOT) + 10]] = o["Size"]
        wanted = month_ends(FIRST, LAST)
        r.log("month-ends asked for: %d (%s .. %s); already kept: %d; ledger days before: %d" % (len(wanted), wanted[0], wanted[-1], len(have), len(ledger)))

        done, skipped, failed, stopped = [], [], [], None
        for d in reversed(wanted):                                   # newest first: the months the live board needs come before the long history
            if any((d - timedelta(days=back)).isoformat() in have for back in range(4)):
                skipped.append(d.isoformat())
                continue
            if time.monotonic() - started > BUDGET_S:
                stopped = d.isoformat()
                break
            results, as_ofs, note = [], set(), None
            for back in range(4):                                    # a month whose last weekday was a holiday: the day before, up to three
                ask = d - timedelta(days=back)
                if ask.weekday() > 4:
                    continue
                results, as_ofs = [], set()
                for fid in FUNDS:
                    try:
                        blob = fetch(BI._api_url(funds[fid], ask.strftime("%Y%m%d")))
                        rows, as_of, _ = BI.parse_api(json.loads(blob.decode("utf-8")), funds[fid])
                    except Exception as e:  # noqa: BLE001
                        rows, as_of, note = [], None, "%s %s %s" % (fid, type(e).__name__, str(e)[:80])
                    time.sleep(PAUSE)
                    if rows and as_of:
                        results.append((funds[fid], rows, "api", as_of))
                        as_ofs.add(as_of)
                main_day = next((got for f, _, _, got in results if f["id"] == "USIG"), None)          # the broad investment-grade book dates the month-end
                if main_day and abs((date.fromisoformat(main_day) - d).days) <= 4:
                    results = [x for x in results if abs((date.fromisoformat(x[3]) - date.fromisoformat(main_day)).days) <= 4]
                    as_ofs = {main_day}
                    break
                results = []
            if not results:
                failed.append((d.isoformat(), note or "the feed returned no holdings near that day (dates returned: %s)" % sorted(as_ofs)))
                r.log("%s: NOT KEPT - %s" % (d, failed[-1][1]))
                if len(failed) >= 6 and not done:
                    stopped = "six month-ends in a row failed; the feed is not answering this runner"
                    break
                continue
            day = next(iter(as_ofs))
            issues = BI.assemble(results, None)
            bonds = [b for b in issues["bonds"] if not b.get("left")]
            rows = [[b.get(c) for c in COLS] for b in bonds]
            n_hold = put(s3, HOLD_ROOT + day + ".json.gz", {"as_of": day, "asked": d.isoformat(), "funds": issues["funds"], "cols": COLS, "rows": rows,
                                                           "source": "the corporate bonds desk's own feed, dated holdings; assembled by the bond desk's own assemble()",
                                                           "written_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "written_by": "ops 6950"})
            points, counts, par_days = C.bond_points(bonds, day, par_rows, {})
            snap = C.snapshot_of(points, day)
            n_led, said = None, ""
            ok = bool(par_days.get("as_of")) and snap["n"] >= 3000
            if ok and day in ledger:                                 # a day the ledger already holds: a real day stays; a worked-back day is replaced only when the two agree
                old = json.loads(gzip.decompress(s3.get_object(Bucket=LIVE, Key=LEDGER_ROOT + day + ".json.gz")["Body"].read()))
                both = sorted(abs(v - snap["bonds"][k]) for k, v in (old.get("bonds") or {}).items() if k in snap["bonds"])
                mid = both[len(both) // 2] if both else None
                p90 = both[int(0.9 * (len(both) - 1))] if both else None
                said = " | the ledger held %d bonds for that day (%s); %d in both, they differ by %s bp in the middle and %s bp at the ninth decile" % (
                    len(old.get("bonds") or {}), "worked back from %s" % old.get("derived_from") if old.get("derived_from") else "a real day", len(both), mid, p90)
                if not old.get("derived_from"):
                    ok, said = False, said + " -> the real day is kept"
                elif mid is None or mid > 5.0 or len(both) < 0.8 * len(old.get("bonds") or {}):
                    ok, said = False, said + " -> THEY DISAGREE, the ledger day is left as it was"
                    failed.append((day, "ledger day not replaced: the worked-back day and the dated holdings disagree"))
            if ok:
                snap["source"] = "the bond desk's dated holdings of that day (ops 6950)"
                n_led = put(s3, LEDGER_ROOT + day + ".json.gz", snap)
            done.append(day)
            have.add(day)
            r.log("%s: %d bonds of %d funds kept (%d KB); %d with a spread on the curve of %s -> ledger %s%s" % (
                day, len(rows), len(results), n_hold // 1000, snap["n"], par_days.get("as_of"), ("%d KB" % (n_led // 1000)) if n_led else "NOT written",
                said))

        r.section("What is kept now")
        got = s3.list_objects_v2(Bucket=LIVE, Prefix=HOLD_ROOT)
        kept = sorted(o["Key"][len(HOLD_ROOT):len(HOLD_ROOT) + 10] for o in got.get("Contents") or [])
        size = sum(o["Size"] for o in got.get("Contents") or [])
        r.log("dated holdings: %d month-ends, %s .. %s, %.1f MB in all" % (len(kept), kept[:1], kept[-1:], size / 1e6))
        r.log("this run: kept %d, already there %d, not served %d%s" % (len(done), len(skipped), len(failed), ("; STOPPED at %s - run again to go on" % stopped) if stopped else ""))
        if failed:
            r.log("not served: %s" % failed[:20])
        r.kv(kept_this_run=len(done), already_there=len(skipped), not_served=len(failed), month_ends_kept=len(kept), stopped=bool(stopped))
        if stopped and not done:
            r.fail("nothing was kept: %s" % stopped)
            sys.exit(1)
        r.ok("%d month-ends kept in all; %s" % (len(kept), "run again to go on from %s" % stopped if stopped else "the range asked for is complete"))


if __name__ == "__main__":
    main()
