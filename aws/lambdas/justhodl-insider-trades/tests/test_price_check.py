"""The insider engine's price check against the tape (v3.1, audit of 2026-11-26). No network: the factor book is a dict."""
import hashlib
import sys
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source"
sys.path.insert(0, str(SRC))
sys.modules.setdefault("boto3", types.SimpleNamespace(client=lambda *a, **k: None))
import lambda_function as L  # noqa: E402

# the real row of 2026-10-26 (ops 6972 read it from data/insider-trades.json) and the book's tape for it
LONA = {"ticker": "LONA", "company": "LeonaBio, Inc.", "cik": "0001620463", "insider": "Holder", "role": "10% Owner", "code": "P", "side": "buy",
        "shares": 3149470, "price": 3149.47, "value": 9919161280.9, "txn_date": "2026-10-26", "accession": "0002126186-26-000002", "filed_at": "2026-10-28T00:00:00+00:00"}
BOOK = {"LONA": {"name": "LeonaBio", "price": 3.47, "close": 3.47, "high_52w": 9.1, "low_52w": 2.2, "mcap": 13.7e6},
        "BH": {"name": "Biglari Holdings", "price": 362.0, "close": 362.0, "high_52w": 400.0, "low_52w": 190.0, "mcap": 8.2e8},
        "AAPL": {"name": "Apple", "price": 230.0, "close": 230.0, "high_52w": 260.0, "low_52w": 170.0, "mcap": 3.4e12}}


def check(cond, msg):
    if not cond:
        raise SystemExit("FAIL " + msg)


def row(ticker, price, shares, side="buy", acc="a-1"):
    return {"ticker": ticker, "company": ticker, "cik": "1", "insider": "Someone", "role": "Director", "side": side, "shares": shares, "price": price,
            "value": round(price * shares, 2), "txn_date": "2026-11-20", "filed_at": "2026-11-21T00:00:00+00:00", "accession": acc}


def test_a_price_that_cannot_be_is_set_aside_and_explained():
    txns = [dict(LONA), row("AAPL", 228.5, 1000), row("BH", 1768.0, 10, acc="a-2"), row("ZZZZ", 5.0, 10000, acc="a-3"), row("AAPL", 229.0, 500, side="sell", acc="a-4")]
    counted, as_filed, aside, s = L.screen_against_tape(txns, lookup=BOOK.get)
    check([t["ticker"] for t in counted] == ["AAPL", "BH", "ZZZZ", "AAPL"], "every other row is counted, in its order")
    check(len(aside) == 1 and aside[0]["ticker"] == "LONA" and aside[0]["value"] == 9919161280.9, "the LONA line is set aside")
    check("worth more than the whole company" in aside[0]["reason"] and aside[0]["tape_price"] == 3.47 and aside[0]["insider"] == "Holder", "with its reason, the tape price and the filer")
    check(as_filed == [LONA], "and kept exactly as filed for the next run")
    check(s == {"n_checked": 5, "n_in_range": 2, "n_off_tape": 1, "n_no_tape": 1, "n_set_aside": 1, "usd_set_aside_buys": 9919161280.9, "usd_set_aside_sells": 0.0, "tickers_on_the_book": 3}, "tally: %s" % s)
    by = {(t["ticker"], t["accession"]): t for t in counted}
    check(by[("BH", "a-2")]["price_check"] == "off_tape" and by[("ZZZZ", "a-3")]["price_check"] == "no_tape", "a share class far from the tape and a name off the book are counted and labelled")
    check(L.stats_summary(counted)["total_value_usd"] == round(228.5 * 1000 + 1768.0 * 10 + 5.0 * 10000, 2), "the total no longer carries the $9.9 billion line")
    check(all(t["ticker"] != "LONA" for t in L.detect_big_buys(counted)), "nor does the list of big buys")


def test_nothing_is_dropped_without_a_tape():
    txns = [dict(LONA), row("AAPL", 228.5, 1000)]
    counted, as_filed, aside, s = L.screen_against_tape(txns, lookup=lambda sym: None)
    check(len(counted) == 2 and not aside and not as_filed and s["n_no_tape"] == 2 and s["tickers_on_the_book"] == 0, "no factor book: every row counted, labelled no_tape")

    def broken(sym):
        raise RuntimeError("S3 is down")
    counted, _, aside, _ = L.screen_against_tape(txns, lookup=broken)
    check(len(counted) == 2 and not aside, "a failing book never stops the engine or drops a row")


def test_a_row_set_aside_is_checked_again_next_run():
    counted, as_filed, aside, _ = L.screen_against_tape([dict(LONA), row("AAPL", 228.5, 1000)], lookup=BOOK.get)
    stored = counted + as_filed                      # what load_existing returns from the published document
    again, as_filed2, aside2, s2 = L.screen_against_tape(stored, lookup=BOOK.get)
    check(as_filed2 == as_filed and len(aside2) == 1 and len(again) == len(counted), "the same row is set aside again, nothing duplicated")
    healed = dict(BOOK, LONA=dict(BOOK["LONA"], price=3100.0, close=3100.0, high_52w=3300.0, low_52w=2800.0, mcap=2e10))
    again, _, aside3, _ = L.screen_against_tape(stored, lookup=healed.get)
    check(not aside3 and any(t["ticker"] == "LONA" for t in again), "and counted again if the tape ever agrees with the filing")


def test_the_store_and_the_copy():
    src = (SRC / "lambda_function.py").read_text(encoding="utf-8")
    check('body.get("set_aside_as_filed", []))' in src and '"set_aside": set_aside,' in src and '"price_check": price_check,' in src, "the document carries the rows set aside and the tally")
    check('output["version"] = "3.1"' in src, "version 3.1")
    mine = hashlib.sha256((SRC / "insider_hygiene.py").read_bytes()).hexdigest()
    theirs = hashlib.sha256((SRC.parents[1] / "justhodl-insider-cluster-scanner" / "source" / "insider_hygiene.py").read_bytes()).hexdigest()
    check(mine == theirs, "insider_hygiene.py is a byte copy of the cluster scanner's")


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
    print("OK %d price-check groups" % len(tests))


if __name__ == "__main__":
    main()
