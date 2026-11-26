"""Dependency-free checks for the catalyst calendar's kept-document sources (real documents of 2026-11-26, cut by ops 6972)."""
import gzip
import json
import sys
from datetime import date
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "source"))
import catalyst_sources as CS  # noqa: E402

FX = json.loads(gzip.decompress((HERE / "fixtures" / "kept-documents-2026-11-26.json.gz").read_bytes()))
TODAY = date(2026, 11, 26)
SHAPE = {"date", "time", "type", "title", "subtitle", "impact", "source", "url", "size_billions"}


def check(cond, msg):
    if not cond:
        raise SystemExit("FAIL " + msg)


def well_formed(events, kind):
    for e in events:
        check(SHAPE <= set(e), "%s: the calendar's own fields are all there (%s)" % (kind, sorted(SHAPE - set(e))))
        check(e["type"] == kind and e["impact"] in CS.IMPACTS, "%s: type and impact" % kind)
        check(TODAY <= date.fromisoformat(e["date"]) <= date(2027, 1, 25), "%s: inside the window (%s)" % (kind, e["date"]))
        check(e["title"] and "None" not in e["title"] and "None" not in (e["subtitle"] or ""), "%s: nothing printed as None (%s)" % (kind, e["title"]))


def test_economic_calendar():
    doc = FX["econ-calendar"]
    ev = CS.econ_events(doc, TODAY, 60)
    well_formed(ev, "ECON")
    rows = [r for r in doc["upcoming"] + doc["this_week"] if not r.get("released") and r["date"] >= "2026-11-26"]
    keys = {(r["date"], r.get("time_utc"), r.get("country"), r["event"].strip()) for r in rows}
    check(len(ev) == len(keys) > 5, "one event for each release not yet out, none twice (%d of %d)" % (len(ev), len(keys)))
    first = next(e for e in ev if e["title"] == "Housing Starts YoY (Oct)")
    check(first["country"] == "Japan" and first["consensus"] == 2.0 and first["previous"] == -2.7, "the figures are the document's")
    check(first["subtitle"] == "Japan · consensus 2% · previous -2.7%", "subtitle reads the figures with their unit: " + first["subtitle"])
    check(all(e["impact"] == "HIGH" for e in ev if e["tier1"]), "a tier-one release is high impact")
    check(CS.econ_events({"upcoming": [dict(doc["upcoming"][1], released=True)]}, TODAY, 60) == [], "a released figure is not ahead")
    check(CS.econ_events(None, TODAY, 60) == [] and CS.econ_events({"upcoming": "x"}, TODAY, 60) == [], "a missing or misshapen document gives nothing")


def test_wider_earnings():
    doc = FX["finviz-earnings-calendar"]
    ev = CS.earnings_wide_events(doc, TODAY, 60)
    well_formed(ev, "EARNINGS")
    names = [r["ticker"] for d in doc["calendar"] if d["date"] >= "2026-11-26" for r in d["reporters"]]
    check(len(ev) == len(set(names)) > 0, "every company on the kept calendar from today on, once")
    check(all(e["ticker"] and e["title"] == e["ticker"] + " earnings" for e in ev), "named by ticker")
    big = [e for e in ev if e["market_cap_usd"] and e["market_cap_usd"] >= 1e11]
    check(all(e["impact"] == "HIGH" for e in big), "the largest are high impact")
    skip = ev[0]["ticker"]
    check(all(e["ticker"] != skip for e in CS.earnings_wide_events(doc, TODAY, 60, have=[skip.lower()])), "a company the tracker already lists is not added again")
    check(CS.earnings_wide_events(doc, date(2026, 12, 30), 60) == [], "nothing is carried past the kept calendar's own dates")


def test_ipos_lockups_quarter_end_russell():
    ipo = CS.ipo_events({"upcoming": [{"symbol": "abcd", "company": "Abcd Inc", "date": "2026-12-03", "exchange": "NYSE", "price_low": 18, "price_high": 20, "price_mid": 19, "shares_offered": 40000000}]}, TODAY, 60)
    well_formed(ipo, "IPO")
    check(ipo[0]["title"] == "ABCD IPO" and ipo[0]["size_billions"] == 0.76 and ipo[0]["impact"] == "MEDIUM", "deal size is price times shares")
    check("$18–$20" in ipo[0]["subtitle"], "price range shown: " + ipo[0]["subtitle"])
    check(CS.ipo_events(FX["ipo-pipeline"], TODAY, 60) == [], "an IPO dated before today is not ahead")
    real = CS.ipo_events(FX["ipo-pipeline"], date(2026, 11, 25), 60)
    check(len(real) == 1 and real[0]["ticker"] == "CPHY" and real[0]["size_billions"] is None and real[0]["impact"] == "LOW", "the kept pipeline's own row, no size invented")
    check(CS.lockup_events(FX["lockup-expiration"], TODAY, 60) == [], "no lock-up in the kept document, none on the calendar")
    lock = CS.lockup_events({"imminent_lockups_7d": [{"symbol": "XYZ", "company_name": "Xyz", "ipo_date": "2026-06-01", "lockup_expiration_date": "2026-11-28"}],
                             "approaching_lockups_21d": [{"symbol": "XYZ", "lockup_expiration_date": "2026-11-28"}]}, TODAY, 60)
    well_formed(lock, "LOCKUP")
    check(len(lock) == 1 and lock[0]["subtitle"] == "Xyz · IPO 2026-06-01", "one row per company")
    q = CS.quarter_end_events(FX["rebalance-radar"], TODAY, 60)
    well_formed(q, "QUARTER_END")
    check(q[0]["date"] == "2026-12-31" and "T-5..T+3" in q[0]["subtitle"], "quarter-end from the radar's own calendar")
    check(CS.russell_events(FX["russell-recon-frontrun"], TODAY, 60) == [], "the reconstitution is 211 days off: not on a 60-day calendar")
    r = CS.russell_events(FX["russell-recon-frontrun"], date(2027, 6, 1), 60)
    check(len(r) == 1 and r[0]["date"] == "2027-06-25" and r[0]["type"] == "REBALANCE", "and on it once it is inside the window")


def test_holidays():
    rows = [{"date": "2026-11-26", "exchange": "NYSE", "name": "Thanksgiving", "status": "closed"},
            {"date": "2026-11-26", "exchange": "NASDAQ", "name": "Thanksgiving", "status": "closed"},
            {"date": "2026-11-27", "exchange": "NYSE", "name": "Thanksgiving", "status": "early-close", "open": "2026-11-27T14:30:00.000Z", "close": "2026-11-27T18:00:00.000Z"},
            {"date": "2026-12-25", "exchange": "NYSE", "name": "Christmas", "status": "closed"},
            {"date": "2027-07-05", "exchange": "NYSE", "name": "Independence Day", "status": "closed"}, {"name": "no date"}, "junk"]
    ev = CS.holiday_events(rows, TODAY, 60)
    well_formed(ev, "MARKET_HOLIDAY")
    check([(e["date"], e["status"]) for e in ev] == [("2026-11-26", "closed"), ("2026-11-27", "early-close"), ("2026-12-25", "closed")], "one row a day, closures and early closes")
    check(ev[0]["subtitle"] == "NASDAQ, NYSE" and ev[1]["time"] == "18:00" and ev[1]["title"] == "Thanksgiving (early close)", "exchanges and the early close hour")


def test_calendar_rules():
    check(CS.third_friday(2026, 12) == date(2026, 12, 18) and CS.third_friday(2027, 1) == date(2027, 1, 15), "third Fridays")
    opex = CS.opex_events(TODAY, 60)
    well_formed(opex, "OPEX")
    check([e["date"] for e in opex] == ["2027-01-15"], "monthly expirations ahead; December's is the quad witching the calendar already lists")
    check([e["date"] for e in CS.opex_events(date(2026, 1, 1), 120)] == ["2026-01-16", "2026-02-20", "2026-04-17"], "a quarter month is never listed twice")
    vix = CS.vix_expiry_events(TODAY, 60)
    well_formed(vix, "VIX_EXPIRY")
    check([e["date"] for e in vix] == ["2026-12-16", "2027-01-20"], "VIX settles 30 days before the next third Friday: %s" % [e["date"] for e in vix])
    check(all(date.fromisoformat(e["date"]).weekday() == 2 for e in CS.vix_expiry_events(date(2026, 1, 1), 400)), "always a Wednesday")


def test_dedupe():
    a = {"date": "2026-12-18", "type": "EARNINGS", "title": "X earnings", "ticker": "X", "source": "one"}
    b = dict(a, source="two")
    c = dict(a, ticker="Y", title="Y earnings")
    check(CS.dedupe([a, b, c]) == [a, c], "the same event from two sources is listed once, the first source kept")


def main():
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
    print("OK %d checks groups" % len(tests))


if __name__ == "__main__":
    main()
