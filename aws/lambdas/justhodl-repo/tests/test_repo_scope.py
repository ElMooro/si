"""The board's added scope, derived spreads and FRED rhythm (2026-11-21).  Network-free."""
from datetime import date, timedelta
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SRC))
import repo_scope as rs  # noqa: E402


def days(start, n, step=1, value=lambda i: 1.0 + i / 100.0):
    d0 = date.fromisoformat(start)
    return [((d0 + timedelta(days=i * step)).isoformat(), value(i)) for i in range(n)]


def test_ofr_extras_adds_what_the_inventory_lacks_and_nothing_else():
    names = {"MMF-MMF_RP_T_OO-M": "MMF repo backed by Treasuries: overnight", "MMF-MMF_RP_wFR-M": "MMF repo with the Federal Reserve",
             "NYPD-PD_SB_T_TOT-A": "Securities borrowed: Treasury", "NYPD-PD_SL_EQT_OO-A": "Securities lent: equities",
             "NYPD-PD_RP_NEW-A": "A dealer repo line", "NYPD-PD_AFtD_NEW-A": "A fails line",
             "REPO-GCF_AR_B27-P": "GCF 2-7 days", "FNYR-SOFR-A": "SOFR", "TYLD-TCMR-10Yr-A": "10-year yield"}
    got = rs.ofr_extras(names, have_ids={"MMF-MMF_RP_wFR-M", "REPO-GCF_AR_B27-P", "FNYR-SOFR-A"})
    by = {e["mnemonic"]: e for e in got}
    assert sorted(by) == ["MMF-MMF_RP_T_OO-M", "NYPD-PD_AFtD_NEW-A", "NYPD-PD_RP_NEW-A", "NYPD-PD_SB_T_TOT-A", "NYPD-PD_SL_EQT_OO-A"], sorted(by)
    assert by["MMF-MMF_RP_T_OO-M"]["family"] == "MMFRP" and by["NYPD-PD_SB_T_TOT-A"]["family"] == "PDSEC" == by["NYPD-PD_SL_EQT_OO-A"]["family"]
    assert by["NYPD-PD_RP_NEW-A"]["family"] == "PDFIN" == by["NYPD-PD_AFtD_NEW-A"]["family"]
    e = by["NYPD-PD_SB_T_TOT-A"]
    assert e["s3_key"] == "data/warm/ofr/series/NYPD-PD_SB_T_TOT-A.json.gz" and e["tier"] == 2 and e["name"] == "Securities borrowed: Treasury"
    assert e["api_url"].endswith("mnemonic=NYPD-PD_SB_T_TOT-A")


def test_ofr_extras_falls_back_to_the_bank_catalog_when_the_names_call_failed():
    got = rs.ofr_extras({}, have_ids=set(), catalog=["MMF-MMF_RP_TOT-M", "NYPD-PD_SB_OO-A", "TYLD-TCMR-2Yr-A"])
    assert [(e["mnemonic"], e["name"]) for e in got] == [("MMF-MMF_RP_TOT-M", ""), ("NYPD-PD_SB_OO-A", "")]
    named = rs.ofr_extras({"MMF-MMF_RP_TOT-M": "MMF repo"}, set(), catalog=["MMF-MMF_RP_TOT-M"])
    assert [(e["mnemonic"], e["name"]) for e in named] == [("MMF-MMF_RP_TOT-M", "MMF repo")]


def test_dtcc_banked_history_is_offered_unless_the_live_fetch_already_scoped_it():
    both = rs.dtcc_banked(set())
    assert [(e["mnemonic"], e["s3_key"], e["tier"]) for e in both] == [
        ("DTCC-TREASURY-FAILS", "data/warm/dtcc-fails/treasury.json", 1), ("DTCC-AGENCY-FAILS", "data/warm/dtcc-fails/agency.json", 2)]
    assert all(e["family"] == "DTCCFAILS" and "banked history" in e["name"] for e in both)
    assert rs.dtcc_banked({"DTCC-TREASURY-FAILS", "DTCC-AGENCY-FAILS"}) == []


def test_refresh_due_asks_by_the_series_own_rhythm():
    today = date(2026, 11, 21)                                           # a Saturday
    daily = days("2026-10-01", 36)                                       # last print 2026-11-05
    assert rs.spacing_days(daily) == 1
    fresh = days("2026-10-16", 36)                                       # last print yesterday
    assert fresh[-1][0] == "2026-11-20" and rs.refresh_due("SOFR", fresh, today) is False
    assert rs.refresh_due("SOFR", days("2026-10-15", 36), today) is True          # two days old: yesterday's print can be out
    weekly = days("2026-06-03", 24, 7)                                   # last 2026-11-11
    assert rs.spacing_days(weekly) == 7 and rs.refresh_due("WRESBAL", weekly, today) is True
    assert rs.refresh_due("WRESBAL", days("2026-06-10", 24, 7), today) is False   # last 2026-11-18, three days old
    assert rs.refresh_due("X", [], today) is True and rs.refresh_due("X", [("2026-01-01", 1.0)], today) is True


def simulate(series_id, pairs, start, days_, asked=None):
    """Run the rule day after day the way the engine does: an ask stamps the document."""
    calls = []
    for k in range(days_):
        today = start + timedelta(days=k)
        if rs.refresh_due(series_id, pairs, today, asked):
            calls.append(today)
            asked = today
    return calls


def test_slow_series_are_not_asked_before_their_next_print_can_exist_and_then_every_fourth_day():
    quarterly = [("%d-%02d-01" % (2024 + q // 4, 1 + 3 * (q % 4)), float(q)) for q in range(10)]       # last 2026-04-01
    assert rs.spacing_days(quarterly) in (90, 91, 92)
    assert rs.refresh_due("BOGZ1FL000Q", quarterly, date(2026, 7, 1)) is False          # 91 days after the last quarter: not out yet
    asked = simulate("BOGZ1FL000Q", quarterly, date(2026, 8, 1), 80)                     # through the release window
    assert asked[0] >= date(2026, 8, 15) and 12 <= len(asked) <= 18 and all((b - a).days == 4 for a, b in zip(asked, asked[1:])), asked
    firsts = {simulate("BOGZ1FL%03dQ" % i, quarterly, date(2026, 8, 1), 80)[0] for i in range(40)}
    assert len(firsts) >= 4                                                              # lines of one release start on different days
    annual = [("%d-01-01" % y, float(y)) for y in range(2015, 2026)]                     # last 2025-01-01
    assert rs.refresh_due("BOGZ1FA000A", annual, date(2026, 2, 1)) is False             # 396 days: not before a year plus 60 days
    assert simulate("BOGZ1FA000A", annual, date(2026, 3, 1), 30)


def test_a_silent_series_is_asked_at_once_when_never_asked_and_then_once_a_month():
    """The dollar complex on 2026-11-21: daily series whose bank stopped on 2026-08-07.  Ended or frozen look the same
    from the last observation; what tells them apart is whether anyone has asked."""
    frozen = days("2026-06-29", 40)                                                      # last print 2026-08-07
    assert frozen[-1][0] == "2026-08-07"
    today = date(2026, 11, 22)
    assert rs.refresh_due("DTWEXBGS", frozen, today) is True                             # never asked: now
    assert rs.refresh_due("DTWEXBGS", frozen, today, asked=date(2026, 8, 7)) is True     # not asked for 107 days: now
    assert rs.refresh_due("DTWEXBGS", frozen, today, asked=date(2026, 11, 21)) is False  # asked yesterday and still silent: it has ended
    ended = [("%d-01-01" % y, float(y)) for y in range(2008, 2020)]                      # an annual line that stopped in 2019
    assert len(simulate("DTWEXM_A", ended, date(2026, 1, 1), 360)) == 12                 # once every thirty days
    assert len(simulate("DTWEXM", days("2019-11-01", 40), date(2026, 1, 1), 360)) == 12
    assert rs.stamp_day("2026-11-21T06:10:17+00:00") == date(2026, 11, 21) and rs.stamp_day("x") is None and rs.stamp_day(None) is None


def test_a_year_of_asking_is_a_fraction_of_asking_everything_every_day():
    """~1,000 series a day before; the mix the board really holds (ops 6892): 660 + 330 slow lines, about 25 daily."""
    quarterly = [("%d-%02d-01" % (2023 + q // 4, 1 + 3 * (q % 4)), 1.0) for q in range(12)]            # last 2025-10-01
    annual = [("%d-01-01" % y, 1.0) for y in range(2015, 2026)]
    calls, worst_day = 0, {}
    for i in range(300):
        for d in simulate("S%04d" % i, quarterly if i % 2 else annual, date(2026, 1, 1), 90):
            calls += 1
            worst_day[d] = worst_day.get(d, 0) + 1
    assert calls < 300 * 90 / 6, calls                 # under a sixth of asking daily, even inside a release window
    assert max(worst_day.values()) <= 300 / 2, max(worst_day.values())      # and never the whole board on one day


def test_derived_spreads_from_banked_components():
    sofr = dict(days("2019-09-10", 8, value=lambda i: [2.20, 2.21, 2.20, 2.43, 5.25, 2.55, 1.95, 1.86][i]))
    effr = dict(days("2019-09-10", 8, value=lambda i: [2.13, 2.13, 2.14, 2.25, 2.30, 2.25, 1.90, 1.90][i]))
    p99 = dict(days("2019-09-10", 8, value=lambda i: [2.30, 2.31, 2.35, 4.60, 9.00, 5.00, 2.30, 2.10][i]))
    ioer = {"2019-08-01": 2.10, "2019-09-19": 1.80}
    bank = {"SOFR": sofr, "FNYR-EFFR-A": effr, "SOFR_P99": p99, "IOER": ioer, "IORB": {"2021-07-29": 0.15},
            "DFF": {"2008-09-12": 2.12, "2008-09-15": 2.64, "2008-12-16": 0.16}, "DFEDTAR": {"2008-04-30": 2.0, "2008-10-08": 1.5},
            "DFEDTARU": {"2008-12-16": 0.25}, "RPONTSYD": {"2019-09-17": 40.85, "2019-09-18": 51.55}, "RPONMBSD": {"2019-09-17": 11.7},
            "RPONAGYD": {"2019-09-17": 0.6, "2019-09-19": 0.3}}
    got = {i: (label, series, tier) for i, label, series, tier in rs.derived_series(lambda k: bank.get(k, {}))}
    assert sorted(got) == ["D_EFFR_TARGET", "D_FED_REPO", "D_SOFR_ADMIN", "D_SOFR_EFFR", "D_SOFR_TAIL"]
    assert round(got["D_SOFR_EFFR"][1]["2019-09-14"], 6) == 295.0                 # 5.25 against 2.30 on the fifth day of the fixture
    assert round(got["D_SOFR_TAIL"][1]["2019-09-14"], 6) == 375.0                 # 9.00 against 5.25
    assert round(got["D_SOFR_ADMIN"][1]["2019-09-14"], 6) == 315.0                # against IOER 2.10, carried forward
    assert "2019-09-10" in got["D_SOFR_ADMIN"][1] and round(got["D_SOFR_ADMIN"][1]["2019-09-10"], 6) == 10.0
    assert round(got["D_EFFR_TARGET"][1]["2008-09-15"], 6) == 64.0                # 2.64 against the 2.00 target
    assert round(got["D_EFFR_TARGET"][1]["2008-12-16"], 6) == -9.0                # 0.16 against the new range's top, 0.25
    assert round(got["D_FED_REPO"][1]["2019-09-17"], 6) == 53.15 and got["D_FED_REPO"][1]["2019-09-19"] == 0.3
    assert all(t == 1 for _, _, t in got.values())


def test_derived_series_with_nothing_banked_come_out_empty_not_broken():
    got = rs.derived_series(lambda k: {})
    assert [len(s) for _, _, s, _ in got] == [0, 0, 0, 0, 0]
    partial = rs.derived_series(lambda k: {"FNYR-SOFR-A": {"2026-11-19": 3.91}, "DFF": {"2026-11-19": 3.88}}.get(k, {}))
    by = {i: s for i, _, s, _ in partial}
    assert round(by["D_SOFR_EFFR"]["2026-11-19"], 6) == 3.0 and by["D_SOFR_TAIL"] == {} and by["D_SOFR_ADMIN"] == {}


def main():
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    for _, fn in tests:
        fn()
    print("repo scope: %d tests passed" % len(tests))


if __name__ == "__main__":
    main()
