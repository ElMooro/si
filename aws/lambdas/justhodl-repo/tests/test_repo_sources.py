"""The board's own source readers (repo_sources.py, 2026-11-22): the dealer survey rate, the tri-party haircut
workbooks, DTCC's moved fails file.  No network, no AWS - workbooks are built here, I/O is three small stubs."""
from datetime import date, timedelta
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE.parent / "source"), str(HERE)]
import repo_scope as R  # noqa: E402
import repo_sources as S  # noqa: E402
from xlsx_fixture import make_xlsx  # noqa: E402

BANNER = [[None, None, None, "NEW YORK FED  ECONOMIC RESEARCH"], [None, None, None, "https://www.newyorkfed.org/research"]]
HEAD_OLD = ["date", "group_id", "group_name", "collateral_value", "share_of_total", "top_3_concentration", "p1", "p10", "median", "p90", "fedwire", "num_obs", "margin_stdev"]
HEAD_NEW = ["date", "group_id", "group_name", "collateral_value", "share_of_total", "top_3_concentration", "p10", "median", "p90", "fedwire", "num_obs", "margin_stdev"]
GROUPS = ["Agency CMOs", "Agency Debentures & Strips", "Agency MBS", "US Treasuries Strips", "US Treasuries excluding Strips", "ABS Investment Grade",
          "ABS Non Investment Grade", "CMO Private Label Investment Grade", "CMO Private Label Non Investment Grade", "Corporates Investment Grade",
          "Corporates Non Investment Grade", "Equities", "International Securities", "Money Market", "Municipality Debt", "Other"]
OLD_DAYS = ["20240909", "20241009", "20241112", "20241210", "20250110"]
NEW_DAYS = ["20241210", "20250110", "20250211", "20250311", "20250409", "20250509", "20250610", "20250710"]


def excel_serial(iso):
    return float((date.fromisoformat(iso) - date(1899, 12, 30)).days)


def weekdays(start, n):
    d, out = date.fromisoformat(start), []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def survey_book(n=4100, title="Overnight Treasury GC Repo Primary Dealer Survey Rate (%)", start="1998-02-20", tweak=None):
    rows = [[None, title]] + [[excel_serial(d), round(5.4 - i * 0.001, 3)] for i, d in enumerate(weekdays(start, n))]
    if tweak:
        tweak(rows)
    return make_xlsx({"Sheet1": rows})


def triparty_book(days, head, shift=0.0, extra=()):
    """Values as the publisher writes them: text for amounts, numbers for ids and counts, short rows for totals."""
    rows = [list(r) for r in BANNER] + [head]
    for day in days:
        for i, g in enumerate(GROUPS):
            vals = {"collateral_value": "%.2f" % (100 + i + shift), "share_of_total": "%.1f" % (i + 1), "top_3_concentration": "%.1f" % (30 + i),
                    "p1": "0.0", "p10": "2.0", "median": "%.1f" % (2 + (i % 4)), "p90": "5.0", "fedwire": 1.0, "num_obs": 700.0 + i, "margin_stdev": 1.25}
            if g == "Other":                                         # too few trades for a haircut or a concentration
                vals.update(top_3_concentration=None, median=None)
            rows.append([day, float(i + 1), g] + [vals[c] for c in head[3:]])
        rows.append([day, 0.0, "Total", "%.2f" % (4000 + shift)])
        rows.append([day, 24.0, "Number of Observations", "17564.00"])
        for g, v in extra:
            rows.append([day, 22.0, g, v, "19.4"])
    return make_xlsx({"Cover Sheet": [[None, None, None, "Tri-Party/GCF Repo Statistics"]], S.HAIRCUT_SHEET: rows, "gcf_repo_total": BANNER + [["date", "collateral_type", "value"]]})


class Bank:
    """read / fetch / put stubs over two dicts, counting what was asked."""

    def __init__(self, objects=None, web=None):
        self.objects, self.web, self.fetched, self.puts = dict(objects or {}), dict(web or {}), [], []

    def read(self, key):
        return self.objects.get(key)

    def fetch(self, url):
        self.fetched.append(url)
        if url not in self.web:
            raise IOError("404 " + url)
        return self.web[url]

    def put(self, key, doc):
        self.objects[key] = doc
        self.puts.append(key)


def test_workbook_reads_text_numbers_and_gaps():
    raw = make_xlsx({"A & B": [["date", None, "x<y"], [35846.0, None, 5.37], []], "Two": [[1]]})
    assert S.workbook(raw) == {"A & B": [["date", None, "x<y"], [35846.0, None, 5.37], []], "Two": [[1.0]]}
    assert S.excel_day(35846) == "1998-02-20" and S.excel_day(43159.0) == "2018-02-28"
    for bad in (b"", b"<html>not found</html>", b"PK\x03\x04broken"):
        try:
            S.workbook(bad)
            raise AssertionError("accepted %r" % bad[:12])
        except ValueError:
            pass


def test_survey_rate_is_read_whole_and_anything_else_is_refused():
    pairs = S.survey_rate(survey_book())
    assert len(pairs) == 4100 and pairs[0] == ("1998-02-20", 5.4) and pairs[1][0] == "1998-02-23" and pairs[-1][1] == round(5.4 - 4099 * 0.001, 3)
    assert [d for d, _ in pairs] == sorted({d for d, _ in pairs})

    def out_of_order(rows):
        rows[10], rows[11] = rows[11], rows[10]

    def absurd(rows):
        rows[50][1] = 55.0

    for label, raw in (("too short", survey_book(n=900)), ("another series", survey_book(title="Some other rate")), ("wrong era", survey_book(start="2005-01-03")),
                       ("out of order", survey_book(tweak=out_of_order)), ("absurd level", survey_book(tweak=absurd)), ("a web page", b"<html>moved</html>")):
        try:
            S.survey_rate(raw)
            raise AssertionError("accepted: " + label)
        except ValueError:
            pass


def test_survey_rate_is_banked_once_and_then_only_read():
    bank = Bank(web={S.SURVEY_URL: survey_book()})
    out = S.bank_survey_rate(bank.read, bank.fetch, bank.put, "2026-11-22T06:10:00+00:00")
    assert out == {"status": "written", "n": 4100, "first": "1998-02-20", "last": S.survey_rate(survey_book())[-1][0]}
    doc = bank.objects[S.SURVEY_KEY]
    assert doc["id"] == "GC_SURVEY" and doc["final"] is True and len(doc["observations"]) == 4100 and doc["observations"][0] == {"date": "1998-02-20", "value": 5.4}
    assert len(doc["source_sha256"]) == 64 and doc["source"] == S.SURVEY_URL
    again = S.bank_survey_rate(bank.read, bank.fetch, bank.put, "later")
    assert again == {"status": "banked", "n": 4100} and bank.fetched == [S.SURVEY_URL] and bank.puts == [S.SURVEY_KEY]
    entry = S.survey_scope()
    assert entry["mnemonic"] == "GC_SURVEY" and entry["s3_key"] == S.SURVEY_KEY and entry["family"] == "GCSURVEY" and entry["tier"] == 1
    # a source that is down leaves no half-written bank
    down = Bank()
    try:
        S.bank_survey_rate(down.read, down.fetch, down.put, "x")
        raise AssertionError("no error from a dead source")
    except IOError:
        assert down.puts == []


def test_haircut_series_keep_the_banks_names_and_the_new_reporting_wins_a_shared_month():
    old = S.triparty_rows(triparty_book(OLD_DAYS, HEAD_OLD))
    new = S.triparty_rows(triparty_book(NEW_DAYS, HEAD_NEW, shift=0.5, extra=(("Sponsored GC", "820.78"),)))
    assert len(old) == 5 * 18 and old[0] == {"date": "2024-09-09", "group": "Agency CMOs", "collateral_value": 100.0, "share_of_total": 1.0, "top_3_concentration": 30.0, "median": 2.0}
    ser = S.haircut_series(old, new)
    ids = set(ser)
    assert {"us-treasuries-excluding-strips--median_haircut", "agency-debentures-strips--collateral_share", "cmo-private-label-non-investment-grade--top3_concentration",
            "total--collateral_value", "number-of-observations--collateral_value", "sponsored-gc--collateral_value", "sponsored-gc--collateral_share", "other--collateral_value"} <= ids
    assert "other--median_haircut" not in ids and "total--median_haircut" not in ids and "other--top3_concentration" not in ids      # no number, no series
    assert len(ids) == 15 * 4 + 2 + 1 + 1 + 2                        # 15 asset classes with all four numbers, Other 2, Total 1, observations 1, Sponsored GC 2
    t = ser["us-treasuries-excluding-strips--median_haircut"]
    assert t["title"] == "Tri-party haircuts: US Treasuries excluding Strips — median haircut"
    assert ser["abs-investment-grade--top3_concentration"]["title"] == "Tri-party haircuts: ABS Investment Grade — top3 concentration"
    cv = ser["agency-cmos--collateral_value"]["observations"]
    assert sorted(cv) == ["2024-09-09", "2024-10-09", "2024-11-12", "2024-12-10", "2025-01-10", "2025-02-11", "2025-03-11", "2025-04-09", "2025-05-09", "2025-06-10", "2025-07-10"]
    assert cv["2024-11-12"] == 100.0 and cv["2024-12-10"] == 100.5 and cv["2025-01-10"] == 100.5          # shared months: the new workbook's number
    for bad in (make_xlsx({"other": [["date"]]}), make_xlsx({S.HAIRCUT_SHEET: BANNER + [["date", "group_name", "collateral_value"]]}), b"<html></html>"):
        try:
            S.triparty_rows(bad)
            raise AssertionError("accepted a workbook without the haircut columns")
        except ValueError:
            pass
    assert S.slug("Agency Debentures & Strips") == "agency-debentures-strips" and S.slug("  CDOs ") == "cdos"


def banked_from(series, upto):
    """A bank as the August parse left it: every series, observations up to `upto`, plus one ended asset class."""
    obj, entries = {}, []
    for sid, ser in series.items():
        obs = {d: v for d, v in ser["observations"].items() if d <= upto}
        if not obs:
            continue
        obj[S.HAIRCUT_PREFIX + sid + ".json"] = {"id": sid, "title": ser["title"], "observations": [{"date": d, "value": obs[d]} for d in sorted(obs)]}
        entries.append({"id": sid, "title": ser["title"], "n": len(obs), "first": min(obs), "last": max(obs)})
    obj[S.HAIRCUT_PREFIX + "whole-loans--median_haircut.json"] = {"id": "whole-loans--median_haircut", "title": "Tri-party haircuts: Whole Loans — median haircut",
                                                                  "observations": [{"date": "2011-08-09", "value": 10.0}]}
    entries.append({"id": "whole-loans--median_haircut", "title": "Tri-party haircuts: Whole Loans — median haircut", "n": 1, "first": "2011-08-09", "last": "2011-08-09"})
    obj[S.HAIRCUT_INDEX] = {"built_at": "2026-08-16T22:58:08+00:00", "series": sorted(entries, key=lambda e: e["id"])}
    return obj


def test_haircut_refresh_asks_when_due_writes_what_changed_and_keeps_the_bank():
    old_book, new_book = triparty_book(OLD_DAYS, HEAD_OLD), triparty_book(NEW_DAYS, HEAD_NEW, shift=0.5)
    full = S.haircut_series(S.triparty_rows(old_book), S.triparty_rows(new_book))
    seeded = banked_from(full, "2025-05-09")
    # one banked observation the source has since dropped: the bank keeps it
    seeded[S.HAIRCUT_PREFIX + "equities--median_haircut.json"]["observations"].insert(0, {"date": "2010-05-11", "value": 8.0})
    web = {S.TRIPARTY_NOW: new_book, S.TRIPARTY_OLD: old_book}

    # 20 days after the newest snapshot the next one cannot be out: nothing is asked
    bank = Bank(seeded, web)
    quiet = S.refresh_haircuts(bank.read, bank.fetch, bank.put, "t", date(2025, 5, 29))
    assert quiet == {"status": "not due", "newest": "2025-05-09", "series": len(full) + 1, "asked": False} and bank.fetched == [] and bank.puts == []

    # a dry run reads the source and writes nothing
    dry = S.refresh_haircuts(bank.read, bank.fetch, bank.put, "t", date(2025, 7, 20), dry_run=True)
    assert dry["status"] == "would write" and dry["written"] == len(full) and dry["observations_added"] == 2 * len(full) and bank.puts == []

    # due: only the current workbook is read; every series gains its two months; the index follows
    bank = Bank(seeded, web)
    st = S.refresh_haircuts(bank.read, bank.fetch, bank.put, "2025-07-20T06:10:00+00:00", date(2025, 7, 20))
    assert bank.fetched == [S.TRIPARTY_NOW] and st["old_workbook_read"] is False
    assert st["status"] == "written" and st["written"] == len(full) and st["unchanged"] == 0 and st["observations_added"] == 2 * len(full) and st["observations_revised"] == 0
    assert st["bank_newest_before"] == "2025-05-09" and st["bank_newest_after"] == "2025-07-10" == st["source_newest"] and st["new_series"] == [] and st["series"] == len(full) + 1
    eq = bank.objects[S.HAIRCUT_PREFIX + "equities--median_haircut.json"]
    assert eq["observations"][0] == {"date": "2010-05-11", "value": 8.0} and eq["observations"][-1]["date"] == "2025-07-10" and eq["title"] == "Tri-party haircuts: Equities — median haircut"
    assert [o["date"] for o in eq["observations"]] == sorted(o["date"] for o in eq["observations"])
    index = bank.objects[S.HAIRCUT_INDEX]
    by = {e["id"]: e for e in index["series"]}
    assert index["built_at"] == "2026-08-16T22:58:08+00:00" and index["refreshed_at"] == "2025-07-20T06:10:00+00:00" and len(by) == len(full) + 1
    assert by["equities--median_haircut"]["last"] == "2025-07-10" and by["equities--median_haircut"]["first"] == "2010-05-11"
    assert by["whole-loans--median_haircut"] == {"id": "whole-loans--median_haircut", "title": "Tri-party haircuts: Whole Loans — median haircut", "n": 1, "first": "2011-08-09", "last": "2011-08-09"}
    assert bank.objects[S.HAIRCUT_PREFIX + "whole-loans--median_haircut.json"]["observations"] == [{"date": "2011-08-09", "value": 10.0}]     # ended in 2011: untouched
    assert bank.puts.count(S.HAIRCUT_INDEX) == 1 and len(bank.puts) == len(full) + 1

    # the day after: not due, nothing asked
    again = S.refresh_haircuts(bank.read, bank.fetch, bank.put, "t", date(2025, 7, 21))
    assert again["status"] == "not due" and bank.fetched == [S.TRIPARTY_NOW]
    # due again but the source has no new month: asked, level, nothing written
    puts = len(bank.puts)
    level = S.refresh_haircuts(bank.read, bank.fetch, bank.put, "t", date(2025, 8, 20))
    assert level["status"] == "level" and level["written"] == 0 and level["unchanged"] == len(full) and len(bank.puts) == puts


def test_haircut_refresh_revises_adds_a_new_asset_class_and_refuses_a_cut_file():
    old_book, new_book = triparty_book(OLD_DAYS, HEAD_OLD), triparty_book(NEW_DAYS, HEAD_NEW, shift=0.5)
    full = S.haircut_series(S.triparty_rows(old_book), S.triparty_rows(new_book))
    seeded = banked_from(full, "2025-07-10")
    revised_book = triparty_book(NEW_DAYS + ["20250811"], HEAD_NEW, shift=0.75, extra=(("Sponsored GC", "820.78"),))
    bank = Bank(seeded, {S.TRIPARTY_NOW: revised_book})
    st = S.refresh_haircuts(bank.read, bank.fetch, bank.put, "t", date(2025, 9, 15))
    assert st["new_series"] == ["sponsored-gc--collateral_share", "sponsored-gc--collateral_value"] and st["observations_revised"] > 0
    assert ["agency-cmos--collateral_value", "2024-12-10", 100.5, 100.75] in st["revised_sample"]
    cv = {o["date"]: o["value"] for o in bank.objects[S.HAIRCUT_PREFIX + "agency-cmos--collateral_value.json"]["observations"]}
    assert cv["2024-11-12"] == 100.0 and cv["2024-12-10"] == 100.75 and cv["2025-08-11"] == 100.75        # the old workbook's months stay, the source's own revision is taken
    assert bank.objects[S.HAIRCUT_PREFIX + "sponsored-gc--collateral_value.json"]["title"] == "Tri-party haircuts: Sponsored GC — collateral value"
    assert "sponsored-gc--collateral_value" in {e["id"] for e in bank.objects[S.HAIRCUT_INDEX]["series"]}

    # a source that ends before the bank does, or has lost most of its rows, changes nothing
    for label, book in (("older than the bank", triparty_book(NEW_DAYS[:5], HEAD_NEW)), ("three groups", make_xlsx({S.HAIRCUT_SHEET: BANNER + [HEAD_NEW] + [
            ["20250811", 1.0, g, "1.0", "1.0", "1.0", "2.0", "2.0", "2.0", 1.0, 5.0, 1.0] for g in GROUPS[:3] for _ in range(40)]}))):
        b = Bank(banked_from(full, "2025-07-10"), {S.TRIPARTY_NOW: book})
        try:
            S.refresh_haircuts(b.read, b.fetch, b.put, "t", date(2025, 9, 15))
            raise AssertionError("accepted a source that is " + label)
        except ValueError:
            assert b.puts == []

    # an empty bank is built from both workbooks; a forced run reads both too
    empty = Bank({}, {S.TRIPARTY_NOW: new_book, S.TRIPARTY_OLD: old_book})
    st = S.refresh_haircuts(empty.read, empty.fetch, empty.put, "now", date(2025, 7, 20))
    assert empty.fetched == [S.TRIPARTY_NOW, S.TRIPARTY_OLD] and st["old_workbook_read"] and st["written"] == len(full) and empty.objects[S.HAIRCUT_INDEX]["built_at"] == "now"
    assert {o["date"] for o in empty.objects[S.HAIRCUT_PREFIX + "agency-mbs--median_haircut.json"]["observations"]} == set(full["agency-mbs--median_haircut"]["observations"])


def fails_file(days, cut=0, trailer=None, head="Date,Treasury Fails Amount,Agency Fails Amount"):
    lines = [head] + ["%s/%s/%s,%.2f,%.2f " % (d[5:7], d[8:10], d[:4], 2.0e10 + i * 1e8, 1.5e8 + i) for i, d in enumerate(days)]
    return "\n".join(lines[:len(lines) - cut] + ["TRAILER,%d,,,,," % (len(days) if trailer is None else trailer)]) + "\n"


def test_dtcc_fails_file_is_read_and_a_cut_one_refused():
    days = weekdays("2025-11-20", 250)
    tsy, agy = S.dtcc_fails(fails_file(days))
    assert len(tsy) == 250 == len(agy) and tsy[days[0]] == 2.0e10 and tsy[days[-1]] == 2.0e10 + 249e8 and agy[days[3]] == 1.5e8 + 3
    assert sorted(tsy) == days
    for label, text in (("cut short", fails_file(days, cut=40)), ("another file", fails_file(days, head="Date,MBS Total PAR Value,MBS Weighted Average")),
                        ("an error page", "error code: 1010"), ("a web page", "<html>Not found</html>"), ("three rows", fails_file(days[:3]))):
        try:
            S.dtcc_fails(text)
            raise AssertionError("accepted: " + label)
        except ValueError:
            pass
    assert S.dtcc_url(S.DTCC_FAILS_URLS[0], 1795319578498) == "https://cms-prod.dtcc.com/data/failsdata.csv?v=1795319578498"
    assert S.DTCC_FAILS_URLS[0].startswith("https://cms-prod.dtcc.com/") and "python" not in S.DTCC_CLIENT["User-Agent"].lower()


def test_bank_io_tells_a_missing_key_from_a_failing_bucket():
    class Missing(Exception):
        response = {"Error": {"Code": "NoSuchKey"}}

    class Store:
        def __init__(self):
            self.put = None

        def get_object(self, Bucket, Key):
            if Key == "absent":
                raise Missing("gone")
            if Key == "broken":
                raise RuntimeError("SlowDown")
            import io
            return {"Body": io.BytesIO(b'{"a": 1}')}

        def put_object(self, **kw):
            self.put = kw

    store = Store()
    read, fetch, put = S.bank_io(store, "b")
    assert read("absent") is None and read("there") == {"a": 1}
    try:
        read("broken")
        raise AssertionError("a failing bucket read as an empty bank")
    except RuntimeError:
        pass
    put("k", {"x": [1, 2]})
    assert store.put["Key"] == "k" and store.put["Body"] == b'{"x":[1,2]}' and store.put["ContentType"] == "application/json" and callable(fetch)


def test_repo_against_policy_spans_the_survey_rate_and_sofr_both_ways_round():
    data = {"GC_SURVEY": {"2008-09-12": 2.20, "2008-09-17": 0.25, "2009-01-02": 0.05, "2018-02-28": 1.40},
            "SOFR": {"2018-04-03": 1.83, "2019-09-17": 5.25},
            "DFEDTAR": {"2008-04-30": 2.0, "2008-10-08": 1.5},
            "DFEDTARU": {"2008-12-16": 0.25, "2018-03-22": 1.75, "2019-09-17": 2.25, "2019-09-19": 2.0},
            "DFEDTARL": {"2008-12-16": 0.0, "2018-03-22": 1.50, "2019-09-17": 2.0, "2019-09-19": 1.75}}
    out = {i: (label, values, tier) for i, label, values, tier in R.repo_against_policy(lambda k: data.get(k, {}))}
    above, below = out["D_REPO_TARGET"][1], out["D_TARGET_REPO"][1]
    assert round(above["2008-09-12"], 6) == 20.0 and round(above["2008-09-17"], 6) == -175.0 and round(below["2008-09-17"], 6) == 175.0
    assert round(above["2009-01-02"], 6) == -7.5                       # 0.05 against the middle of 0-0.25
    assert round(above["2018-02-28"], 6) == 140.0 - 12.5               # carried forward: the middle of 0-0.25 until the next change in this fixture
    assert round(above["2018-04-03"], 6) == round((1.83 - 1.625) * 100, 6) and round(above["2019-09-17"], 6) == 312.5
    assert sorted(above) == sorted(below) and all(below[d] == -above[d] for d in above) and out["D_REPO_TARGET"][2] == 1
    assert "dealer-survey" in out["D_REPO_TARGET"][0] and "Treasury collateral is scarce" in out["D_TARGET_REPO"][0]
    assert R.repo_against_policy(lambda k: {k: v for k, v in data.items() if k != "GC_SURVEY"}.get(k, {})) == []      # no survey rate banked: nothing older than SOFR is claimed
    ids = [i for i, *_ in R.derived_series(lambda k: data.get(k, {}))]
    assert ids[-2:] == ["D_REPO_TARGET", "D_TARGET_REPO"] and ids[:5] == ["D_SOFR_EFFR", "D_SOFR_TAIL", "D_SOFR_ADMIN", "D_EFFR_TARGET", "D_FED_REPO"]


def refusal(code):
    import urllib.error
    return urllib.error.HTTPError("https://api.stlouisfed.org/x", code, "refused", {}, None)


def test_fred_answers_are_asked_again_only_when_the_refusal_can_pass():
    import io
    import json

    class Opener:
        def __init__(self, script):
            self.script, self.calls, self.agents = list(script), 0, []

        def __call__(self, req, timeout=None):
            self.calls += 1
            self.agents.append(req.get_header("User-agent"))
            step = self.script.pop(0)
            if isinstance(step, Exception):
                raise step
            return io.BytesIO(json.dumps(step).encode())

    naps = []
    o = Opener([refusal(503), refusal(429), {"ok": 1}])
    assert S.fred_json("https://api.stlouisfed.org/fred/x", sleep=naps.append, opener=o) == {"ok": 1} and o.calls == 3 and naps == [3, 8] and o.agents[0] == "JustHodl research"
    o = Opener([refusal(400), {"ok": 1}])                              # a bad request stays bad: not asked again
    try:
        S.fred_json("https://api.stlouisfed.org/fred/x", sleep=naps.append, opener=o)
        raise AssertionError("a 400 was retried into an answer")
    except Exception as e:
        assert getattr(e, "code", None) == 400 and o.calls == 1
    o = Opener([refusal(503), OSError("reset"), refusal(502)])
    try:
        S.fred_json("https://api.stlouisfed.org/fred/x", sleep=lambda n: None, opener=o)
        raise AssertionError("three refusals became an answer")
    except Exception as e:
        assert getattr(e, "code", None) == 502 and o.calls == 3


def test_fred_inventory_asks_the_engines_three_questions():
    asked = []

    def get_json(url):
        asked.append(url)
        if "tags/series" in url and "offset=0" in url:
            return {"seriess": [{"id": "A%03d" % i, "title": "t%d" % i} for i in range(500)]}
        if "tags/series" in url:
            return {"seriess": [{"id": "B1", "title": "second page " + "x" * 200}]}
        return {"seriess": [{"id": "SOFR", "title": "Secured Overnight Financing Rate"}, {"id": "OBFR", "title": "not in the SOFR family"}, {"id": "A001", "title": "other title"}]}

    inv = S.fred_repo_inventory("KEY", get_json)
    assert len(inv) == 502 and inv["A001"] == "t1" and inv["SOFR"] == "Secured Overnight Financing Rate" and "OBFR" not in inv and len(inv["B1"]) == 150
    assert len(asked) == 3 and "offset=500" in asked[1] and "repurchase+agreements" in asked[0] and "search_text=SOFR" in asked[2] and all("api_key=KEY" in u for u in asked)
    assert S.inventory_short({}, {}) and not S.inventory_short({"a": ""}, {}) and S.inventory_short({"a": ""}, {str(i): "" for i in range(10)})
    assert not S.inventory_short({str(i): "" for i in range(8)}, {str(i): "" for i in range(10)})


def test_fred_group_inventory_keeps_the_group_when_fred_refuses():
    whole = {"SOFR": "Secured Overnight Financing Rate", "RPONTSYD": "Fed repo", "BOGZ1": "a flow line", "BOGZ2": "another", "BOGZ3": "third"}
    prefix = "data/warm/fred-scoped/FRED_Repo_Complex"
    listing = [prefix + "/SOFR.json", prefix + "/IORB.json", prefix + "/_note.json", prefix + "/readme.txt"]

    def refuse(url):
        raise refusal(503)

    naps = []
    # a whole answer: recorded once, and not written again while it stays the same
    bank = Bank()
    got, how = S.fred_group_inventory(whole, "KEY", bank.read, bank.put, lambda p: listing, prefix, "2026-11-22T06:10:00+00:00", get_json=refuse, sleep=naps.append)
    assert got == whole and how == "fred" and bank.objects[S.FRED_INVENTORY_KEY] == {"at": "2026-11-22T06:10:00+00:00", "ids": whole} and naps == []
    got, how = S.fred_group_inventory(whole, "KEY", bank.read, bank.put, lambda p: listing, prefix, "later", get_json=refuse, sleep=naps.append)
    assert how == "fred" and bank.puts == [S.FRED_INVENTORY_KEY] and bank.objects[S.FRED_INVENTORY_KEY]["at"] == "2026-11-22T06:10:00+00:00"

    # the 06:10 run of 2026-11-22: nothing came back, FRED still refuses -> the last good inventory, nothing recorded
    got, how = S.fred_group_inventory({}, "KEY", bank.read, bank.put, lambda p: listing, prefix, "t", get_json=refuse, sleep=naps.append)
    assert got == whole and how == "fred refused (HTTPError): served from the last good inventory of 2026-11-22" and naps == [3] and bank.puts == [S.FRED_INVENTORY_KEY]
    # half an answer (the second page failed) is short of the last good one: asked again, and the second answer is taken
    got, how = S.fred_group_inventory({"SOFR": "x"}, "KEY", bank.read, bank.put, lambda p: listing, prefix, "t2", get_json=lambda u: {"seriess": [{"id": k, "title": v} for k, v in whole.items()]}, sleep=naps.append)
    assert got == whole and how == "fred, asked again" and bank.puts == [S.FRED_INVENTORY_KEY]
    # FRED itself now lists a new series: two answers agree, the inventory moves on
    grown = dict(whole, NEW1="new line")
    got, how = S.fred_group_inventory(grown, "KEY", bank.read, bank.put, lambda p: listing, prefix, "t3", get_json=refuse, sleep=naps.append)
    assert got == grown and how == "fred" and bank.objects[S.FRED_INVENTORY_KEY] == {"at": "t3", "ids": grown}

    # before any inventory was recorded: the bank's own documents are the inventory (titles come from the documents)
    fresh = Bank()
    got, how = S.fred_group_inventory({}, "KEY", fresh.read, fresh.put, lambda p: listing if p == prefix + "/" else [], prefix, "t", get_json=refuse, sleep=naps.append)
    assert got == {"SOFR": "", "IORB": ""} and how == "fred refused (HTTPError): served from the bank's own listing" and fresh.puts == []
    # without a key nothing is asked; an empty answer still falls back
    got, how = S.fred_group_inventory({}, "", bank.read, bank.put, lambda p: listing, prefix, "t", get_json=refuse, sleep=naps.append)
    assert got == grown and how == "fred answered nothing: served from the last good inventory of t3"
    # a second answer that is empty is a refusal too
    got, how = S.fred_group_inventory({}, "KEY", bank.read, bank.put, lambda p: listing, prefix, "t", get_json=lambda u: {"seriess": []}, sleep=naps.append)
    assert got == grown and "served from the last good inventory" in how


def test_bank_keys_reads_every_page():
    class Store:
        def list_objects_v2(self, **kw):
            assert kw["Bucket"] == "b" and kw["Prefix"] == "p/"
            if "ContinuationToken" not in kw:
                return {"Contents": [{"Key": "p/a"}, {"Key": "p/b"}], "NextContinuationToken": "n"}
            return {"Contents": [{"Key": "p/c"}]}

    assert S.bank_keys(Store(), "b")("p/") == ["p/a", "p/b", "p/c"]


def test_the_module_has_no_contact_line_and_no_engine_state():
    text = (HERE.parent / "source" / "repo_sources.py").read_text()
    assert "@" not in text and "boto3" not in text


def main():
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    for _, fn in tests:
        fn()
    print("repo sources: %d tests passed" % len(tests))


if __name__ == "__main__":
    main()
