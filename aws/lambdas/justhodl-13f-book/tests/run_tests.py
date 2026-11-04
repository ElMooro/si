"""justhodl-13f-book offline tests: an in-memory S3 seeded with two small native fund artifacts, a desk, a clone-alpha
file, a detector and a brief. Boards, statuses, the reclassification / no-current-holder flags, the filing-season
arithmetic and the full build() are checked. No network, no AWS.

Run: python3 aws/lambdas/justhodl-13f-book/tests/run_tests.py
"""
from datetime import date, datetime, timezone
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
sys.path[:0] = [SRC]
from types import SimpleNamespace  # noqa: E402
BEGUN = []
sys.modules["chain_guard"] = SimpleNamespace(begin=lambda event, context=None: BEGUN.append(event) or 0)   # offline: the trigger cap's ledger is not reachable

import lambda_function as lf  # noqa: E402


class S3Stub:
    def __init__(self):
        self.store, self.puts = {}, []

    def put(self, key, doc):
        self.store[key] = json.dumps(doc).encode()

    def get_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError("NoSuchKey: " + Key)
        import io
        return {"Body": io.BytesIO(self.store[Key]), "LastModified": datetime(2026, 11, 3, 15, 41, tzinfo=timezone.utc), "ContentLength": len(self.store[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.store:
            raise KeyError("NoSuchKey: " + Key)
        return {"ContentLength": len(self.store[Key]), "LastModified": datetime(2026, 11, 3, 15, 41, tzinfo=timezone.utc)}

    def put_object(self, **kw):
        self.puts.append(kw)


def pos(cusip, name, qty, value, cls="COM", put_call=None):
    return {"identity": {"cusip": cusip, "class": cls, "quantity_type": "SH", "put_call": put_call}, "issuer_names": [name],
            "reported_quantity": str(qty), "reported_value_usd": str(value), "other_managers": []}


def artifact(cur, pri, rows, cur_p="2026-06-30", pri_p="2026-03-31"):
    return {"contract": "holdings-native-fund.v1", "current_holdings_period": cur_p, "prior_holdings_period": pri_p,
            "periods": {cur_p: {"status": "complete_selected_public_chain", "positions": cur}, pri_p: {"status": "complete_selected_public_chain", "positions": pri}},
            "comparison": {"rows": rows, "counts": {}},
            "filing_summaries": {"0001-26-000915": {"accession": "0001-26-000915", "form": "13F-HR", "period_of_report": cur_p, "filed_at": "2026-08-14",
                                                     "accepted_at": "2026-08-14T21:06:21.000Z", "cover": {"amendment_number": None, "reported_rows": len(cur)}}}}


def seed():
    s3 = S3Stub()
    # fund A: concentrated; Apple unchanged, Alphabet up, Honeywell old CUSIP absent + new CUSIP present (reclass), Apellis absent
    a_cur = {"a": pos("037833100", "APPLE INC", 100, 20000), "b": pos("02079K305", "ALPHABET INC", 120, 9000, "CAP STK CL A"),
             "h2": pos("438516205", "HONEYWELL INTL INC", 10, 500), "o": pos("037833100", "APPLE INC", 5, 700, "COM", "PUT")}
    a_pri = {"a": pos("037833100", "APPLE INC", 100, 15000), "b": pos("02079K305", "ALPHABET INC", 100, 7000, "CAP STK CL A"),
             "h1": pos("438516106", "HONEYWELL INTL INC", 10, 450), "x": pos("03753U106", "APELLIS PHARMACEUTICALS INC", 50, 900)}
    a_rows = [{"position_id": "a", "status": "reported_quantity_unchanged", "reported_quantity_change": "0", "reported_value_change_usd": "5000"},
              {"position_id": "b", "status": "reported_quantity_increased", "reported_quantity_change": "20", "reported_value_change_usd": "2000"},
              {"position_id": "h2", "status": "newly_present_in_public_disclosure"}, {"position_id": "h1", "status": "not_present_in_current_public_disclosure"},
              {"position_id": "x", "status": "not_present_in_current_public_disclosure"}, {"position_id": "o", "status": "newly_present_in_public_disclosure"}]
    # fund B: Apple down, Alphabet up, Apellis absent too
    b_cur = {"a": pos("037833100", "APPLE INC", 80, 16000), "b": pos("02079K305", "ALPHABET INC", 50, 3750, "CAP STK CL A")}
    b_pri = {"a": pos("037833100", "APPLE INC", 90, 13500), "b": pos("02079K305", "ALPHABET INC", 40, 2800, "CAP STK CL A"), "x": pos("03753U106", "APELLIS PHARMACEUTICALS INC", 5, 90)}
    b_rows = [{"position_id": "a", "status": "reported_quantity_decreased", "reported_quantity_change": "-10", "reported_value_change_usd": "2500"},
              {"position_id": "b", "status": "reported_quantity_increased", "reported_quantity_change": "10", "reported_value_change_usd": "950"},
              {"position_id": "x", "status": "not_present_in_current_public_disclosure"}]
    # fund C: dated (notice filed), not in the cohort
    c_cur = {"m": pos("594918104", "MICROSOFT CORP", 10, 5000)}
    s3.put("data/holdings-research/funds/aaa.json", artifact(a_cur, a_pri, a_rows))
    s3.put("data/holdings-research/funds/bbb.json", artifact(b_cur, b_pri, b_rows))
    s3.put("data/holdings-research/funds/ccc.json", artifact(c_cur, {}, [{"position_id": "m", "status": "comparison_unavailable"}], "2026-03-31", "2025-12-31"))
    s3.put("data/13f-desk.json", {
        "contract": "holdings-native-research.v1", "generated_at": "2026-11-03T15:41:18+00:00", "required_period_for_current_cohort": "2026-06-30",
        "limitations": ["Public long-security disclosures omit short positions."], "quality": {"trades_inferred": False},
        "report_period_cohorts": {"2026-06-30": ["ELLIOTT", "BERKSHIRE"], "2026-03-31": ["PERSHING"]},
        "by_fund": {
            "ELLIOTT": {"name": "ELLIOTT", "cik": "0001791786", "current_cohort_eligible": True, "current_chain_status": "complete_selected_public_chain", "observation_age_days": 126,
                        "latest_submission": {"form": "13F-HR", "filed_at": "2026-08-14", "period_of_report": "2026-06-30", "accession": "0001-26-000915"},
                        "detail": {"key": "data/holdings-research/funds/aaa.json", "sha256": "aaa"}, "valuation_reviews": []},
            "BERKSHIRE": {"name": "BERKSHIRE HATHAWAY INC", "cik": "0001067983", "current_cohort_eligible": True, "current_chain_status": "complete_selected_public_chain", "observation_age_days": 126,
                          "latest_submission": {"form": "13F-HR/A", "filed_at": "2026-09-02", "period_of_report": "2026-06-30", "accession": "0002-26-000001"},
                          "detail": {"key": "data/holdings-research/funds/bbb.json", "sha256": "bbb"}, "valuation_reviews": ["units under review"]},
            "PERSHING": {"name": "Pershing Square", "cik": "0001336528", "current_cohort_eligible": False, "current_chain_status": "complete_selected_public_chain", "observation_age_days": 217,
                         "latest_submission": {"form": "13F-NT", "filed_at": "2026-08-14", "period_of_report": "2026-03-31", "accession": "0003-26-000001"},
                         "detail": {"key": "data/holdings-research/funds/ccc.json", "sha256": "ccc"}, "valuation_reviews": []}}})
    s3.put("data/13f-clone-alpha.json", {"as_of": "2026-10-26T08:30:44+00:00", "status": "COMPLETE", "pct_complete": 100.0, "managers": {
        "ELLIOTT": {"name": "Elliott", "n_windows": 10, "skill_score": 77, "label": "WORTH CLONING", "ann_alpha_pct": 8.63, "hit_rate": 0.6, "ir": 0.46,
                    "windows": [{"q": "2025-12-31", "entry": "2026-02-18", "exit": "2026-05-15", "ret": 4.53, "spy": 7.71, "alpha": -3.18, "cov": 1.0}]},
        "BERKSHIRE": {"name": "Berkshire Hathaway", "n_windows": 9, "skill_score": 0, "label": "FAMOUS ≠ SKILLED", "windows": []}}})
    s3.put("data/institutional-positions.json", {"generated_at": "2026-11-03T03:31:29+00:00", "tracked_funds": 3, "filings_seen": 3, "fetch_errors": 0, "new_filings": [],
                                                "by_fund": {"ELLIOTT": {"name": "Elliott", "latest_filing": {"period_of_report": "2026-09-30", "filed_at": "2026-11-02", "accession": "0001-26-001200", "form": "13F-HR"}}}})
    s3.put("data/ai-commentary/13f.json", {"generated_at": "2026-11-03T14:00:49+00:00", "model": "deterministic-source-inventory", "model_api_calls": 0, "portfolio_action": "WAIT",
                                           "commentary": {"headline": "4 of 4 declared inputs parsed", "narrative": "stored engine reports", "posture": "WAIT", "mode": "research_inventory"}})
    return s3


def test_fund_boards():
    s3 = seed()
    detail = json.loads(s3.store["data/holdings-research/funds/aaa.json"])
    b, eq, eqp, comp = lf.fund_boards(detail)
    assert b["n_equity_positions"] == 3 and b["n_option_rows"] == 1 and b["total_value_usd"] == 29500.0 and b["put_value_usd"] == 700.0
    assert b["top"][0]["issuer"] == "APPLE INC" and b["top"][0]["status"] == "reported_quantity_unchanged" and b["top"][0]["value_change_usd"] == 5000.0
    assert b["top"][1]["qty_change_pct"] == 20.0 and b["top10_pct_of_book"] == 100.0
    assert b["value_change_pct"] == 26.3  # 29500 / 23350
    assert [r["issuer"] for r in b["newly_present"]] == ["HONEYWELL INTL INC"] and b["n_newly_present_equity"] == 1
    assert {r["issuer"] for r in b["absent"]} == {"HONEYWELL INTL INC", "APELLIS PHARMACEUTICALS INC"}
    assert b["increased"][0]["issuer"] == "ALPHABET INC" and b["decreased"] == []


def test_cross_boards_flags():
    s3 = seed()
    members = []
    for key, fk in (("aaa", "ELLIOTT"), ("bbb", "BERKSHIRE")):
        detail = json.loads(s3.store["data/holdings-research/funds/%s.json" % key])
        b, eq, eqp, comp = lf.fund_boards(detail)
        members.append((fk, eq, eqp, comp))
    x = lf.cross_boards(members)
    wh = x["widely_held"][0]
    assert wh["issuer"] == "APPLE INC" and wh["n_holders"] == 2 and wh["value_usd"] == 36000.0 and wh["n_dec"] == 1 and wh["n_flat"] == 1
    inc = x["increased"][0]
    assert inc["issuer"] == "ALPHABET INC" and inc["n_inc"] == 2 and inc["inc"] == ["BERKSHIRE", "ELLIOTT"]
    newly = {r["issuer"]: r for r in x["newly_present"]}
    assert newly["HONEYWELL INTL INC"]["reclass_candidate"] is True and newly["HONEYWELL INTL INC"]["no_current_holder"] is False
    dropped = {(r["issuer"], r["cusip"]): r for r in x["dropped"]}
    ap = dropped[("APELLIS PHARMACEUTICALS INC", "03753U106")]
    assert ap["n_absent"] == 2 and ap["no_current_holder"] is True and ap["reclass_candidate"] is False
    hon = dropped[("HONEYWELL INTL INC", "438516106")]
    assert hon["reclass_candidate"] is True and hon["no_current_holder"] is True
    assert x["n_identities"] == 5


def test_status_and_season():
    assert lf.status_of({"latest_submission": {"form": "13F-HR"}}, {"current_period": "2026-06-30", "n_option_rows": 0}, "2026-06-30") == ("CURRENT", [])
    assert lf.status_of({"latest_submission": {"form": "13F-HR/A"}}, {"current_period": "2026-06-30", "n_option_rows": 7282}, "2026-06-30") == ("AMENDED", ["HEDGE_BOOK"])
    assert lf.status_of({"latest_submission": {"form": "13F-NT"}}, {"current_period": "2026-03-31", "n_option_rows": 0}, "2026-06-30") == ("NOTICE", [])
    assert lf.status_of({"latest_submission": {"form": "13F-HR"}, "valuation_reviews": ["x"]}, {"current_period": "2026-06-30", "n_option_rows": 0}, "2026-06-30") == ("CURRENT", ["VALUATION_REVIEW"])
    assert lf.status_of({"latest_submission": {"form": "13F-HR"}}, {"current_period": "2023-12-31", "n_option_rows": 0}, "2026-06-30") == ("DATED", [])
    assert lf.quarter_end_after(date(2026, 6, 30)) == date(2026, 9, 30) and lf.quarter_end_after(date(2026, 12, 31)) == date(2027, 3, 31)
    assert lf.filing_deadline(date(2026, 9, 30)) == date(2026, 11, 16)   # Nov 14 2026 is a Saturday
    assert lf.filing_deadline(date(2026, 6, 30)) == date(2026, 8, 14)
    assert lf.filing_deadline(date(2026, 12, 31)) == date(2027, 2, 16)   # Feb 14 Sun, Feb 15 Presidents' Day
    assert lf.filing_deadline(date(2026, 3, 31)) == date(2026, 5, 15)
    s = lf.season("2026-06-30", date(2026, 11, 3), {"generated_at": "x", "by_fund": {"ELLIOTT": {"latest_filing": {"period_of_report": "2026-09-30"}}}},
                  {"ELLIOTT": {"latest_submission": {"period_of_report": "2026-06-30"}}, "BERKSHIRE": {}})
    assert s["next_period"] == "2026-09-30" and s["deadline"] == "2026-11-16" and s["days_to_deadline"] == 13 and s["filed_for_next"] == ["ELLIOTT"] and s["tracked"] == 2
    assert lf.issuer_key("HONEYWELL INTL INC") == lf.issuer_key("HONEYWELL INTL INC NEW") == "HONEYWELL INTL"
    assert lf.issuer_key("LIBERTY CAP CORP") != lf.issuer_key("GCI LIBERTY INC")


def test_build_and_handler():
    s3 = seed()
    now = datetime(2026, 11, 3, 16, 37, tzinfo=timezone.utc)
    out = lf.build(s3, now, log=lambda *a: None)
    assert out["contract"] == "13f-book.v1" and out["book"]["required_period"] == "2026-06-30"
    m = out["book"]["managers"]
    assert set(m) == {"ELLIOTT", "BERKSHIRE", "PERSHING"}
    assert m["ELLIOTT"]["status"] == "CURRENT" and m["BERKSHIRE"]["status"] == "AMENDED" and "VALUATION_REVIEW" in m["BERKSHIRE"]["flags"]
    assert m["PERSHING"]["status"] == "NOTICE" and m["PERSHING"]["cohort"] is False
    assert out["book"]["cohort"] == ["BERKSHIRE", "ELLIOTT"] and out["book"]["concentrated"] == ["BERKSHIRE", "ELLIOTT"]
    assert out["book"]["cohort_equity_value_usd"] == 29500.0 + 19750.0
    assert m["ELLIOTT"]["clone"]["skill_score"] == 77 and out["clone_alpha"]["ranked"][0]["fund_key"] == "ELLIOTT"
    assert m["ELLIOTT"]["filings"][0]["accession"] == "0001-26-000915" and m["ELLIOTT"]["artifact"]["sha256"] == "aaa"
    assert out["season"]["filed_for_next"] == ["ELLIOTT"] and out["season"]["deadline"] == "2026-11-16"
    assert out["ai_brief"]["portfolio_action"] == "WAIT" and out["ai_brief"]["headline"].startswith("4 of 4")
    assert out["price_divergence"]["error"] == "KeyError" and out["price_divergence"]["state"] is None
    eng = {e["engine"]: e for e in out["engines"]}
    assert eng["justhodl-13f-positions"]["tier"] == "fresh" and eng["justhodl-13f-price-divergence"]["tier"] == "missing"
    assert lf.tier(202.0, 168) == "fresh" and lf.tier(300.0, 168) == "stale" and lf.tier(202.0, 24) == "dead" and lf.tier(20.0, 168) == "fresh" and lf.tier(5.0, None) == "unscheduled" and lf.tier(None, 24) == "missing"
    assert lf.tier(1300.0, 168) == "dead" and lf.tier(60.0, 72) == "fresh" and lf.tier(100.0, 72) == "stale"
    assert out["book"]["options_overlay"][0]["fund_key"] == "ELLIOTT" and out["book"]["options_overlay"][0]["put_share"] == 100.0
    json.dumps(out, allow_nan=False)  # serialisable, finite
    lf._s3 = lambda: s3
    res = lf.lambda_handler({}, None)
    assert res["statusCode"] == 200 and s3.puts and s3.puts[0]["Key"] == "data/13f-book.json" and s3.puts[0]["CacheControl"] == "max-age=300"
    assert BEGUN and BEGUN[-1] == {} and json.loads(s3.puts[0]["Body"])["trigger"] == {"kind": "scheduled", "source_generated_at": None}
    lf.lambda_handler({"trigger": "holdings_research_published", "source_generated_at": "2026-11-03T15:41:16+00:00"}, None)
    assert json.loads(s3.puts[-1]["Body"])["trigger"] == {"kind": "holdings_research_published", "source_generated_at": "2026-11-03T15:41:16+00:00"}


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
    print("justhodl-13f-book tests: all green")
