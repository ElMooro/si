"""justhodl-13f-book offline tests: an in-memory S3 seeded with small native fund artifacts, a desk, a clone-alpha
file, a detector and a brief; OpenFIGI answered by a fake /mapping (the real aws/shared/openfigi.resolution logic runs
on top of it). Boards per universe (ALL / STOCKS / ETFs), the security master's statuses and re-check policy, the
reviewed ETF classes, asset-class rotation, stance, contenders, the period snapshot, the reclassification /
no-current-holder flags, the filing-season arithmetic and the full build() + handler are checked. No network, no AWS.

Run: python3 aws/lambdas/justhodl-13f-book/tests/run_tests.py
"""
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
import importlib.util
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
SHARED = os.path.join(HERE, "..", "..", "..", "shared")
sys.path[:0] = [SRC]
BEGUN = []
sys.modules["chain_guard"] = SimpleNamespace(begin=lambda event, context=None: BEGUN.append(event) or 0)   # offline: the trigger cap's ledger is not reachable

# the real OpenFIGI client (resolution semantics) with boto3 stubbed away when it is not installed, and a fake network
for name in ("boto3", "botocore", "botocore.config"):
    if name not in sys.modules:
        try:
            __import__(name)
        except ImportError:
            sys.modules[name] = SimpleNamespace(client=lambda *a, **k: None, Config=lambda **k: None)
_spec = importlib.util.spec_from_file_location("openfigi", os.path.join(SHARED, "openfigi.py"))
openfigi = importlib.util.module_from_spec(_spec)
sys.modules["openfigi"] = openfigi
_spec.loader.exec_module(openfigi)

# CUSIP -> OpenFIGI rows (what /v3/mapping answers for ID_CUSIP + exchCode US)
def _row(ticker, name, stype, stype2="Common Stock", sector="Equity", figi="BBG000000001", comp=None, exch="US"):
    return {"figi": figi, "name": name, "ticker": ticker, "exchCode": exch, "compositeFIGI": comp or figi, "securityType": stype,
            "marketSector": sector, "shareClassFIGI": "BBG001S00001", "securityType2": stype2, "securityDescription": ticker}

FIGI = {
    "037833100": [_row("AAPL", "APPLE INC", "Common Stock", figi="BBG000B9XRY4")],
    "02079K305": [_row("GOOGL", "ALPHABET INC-CL A", "Common Stock", figi="BBG009S39JX6")],
    "438516106": [_row("HON", "HONEYWELL INTERNATIONAL INC", "Common Stock", figi="BBG000H556T9")],
    "438516205": [_row("HON", "HONEYWELL INTERNATIONAL INC", "Common Stock", figi="BBG000H556U8")],
    "03753U106": [_row("APLS", "APELLIS PHARMACEUTICALS INC", "Common Stock", figi="BBG00DZ5V3C4")],
    "594918104": [_row("MSFT", "MICROSOFT CORP", "Common Stock", figi="BBG000BPH459")],
    "78462F103": [_row("SPY", "SPDR S&P 500 ETF TRUST", "ETP", "Mutual Fund", figi="BBG000BDTBL9")],
    "78463V107": [_row("GLD", "SPDR GOLD SHARES", "ETP", "Mutual Fund", figi="BBG000CRF6Q8")],
    "46438F101": [_row("IBIT", "ISHARES BITCOIN TRUST ETF", "ETP", "Mutual Fund", figi="BBG01LQ8Z7V3")],
    "464287432": [_row("TLT", "ISHARES 20+ YEAR TREASURY BO", "ETP", "Mutual Fund", figi="BBG000BJKYW3")],
    "123456785": [_row("XYZQ", "SOME NEW THEMATIC ETF", "ETP", "Mutual Fund", figi="BBG00XYZQ001")],   # fund-like, not in the reviewed table
    "00206R102": [_row("T 4.3 02/15/30", "AT&T INC", "GLOBAL", "Corp", sector="Corp", figi="BBG00P4BXG51")],   # a note: Corp sector, no exchange
    "88160R101": [_row("TSLA", "TESLA INC", "Common Stock", figi="BBG000N9MNX3")],
    "AMBIG0001": [_row("AMB", "AMBIGUOUS CO", "Common Stock", figi="BBG0000AMB01", comp="BBG0000AMB01"),
                  _row("AMBB", "AMBIGUOUS CO-B", "Common Stock", figi="BBG0000AMB02", comp="BBG0000AMB02")],   # two US rows, different tickers
    "H1467J104": [_row("CB", "CHUBB LTD", "Common Stock", figi="BBG000BR14K5")],                       # a CINS: answered only as ID_CINS
    "30231G102": [_row("EXMOC", "EXXON MOBIL CORP", "Common Stock", figi="BBG00B8TC1V3", exch="CP"),   # OpenFIGI withholds the US CUSIP link ...
                  _row("XOM", "EXXON MOBIL CORP", "Common Stock", figi="BBG01496HVM6", comp="BBG01496HVL7", exch="UZ")],
    "US30231G1022": [_row("XOM", "EXXON MOBIL CORP", "Common Stock", figi="BBG000GZQ728")],            # ... but answers the derived ISIN
    "CEFSAME01": [_row("PTY", "PIMCO CORP&INCOME OPPORTUNITY", "Closed-End Fund", "Mutual Fund", figi="BBG0000CEF01"),
                  _row("PTY", "PIMCO CORP&INCOME OPPORTUNITY", "Closed-End Fund", "Mutual Fund", figi="BBG0000CEF02")],   # two US rows, same ticker + type
    "CEFNAV001": [_row("FSSL", "FS SPECIALTY LENDING FUND", "Closed-End Fund", "Mutual Fund", figi="BBG002VNPPN2"),
                  _row("XFSLX", "FS SPECIALTY LENDING FUND", "Closed-End Fund", "Mutual Fund", figi="BBG01YLVBPK2")],     # the second US row is Bloomberg's NAV-quote ticker
    "MULTI0001": [_row("MLT", "MULTI LISTED LTD", "Common Stock", figi="BBG0000MLT01", comp="BBG0000MLT00", exch="LN"),
                  _row("MLT", "MULTI LISTED LTD", "Common Stock", figi="BBG0000MLT00", comp="BBG0000MLT00", exch="US")],
}
CALLS = []


def fake_mapping(jobs, api_key=None, deadline=None):
    CALLS.append([dict(j) for j in jobs])
    out = []
    for j in jobs:
        c, exch, it = j.get("idValue"), j.get("exchCode"), j.get("idType")
        rows = FIGI.get(c)
        if c == "ERROR0001":
            out.append({"error": "request failed"})
        elif c == "MULTI0001" and exch == "US":
            out.append({"warning": "No identifier found."})          # the US-only pass misses it, the any-exchange pass collapses it
        elif c[:1].isalpha() and it == "ID_CUSIP":
            out.append({"warning": "No identifier found."})          # a CINS is never found as a CUSIP
        elif c == "30231G102" and exch == "US":
            out.append({"warning": "No identifier found."})          # the withheld US link
        elif not rows:
            out.append({"warning": "No identifier found."})
        elif exch == "US":
            out.append({"data": [dict(r) for r in rows if r.get("exchCode") == "US"]} if any(r.get("exchCode") == "US" for r in rows) else {"warning": "No identifier found."})
        else:
            out.append({"data": [dict(r) for r in rows]})
    return out


openfigi.mapping = fake_mapping
FAKE_KEY = "FAKE-OPENFIGI-KEY-NEVER-LOGGED"
openfigi.get_api_key = lambda: FAKE_KEY   # never printed; the fake ignores it

# the FMP bank: banked etf/info rows for a few tickers (FMP's own assetClass), nothing for the rest
BANK = {"XYZQ": [{"symbol": "XYZQ", "name": "Some New Thematic ETF", "assetClass": "Equity", "securityCusip": "123456785", "updatedAt": "2026-10-25T08:15:40.030Z"}],
        "ZZBND": [{"symbol": "ZZBND", "name": "Some Core Fixed Income ETF", "assetClass": "Fixed Income", "securityCusip": "25434V872"}],
        "ODDB": [{"symbol": "ODDB", "name": "Odd Basket", "assetClass": "Thematic Basket", "securityCusip": "000000000"}]}
BANK_CALLS = []
sys.modules["fmp_bank"] = SimpleNamespace(walker=lambda dataset, entity=None, **kw: BANK_CALLS.append((dataset, entity)) or BANK.get(entity))

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
        self.store[kw["Key"]] = kw["Body"]

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000):
        return {"Contents": [{"Key": k, "Size": len(v), "LastModified": datetime(2026, 11, 3, 16, 37, tzinfo=timezone.utc)} for k, v in self.store.items() if k.startswith(Prefix)]}


def pos(cusip, name, qty, value, cls="COM", put_call=None, qt="SH"):
    return {"identity": {"cusip": cusip, "class": cls, "quantity_type": qt, "put_call": put_call}, "issuer_names": [name],
            "reported_quantity": str(qty), "reported_value_usd": str(value), "other_managers": []}


def artifact(cur, pri, rows, cur_p="2026-06-30", pri_p="2026-03-31"):
    return {"contract": "holdings-native-fund.v1", "current_holdings_period": cur_p, "prior_holdings_period": pri_p,
            "periods": {cur_p: {"status": "complete_selected_public_chain", "positions": cur}, pri_p: {"status": "complete_selected_public_chain", "positions": pri}},
            "comparison": {"rows": rows, "counts": {}},
            "filing_summaries": {"0001-26-000915": {"accession": "0001-26-000915", "form": "13F-HR", "period_of_report": cur_p, "filed_at": "2026-08-14",
                                                     "accepted_at": "2026-08-14T21:06:21.000Z", "cover": {"amendment_number": None, "reported_rows": len(cur)}}}}


def seed(master=None):
    s3 = S3Stub()
    # fund A (ELLIOTT): Apple unchanged, Alphabet up, Honeywell old CUSIP absent + new CUSIP present (reclass), Apellis absent,
    # SPY newly present, GLD fewer units, IBIT newly present, a T note at principal (debt), an Apple put, an unresolvable CUSIP, an ambiguous one
    a_cur = {"a": pos("037833100", "APPLE INC", 100, 20000), "b": pos("02079K305", "ALPHABET INC", 120, 9000, "CAP STK CL A"),
             "h2": pos("438516205", "HONEYWELL INTL INC", 10, 500), "spy": pos("78462F103", "SPDR S&P 500 ETF TR", 10, 5000, "TR UNIT"),
             "gld": pos("78463V107", "SPDR GOLD TR", 10, 2000, "GOLD SHS"), "ibit": pos("46438F101", "ISHARES BITCOIN TR ETF", 10, 400),
             "t": pos("00206R102", "AT&T INC NOTES 4.3%", 1000000, 950000, "NOTE 4.300%", qt="PRN"),
             "o": pos("037833100", "APPLE INC", 5, 700, "COM", "PUT"), "zz": pos("999999999", "UNKNOWN PRIVATE CO", 1, 100),
             "amb": pos("AMBIG0001", "AMBIGUOUS CO", 1, 50), "mlt": pos("MULTI0001", "MULTI LISTED LTD", 1, 60), "xyz": pos("123456785", "SOME NEW THEMATIC ETF", 1, 70)}
    a_pri = {"a": pos("037833100", "APPLE INC", 100, 15000), "b": pos("02079K305", "ALPHABET INC", 100, 7000, "CAP STK CL A"),
             "h1": pos("438516106", "HONEYWELL INTL INC", 10, 450), "x": pos("03753U106", "APELLIS PHARMACEUTICALS INC", 50, 900),
             "gld": pos("78463V107", "SPDR GOLD TR", 20, 3600, "GOLD SHS"), "t": pos("00206R102", "AT&T INC NOTES 4.3%", 1000000, 940000, "NOTE 4.300%", qt="PRN")}
    a_rows = [{"position_id": "a", "status": "reported_quantity_unchanged", "reported_quantity_change": "0", "reported_value_change_usd": "5000"},
              {"position_id": "b", "status": "reported_quantity_increased", "reported_quantity_change": "20", "reported_value_change_usd": "2000"},
              {"position_id": "h2", "status": "newly_present_in_public_disclosure"}, {"position_id": "h1", "status": "not_present_in_current_public_disclosure"},
              {"position_id": "x", "status": "not_present_in_current_public_disclosure"}, {"position_id": "o", "status": "newly_present_in_public_disclosure"},
              {"position_id": "spy", "status": "newly_present_in_public_disclosure"}, {"position_id": "ibit", "status": "newly_present_in_public_disclosure"},
              {"position_id": "gld", "status": "reported_quantity_decreased", "reported_quantity_change": "-10", "reported_value_change_usd": "-1600"},
              {"position_id": "t", "status": "reported_quantity_unchanged", "reported_quantity_change": "0", "reported_value_change_usd": "10000"},
              {"position_id": "zz", "status": "newly_present_in_public_disclosure"}, {"position_id": "amb", "status": "newly_present_in_public_disclosure"},
              {"position_id": "mlt", "status": "newly_present_in_public_disclosure"}, {"position_id": "xyz", "status": "newly_present_in_public_disclosure"}]
    # fund B (BERKSHIRE): Apple fewer units, Alphabet more, Apellis absent, SPY newly present, GLD absent, TLT fewer units, TSLA new
    b_cur = {"a": pos("037833100", "APPLE INC", 80, 16000), "b": pos("02079K305", "ALPHABET INC", 50, 3750, "CAP STK CL A"),
             "spy": pos("78462F103", "SPDR S&P 500 ETF TR", 4, 2000, "TR UNIT"), "tlt": pos("464287432", "ISHARES TR 20+ YR TREAS", 5, 450),
             "tsla": pos("88160R101", "TESLA INC", 3, 900)}
    b_pri = {"a": pos("037833100", "APPLE INC", 90, 13500), "b": pos("02079K305", "ALPHABET INC", 40, 2800, "CAP STK CL A"),
             "x": pos("03753U106", "APELLIS PHARMACEUTICALS INC", 5, 90), "gld": pos("78463V107", "SPDR GOLD TR", 2, 360, "GOLD SHS"),
             "tlt": pos("464287432", "ISHARES TR 20+ YR TREAS", 10, 880)}
    b_rows = [{"position_id": "a", "status": "reported_quantity_decreased", "reported_quantity_change": "-10", "reported_value_change_usd": "2500"},
              {"position_id": "b", "status": "reported_quantity_increased", "reported_quantity_change": "10", "reported_value_change_usd": "950"},
              {"position_id": "x", "status": "not_present_in_current_public_disclosure"}, {"position_id": "spy", "status": "newly_present_in_public_disclosure"},
              {"position_id": "gld", "status": "not_present_in_current_public_disclosure"},
              {"position_id": "tlt", "status": "reported_quantity_decreased", "reported_quantity_change": "-5", "reported_value_change_usd": "-430"},
              {"position_id": "tsla", "status": "newly_present_in_public_disclosure"}]
    # fund C (PERSHING): dated (notice filed), not in the cohort
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
    if master is not None:
        s3.put("data/13f-security-master.json", master)
    return s3


NOW = datetime(2026, 11, 3, 16, 37, tzinfo=timezone.utc)


def built(master=None, **kw):
    s3 = seed(master)
    CALLS.clear()
    out = lf.build(s3, NOW, log=lambda *a: None, **kw)
    return s3, out


def test_fund_boards_without_a_master():
    s3 = seed()
    detail = json.loads(s3.store["data/holdings-research/funds/aaa.json"])
    b, sp = lf.fund_boards(detail)
    assert b["n_equity_positions"] == 10 and b["n_option_rows"] == 1 and b["n_debt_rows"] == 1 and b["put_value_usd"] == 700.0 and b["debt_value_usd"] == 950000.0
    assert b["total_value_usd"] == 37180.0 and b["top"][0]["issuer"] == "APPLE INC" and b["top"][0]["status"] == "reported_quantity_unchanged" and b["top"][0]["value_change_usd"] == 5000.0
    assert b["top"][0]["ticker"] is None and b["top"][0]["kind"] == "unmapped"   # no master: honest
    assert b["top"][1]["qty_change_pct"] == 20.0 and b["top10_pct_of_book"] == 100.0
    assert [r["issuer"] for r in b["newly_present"]][:2] == ["SPDR S&P 500 ETF TR", "HONEYWELL INTL INC"] and b["n_newly_present_equity"] == 7
    assert {r["issuer"] for r in b["absent"]} == {"HONEYWELL INTL INC", "APELLIS PHARMACEUTICALS INC"}
    assert b["increased"][0]["issuer"] == "ALPHABET INC" and b["decreased"][0]["issuer"] == "SPDR GOLD TR" and b["decreased"][0]["value_change_usd"] == -1600.0


def test_security_master_statuses_and_recheck_policy():
    rows = {}
    universe = {c: {"value": lf.Decimal(v), "sh": sh} for c, v, sh in (("037833100", 36000, True), ("78462F103", 7000, True), ("999999999", 100, True),
                                                                        ("AMBIG0001", 50, True), ("MULTI0001", 60, True), ("ERROR0001", 10, True), ("00206R102", 950000, False),
                                                                        ("H1467J104", 500, True), ("30231G102", 400, True), ("CEFSAME01", 30, True), ("CEFNAV001", 20, True))}
    CALLS.clear()
    st = lf.master_refresh(rows, universe, NOW, FAKE_KEY, 30.0, 10, log=lambda *a: None)
    assert st["sent_us"] == 11 and st["sent_any"] == 3 and st["sent_isin"] == 2 and st["requests"] == 3 and st["keyed"] is True and st["deadline_hit"] is False and st["partial"] == 0
    assert CALLS[0][0] == {"idType": "ID_CUSIP", "idValue": "037833100", "exchCode": "US"}   # SH by value first ...
    assert CALLS[0][-1]["idValue"] == "00206R102"                                           # ... the PRN identity last
    assert {j["idValue"]: j["idType"] for j in CALLS[0]}["H1467J104"] == "ID_CINS"           # a CINS is typed by its shape
    assert all("exchCode" not in j for j in CALLS[1]) and {j["idValue"] for j in CALLS[1]} == {"999999999", "MULTI0001", "30231G102"}
    assert {j["idValue"] for j in CALLS[2]} == {"US9999999991", "US30231G1022"} and all(j["idType"] == "ID_ISIN" and j["exchCode"] == "US" for j in CALLS[2])
    assert rows["037833100"]["status"] == "resolved" and rows["037833100"]["ticker"] == "AAPL" and rows["037833100"]["figi"] == "BBG000B9XRY4" and rows["037833100"]["query"] == "typed/US" and rows["037833100"]["scheme"] == "v2"
    assert rows["78462F103"]["security_type"] == "ETP" and rows["78462F103"]["security_type2"] == "Mutual Fund"
    assert rows["999999999"]["status"] == "no_match" and rows["999999999"]["query"] == "typed/any"
    assert rows["AMBIG0001"]["status"] == "ambiguous" and len(rows["AMBIG0001"]["candidates"]) == 2 and rows["AMBIG0001"]["n_rows"] == 2
    assert rows["MULTI0001"]["status"] == "resolved" and rows["MULTI0001"]["figi"] == "BBG0000MLT00" and rows["MULTI0001"]["exch_code"] == "US" and rows["MULTI0001"]["n_rows"] == 2 and rows["MULTI0001"]["query"] == "typed/any"
    assert rows["ERROR0001"]["status"] == "error" and "request failed" in rows["ERROR0001"]["reason"]
    assert rows["H1467J104"] ["ticker"] == "CB" and rows["H1467J104"]["query"] == "typed/US"                         # CINS resolved on the first pass
    assert rows["30231G102"]["ticker"] == "XOM" and rows["30231G102"]["query"] == "isin/US" and rows["30231G102"]["isin"] == "US30231G1022" and rows["30231G102"]["figi"] == "BBG000GZQ728"
    assert rows["CEFSAME01"]["status"] == "resolved" and rows["CEFSAME01"]["ticker"] == "PTY" and rows["CEFSAME01"]["n_rows"] == 2   # two US rows, one ticker + type
    assert rows["CEFNAV001"]["status"] == "resolved" and rows["CEFNAV001"]["ticker"] == "FSSL" and rows["CEFNAV001"]["n_rows"] == 2    # the NAV-quote row is set aside
    assert rows["AMBIG0001"]["type_agreed"] is True and rows["AMBIG0001"]["security_type"] == "Common Stock" and rows["AMBIG0001"]["market_sector"] == "Equity" and rows["AMBIG0001"]["name"] == "AMBIGUOUS CO"
    assert FAKE_KEY not in json.dumps(rows)   # the key never lands in the state object
    cov = lf.master_coverage(rows, universe)
    assert cov == {"identities": 11, "resolved": 8, "no_match": 1, "ambiguous": 1, "error": 1, "pending": 0, "typed_only": 1, "typed_only_value_pct": 0.0, "resolved_pct": 72.7, "resolved_value_pct": 100.0}
    # re-check policy: resolved 180 d, ambiguous 30 d, no_match 90 d, error 1 d; the never-checked first, by value
    later = NOW + timedelta(days=2)
    assert lf.master_due(rows, universe, later) == ["ERROR0001"]
    later = NOW + timedelta(days=31)
    assert lf.master_due(rows, universe, later) == ["AMBIG0001", "ERROR0001"]
    later = NOW + timedelta(days=181)
    assert lf.master_due(rows, universe, later)[:2] == ["00206R102", "037833100"]
    universe["NEWONE001"] = {"value": lf.Decimal(1), "sh": True}
    assert lf.master_due(rows, universe, NOW)[0] == "NEWONE001"
    # an unresolved row written under an older query scheme is due again (a resolved one is not)
    old = {"X1": {"status": "no_match", "checked_at": NOW.isoformat(), "query": "ID_CUSIP/US"}, "X2": {"status": "resolved", "ticker": "T", "checked_at": NOW.isoformat(), "query": "ID_CUSIP/US"}}
    assert lf.master_due(old, {"X1": {"value": lf.Decimal(1), "sh": True}, "X2": {"value": lf.Decimal(2), "sh": True}}, NOW) == ["X1"]
    # budget: 1 request keyed = 100 jobs on the first pass; the later passes cannot run, so their rows stay due (scheme v2-partial)
    rows2 = {}
    st2 = lf.master_refresh(rows2, universe, NOW, FAKE_KEY, 30.0, 1, log=lambda *a: None)
    assert st2["requests"] == 1 and st2["partial"] == 4 and rows2["999999999"]["status"] == "no_match" and rows2["999999999"]["scheme"] == "v2-partial" and rows2["037833100"]["status"] == "resolved"
    assert "999999999" in lf.master_due(rows2, universe, NOW) and "037833100" not in lf.master_due(rows2, universe, NOW)
    # deadline exhausted answers are not written (retried next run)
    openfigi.mapping, keep = (lambda jobs, api_key=None, deadline=None: [{"error": "deadline exhausted"} for _ in jobs]), openfigi.mapping
    try:
        rows3 = {}
        st3 = lf.master_refresh(rows3, universe, NOW, FAKE_KEY, 30.0, 10, log=lambda *a: None)
        assert st3["deadline_hit"] is True and rows3 == {} and st3["pending_after"] == 12
    finally:
        openfigi.mapping = keep
    assert lf.id_type("037833100") == "ID_CUSIP" and lf.id_type("G54950103") == "ID_CINS" and lf.isin_from_cusip("037833100") == "US0378331005" and lf.isin_from_cusip("30231G102") == "US30231G1022"


def test_learned_etf_classes_from_the_bank():
    rows = {"123456785": {"status": "resolved", "ticker": "XYZQ", "security_type": "ETP", "security_type2": "Mutual Fund", "market_sector": "Equity"},
            "25434V872": {"status": "resolved", "ticker": "ZZBND", "security_type": "ETP", "security_type2": "Mutual Fund", "market_sector": "Equity"},
            "000000000": {"status": "resolved", "ticker": "ODDB", "security_type": "ETP", "security_type2": "Mutual Fund", "market_sector": "Equity"},
            "111111111": {"status": "resolved", "ticker": "NOTB", "security_type": "ETP", "security_type2": "Mutual Fund", "market_sector": "Equity"},
            "78462F103": {"status": "resolved", "ticker": "SPY", "security_type": "ETP", "security_type2": "Mutual Fund", "market_sector": "Equity"},
            "037833100": {"status": "resolved", "ticker": "AAPL", "security_type": "Common Stock", "security_type2": "Common Stock", "market_sector": "Equity"}}
    universe = {c: {"value": lf.Decimal(v), "sh": True} for c, v in (("123456785", 70), ("25434V872", 50), ("000000000", 30), ("111111111", 20), ("78462F103", 7000), ("037833100", 36000))}
    classes, _ = lf.load_etf_table()
    learned = {}
    BANK_CALLS.clear()
    st = lf.learn_etf_classes(learned, rows, universe, classes, NOW, 10, log=lambda *a: None)
    assert [e for _, e in BANK_CALLS] == ["XYZQ", "ZZBND", "ODDB", "NOTB"]        # fund-like, unknown to the table, by value; SPY (table) and AAPL (stock) never looked up
    assert st["looked_up"] == 4 and st["banked"] == 3 and st["classed"] == 2 and st["unknown_label"] == 1 and st["labels"] == {"Equity": 1, "Fixed Income": 1, "Thematic Basket": 1}
    assert learned["XYZQ"]["class"] == "equity" and learned["XYZQ"]["banked"] is True and learned["ZZBND"]["class"] == "bond" and learned["ODDB"]["class"] is None and learned["ODDB"]["asset_class"] == "Thematic Basket"
    assert learned["NOTB"] == {"banked": False, "asset_class": None, "class": None, "checked_at": NOW.isoformat(timespec="seconds"), "source": "fmp-bank etf/info (not banked)"}
    # budget of 2: only the two largest; re-check: banked 180 d, missing 14 d
    learned2 = {}
    lf.learn_etf_classes(learned2, rows, universe, classes, NOW, 2, log=lambda *a: None)
    assert set(learned2) == {"XYZQ", "ZZBND"}
    BANK_CALLS.clear()
    lf.learn_etf_classes(learned, rows, universe, classes, NOW + timedelta(days=15), 10, log=lambda *a: None)
    assert [e for _, e in BANK_CALLS] == ["NOTB"]
    BANK_CALLS.clear()
    lf.learn_etf_classes(learned, rows, universe, classes, NOW + timedelta(days=181), 10, log=lambda *a: None)
    assert sorted(e for _, e in BANK_CALLS) == ["NOTB", "ODDB", "XYZQ", "ZZBND"]
    cl = lf.Classer(rows, classes, learned)
    ident = lambda c: {"cusip": c, "class": "COM", "quantity_type": "SH", "put_call": None}
    assert cl.of(ident("123456785"))["sub"] == "equity" and cl.of(ident("123456785"))["class_source"] == "fmp" and cl.of(ident("123456785"))["bucket"] == "equities"
    assert cl.of(ident("25434V872"))["bucket"] == "bonds" and cl.of(ident("78462F103"))["class_source"] == "table"
    assert cl.of(ident("000000000"))["sub"] == "unclassified" and cl.of(ident("000000000"))["fmp_asset_class"] == "Thematic Basket" and cl.of(ident("000000000"))["class_source"] is None
    assert lf.fmp_class("Fixed Income") == "bond" and lf.fmp_class("DIGITAL ASSETS") == "crypto" and lf.fmp_class("Real Estate") == "equity" and lf.fmp_class("Currency") == "currency" and lf.fmp_class("weird") is None and lf.fmp_class(None) is None
    assert lf.fmp_class("Sector Equity") == "equity" and lf.fmp_class("Emerging Markets Equity") == "equity" and lf.fmp_class("Active Equities") == "equity" and lf.fmp_class("Leveraged/Inverse") is None and lf.fmp_class("Multi-Asset") == "multi_asset" and lf.fmp_class("Alternatives") == "multi_asset"
    # a product the table keys by CUSIP (OpenFIGI carries no ticker for it)
    cl2 = lf.Classer({"25434V500": {"status": "resolved", "ticker": None, "name": "DIMENSIONAL US SMALL CAP ETF", "security_type": "ETP", "security_type2": "Mutual Fund", "market_sector": "Equity"}}, classes)
    assert cl2.of({"cusip": "25434V500", "class": "COM", "quantity_type": "SH", "put_call": None}) == {"ticker": None, "name": "DIMENSIONAL US SMALL CAP ETF", "security_type": "ETP", "master": "resolved", "class_source": "table", "kind": "fund", "sub": "equity", "bucket": "equities"}


def test_classer_and_universes():
    rows = {"037833100": {"status": "resolved", "ticker": "AAPL", "name": "APPLE INC", "security_type": "Common Stock", "security_type2": "Common Stock", "market_sector": "Equity"},
            "78462F103": {"status": "resolved", "ticker": "SPY", "name": "SPDR S&P 500", "security_type": "ETP", "security_type2": "Mutual Fund", "market_sector": "Equity"},
            "123456785": {"status": "resolved", "ticker": "XYZQ", "name": "NEW ETF", "security_type": "ETP", "security_type2": "Mutual Fund", "market_sector": "Equity"},
            "46438F101": {"status": "resolved", "ticker": "IBIT", "name": "ISHARES BITCOIN", "security_type": "ETP", "security_type2": "Mutual Fund", "market_sector": "Equity"},
            "REIT00001": {"status": "resolved", "ticker": "O", "name": "REALTY INCOME", "security_type": "REIT", "security_type2": "Common Stock", "market_sector": "Equity"},
            "ADR000001": {"status": "resolved", "ticker": "TSM", "name": "TAIWAN SEMI", "security_type": "ADR", "security_type2": "Depositary Receipt", "market_sector": "Equity"},
            "BOND00001": {"status": "resolved", "ticker": "T 4.3", "name": "AT&T", "security_type": "GLOBAL", "security_type2": "Corp", "market_sector": "Corp"},
            "PREF00001": {"status": "resolved", "ticker": "GOOGL 6.25 05/15/29 B", "name": "ALPHABET INC", "security_type": "Convertible Preferred", "security_type2": "Preferred", "market_sector": "Pref"},
            "AMBIG0001": {"status": "ambiguous"}}
    classes, reviewed = lf.load_etf_table()
    assert classes["SPY"] == "equity" and classes["GLD"] == "gold" and classes["IBIT"] == "crypto" and classes["TLT"] == "bond" and classes["BIL"] == "cash" and reviewed
    cl = lf.Classer(rows, classes)
    ident = lambda c, qt="SH", pc=None: {"cusip": c, "class": "COM", "quantity_type": qt, "put_call": pc}
    assert cl.of(ident("037833100")) == {"ticker": "AAPL", "name": "APPLE INC", "security_type": "Common Stock", "master": "resolved", "class_source": None, "kind": "stock", "sub": "common", "bucket": "equities"}
    assert cl.of(ident("PREF00001"))["kind"] == "stock" and cl.of(ident("PREF00001"))["sub"] == "preferred" and cl.of(ident("PREF00001"))["bucket"] == "equities"
    assert cl.of(ident("78462F103"))["kind"] == "fund" and cl.of(ident("78462F103"))["sub"] == "equity" and cl.of(ident("78462F103"))["bucket"] == "equities"
    assert cl.of(ident("46438F101"))["bucket"] == "crypto" and cl.of(ident("123456785")) == {"ticker": "XYZQ", "name": "NEW ETF", "security_type": "ETP", "master": "resolved", "class_source": None, "kind": "fund", "sub": "unclassified", "bucket": "funds_unclassified", "fmp_asset_class": None}
    assert cl.of(ident("REIT00001"))["sub"] == "reit" and cl.of(ident("ADR000001"))["sub"] == "adr" and cl.of(ident("BOND00001")) == {"ticker": "T 4.3", "name": "AT&T", "security_type": "GLOBAL", "master": "resolved", "class_source": None, "kind": "other", "sub": "corp", "bucket": "other"}
    assert cl.of(ident("037833100", "PRN"))["kind"] == "debt" and cl.of(ident("037833100", "PRN"))["bucket"] == "bonds"
    assert cl.of(ident("037833100", "SH", "PUT"))["kind"] == "option" and cl.of(ident("037833100", "SH", "PUT"))["bucket"] is None
    assert cl.of(ident("AMBIG0001")) == {"ticker": None, "name": None, "security_type": None, "master": "ambiguous", "class_source": None, "kind": "unmapped", "sub": "ambiguous", "bucket": "unmapped"}
    assert cl.of(ident("NOTCHECKED"))["sub"] == "pending"
    assert lf.in_universe(cl.of(ident("037833100")), "stocks") and not lf.in_universe(cl.of(ident("037833100")), "etfs") and lf.in_universe(cl.of(ident("037833100")), "all")
    assert lf.in_universe(cl.of(ident("78462F103")), "etfs") and lf.in_universe(cl.of(ident("AMBIG0001")), "all") and not lf.in_universe(cl.of(ident("AMBIG0001")), "stocks")
    assert lf.in_universe(cl.of(ident("037833100", "PRN")), "rotation") and not lf.in_universe(cl.of(ident("037833100", "PRN")), "all")
    assert not lf.in_universe(cl.of(ident("037833100", "SH", "PUT")), "rotation")
    assert lf.stance_of(0, 0) == "QUIET" and lf.stance_of(2, 0) == "ACCUMULATING" and lf.stance_of(4, 2) == "ACCUMULATING" and lf.stance_of(3, 2) == "CONTESTED"
    assert lf.stance_of(0, 1) == "DISTRIBUTING" and lf.stance_of(1, 2) == "DISTRIBUTING" and lf.stance_of(1, 1) == "CONTESTED"
    assert lf.bucket_stance(2, 0) == "ACCUMULATING" and lf.bucket_stance(1, 1) == "CONTESTED" and lf.bucket_stance(60, 40) == "ACCUMULATING" and lf.bucket_stance(55, 45) == "BALANCED" and lf.bucket_stance(40, 60) == "DISTRIBUTING" and lf.bucket_stance(0, 0) == "QUIET"


def test_build_boards_rotation_stance_contenders_snapshot():
    s3, out = built()
    assert out["contract"] == "13f-book.v2" and out["version"] == "1.1.2" and out["book"]["required_period"] == "2026-06-30"
    b = out["book"]
    m = b["managers"]
    assert set(m) == {"ELLIOTT", "BERKSHIRE", "PERSHING"}
    assert m["ELLIOTT"]["status"] == "CURRENT" and m["BERKSHIRE"]["status"] == "AMENDED" and "VALUATION_REVIEW" in m["BERKSHIRE"]["flags"]
    assert m["PERSHING"]["status"] == "NOTICE" and m["PERSHING"]["cohort"] is False
    assert b["cohort"] == ["BERKSHIRE", "ELLIOTT"] and b["concentrated"] == ["BERKSHIRE", "ELLIOTT"]
    # the master was refreshed in-run and written; tickers now flow into the drill-downs
    sm = b["security_master"]
    assert sm["refresh"]["skipped"] is False and sm["refresh"]["sent_us"] == 16 and sm["coverage"]["resolved"] == 14 and sm["coverage"]["no_match"] == 1 and sm["coverage"]["ambiguous"] == 1
    assert "data/13f-security-master.json" in s3.store and json.loads(s3.store["data/13f-security-master.json"])["by_cusip"]["88160R101"]["ticker"] == "TSLA"
    assert m["ELLIOTT"]["top"][0]["ticker"] == "AAPL" and m["ELLIOTT"]["top"][0]["kind"] == "stock" and m["PERSHING"]["top"][0]["ticker"] == "MSFT"
    assert {r["ticker"]: r["kind"] for r in m["ELLIOTT"]["newly_present"]}["SPY"] == "fund" and {r["ticker"] for r in m["ELLIOTT"]["absent"]} == {"HON", "APLS"}
    # boards per universe
    cb = b["boards"]["cohort"]
    assert cb["all"]["n_identities"] == 14 and cb["stocks"]["widely_held"][0]["ticker"] in ("AAPL", "GOOGL") and all(r["kind"] == "stock" for r in cb["stocks"]["widely_held"])
    assert [r["ticker"] for r in cb["etfs"]["widely_held"]][0] == "SPY" and cb["etfs"]["widely_held"][0]["n_holders"] == 2 and all(r["kind"] == "fund" for r in cb["etfs"]["newly_present"])
    assert cb["etfs"]["dropped"][0]["ticker"] == "GLD" and cb["etfs"]["dropped"][0]["n_absent"] == 1 and cb["etfs"]["decreased"][0]["ticker"] in ("GLD", "TLT")
    apple = next(r for r in cb["all"]["widely_held"] if r["ticker"] == "AAPL")
    assert apple["n_holders"] == 2 and apple["n_prior_holders"] == 2 and apple["holder_delta"] == 0 and apple["value_usd"] == 36000.0 and apple["value_prior_usd"] == 28500.0
    assert apple["votes"] == {"bull": 0, "bear": 1, "net": -1, "weighted_net": -0.0, "weighted_coverage": 1} and apple["stance"] == "DISTRIBUTING"
    newly = {r["issuer"]: r for r in cb["all"]["newly_present"]}
    assert newly["HONEYWELL INTL INC"]["reclass_candidate"] is True and newly["HONEYWELL INTL INC"]["no_current_holder"] is False and newly["HONEYWELL INTL INC"]["ticker"] == "HON"
    dropped = {(r["issuer"], r["cusip"]): r for r in cb["all"]["dropped"]}
    ap = dropped[("APELLIS PHARMACEUTICALS INC", "03753U106")]
    assert ap["n_absent"] == 2 and ap["no_current_holder"] is True and ap["reclass_candidate"] is False and ap["ticker"] == "APLS"
    assert dropped[("HONEYWELL INTL INC", "438516106")]["reclass_candidate"] is True
    # stance: GOOGL (2 more) and SPY (2 new) bullish; APLS (2 absent) bearish; HON (reclass) excluded; AAPL (-1) below the agreement floor
    st = b["stance"]["cohort"]
    assert [r["ticker"] for r in st["all"]["bullish"]] == ["GOOGL", "SPY"] and st["all"]["bullish"][0]["votes"]["weighted_net"] == 0.77 and st["all"]["bullish"][0]["stance"] == "ACCUMULATING"
    assert [r["ticker"] for r in st["all"]["bearish"]] == ["GLD", "APLS"] and st["all"]["bearish"][1]["votes"] == {"bull": 0, "bear": 2, "net": -2, "weighted_net": -0.77, "weighted_coverage": 2}
    assert [r["ticker"] for r in st["stocks"]["bullish"]] == ["GOOGL"] and [r["ticker"] for r in st["etfs"]["bullish"]] == ["SPY"] and [r["ticker"] for r in st["etfs"]["bearish"]] == ["GLD"]
    assert st["all"]["counts"]["ACCUMULATING"] >= 2 and st["all"]["min_agreement"] == 2 and "HON" not in {r["ticker"] for r in st["all"]["bullish"]}
    # contenders: holder-count traction (SPY 0 -> 2 new to cohort; APLS 2 -> 0 left; GLD 2 -> 1 is below the floor)
    ct = b["contenders"]["cohort"]
    assert [(r["ticker"], r["holder_delta"], r["new_to_cohort"]) for r in ct["all"]["rising"]] == [("SPY", 2, True)]
    assert [(r["ticker"], r["holder_delta"], r["left_cohort"]) for r in ct["all"]["fading"]] == [("APLS", -2, True)] and ct["all"]["n_new_to_cohort"] == 1
    assert [r["ticker"] for r in ct["etfs"]["rising"]] == ["SPY"] and ct["stocks"]["rising"] == [] and [r["ticker"] for r in ct["stocks"]["fading"]] == ["APLS"]
    # rotation: equities / gold / crypto / bonds (TLT + the T note at principal) / unmapped, with the pairs counted per manager x identity
    rot = {r["bucket"]: r for r in b["rotation"]["cohort"]["buckets"]}
    assert set(rot) == {"equities", "bonds", "gold", "crypto", "unmapped"}   # XYZQ is classed through the bank (FMP assetClass Equity)
    assert rot["equities"]["value_usd"] == 20000 + 9000 + 500 + 5000 + 16000 + 3750 + 2000 + 900 + 60 + 70 + 50 and rot["equities"]["n_managers"] == 2   # + the typed-but-unticked AMBIGUOUS CO
    assert rot["equities"]["pairs"] == {"new": 7, "absent": 3, "more": 2, "fewer": 1, "flat": 1} and rot["equities"]["net_pairs"] == 5 and rot["equities"]["tilt_pct"] == 38.5 and rot["equities"]["stance"] == "ACCUMULATING"
    assert rot["gold"]["value_usd"] == 2000.0 and rot["gold"]["value_prior_usd"] == 3960.0 and rot["gold"]["pairs"] == {"new": 0, "absent": 1, "more": 0, "fewer": 1, "flat": 0} and rot["gold"]["stance"] == "DISTRIBUTING"
    assert rot["bonds"]["value_usd"] == 950000 + 450 and rot["bonds"]["value_prior_usd"] == 940000 + 880 and rot["bonds"]["top"][0]["name"] == "T 4.3 02/15/30"
    assert rot["crypto"]["pairs"]["new"] == 1 and rot["crypto"]["top"][0]["name"] == "IBIT" and rot["unmapped"]["n_identities"] == 1 and rot["gold"]["tilt_pct"] == -100.0
    assert abs(sum(r["share_pct"] for r in rot.values()) - 100.0) < 0.1 and b["rotation"]["cohort"]["unmapped_value_pct"] == rot["unmapped"]["share_pct"]
    # per-manager mix and the review lists
    mix = {r["bucket"]: r for r in m["BERKSHIRE"]["mix"]["buckets"]}
    assert set(mix) == {"equities", "bonds", "gold"} and mix["gold"]["value_usd"] == 0.0 and mix["gold"]["value_prior_usd"] == 360.0 and mix["bonds"]["pairs"]["fewer"] == 1
    assert b["review"]["unclassified_etfs"] == [] and b["review"]["class_sources"] == {"table": 4, "fmp": 1} and b["review"]["learned"]["n_rows"] == 1 and b["review"]["learned"]["last_run"]["classed"] == 1
    learned = json.loads(s3.store["data/13f-etf-classes-learned.json"])
    assert learned["contract"] == "13f-etf-classes-learned.v1" and learned["by_ticker"]["XYZQ"]["class"] == "equity" and learned["by_ticker"]["XYZQ"]["source"] == "fmp-bank etf/info"
    assert {r["ticker"]: r["sub"] for r in cb["etfs"]["widely_held"]}["XYZQ"] == "equity" and {r["ticker"]: r.get("class_source") for r in cb["etfs"]["widely_held"]}.get("XYZQ") is None   # rows carry sub, the source lives in the review block
    assert {r["cusip"] for r in b["review"]["unmapped_top"]} == {"999999999"} and b["review"]["etf_table_size"] > 600
    amb = next(r for r in cb["stocks"]["newly_present"] if r["cusip"] == "AMBIG0001")
    assert amb["ticker"] is None and amb["kind"] == "stock" and amb["sub"] == "common" and amb["name"] == "AMBIGUOUS CO"   # typed by every venue's row, no US ticker
    # the period snapshot was written once and is reported in history
    assert b["history"]["snapshot"] == {"key": "data/13f-book/periods/2026-06-30.json", "written": True, "bytes": b["history"]["snapshot"]["bytes"], "reason": "new period"}
    snap = json.loads(s3.store["data/13f-book/periods/2026-06-30.json"])
    assert snap["contract"] == "13f-book-period.v1" and snap["cohort"] == ["BERKSHIRE", "ELLIOTT"] and snap["n_identities"] == 15
    assert snap["by_identity"]["a"] == ["AAPL", "stock", "equities", 2, 36000.0, 2, "037833100"] and snap["by_identity"]["x"][3] == 0 and snap["by_identity"]["x"][5] == 2 and snap["by_identity"]["xyz"][2] == "equities"
    assert b["history"]["periods"][0]["period"] == "2026-06-30"
    # the rest of the book is unchanged
    assert b["cohort_equity_value_usd"] == 37180.0 + 23100.0
    assert m["ELLIOTT"]["clone"]["skill_score"] == 77 and out["clone_alpha"]["ranked"][0]["fund_key"] == "ELLIOTT"
    assert out["season"]["filed_for_next"] == ["ELLIOTT"] and out["season"]["deadline"] == "2026-11-16"
    assert out["ai_brief"]["portfolio_action"] == "WAIT" and out["price_divergence"]["error"] == "KeyError"
    eng = {e["engine"]: e for e in out["engines"]}
    assert eng["justhodl-13f-positions"]["tier"] == "fresh" and eng["justhodl-13f-price-divergence"]["tier"] == "missing" and "data/13f-security-master.json" in eng["justhodl-13f-book"]["outputs"]
    assert b["options_overlay"][0]["fund_key"] == "ELLIOTT" and b["options_overlay"][0]["put_share"] == 100.0
    json.dumps(out, allow_nan=False)  # serialisable, finite


def test_second_run_reuses_the_master_and_rewrites_nothing_unchanged():
    s3, out = built()
    master = json.loads(s3.store["data/13f-security-master.json"])
    snap = s3.store["data/13f-book/periods/2026-06-30.json"]
    s3b = seed(master)
    s3b.store["data/13f-book/periods/2026-06-30.json"] = snap
    CALLS.clear()
    out2 = lf.build(s3b, NOW + timedelta(hours=1), log=lambda *a: None)
    sm = out2["book"]["security_master"]
    assert sm["refresh"]["due"] == 0 and sm["refresh"]["requests"] == 0 and CALLS == [] and sm["refresh"]["changed"] == 0
    assert "data/13f-security-master.json" not in {p["Key"] for p in s3b.puts}
    s3c = seed(master)
    s3c.store["data/13f-etf-classes-learned.json"] = s3.store["data/13f-etf-classes-learned.json"]
    BANK_CALLS.clear()
    out2c = lf.build(s3c, NOW + timedelta(hours=1), log=lambda *a: None)
    assert BANK_CALLS == [] and out2c["book"]["review"]["learned"]["last_run"]["looked_up"] == 0 and "data/13f-etf-classes-learned.json" not in {p["Key"] for p in s3c.puts}
    assert out2["book"]["history"]["snapshot"] == {"key": "data/13f-book/periods/2026-06-30.json", "written": False, "reason": "unchanged"}
    assert out2["book"]["stance"]["cohort"]["all"]["bullish"][0]["ticker"] == "GOOGL"
    # no time left: the refresh is skipped honestly and the book still builds from the stored master
    out3 = lf.build(seed(master), NOW, log=lambda *a: None, remaining_s=60.0)
    assert out3["book"]["security_master"]["refresh"]["skipped"] is True and "no time left" in out3["book"]["security_master"]["refresh"]["reason"]
    assert out3["book"]["boards"]["cohort"]["stocks"]["widely_held"][0]["ticker"] in ("AAPL", "GOOGL")
    # master disabled entirely: everything is unmapped, nothing breaks
    out4 = lf.build(seed(), NOW, log=lambda *a: None, master_enabled=False)
    assert out4["book"]["security_master"]["coverage"]["pending"] == 16 and out4["book"]["boards"]["cohort"]["stocks"]["n_identities"] == 0 and out4["book"]["review"]["learned"]["last_run"]["skipped"] is True
    assert out4["book"]["rotation"]["cohort"]["buckets"][0]["bucket"] == "bonds" and out4["book"]["rotation"]["cohort"]["unmapped_value_pct"] > 0


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
    assert lf.tier(202.0, 168) == "fresh" and lf.tier(300.0, 168) == "stale" and lf.tier(202.0, 24) == "dead" and lf.tier(20.0, 168) == "fresh" and lf.tier(5.0, None) == "unscheduled" and lf.tier(None, 24) == "missing"
    assert lf.tier(1300.0, 168) == "dead" and lf.tier(60.0, 72) == "fresh" and lf.tier(100.0, 72) == "stale"


def test_handler():
    s3 = seed()
    lf._s3 = lambda: s3
    res = lf.lambda_handler({}, None)
    assert res["statusCode"] == 200 and res["master_resolved"] == 14 and res["master_identities"] == 16
    keys = [p["Key"] for p in s3.puts]
    assert keys == ["data/13f-security-master.json", "data/13f-etf-classes-learned.json", "data/13f-book/periods/2026-06-30.json", "data/13f-book.json"]
    book_put = s3.puts[-1]
    assert book_put["CacheControl"] == "max-age=300" and BEGUN and BEGUN[-1] == {} and json.loads(book_put["Body"])["trigger"] == {"kind": "scheduled", "source_generated_at": None}
    lf.lambda_handler({"trigger": "holdings_research_published", "source_generated_at": "2026-11-03T15:41:16+00:00"}, SimpleNamespace(get_remaining_time_in_millis=lambda: 299000))
    assert json.loads(s3.puts[-1]["Body"])["trigger"] == {"kind": "holdings_research_published", "source_generated_at": "2026-11-03T15:41:16+00:00"}
    assert [p["Key"] for p in s3.puts[4:]] == ["data/13f-book.json"]   # second run: master, learned classes and snapshot unchanged, only the book rewritten


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
    print("justhodl-13f-book tests: all green")
