"""justhodl-corporate-actions learns the Polygon plan's history floor once per invocation (ops 6806, 2026-11-13).

The 06:30 UTC sweeps of Nov 12 and Nov 13 each spent their whole 250 s budget on pre-2021 dividends: one 403 per
dividend (184 identical 403s for 24 tickers on Nov 13, 179 due). The plan's history is one trailing window for the
account, so after the first 403 every session at or older than that date is answered without a call, for every ticker
of the run; dividends are resolved newest-first so one ticker's first 403 floors the rest of its own list.

Pinned here, offline (no network, no AWS): the pure floor rules, the newest-first order, and the resolver's calls
against a fake HTTP layer - 1 call for a ticker with five pre-floor dividends, 0 calls for the next ticker.
"""
import importlib.util
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "aws/lambdas/justhodl-corporate-actions/source/lambda_function.py"


def _load():
    sys.path.insert(0, str(ROOT / "aws" / "shared"))
    if "boto3" not in sys.modules:   # the runner may lack boto3; the engine imports it at module level and this test never uses it
        try:
            import boto3  # noqa: F401
        except ImportError:
            fake_boto3 = types.ModuleType("boto3")
            fake_boto3.client = lambda *a, **k: None
            fake_bc, fake_exc = types.ModuleType("botocore"), types.ModuleType("botocore.exceptions")
            fake_exc.ClientError = type("ClientError", (Exception,), {})
            fake_bc.exceptions = fake_exc
            sys.modules.update({"boto3": fake_boto3, "botocore": fake_bc, "botocore.exceptions": fake_exc})
    spec = importlib.util.spec_from_file_location("corpact_floor_test", SOURCE)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["corpact_floor_test"] = mod
    spec.loader.exec_module(mod)
    mod.PACE_S = 0.0
    return mod


def test_floor_rules_are_pure_and_guarded():
    m = _load()
    assert m.older_than_floor("2020-12-30", "2021-06-01") is True
    assert m.older_than_floor("2021-06-01", "2021-06-01") is True
    assert m.older_than_floor("2021-06-02", "2021-06-01") is False
    assert m.older_than_floor("2020-01-01", None) is False
    assert m.older_than_floor("", "2021-06-01") is False
    # a 403 on a recent session never becomes the floor (entitlement / bad symbol, not history)
    assert m.floor_candidate("2026-11-01", None, "2026-11-13") is False
    assert m.floor_candidate("2021-06-01", None, "2026-11-13") is True
    assert m.floor_candidate("2021-06-01", "2021-09-01", "2026-11-13") is False   # not newer than the current floor
    assert m.floor_candidate("2021-10-01", "2021-09-01", "2026-11-13") is True
    assert m.floor_candidate("not-a-date", None, "2026-11-13") is False
    ev = [{"type": "dividend_cash", "date": "2019-01-01"}, {"type": "split", "date": "2024-01-01"},
          {"type": "dividend_cash", "date": "2024-05-01"}, {"type": "dividend_cash", "date": "2021-03-01"}]
    assert [e["date"] for e in m._dividends_newest_first(ev)] == ["2024-05-01", "2021-03-01", "2019-01-01"]


def test_resolver_makes_one_call_per_run_for_pre_floor_dividends():
    m = _load()
    calls = []

    def fake_http(url, timeout=25):
        calls.append(url)
        day = url.split("/range/1/day/")[1].split("/")[0]
        if day < "2021-11-13":            # the plan's history floor: five years back from 'today'
            m._LAST_HTTP_CODE = 403
            return None
        m._LAST_HTTP_CODE = None
        return {"results": [{"c": 100.0}]}

    m._http_get_json = fake_http
    m._PLAN_FLOOR_403, m._FLOORED_N = None, 0
    divs_a = [{"type": "dividend_cash", "date": d, "cash_amount": 0.5} for d in
              ("2015-03-02", "2016-03-02", "2017-03-02", "2018-03-02", "2019-03-02", "2024-03-02")]
    out = m.resolve_dividend_factors(divs_a, {}, "AAA", "key", now_iso="2026-11-13T06:30:00+00:00")
    by_date = {e["date"]: e for e in out}
    assert by_date["2024-03-02"].get("factor") == (100.0 - 0.5) / 100.0
    assert all(by_date[d].get("pending_reason") == m.PLAN_HISTORY_403 for d in
               ("2015-03-02", "2016-03-02", "2017-03-02", "2018-03-02", "2019-03-02"))
    # newest first: the 2024 close (1 call), then the first pre-floor dividend 403s (1 call) and floors the other four
    assert len(calls) == 2, calls
    assert m._PLAN_FLOOR_403 == "2019-03-01"
    assert m._FLOORED_N == 4
    # the next ticker in the same invocation: its old dividends cost nothing
    divs_b = [{"type": "dividend_cash", "date": d, "cash_amount": 1.0} for d in ("2012-06-01", "2018-06-01")]
    out_b = m.resolve_dividend_factors(divs_b, {}, "BBB", "key", now_iso="2026-11-13T06:30:00+00:00")
    assert len(calls) == 2, calls
    assert all(e.get("pending_reason") == m.PLAN_HISTORY_403 and e.get("checked_at") for e in out_b)
    assert m._FLOORED_N == 6
    # a dividend newer than the floor is still asked for
    out_c = m.resolve_dividend_factors([{"type": "dividend_cash", "date": "2023-06-01", "cash_amount": 1.0}], {}, "CCC", "key",
                                       now_iso="2026-11-13T06:30:00+00:00")
    assert len(calls) == 3 and out_c[0].get("factor") == 0.99
    # a recent 403 (entitlement) does not move the floor
    m._LAST_HTTP_CODE = None

    def fake_recent_403(url, timeout=25):
        calls.append(url)
        m._LAST_HTTP_CODE = 403
        return None
    m._http_get_json = fake_recent_403
    m.resolve_dividend_factors([{"type": "dividend_cash", "date": "2026-11-02", "cash_amount": 1.0}], {}, "DDD", "key",
                               now_iso="2026-11-13T06:30:00+00:00")
    assert m._PLAN_FLOOR_403 == "2019-03-01"


def test_soft_budget_follows_the_function_timeout():
    m = _load()

    class Ctx:
        def __init__(self, ms):
            self.ms = ms

        def get_remaining_time_in_millis(self):
            return self.ms
    assert m.soft_budget_s(Ctx(900000)) == 840.0            # 900 s timeout: the full budget
    assert m.soft_budget_s(Ctx(300000)) == 240.0            # an older 300 s timeout: 60 s headroom, never the 840 s constant
    assert m.soft_budget_s(Ctx(50000)) == 30.0              # floor
    assert m.soft_budget_s(None) == 840.0                   # no context (offline)
    import json
    cfg = json.loads((ROOT / "aws/lambdas/justhodl-corporate-actions/config.json").read_text(encoding="utf-8"))
    assert cfg["timeout"] == 900 and cfg["timeout"] - m.SOFT_BUDGET_S >= 60
