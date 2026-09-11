"""justhodl-stock-buying unit tests (dependency-free; the deploy workflow runs this before AWS is touched).

Covers the v1.5.2 FMP /stable/ migration: URL shape, symbol-as-query, /stable/earnings field names
(epsActual/epsEstimated) with the v3 spellings still readable from a warm cache body, future-quarter
skipping, and the invariant that no v3 base URL survives in source.
"""
from __future__ import annotations

import importlib.util
import io
import json
import sys
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source" / "lambda_function.py"
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parent / "source"))


class _NoAWS:
    def __getattr__(self, name):
        def _boom(*a, **k):
            raise AssertionError("AWS call attempted in unit test: %s" % name)
        return _boom


def _load():
    fake = types.ModuleType("boto3")
    fake.client = lambda *a, **k: _NoAWS()
    sys.modules["boto3"] = fake
    spec = importlib.util.spec_from_file_location("stock_buying_under_test", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_source_has_no_v3_base_url():
    s = SRC.read_text()
    assert "financialmodelingprep.com/api/v3" not in s, "v3 base URL survived the /stable/ migration"
    assert "financialmodelingprep.com/stable/" in s
    assert 'os.environ.get("FMP_API_KEY", "")' not in s, "key must resolve through managed_secret"


def test_fmp_builds_stable_url_with_symbol_query(mod):
    seen = {}

    class _Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, timeout=0):
        seen["url"] = req.full_url
        seen["ua"] = req.headers.get("User-agent")
        return _Resp(json.dumps([{"epsActual": 1.0}]).encode())

    mod.FMP_KEY = "k"
    mod.FMP_BUDGET["n"] = 5
    mod.urllib.request.urlopen = fake_urlopen
    mod.time.sleep = lambda *_: None
    out = mod.fmp("income-statement", "symbol=ACME&period=quarter&limit=6")
    assert out == [{"epsActual": 1.0}], out
    assert seen["url"].startswith("https://financialmodelingprep.com/stable/income-statement?symbol=ACME&"), seen
    assert "apikey=k" in seen["url"] and "/api/v3/" not in seen["url"]
    assert seen["ua"] == "justhodl-fleet"
    assert mod.FMP_BUDGET["n"] == 4


def test_fmp_returns_none_without_key_or_budget(mod):
    mod.FMP_KEY = ""
    assert mod.fmp("earnings", "symbol=ACME") is None
    mod.FMP_KEY = "k"
    mod.FMP_BUDGET["n"] = 0
    assert mod.fmp("earnings", "symbol=ACME") is None


def test_eps_beats_reads_stable_fields_and_skips_future_quarters(mod):
    rows = [
        {"date": "2026-10-20", "epsActual": None, "epsEstimated": 2.1},      # future quarter -> skipped
        {"date": "2026-07-20", "epsActual": 2.4, "epsEstimated": 2.0},       # beat +20%
        {"date": "2026-04-20", "epsActual": 1.5, "epsEstimated": 1.6},       # miss
        {"date": "2026-01-20", "epsActual": 1.1, "epsEstimated": 1.0},       # beat +10%
        {"date": "2025-10-20", "epsActual": 0.5, "epsEstimated": 0.5},       # inline
        {"date": "2025-07-20", "epsActual": 9.0, "epsEstimated": 1.0},       # 5th reported -> outside n=4
    ]
    n_beat, mag = mod.eps_beats(rows)
    assert n_beat == 2, n_beat
    assert [round(m, 2) for m in mag] == [20.0, -6.25, 10.0, 0.0], mag


def test_eps_beats_still_parses_v3_spelling_from_a_warm_cache_body(mod):
    rows = [{"actualEarningResult": 1.2, "estimatedEarning": 1.0},
            {"actualEarningResult": 0.9, "estimatedEarning": 1.0}]
    n_beat, mag = mod.eps_beats(rows)
    assert n_beat == 1 and [round(m, 2) for m in mag] == [20.0, -10.0], mag


def test_eps_beats_none_when_nothing_reported(mod):
    assert mod.eps_beats(None) is None
    assert mod.eps_beats([]) is None
    assert mod.eps_beats([{"epsActual": None, "epsEstimated": 1.0}]) is None
    assert mod.eps_beats([{"epsActual": 1.0, "epsEstimated": 0.0}]) == (1, [])


def main():
    mod = _load()
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for name, fn in tests:
        try:
            fn(mod) if fn.__code__.co_argcount else fn()
            print("ok   ", name)
        except Exception as e:  # noqa: BLE001
            failed += 1
            print("FAIL ", name, "->", repr(e))
    print("%d passed, %d failed" % (len(tests) - failed, failed))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
