#!/usr/bin/env python3
"""Tests of the macro grid's history completion (2026-11-27). No network, no AWS: the functions are read out of the
engine's source, never imported (importing it reads a key).

    python3 aws/lambdas/justhodl-canary-grid/tests/run_tests.py
"""
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SRC = (ROOT / "aws/lambdas/justhodl-canary-grid/source/lambda_function.py").read_text(encoding="utf-8")
TREE = ast.parse(SRC)


def fn(name, scope=None):
    node = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == name)
    scope = scope if scope is not None else {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "grid", "exec"), scope)
    return scope[name]


def signals():
    node = next(n for n in TREE.body if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "SIGNALS")
    return [{k.arg: ast.literal_eval(k.value) for k in el.keywords} for el in node.value.elts]


def test_a_short_answer_is_completed_from_the_warehouse_and_freds_rows_are_kept():
    merge = fn("merge_older")
    fred = [("2026-11-24", 3.72), ("2026-11-21", None), ("2026-11-20", 3.76), ("2026-11-19", 3.5)]
    house = [("2026-11-20", 9.99), ("2026-11-18", 3.4), ("2026-11-17", 3.3), ("2026-11-14", 3.2), ("2026-11-13", None), ("2026-11-24", 9.99)]
    got = merge(fred, house, 6)
    assert got[:4] == fred                                              # FRED's rows untouched, its own missing value included
    assert got[4:] == [("2026-11-18", 3.4), ("2026-11-17", 3.3)]        # only older days, newest first, up to what was asked
    assert merge(fred, house, 100)[4:] == [("2026-11-18", 3.4), ("2026-11-17", 3.3), ("2026-11-14", 3.2)]
    assert merge(fred, house, 4) is fred and merge(fred, [], 9) == fred and merge([], house, 9) == []
    dates = [d for d, _ in merge(fred, house, 100)]
    assert dates == sorted(dates, reverse=True) and len(dates) == len(set(dates))


def test_the_warehouse_is_asked_only_when_fred_answers_short():
    assert "if out and len(out) < limit * 0.9:" in SRC and "merge_older(out, warehouse_rows(series_id), limit)" in SRC
    body = ast.get_source_segment(SRC, next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == "warehouse_rows"))
    assert "api_key" not in body and "FRED_KEY" not in body and '"fred%3A" + series_id' in body       # the warehouse needs no key
    assert "_WAREHOUSE_SEEN[series_id] = out" in body                                                  # asked once a run
    seen = {}

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b'{"obs": [["1996-12-31", 3.13], ["1997-01-02", "."], {"date": "1997-01-03", "value": "3.09"}]}'

    class Url:
        class request:
            @staticmethod
            def Request(url, headers=None):
                seen["url"] = url
                return url

            @staticmethod
            def urlopen(req, timeout=0):
                return Resp()
    import json
    rows = fn("warehouse_rows", {"_WAREHOUSE_SEEN": {}, "WAREHOUSE": "https://w/series?id=", "urllib": Url, "json": json})("BAMLH0A0HYM2")
    assert rows == [("1996-12-31", 3.13), ("1997-01-03", 3.09)] and seen["url"] == "https://w/series?id=fred%3ABAMLH0A0HYM2"


def test_the_high_yield_spread_reads_against_five_years_like_its_siblings():
    by = {s["key"]: s for s in signals()}
    assert by["hy_credit_oas"]["limit"] == 1300 and by["hy_credit_oas"]["kind"] == "level" and by["hy_credit_oas"]["dir"] == "rise"
    for k in ("euro_hy_oas", "em_corp_oas", "em_hy_oas", "us_hy_ytw", "em_hy_ytw", "bbb_aaa"):
        assert by[k]["limit"] == 1300, k
    assert len(by) == 64


def main():
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for name, f in tests:
        try:
            f()
            print("ok   %s" % name)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            import traceback
            print("FAIL %s: %s" % (name, str(exc)[:300]))
            traceback.print_exc(limit=2)
    print("%d tests, %d failed" % (len(tests), failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
