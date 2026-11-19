"""The search directory lists its policy pins whether or not the query words matched them (Khalid 2026-11-19: "im trying
to pull sp500 and i cant", then "Fix all of them").

SEARCH_POLICY says what a benchmark word means: "sp500" -> the S&P 500 index (I:SPX), SPY, then FRED's copy.  Until this
change a pin only re-ranked a document the tokens had already matched; "Standard & Poor's 500" and "SPDR S&P 500 ETF
Trust" do not contain the letters "sp500", so typing sp500 led with the FRED series and never listed the index.

Pins here: the real search() over a small directory built with the engine's own doc() / doc_tokens().
"""
import importlib.util
import sys
import types
from array import array
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "aws/lambdas/justhodl-symdir/source"


def _load():
    boto3 = types.ModuleType("boto3")
    boto3.client = lambda *a, **k: types.SimpleNamespace()
    config_mod = types.ModuleType("botocore.config")
    config_mod.Config = lambda **kwargs: kwargs
    botocore = types.ModuleType("botocore")
    botocore.config = config_mod
    saved = {k: sys.modules.get(k) for k in ("boto3", "botocore", "botocore.config")}
    path = list(sys.path)
    sys.modules.update({"boto3": boto3, "botocore": botocore, "botocore.config": config_mod})
    sys.path[:0] = [str(SOURCE), str(ROOT / "aws/shared")]
    try:
        spec = importlib.util.spec_from_file_location("symdir_policy_pins_under_test", SOURCE / "lambda_function.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path[:] = path
        for k, old in saved.items():
            if old is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = old


def _directory(symdir, rows):
    docs = [symdir.doc(i, prov, title, kind, pop) for i, prov, title, kind, pop in rows]
    post = defaultdict(lambda: array("I"))
    for n, d in enumerate(docs):
        for token in symdir.doc_tokens(d):
            post[token].append(n)
    symdir._IDX.update({
        "docs": docs, "pop": array("f", [d[symdir.D_POP] for d in docs]), "index": dict(post), "toklist": sorted(post),
        "ids": sorted((d[symdir.D_ID].upper(), n) for n, d in enumerate(docs)),
        "bare": sorted((d[symdir.D_ID].rsplit(":", 1)[-1].upper(), n) for n, d in enumerate(docs)),
        "built_at": "test",
    })
    symdir.warehouse_search = lambda *a, **k: {"rows": [], "more": False, "facets": []}


ROWS = [
    ("I:SPX", "polygon", "Standard & Poor's 500", "instrument", 0.9),
    ("SPY", "polygon", "SPDR S&P 500 ETF Trust", "instrument", 1.0),
    ("fred:SP500", "fred", "S&P 500", "series", 0.8),
    ("MULTPL:SP500_PE_RATIO_YEAR", "tv", "SP500_PE_RATIO_YEAR (MULTPL)", "instrument", 0.2),
    ("SPCFD:SP500LVOL", "tv", "SP500LVOL (SPCFD)", "instrument", 0.2),
    ("I:NDX", "polygon", "Nasdaq-100", "instrument", 0.9),
    ("QQQ", "polygon", "Invesco QQQ Trust", "instrument", 1.0),
    ("AAPL", "polygon", "Apple Inc.", "instrument", 1.0),
]


def test_sp500_lists_the_index_and_spy_before_the_fred_copy():
    symdir = _load()
    _directory(symdir, ROWS)
    out = symdir.search("sp500", 10)
    ids = [r["id"] for r in out["rows"]]
    assert ids[:3] == ["I:SPX", "SPY", "fred:SP500"], ids
    assert [bool(r.get("pinned")) for r in out["rows"][:3]] == [True, True, True]
    assert out["rows"][0]["rank_reason"] == "policy:sp500"
    # the rows the letters did match are still there, after the pins
    assert "MULTPL:SP500_PE_RATIO_YEAR" in ids and "SPCFD:SP500LVOL" in ids
    assert not any(r.get("pinned") for r in out["rows"][3:])


def test_the_policy_itself_is_what_gets_listed():
    symdir = _load()
    _directory(symdir, ROWS)
    assert symdir.SEARCH_POLICY["sp500"] == ["I:SPX", "SPY", "fred:SP500"]
    assert symdir.SEARCH_POLICY["spx"] == symdir.SEARCH_POLICY["sp500"] == symdir.SEARCH_POLICY["s&p 500"]
    for word in ("sp500", "spx", "s&p 500", "SP500", " Sp500 "):
        ids = [r["id"] for r in symdir.search(word, 10)["rows"]]
        assert ids[:3] == ["I:SPX", "SPY", "fred:SP500"], (word, ids)
    # "ndx": QQQ's name does not contain the letters either
    ids = [r["id"] for r in symdir.search("ndx", 10)["rows"]]
    assert ids[:2] == ["I:NDX", "QQQ"], ids


def test_filters_still_decide_and_a_missing_pin_adds_nothing():
    symdir = _load()
    _directory(symdir, ROWS)
    only_fred = symdir.search("sp500", 10, prov="fred")["rows"]
    assert [r["id"] for r in only_fred] == ["fred:SP500"]
    only_series = symdir.search("sp500", 10, kind="series")["rows"]
    assert [r["id"] for r in only_series] == ["fred:SP500"]
    # a directory without the index: the pin is simply absent, nothing is invented
    _directory(symdir, [r for r in ROWS if r[0] != "I:SPX"])
    ids = [r["id"] for r in symdir.search("sp500", 10)["rows"]]
    assert ids[:2] == ["SPY", "fred:SP500"] and "I:SPX" not in ids, ids


def test_a_word_without_a_policy_is_untouched():
    symdir = _load()
    _directory(symdir, ROWS)
    assert "aapl" not in symdir.SEARCH_POLICY
    out = symdir.search("AAPL", 10)
    assert [r["id"] for r in out["rows"]] == ["AAPL"]
    assert not out["rows"][0].get("pinned")
    out = symdir.search("apple", 10)
    assert [r["id"] for r in out["rows"]] == ["AAPL"]
