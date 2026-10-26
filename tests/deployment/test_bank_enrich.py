"""bank_enrich (the factor-book SDK) never raises, never fabricates, stays additive, and names only fields the factor
book writes (deploy gate; the engine's own offline tests live in aws/lambdas/justhodl-bank-factors/tests)."""
import importlib.util
import io
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "aws", "shared"))

import bank_enrich as be  # noqa: E402


class DeadS3:
    def get_object(self, Bucket, Key):
        raise RuntimeError("no bucket here")

    def put_object(self, *a, **k):
        raise AssertionError("the SDK must never write")


def _engine_module():
    spec = importlib.util.spec_from_file_location("bank_factors", os.path.join(ROOT, "aws", "lambdas", "justhodl-bank-factors", "source", "lambda_function.py"))
    m = importlib.util.module_from_spec(spec)
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    spec.loader.exec_module(m)
    return m


def test_bank_enrich_never_raises_and_attach_is_additive_without_the_book():
    be._tls.c = DeadS3()
    be._DOCS.clear()
    be._MATRIX.update(at=0, doc=None)
    be._MANIFEST.update(at=0, doc=None)
    assert be.manifest() == {} and be.as_of() == {} and not be.available()
    assert be.factor("AAPL") is None and be.factors(["AAPL", "MSFT"]) == {} and be.matrix() is None
    assert be.rows() == [] and be.screen(lambda r: True) == [] and be.lookup("AAPL") is None
    out = {"picks": [{"ticker": "AAPL"}], "x": 1}
    same = be.attach(out)
    assert same is out and out["x"] == 1 and out["picks"] == [{"ticker": "AAPL"}]
    b = out["bank_enrichment"]
    assert b["source"] == "justhodl-bank-factors" and b["rows"] == {} and b["missing"] == ["AAPL"] and b["n"] == 0
    assert be.attach([1, 2]) == [1, 2] and be.attach(None) is None
    assert be.attach({"nothing": True})["bank_enrichment"]["note"] == "no symbols to enrich"


def test_default_fields_exist_in_the_factor_book_matrix_and_the_sdk_installs_nothing():
    m = _engine_module()
    matrix_names = set(m.IDENTITY) | {f for f, _, _ in m.FIELDS} | {"pct_" + f for f, _ in m.RANKED} | {"spct_" + f for f, _ in m.RANKED} | {"composite_" + c for c in m.COMPOSITES}
    missing = [f for f in be.DEFAULT_FIELDS if f not in matrix_names]
    assert not missing, missing
    assert all(f in matrix_names for parts in m.COMPOSITES.values() for f in parts), m.COMPOSITES
    assert all(f in {x for x, _, _ in m.FIELDS} for f, _ in m.RANKED), [f for f, _ in m.RANKED if f not in {x for x, _, _ in m.FIELDS}]
    src = open(os.path.join(ROOT, "aws", "shared", "bank_enrich.py"), encoding="utf-8").read()
    assert "urlopen" not in src and "put_object" not in src and "install(" not in src           # a reader, never a shim or a writer
    assert "financialmodelingprep" not in src and "polygon.io" not in src                     # the SDK never calls a provider


if __name__ == "__main__":
    test_bank_enrich_never_raises_and_attach_is_additive_without_the_book()
    test_default_fields_exist_in_the_factor_book_matrix_and_the_sdk_installs_nothing()
    print("ok bank_enrich")
