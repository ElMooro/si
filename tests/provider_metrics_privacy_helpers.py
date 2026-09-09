"""Actual provider-reader/handler regressions. Fake SDK/network, no cloud calls."""
import contextlib
import importlib.util
import io
import json
import sys
import types
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
CANARY = "SYNTHETIC_PROVIDER_PRIVATE_DIAGNOSTIC"


class Storage:
    def __init__(self):
        self.writes = {}

    def put_object(self, **kw):
        self.writes[kw["Key"]] = json.loads(kw["Body"])

    def __getattr__(self, method):
        raise AssertionError("Unexpected cloud method " + method)


def load(name):
    storage = Storage()
    boto = types.ModuleType("boto3")
    boto.client = lambda *a, **k: storage
    source = ROOT / "aws/lambdas" / name / "source/lambda_function.py"
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), source)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"boto3": boto}):
        spec.loader.exec_module(module)
    module.POLYGON_KEY = "synthetic-test-token"
    universe = "ETF_UNIVERSE" if name == "justhodl-etf-fund-flows" else "ALL_UNIVERSE"
    setattr(module, universe, {k: getattr(module, universe)[k] for k in ("SPY", "QQQ")})
    return module, storage


def run(name):
    is_etf = name == "justhodl-etf-fund-flows"
    read_name = "fetch_etf_flow_window" if is_etf else "fetch_daily_bars"
    universe_name = "fetch_universe_parallel" if is_etf else "fetch_universe"
    from public_brain_projection import PUBLIC_PROVIDER_METRICS, public_provider_diagnostics, sanitize_public

    # Actual provider reader must not read or expose an HTTP error body/URL.
    module, storage = load(name)
    class ErrorBody:
        def read(self, *a):
            raise AssertionError("HTTP error body must never be read")
        def close(self): pass
    error = urllib.error.HTTPError("https://provider.invalid/?apiKey=" + CANARY, 403, CANARY, {}, ErrorBody())
    with patch.object(module.urllib.request, "urlopen", side_effect=error):
        result = getattr(module, read_name)("SPY")
    assert result == {"ticker": "SPY", "error": "PROVIDER_HTTP_ERROR", "error_code": "PROVIDER_HTTP_ERROR", "http_status": 403}

    # Actual reader handles arbitrary exception text and provider status bodies.
    with patch.object(module.urllib.request, "urlopen", side_effect=RuntimeError(CANARY)):
        assert CANARY not in json.dumps(getattr(module, read_name)("SPY"))
    class Response:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def read(self): return json.dumps({"results": [], "status": CANARY, "request_id": CANARY, "queryCount": CANARY}).encode()
    with patch.object(module.urllib.request, "urlopen", return_value=Response()):
        result = getattr(module, read_name)("SPY")
    assert result["error_code"] == "PROVIDER_NO_RESULTS" and CANARY not in json.dumps(result)

    # The actual concurrent collector normalizes failures escaping an adapter.
    setattr(module, read_name, lambda *a, **k: (_ for _ in ()).throw(RuntimeError(CANARY)))
    results = getattr(module, universe_name)()
    assert len(results) == 2 and CANARY not in json.dumps(results)
    assert all(row["error_code"] == "PROVIDER_REQUEST_FAILED" for row in results.values())

    # Actual handler and analytics retain the successful zero-flow / real marks;
    # synthetic legacy failed rows cannot leak through current, archive or HTTP.
    module, storage = load(name)
    failed = {"ticker": "QQQ", "error": CANARY, "body": CANARY, "request_id": CANARY,
              "headers": {"Authorization": CANARY}, "nav": 0, "latest_close": None}
    if is_etf:
        good = {"ticker": "SPY", "processed_date": "2026-09-09", "nav": 100, "shares_outstanding": 1000,
                "aum_usd": 100000, "daily_flow_usd": 0, "fund_flow_5d_usd": 0, "fund_flow_21d_usd": 0, "history": []}
        before = module.compute_per_etf_metrics(good, [])
        # Actual research-signal storage is a separate side effect, isolated here.
        module.emit_divergence_signals = lambda *a, **k: 0
    else:
        good = {"ticker": "SPY", "latest_date": "2026-09-09T00:00:00+00:00", "bars": [
                {"date": "2026-09-09", "close": 100, "volume": 0}, {"date": "2026-09-08", "close": 99, "volume": 0}]}
        before = module.compute_asset_metrics(good)
    setattr(module, universe_name, lambda: {"SPY": good, "QQQ": failed})
    logs = io.StringIO()
    with contextlib.redirect_stdout(logs):
        response = module.lambda_handler({}, None)
    assert response["statusCode"] == 200
    assert CANARY not in json.dumps(storage.writes) + response["body"] + logs.getvalue()
    current = "etf-flows/daily.json" if is_etf else "macro/regime.json"
    historical = "etf-flows/history/" if is_etf else "macro/history/"
    field = "metrics" if is_etf else "asset_metrics"
    keys = [current] + [key for key in storage.writes if key.startswith(historical)]
    assert len(keys) == 2
    for key in keys:
        doc = storage.writes[key]
        assert doc["publication"] == PUBLIC_PROVIDER_METRICS
        assert next(row for row in doc[field] if row["ticker"] == "SPY") == before
        row = next(row for row in doc[field] if row["ticker"] == "QQQ")
        assert row["error_code"] == "PROVIDER_REQUEST_FAILED" and row["nav"] == 0 and row["latest_close"] is None
        assert sanitize_public(key, doc) == doc

    # Legacy stored metrics are cleaned regardless of forged/missing marker.
    legacy = {"publication": PUBLIC_PROVIDER_METRICS, field: [before, failed], "generated_at": "2026-09-09T00:00:00Z",
              "nested": {"response_body": CANARY, "zero": 0, "missing": None}}
    clean = sanitize_public(current, legacy)
    assert clean[field][0] == before and CANARY not in json.dumps(clean)
    assert clean["nested"] == {"zero": 0, "missing": None}
    assert sanitize_public(current, clean) == clean
    assert sanitize_public(historical + "2026-09-09.json", legacy) == clean
    assert public_provider_diagnostics({"error": {"private": CANARY}, "body": CANARY})["error_code"] == "PROVIDER_REQUEST_FAILED"

    if not is_etf:
        # Empty provider bars cannot bypass the failure guard and crash by_role.
        no_close = module.compute_asset_metrics({"ticker": "SPY", "bars": [{"close": None}], "body": CANARY})
        assert no_close["error_code"] == "PROVIDER_NO_PRICES" and module.by_role([no_close]) == {}
    print(name + ": provider reader, concurrent adapter, current/archive/HTTP, legacy projection and metric-preservation checks passed")
