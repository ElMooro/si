"""Synthetic owner watchlist/volatility tests; no remote operations."""
from contextlib import redirect_stdout
from copy import deepcopy
import io
import hashlib
import json
import os
from pathlib import Path
import runpy
import sys
import types
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "aws/shared"), str(ROOT / "tests")]
from private_artifact import private_http_denied
from public_brain_projection import PUBLIC_VOL_UNIVERSE
from downstream_privacy_test_support import Store

TOKEN = "synthetic-watchlist-service-5230"
PRIVATE_SYMBOL = "ZZZOWN"


class WatchlistStore(Store):
    def etag(self, key):
        return '"' + hashlib.sha256(json.dumps(self.docs[key], sort_keys=True).encode()).hexdigest() + '"'

    def error(self, code):
        exc = self.ClientError("synthetic storage conflict")
        exc.response = {"Error": {"Code": code}}
        return exc

    def get_object(self, **kw):
        if kw["Key"] not in self.docs:
            raise self.error("NoSuchKey")
        return {**super().get_object(**kw), "ETag": self.etag(kw["Key"])}

    def put_object(self, **kw):
        key = kw["Key"]
        if getattr(self, "concurrent_write", False):
            self.concurrent_write = False
            self.docs[key]["version"] += 1
            self.docs[key]["categories"]["watching"] = ["MSFT"]
        if ((kw.get("IfMatch") and kw["IfMatch"] != self.etag(key)) or
                (kw.get("IfNoneMatch") == "*" and key in self.docs)):
            raise self.error("PreconditionFailed")
        return super().put_object(**kw)


def load(engine, store):
    mirrors = []
    errors = types.ModuleType("botocore.exceptions")
    errors.ClientError = type("ClientError", (Exception,), {})
    store.ClientError = errors.ClientError
    store.get_parameter = lambda **kw: {"Parameter": {"Value": TOKEN}}
    with patch.dict(sys.modules, {
        "boto3": types.SimpleNamespace(client=lambda *a, **kw: store),
        "botocore": types.ModuleType("botocore"), "botocore.exceptions": errors,
        "private_artifact": types.SimpleNamespace(private_http_denied=private_http_denied,
            publish_private=lambda kind, doc: mirrors.append((kind, deepcopy(doc)))),
    }):
        scope = runpy.run_path(str(ROOT / "aws/lambdas" / ("justhodl-" + engine) / "source/lambda_function.py"))
    return scope["lambda_handler"], mirrors, scope["lambda_handler"].__globals__


def store_with_private_watchlist():
    store = WatchlistStore()
    store.docs["data/user-watchlist.json"] = {"version": 1, "categories": {"holdings": [PRIVATE_SYMBOL], "watching": []},
        "tags": {"planned_sale": [PRIVATE_SYMBOL]}, "history": [{"op": "add", "ticker": PRIVATE_SYMBOL}], "settings": {}}
    return store


def run_watchlist():
    with patch.dict(os.environ, {"JH_SERVICE_TOKEN": TOKEN}), patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
        for method in ("GET", "POST"):
            store = store_with_private_watchlist()
            handler, mirrors, _ = load("watchlist", store)
            response = handler({"headers": {}, "httpMethod": method, "body": "{}"}, None)
            assert response["statusCode"] == 401
            assert not store.reads and not store.writes and not mirrors
        for header in ("x-justhodl-token", "X-JH-Service-Token"):
            store = store_with_private_watchlist()
            handler, mirrors, _ = load("watchlist", store)
            event = {"headers": {header: TOKEN}, "httpMethod": "GET"}
            response = handler(event, None)
            assert response["statusCode"] == 200 and PRIVATE_SYMBOL in response["body"]
            assert "no-store" in response["headers"]["Cache-Control"]
            event.update({"httpMethod": "POST", "rawPath": "/add", "body": json.dumps({"ticker": "NVDA", "category": "watching", "version": 1})})
            response = handler(event, None)
            assert response["statusCode"] == 200
            assert store.docs["data/user-watchlist.json"]["categories"]["watching"] == ["NVDA"]
            assert ("user-watchlist", store.docs["data/user-watchlist.json"]) in mirrors
            assert store.writes[-1]["CacheControl"] == "private, no-store"
            assert store.writes[-1].get("IfMatch") and "source_etag" not in response["body"]
        for version in (None, 0, True):
            store = store_with_private_watchlist()
            handler, mirrors, _ = load("watchlist", store)
            response = handler({"headers": {"X-JH-Service-Token": TOKEN}, "httpMethod": "POST", "rawPath": "/add",
                "body": json.dumps({"ticker": "NVDA", "category": "watching", "version": version})}, None)
            assert response["statusCode"] == 409 and not store.writes and not mirrors
        store = store_with_private_watchlist()
        handler, mirrors, _ = load("watchlist", store)
        store.concurrent_write = True
        response = handler({"headers": {"X-JH-Service-Token": TOKEN}, "httpMethod": "POST", "rawPath": "/add",
            "body": json.dumps({"ticker": "NVDA", "category": "watching", "version": 1})}, None)
        assert response["statusCode"] == 409 and not mirrors and not store.writes
        assert store.docs["data/user-watchlist.json"]["categories"]["watching"] == ["MSFT"]
        store = WatchlistStore()
        handler, mirrors, _ = load("watchlist", store)
        response = handler({"headers": {"X-JH-Service-Token": TOKEN}, "httpMethod": "POST", "rawPath": "/add",
            "body": json.dumps({"ticker": "NVDA", "category": "watching", "version": 0})}, None)
        assert response["statusCode"] == 200 and mirrors
        assert store.writes[0]["IfNoneMatch"] == "*"
        store = store_with_private_watchlist()
        handler, _, _ = load("watchlist", store)
        response = handler({"httpMethod": "OPTIONS", "headers": {}}, None)
        assert response["statusCode"] == 200 and not store.reads
    print("watchlist: 10 private read/write/revision/race scenarios passed")


def run_vol():
    with patch.dict(os.environ, {"JH_SERVICE_TOKEN": TOKEN}), patch("urllib.request.urlopen", side_effect=AssertionError("no market network")):
        store = store_with_private_watchlist()
        handler, mirrors, env = load("vol-regime", store)
        denied = handler({"headers": {}}, None)
        assert denied["statusCode"] == 401 and not store.reads
        env["assemble_ticker"] = lambda symbol: {"ticker": symbol, "regime": "PANIC" if symbol == PRIVATE_SYMBOL else "NORMAL",
            "iv_atm_30d": 22, "rv_z": 1, "iv_rv_ratio": 1.2}
        with redirect_stdout(io.StringIO()):
            result = handler({}, None)
        assert result["statusCode"] == 200
        private = store.docs["data/vol-regime-private.json"]
        public = store.docs["data/vol-regime.json"]
        assert PRIVATE_SYMBOL in json.dumps(private) and PRIVATE_SYMBOL not in json.dumps(public)
        assert ("vol-regime-private", private) in mirrors
        assert {r["ticker"] for r in public["tickers"]} == set(PUBLIC_VOL_UNIVERSE)
        assert public["n_tickers"] == 8 and public["regime_counts"] == {"NORMAL": 8}
        assert public["composite_score"] == private["composite_score"]
        assert public["tickers"][0]["iv_atm_30d"] == 22
        assert all(w["CacheControl"] == "private, no-store" for w in store.writes if "-private" in w["Key"])
    print("vol-regime: 2 private guard/full-preservation/public-core scenarios passed")


def run(engine):
    {"watchlist": run_watchlist, "vol-regime": run_vol}[engine]()


if __name__ == "__main__":
    for engine in ("watchlist", "vol-regime"):
        run(engine)
