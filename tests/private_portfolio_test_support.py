"""Offline full-handler tests for private account artifact publication.

AWS, market HTTP, and Telegram are replaced with in-memory fakes. The real
shared HTTP identity guard runs against a synthetic environment service token.
"""
from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import runpy
import sys
import types
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws/shared"))
from private_artifact import private_http_denied

TOKEN = "synthetic-private-portfolio-service-5230"
SYMBOL = "SYNTHETIC_HELD_POSITION"
PRIMARY = {"portfolio-risk": ("portfolio/risk.json", "portfolio-risk"),
           "portfolio-sizer": ("portfolio/sizing.json", "portfolio-sizing"),
           "portfolio-catalysts": ("portfolio/catalysts.json", "portfolio-catalysts"),
           "pm-decision": ("data/pm-decision.json", "pm-decision"),
           "behavior-mirror": ("data/behavior-mirror.json", "behavior-mirror")}


class Store:
    def __init__(self, empty=False):
        self.position = {"symbol": SYMBOL, "qty": 7, "market_value": 700, "last_price": 100,
                         "sector": "Technology", "alpha_score": 60, "open_at": "2026-09-01T00:00:00Z"}
        self.docs = {
            "portfolio/snapshot.json": {"positions": [] if empty else [self.position],
                "portfolio_summary": {"total_market_value": 700, "total_pnl_pct": 2}},
            "portfolio/risk.json": {"position_metrics": {SYMBOL: {"beta_spy": 1.4, "annual_vol_pct": 22}},
                                   "portfolio_risk": {"portfolio_beta_spy": 1.4}},
            "data/master-ranker.json": {"regime_context": {"risk_posture": "DEFENSIVE"}},
            "data/earnings-pead.json": {"signals": [{"symbol": SYMBOL, "direction": "UP", "magnitude": 1.2}]},
            "data/pm-decision-history.json": {"snapshots": []},
            "data/history/behavior-mirror-history.json": {"snapshots": []},
        }
        self.reads, self.writes = [], []
        self.exceptions = types.SimpleNamespace(NoSuchKey=KeyError, ResourceNotFoundException=KeyError)

    def get_object(self, **kw):
        self.reads.append(kw["Key"])
        return {"Body": io.BytesIO(json.dumps(self.docs.get(kw["Key"], {})).encode())}

    def put_object(self, **kw):
        self.writes.append({**kw, "document": json.loads(kw["Body"])})
        self.docs[kw["Key"]] = json.loads(kw["Body"])
        return {}

    def get_parameter(self, **kw):
        raise RuntimeError("synthetic SSM unavailable")

    def Table(self, *args, **kw):
        return self

    def scan(self, **kw):
        self.reads.append("DDB")
        return {"Items": []}


def load(engine, store, fail_publish=False):
    mirrors = []
    def publish(kind, doc):
        if fail_publish:
            raise RuntimeError("synthetic private mirror unavailable")
        mirrors.append((kind, deepcopy(doc)))
    with patch.dict(sys.modules, {
        "boto3": types.SimpleNamespace(client=lambda *a, **kw: store, resource=lambda *a, **kw: store),
        "private_artifact": types.SimpleNamespace(publish_private=publish, private_http_denied=private_http_denied),
    }):
        scope = runpy.run_path(str(ROOT / "aws/lambdas" / ("justhodl-" + engine) / "source/lambda_function.py"))
    env = scope["lambda_handler"].__globals__
    env.update({"TELEGRAM_TOKEN": "", "TELEGRAM_BOT_TOKEN": "", "TELEGRAM_CHAT_ID": "",
                "get_chat_id": lambda: None,
                "send_telegram": lambda *a, **kw: (_ for _ in ()).throw(AssertionError("no Telegram permitted in tests")),
                "maybe_telegram": lambda *a, **kw: (_ for _ in ()).throw(AssertionError("no Telegram permitted in tests"))})
    if engine == "portfolio-risk":
        env["batch_fetch_bars"] = lambda symbols, days: {s: [{"c": 100 + i + (i % 3)} for i in range(90)] for s in symbols}
    if engine == "portfolio-catalysts":
        env["scan_ddb_all"] = lambda: ({SYMBOL: store.position}, {})
    if engine == "behavior-mirror":
        env["scan_table"] = lambda table: [store.position] if table == "justhodl-portfolio" else []
    return scope["lambda_handler"], mirrors, env


def run(engine):
    checks = 0
    with patch.dict(os.environ, {"JH_SERVICE_TOKEN": TOKEN}), patch("urllib.request.urlopen", side_effect=AssertionError("no external HTTP permitted")):
        # Direct function URL requests cannot reach any personal source or write.
        for event in ({"requestContext": {"http": {"method": "GET"}}, "headers": {}},
                      {"headers": {"X-JH-Service-Token": "wrong"}, "body": "{}"}):
            store = Store()
            handler, mirrors, _ = load(engine, store)
            result = handler(event, None)
            assert result["statusCode"] == 401
            assert "no-store" in result["headers"]["Cache-Control"]
            assert not store.reads and not store.writes and not mirrors
            checks += 1

        # Full computational handler executes privately for an authenticated caller.
        store = Store()
        handler, mirrors, env = load(engine, store)
        with redirect_stdout(io.StringIO()):
            result = handler({"requestContext": {"http": {"method": "POST"}}, "headers": {"x-jh-service-token": TOKEN}}, None)
        assert result["statusCode"] == 200, (engine, result)
        assert "no-store" in result["headers"]["Cache-Control"]
        key, kind = PRIMARY[engine]
        assert any(w["Key"] == key for w in store.writes)
        assert (kind, store.docs[key]) in mirrors
        assert all(w.get("CacheControl") == "private, no-store" for w in store.writes)
        if engine in {"portfolio-risk", "portfolio-sizer", "portfolio-catalysts", "pm-decision"}:
            assert SYMBOL in json.dumps(store.docs[key]), engine
        if engine == "pm-decision":
            assert ("pm-decision-history", store.docs["data/pm-decision-history.json"]) in mirrors
        if engine == "behavior-mirror":
            assert any(w["Key"] == "data/history/behavior-mirror-history.json" for w in store.writes)
            assert ("behavior-mirror-history", store.docs["data/history/behavior-mirror-history.json"]) in mirrors
        checks += 1

        # Failure to refresh the authenticated mirror is observable, never success.
        store = Store()
        handler, mirrors, env = load(engine, store, fail_publish=True)
        with redirect_stdout(io.StringIO()):
            try:
                result = handler({}, None)
            except RuntimeError as exc:
                assert str(exc) == "synthetic private mirror unavailable"
            else:
                assert result["statusCode"] == 500
                assert "no-store" in result["headers"]["Cache-Control"]
        assert any(w["Key"] == key for w in store.writes)
        assert all(w.get("CacheControl") == "private, no-store" for w in store.writes)
        checks += 1

        if engine == "portfolio-risk":
            store = Store(empty=True)
            handler, mirrors, _ = load(engine, store)
            with redirect_stdout(io.StringIO()):
                result = handler({}, None)
            assert result["statusCode"] == 200
            assert store.docs[key]["status"] == "no_positions"
            assert (kind, store.docs[key]) in mirrors
            checks += 1
        if "save_alert_history" in env:
            history_store = Store()
            _, history_mirrors, env = load(engine, history_store)
            history = {SYMBOL: "2026-09-09T00:00:00Z"}
            env["save_alert_history"](history)
            assert history_store.writes[-1]["CacheControl"] == "private, no-store"
            kind = {"portfolio-risk": "portfolio-risk-history", "portfolio-sizer": "portfolio-sizing-history", "portfolio-catalysts": "portfolio-catalyst-history"}[engine]
            assert (kind, history) in history_mirrors
            assert history_store.docs[env["ALERT_HISTORY_KEY"]] == history
            checks += 1

            # A source outage must not be interpreted as an empty history.
            env["s3"].get_object = lambda **kw: (_ for _ in ()).throw(RuntimeError("synthetic source unavailable"))
            try: env["load_alert_history"]()
            except RuntimeError as error: assert str(error) == "synthetic source unavailable"
            else: raise AssertionError("failed source read became empty history")
            assert len(history_store.writes) == 1
            checks += 1

            # Successful canonical storage survives mirror failure; failure is
            # observable so a missing owner output cannot be called successful.
            history_store = Store()
            _, _, env = load(engine, history_store, fail_publish=True)
            try: env["save_alert_history"](history)
            except RuntimeError as error: assert str(error) == "synthetic private mirror unavailable"
            else: raise AssertionError("private history publication failed silently")
            assert history_store.docs[env["ALERT_HISTORY_KEY"]] == history
            checks += 1
    print(f"{engine}: {checks} private handler checks passed")


if __name__ == "__main__":
    for engine in PRIMARY:
        run(engine)
