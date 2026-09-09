"""Synthetic downstream privacy checks; never connects to real services."""
import ast
from contextlib import redirect_stdout
from copy import deepcopy
import gzip
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
from public_brain_projection import PUBLIC_CONTEXT_PRIVACY_VERSION

MARKER = "SYNTHETIC_PRIVATE_DOWNSTREAM_5230"
TOKEN = "synthetic-history-service-5230"


class Store:
    def __init__(self):
        self.docs, self.reads, self.writes = {}, [], []
        self.item = {"sk": {"S": "fixture-ts"}, "encoding": {"S": "utf8"},
                     "content": {"S": json.dumps({"private": MARKER})}}

    def get_object(self, **kw):
        key = kw["Key"]
        self.reads.append(key)
        doc = self.docs.get(key, {})
        raw = doc if isinstance(doc, bytes) else json.dumps(doc).encode()
        return {"Body": io.BytesIO(raw)}

    def put_object(self, **kw):
        self.writes.append({**kw, "document": json.loads(kw["Body"])})
        self.docs[kw["Key"]] = json.loads(kw["Body"])

    def list_objects_v2(self, **kw):
        return {"Contents": [{"Key": k} for k in self.docs if k.startswith(kw["Prefix"])]}

    def get_item(self, **kw):
        self.reads.append("DDB:" + kw["Key"]["pk"]["S"])
        return {"Item": deepcopy(self.item)}

    def query(self, **kw):
        self.reads.append("DDB-query")
        return {"Items": [deepcopy(self.item)]}


def load(engine, store):
    errors = types.ModuleType("botocore.exceptions")
    errors.ClientError = type("ClientError", (Exception,), {})
    with patch.dict(sys.modules, {
        "boto3": types.SimpleNamespace(client=lambda *a, **kw: store),
        "botocore": types.ModuleType("botocore"), "botocore.exceptions": errors,
        "anthropic_shim": types.ModuleType("anthropic_shim"),
    }):
        return runpy.run_path(str(ROOT / "aws/lambdas" / ("justhodl-" + engine) / "source/lambda_function.py"))


def run_chat():
    store = Store()
    path = ROOT / "aws/lambdas/justhodl-ai-chat/source/lambda_function.py"
    fn = next(n for n in ast.parse(path.read_text()).body if isinstance(n, ast.FunctionDef) and n.name == "get_s3")
    env = {"json": json, "boto3": types.SimpleNamespace(client=lambda *a, **kw: store), "S3_BUCKET": "fixture"}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(path), "exec"), env)
    for key in ("risk/recommendations.json", "data/pm-decision.json", "portfolio/snapshot.json", "data/ai-brief.json"):
        assert env["get_s3"](key) is None
    assert not store.reads
    store.docs["data/report.json"] = {"risk_score": 42}
    assert env["get_s3"]("data/report.json") == {"risk_score": 42}
    print("ai-chat: 5 private-source boundary checks passed")


def run_commentary():
    for valid_history in (False, True):
        store = Store()
        store.docs["data/pm-decision.json"] = {"secret": MARKER}
        store.docs["data/ai-commentary/portfolio.json"] = {"commentary": {"headline": MARKER}, "generated_at": "old-current"}
        store.docs["data/ai-commentary/history/portfolio/old.json"] = {"commentary": {"headline": MARKER}, "generated_at": "old-history"}
        if valid_history:
            store.docs["data/ai-commentary/history/portfolio/new.json"] = {"commentary": {"headline": "Simulated book review"},
                "privacy_version": PUBLIC_CONTEXT_PRIVACY_VERSION, "generated_at": "safe-history"}
        scope = load("page-ai-commentary", store)
        handler = scope["lambda_handler"]
        env = handler.__globals__
        # Reintroduce the old config as a canary: the reader itself must deny it.
        env["PAGE_CONFIGS"]["portfolio"]["data_files"].append("data/pm-decision.json")
        captured = []
        def fail_llm(page, context):
            captured.append(context)
            return {"error": "synthetic_no_response"}
        env["generate_commentary"] = fail_llm
        with redirect_stdout(io.StringIO()), patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
            response = handler({"page": "portfolio"}, None)
        assert response["statusCode"] == 200
        assert "data/pm-decision.json" not in store.reads
        assert MARKER not in json.dumps(captured)
        assert all(MARKER not in json.dumps(w["document"]) for w in store.writes)
        current = store.docs["data/ai-commentary/portfolio.json"]
        assert current["privacy_version"] == PUBLIC_CONTEXT_PRIVACY_VERSION
        assert current["preserved_from"] == ("safe-history" if valid_history else None)
        assert all(w["CacheControl"] == "private, no-store" for w in store.writes if "/history/portfolio/" in w["Key"])
    # A successful regeneration uses only public inputs and can replace the marker.
    store = Store()
    scope = load("page-ai-commentary", store)
    scope["lambda_handler"].__globals__["generate_commentary"] = lambda page, context: {"headline": "Public simulated research"}
    with redirect_stdout(io.StringIO()):
        scope["lambda_handler"]({"page": "portfolio"}, None)
    assert store.docs["data/ai-commentary/portfolio.json"]["commentary"]["headline"] == "Public simulated research"
    print("page-ai-commentary: 3 source/fallback/regeneration scenarios passed")


def run_history():
    with patch.dict(os.environ, {"JH_SERVICE_TOKEN": TOKEN}), patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
        for route in ("/snapshot", "/latest", "/timestamps"):
            for key in ("data/ai-brief.json", "feed#data/ai-brief.json", "portfolio/snapshot.json"):
                store = Store()
                handler = load("history-api", store)["lambda_handler"]
                event = {"rawPath": route, "headers": {}, "queryStringParameters": {"key": key, "ts": "fixture-ts"}}
                denied = handler(event, None)
                assert denied["statusCode"] == 401
                assert not store.reads
                event["headers"] = {"x-jh-service-token": TOKEN}
                private = handler(event, None)
                assert private["statusCode"] == 200
                assert "no-store" in private["headers"]["Cache-Control"]
        store = Store()
        store.item["content"] = {"S": json.dumps({"risk_score": 42})}
        handler = load("history-api", store)["lambda_handler"]
        event = {"rawPath": "/snapshot", "headers": {}, "queryStringParameters": {"key": "data/report.json", "ts": "fixture-ts"}}
        public = handler(event, None)
        assert json.loads(public["body"])["content"] == {"risk_score": 42}
        assert "public" in public["headers"]["Cache-Control"]
        # A public feed must never launder a pointer to a private archive object.
        archive = "history/archive/feed/data/ai-brief.json/fixture.gz"
        store.item = {"sk": {"S": "fixture-ts"}, "encoding": {"S": "s3-archive-gzip"}, "content_archive_key": {"S": archive}}
        store.docs[archive] = gzip.compress(json.dumps({"private": MARKER}).encode())
        response = handler(event, None)
        assert archive not in store.reads and MARKER not in response["body"]
        event["queryStringParameters"]["key"] = "data/ai-brief.json"
        event["headers"] = {"x-jh-service-token": TOKEN}
        response = handler(event, None)
        assert archive in store.reads and MARKER in response["body"]
        assert "no-store" in response["headers"]["Cache-Control"]
    print("history-api: 12 private/public/archive route scenarios passed")


def run(engine):
    {"ai-chat": run_chat, "page-ai-commentary": run_commentary, "history-api": run_history}[engine]()


if __name__ == "__main__":
    for engine in ("ai-chat", "page-ai-commentary", "history-api"):
        run(engine)
