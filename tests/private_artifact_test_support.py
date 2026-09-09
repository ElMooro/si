"""Network-free, full-handler tests for private artifact publishing/retrieval."""
import importlib.util
import io
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class Response:
    status = 200
    def __init__(self, body): self.body = json.dumps(body).encode()
    def read(self): return self.body
    def __enter__(self): return self
    def __exit__(self, *args): pass


class S3:
    def __init__(self, docs): self.docs = docs; self.writes = []; self.reads = []
    def get_object(self, **kw):
        self.reads.append(kw["Key"])
        if kw["Key"] not in self.docs: raise KeyError(kw["Key"])
        return {"Body": io.BytesIO(json.dumps(self.docs[kw["Key"]]).encode())}
    def put_object(self, **kw):
        self.writes.append(kw)
        self.docs[kw["Key"]] = json.loads(kw["Body"]) if kw.get("ContentType", "application/json") == "application/json" else kw["Body"].decode()
    def get_parameter(self, **kw): return {"Parameter": {"Value": "fixture-service-token"}}


def load(engine, docs):
    s3 = S3(docs)
    b = types.ModuleType("boto3"); b.client = lambda *a, **k: s3; sys.modules["boto3"] = b
    sys.modules["anthropic_shim"] = types.ModuleType("anthropic_shim")
    managed = types.ModuleType("managed_secret"); managed.managed_secret = lambda *a, **k: "fixture-service-token"
    sys.modules["managed_secret"] = managed
    for name, path in [("private_artifact", ROOT / "aws/shared/private_artifact.py"),
                       ("under_test", ROOT / "aws/lambdas" / engine / "source/lambda_function.py")]:
        spec = importlib.util.spec_from_file_location(name, path); mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod; spec.loader.exec_module(mod)
    return mod, s3


def run(engine):
    import urllib.request
    original_urlopen = urllib.request.urlopen
    try:
        if engine in ("justhodl-brain-sync", "justhodl-journal-grader"):
            now = datetime.now(timezone.utc).isoformat()
            doc = {"notes": [{"id": "fixture", "text": "SYNTHETIC PRIVATE NOTE", "cat": "rule", "created": 1}]}
            if engine.endswith("journal-grader"):
                doc = {"entries": [{"id": "fixture", "ticker": "SPY", "direction": "watch", "thesis": "SYNTHETIC PRIVATE THESIS", "created": datetime.now(timezone.utc).timestamp()*1000, "horizon_days": 30, "entry_price": 100},
                                   {"id": "correction", "correction_of": "fixture", "thesis": "later note"}]}
            mod, s3 = load(engine, {"data/brain.json": {"directive": {"hard_rules": ["fixture"]}, "last_distill_at": now, "regime_read": {}}, "data/brain-history.json": {"history": []}})
            published = []; reads = []
            def urlopen(request, **kwargs):
                url = request.full_url
                if url.startswith(getattr(mod, "BRAIN_URL", "https://no.invalid")) or url.startswith(getattr(mod, "JOURNAL_URL", "https://no.invalid")):
                    assert request.get_header("X-jh-service-token") == "fixture-service-token"
                    reads.append(url); return Response(doc)
                if "/private-artifact?kind=" in url:
                    assert request.get_method() == "PUT"
                    assert request.get_header("X-jh-service-token") == "fixture-service-token"
                    published.append((url.split("kind=")[1], json.loads(request.data))); return Response({"ok": True})
                raise AssertionError("network forbidden")
            urllib.request.urlopen = urlopen
            assert mod.lambda_handler({})["statusCode"] == 200
            assert reads and published
            assert all(w.get("CacheControl") == "private, no-store" for w in s3.writes)
            if engine.endswith("brain-sync"):
                assert {p[0] for p in published} == {"brain", "brain-history"}
                assert s3.docs["data/brain.json"]["notes"][0]["text"] == "SYNTHETIC PRIVATE NOTE"
            else:
                assert published[0][0] == "journal-graded"
                assert [g["id"] for g in s3.docs["data/journal-graded.json"]["graded"]] == ["fixture"]
            writes = len(s3.writes)
            def denied(*a, **k): raise OSError("synthetic source unavailable")
            urllib.request.urlopen = denied
            assert mod.lambda_handler({})["statusCode"] == 502
            assert len(s3.writes) == writes, "source failure must not publish empty success"
            print(engine + ": 3 checks passed (private publication, IAM source preservation, source failure)")
        elif engine == "justhodl-ask":
            mod, _ = load(engine, {})
            calls = []
            mod.build_context = lambda: calls.append("private context") or {"_brain": {"prompt_block": "SYNTHETIC PRIVATE NOTE"}}
            mod.call_claude = lambda system, prompt: json.dumps({"answer": "fixture private answer", "results": []})
            event = {"requestContext": {"http": {"method": "POST"}}, "body": json.dumps({"q": "fixture?"})}
            for headers in ({}, {"x-jh-service-token": "invalid"}):
                assert mod.lambda_handler({**event, "headers": headers})["statusCode"] == 401
            assert not calls, "unauthenticated query reached private context"
            r = mod.lambda_handler({**event, "headers": {"X-JH-Service-Token": "fixture-service-token"}})
            assert r["statusCode"] == 200 and calls and r["headers"]["Cache-Control"] == "private, no-store"
            print(engine + ": 3 checks passed (anonymous denial, invalid token denial, private authorized answer)")
        elif engine == "justhodl-ask-desk":
            private = sys.modules.get("private_artifact")
            mod, s3 = load(engine, {"data/engine-manifest.json": {"engines": [
                {"engine": "brain", "keys": ["data/brain.json"]}, {"engine": "public", "keys": ["data/signal-board.json"]}]},
                "data/brain.json": {"notes": ["SYNTHETIC PRIVATE NOTE"]}, "data/signal-board.json": {"signal": "PUBLIC FIXTURE"}})
            cat = mod.catalog(); assert not any(line.startswith("data/brain.json") for line in cat)
            before = len(s3.reads); assert "private source" in mod.fetch_slim("data/brain.json"); assert len(s3.reads) == before
            prompts = []
            def claude(system, prompt, models, budget):
                prompts.append(prompt)
                if system == mod.ROUTER_SYS: return json.dumps({"keys": ["data/brain.json", "data/_askdesk/old.json", "data/not-in-catalog.json"]}), "fixture"
                assert "SYNTHETIC PRIVATE NOTE" not in prompt
                return "Public fixture answer", "fixture"
            mod.claude = claude; mod.DESK_KEY = "fixture-key"
            result = mod.lambda_handler({"requestContext": {"http": {"method": "POST"}}, "headers": {"x-desk-key": "fixture-key"}, "body": json.dumps({"question": "fixture?"})})
            assert result["statusCode"] == 200
            assert json.loads(result["body"])["sources_used"] == ["data/signal-board.json"]
            assert "data/brain.json" not in s3.reads
            assert s3.writes[0]["Key"].startswith("data/_askdesk/") and s3.writes[0]["CacheControl"] == "private, no-store"
            print(engine + ": 4 checks passed (catalog filter, fetch guard, router allowlist, private archive)")
        elif engine == "justhodl-ai-brief":
            mod, s3 = load(engine, {"risk/recommendations.json": {
                "drawdown_status": {"current_drawdown_pct": -4.25},
                "sized_recommendations": [{"symbol": "PRIVATE-FIXTURE", "recommended_size_pct": 3.5}]}})
            mod.SKIP_TELEGRAM = True
            mod.get_anthropic_key = lambda: None
            mod.send_telegram = lambda *a, **k: (_ for _ in ()).throw(AssertionError("message forbidden"))
            published = []
            def urlopen(request, **kwargs):
                assert "/private-artifact?kind=ai-brief" in request.full_url
                assert request.get_header("X-jh-service-token") == "fixture-service-token"
                assert request.get_method() == "PUT"
                published.append(json.loads(request.data)); return Response({"ok": True})
            urllib.request.urlopen = urlopen
            for headers in ({}, {"X-JH-Service-Token": "invalid"}, []):
                assert mod.lambda_handler({"headers": headers, "requestContext": {"http": {"method": "POST"}}})["statusCode"] == 401
            assert not s3.reads and not s3.writes and not published
            assert mod.lambda_handler({"validate_only": True})["statusCode"] == 200
            assert not s3.writes and not published
            result = mod.lambda_handler({})
            assert result["statusCode"] == 200 and result["headers"]["Cache-Control"] == "private, no-store"
            assert len(published) == 1 and published[0] == s3.docs["data/ai-brief.json"]
            assert published[0]["snapshot"]["risk_sizer"]["top_5_sized"][0]["symbol"] == "PRIVATE-FIXTURE"
            assert all(w["CacheControl"] == "private, no-store" for w in s3.writes if w["Key"] in ("data/ai-brief.json", "data/ai-brief.md"))
            assert "PRIVATE-FIXTURE" not in json.dumps(s3.docs["data/decisive-call-history.json"])
            writes = len(s3.writes)
            def fail(*a, **k): raise OSError("fixture mirror unavailable")
            urllib.request.urlopen = fail
            try: mod.lambda_handler({})
            except OSError: pass
            else: raise AssertionError("mirror failure must fail handler")
            assert len(s3.writes) == writes
            print(engine + ": 6 checks passed (HTTP auth, dry run, full private mirror, IAM original/cache, public ledger projection, publication failure)")
        else:
            raise ValueError(engine)
    finally:
        urllib.request.urlopen = original_urlopen
