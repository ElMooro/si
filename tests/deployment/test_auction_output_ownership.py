"""Execute Treasury handlers with local provider fixtures and record every write."""
import ast
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime, timezone
import gzip
import io
import json
from pathlib import Path
import runpy
import sys
import types
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
LAMBDAS = ROOT / "aws/lambdas"
BASE = "data/auction-crisis.json"


class Store:
    def __init__(self, docs=None):
        self.docs = deepcopy(docs or {})
        self.writes = []
        self.reads = []

    def get_object(self, **kwargs):
        key = kwargs["Key"]
        self.reads.append(key)
        if key not in self.docs:
            raise KeyError(key)
        body = json.dumps(self.docs[key]).encode()
        if key.endswith(".gz"):
            body = gzip.compress(body)
        return {"Body": io.BytesIO(body), "LastModified": datetime.now(timezone.utc), "ETag": '"fixture"'}

    def put_object(self, **kwargs):
        self.writes.append(kwargs["Key"])
        body = kwargs["Body"]
        if kwargs.get("ContentEncoding") == "gzip":
            body = gzip.decompress(body)
        self.docs[kwargs["Key"]] = json.loads(body)
        return {"ETag": '"fixture-new"'}


def source(engine):
    return LAMBDAS / ("justhodl-" + engine) / "source/lambda_function.py"


def load(engine, store):
    stubs = {
        "boto3": types.SimpleNamespace(client=lambda *a, **kw: store),
        "managed_secret": types.SimpleNamespace(managed_secret=lambda *a, **kw: "fixture-only"),
        "_fred_shim": types.ModuleType("_fred_shim"),
        "anthropic_shim": types.ModuleType("anthropic_shim"),
        "_sentry_lite": types.SimpleNamespace(track_errors=lambda fn: fn),
    }
    old_path = sys.path[:]
    try:
        sys.path.insert(0, str(source(engine).parent))
        with patch.dict(sys.modules, stubs):
            return runpy.run_path(str(source(engine)))
    finally:
        sys.path[:] = old_path


def auction():
    today = datetime.now(timezone.utc).date().isoformat()
    return {"auction_date": today, "issue_date": today, "security_type": "Note", "security_term": "10-Year",
            "cusip": "TEST12345", "bid_to_cover_ratio": "2.4", "high_yield": "4.0", "low_yield": "3.9",
            "median_yield": "3.98", "allocation_pctage": "70", "primary_dealer_accepted": "2000000000",
            "direct_bidder_accepted": "1000000000", "indirect_bidder_accepted": "7000000000",
            "total_accepted": "10000000000"}


def detector_fixture():
    store = Store()
    scope = load("auction-crisis-detector", store)
    handler = scope["lambda_handler"]
    env = handler.__globals__
    env.update({"fetch_fiscal_auctions": lambda *a, **kw: [auction()], "get_fed_funds_rate": lambda: 4.25,
                "fetch_upcoming_auctions": lambda *a, **kw: [], "compute_cross_signals": lambda: {},
                "compute_preauction_concession": lambda rows: rows,
                "compute_postissue_performance": lambda rows, **kw: rows})
    with redirect_stdout(io.StringIO()), patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
        result = handler({}, None)
    assert result["statusCode"] == 200
    return store, scope


def test_desk_scoring_module_is_pure_and_preserves_detector_math():
    path = source("auction-desk").parent / "crisis_scoring.py"
    scoring = runpy.run_path(str(path))
    assert "lambda_handler" not in scoring
    assert not any(isinstance(n, (ast.Import, ast.ImportFrom)) for n in ast.parse(path.read_text()).body)
    assert not (path.parent / "auction_crisis_v2.py").exists()
    reference = ast.parse(source("auction-crisis-detector").read_text())
    names = {"parse_float", "classify_tenor_bucket", "parse_term_to_days", "compute_record_metrics", "score_indicators"}
    nodes = [n for n in reference.body if isinstance(n, ast.FunctionDef) and n.name in names
             or isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "NORMAL_2024_BASELINE" for t in n.targets)]
    env = {}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "detector-pure-scoring", "exec"), env)
    for kind, term, btc in (("Note", "10-Year", "2.4"), ("Bill", "4-Week", "4.8"),
                            ("Bill", "13-Week", "1.3"), ("TIPS", "5-Year", "1.7"), ("FRN", "2-Year", "2.1")):
        row = {**auction(), "security_type": kind, "security_term": term, "bid_to_cover_ratio": btc}
        actual = scoring["compute_record_metrics"](row)
        expected = env["compute_record_metrics"](row)
        assert actual == expected
        for fed_funds in (None, 0, 4.25):
            assert scoring["score_indicators"](actual, fed_funds) == env["score_indicators"](expected, fed_funds)


def test_detector_actual_handler_owns_live_and_detector_archive_schema():
    store, _ = detector_fixture()
    assert store.writes[0] == BASE
    assert len(store.writes) == 2 and store.writes[1].startswith("data/archive/auction-crisis/")
    doc = store.docs[BASE]
    assert doc["engine"] == "justhodl-auction-crisis-detector"
    assert doc["schema_version"] == "2.1" and len(doc["recent_auctions"]) == 1
    assert doc["recent_auctions"][0]["cusip"] == auction()["cusip"]
    assert {"tenor_decomposition", "composite_history", "forward_calendar", "tail_risk", "triggers"} <= doc.keys()
    assert store.docs[store.writes[1]] == doc


def test_desk_actual_handler_uses_pure_scoring_and_never_overwrites_detector():
    detector, _ = detector_fixture()
    base = deepcopy(detector.docs[BASE])
    store = Store({BASE: base})
    scope = load("auction-desk", store)
    handler = scope["lambda_handler"]
    env = handler.__globals__
    assert env["crisis_scoring"] is not None
    env.update({"fetch_fd": lambda endpoint, *a, **kw: [auction()] if endpoint == "auctions_query" else [],
                "fetch_td": lambda *a, **kw: [], "load_assets": lambda **kw: {"series": {}},
                "load_full_bank": lambda **kw: {"rows": {"fixture": auction()}, "n": 1},
                "fetch_fred_daily": lambda *a: {auction()["auction_date"]: 4.25}})
    with redirect_stdout(io.StringIO()), patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
        result = handler({"no_ai": True}, None)
    assert result["ok"] is True
    assert BASE in store.reads and store.docs[BASE] == base
    assert BASE not in store.writes
    assert not any(key.startswith("data/archive/auction-crisis/") for key in store.writes)
    assert all(key == "data/auction-desk.json" or key.startswith("data/warm/treasury-auctions/") for key in store.writes)
    desk = store.docs["data/auction-desk.json"]
    assert desk["engine"] == "justhodl-auction-desk" and desk["auctions"][0]["cusip"] == auction()["cusip"]
    assert desk["composite_history"]["series"]
    assert any(key.startswith("data/warm/treasury-auctions/daily/") for key in store.writes)


def test_ai_actual_handler_reads_detector_and_writes_only_narrative_keys():
    detector, _ = detector_fixture()
    base = deepcopy(detector.docs[BASE])
    store = Store({BASE: base})
    scope = load("auction-crisis-ai", store)
    handler = scope["lambda_handler"]
    env = handler.__globals__
    env["call_anthropic"] = lambda *a, **kw: json.dumps({"executive_summary": "Fixture narrative", "decisive_call": "Fixture call"})
    env["maybe_send_alerts"] = lambda *a, **kw: []
    with redirect_stdout(io.StringIO()), patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
        result = handler({}, None)
    assert result["statusCode"] == 200
    assert BASE in store.reads and store.docs[BASE] == base
    assert all(key == "data/auction-crisis-ai.json" or key.startswith("data/archive/auction-crisis-ai/") for key in store.writes)
    doc = store.docs["data/auction-crisis-ai.json"]
    assert doc["data_generated_at"] == base["generated_at"] and doc["composite"] == base["composite_score"]
    assert doc["ai_commentary"]["executive_summary"] == "Fixture narrative"


def test_tenor_actual_handler_owns_separate_signal_key():
    store = Store()
    handler = load("tenor-signal-interpreter", store)["lambda_handler"]
    env = handler.__globals__
    env.update({"fetch_fred_fed_funds": lambda: 4.25, "fetch_auctions_window": lambda *a: []})
    with redirect_stdout(io.StringIO()), patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
        result = handler({}, None)
    assert result["statusCode"] == 200
    assert store.writes == ["data/auction-tenor-signals.json"]
    assert set(store.docs[store.writes[0]]["signals"]) == {"fed_path", "eurodollar", "qe_imminence"}


def test_grader_actual_handler_consumes_detector_rows_without_changing_base():
    detector, _ = detector_fixture()
    base = deepcopy(detector.docs[BASE])
    store = Store({BASE: base})
    handler = load("auction-grader", store)["lambda_handler"]
    handler.__globals__["maybe_telegram"] = lambda *a, **kw: None
    with redirect_stdout(io.StringIO()), patch("urllib.request.urlopen", side_effect=AssertionError("no network")):
        result = handler({}, None)
    assert result["statusCode"] == 200 and store.docs[BASE] == base
    assert store.writes == ["data/auction-grades.json"]
    graded = store.docs[store.writes[0]]
    assert graded["n_graded"] == 1 and graded["graded_auctions"][0]["cusip"] == auction()["cusip"]
    assert graded["regime_from_crisis_detector"] == base["regime"]
    assert graded["composite_score_crisis"] == base["composite_score"]


def functions(engine, names, env=None):
    tree = ast.parse(source(engine).read_text())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    scope = dict(env or {})
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {"REGIMES", "ASSETS"}:
                    scope[target.id] = ast.literal_eval(node.value)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source(engine)), "exec"), scope)
    return scope


def test_chart_reader_uses_nested_detector_history_and_observation_time():
    detector, _ = detector_fixture()
    doc = detector.docs[BASE]
    store = Store({BASE: doc})
    reader = load("chart-data", store)["fetch_internal"]
    expected = [{"time": row["date"], "value": row["composite"]}
                for row in doc["composite_history"]["series"] if row["composite"] is not None]
    assert reader("auction_crisis") == expected
    store.docs[BASE] = {"composite_score": 0, "generated_at": "2026-08-31T13:00:00Z"}
    assert reader("auction_crisis") == [{"time": "2026-08-31", "value": 0}]
    store.docs[BASE] = {"composite_score": 0}
    assert reader("auction_crisis") is None  # no invented current observation date


def test_regime_router_and_brief_read_the_detector_score_including_zero():
    route = functions("regime-conditional-router", {"detect_treasury_auction_crisis"},
                      {"safe_get": lambda d, key: (d or {}).get(key)})["detect_treasury_auction_crisis"]
    compress = functions("ai-brief", {"compress_auction"})["compress_auction"]
    for score, regime in ((0, "CALM"), (80, "ACUTE_STRESS")):
        doc = {"composite_score": score, "regime": regime, "interpretation": "Fixture detector explanation"}
        value, evidence = route(doc, {})
        assert value == score and evidence["auction_state"] == regime
        assert evidence["auction_crisis_score"] == score
        assert compress(doc) == {"score": score, "regime": regime, "regime_desc": doc["interpretation"]}


def test_canary_zero_score_is_retained_in_actual_read_block():
    tree = ast.parse(source("crisis-canaries").read_text())
    handler = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "lambda_handler")
    block = next(n for n in handler.body if isinstance(n, ast.Try)
                 and any(isinstance(c, ast.Constant) and c.value == BASE for c in ast.walk(n)))
    store = Store({BASE: {"composite_score": 0}})
    env = {"S3": store, "BUCKET": "fixture", "json": json, "auc": None}
    exec(compile(ast.Module(body=[block], type_ignores=[]), "actual-auction-canary-read", "exec"), env)
    assert env["auc"] == 0 and store.writes == []


def test_interpreter_prompt_preserves_actual_tenor_metrics_calendar_and_aggregate():
    scope = functions("auction-interpreter", {"_fmt_indicator", "_recent_auctions_summary", "build_prompt"}, {"json": json})
    detector, _ = detector_fixture()
    doc = deepcopy(detector.docs[BASE])
    doc["indicator_aggregate_14d"] = {"pd_absorption": {"n_fired": 2, "max_score": 70}}
    doc["recent_auctions"][0].update({"indirect_pct": 0, "primary_dealer_pct": 0, "tail_bp": 0})
    doc["forward_calendar"] = [{"auction_date": "2026-09-10", "security_term": "10-Year", "offering_amount_billions": 42}]
    row = scope["_recent_auctions_summary"](doc)[0]
    assert row["tenor"] == "10-Year" and row["btc"] == 2.4
    assert row["indirect_pct"] == row["primary_dealer_pct"] == row["tail_bps"] == 0
    assert "not a when-issued quote" in row["tail_measurement"]
    prompt = scope["build_prompt"](doc, {}, [], {})
    assert '"n_fired_14d": 2' in prompt and '"max_score_14d": 70' in prompt
    assert '"amount_b": 42' in prompt and '"tenor": "10-Year"' in prompt
    assert "30-day calendar" in prompt
