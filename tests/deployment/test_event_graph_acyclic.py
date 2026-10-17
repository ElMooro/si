"""Deploy gate: no Lambda may be wired to trigger itself through the event bus (2026-10-12 incident class).

Runs on every Lambda deploy (tests/deployment/run_tests.py discovers test_*.py). scripts/event_graph.py maps
who publishes which event, which coordinator route invokes whom, direct lambda.invoke calls and self-chains.
  * a cycle through the event bus -> every deploy fails until the route or the publisher is fixed
  * a cycle of direct invokes only -> must be listed with its stop condition in config/loop-allowlist.json
The fixture tests prove the analyzer still catches the exact loop that cost $2,300 (docs/ops/POSTMORTEM_2026-10_EVENT_LOOP.md).
"""
from __future__ import annotations

import importlib.util
import json
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("event_graph", REPO / "scripts" / "event_graph.py")
eg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(eg)
_VERDICT = {}


def _repo_verdict():
    if "v" not in _VERDICT:
        _VERDICT["v"] = eg.verdict(REPO)
    return _VERDICT["v"]


def _fixture(files: dict, allow=None, bus_rules=None) -> Path:
    root = Path(tempfile.mkdtemp(prefix="eg-fixture-"))
    for rel, body in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(body)
    if allow is not None:
        (root / "config").mkdir(exist_ok=True)
        (root / "config" / "loop-allowlist.json").write_text(json.dumps({"invoke_cycles": allow}))
    if bus_rules is not None:
        (root / "config").mkdir(exist_ok=True)
        (root / "config" / "eventbridge-bus-rules.json").write_text(json.dumps({"rules": bus_rules}))
    return root


COORD = "aws/lambdas/justhodl-event-coordinator/source/lambda_function.py"
SYSTEM_EVENTS = '''
EVT_NEAR_MISS_EXTREME = "near_miss.extreme"
def publish(event_name, detail, source_engine=None, bus_name=None):
    pass
def publish_many(events, bus_name=None, client=None):
    pass
'''


def _coordinator(routes: dict) -> str:
    return "ROUTES = %r\n" % {k: {"invoke": v, "notify": False, "audit": True} for k, v in routes.items()}


def test_no_event_loops_in_the_fleet():
    v = _repo_verdict()
    assert not v["event_loops"], "EVENT LOOP(S) - a Lambda triggers itself through the bus:\n  " + "\n  ".join(
        " ".join(c["path"]) for c in v["event_loops"])


def test_invoke_chains_are_allowlisted():
    v = _repo_verdict()
    assert not v["unallowed_invoke_cycles"], (
        "new Lambda invoke cycle(s); add the stop condition to config/loop-allowlist.json or break the chain:\n  "
        + "\n  ".join(" ".join(c["path"]) for c in v["unallowed_invoke_cycles"]))


def test_analyzer_still_sees_the_fleet():
    g = _repo_verdict()["graph"]
    assert len(g["routes"]) >= 20, "coordinator ROUTES not parsed (%d)" % len(g["routes"])
    assert sum(1 for n in g["nodes"].values() if n["published"]) >= 10, "publishers not detected"
    assert len(g["edges"]) >= 10, "graph lost its edges (%d)" % len(g["edges"])
    assert "justhodl-miss-calibrator" in g["nodes"], "fleet inventory incomplete"


def test_catches_the_2026_10_12_loop():
    calibrator = '''
from system_events import publish, publish_many
def handler(event, context):
    events_to_publish = []
    for sig_type, count in {"a": 60}.items():
        if count >= 50:
            events_to_publish.append(("near_miss.extreme", {"signal_type": sig_type, "count": count}))
    publish_many(events_to_publish)
'''
    root = _fixture({COORD: _coordinator({"near_miss.extreme": ["justhodl-miss-calibrator"]}),
                     "aws/lambdas/justhodl-miss-calibrator/source/lambda_function.py": calibrator,
                     "aws/shared/system_events.py": SYSTEM_EVENTS})
    v = eg.verdict(root)
    assert v["event_loops"] and v["event_loops"][0]["nodes"] == ["justhodl-miss-calibrator"], v["cycles"]


def test_catches_a_two_hop_loop_through_constants_and_wrappers():
    a = '''
import system_events
from system_events import EVT_NEAR_MISS_EXTREME
def handler(event, context):
    system_events.publish(EVT_NEAR_MISS_EXTREME, {})
'''
    b = '''
from system_events import publish
def emit(name, payload):
    publish(name, payload)
def handler(event, context):
    emit("regime.changed", {"x": 1})
'''
    root = _fixture({COORD: _coordinator({"near_miss.extreme": ["justhodl-b"], "regime.changed": ["justhodl-a"]}),
                     "aws/lambdas/justhodl-a/source/lambda_function.py": a,
                     "aws/lambdas/justhodl-b/source/lambda_function.py": b,
                     "aws/shared/system_events.py": SYSTEM_EVENTS})
    v = eg.verdict(root)
    assert v["event_loops"] and v["event_loops"][0]["nodes"] == ["justhodl-a", "justhodl-b"], v["cycles"]


def test_new_self_chain_needs_an_allowlist_entry():
    src = '''
import boto3
def handler(event, context):
    boto3.client("lambda").invoke(FunctionName=context.function_name, InvocationType="Event", Payload=b"{}")
'''
    files = {COORD: _coordinator({}), "aws/lambdas/justhodl-chain/source/lambda_function.py": src}
    v = eg.verdict(_fixture(files))
    assert v["unallowed_invoke_cycles"] and not v["event_loops"], v["cycles"]
    v2 = eg.verdict(_fixture(files, allow=[{"nodes": ["justhodl-chain"], "stop": "depth cap"}]))
    assert not v2["unallowed_invoke_cycles"], v2["cycles"]


def test_bus_rule_straight_to_a_lambda_counts_as_an_event_edge():
    src = '''
from system_events import publish
def handler(event, context):
    publish("signal.fired", {})
'''
    root = _fixture({COORD: _coordinator({}), "aws/lambdas/justhodl-x/source/lambda_function.py": src,
                     "aws/shared/system_events.py": SYSTEM_EVENTS},
                    bus_rules=[{"name": "r", "detail_types": ["signal.fired"], "targets": ["justhodl-x"]}])
    v = eg.verdict(root)
    assert v["event_loops"] and "rule" in v["event_loops"][0]["kinds"], v["cycles"]
