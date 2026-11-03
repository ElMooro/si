"""Deploy gate for the fleet-wide trigger cap (Khalid, 2026-11-02: no engine may start other engines, or itself, past a
hard limit; past it the engines are shut down and only an operator turns them back on).

1. chain_guard.LIMITS is exactly config/trigger-limits.json (the module embeds the file; a change to one without the
   other fails the deploy), and the limits are sane.
2. Every engine that starts another engine with a Lambda invoke (every `.invoke(` call site in aws/lambdas/*/source,
   as scripts/event_graph.py also sees them) does so through chain_guard - the guarded client or chain_invoke - and
   builds no raw Lambda client.  The control plane is allowlisted by name with its reason.
3. Every relay (an engine that is started by another engine AND starts engines or publishes) begins its lineage at
   handler entry (chain_guard.begin / lambda_client(event, ...)), so a chain is never silently reset to hop 0.
4. system_events stamps the lineage and refuses at the depth cap (its guard rails are in the source).
"""
from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import event_graph  # noqa: E402

LAMBDAS = ROOT / "aws" / "lambdas"
# Functions that invoke fleet Lambdas as part of running the platform, not as a step of an engine's work.
CONTROL_PLANE = {
    "justhodl-event-coordinator": "the router: it IS an enforcement point (chain_guard.admit before every invoke)",
    "justhodl-spend-governor": "the lever: holds and restores functions",
    "justhodl-scheduler": "the fan-out tick router: every invoke is a scheduled root run (manifest-bounded)",
    "justhodl-chain-resumer": "re-invokes parked walkers from tickets (RESUME_MAX_PER_DAY, MAX_PER_RUN)",
    "justhodl-schedule-liveness": "repairs a missed schedule by invoking the engine once (a root run)",
    "justhodl-fleet-integrity": "fleet probes / self-tests of the deploy lane",
}
# Engines whose source is pinned by a predecessor differential (a reviewed contract): their only self-start is a bounded
# fan-out whose stop condition is recorded in config/loop-allowlist.json, and the contract test fails any change to it.
REVIEWED_CONTRACT = {
    "justhodl-sdmx-walker": "the cron run fans out one async run per agency (5); an agency run never fans out; "
                            "tests/run_tests.py pins the whole module to its predecessor (only _order may differ)",
}
RAW_CLIENT = re.compile(r"""boto3\.(?:client|resource)\(\s*['"]lambda['"]""")


def engine_sources():
    out = {}
    for d in sorted(LAMBDAS.iterdir()):
        src = d / "source"
        if not src.is_dir():
            continue
        texts = []
        for f in sorted(src.rglob("*.py")):
            if any(part in ("urllib3", "site-packages", "__pycache__") or part.endswith(".dist-info") for part in f.relative_to(src).parts):
                continue
            try:
                texts.append(f.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                continue
        out[d.name] = "\n".join(texts)
    return out


def invokers(sources):
    """Engines whose own source calls .invoke( on a Lambda client (the chain_guard module itself is shared, not an engine)."""
    pat = re.compile(r"\.invoke\(\s*\n?\s*FunctionName", re.S)
    return {fn for fn, text in sources.items() if pat.search(text) or re.search(r"\.invoke\(\s*\*\*", text)}


def test_limits_match_config_and_are_sane():
    sys.path.insert(0, str(ROOT / "aws" / "shared"))
    import importlib
    cg = importlib.import_module("chain_guard")
    cfg = json.loads((ROOT / "config" / "trigger-limits.json").read_text(encoding="utf-8"))
    cfg.pop("doc", None)
    assert cg.LIMITS == cfg, "aws/shared/chain_guard.LIMITS must equal config/trigger-limits.json (minus doc)"
    h, s_, d = cfg["handoff"], cfg["self"], cfg["daily"]
    assert 1 <= h["max_hops"] <= 6, "a cascade deeper than 6 hand-offs is never intended"
    assert 2 <= h["max_engines_per_chain"] <= 32
    assert h["max_handoffs_per_chain"] >= 1 and all(v >= 1 for v in h["max_handoffs_per_chain_by_source"].values())
    assert 1 <= s_["max_self_hops"] <= 15, "AWS drops a recursive invoke at 16 hops; the walkers park before that"
    assert d["triggered_default"] >= 1 and d["called_default"] >= 1 and d["triggers_out_default"] >= 1 and d["self_default"] >= 1
    allow = json.loads((ROOT / "config" / "loop-allowlist.json").read_text(encoding="utf-8"))
    pairs = sorted(sorted(c["nodes"]) for c in allow["invoke_cycles"] if len(c["nodes"]) > 1)
    assert sorted(sorted(p) for p in h["repeat_allowed_pairs"]) == pairs, "repeat_allowed_pairs must be exactly the multi-engine cycles of config/loop-allowlist.json"
    rules = set(cfg["hold"])
    assert rules == {"loop", "max_hops", "max_engines_per_chain", "max_handoffs_per_chain", "max_calls_per_chain", "daily_triggered",
                     "daily_called", "daily_triggers_out", "max_self_per_chain", "daily_self"}, rules
    assert all(set(v) <= {"source", "target"} and v for v in cfg["hold"].values())


def test_every_engine_start_goes_through_chain_guard():
    sources = engine_sources()
    bad = []
    for fn in sorted(invokers(sources)):
        if fn in CONTROL_PLANE or fn in REVIEWED_CONTRACT:
            continue
        text = sources[fn]
        uses_guard = "chain_guard.lambda_client(" in text or "chain_guard.chain_invoke(" in text or "from chain_guard import" in text and ("lambda_client(" in text or "chain_invoke(" in text)
        raw = RAW_CLIENT.search(text)
        if not uses_guard:
            bad.append("%s: starts engines without chain_guard" % fn)
        elif raw:
            bad.append("%s: builds a raw Lambda client (%s) - every invoke must go through chain_guard.lambda_client" % (fn, raw.group(0)))
    assert not bad, "\n".join(bad)


def test_relays_begin_their_lineage():
    g = event_graph.build_graph(ROOT)
    edges = g["edges"]
    routes = event_graph.load_routes(ROOT)
    rules = event_graph.load_bus_rules(ROOT)
    dsts = {e[1] for e in edges if e[2] in ("event", "invoke")} | {t for v in routes.values() for t in v} | {t for v in rules.values() for t in v}
    dsts = {fn for fn in dsts if (LAMBDAS / fn / "source").is_dir()}

    def starts_engines(fn):
        node = g["nodes"].get(fn) or {}
        return bool(node.get("published") or node.get("invokes") or node.get("self_invoke") or node.get("dynamic_publish") or node.get("dynamic_invoke"))
    sources = engine_sources()
    # a relay is started by another engine AND publishes or invokes anything - whether or not a route consumes it today
    relays = sorted({fn for fn in dsts if starts_engines(fn)} - set(CONTROL_PLANE) - set(REVIEWED_CONTRACT))
    bad = []
    for fn in relays:
        text = sources.get(fn, "")
        if not ("chain_guard.begin(" in text or "chain_guard.lambda_client(event" in text or "chain_guard.chain_invoke(" in text and "event=" in text):
            bad.append(fn)
    assert not bad, "relay engines must call chain_guard.begin(event, context) at handler entry: %s (destinations %s)" % (bad, sorted(dsts))


def test_publish_side_guard_rails_present():
    text = (ROOT / "aws" / "shared" / "system_events.py").read_text(encoding="utf-8")
    assert "_lineage_or_refusal(" in text and "_cg.LINEAGE_KEY" in text and "max_hops" in text


def test_coordinator_admits_before_every_invoke():
    text = (ROOT / "aws" / "lambdas" / "justhodl-event-coordinator" / "source" / "lambda_function.py").read_text(encoding="utf-8")
    assert "import chain_guard" in text and "trigger_admit(source_fn, fn, lineage)" in text and "invoke_target(fn, event_name, detail, cap.get(\"lineage\"))" in text


def test_governor_has_the_shutdown_action():
    text = (ROOT / "aws" / "lambdas" / "justhodl-spend-governor" / "source" / "lambda_function.py").read_text(encoding="utf-8")
    assert 'if action == "trigger_breach":' in text and "def sweep_breaches(" in text and "def hold_under_cap(" in text
    wf = (ROOT / ".github" / "workflows" / "governor-alerts.yml").read_text(encoding="utf-8")
    assert '"trigger-breach"' in wf, "the GitHub-issue relay must know the trigger-breach event kind"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
    print("trigger_cap tests passed:", len(tests))
