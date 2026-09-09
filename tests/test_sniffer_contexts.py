"""Execute the real router handler with offline public feeds and model replies."""
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import io
import json
from pathlib import Path
import sys
import time
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws/shared"))
from private_artifact import is_private_source
SOURCE = ROOT / "aws/lambdas/justhodl-ai-brief-router/source/lambda_function.py"
REGISTRY = json.loads((ROOT / "config/ai-brief-contexts.json").read_text())["contexts"]
CONTEXTS = ("frontrun-sniffer", "macro-frontrun-sniffer")


def fixture(configs=None, fail_model=False):
    configs = deepcopy(configs or {key: REGISTRY[key] for key in CONTEXTS})
    reads, writes, alerts = [], {}, []
    def get_json(key, default=None):
        reads.append(key)
        if is_private_source(key):
            raise AssertionError("Private feed must never be read")
        if key == "config/ai-brief-contexts.json": return {"contexts": configs}
        if key.endswith("-history.json"): return {"snapshots": []}
        return {"value": 1, "regime": "NORMAL", "generated_at": datetime.now(timezone.utc).isoformat()}
    def put_json(key, doc, **kwargs): writes[key] = deepcopy(doc)
    tree = ast.parse(SOURCE.read_text())
    constants = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            try: constants[node.targets[0].id] = ast.literal_eval(node.value)
            except (ValueError, TypeError): pass
    scope = {**constants, "json":json, "time":time, "datetime":datetime, "timezone":timezone, "timedelta":timedelta,
             "ThreadPoolExecutor":ThreadPoolExecutor, "as_completed":as_completed, "is_private_source":is_private_source,
             "s3io":types.SimpleNamespace(get_json=get_json, put_json=put_json),
             "kb":types.SimpleNamespace(lookup=lambda *_args, **_kwargs: [])}
    exec(compile(ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef)], type_ignores=[]), str(SOURCE), "exec"), scope)
    scope["claude_json"] = lambda *_args, **_kwargs: (None, "synthetic_model_unavailable") if fail_model else (
        {"headline":"Public research fixture", "anomaly_regime":"NORMAL", "overall_anomaly_score":0,
         "macro_regime":"NORMAL", "overall_macro_score":0, "suspected_setups":[], "macro_setups":[]}, None)
    scope["_maybe_alert_equity_sniffer"] = lambda _brief: alerts.append("mock_equity")
    scope["_maybe_alert_macro_sniffer"] = lambda _brief: alerts.append("mock_macro")
    return scope, reads, writes, alerts


class SnifferContextTests(unittest.TestCase):
    def test_real_handler_publishes_each_source_owned_current_and_history_pair(self):
        scope, reads, writes, alerts = fixture()
        with redirect_stdout(io.StringIO()): result = scope["lambda_handler"]({"contexts":list(CONTEXTS)})
        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(set(writes), {"data/" + key + suffix + ".json" for key in CONTEXTS for suffix in ("", "-history")})
        for key in CONTEXTS:
            self.assertEqual(writes["data/"+key+".json"]["context"], key)
            self.assertEqual(len(writes["data/"+key+"-history.json"]["snapshots"]), 1)
        self.assertEqual(len(alerts), 2)
        self.assertFalse(any(is_private_source(key) for key in reads))

    def test_real_handler_does_not_overwrite_specialized_schema_on_model_failure(self):
        scope, reads, writes, alerts = fixture(fail_model=True)
        with redirect_stdout(io.StringIO()): result = scope["lambda_handler"]({"contexts":list(CONTEXTS)})
        self.assertFalse(writes); self.assertFalse(alerts)
        self.assertNotIn("OK_DET", result["body"])
        self.assertIn("ERR_CLAUDE", result["body"])
        self.assertNotIn("", reads)  # Generic deterministic fallback was not entered.

    def test_output_alias_type_and_private_input_drift_rejected_before_any_feed_read(self):
        for context in CONTEXTS:
            for mutation in ("output", "type", "private", "traversal", "empty"):
                with self.subTest(context=context, mutation=mutation):
                    cfg = deepcopy(REGISTRY[context])
                    field = "flow_sources" if context == CONTEXTS[0] else "pillar_feeds"
                    if mutation == "output": cfg["output_key"] = "brain"
                    if mutation == "type": cfg["brief_type"] = "regime"
                    if mutation == "private": cfg[field] = {"private":"data/brain.json"}
                    if mutation == "traversal": cfg[field] = {"private":"data/../brain.json"}
                    if mutation == "empty": cfg[field] = {}
                    scope, reads, writes, alerts = fixture()
                    result = scope["generate_one_brief"](context, cfg, {})
                    self.assertEqual(result["status"], "ERR_CONFIG")
                    self.assertFalse(reads); self.assertFalse(writes); self.assertFalse(alerts)

    def test_alternate_context_cannot_claim_pinned_writer_or_enter_generic_fallback(self):
        scope, reads, writes, alerts = fixture()
        result = scope["generate_one_brief"]("unreviewed-alias", deepcopy(REGISTRY[CONTEXTS[0]]), {})
        self.assertEqual(result["status"], "ERR_CONFIG"); self.assertFalse(reads); self.assertFalse(writes)


if __name__ == "__main__": unittest.main()
