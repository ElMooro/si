"""The market read's OWNED voice (2026-09-17): when the hosted voices are silent, the read is handed to the owned Qwen
endpoint (async) and settled on the next tick -- validated, blockers kept, calls ledgered, republished."""
import importlib.util
import json
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared'))
sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-ai/source'))
sys.path.insert(0, str(ROOT / 'tests/factory'))
from test_factory import MemoryS3  # noqa: E402
from factory_store import Store  # noqa: E402
import factory_inference as fi  # noqa: E402
import market_read as mr  # noqa: E402


def engine_module():
    """The engine's lambda_function: reuse the one the engine harness already imported (its boto3 is a fake there),
    otherwise load it under a private name so another engine's lambda_function in sys.modules is never mistaken for it."""
    cached = sys.modules.get('lambda_function')
    if cached is not None and hasattr(cached, 'settle_owned_read'):
        return cached
    spec = importlib.util.spec_from_file_location('justhodl_ai_engine', str(ROOT / 'aws/lambdas/justhodl-ai/source/lambda_function.py'))
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod

BOARD = {"generated_at": "2026-09-17T05:45:00Z", "sources": {"fusion": {"status": "FRESH"}, "risk_gate": {"status": "FRESH"}, "khalid_risk": {"status": "FRESH"}},
         "risk_gate": {"posture": "RISK_ON", "sizing": "FULL"}, "candidates": ["AAPL", "TLT", "GLD", "BTC-USD"], "fleet_digest": [{"feed": "x", "line": "y"}] * 40}
PLAY = {"available": True, "notes": {"stocks": [{"similarity": 0.8, "label": "thesis", "pinned": False, "note_id": "n1", "text": "buy strength after a selling climax"}]}}
ANSWER = {"overall": "Risk appetite is intact; the gate is on.", "macro": "Growth steady, inflation cooling.",
          "stocks": {"stance": "RISK_ON", "read": "Breadth improving."}, "bonds": {"stance": "NEUTRAL", "read": "Range."},
          "metals": {"stance": "ACCUMULATE", "read": "Gold bid."}, "crypto": {"stance": "HOLD", "read": "BTC consolidating."},
          "best_opportunities": [{"ticker": "AAPL", "side": "LONG", "why": "leadership and breadth", "horizon_days": 21}],
          "what_would_change_my_mind": ["gate flips"], "data_gaps": [],
          "calls": [{"ticker": "AAPL", "direction": "UP", "horizon_days": 21, "confidence": 0.6, "thesis": "leadership and breadth"}]}


class OwnedReadPromptTests(unittest.TestCase):
    def test_budget_bounds_the_prompt_and_keeps_the_wording(self):
        full = mr.build_prompt(BOARD, PLAY, {"lessons": ["do not chase"]}, True, mr.DEFAULT_BUDGET)
        small = mr.build_prompt(BOARD, PLAY, {"lessons": ["do not chase"]}, True, mr.OWNED_BUDGET)
        for p in (full, small):
            self.assertTrue(p.startswith("LESSONS FROM YOUR OWN GRADED CALLS"))
            self.assertIn("CANDIDATES (only tickers allowed in opportunities/calls):\nAAPL, TLT, GLD, BTC-USD", p)
            self.assertTrue(p.endswith("Produce the JSON."))
            self.assertIn("selling climax", p)                                   # note text travels to the owned model
        big = dict(BOARD, fleet_digest=[{"feed": "f%d" % i, "line": "x" * 200} for i in range(200)])   # a real digest is ~30k chars
        self.assertLess(len(mr.build_prompt(big, PLAY, None, True, mr.OWNED_BUDGET)), len(mr.build_prompt(big, PLAY, None, True, mr.DEFAULT_BUDGET)))
        self.assertLessEqual(len(mr.build_prompt(big, PLAY, None, True, mr.OWNED_BUDGET)), sum(mr.OWNED_BUDGET) + 4000)
        self.assertLessEqual(sum(mr.OWNED_BUDGET) + 4000 + len(mr.SYSTEM), 16384 * 3 - 1400 * 3)  # fits the 16k endpoint (ops 5822) with the answer
        self.assertNotIn("selling climax", mr.build_prompt(BOARD, PLAY, None, False))   # and not to a third party

    def test_compose_read_uses_the_shared_parser_and_falls_back_deterministically(self):
        got = mr.compose_read(BOARD, PLAY, lambda prompt, **kw: json.dumps(ANSWER))
        self.assertFalse(got.get("parse_error")); self.assertEqual(got["stocks"]["stance"], "RISK_ON")
        empty = mr.compose_read(BOARD, PLAY, lambda prompt, **kw: "")
        self.assertTrue(empty.get("fallback") and empty.get("empty"))
        fenced = mr.parse_read_text("Sure! ```json\n" + json.dumps({**ANSWER, "calls": [{"ticker": "NVDA", "direction": "UP", "horizon_days": 21, "confidence": 0.5, "thesis": "not a candidate"}]}) + "\n```", {"AAPL"})
        self.assertFalse(fenced.get("parse_error")); self.assertEqual(fenced["calls"], [])       # a call outside the candidates is dropped, not trusted
        bad = mr.parse_read_text(json.dumps({**ANSWER, "stocks": {"stance": "MOON", "read": "x"}}), {"AAPL"})
        self.assertTrue(bad.get("parse_error") and "stance is invalid" in json.dumps(bad))


class OwnedReadSettleTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 9, 17, 5, 45, tzinfo=timezone.utc)
        self.cloud = MemoryS3()
        if not hasattr(self.cloud, 'delete_object'):
            self.cloud.delete_object = lambda Bucket, Key: self.cloud.rows.pop((Bucket, Key), None)
        self.store = Store(self.cloud, 'private', 'public', lambda: self.now)
        self.control = {'enabled': True, 'endpoint_name': 'jh-owned-coder-async', 'model_id': 'qwen2-5-coder-7b-instruct', 'revision': 'c03e6d358207e414', 'max_new_tokens': 700}
        self.cloud.rows[('private', fi.CONTROL_KEY)] = json.dumps(self.control).encode()

    def test_submit_task_claims_then_invokes_with_the_callers_system_prompt(self):
        calls = []
        rt = types.SimpleNamespace(invoke_endpoint_async=lambda **kw: calls.append(kw) or {'OutputLocation': 's3://private/factory/inference/out/x.json', 'FailureLocation': 's3://private/factory/inference/fail/x.json'})
        prompt = mr.build_prompt(BOARD, PLAY, None, True, mr.OWNED_BUDGET)
        pending = fi.submit_task(self.store, rt, self.control, 'market-read', mr.SYSTEM, prompt, max_new_tokens=1400, meta={'candidates': BOARD['candidates']})
        self.assertEqual(pending['state'], 'queued'); self.assertEqual(pending['kind'], 'task'); self.assertEqual(pending['meta']['candidates'], BOARD['candidates'])
        req = json.loads(self.cloud.rows[('private', fi.REQ_PREFIX + pending['id'] + '.json')])
        self.assertTrue(req['inputs'].startswith('<|im_start|>system\n' + mr.SYSTEM[:40]))
        self.assertEqual(req['parameters']['max_new_tokens'], 1400)
        self.assertEqual(len(calls), 1)
        again = fi.submit_task(self.store, rt, self.control, 'market-read', mr.SYSTEM, prompt, max_new_tokens=1400)
        self.assertTrue(again.get('replay')); self.assertEqual(len(calls), 1)                      # same question in flight: no second invoke
        pkey = fi.pending_key('market-read', pending['id'][len('req-'):])
        self.assertIn(('private', pkey), self.cloud.rows)

    def test_settle_replaces_the_deterministic_read_with_the_validated_owned_answer(self):
        lf = engine_module()
        lf.PRIVATE_BUCKET, lf.PUBLIC_BUCKET = 'private', 'public'
        rt = types.SimpleNamespace(invoke_endpoint_async=lambda **kw: {'OutputLocation': 's3://private/factory/inference/out/x.json', 'FailureLocation': 's3://private/factory/inference/fail/x.json'})
        clients = {'s3': self.cloud, 'sagemaker-runtime': rt}
        lf.client = lambda name: clients[name]
        lf.get_json = lambda bucket, key: (lambda raw: json.loads(raw) if raw else None)(self.cloud.rows.get((bucket, key)))
        lf.put_private = lambda key, doc: self.cloud.rows.__setitem__(('private', key), json.dumps(doc, default=str).encode())
        lf.run_inventory = lambda *a, **k: None
        lf._signals_table = lambda: None
        logged = []
        lf.mr.log_calls = lambda table, rid, calls, log_signal, yprice: logged.extend([{'signal_id': 's-' + c['ticker'], 'logged': True} for c in calls]) or logged
        sys.modules['signals_emit'] = types.SimpleNamespace(log_signal=lambda *a, **k: None, yprice=lambda *a, **k: 1.0)
        # 1) the read action's tail: hosted voices silent -> deterministic read + owned submission
        det = mr.deterministic_read(BOARD); det.update(fallback=True, empty=True, llm_path='governed-router | glm: 429')
        ov = lf._submit_owned_read(BOARD, PLAY, {'lessons': []}, {'playbook_text_to_llm': True})
        self.assertEqual(ov['state'], 'queued'); self.assertTrue(ov['pending_id'].startswith('req-'))
        det['owned_voice'] = ov; det['decision_status'] = 'ADVISORY_ONLY'; det['release_blockers'] = ['registry has 80 unique feeds; minimum is 150']
        import os; os.environ['AI_ENVIRONMENT'] = 'review'
        lf._policy = lambda: {}
        doc = {'read_id': '20260917T054500Z', 'generated_at': '2026-09-17T05:45:00Z', 'board': BOARD, 'playbook': PLAY, 'read': det}
        lf.put_private(lf.READ_KEY, doc)
        # 2) before the answer lands: the read is untouched and waits
        self.assertEqual(lf.settle_owned_read()['state'], 'queued')
        self.assertTrue(json.loads(self.cloud.rows[('private', lf.READ_KEY)])['read']['fallback'])
        # 3) the endpoint answers (the object at OutputLocation): the owned read becomes THE read, calls ledgered
        self.cloud.rows[('private', 'factory/inference/out/x.json')] = json.dumps({'generated_text': 'Here you go:\n' + json.dumps(ANSWER), 'details': {'finish_reason': 'eos_token'}}).encode()
        res = lf.settle_owned_read()
        self.assertEqual(res['state'], 'done'); self.assertEqual(res['stances']['stocks'], 'RISK_ON'); self.assertEqual(res['calls_logged'], 1)
        final = json.loads(self.cloud.rows[('private', lf.READ_KEY)])['read']
        self.assertEqual(final['voice'], 'owned'); self.assertFalse(final.get('fallback')); self.assertEqual(final['owned_voice']['state'], 'done')
        self.assertEqual(final['decision_status'], 'ADVISORY_ONLY'); self.assertEqual(final['calls'][0]['ticker'], 'AAPL')   # advisory, yet the call is ledgered for grading
        self.assertTrue(final['calls_ledgered_while_advisory'])
        os.environ['AI_ENVIRONMENT'] = 'production'
        self.assertFalse(lf._ledger_while_advisory({}))                                                                       # production (and test) keep the mute
        os.environ['AI_ENVIRONMENT'] = 'test'
        self.assertFalse(lf._ledger_while_advisory({}))
        self.assertTrue(lf._ledger_while_advisory({'ledger_calls_when_advisory': True}))
        os.environ['AI_ENVIRONMENT'] = 'review'
        self.assertIn('owned model owned:qwen2-5-coder-7b-instruct', final['llm_path'])
        # 4) settled = delivered: a second settle is a no-op, and history carries the owned read
        self.assertFalse(lf.settle_owned_read()['waiting'])
        self.assertEqual(json.loads(self.cloud.rows[('private', 'ai/market-read/history/20260917T054500Z.json')])['read']['voice'], 'owned')

    def test_settle_keeps_the_deterministic_read_when_the_answer_is_not_a_valid_read(self):
        lf = engine_module()
        lf.PRIVATE_BUCKET, lf.PUBLIC_BUCKET = 'private', 'public'
        rt = types.SimpleNamespace(invoke_endpoint_async=lambda **kw: {'OutputLocation': 's3://private/factory/inference/out/y.json', 'FailureLocation': 's3://private/factory/inference/fail/y.json'})
        clients = {'s3': self.cloud, 'sagemaker-runtime': rt}
        lf.client = lambda name: clients[name]
        lf.get_json = lambda bucket, key: (lambda raw: json.loads(raw) if raw else None)(self.cloud.rows.get((bucket, key)))
        lf.put_private = lambda key, doc: self.cloud.rows.__setitem__(('private', key), json.dumps(doc, default=str).encode())
        lf.run_inventory = lambda *a, **k: None
        det = mr.deterministic_read(BOARD); det.update(fallback=True, empty=True)
        det['owned_voice'] = lf._submit_owned_read(BOARD, PLAY, {}, {})
        lf.put_private(lf.READ_KEY, {'read_id': 'r2', 'board': BOARD, 'read': det})
        self.cloud.rows[('private', 'factory/inference/out/y.json')] = json.dumps({'generated_text': 'I think stocks look fine.'}).encode()
        res = lf.settle_owned_read()
        self.assertEqual(res['state'], 'repairing')                                   # prose -> one repair round first
        mid = json.loads(self.cloud.rows[('private', lf.READ_KEY)])['read']
        self.assertTrue(mid['fallback']); self.assertIn('no JSON', mid['owned_voice']['repair']['first_error'])
        self.cloud.rows[('private', 'factory/inference/out/y.json')] = json.dumps({'generated_text': 'still prose, sorry'}).encode()
        res = lf.settle_owned_read()
        self.assertEqual(res['state'], 'malformed')                                   # the repair also failed: terminal, deterministic stands
        final = json.loads(self.cloud.rows[('private', lf.READ_KEY)])['read']
        self.assertTrue(final['fallback']); self.assertEqual(final['owned_voice']['state'], 'malformed'); self.assertIn('no JSON', final['owned_voice']['error'])



class OwnedReadContractTests(unittest.TestCase):
    """What a 7B voice actually writes (ops 5622: 'stocks.read must be a non-empty string') is coerced, never invented."""

    def test_near_miss_shapes_are_normalised_and_every_coercion_is_recorded(self):
        near = {"summary": "Mixed regime with a risk-off posture; caution is warranted across assets, and the cycle is mildly hawkish.",
                "macro": "Growth slowing, inflation sticky.",
                "stocks": {"stance": "risk-off", "reading": "Breadth weak and the gate is defensive."},
                "bonds": {"posture": "long", "read": "Curve bull-steepening favours duration."},
                "metals": "Hold gold; no new buys until the dollar turns.",
                "crypto": {"stance": "neutral", "commentary": "BTC range-bound."},
                "best_opportunities": [{"ticker": "AAPL", "direction": "buy", "reason": "relative strength", "horizon": 21}],
                "what_would_change_my_mind": ["gate flips"], "data_gaps": [],
                "calls": [{"ticker": "TLT", "side": "long", "horizon": 63, "confidence": 65, "why": "duration bid"}]}
        got = mr.parse_read_text("```json\n" + json.dumps(near) + "\n```", {"AAPL", "TLT"})
        self.assertFalse(got.get("parse_error"), got)
        self.assertEqual(got["stocks"]["stance"], "DEFENSIVE"); self.assertEqual(got["bonds"]["stance"], "LONG_DURATION")
        self.assertEqual(got["metals"]["stance"], "HOLD"); self.assertEqual(got["crypto"]["stance"], "HOLD")
        self.assertEqual(got["best_opportunities"][0]["side"], "LONG"); self.assertEqual(got["calls"][0]["direction"], "UP")
        self.assertEqual(got["calls"][0]["confidence"], 0.65); self.assertEqual(got["calls"][0]["horizon_days"], 63)
        self.assertTrue(any("stocks.stance" in f for f in got["coercions"]) and any("overall <- summary" in f for f in got["coercions"]))

    def test_a_missing_read_is_still_a_rejection_not_an_invention(self):
        missing = {"overall": "x" * 30, "macro": "y" * 12, "stocks": {"stance": "DEFENSIVE"}, "bonds": {"stance": "NEUTRAL", "read": "r"},
                   "metals": {"stance": "HOLD", "read": "r"}, "crypto": {"stance": "HOLD", "read": "r"}}
        got = mr.parse_read_text(json.dumps(missing), set())
        self.assertTrue(got.get("parse_error")); self.assertIn("stocks.read", got["validation_error"])

    def test_owned_prompt_carries_the_skeleton_and_repair_prompt_carries_the_error(self):
        p = mr.build_prompt(BOARD, PLAY, None, True, mr.OWNED_BUDGET, schema_hint=True)
        self.assertIn('"stocks": {"stance": "RISK_ON|SELECTIVE|DEFENSIVE|AVOID"', p); self.assertIn("Copy exactly this shape", p)
        self.assertIn("at least 2 dated calls", p)                     # 2026-09-19: a read with 0 calls (like that morning's) is never graded
        self.assertNotIn("Copy exactly this shape", mr.build_prompt(BOARD, PLAY, None, True))
        rp = mr.repair_prompt('{"stocks": {"stance": "DEFENSIVE"}}', "validation: stocks.read must be a non-empty string")
        self.assertIn("stocks.read must be a non-empty string", rp); self.assertIn(mr.SCHEMA_SKELETON, rp); self.assertIn('"stance": "DEFENSIVE"', rp)

    def test_settle_repairs_once_then_accepts_the_corrected_answer(self):
        lf = engine_module()
        lf.PRIVATE_BUCKET, lf.PUBLIC_BUCKET = 'private', 'public'
        cloud = MemoryS3()
        if not hasattr(cloud, 'delete_object'):
            cloud.delete_object = lambda Bucket, Key: cloud.rows.pop((Bucket, Key), None)
        control = {'enabled': True, 'endpoint_name': 'jh-owned-coder-async', 'model_id': 'qwen2-5-coder-7b-instruct', 'revision': 'c03e6d358207e414'}
        cloud.rows[('private', fi.CONTROL_KEY)] = json.dumps(control).encode()
        n = {'i': 0}
        def invoke(**kw):
            n['i'] += 1
            return {'OutputLocation': 's3://private/factory/inference/out/r%d.json' % n['i'], 'FailureLocation': 's3://private/factory/inference/fail/r%d.json' % n['i']}
        rt = types.SimpleNamespace(invoke_endpoint_async=invoke)
        clients = {'s3': cloud, 'sagemaker-runtime': rt}
        lf.client = lambda name: clients[name]
        lf.get_json = lambda bucket, key: (lambda raw: json.loads(raw) if raw else None)(cloud.rows.get((bucket, key)))
        lf.put_private = lambda key, doc: cloud.rows.__setitem__(('private', key), json.dumps(doc, default=str).encode())
        lf.run_inventory = lambda *a, **k: None
        lf._signals_table = lambda: None
        lf.mr.log_calls = lambda table, rid, calls, log_signal, yprice: [{'signal_id': 's-' + c['ticker'], 'logged': True} for c in calls]
        sys.modules['signals_emit'] = types.SimpleNamespace(log_signal=lambda *a, **k: None, yprice=lambda *a, **k: 1.0)
        det = mr.deterministic_read(BOARD); det.update(fallback=True, empty=True, decision_status='EVIDENCE_READY', release_blockers=[])
        det['owned_voice'] = lf._submit_owned_read(BOARD, PLAY, {}, {})
        lf.put_private(lf.READ_KEY, {'read_id': 'r3', 'board': BOARD, 'read': det})
        self.assertEqual(n['i'], 1)
        # first answer: a near-miss the normaliser cannot save (no read text at all) -> a repair is submitted, the deterministic read stands
        bad = {**ANSWER, 'stocks': {'stance': 'DEFENSIVE'}}
        cloud.rows[('private', 'factory/inference/out/r1.json')] = json.dumps({'generated_text': json.dumps(bad)}).encode()
        res = lf.settle_owned_read()
        self.assertEqual(res['state'], 'repairing'); self.assertEqual(n['i'], 2)
        mid = json.loads(cloud.rows[('private', lf.READ_KEY)])['read']
        self.assertTrue(mid['fallback']); self.assertEqual(mid['owned_voice']['repair']['attempt'], 1); self.assertIn('stocks.read', mid['owned_voice']['repair']['first_error'])
        req = json.loads(cloud.rows[('private', fi.REQ_PREFIX + mid['owned_voice']['pending_id'] + '.json')])
        self.assertIn('was rejected by the validator', req['inputs']); self.assertIn('"stance": "DEFENSIVE"', req['inputs'])
        # the repaired answer lands -> it becomes the read, marked repaired
        cloud.rows[('private', 'factory/inference/out/r2.json')] = json.dumps({'generated_text': json.dumps(ANSWER)}).encode()
        res = lf.settle_owned_read()
        self.assertEqual(res['state'], 'done')
        final = json.loads(cloud.rows[('private', lf.READ_KEY)])['read']
        self.assertEqual(final['voice'], 'owned'); self.assertTrue(final['owned_voice']['repaired']); self.assertEqual(final['stocks']['read'], 'Breadth improving.')
        # a second failure after the repair is terminal (no infinite repair loop)
        det2 = mr.deterministic_read(BOARD); det2.update(fallback=True, empty=True)
        det2['owned_voice'] = dict(lf._submit_owned_read(dict(BOARD, generated_at='2026-09-17T06:00:00Z'), PLAY, {}, {}), repair={'attempt': 1})
        lf.put_private(lf.READ_KEY, {'read_id': 'r4', 'board': BOARD, 'read': det2})
        cloud.rows[('private', 'factory/inference/out/r3.json')] = json.dumps({'generated_text': 'still prose'}).encode()
        self.assertEqual(lf.settle_owned_read()['state'], 'malformed'); self.assertEqual(n['i'], 3)



class PublicReadModelTests(unittest.TestCase):
    def test_public_read_model_carries_the_market_exam(self):
        src = (ROOT / 'aws/lambdas/justhodl-ai/source/lambda_function.py').read_text()
        # the public data/ai.json is composed field by field from `out`; a block added to `out` alone never reaches the page (2026-09-17)
        self.assertIn('"market_exam": _safe(public_market_exam)', src)
        self.assertIn('"market_exam": out.get("market_exam")', src)


class CodingExamVerdictTests(unittest.TestCase):
    def test_plateau_is_stated_in_plain_words_and_learning_is_recognised(self):
        lf = engine_module()
        base = {"score": 0.823, "passed": 135, "n": 164}
        cands = [{"generation": "gen-10", "score": 0.817, "passed": 134, "n": 164, "at": "2026-09-17T13:52:33Z", "critical_failures": 0},
                 {"generation": "gen-11", "score": 0.799, "passed": 131, "n": 164, "at": "2026-09-17T13:57:54Z", "critical_failures": 0},
                 {"generation": "gen-12", "score": 0.95, "passed": 156, "n": 164, "at": "2026-09-18T00:00:00Z", "critical_failures": 2}]   # untrusted: excluded
        v = lf.coding_exam_verdict(base, cands, 570)
        self.assertEqual(v["n_candidates"], 2); self.assertEqual(v["learning_pts"], -0.6); self.assertEqual(v["best_candidate"]["generation"], "gen-10")
        self.assertIn("No learning yet", v["verdict"]); self.assertIn("570 tasks", v["verdict"]); self.assertIn("preference training", v["verdict"])
        good = lf.coding_exam_verdict(base, cands[:2] + [{"generation": "gen-13", "score": 0.86, "passed": 141, "n": 164, "at": "2026-09-19T00:00:00Z", "critical_failures": 0}], 900)
        self.assertEqual(good["learning_pts"], 3.7); self.assertIn("beats the base by 3.7 points", good["verdict"])
        self.assertIn("No trusted base", lf.coding_exam_verdict(None, cands, None)["verdict"])
        self.assertIn("no candidate", lf.coding_exam_verdict(base, [], 570)["verdict"])


class StudentDeskTests(unittest.TestCase):
    def test_desk_lines_are_computed_from_facts(self):
        lf = engine_module()
        pub = {"scoreboard": {"voice": "online (owned model x)", "calls_made": 11, "calls_graded": 0, "hit_rate_by_window": {}, "understanding_score": 0.92},
               "market_read": {"decision_status": "ADVISORY_ONLY", "n_blockers": 4, "stances": {"stocks": "SELECTIVE"}},
               "coding_exam": {"base_score": 0.823, "base_passed": "135/164", "learning_pts": 0.0, "verdict": "No learning yet: ..."},
               "market_exam": {"holdout": {"n_drills": 55, "model_scores": {"score": 0.4836, "direction_acc": 0.55}, "baselines": {"prior": {"score": 0.5127}}}},
               "reasoning_exam": {"families": {"gsm8k": {"accuracy": 0.825}, "code_reading": {"accuracy": 0.2}}},
               "gear_b": {"champion": {"generation": 0, "note": "base model; no weights promoted"}}, "pipeline": {"status": "idle"}, "student_wall": None}
        d = lf.student_desk(pub)
        self.assertFalse(d["market_exam_holdout"]["beats_prior"]); self.assertEqual(d["calls"]["graded"], 0)
        self.assertTrue(any("Beat the naive prior" in c for c in d["cannot_do_yet"])); self.assertTrue(any("0 of 11 calls graded" in c for c in d["cannot_do_yet"]))
        self.assertIn("never will without a human apply-lane", " ".join(d["cannot_do_yet"])); self.assertIn("first 5-day grades", d["next_lesson"])
        pub["market_exam"]["holdout"]["model_scores"]["score"] = 0.60; pub["scoreboard"]["calls_graded"] = 25
        d2 = lf.student_desk(pub)
        self.assertTrue(d2["market_exam_holdout"]["beats_prior"]); self.assertFalse(any("Beat the naive prior" in c for c in d2["cannot_do_yet"])); self.assertIn("first non-advisory step", d2["next_lesson"])

if __name__ == '__main__':
    unittest.main()
