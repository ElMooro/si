"""chain_guard v2 - the fleet-wide trigger cap, offline: a faithful fake of the DynamoDB ledger (conditional ADD / string
sets), a fake Lambda client and a fake governor.  No AWS, no network.

Covers: root runs touch no ledger; depth, loop, fan-out, volume, daily budgets and the per-source volume override; sync
calls vs hand-offs; self-continuations (park at the hop cap, breach past the daily budget); the hold sets; one breach
record per (day, source, target, rule) and one governor call; fail-closed when the ledger is unavailable (no hold);
stale warm-container state reset; lineage stamping end to end (publisher -> coordinator-style admit -> target)."""
from pathlib import Path
import io
import json
import os
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws" / "shared"))


from botocore.exceptions import ClientError  # the real class: chain_guard tells a refused condition from an outage by it


def ConditionFailed():
    return ClientError({"Error": {"Code": "ConditionalCheckFailedException", "Message": "x"}}, "UpdateItem")


class FakeDDB:
    """Enough of DynamoDB for chain_guard: ADD n with 'n < :limit', ADD string set with size/contains, conditional put."""

    def __init__(self):
        self.items = {}
        self.calls = 0
        self.down = False

    def _check(self):
        self.calls += 1
        if self.down:
            raise RuntimeError("ProvisionedThroughputExceededException")

    def update_item(self, TableName, Key, UpdateExpression, ConditionExpression=None, ExpressionAttributeValues=None, ExpressionAttributeNames=None):
        self._check()
        pk = Key["pk"]["S"]
        item = self.items.setdefault(pk, {})
        v = ExpressionAttributeValues or {}
        if "ADD n :one" in UpdateExpression:
            limit = int(v[":limit"]["N"])
            if "n" in item and item["n"] >= limit:
                raise ConditionFailed()
            item["n"] = item.get("n", 0) + 1
            item.setdefault("expires_at", int(v[":exp"]["N"]))
            return {}
        if "ADD engines :e" in UpdateExpression:
            limit = int(v[":limit"]["N"])
            t = v[":t"]["S"]
            eng = item.get("engines", set())
            if eng and t not in eng and len(eng) >= limit:
                raise ConditionFailed()
            item["engines"] = eng | set(v[":e"]["SS"])
            item.setdefault("expires_at", int(v[":exp"]["N"]))
            return {}
        if "SET #s = :s" in UpdateExpression:
            item["status"] = v[":s"]["S"]
            return {}
        raise AssertionError(UpdateExpression)

    def put_item(self, TableName, Item, ConditionExpression=None):
        self._check()
        pk = Item["pk"]["S"]
        if ConditionExpression and pk in self.items:
            raise ConditionFailed()
        self.items[pk] = {k: (list(val.values())[0]) for k, val in Item.items()}
        return {}

    def get_item(self, TableName, Key):
        it = self.items.get(Key["pk"]["S"])
        if it is None:
            return {}
        return {"Item": {k: ({"S": v} if isinstance(v, str) else {"N": str(v)}) for k, v in it.items() if not isinstance(v, set)}}


class FakeLambda:
    def __init__(self):
        self.invokes = []

    def invoke(self, **kw):
        self.invokes.append(kw)
        return {"StatusCode": 202 if kw.get("InvocationType") == "Event" else 200, "Payload": io.BytesIO(b'{"ok": true}')}


def load_guard(ddb, lam, gov):
    for m in ("chain_guard",):
        sys.modules.pop(m, None)
    sys.modules["boto3"] = types.SimpleNamespace(client=lambda name, **k: {"dynamodb": ddb, "lambda": lam}[name])
    import chain_guard as cg
    cg._ddb = ddb
    cg._lam = lam
    cg._gov = gov
    cg._s3 = types.SimpleNamespace(put_object=lambda **k: None)
    cg._ctx.update({"trace": None, "request_id": None, "lineage": None, "fn": None})
    return cg


class Ctx:
    def __init__(self, fn, rid):
        self.function_name = fn
        self.aws_request_id = rid


class Tests(unittest.TestCase):
    def setUp(self):
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "justhodl-alpha"
        os.environ.pop("_X_AMZN_TRACE_ID", None)
        self.ddb, self.lam, self.gov = FakeDDB(), FakeLambda(), FakeLambda()
        self.cg = load_guard(self.ddb, self.lam, self.gov)

    # ---- lineage ------------------------------------------------------------------------------------------
    def test_root_run_has_no_lineage_and_touches_no_ledger(self):
        cg = self.cg
        hop = cg.begin({"some": "schedule payload"}, Ctx("justhodl-alpha", "r1"))
        self.assertEqual(hop, 0)
        lin = cg.current(Ctx("justhodl-alpha", "r1"))
        self.assertEqual((lin["hop"], lin["path"], lin["root"]), (0, ["justhodl-alpha"], "justhodl-alpha"))
        self.assertEqual(self.ddb.calls, 0)

    def test_begin_reads_lineage_from_payload_detail_and_trigger_detail(self):
        cg = self.cg
        lin = {"id": "x-1", "root": "justhodl-root", "hop": 1, "path": ["justhodl-root", "justhodl-alpha"], "self_hop": 0, "n": 1}
        for event in ({cg.LINEAGE_KEY: lin}, {"detail": {cg.LINEAGE_KEY: lin}}, {"trigger_detail": {cg.LINEAGE_KEY: lin}}):
            cg.begin(event, Ctx("justhodl-alpha", "r2"))
            self.assertEqual(cg.current(Ctx("justhodl-alpha", "r2"))["id"], "x-1")

    def test_stale_warm_container_state_is_reset_on_a_new_invocation(self):
        cg = self.cg
        lin = {"id": "old-chain", "root": "justhodl-root", "hop": 2, "path": ["justhodl-root", "justhodl-mid", "justhodl-alpha"], "self_hop": 0, "n": 2}
        cg.begin({cg.LINEAGE_KEY: lin}, Ctx("justhodl-alpha", "r3"))
        self.assertEqual(cg.current(Ctx("justhodl-alpha", "r3"))["id"], "old-chain")
        fresh = cg.current(Ctx("justhodl-alpha", "r4"))               # next invocation, begin() never called
        self.assertNotEqual(fresh["id"], "old-chain")
        self.assertEqual((fresh["hop"], fresh["path"]), (0, ["justhodl-alpha"]))

    # ---- hand-offs ----------------------------------------------------------------------------------------
    def test_depth_cap_refuses_the_fourth_hop_and_holds_the_source(self):
        cg = self.cg
        lin = cg.fresh("justhodl-a")
        for src, dst in (("justhodl-a", "justhodl-b"), ("justhodl-b", "justhodl-c"), ("justhodl-c", "justhodl-d")):
            v = cg.admit(src, dst, lin, "handoff")
            self.assertTrue(v["allow"], v)
            lin = v["lineage"]
        self.assertEqual((lin["hop"], lin["path"]), (3, ["justhodl-a", "justhodl-b", "justhodl-c", "justhodl-d"]))
        v = cg.admit("justhodl-d", "justhodl-e", lin, "handoff")
        self.assertFalse(v["allow"])
        self.assertEqual((v["reason"], v["hold"]), ("max_hops", ["justhodl-d"]))
        self.assertTrue(v["breach_id"].startswith("breach#"))
        self.assertEqual(len(self.gov.invokes), 1)
        self.assertEqual(json.loads(self.gov.invokes[0]["Payload"])["action"], "trigger_breach")

    def test_loop_is_refused_at_once_and_holds_both_ends(self):
        cg = self.cg
        v = cg.admit("justhodl-a", "justhodl-b", cg.fresh("justhodl-a"), "handoff")
        v = cg.admit("justhodl-b", "justhodl-a", v["lineage"], "handoff")
        self.assertEqual((v["allow"], v["reason"], v["hold"]), (False, "loop", ["justhodl-a", "justhodl-b"]))

    def test_allowlisted_pair_may_revisit_but_depth_still_binds(self):
        cg = self.cg
        lin = cg.fresh("justhodl-audit-loop")
        v = cg.admit("justhodl-audit-loop", "justhodl-a2a-bus", lin, "call")             # hop 1
        v = cg.admit("justhodl-a2a-bus", "justhodl-backend-agent", v["lineage"], "handoff")   # hop 2
        v = cg.admit("justhodl-backend-agent", "justhodl-a2a-bus", v["lineage"], "call")      # hop 3: a revisit of the allowlisted pair
        self.assertTrue(v["allow"], v)
        self.assertEqual(v["lineage"]["hop"], 3)
        v = cg.admit("justhodl-a2a-bus", "justhodl-backend-agent", v["lineage"], "handoff")   # hop 4: too deep, whatever the pair
        self.assertEqual((v["allow"], v["reason"], v["hold"]), (False, "max_hops", ["justhodl-a2a-bus"]))
        v = cg.admit("justhodl-x", "justhodl-y", cg.admit("justhodl-y", "justhodl-x", cg.fresh("justhodl-y"), "handoff")["lineage"], "handoff")
        self.assertEqual(v["reason"], "loop")                                                   # any other pair: a loop

    def test_self_route_through_the_bus_is_a_loop(self):
        cg = self.cg
        v = cg.admit("justhodl-a", "justhodl-a", cg.fresh("justhodl-a"), "handoff")
        self.assertEqual((v["allow"], v["reason"]), (False, "loop"))

    def test_fan_out_cap_counts_distinct_engines_per_chain(self):
        cg = self.cg
        lin = cg.fresh("justhodl-hub")
        n = cg.LIMITS["handoff"]["max_engines_per_chain"]
        for i in range(n - 1):                                         # the hub itself is engine #1
            self.assertTrue(cg.admit("justhodl-hub", "justhodl-t%d" % i, lin, "handoff")["allow"])
        self.assertTrue(cg.admit("justhodl-hub", "justhodl-t0", lin, "handoff")["allow"])   # a repeat target is not a new engine
        v = cg.admit("justhodl-hub", "justhodl-new", lin, "handoff")
        self.assertEqual((v["allow"], v["reason"], v["hold"]), (False, "max_engines_per_chain", ["justhodl-hub"]))

    def test_volume_cap_per_chain_with_the_per_source_override(self):
        cg = self.cg
        lin = cg.fresh("justhodl-hub")
        cap = cg.LIMITS["handoff"]["max_handoffs_per_chain"]
        for _ in range(cap):
            self.assertTrue(cg.admit("justhodl-hub", "justhodl-t", lin, "handoff")["allow"])
        v = cg.admit("justhodl-hub", "justhodl-t", lin, "handoff")
        self.assertEqual((v["allow"], v["reason"]), (False, "max_handoffs_per_chain"))
        lin2 = cg.fresh("justhodl-equity-prewarm")
        for _ in range(cap + 5):                                        # prewarm's override allows 1500 critiques per run
            self.assertTrue(cg.admit("justhodl-equity-prewarm", "justhodl-research-critique", lin2, "handoff")["allow"])

    def test_daily_budgets_hold_the_right_engines(self):
        cg = self.cg
        cg.LIMITS["daily"]["triggered"]["justhodl-sink"] = 2
        try:
            for i in range(2):
                self.assertTrue(cg.admit("justhodl-src%d" % i, "justhodl-sink", cg.fresh("justhodl-src%d" % i), "handoff")["allow"])
            v = cg.admit("justhodl-src9", "justhodl-sink", cg.fresh("justhodl-src9"), "handoff")
            self.assertEqual((v["allow"], v["reason"], v["hold"]), (False, "daily_triggered", ["justhodl-sink", "justhodl-src9"]))
        finally:
            cg.LIMITS["daily"]["triggered"].pop("justhodl-sink", None)
        cg.LIMITS["daily"]["triggers_out"]["justhodl-chatty"] = 1
        try:
            self.assertTrue(cg.admit("justhodl-chatty", "justhodl-x", cg.fresh("justhodl-chatty"), "handoff")["allow"])
            v = cg.admit("justhodl-chatty", "justhodl-y", cg.fresh("justhodl-chatty"), "handoff")
            self.assertEqual((v["allow"], v["reason"], v["hold"]), (False, "daily_triggers_out", ["justhodl-chatty"]))
        finally:
            cg.LIMITS["daily"]["triggers_out"].pop("justhodl-chatty", None)

    def test_sync_calls_use_their_own_budgets(self):
        cg = self.cg
        lin = cg.fresh("justhodl-census")
        for _ in range(cg.LIMITS["handoff"]["max_handoffs_per_chain"] + 20):
            v = cg.admit("justhodl-census", "justhodl-graphs", lin, "call")
            self.assertTrue(v["allow"], v)
        self.assertEqual(v["lineage"]["hop"], 1)
        cg.LIMITS["daily"]["called"]["justhodl-w"] = 1
        try:
            self.assertTrue(cg.admit("justhodl-c", "justhodl-w", cg.fresh("justhodl-c"), "call")["allow"])
            v = cg.admit("justhodl-c", "justhodl-w", cg.fresh("justhodl-c"), "call")
            self.assertEqual((v["reason"], v["hold"]), ("daily_called", ["justhodl-c", "justhodl-w"]))
        finally:
            cg.LIMITS["daily"]["called"].pop("justhodl-w", None)

    # ---- self-continuation --------------------------------------------------------------------------------
    def test_self_hops_park_at_the_cap_and_breach_past_the_daily_budget(self):
        cg = self.cg
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "justhodl-walker"
        ctx = Ctx("justhodl-walker", "w1")
        cg.begin({}, ctx)
        out = cg.chain_invoke({"cursor": 5}, context=ctx)
        self.assertEqual((out["chained"], out["hop"]), (True, 1))
        sent = json.loads(self.lam.invoke_kw["Payload"]) if hasattr(self.lam, "invoke_kw") else json.loads(self.lam.invokes[-1]["Payload"])
        self.assertEqual((sent[cg.HOP_KEY], sent[cg.LINEAGE_KEY]["self_hop"], sent["cursor"]), (1, 1, 5))
        deep = dict(sent[cg.LINEAGE_KEY], self_hop=cg.LIMITS["self"]["max_self_hops"])
        cg.begin({cg.LINEAGE_KEY: deep}, Ctx("justhodl-walker", "w2"))
        out = cg.chain_invoke({"cursor": 6}, context=Ctx("justhodl-walker", "w2"))
        self.assertEqual((out["chained"], out["parked"]), (False, True))
        cg.LIMITS["daily"]["self"]["justhodl-walker"] = 1
        try:
            cg.begin({}, Ctx("justhodl-walker", "w3"))
            out = cg.chain_invoke({"cursor": 7}, context=Ctx("justhodl-walker", "w3"))
            self.assertEqual((out["chained"], out["refused"], out["hold"]), (False, "daily_self", ["justhodl-walker"]))
        finally:
            cg.LIMITS["daily"]["self"].pop("justhodl-walker", None)

    # ---- the guarded client -------------------------------------------------------------------------------
    def test_guarded_client_stamps_lineage_and_raises_on_refusal(self):
        cg = self.cg
        ctx = Ctx("justhodl-alpha", "g1")
        client = cg.lambda_client({"detail": {}}, ctx, client=self.lam)
        client.invoke(FunctionName="justhodl-beta", InvocationType="Event", Payload=json.dumps({"x": 1}).encode())
        sent = json.loads(self.lam.invokes[-1]["Payload"])
        self.assertEqual((sent["x"], sent[cg.LINEAGE_KEY]["hop"], sent[cg.LINEAGE_KEY]["path"]), (1, 1, ["justhodl-alpha", "justhodl-beta"]))
        # the target continues the chain and tries to come back: refused, both held, one governor call
        os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "justhodl-beta"
        tclient = cg.lambda_client(sent, Ctx("justhodl-beta", "g2"), client=self.lam)
        with self.assertRaises(cg.TriggerRefused) as caught:
            tclient.invoke(FunctionName="arn:aws:lambda:us-east-1:1:function:justhodl-alpha:live", InvocationType="Event", Payload=b"{}")
        self.assertEqual((caught.exception.verdict["reason"], caught.exception.verdict["hold"]), ("loop", ["justhodl-alpha", "justhodl-beta"]))
        self.assertEqual(len(self.gov.invokes), 1)
        soft = cg.lambda_client(sent, Ctx("justhodl-beta", "g3"), client=self.lam, soft=True)
        res = soft.invoke(FunctionName="justhodl-alpha", InvocationType="Event", Payload=b"{}")
        self.assertEqual((res["StatusCode"], res["TriggerRefused"]["reason"]), (0, "loop"))
        self.assertEqual(len(self.gov.invokes), 1)                       # same day, same rule: already reported
        self.assertEqual(len([k for k in self.ddb.items if k.startswith("breach#")]), 1)

    def test_ledger_unavailable_refuses_without_holding_anyone(self):
        cg = self.cg
        self.ddb.down = True
        v = cg.admit("justhodl-a", "justhodl-b", cg.fresh("justhodl-a"), "handoff")
        self.assertEqual((v["allow"], v["reason"], v["hold"], v["breach"]), (False, "ledger_unavailable", [], False))
        self.assertEqual(self.gov.invokes, [])

    # ---- publish side -------------------------------------------------------------------------------------
    def test_publish_stamps_lineage_and_refuses_at_the_depth_cap(self):
        cg = self.cg
        sys.modules.pop("system_events", None)
        import system_events as se
        se._cg = cg
        sent = []
        client = types.SimpleNamespace(put_events=lambda Entries: sent.extend(Entries) or {"FailedEntryCount": 0, "Entries": [{"EventId": "e"} for _ in Entries]})
        sys.modules["boto3"] = types.SimpleNamespace(client=lambda name, **k: client)
        cg.begin({}, Ctx("justhodl-alpha", "p1"))
        self.assertTrue(se.publish("regime.changed", {"a": 1}))
        d = json.loads(sent[-1]["Detail"])
        self.assertEqual((d["a"], d[cg.LINEAGE_KEY]["hop"], d[cg.LINEAGE_KEY]["path"]), (1, 0, ["justhodl-alpha"]))
        deep = {"id": "c", "root": "justhodl-r", "hop": cg.LIMITS["handoff"]["max_hops"], "path": ["justhodl-r", "justhodl-x", "justhodl-y", "justhodl-alpha"], "self_hop": 0, "n": 3}
        cg.begin({cg.LINEAGE_KEY: deep}, Ctx("justhodl-alpha", "p2"))
        n = len(sent)
        self.assertFalse(se.publish("regime.changed", {"a": 2}))
        res = se.publish_many([("regime.changed", {"a": 3})], client=client)
        self.assertEqual((res["ok"], res["refused"], len(sent)), (False, "max_hops", n))

    def test_limits_match_the_committed_config(self):
        cfg = json.loads((ROOT / "config" / "trigger-limits.json").read_text())
        cfg.pop("doc", None)
        self.assertEqual(self.cg.LIMITS, cfg)


if __name__ == "__main__":
    result = unittest.main(exit=False, verbosity=2).result
    sys.exit(0 if result.wasSuccessful() else 1)
