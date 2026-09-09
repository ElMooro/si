"""ops 5260 — chain_guard contract + fleet gate.

1. chain_invoke chains while the lineage hop is under the cap and PARKS a
   resume ticket at the cap (no self-invoke), with the hop reset to 0 in the
   ticket so the resumer starts a fresh lineage.
2. Static fleet gate: no engine may Event-invoke ITSELF directly any more
   unless it is a one-shot fan-out (shard dispatch, no continuation). The
   Sep-7 Health event came from exactly this pattern; the gate makes the
   regression impossible to ship silently.
"""
from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "aws" / "shared"))

import chain_guard  # noqa: E402

# Fan-outs: one async invoke per shard, no continuation payload. Structurally depth 1.
FANOUT_ALLOW = {
    "justhodl-boj-full", "justhodl-sdmx-walker", "justhodl-tv-bars", "justhodl-symdir",
    "justhodl-census-us",
    "justhodl-equity-research",  # ops 4242: async kickoff, child can never kick off again (depth 2)
}
# Engines that carry their own explicit cap under AWS's 16 (census v1.11.0, fleet-integrity, 13f-clone-alpha).
OWN_CAP_ALLOW = {"justhodl-fundamental-census", "justhodl-fleet-integrity", "justhodl-13f-clone-alpha"}


class _Lam:
    def __init__(self):
        self.calls = []

    def invoke(self, **kw):
        self.calls.append(kw)
        return {"StatusCode": 202}


class _S3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = json.loads(Body)


class _Ctx:
    function_name = "justhodl-test-walker"


def test_chain_invoke_chains_under_cap():
    lam, s3 = _Lam(), _S3()
    r = chain_guard.chain_invoke({"chain_depth": 4}, event={"chain_depth": 3, chain_guard.HOP_KEY: 3},
                                 context=_Ctx(), lam=lam, s3=s3, max_hops=12)
    assert r["chained"] is True and r["hop"] == 4, r
    payload = json.loads(lam.calls[0]["Payload"].decode())
    assert payload["chain_depth"] == 4 and payload[chain_guard.HOP_KEY] == 4
    assert lam.calls[0]["FunctionName"] == "justhodl-test-walker"
    assert lam.calls[0]["InvocationType"] == "Event"
    assert not s3.objects


def test_chain_invoke_parks_at_cap_and_resets_hop():
    lam, s3 = _Lam(), _S3()
    r = chain_guard.chain_invoke({"chain_depth": 13}, event={chain_guard.HOP_KEY: 12},
                                 context=_Ctx(), lam=lam, s3=s3, max_hops=12)
    assert r["parked"] is True and r["chained"] is False, r
    assert not lam.calls, "must NOT self-invoke at the cap"
    key, doc = next(iter(s3.objects.items()))
    assert key.startswith(chain_guard.PARK_PREFIX + "justhodl-test-walker/")
    assert doc["payload"][chain_guard.HOP_KEY] == 0 and doc["payload"]["chain_depth"] == 13
    assert doc["hops_walked"] == 12 and doc["function"] == "justhodl-test-walker"


def test_cap_is_under_aws_limit():
    assert chain_guard.MAX_HOPS < chain_guard.AWS_HARD_LIMIT - 2, "leave headroom for an upstream dispatcher hop"


def test_begin_stash_path():
    lam, s3 = _Lam(), _S3()
    chain_guard.begin({chain_guard.HOP_KEY: 5})
    r = chain_guard.chain_invoke({"phase": "x"}, context=_Ctx(), lam=lam, s3=s3)
    assert r["hop"] == 6 and json.loads(lam.calls[0]["Payload"].decode())[chain_guard.HOP_KEY] == 6


def test_fresh_event_starts_at_zero():
    lam, s3 = _Lam(), _S3()
    r = chain_guard.chain_invoke({"phase": "x"}, event={"phase": "start"}, context=_Ctx(), lam=lam, s3=s3)
    assert r["hop"] == 1


def test_no_unguarded_self_chain_in_fleet():
    """Every self Event-invoke must go through chain_guard, be a listed fan-out, or carry its own cap."""
    self_pat = re.compile(r"(context\.function_name|ctx\.function_name|AWS_LAMBDA_FUNCTION_NAME|FunctionName=SELF_FN)")
    offenders = []
    for src in sorted((ROOT / "aws" / "lambdas").glob("*/source/lambda_function.py")):
        fn = src.parent.parent.name
        code = src.read_text(errors="ignore")
        if "InvocationType=\"Event\"" not in code and "InvocationType='Event'" not in code:
            continue
        tree = ast.parse(code)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "invoke"):
                continue
            kws = {k.arg: k.value for k in node.keywords}
            it = kws.get("InvocationType")
            if not (isinstance(it, ast.Constant) and it.value == "Event"):
                continue
            seg = ast.get_source_segment(code, kws.get("FunctionName")) or ""
            self_names = set(re.findall(r"^\s*(\w+)\s*=\s*[^#\n]*(?:AWS_LAMBDA_FUNCTION_NAME|context\.function_name|ctx\.function_name)",
                                        code, re.M))
            if not (self_pat.search(seg) or ("\"%s\"" % fn) in seg or seg.strip() in self_names):
                continue
            if fn in FANOUT_ALLOW or fn in OWN_CAP_ALLOW:
                continue
            offenders.append("%s:L%d" % (fn, node.lineno))
    assert not offenders, "unguarded self-chain (use chain_guard.chain_invoke): %s" % offenders


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok  ", name)
    print("chain_guard tests passed")
