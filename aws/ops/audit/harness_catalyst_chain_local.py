"""LOCAL PUSH-GATE HARNESS -- catalyst-chain v1.0.0 (ops 4853).
Fixtures for all four feeds.  Asserts: first-order chain (self,
conf 1.0); second-order ev = sum of source catalyst scores,
fallback max_score/20; S3 metric hierarchy (rpo_qoq>0 beats
deferred) + no-coverage NAMED gap (None, counted); S4 UP -> score 0
+ completed only; DOWN -> 1.2 boost; UNKNOWN flagged; stage calc;
score identity vs independent recompute; unpriced/completed
disjoint + unpriced needs stage>=2 & score>0; spine missing ->
INSUFFICIENT; stale stamping; write discipline.  Exit 1 on fail.
"""
import json
import sys
import types
from pathlib import Path

FAILS = []


def chk(name, cond, detail=""):
    print("  [%s] %s %s" % ("PASS" if cond else "FAIL", name,
                            detail))
    if not cond:
        FAILS.append(name)


STORE, PUTS = {}, []


class _Body:
    def __init__(self, b):
        self._b = b

    def read(self):
        return self._b


class _S3:
    def get_object(self, Bucket, Key):
        if Key not in STORE:
            raise KeyError(Key)
        return {"Body": _Body(json.dumps(STORE[Key]).encode())}

    def put_object(self, Bucket, Key, Body, **kw):
        PUTS.append(Key)
        STORE[Key] = json.loads(Body)


_stub = types.ModuleType("boto3")
_stub.client = lambda *a, **k: _S3()
sys.modules["boto3"] = _stub

SRC = (Path(__file__).resolve().parents[2] / "lambdas"
       / "justhodl-catalyst-chain" / "source")
sys.path.insert(0, str(SRC))
import lambda_function as eng  # noqa: E402

NOW = "2026-08-17T19:00:00"
CAT = {"generated_at": NOW, "by_ticker": {
    "SRC1": {"score": 2.5, "top_class": "MAJOR_CONTRACT",
             "catalysts": [{"class": "MAJOR_CONTRACT"}]},
    "SRC2": {"score": 1.5, "top_class": "GUIDE_UP",
             "catalysts": [{"class": "GUIDE_UP"}]}}}
RT = {"generated_at": NOW,
      "industry_boom_context": {"Industrials": 81.0},
      "beneficiaries": [
          {"ticker": "BENE", "tier": "T2_SUPPLIER",
           "edge_confidence": 0.8, "why": "named supplier"},
          {"ticker": "BENE", "tier": "T5_COMPETITOR_VALIDATION",
           "edge_confidence": 0.45, "why": "peer"},
          {"ticker": "WEAK", "tier": "T5_COMPETITOR_VALIDATION",
           "edge_confidence": 0.4, "why": "peer"},
          {"ticker": "GHOST", "tier": "T3_SUPPLIER",
           "edge_confidence": 0.6, "why": "supplier"}],
      "by_beneficiary": [
          {"ticker": "BENE", "catalysts": ["SRC1", "SRC2"],
           "best_tier": "T2_SUPPLIER", "max_score": 44.0,
           "quadrant": "TWICE_UNPRICED",
           "implied_order_usd_total": 5e8},
          {"ticker": "WEAK", "catalysts": ["NOCAT"],
           "best_tier": "T5_COMPETITOR_VALIDATION",
           "max_score": 30.0, "quadrant": "X"},
          {"ticker": "GHOST", "catalysts": ["SRC1"],
           "best_tier": "T3_SUPPLIER", "max_score": 20.0,
           "quadrant": "Y"}]}
BL = {"generated_at": NOW, "by_ticker": {
    "SRC1": {"rpo_qoq": 6.1, "rpo_asof": "2026-06-30",
             "deferred_qoq": 2.0, "group": "Industrials"},
    "BENE": {"rpo_qoq": None, "deferred_accelerating": True,
             "deferred_qoq": 52.3},
    "WEAK": {"rpo_qoq": -3.0, "deferred_qoq": 1.0}}}
ER = {"generated_at": NOW, "direction_map": {
    "SRC1": "UP", "BENE": "FLAT", "WEAK": "DOWN"}}


def seed():
    STORE.clear()
    PUTS.clear()
    STORE[eng.K_CAT] = json.loads(json.dumps(CAT))
    STORE[eng.K_RT] = json.loads(json.dumps(RT))
    STORE[eng.K_BL] = json.loads(json.dumps(BL))
    STORE[eng.K_ER] = json.loads(json.dumps(ER))


def row(doc, t):
    return next(r for r in doc["chains"] if r["t"] == t)


def main():
    print("== C1 full build ==")
    seed()
    out = eng.lambda_handler({}, None)
    doc = STORE[eng.OUT_KEY]
    chk("C1 LIVE, subjects = 2 first + 3 second",
        out["ok"] and doc["diag"]["n_subjects"] == 5)

    print("== C2 first-order SRC1: confirmed + UP -> completed ==")
    r = row(doc, "SRC1")
    chk("C2 stage 4, RPO metric wins, score 0 (priced)",
        r["stage"] == 4 and "RPO qoq +6.1%" in r["s3_why"]
        and r["score"] == 0.0
        and r["score_if_unpriced"] == round(2.5 * 1.0 * 1.5, 3))
    chk("C2 in completed, NOT unpriced",
        any(x["t"] == "SRC1" for x in doc["completed"])
        and not any(x["t"] == "SRC1" for x in doc["unpriced"]))
    chk("C2 industry boom joined", r.get("industry_boom") == 81.0)

    print("== C3 second-order BENE: the alpha shape ==")
    r = row(doc, "BENE")
    ev = 2.5 + 1.5
    chk("C3 ev = sum of source scores, conf = MAX row (0.8)",
        r["s2_conf"] == 0.8 and r["s2_tier"] == "T2_SUPPLIER")
    chk("C3 stage 3 via deferred_accelerating",
        r["stage"] == 3 and "accelerating" in r["s3_why"])
    chk("C3 score identity FLAT",
        r["score"] == round(ev * 0.8 * 1.5 * 1.0, 3)
        and r["score"] == r["score_if_unpriced"])
    chk("C3 tops unpriced", doc["unpriced"][0]["t"] == "BENE"
        and r.get("rt_quadrant") == "TWICE_UNPRICED")

    print("== C4 WEAK: fallback ev + DOWN boost, no confirm ==")
    r = row(doc, "WEAK")
    ev_w = round(30.0 / 20.0, 3)
    chk("C4 fallback ev = max_score/20, stage 2, DOWN x1.2",
        r["stage"] == 2
        and r["score"] == round(ev_w * 0.4 * 1.0 * 1.2, 3)
        and "cutting INTO" in r["why"][-1])

    print("== C5 GHOST: no filing coverage = NAMED gap ==")
    r = row(doc, "GHOST")
    chk("C5 s3 None + gap why + counted",
        r["s3_confirmed"] is None
        and "no filing coverage" in r["s3_why"]
        and doc["diag"]["n_no_filing_coverage"] == 2)  # GHOST + SRC2
    chk("C5 s4 UNKNOWN flagged, still unpriced-eligible",
        r["s4_direction"] == "UNKNOWN"
        and any(x["t"] == "GHOST" for x in doc["unpriced"]))

    print("== C6 structure + honesty ==")
    up = {x["t"] for x in doc["unpriced"]}
    co = {x["t"] for x in doc["completed"]}
    chk("C6 unpriced/completed disjoint; unpriced stage>=2 "
        "score>0", not (up & co)
        and all(x["stage"] >= 2 and x["score_if_unpriced"] > 0
                for x in doc["unpriced"]))
    chk("C6 stage hist sums", sum(doc["diag"]["stage_hist"]
                                  .values()) == 5)
    seed()
    del STORE[eng.K_RT]
    eng.lambda_handler({}, None)
    chk("C6 spine missing -> INSUFFICIENT",
        STORE[eng.OUT_KEY]["status"] == "INSUFFICIENT_DATA")
    seed()
    STORE[eng.K_BL]["generated_at"] = "2026-08-10T00:00:00"
    eng.lambda_handler({}, None)
    chk("C6 stale input stamped",
        STORE[eng.OUT_KEY]["inputs"]["backlog"]["stale"] is True)
    chk("C6 write discipline (OUT only)",
        all(p == eng.OUT_KEY for p in PUTS))

    print()
    if FAILS:
        print("HARNESS FAILED: %s" % FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4853)")


if __name__ == "__main__":
    main()
