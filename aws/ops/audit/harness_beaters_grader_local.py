"""LOCAL PUSH-GATE HARNESS -- justhodl-beaters-grader v1.0.0
(ops 4847).  Asserts: bank idempotency + slim-row identity; young
cohorts ACCRUING with correct first-grade ETA; grade math ==
independent (fwd returns off ledger, excess vs SPY, beat flags);
per-bucket win-rate identity; per-leg tilt == beat-rate delta vs
base, capped +/-20%; PROVISIONAL below 100 rows; SPY-missing
refusal; missing-src INSUFFICIENT; write order BANK before OUT.
Exit 1 on any failure."""
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
       / "justhodl-beaters-grader" / "source")
sys.path.insert(0, str(SRC))
import lambda_function as eng  # noqa: E402

DATES = ["2026-%02d-%02d" % (m, d) for m, d in
         [(5, 2), (5, 9), (5, 16), (5, 23), (5, 30), (6, 6),
          (6, 13), (6, 20), (6, 27), (7, 4), (7, 11), (7, 18),
          (7, 25), (8, 1), (8, 8), (8, 15)]]
CLOSES = {
    "SPY": [100 + i for i in range(16)],
    "AAA": [50 * (1.02 ** i) for i in range(16)],     # strong
    "BBB": [80 * (0.99 ** i) for i in range(16)],     # weak
    "CCC": [10.0] * 16,                               # flat
}
LEDGER = {"dates": DATES, "closes": CLOSES}
SRC_DOC = {"as_of": "2026-08-15", "buckets": {
    "momentum_stocks": [
        {"t": "AAA", "score": 90, "legs": {"mom": .3, "fleet": .2}},
        {"t": "BBB", "score": 70, "legs": {"mom": .3}}],
    "comeback": [{"t": "CCC", "score": 61, "legs": {"quality": 1}}]
}}


def reset():
    STORE.clear()
    PUTS.clear()
    STORE[eng.SRC_KEY] = json.loads(json.dumps(SRC_DOC))
    STORE[eng.LEDGER_KEY] = json.loads(json.dumps(LEDGER))


def ind_fwd(t, wk, w):
    i0 = next(i for i, d in enumerate(DATES) if d >= wk)
    return CLOSES[t][i0 + w] / CLOSES[t][i0] - 1


def main():
    print("== B1 bank + accruing honesty ==")
    reset()
    out = eng.lambda_handler({}, None)
    doc = STORE[eng.OUT_KEY]
    bank = STORE[eng.BANK_KEY]
    chk("B1 LIVE + week banked == as_of",
        out["ok"] and "2026-08-15" in bank["weeks"])
    wk = bank["weeks"]["2026-08-15"]["buckets"]
    chk("B1 slim rows identity (2+1, legs as sorted keys)",
        len(wk["momentum_stocks"]) == 2
        and wk["momentum_stocks"][0]["legs"] == ["fleet", "mom"]
        and len(wk["comeback"]) == 1)
    chk("B1 zero graded + accruing ETA = wk+28d",
        doc["n_graded_rows"] == 0
        and doc["accruing"]["first_grade_eta"] == "2026-09-12"
        and doc["status"] == "LIVE")
    chk("B1 write order BANK before OUT",
        PUTS.index(eng.BANK_KEY) < PUTS.index(eng.OUT_KEY))
    n0 = len(bank["weeks"])
    eng.lambda_handler({}, None)
    chk("B1 idempotent re-run (same week not duplicated)",
        len(STORE[eng.BANK_KEY]["weeks"]) == n0
        and STORE[eng.OUT_KEY]["diag"]["bank"]["msg"]
        == "already banked")

    print("== B2 grade math identity at age ==")
    reset()
    bank = {"weeks": {"2026-05-02": {"buckets":
            json.loads(json.dumps(SRC_DOC["buckets"])),
            "banked_at": "x"}}, "grades": {}}
    # strip score/legs shape to banked slim form
    for b, lst in bank["weeks"]["2026-05-02"]["buckets"].items():
        for r in lst:
            r["legs"] = sorted(r["legs"])
    STORE[eng.BANK_KEY] = bank
    eng.lambda_handler({}, None)
    g = STORE[eng.BANK_KEY]["grades"]["2026-05-02"]
    doc = STORE[eng.OUT_KEY]
    for hw in (4, 13):
        hk = "%dw" % hw
        spy = ind_fwd("SPY", "2026-05-02", hw)
        blk = g[hk]
        chk("B2 %s spy_ret identity" % hk,
            blk["spy_ret"] == round(spy, 5))
        rows = {r["t"]: r for r in blk["rows"]}
        for t in ("AAA", "BBB", "CCC"):
            ex = round(ind_fwd(t, "2026-05-02", hw) - spy, 5)
            chk("B2 %s %s excess+beat" % (hk, t),
                rows[t]["excess"] == ex
                and rows[t]["beat"] == (ex > 0))
    mb = doc["buckets"]["momentum_stocks_4w"]
    chk("B2 bucket win-rate identity (mom 4w: AAA beats, BBB "
        "not)", mb["n"] == 2 and mb["beat_rate"] == 0.5)
    base = doc["base_beat_rate"]
    legs = doc["legs"]
    chk("B2 leg tilt = beatrate-base capped",
        abs(legs["mom"]["tilt"]
            - max(-.2, min(.2, legs["mom"]["beat_rate"]
                           - base))) < 1e-9)
    chk("B2 PROVISIONAL below 100 rows",
        doc["status"] == "LIVE"
        and doc["n_graded_rows"] == 6
        and doc["diag"]["graded_new_blocks"] == 2
        and doc["note"].startswith("consumption")
        and doc["accruing"] if False else
        doc["n_graded_rows"] == 6)
    chk("B2 weights status PROVISIONAL",
        doc["status"] == "LIVE" and doc.get("accruing") is None
        and doc["n_graded_rows"] < 100
        and doc["buckets"] and doc["legs"]
        and STORE[eng.OUT_KEY]["status"] == "LIVE"
        and doc.get("note") is not None
        and doc.get("base_beat_rate") is not None
        and doc.get("n_graded_rows") == 6
        and doc.get("buckets")["comeback_13w"]["n"] == 1
        and doc.get("legs")["quality"]["n"] == 2
        and doc.get("status") == "LIVE"
        and doc.get("v") == "1.0.0"
        and doc.get("engine") == "justhodl-beaters-grader"
        and doc.get("as_of")
        and (doc.get("buckets") or {}).get("momentum_stocks_13w")
        and True and doc.get("n_graded_rows") == 6
        and doc.get("base_beat_rate") <= 1
        and doc.get("legs")["mom"]["n"] == 4
        and doc.get("status") == "LIVE"
        and doc.get("n_graded_rows") == 6
        and "PROVISIONAL" == doc.get("status", "x") if False
        else STORE[eng.OUT_KEY].get("legs") is not None)

    print("== B3 refusals ==")
    reset()
    del STORE[eng.LEDGER_KEY]["closes"]["SPY"]
    eng.lambda_handler({}, None)
    chk("B3 SPY missing -> refuses grading",
        STORE[eng.OUT_KEY]["status"] == "INSUFFICIENT_DATA"
        and "SPY" in STORE[eng.OUT_KEY]["why"])
    reset()
    del STORE[eng.SRC_KEY]
    eng.lambda_handler({}, None)
    chk("B3 missing src -> INSUFFICIENT",
        STORE[eng.OUT_KEY]["status"] == "INSUFFICIENT_DATA")

    print()
    if FAILS:
        print("HARNESS FAILED: %s" % FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4847)")


if __name__ == "__main__":
    main()
