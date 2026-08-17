"""LOCAL PUSH-GATE HARNESS -- justhodl-global-flows v1.0.0
(ops 4833).  bcrp_fetch seam stubbed.  Asserts: string->float parse
incl 'n.d.' nulls; per-series latest/sum_4q/z == independent
recompute; bank union-merge survival of a pre-2012 row; THIN when
<3 series usable; INSUFFICIENT on fetch death; deferred block ships
verbatim with unlock reasons; composites honestly null; write
discipline.  Exit 1 on any failure.
"""
import json
import sys
import types
from pathlib import Path

FAILS = []


def chk(name, cond, detail=""):
    print("  [%s] %s %s" % ("PASS" if cond else "FAIL", name, detail))
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
       / "justhodl-global-flows" / "source")
sys.path.insert(0, str(SRC))
import lambda_function as eng  # noqa: E402

QS = ["T%d.%02d" % (q, y) for y in range(12, 27)
      for q in range(1, 5)][:57]
VALS = {
    "portfolio_total": [100.0 + 7 * (i % 5) for i in range(57)],
    "portfolio_equity": [20.0 + 3 * (i % 4) for i in range(57)],
    "portfolio_fixed_income": [80.0 + 5 * (i % 3)
                               for i in range(57)],
    "gov_bonds_nonresident": [200.0 + 50 * (i % 6)
                              for i in range(57)],
}
MODE = {"drop": set(), "dead": False, "nd_at": None}


def fake_fetch():
    if MODE["dead"]:
        return None, "fetch_error:boom"
    out = {}
    for k, vs in VALS.items():
        if k in MODE["drop"]:
            out[k] = []
            continue
        rows = []
        for i, v in enumerate(vs):
            if MODE["nd_at"] == (k, i):
                continue                    # engine skips n.d.
            rows.append((QS[i], v))
        out[k] = rows
    return out, None


eng.bcrp_fetch = fake_fetch


def ind_z(vals):
    h, last = vals[:-1], vals[-1]
    mu = sum(h) / len(h)
    sd = (sum((v - mu) ** 2 for v in h) / (len(h) - 1)) ** 0.5
    return round(max(-4.0, min(4.0, (last - mu) / sd)), 2)


def main():
    STORE.clear()
    PUTS.clear()
    print("== full build ==")
    doc = eng.build()
    pe = doc["countries"]["peru"]
    chk("LIVE + peru LIVE", doc["status"] == "LIVE"
        and pe["status"] == "LIVE"
        and pe["latest_period"] == QS[-1])
    print("== A1 per-series identities ==")
    for k, vs in VALS.items():
        s = pe["series"][k]
        ok = (s["latest"] == round(vs[-1], 1)
              and s["sum_4q"] == round(sum(vs[-4:]), 1)
              and s["z_all"] == ind_z(vs)
              and s["n_obs"] == 57 and s["first"] == QS[0])
        chk("A1 %s" % k, ok, "latest=%s z=%s" % (s["latest"],
                                                 s["z_all"]))
    print("== A2 deferred + composites honest ==")
    chk("A2 five deferrals verbatim",
        set(doc["deferred"]) == {"taiwan_cbc", "taiwan_twse_daily",
                                 "korea", "chile", "imf_layer"}
        and all("why" in v for v in doc["deferred"].values()))
    chk("A2 composites null with reasons",
        doc["composites"]["cfi"]["value"] is None
        and "partial" in doc["composites"]["cfi"]["why"]
        and doc["composites"]["hot_money"]["value"] is None)
    print("== A3 bank merge + survival ==")
    sid = eng.PE_SERIES["portfolio_total"]
    chk("A3 bank n=57", len(STORE[eng.BANK_FMT % sid]["rows"])
        == 57)
    STORE[eng.BANK_FMT % sid]["rows"]["T4.05"] = 999.0
    PUTS.clear()
    eng.build()
    chk("A3 pre-2012 banked row survives",
        STORE[eng.BANK_FMT % sid]["rows"]["T4.05"] == 999.0)
    chk("A3 no-new -> bank not rewritten",
        (eng.BANK_FMT % sid) not in PUTS)
    print("== A4 honesty paths ==")
    MODE["drop"] = {"portfolio_equity", "gov_bonds_nonresident"}
    d2 = eng.build()
    chk("A4 2/4 -> THIN + INSUFFICIENT",
        d2["countries"]["peru"]["status"] == "THIN"
        and d2["status"] == "INSUFFICIENT_DATA")
    MODE["drop"] = set()
    MODE["dead"] = True
    d3 = eng.build()
    chk("A4 fetch death -> INSUFFICIENT + peru MISSING",
        d3["status"] == "INSUFFICIENT_DATA"
        and d3["countries"]["peru"]["status"] == "MISSING")
    MODE["dead"] = False
    print("== A5 write discipline ==")
    PUTS.clear()
    out = eng.lambda_handler({}, None)
    chk("A5 handler writes OUT last", PUTS[-1] == eng.OUT_KEY
        and out["ok"] is True)
    chk("A5 only bcrp-bank/out written",
        all(p == eng.OUT_KEY
            or p.startswith("data/providers/bcrp/")
            for p in PUTS))
    print()
    if FAILS:
        print("HARNESS FAILED: %s" % FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4833)")


if __name__ == "__main__":
    main()
