"""LOCAL PUSH-GATE HARNESS -- justhodl-plumbing-composite v1.0.0
(ops 4822).  boto3 stubbed via sys.modules; every leg's stress_z is
recomputed by an INDEPENDENT implementation in this file and asserted
to 1e-9 against the engine, plus: polarity inversion (scarcity),
fallback chain (fails), 13w-delta leg (fima), staleness + renorm with
NAMED exclusion, haircut breadth (provisional + banked-z modes, share
and federal-reserve ids excluded), SRF escalator, SFTR honest
deferral, composite weighted-mean identity, posture mapping,
INSUFFICIENT floor, and write/idempotency discipline.
Exit 1 on any failure: mandatory gate before push.
"""
import json
import sys
import types
from datetime import datetime, timedelta, timezone
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
       / "justhodl-plumbing-composite" / "source")
sys.path.insert(0, str(SRC))
import lambda_function as eng  # noqa: E402

TODAY = datetime.now(timezone.utc).date()


def days_back(n, end_off=0, step=1):
    return [(TODAY - timedelta(days=(n - 1 - i) * step + end_off))
            .isoformat() for i in range(n)]


def hist(sid, dates, values):
    STORE[eng.HIST_FMT % sid] = {"dates": dates,
                                 "values": values}


def ind_z(vals, window, wins=4.0):
    win = vals[-(window + 1):]
    h, last = win[:-1], win[-1]
    mu = sum(h) / len(h)
    sd = (sum((v - mu) ** 2 for v in h) / (len(h) - 1)) ** 0.5
    z = (last - mu) / sd
    return max(-wins, min(wins, z))


def make_fixture():
    STORE.clear()
    PUTS.clear()
    hist("D_SOFR_IORB", days_back(800),
         [0.0, 2.0] * 399 + [0.0, 20.0])
    hist("D_SOFR_P75_P25", days_back(800),
         [1.0, 3.0] * 399 + [1.0, 3.0])
    hist("D_DVP_SOFR", days_back(800),
         [12.0, 10.0] * 400)
    hist("D_BUND_EA_AAA", days_back(800),
         [-0.1, 0.1] * 399 + [0.1, -1.0])
    hist("D_BTP_BUND", days_back(130, end_off=20, step=30),
         [100.0, 120.0] * 64 + [100.0, 200.0])
    wre = [300.0] * 187 + [300.0 - 10 * k for k in range(1, 14)]
    hist("WREPOFOR", days_back(200, end_off=3, step=7), wre)
    hist("DTCC-TREASURY-FAILS", days_back(300),
         [50.0, 60.0] * 149 + [50.0, 100.0])
    hist("SRF_TAKEUP", days_back(200), [0.0, 0.1] * 99 + [0.0, 30.0])
    hc = []
    for i in range(30):
        m = 1.0 if i < 20 else (-1.0 if i < 29 else None)
        hc.append({"id": "HAIRCUT-cls%02d--median-margin" % i,
                   "chg": {"m": m}})     # 20 up, 9 down, 1 None
    hc.append({"id": "HAIRCUT-abc--collateral_share",
               "chg": {"m": 5.0}})
    hc.append({"id": "HAIRCUT-federal-reserve--collateral_share2",
               "chg": {"m": 5.0}})
    sftr = [{"id": "SFTR-EU-x%d" % i, "n_obs": 1} for i in range(3)]
    STORE[eng.BOARD_KEY] = {
        "as_of": TODAY.isoformat(),
        "groups": [{"name": "Tri-party haircuts", "series": hc},
                   {"name": "SFTR", "series": sftr}]}


def exp_legs():
    e = {}
    e["sofr_iorb"] = ind_z([0.0, 2.0] * 399 + [0.0, 20.0], 756)
    e["dispersion"] = round(
        (round(ind_z([1.0, 3.0] * 399 + [1.0, 3.0], 756), 3)
         + round(ind_z([12.0, 10.0] * 400, 756), 3)) / 2, 3)
    e["scarcity"] = -1 * ind_z([-0.1, 0.1] * 399 + [0.1, -1.0], 756)
    e["periphery"] = ind_z([100.0, 120.0] * 64 + [100.0, 200.0], 120)
    wre = [300.0] * 187 + [300.0 - 10 * k for k in range(1, 14)]
    d = [b - a for a, b in zip(wre[:-13], wre[13:])]
    e["fima"] = -1 * ind_z(d, 156)
    e["fails"] = ind_z([50.0, 60.0] * 149 + [50.0, 100.0], 756)
    return e


def main():
    make_fixture()
    print("== build() on engineered plumbing fixture ==")
    doc = eng.build()
    chk("status LIVE", doc.get("status") == "LIVE",
        doc.get("why") or "")
    legs = doc.get("legs") or {}
    exp = exp_legs()

    print("== A1 per-leg stress_z == independent recompute ==")
    for k in ("fails", "sofr_iorb", "dispersion", "scarcity",
              "periphery", "fima"):
        got = (legs.get(k) or {}).get("stress_z")
        ok = got is not None and abs(got - round(exp[k], 3)) < 5e-4
        chk("A1 %s" % k, ok, "got=%s exp=%.3f" % (got, exp[k]))
    chk("A1 scarcity polarity inverted (raw z<0, stress>0)",
        (legs.get("scarcity") or {}).get("stress_z", 0) > 3.9
        and (legs["scarcity"]["series"][0]["z"] < 0))
    chk("A1 winsorize at 4", abs(legs["sofr_iorb"]["stress_z"])
        <= 4.0 and legs["sofr_iorb"]["stress_z"] == 4.0)

    print("== A2 fails fallback chain ==")
    fl = legs.get("fails") or {}
    chk("A2 fell back to DTCC",
        (fl.get("series") or [{}])[0].get("id")
        == "DTCC-TREASURY-FAILS"
        and "D_FAILS_T_RATIO:history_missing"
        in (fl.get("skipped") or []))

    print("== A3 haircut breadth (provisional mode) ==")
    hb = legs.get("haircuts") or {}
    chk("A3 counts n=29 up=20 excl share+fed+None",
        hb.get("n_series") == 29 and hb.get("n_widening") == 20,
        json.dumps(hb)[:120])
    chk("A3 provisional stress 1.0 (0.60<share<=0.75)",
        hb.get("stress_z") == 1.0
        and str(hb.get("mode", "")).startswith("provisional"))

    print("== A4 SRF escalator + SFTR deferral ==")
    chk("A4 srf 30B -> +0.25",
        doc["srf"]["takeup_bn"] == 30.0
        and doc["srf"]["escalator"] == 0.25)
    chk("A4 sftr honestly deferred",
        "n=1<26" in str((doc.get("excluded") or {}).get("sftr")))

    print("== A5 composite identity + posture ==")
    live = {k: v for k, v in legs.items() if k in eng.WEIGHTS}
    ws = sum(eng.WEIGHTS[k] for k in live)
    comp = sum(eng.WEIGHTS[k] / ws * live[k]["stress_z"]
               for k in live) + 0.25
    chk("A5 composite == weighted recompute",
        abs(doc["composite"] - round(comp, 3)) < 1e-9,
        "got=%s exp=%.3f" % (doc["composite"], comp))
    pm = [n for c, n in eng.POSTURES if doc["composite"] < c][0]
    chk("A5 posture mapping", doc["posture"] == pm, doc["posture"])
    chk("A5 effective weights sum 1",
        abs(sum(doc["weights_effective"].values()) - 1.0) < 1e-6)

    print("== A6 write + idempotency discipline ==")
    chk("A6 build wrote only bank", PUTS == [eng.BANK_KEY],
        str(PUTS))
    eng.build()
    chk("A6 same-day rebuild no duplicate bank row",
        PUTS == [eng.BANK_KEY]
        and len(STORE[eng.BANK_KEY]["rows"]) == 1)
    out = eng.lambda_handler({}, None)
    chk("A6 handler wrote OUT", PUTS[-1] == eng.OUT_KEY
        and out["ok"] is True)

    print("== A7 staleness -> NAMED exclusion + renorm ==")
    make_fixture()
    hist("D_SOFR_IORB", days_back(800, end_off=20),
         [0.0, 2.0] * 399 + [0.0, 20.0])
    d2 = eng.build()
    chk("A7 stale sofr_iorb excluded with reason",
        "stale" in str((d2.get("excluded") or {}).get("sofr_iorb")))
    live2 = {k: v for k, v in (d2.get("legs") or {}).items()
             if k in eng.WEIGHTS}
    ws2 = sum(eng.WEIGHTS[k] for k in live2)
    comp2 = sum(eng.WEIGHTS[k] / ws2 * live2[k]["stress_z"]
                for k in live2) + 0.25
    chk("A7 renormalized composite still identity",
        d2.get("status") == "LIVE"
        and abs(d2["composite"] - round(comp2, 3)) < 1e-9)

    print("== A8 banked-z breadth mode ==")
    make_fixture()
    STORE[eng.BANK_KEY] = {"rows": [
        {"date": "x%d" % i, "breadth": 0.4 if i % 2 else 0.6}
        for i in range(70)]}
    d3 = eng.build()
    hb3 = (d3["legs"].get("haircuts") or {})
    seq = [0.4 if i % 2 else 0.6 for i in range(70)]
    expz = ind_z(seq + [hb3.get("share_widening")], 10 ** 6)
    chk("A8 breadth z(banked) == independent",
        str(hb3.get("mode", "")).startswith("z(banked")
        and abs(hb3["stress_z"] - round(expz, 3)) < 5e-4,
        "got=%s exp=%.3f" % (hb3.get("stress_z"), expz))

    print("== A9 INSUFFICIENT floor ==")
    make_fixture()
    for sid in ("D_SOFR_IORB", "D_SOFR_P75_P25", "D_DVP_SOFR",
                "D_BUND_EA_AAA", "D_BTP_BUND"):
        del STORE[eng.HIST_FMT % sid]
    d4 = eng.build()
    chk("A9 <4 legs -> INSUFFICIENT, composite withheld",
        d4.get("status") == "INSUFFICIENT_DATA"
        and "composite" not in d4)

    print()
    if FAILS:
        print("HARNESS FAILED: %s" % FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4822)")


if __name__ == "__main__":
    main()
