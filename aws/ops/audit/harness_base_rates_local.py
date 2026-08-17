"""LOCAL PUSH-GATE HARNESS -- justhodl-base-rates v1.0.0 (ops 4818).

Runs in the sandbox with boto3 stubbed via sys.modules before the
engine imports.  Ground truth comes from two independent paths:
(1) ORACLE: a verbatim copy of spx-beaters v1.2 base_rates() -- the
    engine's 26w s=0 quintiles/thresholds/comeback must be identical.
(2) BRUTE: a from-scratch reimplementation of the 4w rolling-formation
    pooling -- n_obs, n_formations and quintile beat rates must match.
Plus behavior gates: the beaters ledger is NEVER written, the honest
INSUFFICIENT_DATA path fires on a shallow ledger, quintile mapping is
byte-identical to the league's quintile_odds, bucket normalization
matches the scan (mega->large, nano->micro, mcap fallback), dd-band
edges are pinned, and Wilson LB <= beat rate everywhere.
Exit 1 on any failure: this is the mandatory gate before push.
"""
import json
import sys
import types
from pathlib import Path

FAILS = []


def chk(name, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    print("  [%s] %s %s" % (tag, name, detail))
    if not cond:
        FAILS.append(name)


# ------------------------------------------------- boto3 stub -------
STORE = {}
PUTS = []


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
       / "justhodl-base-rates" / "source")
sys.path.insert(0, str(SRC))
import lambda_function as eng  # noqa: E402


# --------------------------------------------- deterministic fixture -
def lcg(seed):
    s = seed

    def nxt():
        nonlocal s
        s = (1103515245 * s + 12345) % (2 ** 31)
        return s / (2 ** 31) - 0.5
    return nxt


def make_fixture():
    dates = ["2025-W%02d" % i for i in range(53)]
    closes = {}
    rnd = lcg(20260817)
    spy = [100.0]
    for _ in range(52):
        spy.append(round(spy[-1] * (1 + 0.0015 + 0.002 * rnd()), 4))
    closes["SPY"] = spy
    for i in range(200):                       # momentum-diverse
        base = -0.008 + 0.020 * (i / 199.0)
        p = [50.0 + i]
        for _ in range(52):
            p.append(round(p[-1] * (1 + base + 0.01 * rnd()), 4))
        closes["N%03d" % i] = p
    for i in range(45):                        # comeback cohort
        p = [100.0]
        for w in range(1, 15):
            p.append(round(p[-1] * 1.02, 4))
        for w in range(15, 23):
            p.append(round(p[-1] * 0.88, 4))
        for w in range(23, 27):
            p.append(round(p[-1] * 1.001, 4))
        for w in range(27, 53):
            p.append(round(p[-1] * (1 + 0.004 + 0.008 * rnd()), 4))
        closes["C%03d" % i] = p
    for i in range(20):                        # short history
        closes["S%03d" % i] = [10.0 + 0.1 * w for w in range(10)]
    for i in range(15):                        # gappy
        p = []
        v = 30.0 + i
        for w in range(53):
            if w % 9 == 4:
                p.append(None)
            else:
                v = round(v * (1 + 0.001 + 0.006 * rnd()), 4)
                p.append(v)
        closes["G%03d" % i] = p
    led = {"dates": dates, "closes": closes}
    stocks = []
    for i in range(60):
        stocks.append({"ticker": "N%03d" % i, "cap_bucket": "mega"
                       if i < 5 else "large" if i < 20 else "nano"
                       if i < 25 else "mid"})
    for i in range(60, 120):
        stocks.append({"ticker": "N%03d" % i,
                       "market_cap": 5e9 if i < 80 else 5e8
                       if i < 100 else 5e7})
    uni = {"stocks": stocks}
    return led, uni


# ------------------------------------- ORACLE: verbatim v1.2 copy ----
def oracle_base_rates(led, spy_arr, meta):
    if not spy_arr or len(spy_arr) < 53:
        return None, {}
    spy_out = spy_arr[-1] / spy_arr[-27] - 1
    rows = []
    for t in meta:
        arr = [v for v in (led["closes"].get(t) or []) if v]
        if len(arr) < 53:
            continue
        form = arr[-27] / arr[-53] - 1
        out = arr[-1] / arr[-27] - 1
        dd_form = None
        peak = arr[0]
        for v in arr[:-26]:
            peak = max(peak, v)
        if peak:
            dd_form = arr[-27] / peak - 1
        base4 = arr[-27] / arr[-31] - 1 if len(arr) >= 31 else None
        rows.append((t, form, out - spy_out, dd_form, base4))
    if len(rows) < 200:
        return None, {}
    rows.sort(key=lambda r: r[1])
    n = len(rows)
    quints = []
    for q in range(5):
        seg = rows[int(n * q / 5):int(n * (q + 1) / 5)]
        ex = sorted(x[2] for x in seg)
        beat = sum(1 for x in seg if x[2] > 0)
        quints.append({
            "q": q + 1,
            "form_6m_min_pct": round(seg[0][1] * 100, 1),
            "form_6m_max_pct": round(seg[-1][1] * 100, 1),
            "n": len(seg),
            "beat_spy_26w_pct": round(100 * beat / len(seg), 1),
            "median_excess_pp": round(ex[len(ex) // 2] * 100, 1)})
    cb = [x for x in rows if x[3] is not None and x[3] <= -0.30
          and x[4] is not None and x[4] >= -0.03]
    cb_stat = None
    if len(cb) >= 25:
        ex = sorted(x[2] for x in cb)
        cb_stat = {"n": len(cb),
                   "beat_spy_26w_pct": round(
                       100 * sum(1 for x in cb if x[2] > 0)
                       / len(cb), 1),
                   "median_excess_pp": round(ex[len(ex) // 2]
                                             * 100, 1)}
    th = [q["form_6m_max_pct"] / 100 for q in quints[:-1]]
    return th, {"momentum_quintiles": quints, "comeback_cohort":
                cb_stat}


# ------------------------------- BRUTE: independent 4w recompute -----
def brute_4w(led, spy):
    pooled = []
    n_forms = 0
    for s in range(23):
        ms = len(spy)
        si, fi = ms - 1 - s, ms - 1 - s - 4
        if fi - 26 < 0:
            break
        spy_out = spy[si] / spy[fi] - 1
        rows = []
        for t, raw in led["closes"].items():
            if t == "SPY":
                continue
            arr = [v for v in raw if v]
            i = len(arr) - 1 - 4 - s
            if i - 26 < 0:
                continue
            rows.append((arr[i] / arr[i - 26] - 1,
                         (arr[len(arr) - 1 - s] / arr[i] - 1)
                         - spy_out))
        if len(rows) < 200:
            continue
        rows.sort()
        n_forms += 1
        n = len(rows)
        for q in range(5):
            for r in rows[int(n * q / 5):int(n * (q + 1) / 5)]:
                pooled.append((q + 1, r[1]))
    stats = {}
    for qn in range(1, 6):
        ex = [e for q, e in pooled if q == qn]
        stats[qn] = round(100 * sum(1 for e in ex if e > 0)
                          / len(ex), 1)
    return len(pooled), n_forms, stats


def main():
    led, uni = make_fixture()
    STORE.clear()
    PUTS.clear()
    STORE[eng.LEDGER_KEY] = led
    STORE[eng.UNIVERSE_KEY] = uni

    print("== build() on 280-ticker synthetic ledger ==")
    doc = eng.build()
    chk("status LIVE", doc.get("status") == "LIVE")
    c26 = (doc.get("cohorts") or {}).get("26w") or {}

    print("== A1-A3 identity vs verbatim v1.2 oracle ==")
    spy = [v for v in led["closes"]["SPY"] if v]
    meta = {t: {} for t in led["closes"] if t != "SPY"}
    o_th, o_br = oracle_base_rates(led, spy, meta)
    mine = c26.get("s0_quintiles") or []
    oq = o_br["momentum_quintiles"]
    same = len(mine) == 5
    for a, b in zip(mine, oq):
        same = same and a["form_6m_min_pct"] == b["form_6m_min_pct"]
        same = same and a["form_6m_max_pct"] == b["form_6m_max_pct"]
        same = same and a["n"] == b["n"]
        same = same and a["beat_pct"] == b["beat_spy_26w_pct"]
        same = same and (a["median_excess_pp"]
                         == b["median_excess_pp"])
    chk("A1 26w s0 quintiles == oracle", same,
        json.dumps([q["beat_pct"] for q in mine]))
    chk("A2 thresholds == oracle",
        doc.get("quintile_thresholds_26w") == o_th)
    cb, ocb = doc.get("comeback_26w") or {}, o_br["comeback_cohort"]
    chk("A3 comeback == oracle",
        ocb is not None and cb.get("n") == ocb["n"]
        and cb.get("beat_pct") == ocb["beat_spy_26w_pct"]
        and cb.get("median_excess_pp") == ocb["median_excess_pp"],
        "n=%s" % cb.get("n"))

    print("== A4 brute 4w recompute ==")
    bn, bf, bq = brute_4w(led, spy)
    c4 = (doc["cohorts"].get("4w") or {})
    q4 = {q["q"]: q["beat_pct"] for q in c4.get("quintiles") or []}
    chk("A4 4w n_obs/n_forms/beats == brute",
        c4.get("n_obs") == bn and c4.get("n_formations") == bf
        and q4.get(1) == bq[1] and q4.get(5) == bq[5],
        "n=%s forms=%s" % (bn, bf))

    print("== A5 write discipline ==")
    chk("A5 ledger never written", eng.LEDGER_KEY not in PUTS)
    chk("A5 build wrote only history",
        PUTS == [eng.HIST_KEY], str(PUTS))
    out = eng.lambda_handler({}, None)
    chk("A5 handler wrote OUT_KEY", PUTS[-1] == eng.OUT_KEY
        and out["ok"] is True)

    print("== A6 honest empty path ==")
    STORE[eng.LEDGER_KEY] = {"dates": led["dates"][:10],
                             "closes": {"SPY": spy[:10]}}
    d2 = eng.build()
    chk("A6 shallow ledger -> INSUFFICIENT_DATA",
        d2.get("status") == "INSUFFICIENT_DATA"
        and "cohorts" not in d2)
    STORE[eng.LEDGER_KEY] = led

    print("== A7 assignment mapping + buckets ==")
    a = doc["current_assignments"]

    def qmap(t):
        arr = [v for v in led["closes"][t] if v]
        r6c = arr[-1] / arr[-27] - 1
        qi = 0
        for th in o_th:
            if r6c > th:
                qi += 1
        return qi + 1
    ok7 = all(a[t]["q"] == qmap(t)
              for t in ("N000", "N199", "C000", "G000"))
    chk("A7 q == league quintile_odds mapping", ok7)
    chk("A7 bucket normalization",
        a["N000"]["b"] == "large" and a["N020"]["b"] == "micro"
        and a["N060"]["b"] == "mid" and a["N150"]["b"] == "other")
    chk("A7 short-history excluded",
        "S000" not in a
        and doc["diag"]["excluded"]["short_history"] == 20)

    print("== A8 dd-band edges ==")
    chk("A8 edges pinned",
        eng.dd_band(-0.10) == "0_to_-10"
        and eng.dd_band(-0.100001) == "-10_to_-20"
        and eng.dd_band(-0.35) == "-20_to_-35"
        and eng.dd_band(-0.350001) == "below_-35")

    print("== A9/A10 wilson + field shapes ==")
    chk("A9 wilson_lb(50,100) ~= 40.4",
        abs(eng.wilson_lb(50, 100) - 40.4) < 0.15)
    ok = True
    for h, cell in doc["cohorts"].items():
        for q in cell.get("quintiles") or []:
            ok = ok and isinstance(q["beat_pct"], float)
            ok = ok and isinstance(q["n"], int)
            ok = ok and q["wilson_lb95_pct"] <= q["beat_pct"] + 1e-9
    chk("A10 every quintile cell numeric + LB<=beat", ok)

    print()
    if FAILS:
        print("HARNESS FAILED: %s" % FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4818)")


if __name__ == "__main__":
    main()
