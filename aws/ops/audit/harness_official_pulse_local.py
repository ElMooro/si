"""LOCAL PUSH-GATE HARNESS -- official-pulse v1.0.0 (ops 4864).
Seams: fetch_obs + search_weekly stubbed.  Asserts: metrics math
(latest/4w/13w identity, z window); custody resolver (stale
candidate skipped -> search winner bound; ALL stale -> OMITTED
with tried-list, never a stale bind); dollar_leg truth table
(0/1/2 firing -> CALM/WATCH/STRESS; unavailable custody excluded
from denominator); RRP dead -> INSUFFICIENT; banking + write
discipline.  Exit 1 on failure."""
import json
import sys
import types
from datetime import datetime, timedelta
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
       / "justhodl-official-pulse" / "source")
sys.path.insert(0, str(SRC))
import lambda_function as eng  # noqa: E402

eng.FRED_KEY = "FIXTURE"
eng.time.sleep = lambda *_: None

NOW = datetime(2026, 8, 17)
N = 620


def wk_rows(base, step, last_dt=NOW):
    out = []
    for i in range(N):
        d = last_dt - timedelta(weeks=N - 1 - i)
        out.append((d.date().isoformat(), base + i * step))
    return out

RRP_ROWS = wk_rows(200000.0, 260.0)          # musd, rising
CUR_ROWS = []
for _i in range(N):
    _d = NOW - timedelta(weeks=N - 1 - _i)
    CUR_ROWS.append((_d.date().isoformat(),
                     2900000.0 - 400.0 * _i
                     + 900.0 * ((_i * 37) % 29) / 29.0))
STALE_ROWS = wk_rows(2900000.0, 100.0,
                     last_dt=datetime(2012, 11, 7))
MODE = {"rrp_dead": False, "cust": "search"}


def fake_fetch(sid):
    if sid == eng.RRP_SID:
        if MODE["rrp_dead"]:
            return None, "fetch_error:boom"
        return list(RRP_ROWS), None
    if sid in eng.CUSTODY_CANDIDATES:
        return list(STALE_ROWS), None
    if sid == "RESPPXCUST":
        if MODE["cust"] == "search":
            return list(CUR_ROWS), None
        return list(STALE_ROWS), None
    return None, "fetch_error:404"


def fake_search(text):
    return ["RESPPXCUST"]


eng.fetch_obs = fake_fetch
eng.search_weekly = fake_search


def seed(ff=True):
    STORE.clear()
    PUTS.clear()
    if ff:
        STORE[eng.FF_KEY] = {
            "generated_at": "2026-08-17T21:45:00",
            "holder_splits": {"lt_total": {
                "official": {"z_10y": -1.4}}},
            "signals": {"safe_haven": {"z_10y": -1.95}}}


def main():
    print("== P1 metrics + resolver-through-search ==")
    seed()
    out = eng.lambda_handler({}, None)
    doc = STORE[eng.OUT_KEY]
    r = doc["foreign_rrp"]
    exp_latest = round(RRP_ROWS[-1][1] / 1000.0, 1)
    exp_13 = round((RRP_ROWS[-1][1] - RRP_ROWS[-14][1])
                   / 1000.0, 1)
    chk("P1 RRP latest + 13w identity",
        out["ok"] and r["latest_bn"] == exp_latest
        and r["chg_13w_bn"] == exp_13
        and r["latest_date"] == RRP_ROWS[-1][0])
    chk("P1 custody: stale candidates SKIPPED, search winner "
        "bound", doc["custody"]["status"] == "LIVE"
        and doc["custody"]["id"] == "RESPPXCUST"
        and doc["custody"]["chg_13w_bn"] == round(
            (CUR_ROWS[-1][1] - CUR_ROWS[-14][1]) / 1000.0, 1))
    chk("P1 banks written for both",
        eng.BANK_PREFIX + "WLRRAFOIAL.json" in STORE
        and eng.BANK_PREFIX + "RESPPXCUST.json" in STORE)

    print("== P2 dollar leg truth table ==")
    dl = doc["dollar_leg"]
    chk("P2 STRESS at >=2 firing (official -1.4, safe-haven "
        "-1.95, custody drain)",
        dl["status"] == "STRESS" and dl["legs_firing"] >= 2
        and dl["legs"]["safe_haven_monthly"]["fires"] is True)
    seed()
    STORE[eng.FF_KEY]["holder_splits"]["lt_total"]["official"][
        "z_10y"] = 0.5
    STORE[eng.FF_KEY]["signals"]["safe_haven"]["z_10y"] = -1.6
    MODE["cust"] = "stale"
    eng.lambda_handler({}, None)
    dl = STORE[eng.OUT_KEY]["dollar_leg"]
    chk("P2 custody OMITTED -> excluded from denominator; one "
        "firing -> WATCH",
        STORE[eng.OUT_KEY]["custody"]["status"] == "OMITTED"
        and "tried" in STORE[eng.OUT_KEY]["custody"]["why"]
        and dl["available"] == 2 and dl["status"] == "WATCH")
    MODE["cust"] = "search"
    seed(ff=False)
    eng.lambda_handler({}, None)
    dl = STORE[eng.OUT_KEY]["dollar_leg"]
    chk("P2 no foreign-flows doc -> monthly legs unavailable, "
        "CALM with available count honest",
        dl["available"] == 1 and dl["status"] == "CALM")

    print("== P3 honesty + discipline ==")
    seed()
    MODE["rrp_dead"] = True
    eng.lambda_handler({}, None)
    chk("P3 RRP dead -> INSUFFICIENT",
        STORE[eng.OUT_KEY]["status"] == "INSUFFICIENT_DATA")
    MODE["rrp_dead"] = False
    seed()
    PUTS.clear()
    eng.lambda_handler({}, None)
    chk("P3 writes: banks + OUT only, OUT last",
        set(PUTS) <= {eng.OUT_KEY,
                      eng.BANK_PREFIX + "WLRRAFOIAL.json",
                      eng.BANK_PREFIX + "RESPPXCUST.json"}
        and PUTS[-1] == eng.OUT_KEY)

    print()
    if FAILS:
        print("HARNESS FAILED: %s" % FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4864)")


if __name__ == "__main__":
    main()
