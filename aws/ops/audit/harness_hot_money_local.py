"""LOCAL PUSH-GATE HARNESS -- justhodl-hot-money v1.0.0 (ops
4855).  CRITICAL asserts: ledger TAKEOVER continuity -- a
pre-seeded 62-row ledger is UNION-appended, never shrunk; sums off
the FULL ledger; day-one-no-ledger honesty; backfill cap; today
fetch-fail keeps history; korea deferral; write discipline."""
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
       / "justhodl-hot-money" / "source")
sys.path.insert(0, str(SRC))
import lambda_function as eng  # noqa: E402

eng.BACKFILL_SLEEP = 0.0
LED_DAYS = ["202606%02d" % d for d in range(1, 31)] \
    + ["202607%02d" % d for d in range(1, 32)] + ["20260801"]
TODAY = ("20260817", 45.45e9)
MODE = {"today_dead": False}


def fake_twse(day=None):
    if day is None:
        if MODE["today_dead"]:
            return None, "stat=NO"
        return TODAY
    return None, "stat=NO"          # backfill days all "holiday"


eng.twse_fetch = fake_twse


def seed():
    STORE.clear()
    PUTS.clear()
    STORE[eng.TWSE_LEDGER] = {"source": "x", "rows": {
        d: 1e9 * (i - 30) for i, d in enumerate(LED_DAYS)}}


def main():
    print("== H1 ledger takeover continuity ==")
    seed()
    out = eng.lambda_handler({}, None)
    tw = STORE[eng.OUT_KEY]["countries"]["taiwan"]
    led = STORE[eng.TWSE_LEDGER]["rows"]
    chk("H1 62 pre-seeded + today = 63, nothing lost",
        out["ok"] and len(led) == 63
        and all(d in led for d in LED_DAYS)
        and led["20260817"] == TODAY[1])
    nets = [led[d] for d in sorted(led)]
    chk("H1 sums off FULL ledger",
        tw["sum_5d_bn"] == round(sum(nets[-5:]) / 1e9, 2)
        and tw["sum_60d_bn"] == round(sum(nets[-60:]) / 1e9, 2)
        and tw["latest_bn"] == round(TODAY[1] / 1e9, 2))
    chk("H1 z present at n>=24", tw["z_60d"] is not None)
    chk("H1 korea deferral named",
        "korea" in STORE[eng.OUT_KEY]["deferred"])

    print("== H2 today-dead keeps history ==")
    seed()
    MODE["today_dead"] = True
    eng.lambda_handler({}, None)
    tw = STORE[eng.OUT_KEY]["countries"]["taiwan"]
    chk("H2 fetch fail -> LIVE off ledger, why recorded, no "
        "ledger write",
        tw["status"] == "LIVE" and tw["ledger_days"] == 62
        and "stat=NO" in tw.get("today_fetch", "")
        and eng.TWSE_LEDGER not in PUTS)
    MODE["today_dead"] = False

    print("== H3 backfill cap + day-one honesty ==")
    seed()
    old = eng.BACKFILL_CAP
    eng.BACKFILL_CAP = 5
    eng.lambda_handler({"twse_backfill_days": 30}, None)
    tw = STORE[eng.OUT_KEY]["countries"]["taiwan"]
    chk("H3 cap=5 attempts honored",
        tw.get("backfill_attempts") == 5)
    eng.BACKFILL_CAP = old
    STORE.clear()
    PUTS.clear()
    MODE["today_dead"] = True
    eng.lambda_handler({}, None)
    chk("H3 no ledger + dead feed -> INSUFFICIENT",
        STORE[eng.OUT_KEY]["status"] == "INSUFFICIENT_DATA")
    MODE["today_dead"] = False

    print("== H4 write discipline ==")
    seed()
    PUTS.clear()
    eng.lambda_handler({}, None)
    chk("H4 only ledger + out written; out last",
        set(PUTS) <= {eng.TWSE_LEDGER, eng.OUT_KEY}
        and PUTS[-1] == eng.OUT_KEY)

    print()
    if FAILS:
        print("HARNESS FAILED: %s" % FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4855)")


if __name__ == "__main__":
    main()
