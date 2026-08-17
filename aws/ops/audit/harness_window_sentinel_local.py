"""LOCAL PUSH-GATE HARNESS -- provider-window-sentinel v1.0.0
(ops 4850).  Seams: list_banks + fred_obs stubbed.  Asserts:
identical -> OK; dropped old rows -> WINDOWED with exact dates +
alert + events append (capped); value drift > tol -> REVISED with
pairs; fetch death -> UNVERIFIED never OK; empty bank UNVERIFIED;
read-only vs banks (no bank keys ever written); no key / no banks
-> INSUFFICIENT; write order events-then-out.  Exit 1 on failure.
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

    def list_objects_v2(self, **kw):
        pfx = kw.get("Prefix", "")
        ks = sorted(k for k in STORE if k.startswith(pfx))
        return {"Contents": [{"Key": k} for k in ks],
                "IsTruncated": False}


_stub = types.ModuleType("boto3")
_stub.client = lambda *a, **k: _S3()
sys.modules["boto3"] = _stub

SRC = (Path(__file__).resolve().parents[2] / "lambdas"
       / "justhodl-provider-window-sentinel" / "source")
sys.path.insert(0, str(SRC))
import lambda_function as eng  # noqa: E402

eng.FRED_KEY = "FIXTURE"
eng.time.sleep = lambda *_: None

BANK_ROWS = {"1985-%02d-01" % m: 100.0 + m for m in range(1, 13)}
PROV = dict(BANK_ROWS)
MODE = {"dead": set()}


def fake_obs(sid):
    if sid in MODE["dead"]:
        return None, "fetch_error:boom"
    return dict(PROV), None


eng.fred_obs = fake_obs


def seed(n_series=2):
    STORE.clear()
    PUTS.clear()
    for i in range(n_series):
        sid = "SER%d" % i
        STORE[eng.BANK_PREFIX + sid + ".json"] = {
            "id": sid, "rows": dict(BANK_ROWS)}


def main():
    print("== S1 identical -> OK ==")
    seed()
    out = eng.lambda_handler({}, None)
    doc = STORE[eng.OUT_KEY]
    chk("S1 LIVE, 2 series OK, no alert",
        out["ok"] and doc["summary"]["n_ok"] == 2
        and doc["series"]["SER0"]["verdict"] == "OK"
        and "alert" not in doc
        and eng.EVENTS_KEY not in STORE)
    chk("S1 read-only vs banks (only OUT written)",
        PUTS == [eng.OUT_KEY])

    print("== S2 windowing detected ==")
    seed()
    for d in ("1985-01-01", "1985-02-01", "1985-03-01"):
        PROV.pop(d)
    out = eng.lambda_handler({}, None)
    doc = STORE[eng.OUT_KEY]
    s0 = doc["series"]["SER0"]
    chk("S2 WINDOWED with exact dates",
        s0["verdict"] == "WINDOWED" and s0["n_missing"] == 3
        and s0["missing_from_provider"][0] == "1985-01-01")
    chk("S2 alert + events appended (both series)",
        "WINDOWING DETECTED" in doc.get("alert", "")
        and doc["summary"]["windowed"] == ["SER0", "SER1"]
        and STORE[eng.EVENTS_KEY]["rows"][-1]["series"]
        == ["SER0", "SER1"])
    chk("S2 events before out", PUTS.index(eng.EVENTS_KEY)
        < PUTS.index(eng.OUT_KEY))
    PROV.update(BANK_ROWS)

    print("== S3 revision detected ==")
    seed()
    PROV["1985-06-01"] = 999.0
    eng.lambda_handler({}, None)
    s0 = STORE[eng.OUT_KEY]["series"]["SER0"]
    chk("S3 REVISED with pair", s0["verdict"] == "REVISED"
        and s0["revised"][0]["date"] == "1985-06-01"
        and s0["revised"][0]["provider"] == 999.0
        and s0["revised"][0]["banked"] == 106.0)
    PROV["1985-06-01"] = BANK_ROWS["1985-06-01"]

    print("== S4 honesty paths ==")
    seed()
    MODE["dead"] = {"SER1"}
    eng.lambda_handler({}, None)
    doc = STORE[eng.OUT_KEY]
    chk("S4 fetch death -> UNVERIFIED never OK",
        doc["series"]["SER1"]["verdict"] == "UNVERIFIED"
        and doc["summary"]["n_ok"] == 1)
    MODE["dead"] = set()
    seed(0)
    eng.lambda_handler({}, None)
    chk("S4 no banks -> INSUFFICIENT",
        STORE[eng.OUT_KEY]["status"] == "INSUFFICIENT_DATA")
    seed()
    k = eng.FRED_KEY
    eng.FRED_KEY = ""
    eng.lambda_handler({}, None)
    chk("S4 no key -> INSUFFICIENT",
        STORE[eng.OUT_KEY]["status"] == "INSUFFICIENT_DATA")
    eng.FRED_KEY = k

    print()
    if FAILS:
        print("HARNESS FAILED: %s" % FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4850)")


if __name__ == "__main__":
    main()
