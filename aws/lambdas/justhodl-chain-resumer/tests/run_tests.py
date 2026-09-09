"""Dependency-free tests for justhodl-chain-resumer (deploy gate)."""
import json
import os
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ["SNS_ARN"] = "arn:aws:sns:us-east-1:000:test"

import lambda_function as lf  # noqa: E402

FAILS = []


def check(cond, msg):
    (print("ok  ", msg) if cond else (FAILS.append(msg), print("FAIL", msg)))


class FakeBody:
    def __init__(self, b):
        self.b = b

    def read(self):
        return self.b


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": FakeBody(self.objects[Key])}

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def list_objects_v2(self, Bucket, Prefix, **kw):
        return {"Contents": [{"Key": k} for k in sorted(self.objects) if k.startswith(Prefix)], "IsTruncated": False}


class FakeLam:
    def __init__(self, status=202):
        self.calls, self.status = [], status

    def invoke(self, **kw):
        self.calls.append(kw)
        return {"StatusCode": self.status}


class FakeSns:
    def __init__(self):
        self.msgs = []

    def publish(self, **kw):
        self.msgs.append(kw)


def ticket(s3, fn, payload, digest="abc"):
    key = "data/_state/chain-parked/%s/%s.json" % (fn, digest)
    s3.put_object("b", key, json.dumps({"function": fn, "payload": payload, "hops_walked": 12,
                                        "parked_at": "2026-09-09T12:00:00+00:00"}).encode())
    return key


def test_resume_invokes_and_deletes():
    s3, lam, sns = FakeS3(), FakeLam(), FakeSns()
    lf.clients = lambda: (s3, lam, sns)
    key = ticket(s3, "justhodl-worldbank-full", {"chain_depth": 13, "_lineage_hop": 0})
    out = lf.lambda_handler({"mode": "resume"})
    check(len(lam.calls) == 1 and lam.calls[0]["FunctionName"] == "justhodl-worldbank-full", "engine invoked once")
    p = json.loads(lam.calls[0]["Payload"].decode())
    check(p["chain_depth"] == 13 and p["_lineage_hop"] == 0 and p["_resumed_from"] == key, "payload carried, hop reset to 0")
    check(lam.calls[0]["InvocationType"] == "Event", "async invoke")
    check(key not in s3.objects, "ticket deleted after accepted invoke")
    check(len(out["resumed"]) == 1 and not out["errors"], "ledger reports one resume, no errors")
    check("data/_state/chain-resumer/last-run.json" in s3.objects and "data/_state/chain-resumer/log.json" in s3.objects, "ledger written")


def test_failed_invoke_keeps_ticket():
    s3, lam, sns = FakeS3(), FakeLam(status=500), FakeSns()
    lf.clients = lambda: (s3, lam, sns)
    key = ticket(s3, "justhodl-imf-full", {"chain_depth": 2})
    out = lf.lambda_handler({})
    check(key in s3.objects and not out["resumed"], "ticket kept when invoke not accepted")


def test_refuses_non_fleet_function_and_malformed():
    s3, lam, sns = FakeS3(), FakeLam(), FakeSns()
    lf.clients = lambda: (s3, lam, sns)
    ticket(s3, "evil-function", {"x": 1})
    s3.put_object("b", "data/_state/chain-parked/justhodl-x/bad.json", b"not json")
    out = lf.lambda_handler({})
    check(not lam.calls, "non-fleet function never invoked")
    check(len(out["errors"]) == 2, "both refusals recorded")
    check("data/_state/chain-parked/justhodl-x/bad.json" not in s3.objects, "malformed ticket removed")


def test_circuit_breaker_holds_and_alerts():
    s3, lam, sns = FakeS3(), FakeLam(), FakeSns()
    lf.clients = lambda: (s3, lam, sns)
    now = datetime.now(timezone.utc).isoformat()
    log = [{"at": now, "function": "justhodl-gdelt-full", "ok": True} for _ in range(lf.RESUME_MAX_PER_DAY)]
    s3.put_object("b", "data/_state/chain-resumer/log.json", json.dumps(log).encode())
    key = ticket(s3, "justhodl-gdelt-full", {"chain_depth": 40})
    out = lf.lambda_handler({})
    check(not lam.calls, "breaker: engine NOT invoked")
    check(key not in s3.objects and any(k.startswith("data/_state/chain-parked-held/") for k in s3.objects), "ticket moved to held")
    check(len(sns.msgs) == 1 and "circuit" in sns.msgs[0]["Subject"], "SNS alert published")
    check(len(out["held"]) == 1, "ledger reports the hold")


if __name__ == "__main__":
    for t in (test_resume_invokes_and_deletes, test_failed_invoke_keeps_ticket,
              test_refuses_non_fleet_function_and_malformed, test_circuit_breaker_holds_and_alerts):
        try:
            t()
        except Exception as e:  # noqa: BLE001
            FAILS.append("%s raised %r" % (t.__name__, e))
            print("FAIL", t.__name__, repr(e))
    print("%d failure(s)" % len(FAILS))
    sys.exit(1 if FAILS else 0)
