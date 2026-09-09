"""Dependency-free tests for justhodl-guardrail-notify (deploy gate)."""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "source"))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import lambda_function as lf  # noqa: E402

FAILS = []


def check(cond, msg):
    if not cond:
        FAILS.append(msg)
        print("FAIL", msg)
    else:
        print("ok  ", msg)


def test_format_alarm():
    a = {"AlarmName": "justhodl-guard-lambda-recursion-dropped", "NewStateValue": "ALARM",
         "OldStateValue": "OK", "NewStateReason": "Threshold Crossed: 1 datapoint [3.0] > 0",
         "StateChangeTime": "2026-09-09T13:00:00.000+0000",
         "Trigger": {"MetricName": "RecursiveInvocationsDropped"}}
    t = lf._fmt_alarm(a)
    check("recursion-dropped" in t and "OK -> ALARM" in t, "alarm text carries name + transition")
    check("RecursiveInvocationsDropped" in t, "alarm text carries metric")
    check("kill switch" in t, "ALARM state carries the runbook hint")
    a["NewStateValue"] = "OK"
    check("kill switch" not in lf._fmt_alarm(a), "OK state has no runbook hint")


def test_records_parsing():
    ev = {"Records": [{"Sns": {"Message": json.dumps({"AlarmName": "x", "NewStateValue": "ALARM"})}},
                      {"Sns": {"Subject": "plain", "Message": "not json"}}]}
    recs = list(lf._records(ev))
    check(len(recs) == 2, "two SNS records parsed")
    check(recs[0]["AlarmName"] == "x", "json message parsed as alarm")
    check(recs[1]["NewStateValue"] == "INFO" and "not json" in recs[1]["NewStateReason"],
          "non-json message wrapped, not dropped")
    check(list(lf._records({})) == [], "empty event yields nothing")


def test_handler_never_raises_without_network():
    # No SSM/Telegram/S3 in the test sandbox: the handler must degrade, not raise.
    sent = {}
    lf._telegram = lambda text: (False, "offline")
    lf._ledger = lambda rec, ok, info: sent.setdefault("n", 0) or sent.update(n=sent.get("n", 0) + 1)
    out = lf.lambda_handler({"Records": [{"Sns": {"Message": json.dumps({"AlarmName": "y"})}}]})
    check(out["failed"] == 1 and out["sent"] == 0, "offline delivery counted as failed, no exception")
    out = lf.lambda_handler({"test": True})
    check(out.get("telegram_ok") is False and out["telegram_info"] == "offline", "test invoke reports delivery state")


if __name__ == "__main__":
    for fn in (test_format_alarm, test_records_parsing, test_handler_never_raises_without_network):
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            FAILS.append("%s raised %s" % (fn.__name__, e))
            print("FAIL", fn.__name__, e)
    print("%d failure(s)" % len(FAILS))
    sys.exit(1 if FAILS else 0)
