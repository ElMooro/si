"""justhodl-fleet-error-monitor v1.3.0: dead letters are judged by what arrived and from whom, not by the queue's depth.

2026-11-20 (ops 6856): the monitor paged "CRITICAL - DLQ has 3255 messages" every day. 2,845 of them were bus events
refused by justhodl-event-coordinator, held at reserved concurrency 0 by the spend governor since Oct 20; the other
~400 came from ~25 engines failing their daily run, none of them named. These tests run the real handler and the real
helpers (extracted by AST) against a fake AWS; nothing is contacted.
"""
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import sys
import time
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws/shared"))
from public_brain_projection import fleet_error_category, sanitize_public  # noqa: E402

SOURCE = ROOT / "aws/lambdas/justhodl-fleet-error-monitor/source/lambda_function.py"
HELD = "justhodl-event-coordinator"


class AWS:
    """Fake account: `dropped` {function: [hourly AsyncEventsDropped]}, `paused` at reserved concurrency 0."""

    def __init__(self, dropped=None, paused=(), depth=3255, history=None, metric_data_error=False, functions=None):
        self.dropped, self.paused, self.depth = dropped or {}, set(paused), depth
        self.history = history or {}
        self.metric_data_error = metric_data_error
        self.functions = functions or sorted(set(self.dropped) | {"justhodl-quiet"})
        self.notifications, self.writes, self.metric_data_calls = [], {}, []

    def get_paginator(self, name):
        assert name == "list_functions"
        return types.SimpleNamespace(paginate=lambda: [{"Functions": [{"FunctionName": n} for n in self.functions]}])

    def get_metric_statistics(self, **kwargs):        # the 60-minute error-rate check: quiet
        return {"Datapoints": [{"Sum": 0}]}

    def get_metric_data(self, *, MetricDataQueries, StartTime, EndTime, **kwargs):
        if self.metric_data_error:
            raise RuntimeError("AccessDenied")
        self.metric_data_calls.append((len(MetricDataQueries), StartTime, EndTime))
        out = []
        for q in MetricDataQueries:
            m = q["MetricStat"]
            assert m["Metric"]["MetricName"] == "AsyncEventsDropped" and m["Stat"] == "Sum" and m["Period"] == 3600
            out.append({"Id": q["Id"], "Values": list(self.dropped.get(m["Metric"]["Dimensions"][0]["Value"], []))})
        return {"MetricDataResults": out}

    def get_function_concurrency(self, FunctionName):
        return {"ReservedConcurrentExecutions": 0} if FunctionName in self.paused else {}

    def filter_log_events(self, **kwargs):
        return {"events": []}

    def get_queue_url(self, **kwargs):
        return {"QueueUrl": "fixture"}

    def get_queue_attributes(self, **kwargs):
        return {"Attributes": {"ApproximateNumberOfMessages": str(self.depth), "ApproximateNumberOfMessagesNotVisible": "0"}}

    def get_object(self, *, Key, **kwargs):
        assert Key == "data/_fleet-monitor-alert-history.json"
        return {"Body": io.BytesIO(json.dumps(self.history).encode())}

    def put_object(self, *, Key, Body, **kwargs):
        self.writes[Key] = json.loads(Body)
        if Key == "data/_fleet-monitor-alert-history.json":
            self.history = json.loads(Body)

    def publish(self, **kwargs):
        self.notifications.append(("sns", kwargs["Subject"], kwargs["Message"]))


def scope(aws, **over):
    functions = [node for node in ast.parse(SOURCE.read_text(encoding="utf-8")).body if isinstance(node, ast.FunctionDef)]
    ns = dict(json=json, datetime=datetime, timedelta=timedelta, timezone=timezone, time=time,
              ThreadPoolExecutor=ThreadPoolExecutor, as_completed=as_completed,
              lam=aws, cw=aws, logs=aws, sqs=aws, sns=aws, s3=aws,
              VERSION="1.3.0", BUCKET="fixture", REGION="us-east-1", SNS_ARN="fixture", DLQ_NAME="fixture",
              ERROR_RATE_THRESHOLD=5, MIN_INVOCATIONS=5, LOOKBACK_MINUTES=60, DLQ_DEPTH_THRESHOLD=1,
              DEDUPE_WINDOW_MINUTES=60, DLQ_LOOKBACK_HOURS=25, DLQ_CRITICAL_COUNT=100, DLQ_REALERT_HOURS=168,
              DLQ_ENGINES_KEY="<DLQ>:engines", EXCLUDE={"justhodl-fleet-error-monitor"},
              fleet_error_category=fleet_error_category, sanitize_public=sanitize_public)
    ns.update(over)
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(SOURCE), "exec"), ns)
    ns["send_telegram"] = lambda message: False
    return ns


def run(aws, event=None, **over):
    with redirect_stdout(io.StringIO()):
        result = scope(aws, **over)["lambda_handler"](event or {}, None)
    return json.loads(result["body"])


class DeadLetterHandlerTests(unittest.TestCase):
    def test_the_2026_11_20_page_a_deep_queue_fed_only_by_a_held_engine_is_not_an_alert(self):
        aws = AWS(dropped={HELD: [250, 12]}, paused={HELD}, depth=3255)
        body = run(aws)
        self.assertEqual(aws.notifications, [])
        self.assertEqual(body["dead_letters"]["held"], {HELD: 262})
        self.assertEqual(body["dead_letters"]["failing"], {})
        report = aws.writes["data/_fleet-monitor.json"]
        self.assertEqual(report["n_alerts_detected"], 0)
        self.assertEqual(report["dlq_status"]["total"], 3255)          # the stock is still published
        self.assertNotIn("data/_fleet-monitor-alert-history.json", aws.writes)

    def test_a_deep_queue_with_no_arrivals_at_all_is_not_an_alert(self):
        aws = AWS(dropped={}, depth=3255)
        self.assertEqual(run(aws)["n_alerts"], 0)
        self.assertEqual(aws.notifications, [])

    def test_failing_engines_are_named_and_held_refusals_are_reported_apart(self):
        aws = AWS(dropped={HELD: [262], "justhodl-earnings-blackout": [1, 1], "justhodl-signal-genealogy": [2],
                           "justhodl-chart-patterns": [1]}, paused={HELD})
        body = run(aws)
        self.assertEqual(len(aws.notifications), 1)
        _, subject, message = aws.notifications[0]
        self.assertEqual(subject, "Fleet alert: 1 issue(s)")
        self.assertIn("WARNING (1)", message)
        self.assertNotIn("CRITICAL", message)
        self.assertIn("5 dead letters in the last 25 h from 3 failing engines", message)
        for name in ("`earnings-blackout` 2", "`signal-genealogy` 2", "`chart-patterns` 1"):
            self.assertIn(name, message)
        self.assertIn("Not failures: 262 events refused by held engine at concurrency 0 (`event-coordinator` 262)", message)
        self.assertIn("Queue holds 3255", message)
        self.assertNotIn("(new)", message)                               # all of them are new: no per-name marks
        self.assertEqual(body["dead_letters"]["failing"],
                         {"justhodl-earnings-blackout": 2, "justhodl-signal-genealogy": 2, "justhodl-chart-patterns": 1})
        row = aws.writes["data/_fleet-monitor.json"]["alerts"][0]
        self.assertEqual((row["lambda"], row["severity"], row["error_category"]), ("<DLQ>", "WARNING", "DLQ_BACKLOG"))
        self.assertEqual(sorted(aws.history["<DLQ>:engines"]),
                         ["justhodl-chart-patterns", "justhodl-earnings-blackout", "justhodl-signal-genealogy"])
        self.assertNotIn(HELD, aws.history["<DLQ>:engines"])

    def test_a_standing_set_is_not_repeated_the_next_day_but_a_new_engine_is(self):
        aws = AWS(dropped={"justhodl-earnings-blackout": [2], "justhodl-chart-patterns": [1]},
                  functions=["justhodl-chart-patterns", "justhodl-earnings-blackout", "justhodl-port-cargo"])
        run(aws)
        self.assertEqual(len(aws.notifications), 1)
        body = run(aws)                                                  # the next day, the same two engines
        self.assertEqual(len(aws.notifications), 1)
        self.assertEqual((body["n_alerts"], body["suppressed"]), (0, 1))
        self.assertEqual(aws.writes["data/_fleet-monitor.json"]["n_alerts_detected"], 1)   # still a current alarm
        aws.dropped["justhodl-port-cargo"] = [1]                         # a third engine starts failing
        run(aws)
        self.assertEqual(len(aws.notifications), 2)
        message = aws.notifications[1][2]
        self.assertIn("from 3 failing engines (1 not reported before): `port-cargo` 1 (new)", message)
        self.assertIn("`earnings-blackout` 2,", message)
        self.assertNotIn("`earnings-blackout` 2 (new)", message)

    def test_the_standing_set_is_repeated_after_a_week(self):
        old = (datetime.now(timezone.utc) - timedelta(hours=169)).isoformat()
        aws = AWS(dropped={"justhodl-earnings-blackout": [2]}, history={"<DLQ>:engines": {"justhodl-earnings-blackout": old}})
        run(aws)
        self.assertEqual(len(aws.notifications), 1)
        recent = (datetime.now(timezone.utc) - timedelta(hours=100)).isoformat()
        aws = AWS(dropped={"justhodl-earnings-blackout": [2]}, history={"<DLQ>:engines": {"justhodl-earnings-blackout": recent}})
        run(aws)
        self.assertEqual(aws.notifications, [])

    def test_a_storm_from_running_engines_is_critical_and_follows_the_ordinary_window(self):
        aws = AWS(dropped={"justhodl-miss-calibrator": [60, 70]})
        run(aws)
        self.assertIn("CRITICAL (1)", aws.notifications[0][2])
        self.assertIn("130 dead letters", aws.notifications[0][2])
        run(aws)                                                         # minutes later: inside the 60-minute window
        self.assertEqual(len(aws.notifications), 1)
        aws.history["<DLQ>:CRITICAL"] = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        run(aws)                                                         # the next day it is still a storm: paged again
        self.assertEqual(len(aws.notifications), 2)

    def test_held_refusals_never_make_a_storm(self):
        aws = AWS(dropped={HELD: [5000]}, paused={HELD})
        run(aws)
        self.assertEqual(aws.notifications, [])

    def test_unreadable_metrics_fall_back_to_the_depth_rule_instead_of_going_blind(self):
        aws = AWS(metric_data_error=True, depth=3255)
        body = run(aws)
        self.assertIsNone(body["dead_letters"])
        self.assertEqual(len(aws.notifications), 1)
        self.assertIn("DLQ has 3255 messages", aws.notifications[0][2])
        self.assertIn("per-engine arrivals unavailable", aws.notifications[0][2])
        aws = AWS(metric_data_error=True, depth=0)
        run(aws)
        self.assertEqual(aws.notifications, [])

    def test_quiet_audit_refresh_reports_without_notifying_or_touching_history(self):
        aws = AWS(dropped={"justhodl-earnings-blackout": [2]})
        body = run(aws, {"mode": "audit_refresh"})
        self.assertEqual(aws.notifications, [])
        self.assertEqual(set(aws.writes), {"data/_fleet-monitor.json"})
        self.assertEqual(body["dead_letters"]["failing"], {"justhodl-earnings-blackout": 2})
        self.assertEqual(aws.writes["data/_fleet-monitor.json"]["n_alerts_detected"], 1)


class DeadLetterHelperTests(unittest.TestCase):
    def test_one_metric_data_call_per_500_functions_over_whole_hours_and_self_is_excluded(self):
        names = ["justhodl-f%04d" % i for i in range(1101)] + ["justhodl-fleet-error-monitor"]
        aws = AWS(dropped={"justhodl-f0003": [1, 0, 2], "justhodl-f0777": [4], "justhodl-fleet-error-monitor": [9]}, functions=names)
        out = scope(aws)["fetch_dead_letters"](names)
        self.assertEqual(out, {"justhodl-f0003": 3, "justhodl-f0777": 4})
        self.assertEqual([n for n, _, _ in aws.metric_data_calls], [500, 500, 101])
        _, start, end = aws.metric_data_calls[0]
        self.assertEqual((start.minute, start.second, start.microsecond), (0, 0, 0))
        self.assertTrue(timedelta(hours=25) <= end - start < timedelta(hours=26))

    def test_metric_pages_are_followed(self):
        class Paged(AWS):
            def get_metric_data(self, *, MetricDataQueries, **kwargs):
                self.metric_data_calls.append(kwargs.get("NextToken"))
                if not kwargs.get("NextToken"):
                    return {"MetricDataResults": [{"Id": "d0", "Values": [2]}], "NextToken": "t"}
                return {"MetricDataResults": [{"Id": "d0", "Values": [3]}]}
        aws = Paged()
        self.assertEqual(scope(aws)["fetch_dead_letters"](["justhodl-a"]), {"justhodl-a": 5})
        self.assertEqual(aws.metric_data_calls, [None, "t"])

    def test_cloudwatch_failure_is_none_not_an_empty_all_clear(self):
        self.assertIsNone(scope(AWS(metric_data_error=True))["fetch_dead_letters"](["justhodl-a"]))

    def test_split_and_alert_are_pure(self):
        ns = scope(AWS())
        failing, held = ns["split_dead_letters"]({"a": 1, "b": 2}, {"b"})
        self.assertEqual((failing, held), ({"a": 1}, {"b": 2}))
        self.assertIsNone(ns["dead_letter_alert"]({}, {"b": 2}, {"available": True, "total": 9}, {}))
        alert = ns["dead_letter_alert"]({"justhodl-a": 1}, {}, {"available": False, "error": "DLQ_STATUS_UNAVAILABLE"}, {})
        self.assertEqual(alert["severity"], "WARNING")
        self.assertEqual(alert["note"], "1 dead letter in the last 25 h from 1 failing engine: `a` 1")
        many = {"justhodl-e%02d" % i: 1 for i in range(20)}
        note = ns["dead_letter_alert"](many, {}, {"available": True, "total": 20}, {})["note"]
        self.assertIn("+5 more", note)
        self.assertEqual(note.count("`"), 30)

    def test_a_corrupt_history_entry_counts_as_never_reported(self):
        ns = scope(AWS())
        for bad in ({"<DLQ>:engines": "oops"}, {"<DLQ>:engines": {"justhodl-a": "not a date"}}):
            self.assertEqual(ns["dead_letter_alert"]({"justhodl-a": 1}, {}, {}, bad)["dead_letters"]["new"], ["justhodl-a"])


if __name__ == "__main__":
    unittest.main()
