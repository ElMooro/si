"""Real fleet-error handler with fake AWS; never contact notification services."""
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import redirect_stdout
from copy import deepcopy
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
from public_brain_projection import fleet_error_category, sanitize_public

SOURCE = ROOT / "aws/lambdas/justhodl-fleet-error-monitor/source/lambda_function.py"
MARKER = "PRIVATE_CLOUDWATCH_NOTE_OR_SECRET_SENTINEL"


class AWSFixture:
    def __init__(self, *, dlq_error=False, metric_error=False, deduped=False):
        self.log_reads, self.notifications, self.writes = [], [], {}
        self.dlq_error, self.metric_error = dlq_error, metric_error
        self.history = {"justhodl-fixture:CRITICAL": datetime.now(timezone.utc).isoformat()} if deduped else {}

    def get_paginator(self, name):
        assert name == "list_functions"
        return types.SimpleNamespace(paginate=lambda: [{"Functions": [{"FunctionName": "justhodl-fixture"}]}])

    def get_metric_statistics(self, *, MetricName, **kwargs):
        if self.metric_error:
            raise RuntimeError(MARKER)
        return {"Datapoints": [{"Sum": {"Invocations": 10, "Errors": 6, "Throttles": 0}[MetricName]}]}

    def filter_log_events(self, **kwargs):
        self.log_reads.append(kwargs)
        return {"events": [{"message": "Task timed out: " + MARKER}]}

    def get_queue_url(self, **kwargs):
        if self.dlq_error:
            raise RuntimeError(MARKER)
        return {"QueueUrl": "fixture"}

    def get_queue_attributes(self, **kwargs):
        return {"Attributes": {"ApproximateNumberOfMessages": "0", "ApproximateNumberOfMessagesNotVisible": "0"}}

    def get_object(self, *, Key, **kwargs):
        assert Key == "data/_fleet-monitor-alert-history.json"
        return {"Body": io.BytesIO(json.dumps(self.history).encode())}

    def put_object(self, *, Key, Body, **kwargs):
        self.writes[Key] = json.loads(Body)

    def publish(self, **kwargs):
        self.notifications.append(("sns", kwargs["Message"]))


def scope(fixture):
    functions = [node for node in ast.parse(SOURCE.read_text()).body if isinstance(node, ast.FunctionDef)]
    namespace = dict(json=json, datetime=datetime, timedelta=timedelta, timezone=timezone, time=time,
        ThreadPoolExecutor=ThreadPoolExecutor, as_completed=as_completed,
        lam=fixture, cw=fixture, logs=fixture, sqs=fixture, sns=fixture, s3=fixture,
        VERSION="1.1.0", BUCKET="fixture", REGION="us-east-1", SNS_ARN="fixture", DLQ_NAME="fixture",
        ERROR_RATE_THRESHOLD=5, MIN_INVOCATIONS=5, LOOKBACK_MINUTES=15, DLQ_DEPTH_THRESHOLD=1,
        DEDUPE_WINDOW_MINUTES=60, EXCLUDE=set(), fleet_error_category=fleet_error_category, sanitize_public=sanitize_public)
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(SOURCE), "exec"), namespace)
    namespace["send_telegram"] = lambda message: fixture.notifications.append(("telegram", message)) or True
    return namespace


class FleetErrorPrivacyTests(unittest.TestCase):
    def test_actual_handler_keeps_private_notification_context_out_of_public_report(self):
        aws = AWSFixture(dlq_error=True)
        logs = io.StringIO()
        with redirect_stdout(logs):
            result = scope(aws)["lambda_handler"]({}, None)
        report = aws.writes["data/_fleet-monitor.json"]
        self.assertEqual(report["privacy_version"], "fleet-errors-metadata-20260909-v1")
        self.assertEqual(report["n_alerts_detected"], 1)
        self.assertEqual(report["alerts_scope"], "ALL_CURRENT_ALARMS")
        self.assertEqual(report["alerts"][0]["error_rate_pct"], 60)
        self.assertEqual(report["alerts"][0]["error_category"], "TIMEOUT")
        self.assertFalse(report["dlq_status"]["available"])
        self.assertNotIn(MARKER, json.dumps([aws.writes, result, logs.getvalue()]))
        self.assertEqual({channel for channel, _ in aws.notifications}, {"sns", "telegram"})
        self.assertTrue(all(MARKER in message for _, message in aws.notifications))
        self.assertIn("data/_fleet-monitor-alert-history.json", aws.writes)

    def test_quiet_iam_refresh_writes_only_sanitized_current_report_without_logs_or_notifications(self):
        aws = AWSFixture()
        with redirect_stdout(io.StringIO()):
            result = scope(aws)["lambda_handler"]({"mode": "audit_refresh"}, None)
        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(set(aws.writes), {"data/_fleet-monitor.json"})
        self.assertEqual(aws.log_reads, [])
        self.assertEqual(aws.notifications, [])
        report = aws.writes["data/_fleet-monitor.json"]
        self.assertEqual(report["notification_mode"], "suppressed_for_audit")
        self.assertFalse(report["telegram_sent"])
        self.assertFalse(report["sns_sent"])
        self.assertEqual(report["alerts"][0]["error_category"], "RUNTIME_ERROR")

    def test_quiet_mode_http_envelope_is_denied_before_reads_and_writes(self):
        aws = AWSFixture()
        result = scope(aws)["lambda_handler"]({"mode": "audit_refresh", "requestContext": {}}, None)
        self.assertEqual(result["statusCode"], 403)
        self.assertFalse(aws.writes or aws.notifications or aws.log_reads)

    def test_deduped_alarms_remain_visible_without_repeat_notifications(self):
        aws = AWSFixture(deduped=True)
        with redirect_stdout(io.StringIO()):
            scope(aws)["lambda_handler"]({}, None)
        report = aws.writes["data/_fleet-monitor.json"]
        self.assertEqual(report["n_alerts_raised"], 0)
        self.assertEqual(report["n_alerts_suppressed"], 1)
        self.assertEqual(report["n_alerts_detected"], 1)
        self.assertEqual(len(report["alerts"]), 1)
        self.assertFalse(aws.notifications)

    def test_cloudwatch_read_failure_becomes_unknown_measurement_not_zero_errors(self):
        aws = AWSFixture(metric_error=True)
        with redirect_stdout(io.StringIO()):
            scope(aws)["lambda_handler"]({"mode": "audit_refresh"}, None)
        row = aws.writes["data/_fleet-monitor.json"]["alerts"][0]
        self.assertIsNone(row["errors"])
        self.assertEqual(row["metric_status"], "UNAVAILABLE")
        self.assertEqual(row["error_category"], "METRIC_READ_UNAVAILABLE")
        self.assertFalse(aws.log_reads or aws.notifications)

    def test_legacy_projection_is_deterministic_complete_at_boundary_and_preserves_metrics(self):
        legacy = {"version": "1.0.0", "n_lambdas_scanned": 42, "n_alerts_raised": 1,
            "dlq_status": {"error": MARKER}, "alerts": [{"lambda": "justhodl-fixture", "severity": "WARNING",
                "invocations": 100, "errors": 8, "throttles": 0, "error_rate_pct": 8,
                "last_error_log": "AccessDenied: " + MARKER, "unknown_private_blob": MARKER}],
            "thresholds": {"lookback_minutes": 15}, "raw_debug": MARKER}
        original = deepcopy(legacy)
        projected = sanitize_public("data/_fleet-monitor.json", legacy)
        self.assertEqual(projected["n_lambdas_scanned"], 42)
        self.assertEqual(projected["alerts_scope"], "LEGACY_NEW_ALERTS_ONLY")
        self.assertEqual(projected["alerts"][0]["error_rate_pct"], 8)
        self.assertEqual(projected["alerts"][0]["error_category"], "ACCESS_DENIED")
        self.assertNotIn(MARKER, json.dumps(projected))
        self.assertNotIn("last_error_log", json.dumps(projected))
        self.assertEqual(legacy, original)
        self.assertEqual(sanitize_public("_fleet-monitor.json", projected), projected)


if __name__ == "__main__":
    unittest.main()
