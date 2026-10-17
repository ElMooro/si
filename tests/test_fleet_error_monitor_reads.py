"""ops 6528 regressions for justhodl-fleet-error-monitor v1.2.0: billable reads and paused functions.

Executes the real check_lambda/is_paused/fetch_cw_metric extracted by AST with a counting fake; never contacts AWS.
"""
import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws/shared"))
from public_brain_projection import fleet_error_category  # noqa: E402

SOURCE = ROOT / "aws/lambdas/justhodl-fleet-error-monitor/source/lambda_function.py"
NAMES = {"fetch_cw_metric", "fetch_last_error_log", "is_paused", "check_lambda"}


class Fake:
    def __init__(self, metrics, reserved=None):
        self.metrics, self.reserved, self.reads = metrics, reserved, []

    def get_metric_statistics(self, *, MetricName, Dimensions, **kw):
        self.reads.append((Dimensions[0]["Value"], MetricName))
        return {"Datapoints": [{"Sum": self.metrics[MetricName]}]}

    def get_function_concurrency(self, FunctionName):
        return {} if self.reserved is None else {"ReservedConcurrentExecutions": self.reserved}

    def filter_log_events(self, **kw):
        return {"events": [{"message": "Task timed out after 300 seconds"}]}


def load(fake):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in NAMES]
    assert {n.name for n in nodes} == NAMES, {n.name for n in nodes}
    ns = {"datetime": datetime, "timedelta": timedelta, "timezone": timezone, "time": __import__("time"),
          "cw": fake, "lam": fake, "logs": fake, "EXCLUDE": set(), "LOOKBACK_MINUTES": 60,
          "MIN_INVOCATIONS": 5, "ERROR_RATE_THRESHOLD": 5, "fleet_error_category": fleet_error_category}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), "exec"), ns)
    return ns


class ReadTests(unittest.TestCase):
    def test_quiet_function_costs_two_reads_and_no_alert(self):
        fake = Fake({"Invocations": 40, "Errors": 0, "Throttles": 0})
        self.assertIsNone(load(fake)["check_lambda"]("justhodl-x"))
        self.assertEqual([m for _, m in fake.reads], ["Errors", "Throttles"])

    def test_erroring_function_still_alerts_with_rate(self):
        fake = Fake({"Invocations": 10, "Errors": 6, "Throttles": 0})
        alert = load(fake)["check_lambda"]("justhodl-x")
        self.assertEqual(alert["error_rate_pct"], 60.0)
        self.assertEqual(alert["severity"], "CRITICAL")
        self.assertEqual(sorted(m for _, m in fake.reads), ["Errors", "Invocations", "Throttles"])

    def test_low_volume_errors_below_floor_stay_silent(self):
        fake = Fake({"Invocations": 2, "Errors": 1, "Throttles": 0})
        self.assertIsNone(load(fake)["check_lambda"]("justhodl-x"))

    def test_paused_function_throttles_are_not_an_alert(self):
        fake = Fake({"Invocations": 0, "Errors": 0, "Throttles": 37}, reserved=0)
        self.assertIsNone(load(fake)["check_lambda"]("justhodl-x"))

    def test_unpaused_throttles_still_alert(self):
        fake = Fake({"Invocations": 0, "Errors": 0, "Throttles": 37}, reserved=None)
        alert = load(fake)["check_lambda"]("justhodl-x")
        self.assertEqual(alert["throttles"], 37)
        self.assertEqual(alert["severity"], "CRITICAL")

    def test_fleet_read_volume_drops_by_a_third(self):
        fakes = [Fake({"Invocations": 30, "Errors": 0, "Throttles": 0}) for _ in range(97)]
        fakes += [Fake({"Invocations": 30, "Errors": 3, "Throttles": 0}) for _ in range(3)]
        reads = 0
        for f in fakes:
            load(f)["check_lambda"]("justhodl-x")
            reads += len(f.reads)
        self.assertEqual(reads, 2 * 100 + 3)  # was 3 * 100


if __name__ == "__main__":
    unittest.main()
