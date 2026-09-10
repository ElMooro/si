from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SRC))

from point_in_time import (  # noqa: E402
    FeatureObservation,
    InMemoryFeatureStore,
    PointInTimeError,
    PointInTimeFeatureAssembler,
)
from signal_envelope import (  # noqa: E402
    SignalEnvelopeError,
    TaintedSignalError,
    envelope_fingerprint,
    mark_tainted,
    require_clean,
    validate_signal_envelope,
)

UTC = timezone.utc
T0 = datetime(2026, 9, 1, 14, 0, tzinfo=UTC)


def envelope(**overrides):
    value = {
        "schema_version": "1.0",
        "signal_id": "sig-1",
        "source": "market-feed",
        "entity_id": "BTC-USD",
        "event_time": "2026-09-01T10:00:00-04:00",
        "available_at": "2026-09-01T14:01:00Z",
        "produced_at": "2026-09-01T14:02:00Z",
        "payload": {"score": 0.7, "regime": "RISK_ON"},
        "provenance": [{"source_id": "raw-1", "fingerprint": "a" * 64, "tainted": False}],
        "taint": {"status": "CLEAN", "reasons": []},
    }
    value.update(overrides)
    return value


class SignalEnvelopeTests(unittest.TestCase):
    def test_validation_canonicalizes_timestamps_and_is_detached(self):
        raw = envelope()
        normalized = require_clean(raw)
        self.assertEqual(normalized["event_time"], "2026-09-01T14:00:00Z")
        self.assertEqual(normalized["taint"], {"status": "CLEAN", "reasons": []})
        raw["payload"]["score"] = 0.1
        self.assertEqual(normalized["payload"]["score"], 0.7)

    def test_payload_taint_is_propagated_and_rejected(self):
        raw = envelope(payload={"score": 0.8, "contains_future_data": True})
        normalized = validate_signal_envelope(raw)
        self.assertEqual(normalized["taint"]["status"], "TAINTED")
        self.assertEqual(normalized["taint"]["reasons"], ["payload.contains_future_data"])
        with self.assertRaises(TaintedSignalError):
            require_clean(raw, purpose="training")

    def test_provenance_taint_cannot_be_declared_clean(self):
        raw = envelope(provenance=[{
            "source_id": "leaky-parent", "fingerprint": "b" * 64, "tainted": True,
        }])
        with self.assertRaisesRegex(TaintedSignalError, "leaky-parent"):
            require_clean(raw)

    def test_mark_tainted_is_sticky_and_fingerprint_is_stable(self):
        raw = envelope()
        first = mark_tainted(raw, "post-outcome revision")
        second = mark_tainted(first, "post-outcome revision")
        self.assertEqual(second["taint"]["reasons"], ["post-outcome revision"])
        self.assertEqual(envelope_fingerprint(second), envelope_fingerprint(first))

    def test_temporal_and_numeric_contracts_fail_closed(self):
        with self.assertRaisesRegex(SignalEnvelopeError, "must not precede"):
            validate_signal_envelope(envelope(available_at="2026-09-01T13:59:00Z"))
        with self.assertRaisesRegex(SignalEnvelopeError, "non-finite"):
            validate_signal_envelope(envelope(payload={"score": float("nan")}))
        with self.assertRaisesRegex(SignalEnvelopeError, "timezone"):
            validate_signal_envelope(envelope(event_time="2026-09-01T14:00:00"))


class PointInTimeTests(unittest.TestCase):
    def setUp(self):
        self.rows = [
            FeatureObservation("BTC", "momentum", T0, T0, 0.1, "prices", revision=0),
            FeatureObservation(
                "BTC", "momentum", T0 + timedelta(hours=1), T0 + timedelta(hours=3),
                0.9, "prices", revision=0,
            ),
            FeatureObservation(
                "BTC", "momentum", T0, T0 + timedelta(minutes=30),
                0.2, "prices", revision=1,
            ),
            FeatureObservation("BTC", "volatility", T0, T0, 0.3, "risk"),
        ]
        self.assembler = PointInTimeFeatureAssembler(InMemoryFeatureStore(self.rows))

    def test_late_data_is_not_visible_before_available_at(self):
        vector = self.assembler.assemble(
            "BTC", T0 + timedelta(hours=2), ["momentum", "volatility"]
        )
        self.assertEqual(vector.values, {"momentum": 0.2, "volatility": 0.3})
        self.assertEqual(vector.lineage[0]["revision"], 1)

    def test_later_as_of_selects_newer_event(self):
        vector = self.assembler.assemble(
            "BTC", T0 + timedelta(hours=4), ["momentum"]
        )
        self.assertEqual(vector.values["momentum"], 0.9)

    def test_missing_stale_and_tainted_features_are_rejected(self):
        with self.assertRaisesRegex(PointInTimeError, "missing"):
            self.assembler.assemble("BTC", T0, ["unknown"])
        with self.assertRaisesRegex(PointInTimeError, "missing"):
            self.assembler.assemble(
                "BTC", T0 + timedelta(days=3), ["volatility"], max_age=timedelta(days=1)
            )
        tainted = InMemoryFeatureStore([
            FeatureObservation("BTC", "label_like", T0, T0, 1, "bad", tainted=True)
        ])
        with self.assertRaisesRegex(PointInTimeError, "tainted"):
            PointInTimeFeatureAssembler(tainted).assemble("BTC", T0, ["label_like"])

    def test_fingerprint_is_deterministic_and_order_sensitive_contract_is_removed(self):
        one = self.assembler.assemble("BTC", T0 + timedelta(hours=2), ["volatility", "momentum"])
        two = self.assembler.assemble("BTC", T0 + timedelta(hours=2), ["volatility", "momentum"])
        self.assertEqual(one.fingerprint, two.fingerprint)
        self.assertEqual(tuple(one.values), ("volatility", "momentum"))


if __name__ == "__main__":
    unittest.main()
