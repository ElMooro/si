from __future__ import annotations

import hashlib
import io
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SRC))

from s3_event_outbox import OutboxConflictError, S3EventOutbox  # noqa: E402


T0 = datetime(2026, 9, 9, 18, 0, tzinfo=timezone.utc)


class ConditionalFailure(Exception):
    pass


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.versions = {}

    def _etag(self, body):
        return '"%s"' % hashlib.md5(body).hexdigest()  # nosec - models S3 ETag only

    def put_object(self, Bucket, Key, Body, IfNoneMatch=None, IfMatch=None, **kwargs):
        current = self.objects.get((Bucket, Key))
        if IfNoneMatch == "*" and current is not None:
            raise ConditionalFailure("PreconditionFailed")
        if IfMatch is not None and (
            current is None or self._etag(current) != IfMatch
        ):
            raise ConditionalFailure("PreconditionFailed")
        body = Body if isinstance(Body, bytes) else str(Body).encode()
        self.objects[(Bucket, Key)] = body
        self.versions[(Bucket, Key)] = self.versions.get((Bucket, Key), 0) + 1
        return {"ETag": self._etag(body)}

    def get_object(self, Bucket, Key):
        try:
            body = self.objects[(Bucket, Key)]
        except KeyError:
            raise KeyError("NoSuchKey")
        return {"Body": io.BytesIO(body), "ETag": self._etag(body)}


class Clock:
    def __init__(self):
        self.value = T0

    def __call__(self):
        return self.value


def payload(value=1.25):
    return {"signal_id": "sig-1", "payload": {"value": value}}


def message_id(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class S3EventOutboxTests(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()
        self.clock = Clock()
        self.value = payload()
        self.ident = message_id(self.value)
        self.outbox = S3EventOutbox(
            self.s3,
            outbox_bucket="private",
            archive_bucket="archive",
            lease_seconds=30,
            now=self.clock,
        )
        self.contract = {
            "source": "justhodl.engine",
            "detail_type": "SignalEnvelope/v1",
        }

    def invoke(self, publisher):
        return self.outbox.ingest(
            message_id=self.ident,
            payload=self.value,
            archive_key="signals/v1/%s.json" % self.ident,
            event_contract=self.contract,
            publisher=publisher,
        )

    def test_failure_is_pending_and_same_envelope_redrives_then_deduplicates(self):
        calls = []

        def fail_then_succeed(value, contract):
            calls.append((value, contract))
            if len(calls) == 1:
                raise RuntimeError("temporary")
            return "event-2"

        with self.assertRaises(RuntimeError):
            self.invoke(fail_then_succeed)
        key = ("private", self.outbox.state_key(self.ident))
        pending = json.loads(self.s3.objects[key])
        self.assertEqual(pending["status"], "PENDING")
        self.assertEqual(pending["attempts"], 1)
        self.assertIn(("archive", "signals/v1/%s.json" % self.ident), self.s3.objects)

        redriven = self.invoke(fail_then_succeed)
        self.assertTrue(redriven["duplicate"])
        self.assertTrue(redriven["redriven"])
        self.assertEqual(redriven["attempts"], 2)
        self.assertEqual(json.loads(self.s3.objects[key])["status"], "PUBLISHED")

        duplicate = self.invoke(fail_then_succeed)
        self.assertTrue(duplicate["duplicate"])
        self.assertFalse(duplicate["delivery_attempted"])
        self.assertEqual(len(calls), 2)

    def test_live_lease_prevents_concurrent_delivery_and_expiry_allows_redrive(self):
        self.outbox.stage(
            message_id=self.ident,
            payload=self.value,
            archive_key="signals/v1/%s.json" % self.ident,
            event_contract=self.contract,
        )
        state, _, status = self.outbox._claim(self.ident)
        self.assertEqual(status, "CLAIMED")
        called = []
        current = self.outbox.deliver(
            self.ident, lambda value, contract: called.append(value) or "event"
        )
        self.assertEqual(current["status"], "IN_FLIGHT")
        self.assertEqual(called, [])
        self.clock.value += timedelta(seconds=31)
        delivered = self.outbox.redrive(
            self.ident, lambda value, contract: called.append(value) or "event"
        )
        self.assertTrue(delivered["published"])
        self.assertEqual(delivered["attempts"], state["attempts"] + 1)
        self.assertEqual(len(called), 1)

    def test_message_id_collision_and_archive_mismatch_fail_closed(self):
        self.outbox.stage(
            message_id=self.ident,
            payload=self.value,
            archive_key="signals/v1/%s.json" % self.ident,
            event_contract=self.contract,
        )
        with self.assertRaises(OutboxConflictError):
            self.outbox.stage(
                message_id=self.ident,
                payload=payload(9),
                archive_key="signals/v1/%s.json" % self.ident,
                event_contract=self.contract,
            )
        self.s3.put_object(
            Bucket="archive",
            Key="signals/v1/%s.json" % self.ident,
            Body=b'{"wrong":true}',
        )
        loaded = self.outbox._get(self.ident)
        with self.assertRaises(OutboxConflictError):
            self.outbox.ensure_archive(loaded[0])


if __name__ == "__main__":
    unittest.main()
