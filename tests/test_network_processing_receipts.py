"""Regression coverage for falsely accepting a prior publication receipt."""
import hashlib
from datetime import datetime, timedelta
import unittest
from unittest.mock import patch
import test_research_network as fixture

N, S, C, NOW = fixture.N, fixture.S, fixture.C, fixture.NOW


class ProcessingReceiptTests(unittest.TestCase):
    def setUp(self):
        self.store = fixture.Store()
        self.store.data[fixture.SOURCES['fundamentals']['key']] = N.canonical(fixture.native())
        self.prior = S.publish_network(self.store, 'test', now=NOW)
        self.raw = self.store.data[S.KEY]
        self.sha = hashlib.sha256(self.raw).hexdigest()
        self.receipt = C.consume(self.store, 'test', 'engine-fusion', now=NOW)

    def result(self, receipt=None, source='engine-fusion', consumer='engine-fusion'):
        sources = {source: {'consumer_receipt': self.receipt if receipt is None else receipt}}
        return S.processing_receipt(consumer, sources, self.prior, self.sha, NOW+timedelta(minutes=1))

    def test_exact_id_hash_owner_and_clock_accepted(self):
        result = self.result()
        self.assertTrue(result['processed_previous_publication'])
        self.assertEqual(result['verification_reasons'], [])

    def test_same_id_with_wrong_or_absent_hash_rejected(self):
        for value in ('0'*64, None, '', True):
            with self.subTest(value=value):
                receipt = {**self.receipt, 'source_sha256': value}
                result = self.result(receipt)
                self.assertFalse(result['processed_previous_publication'])
                self.assertIn('previous_publication_bytes_mismatch', result['verification_reasons'])

    def test_foreign_output_cannot_attest_for_consumer(self):
        result = self.result(source='alpha-council')
        self.assertFalse(result['processed_previous_publication'])
        self.assertIsNone(result['receipt'])
        self.assertIn('owning_source_receipt_missing', result['verification_reasons'])

    def test_wrong_consumer_in_owning_output_rejected(self):
        result = self.result({**self.receipt, 'consumer': 'alpha-council'})
        self.assertFalse(result['processed_previous_publication'])
        self.assertIn('consumer_identity_mismatch', result['verification_reasons'])

    def test_invalid_or_outside_window_clock_rejected(self):
        for value in (None, 'invalid', NOW.replace(tzinfo=None).isoformat(),
                      (NOW-timedelta(seconds=1)).isoformat(),
                      (NOW+timedelta(minutes=2)).isoformat()):
            with self.subTest(value=value):
                result = self.result({**self.receipt, 'read_at': value})
                self.assertFalse(result['processed_previous_publication'])
                self.assertIn('processing_clock_invalid', result['verification_reasons'])

    def test_unavailable_or_other_publication_rejected(self):
        for field, value in (('status','unavailable'), ('publication_id','0'*64)):
            self.assertFalse(self.result({**self.receipt, field: value})['processed_previous_publication'])

    def test_alias_uses_its_declared_output_and_private_receipts_stay_private(self):
        result = self.result({**self.receipt,'consumer':'prospective-evaluator'},
                             source='prospective-outcomes', consumer='prospective-evaluator')
        self.assertTrue(result['processed_previous_publication'])
        self.assertEqual(result['receipt_source_id'], 'prospective-outcomes')
        private = S.processing_receipt('portfolio-risk', {}, self.prior, self.sha, NOW)
        self.assertEqual(private, {'status': 'private_receipt_in_authenticated_risk_output'})

    def test_publisher_compares_original_bytes_not_reserialized_value(self):
        # A harmless newline changes byte identity without changing JSON values.
        self.store.data[S.KEY] = self.raw+b'\n'
        receipt = C.consume(self.store, 'test', 'engine-fusion', now=NOW)
        self.store.data[fixture.SOURCES['engine-fusion']['key']] = N.canonical(
            {'generated_at': NOW.isoformat(), 'research_network': receipt})
        second = S.publish_network(self.store, 'test', now=NOW+timedelta(minutes=1))
        self.assertTrue(second['consumer_processing']['engine-fusion']['processed_previous_publication'])
        receipt['publication_id'] = second['publication_id']
        receipt['source_sha256'] = '0'*64
        receipt['read_at'] = (NOW+timedelta(minutes=1)).isoformat()
        self.store.data[fixture.SOURCES['engine-fusion']['key']] = N.canonical({'research_network': receipt})
        third = S.publish_network(self.store, 'test', now=NOW+timedelta(minutes=2))
        self.assertFalse(third['consumer_processing']['engine-fusion']['processed_previous_publication'])

    def test_consumer_finishing_during_collection_is_not_marked_future(self):
        receipt = {**self.receipt, 'read_at': (NOW+timedelta(minutes=2)).isoformat()}
        self.store.data[fixture.SOURCES['engine-fusion']['key']] = N.canonical({'research_network': receipt})
        ticks = iter((NOW+timedelta(minutes=1), NOW+timedelta(minutes=3)))
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None): return next(ticks)
        with patch.object(S, 'datetime', Clock):
            result = S.publish_network(self.store, 'test')
        self.assertTrue(result['consumer_processing']['engine-fusion']['processed_previous_publication'])


if __name__ == '__main__': unittest.main()
