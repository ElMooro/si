"""Whole synthetic captures: all bodies, exact clocks, all relationships, no AWS."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/ops/checks')]
import genealogy_public_archive as model
from instrument_identity import resolve_instrument
from prospective_journal import projection, protocol_document, digest, canonical, CONTRACT

NOW = datetime(2026, 9, 18, 19, tzinfo=timezone.utc)


class Store:
    def __init__(self):
        self.objects = {}
        self.reads = []
        self.streams = []

    def add(self, key, doc, at=NOW):
        self.objects[key] = (canonical(doc), at)

    def get_object(self, **kw):
        assert kw['Bucket'] == model.BUCKET
        key = kw['Key']
        self.reads.append(key)
        raw, stamp = self.objects[key]
        stream = io.BytesIO(raw)
        self.streams.append(stream)
        return {'Body': stream, 'ContentLength': len(raw), 'LastModified': stamp}

    def inventories(self):
        out = {}
        for prefix in (model.PREFIX+'captures/', model.PREFIX+'records/'):
            rows = [{'key': k, 'bytes': len(v[0]), 'last_modified': v[1].isoformat()}
                    for k, v in sorted(self.objects.items()) if k.startswith(prefix)]
            out[prefix] = {'prefix': prefix, 'objects': rows, 'cutoff': (NOW+timedelta(hours=1)).isoformat(),
                          'listing_complete': True, 'objects_at_cutoff': len(rows),
                          'inventory_sha256': digest(rows), 'total_bytes': sum(r['bytes'] for r in rows)}
        return out


def fixture(first_source=None):
    store = Store()
    protocol = protocol_document()
    pref = {'key': model.PREFIX+'protocols/'+digest(protocol)+'.json', 'sha256': digest(protocol), 'first_stored_at': NOW.isoformat()}
    store.add(pref['key'], protocol)
    sources, refs, records = [], [], []
    for index, (symbol, direction) in enumerate((('AAA', 'UP'), ('BBB', 'DOWN'))):
        key = first_source if index == 0 and first_source else 'data/synthetic-'+str(index)+'.json'
        observed = {'instrument': resolve_instrument(symbol), 'direction': direction, 'origin': 'explicit_direction'}
        source = projection(key, {'generated_at': NOW.isoformat()},
                            [{'identity': observed['instrument'], 'direction': direction, 'prediction_origin': 'explicit_direction'}],
                            str(index)*64, NOW.isoformat())
        sources.append(source)
        identity = {k: source[k] for k in ('source_key', 'source_bytes_sha256', 'source_generated_at')}
        fid = digest({'source': identity, 'observation': observed, 'protocol_sha256': pref['sha256']})
        record = {'contract': CONTRACT, 'forecast_id': fid, 'registered_at': NOW.isoformat(), 'registration_date_et': '2026-09-18',
                  'source': {k: source[k] for k in ('source_key', 'source_bytes_sha256', 'source_generated_at', 'source_received_at', 'quality_status', 'scope')},
                  'observation': observed, 'protocol': protocol, 'protocol_ref': pref,
                  'collector': {'contract': 'signal-harvester-projection.v1', 'source_sha256': 'c'*64},
                  'eligibility': {'measurement_only': True, 'sizing_eligible': False, 'promotion_eligible': False,
                                  'original_model_replay_verified': False, 'out_of_sample_validation': False}}
        path = model.PREFIX+'records/'+fid+'.json'
        store.add(path, record)
        records.append(record)
        refs.append({'forecast_id': fid, 'key': path, 'sha256': digest(record), 'created': True,
                     'registered_at': NOW.isoformat(), 'symbol': symbol, 'direction': direction, 'source_key': key})
    capture = {'contract': 'prospective-research-capture.v1', 'started_at': (NOW-timedelta(seconds=1)).isoformat(),
               'generated_at': (NOW+timedelta(seconds=1)).isoformat(), 'protocol_ref': pref, 'sources': sources, 'records': refs,
               'coverage': {'candidate_sources': 4, 'sources_scanned': 3, 'source_read_failures': ['data/synthetic-failure.json'], 'candidate_scan_complete': False},
               'sizing_eligible': False, 'promotion_eligible': False}
    add_capture(store, capture)
    return store, capture, records


def add_capture(store, doc):
    key = model.PREFIX+'captures/'+digest(doc)+'.json'
    store.add(key, doc, model.clock(doc['generated_at']))
    return key


class Archive(unittest.TestCase):
    def test_legacy_echo_is_preserved_but_cannot_claim_the_new_source_selection_policy(self):
        store, capture, _ = fixture('data/prospective-research.json')
        result = model.audit(store, store.inventories())
        self.assertTrue(result['record_body_and_storage_checks_complete'])
        self.assertEqual(result['validated_records'], 2)
        self.assertEqual(result['source_issue_counts'], {'derived_research_summary_is_not_an_original_forecast': 1})
        capture['source_selection_policy'] = model.source_selection_policy()
        with self.assertRaisesRegex(ValueError, 'capture_research_output_echo'):
            model.validate_capture(capture, {'key':'test','sha256':'a'*64,'last_modified':capture['generated_at']})
        _, good, _ = fixture()
        good['source_selection_policy'] = model.source_selection_policy()
        summary, refs = model.validate_capture(good, {'key':'test','sha256':'b'*64,'last_modified':good['generated_at']})
        self.assertEqual(summary['source_selection_policy'], model.source_selection_policy())
        self.assertEqual(len(refs), 2)

    def test_complete_repeated_capture_reconciliation_deduplicates_records_not_history(self):
        store, capture, _ = fixture()
        second = deepcopy(capture)
        second['generated_at'] = (NOW+timedelta(minutes=1)).isoformat()
        for ref in second['records']:
            ref['created'] = False
        add_capture(store, second)
        result = model.audit(store, store.inventories())
        self.assertTrue(result['record_body_and_storage_checks_complete'])
        self.assertEqual((result['validated_captures'], result['validated_records']), (2, 2))
        self.assertEqual(result['total_record_reference_occurrences'], 4)
        self.assertEqual(result['unique_referenced_records'], 2)
        self.assertEqual(result['registration_counts_by_utc_day'], {'2026-09-18': 2})
        self.assertEqual(result['capture_scans_complete'], 0)
        self.assertEqual(len(store.reads), 5)
        self.assertTrue(all(s.closed for s in store.streams))
        self.assertFalse(result['forecast_qualified'])
        self.assertFalse(result['calls_eligible'])

    def test_changed_truncated_and_invalid_bodies_are_accounted_without_early_stop(self):
        for kind in ('clock', 'truncated', 'duplicate_json', 'nonfinite_json', 'path_hash'):
            store, _, _ = fixture()
            inventory = store.inventories()
            key = next(k for k in store.objects if '/captures/' in k)
            raw, stamp = store.objects[key]
            if kind == 'clock':
                store.objects[key] = (raw, stamp+timedelta(seconds=1))
            elif kind == 'truncated':
                store.objects[key] = (raw[:-1], stamp)
            else:
                new = {'duplicate_json': b'{"x":1,"x":2}', 'nonfinite_json': b'{"x":NaN}', 'path_hash': b'{}'}[kind]
                store.objects[key] = (new, stamp)
                inventory = store.inventories()
            result = model.audit(store, inventory)
            self.assertFalse(result['record_body_and_storage_checks_complete'], kind)
            self.assertEqual(len(result['failures']), 1)
            self.assertEqual(result['validated_records'], 2)
            self.assertEqual(len(result['unreferenced_record_ids']), 2)
            self.assertTrue(all(s.closed for s in store.streams))

    def test_capture_ref_does_not_borrow_another_source_identity_or_omit_eligible_rows(self):
        for mutation in ('omit', 'wrong_fid', 'created_integer', 'authority_integer', 'scan_claim'):
            store, capture, _ = fixture()
            evidence = {'key': 'test', 'sha256': 'a'*64, 'last_modified': capture['generated_at']}
            if mutation == 'omit': capture['records'].pop()
            elif mutation == 'wrong_fid': capture['records'][0]['forecast_id'] = 'd'*64
            elif mutation == 'created_integer': capture['records'][0]['created'] = 1
            elif mutation == 'authority_integer': capture['sizing_eligible'] = 0
            else: capture['coverage']['candidate_scan_complete'] = True
            with self.assertRaises(ValueError, msg=mutation):
                model.validate_capture(capture, evidence)

    def test_mismatched_record_hash_and_conflicting_references_remain_explicit(self):
        store, capture, _ = fixture()
        capture['records'][0]['sha256'] = 'd'*64
        add_capture(store, capture)
        result = model.audit(store, store.inventories())
        self.assertFalse(result['record_body_and_storage_checks_complete'])
        self.assertEqual(len(result['reference_conflicts']), 1)
        # The conflict itself remains a failure even if iteration ends on the
        # correct reference and the final hash comparison happens to match.
        self.assertEqual(result['validated_records'], 2)

    def test_record_backdating_authority_types_and_protocol_storage_are_rejected(self):
        for mutation in ('backdate', 'authority', 'protocol_clock'):
            store, _, records = fixture()
            record = deepcopy(records[0])
            key = model.PREFIX+'records/'+record['forecast_id']+'.json'
            if mutation == 'backdate':
                store.add(key, record, NOW+timedelta(minutes=6))
            elif mutation == 'authority':
                record['eligibility']['sizing_eligible'] = 0
                store.add(key, record)
            else:
                pk = record['protocol_ref']['key']
                raw, stamp = store.objects[pk]
                store.objects[pk] = (raw, stamp+timedelta(seconds=1))
            result = model.audit(store, store.inventories())
            self.assertFalse(result['record_body_and_storage_checks_complete'], mutation)
            self.assertTrue(result['failures'])

    def test_inventory_integrity_and_forbidden_paths_fail_before_any_body_read(self):
        for mutation in ('count', 'bytes', 'hash', 'order', 'path'):
            store, _, _ = fixture()
            inv = store.inventories()
            item = inv[model.PREFIX+'records/']
            if mutation == 'count': item['objects_at_cutoff'] = True
            elif mutation == 'bytes': item['total_bytes'] += 1
            elif mutation == 'hash': item['inventory_sha256'] = '0'*64
            elif mutation == 'order':
                item['objects'].reverse()
                item['inventory_sha256'] = digest(item['objects'])
            else:
                item['objects'][0]['key'] = 'private/ledger.json'
                item['inventory_sha256'] = digest(item['objects'])
            with self.assertRaises(ValueError): model.audit(store, inv)
            self.assertEqual(store.reads, [])
        store = Store()
        with self.assertRaises(ValueError): model.read_object(store, 'data/ai-brief.json')
        self.assertEqual(store.reads, [])

    def test_valid_orphan_record_is_preserved_and_not_falsely_given_capture_evidence(self):
        store, _, _ = fixture()
        for key in list(store.objects):
            if '/captures/' in key: del store.objects[key]
        result = model.audit(store, store.inventories())
        self.assertEqual(result['validated_records'], 2)
        self.assertEqual(len(result['unreferenced_record_ids']), 2)
        self.assertEqual(result['unique_referenced_records'], 0)
        self.assertEqual(result['capture_scans_complete'], 0)

    def test_research_context_receipts_are_admitted_per_capture_and_never_conflict(self):
        """2026-10-26: 205 live captures failed record_ref_schema because the collector (prospective_journal.register)
        now attaches a research_context receipt to every record reference.  The three collector shapes validate, a
        repeated capture of the same forecast (existing_forecast_not_backfilled) is not a reference conflict, and the
        pre-receipt shape still validates."""
        store, capture, _ = fixture()
        first, second_ref = capture['records']
        first['research_context'] = {'key': model.PREFIX+'contexts/'+first['forecast_id']+'.json', 'sha256': 'e'*64, 'status': 'retained'}
        second_ref['research_context'] = {'status': 'context_not_retained', 'reason': 'ClientError'}
        for key in list(store.objects):
            if '/captures/' in key: del store.objects[key]
        add_capture(store, capture)
        repeat = deepcopy(capture)
        repeat['generated_at'] = (NOW+timedelta(minutes=1)).isoformat()
        for ref in repeat['records']:
            ref['created'] = False
            ref['research_context'] = {'status': 'existing_forecast_not_backfilled'}
        add_capture(store, repeat)
        result = model.audit(store, store.inventories())
        self.assertTrue(result['record_body_and_storage_checks_complete'], result['failures'])
        self.assertEqual((result['validated_captures'], result['validated_records']), (2, 2))
        self.assertEqual(result['reference_conflicts'], [])
        self.assertEqual(result['unique_referenced_records'], 2)
        self.assertEqual(result['total_record_reference_occurrences'], 4)
        self.assertFalse(result['forecast_qualified'])
        evidence = {'key': 'test', 'sha256': 'a'*64, 'last_modified': capture['generated_at']}
        bad = (
            ('status_missing', {'key': 'x', 'sha256': 'e'*64}, True),
            ('status_unknown', {'status': 'backfilled'}, True),
            ('retained_extra_key', {'key': model.PREFIX+'contexts/'+first['forecast_id']+'.json', 'sha256': 'e'*64, 'status': 'retained', 'note': 1}, True),
            ('retained_wrong_path', {'key': model.PREFIX+'contexts/'+'f'*64+'.json', 'sha256': 'e'*64, 'status': 'retained'}, True),
            ('retained_bad_hash', {'key': model.PREFIX+'contexts/'+first['forecast_id']+'.json', 'sha256': 'zz', 'status': 'retained'}, True),
            ('retained_but_existing', {'key': model.PREFIX+'contexts/'+first['forecast_id']+'.json', 'sha256': 'e'*64, 'status': 'retained'}, False),
            ('not_retained_without_reason', {'status': 'context_not_retained'}, True),
            ('existing_but_created', {'status': 'existing_forecast_not_backfilled'}, True),
            ('not_a_dict', 'retained', True),
        )
        for name, context, created in bad:
            mutated = deepcopy(capture)
            mutated['records'][0]['created'] = created
            mutated['records'][0]['research_context'] = context
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'record_ref_context_'):
                model.validate_capture(mutated, evidence)
        plain = deepcopy(capture)
        for ref in plain['records']:
            del ref['research_context']
        summary, refs = model.validate_capture(plain, evidence)
        self.assertEqual(len(refs), 2)
        with self.assertRaisesRegex(ValueError, 'record_ref_schema'):
            extra = deepcopy(plain); extra['records'][0]['other'] = 1
            model.validate_capture(extra, evidence)
        with self.assertRaisesRegex(ValueError, 'record_ref_context_schema'):
            noisy = deepcopy(capture); noisy['records'][1]['research_context'] = {'status': 'context_not_retained', 'reason': 'boom: ' + 'x' * 80}
            model.validate_capture(noisy, evidence)

    def test_real_collector_references_reconcile_with_sidecar_success_failure_and_mixed_vintages(self):
        """The producer -> validator contract itself (the 2026-10-26 drift ran live because nothing drove the real
        prospective_journal.register() into the validator): first registration (sidecar retained / retention failed),
        repeat registration (existing_forecast_not_backfilled), and a pre-receipt capture next to a post-receipt repeat."""
        import prospective_journal as pj

        class Precondition(Exception):
            def __init__(self):
                super().__init__('412'); self.response = {'Error': {'Code': 'PreconditionFailed'}}

        class S3(Store):
            def __init__(self, fail_context):
                super().__init__(); self.fail_context = fail_context
            def put_object(self, **kw):
                key = kw['Key']
                if self.fail_context and '/contexts/' in key: raise RuntimeError('boom')
                if kw.get('IfNoneMatch') == '*' and key in self.objects: raise Precondition()
                self.objects[key] = (kw['Body'], NOW)
            def head_object(self, **kw):
                return {'LastModified': self.objects[kw['Key']][1]}

        def capture_from(store, pref, source, refs, at):
            return add_capture(store, {'contract': 'prospective-research-capture.v1', 'started_at': (NOW-timedelta(seconds=1)).isoformat(),
                'generated_at': (at+timedelta(seconds=1)).isoformat(), 'protocol_ref': pref, 'sources': [source], 'records': refs,
                'coverage': {'candidate_sources': 1, 'sources_scanned': 1, 'source_read_failures': [], 'candidate_scan_complete': True},
                'sizing_eligible': False, 'promotion_eligible': False})

        for fail_context in (False, True):
            with self.subTest(fail_context=fail_context):
                base, capture, _ = fixture()
                for key in list(base.objects):
                    if '/captures/' in key or '/records/' in key: del base.objects[key]
                s3 = S3(fail_context); s3.objects = base.objects
                pref, source = capture['protocol_ref'], capture['sources'][0]
                first = pj.register(s3, model.BUCKET, source, pref, 'c'*64, now=NOW)
                repeat = pj.register(s3, model.BUCKET, source, pref, 'c'*64, now=NOW+timedelta(minutes=1))
                self.assertEqual([r['created'] for r in first + repeat], [True, False])
                self.assertEqual(first[0]['research_context']['status'], 'context_not_retained' if fail_context else 'retained')
                self.assertEqual(repeat[0]['research_context'], {'status': 'existing_forecast_not_backfilled'})
                capture_from(s3, pref, source, first, NOW)
                capture_from(s3, pref, source, repeat, NOW+timedelta(minutes=1))
                result = model.audit(s3, s3.inventories())
                self.assertTrue(result['record_body_and_storage_checks_complete'], result['failures'])
                self.assertEqual((result['reference_conflicts'], result['reference_errors']), ([], []))
                self.assertEqual((result['validated_captures'], result['validated_records'], result['total_record_reference_occurrences']), (2, 1, 2))
                self.assertNotIn('research_context', json.dumps(result), 'the receipt never reaches the published audit')
                # mixed vintages: a pre-receipt capture (no research_context) next to the post-receipt repeat
                plain = deepcopy(first)
                for r in plain: del r['research_context']
                for key in list(s3.objects):
                    if '/captures/' in key: del s3.objects[key]
                capture_from(s3, pref, source, plain, NOW)
                capture_from(s3, pref, source, repeat, NOW+timedelta(minutes=1))
                result = model.audit(s3, s3.inventories())
                self.assertTrue(result['record_body_and_storage_checks_complete'], result['failures'])
                self.assertEqual(result['reference_conflicts'], [])


if __name__ == '__main__':
    unittest.main()
