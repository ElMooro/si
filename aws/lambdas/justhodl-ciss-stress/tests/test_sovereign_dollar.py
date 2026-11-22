"""The dollar-shortage document: the desk's three auxiliaries again, from the same retained responses, with whole histories."""
from datetime import datetime
import copy
import json
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path[:0] = [str(HERE.parents[1] / 'source'), str(HERE.parents[3] / 'shared'), str(HERE.parent)]

import ciss_source_model as model          # noqa: E402
import ciss_source_store as store          # noqa: E402
import sovereign_desk as desk              # noqa: E402
import dollar_shortage as dollar           # noqa: E402
import test_sovereign_desk as base         # noqa: E402

LATER = '2026-11-22T18:55:00+00:00'


def published():
    """A bucket holding the originals and the desk document compiled from them; (s3, desk document, read)."""
    s3, binding, receipts, aux = base.staged()
    read = lambda receipt: store.original(s3, 'test', receipt)                                  # noqa: E731
    doc, _ = desk.compile_desk(binding, receipts, aux, read, base.NOW, {})
    doc['replay'] = {'manifest_key': desk.RUNS + 'abc.json', 'output_sha256': 'f' * 64}
    s3.objects[desk.CURRENT] = model.encoded(doc)
    return s3, doc, read


class Compile(unittest.TestCase):
    def test_the_three_measures_are_the_desks_own_with_every_observation(self):
        s3, doc, read = published()
        out = dollar.compile_dollar(doc, read, LATER)
        self.assertEqual((out['contract'], out['version'], out['generated_at']), ('ciss-dollar-shortage.v1', dollar.VERSION, LATER))
        self.assertEqual(out['order'], ['AUX_EURUSD', 'AUX_FX_EA', 'AUX_FX_NONEA'])
        self.assertEqual(set(out['measures']), set(out['series']))
        for mid in out['order']:
            m, s, d = out['measures'][mid], out['series'][mid], doc['measures'][mid]
            for field in ('value', 'date', 'period', 'exact', 'n', 'start', 'high', 'low', 'change', 'pctile', 'peaks', 'key', 'evidence', 'source_row', 'unit', 'freq', 'label'):
                self.assertEqual(m[field], d[field], mid + '.' + field)
            self.assertEqual((len(s['d']), s['d'][0], s['d'][-1], s['v'][-1]), (m['n'], m['start'], m['date'], m['value']))
            self.assertEqual(s['d'], sorted(set(s['d'])))
            self.assertTrue(m['what'] and m['short'])
        self.assertEqual(out['measures']['AUX_EURUSD']['value'], 1.099)
        self.assertEqual(out['series']['AUX_EURUSD']['v'][-2:], [1.0992, 1.099])
        self.assertEqual(out['measures']['AUX_FX_EA']['value'], 20.193)                         # EUR millions at the ECB, billions here
        self.assertEqual(out['measures']['AUX_FX_NONEA']['value'], 521.892)
        self.assertEqual([out['measures'][k]['worse'] for k in out['order']], ['lower', 'higher', None])
        self.assertEqual(out['desk'], {'key': desk.CURRENT, 'generated_at': base.NOW, 'version': desk.VERSION, 'replay': doc['replay']})
        self.assertEqual(out['episodes'], desk.EPISODES)
        self.assertEqual(out['coverage'], {'measures': 3, 'observations': sum(doc['measures'][k]['n'] for k in out['order']), 'status': 'measured'})
        self.assertEqual((out['calls_eligible'], out['sizing_eligible']), (False, False))

    def test_the_reference_rate_is_kept_for_every_day_where_the_desks_history_keeps_one_a_week(self):
        s3, doc, read = published()
        out = dollar.compile_dollar(doc, read, LATER)
        days = out['series']['AUX_EURUSD']['d']
        self.assertEqual(len(days), len(base.business_days('2026-10-01', '2026-11-20')))
        self.assertEqual(days[:3], ['2026-10-01', '2026-10-02', '2026-10-05'])                  # consecutive working days
        weekly = desk.weekly(list(zip(days, out['series']['AUX_EURUSD']['v'])))
        self.assertLess(len(weekly), len(days) / 4)
        self.assertEqual(out['series']['AUX_FX_EA']['d'][-1], '2026-11-13')                     # the Friday of ISO week 46

    def test_each_source_names_the_ecb_response_and_its_receipt(self):
        s3, doc, read = published()
        out = dollar.compile_dollar(doc, read, LATER)
        self.assertEqual([(s['id'], s['name'], s['path'], s['key']) for s in out['sources']], [
            ('AUX_EURUSD', 'eurusd', 'EXR/D.USD.EUR.SP00.A', 'EXR.D.USD.EUR.SP00.A'),
            ('AUX_FX_EA', 'fx_claims_ea', 'ILM/W.U2.C.A030000.U2.Z06', 'ILM.W.U2.C.A030000.U2.Z06'),
            ('AUX_FX_NONEA', 'fx_claims_nonea', 'ILM/W.U2.C.A020000.U4.Z06', 'ILM.W.U2.C.A020000.U4.Z06')])
        for s in out['sources']:
            self.assertEqual(s['evidence'], doc['measures'][s['id']]['evidence'])
            self.assertEqual(store.original(s3, 'test', s['evidence'])[:3], b'KEY')
            self.assertEqual((s['observations'], s['carried'], s['acquired_at']), (doc['measures'][s['id']]['n'], False, base.NOW))

    def test_the_same_inputs_give_the_same_bytes_and_the_desk_document_is_not_changed(self):
        s3, doc, read = published()
        before = copy.deepcopy(doc)
        self.assertEqual(model.digest(dollar.compile_dollar(doc, read, LATER)), model.digest(dollar.compile_dollar(copy.deepcopy(doc), read, LATER)))
        self.assertEqual(doc, before)

    def test_a_figure_that_does_not_reproduce_the_desks_is_refused(self):
        s3, doc, read = published()
        for field, wrong in (('value', 1.2), ('date', '2026-11-19'), ('n', 5), ('source_row', 1)):
            bad = copy.deepcopy(doc)
            bad['measures']['AUX_EURUSD'][field] = wrong
            with self.assertRaisesRegex(ValueError, 'AUX_EURUSD does not reproduce the desk document'):
                dollar.compile_dollar(bad, read, LATER)
        gone = copy.deepcopy(doc)
        del gone['measures']['AUX_FX_EA']
        with self.assertRaisesRegex(ValueError, 'does not carry AUX_FX_EA'):
            dollar.compile_dollar(gone, read, LATER)
        other = copy.deepcopy(doc)
        other['measures']['AUX_FX_EA']['evidence'] = doc['measures']['AUX_EURUSD']['evidence']       # a receipt for another response
        with self.assertRaisesRegex(ValueError, 'does not hold ILM.W.U2.C.A030000.U2.Z06'):
            dollar.compile_dollar(other, read, LATER)

    def test_an_observation_newer_than_the_desks_own_day_is_left_for_the_next_desk_document(self):
        s3, doc, read = published()
        older = copy.deepcopy(doc)
        older['generated_at'] = '2026-11-19T07:20:00+00:00'                                     # the desk as it was compiled on the 19th
        m = older['measures']['AUX_EURUSD']
        m.update({'value': 1.0992, 'date': '2026-11-19', 'n': m['n'] - 1, 'source_row': m['source_row'] - 1})
        out = dollar.compile_dollar(older, read, LATER)
        self.assertEqual(out['series']['AUX_EURUSD']['d'][-1], '2026-11-19')


class Publication(unittest.TestCase):
    def test_run_reads_the_published_desk_document_and_writes_one_object(self):
        s3, doc, _ = published()
        s3.puts.clear()
        out = dollar.run(s3, 'test', store, now=datetime.fromisoformat(LATER))
        self.assertEqual(s3.puts, [dollar.CURRENT])
        self.assertEqual((out['published'], out['generated_at'], out['desk_generated_at']), (True, LATER, base.NOW))
        self.assertEqual(out['latest'], {'AUX_EURUSD': [1.099, '2026-11-20'], 'AUX_FX_EA': [20.193, '2026-11-13'], 'AUX_FX_NONEA': [521.892, '2026-11-13']})
        written = json.loads(s3.objects[dollar.CURRENT])
        sha = written.pop('sha256')
        self.assertEqual(sha, model.digest(written))
        self.assertEqual(written, dollar.compile_dollar(doc, lambda r: store.original(s3, 'test', r), LATER))

    def test_nothing_is_written_without_the_desk_document_or_when_a_figure_differs(self):
        empty = base.MemoryS3()
        with self.assertRaises(base.StorageError):
            dollar.run(empty, 'test', store)
        s3, doc, _ = published()
        s3.objects[desk.CURRENT] = model.encoded({**doc, 'contract': 'something-else'})
        s3.puts.clear()
        with self.assertRaisesRegex(ValueError, 'sovereign desk document required'):
            dollar.run(s3, 'test', store)
        bad = copy.deepcopy(doc)
        bad['measures']['AUX_FX_NONEA']['value'] = 1.0
        s3.objects[desk.CURRENT] = model.encoded(bad)
        with self.assertRaisesRegex(ValueError, 'AUX_FX_NONEA does not reproduce'):
            dollar.run(s3, 'test', store)
        self.assertEqual(s3.puts, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
