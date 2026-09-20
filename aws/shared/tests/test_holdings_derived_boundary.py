"""Exercise production score paths, not copies of the exclusion implementation."""
from copy import deepcopy
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).resolve().parent)]
from ciss_vintage_test_support import load
from holdings_derived_boundary import BASIS, DIRECT, CLUSTER, compound_rows, current_basis, flow_rows


def legacy(value):
    return {'generated_at': '2026-09-19T20:00:00Z', 'calls_eligible': True,
            'vote_eligible': True, 'sizing_eligible': True,
            'most_bought': [{'ticker': 'KO', 'net_action_score': value, 'n_funds_adding': 99},
                            {'ticker': 'FAKE', 'net_action_score': value}],
            'most_sold': [{'ticker': 'KO', 'net_action_score': -value}],
            'clusters': [{'ticker': 'KO', 'score': value, 'n_buyers': 99,
                          'legend_buyers': ['BERKSHIRE'], 'n_funds_holding': 99},
                         {'ticker': 'FAKE', 'score': value, 'n_buyers': 99}]}


class Storage:
    def __init__(self, objects):
        self.objects = deepcopy(objects); self.writes = {}; self.reads = []

    def get_object(self, **kw):
        self.reads.append(kw['Key'])
        return {'Body': io.BytesIO(json.dumps(self.objects.get(kw['Key'], {})).encode())}

    def put_object(self, **kw):
        value = json.loads(kw['Body'])
        self.writes[kw['Key']] = value; self.objects[kw['Key']] = value


class Tests(unittest.TestCase):
    def test_malformed_or_mixed_version_composites_do_not_raise_or_pass(self):
        for packet in (None, [], {}, {'holdings_exclusions': None}, {'holdings_exclusions': []}):
            self.assertFalse(current_basis(packet))
            self.assertEqual(compound_rows(packet), []); self.assertEqual(flow_rows(packet), [])
        base = {'holdings_exclusions': {'basis': BASIS}}
        for row in (None, {}, {'systems': [{}], 'scores': {}},
                    {'systems': ['x'], 'scores': {'x': float('nan')}, 'n_systems': 1},
                    {'systems': ['x'], 'scores': {'x': 4}, 'n_systems': 1, 'compound_score': 400},
                    {'systems': ['x', 'x'], 'scores': {'x': 4}, 'n_systems': 2}):
            self.assertEqual(compound_rows({**base, 'compound': [row]}), [])
        for engines in (['13f', 'dark-pool'], ['smart-money', 'dark-pool'], ['dark-pool', 'dark-pool']):
            self.assertEqual(flow_rows({**base, 'multi_engine_confluence': [{'engines': engines, 'n_engines': 2}]}), [])
        self.assertEqual(len(flow_rows({**base, 'multi_engine_confluence': [
            {'engines': ['dark-pool', 'options-flow'], 'n_engines': 2}]})), 1)

    def test_compound_exclusion_precedes_universe_multiplier_and_history(self):
        baseline = None
        for packet in ({}, legacy(10**12), legacy(-10**12)):
            m = load('justhodl-compound-aggregator')
            db = Storage({CLUSTER: packet,
                'data/nobrainers.json': {'summary': {'top_25_overall': [{'ticker': 'KO', 'score': 40}]}},
                'data/insider-clusters.json': {'clusters': [{'ticker': 'KO', 'score': 60}]},
                'data/compound-history.json': {'days': [{'d': '2026-01-01', 'scores': {'KO': 10**12}}]},
                'data/compound-signals-state.json': {'alerted_keys': ['retain-me']}})
            with patch.object(m, 'S3', db), patch.object(m, 'emit_alerts', side_effect=AssertionError('No acceptance notifications')):
                result = m.lambda_handler({'suppress_alerts': True}, None)
            self.assertEqual(result['statusCode'], 200)
            out = db.writes[m.S3_KEY]; row = out['compound'][0]
            self.assertEqual(out['stats']['n_total_names'], 1)
            self.assertEqual(row['n_systems'], 2)
            self.assertEqual(row['compound_score'], 150)
            self.assertEqual(set(row['scores']), {'nobrainers', 'insiders'})
            self.assertNotIn('pctile_90d_self', row)
            self.assertEqual(out['feed_stats']['smart_money'], 0)
            self.assertFalse(out['holdings_exclusions']['sources'][0]['vote_eligible'])
            self.assertNotIn(m.STATE_KEY, db.writes)
            self.assertEqual(out['new_alerts'], [])
            days = db.writes['data/compound-history.json']['days']
            self.assertEqual(days[0]['scores']['KO'], 10**12)
            self.assertEqual(days[-1]['score_basis'], BASIS)
            self.assertTrue(compound_rows(out))
            if baseline is None: baseline = row
            self.assertEqual(row, baseline)

    def test_compound_notify_false_does_not_emit_or_consume_alert_state(self):
        m = load('justhodl-compound-aggregator'); db = Storage({})
        with patch.object(m, 'S3', db), patch.object(m, 'emit_alerts', side_effect=AssertionError('No send')):
            m.lambda_handler({'notify': False}, None)
        self.assertNotIn(m.STATE_KEY, db.writes)
        self.assertTrue(db.writes[m.S3_KEY]['notifications_suppressed'])

    def test_attention_full_handler_removes_score_denominator_and_selling_flags(self):
        baseline = None
        for packet in ({}, legacy(10**12), legacy(-10**12)):
            m = load('justhodl-attention-confluence')
            inputs = {DIRECT: packet, CLUSTER: packet,
                      'data/insider-clusters.json': {'clusters': [{'ticker': 'KO', 'score': 60, 'n_insiders': 3}]}}
            db = Storage(inputs)
            with patch.object(m, 's3', db): m.lambda_handler({}, None)
            out = db.writes[m.OUT_KEY]
            self.assertEqual(set(out['tickers']), {'KO'})
            row = out['tickers']['KO']
            self.assertEqual(row['smart_score'], 42)  # Existing insider adapter: 3 reporters * 14.
            self.assertEqual(row['confluence_smart'], 1)
            self.assertEqual(row['families_firing'], ['insider'])
            self.assertFalse(row['distributing'])
            self.assertIsNone(row['signals']['funds'])
            self.assertNotIn('funds', out['scoring']['smart_families'])
            self.assertEqual(out['panels']['smart_money'], [])
            self.assertEqual(m.x_13f(packet), {}); self.assertEqual(m.x_smart_money(packet), {})
            if baseline is None: baseline = row
            self.assertEqual(row, baseline)

    def test_flow_full_handler_keeps_other_inputs_without_false_agreement(self):
        baseline = None
        for packet in ({}, legacy(10**12), legacy(-10**12)):
            m = load('justhodl-flow-confluence')
            db = Storage({DIRECT: packet, CLUSTER: packet,
                          'data/dark-pool.json': {'top_accumulation': [{'ticker': 'KO'}]}})
            with patch.object(m, 's3', db): m.lambda_handler({}, None)
            out = db.writes[m.OUT_KEY]
            self.assertEqual(set(out['ticker_map']), {'KO'})
            row = out['ticker_map']['KO']
            self.assertEqual(row['score'], 0.7); self.assertEqual(row['n_engines'], 1)
            self.assertEqual(row['posture'], 'ACCUMULATION_LEAN')
            self.assertEqual(out['multi_engine_confluence'], [])
            self.assertTrue(current_basis(out))
            if baseline is None: baseline = row
            self.assertEqual(row, baseline)

    def test_ranker_actual_index_blocks_cluster_and_previous_compound_and_flow(self):
        m = load('justhodl-master-ranker'); m.engine_trust = None
        for packet in ({}, legacy(10**12), legacy(-10**12)):
            old = {'compound': [{'symbol': 'FAKE', 'compound_score': 10**12, 'n_systems': 50}],
                   'multi_engine_confluence': [{'ticker': 'FAKE', 'score': 10**12, 'n_engines': 50}]}
            inputs = {DIRECT: packet, CLUSTER: packet, 'data/compound-signals.json': old,
                      'data/flow-confluence.json': old,
                      'data/insider-clusters.json': {'clusters': [{'ticker': 'KO', 'n_insiders': 3, 'total_value': 100}]}}
            with patch.object(m, 'fetch_json', side_effect=lambda key, **kw: inputs.get(key)):
                idx, feeds = m.build_ticker_index()
            self.assertEqual(set(idx), {'KO'}); self.assertEqual(set(idx['KO']), {'insider'})
            self.assertEqual(feeds['smart_money'], packet)
            self.assertEqual(feeds['holdings_exclusions']['composite_basis_present'], {'compound': False, 'flow_confluence': False})

    def test_ranker_accepts_revised_components_and_rejects_legacy_component_hidden_by_flag(self):
        m = load('justhodl-master-ranker'); m.engine_trust = None
        good = {'holdings_exclusions': {'basis': BASIS}, 'compound': [
            {'symbol': 'KO', 'compound_score': 150, 'n_systems': 2,
             'systems': ['insiders', 'deep_value'], 'scores': {'insiders': 40, 'deep_value': 60}}]}
        for value in (10**12, -10**12):
            bad = deepcopy(good['compound'][0]); bad.update(symbol='FAKE', n_systems=3)
            bad['systems'].append('smart_money'); bad['scores']['smart_money'] = value
            mixed = {**good, 'compound': [*good['compound'], bad]}
            with patch.object(m, 'fetch_json', side_effect=lambda key, **kw: mixed if key == 'data/compound-signals.json' else {}):
                idx, _ = m.build_ticker_index()
            self.assertEqual(set(idx), {'KO'}); self.assertEqual(idx['KO']['compound']['score'], 150)


if __name__ == '__main__': unittest.main()
