"""Synthetic whole-source replay and eligibility; no live context reads."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime,date,timezone
from decimal import Decimal
import ast,gzip,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-velocity-acceleration/source')]
import context_evidence_store as store
import velocity_evidence as m
from test_velocity_volume_observations import rows
AT='2026-09-28T07:00:00Z';ASOF=date(2026,9,28)


def parent(tickers=('SYNA',)):
    members=[{'ticker':ticker,'request_index':i} for i,ticker in enumerate(tickers)]
    # Use the actual parent's declared flags, not the successor's stronger shape.
    tree=ast.parse((ROOT/'aws/lambdas/justhodl-momentum-leaders/source/leader_price_observations.py').read_bytes())
    flags=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='FLAGS' for t in n.targets))
    ns={};exec(compile(ast.Module(body=[flags],type_ignores=[]),'<parent literal flags only>','exec'),ns)
    return {'measurement_contract':'leader-price-observations.v1','status':'RESEARCH_ONLY','generated_at':AT,**ns['FLAGS'],'call':None,
        'universe_membership':{'selected':members},'request_records':deepcopy(members)}


def captures(tickers=('SYNA',)):
    inputs={};attempts=[];sources={}
    def received(raw):
        ref=store.identity(raw,m.PRIVATE,'sources');sources[ref['key']]=raw
        return {'status':'received','original_ref':ref,'requested_at':AT,'received_at':AT,'content_encoding':''}
    for name,key in m.ALL_INPUTS.items():
        if name=='breakout':inputs[name]={'source_key':key,'status':'not_read_existing_research_exclusion','requested_at':AT,'received_at':AT};continue
        raw=store.encode(parent(tickers) if name=='momentum' else {'synthetic_context':'private, not a confirmation','pending':{'OTHER':{'status':'confirmed'}}})
        inputs[name]={'source_key':key,**received(raw)}
    for ticker in tickers[:80]:
        raw=store.encode([dict(row,symbol=ticker) for row in rows()])
        attempts.append({'ticker':ticker,'endpoint':m.endpoint(ticker,ASOF),'http_status':200,'network_attempted':True,**received(raw)})
    return inputs,attempts,sources


class Tests(unittest.TestCase):
    def test_complete_replay_preserves_source_coordinates_and_withholds_authority(self):
        a,b,s=captures();before=deepcopy((a,b,s));p=m.build(a,b,s,AT)
        self.assertEqual((a,b,s),before);self.assertEqual(p,m.build(a,b,s,AT))
        self.assertEqual(p['coverage']['measured_descriptive_fields'],5);self.assertEqual(p['call'],'WAIT')
        self.assertEqual(p['volume_observations'][0]['measurements'][0]['value_exact'],'100')
        self.assertEqual(len(p['source_inputs']),6);self.assertEqual(p['actionable_tickers'],[])
        self.assertNotIn('synthetic_context',store.encode(p).decode());self.assertNotIn('private, not a confirmation',store.encode(p).decode())
        for field in m.FLAGS:self.assertIs(p[field],False)
        self.assertIsNone(p['n_actionable']);self.assertIsNone(p['trading_date']);self.assertEqual(p['pending_state_writes'],0)
    def test_all_parent_occurrences_retained_but_original_eighty_limit_stays(self):
        d=parent(tuple('T'+str(i) for i in range(85)));out=m.selection(d,datetime.fromisoformat(AT.replace('Z','+00:00')))
        self.assertEqual(len(out['occurrences']),85);self.assertEqual(len(out['selected_tickers']),80)
        self.assertEqual(out['occurrences'][-1]['status'],'outside_original_eighty_limit')
        self.assertFalse(out['selection_is_rank_or_recommendation'])
    def test_wrong_parent_legacy_eligible_and_ambiguous_members_cannot_seed_requests(self):
        edits=[{},parent(('SYNA','SYNA')),dict(parent(),sizing_eligible=True),dict(parent(),generated_at='2099-01-01T00:00:00Z')]
        d=parent();d['request_records'][0]['ticker']='OTHER';edits.append(d)
        d=parent();d['universe_membership']['selected'][0]['request_index']=False;edits.append(d)
        for d in edits:
            with self.assertRaises(ValueError):m.selection(d,datetime.fromisoformat(AT.replace('Z','+00:00')))
    def test_strict_parser_exact_decimal_and_whole_gzip_duplicate_nonfinite_rejection(self):
        self.assertEqual(m.strict(b'{"volume":100.05}')['volume'],Decimal('100.05'))
        self.assertEqual(m.strict(gzip.compress(b'[]'),'gzip'),[])
        for raw,enc in [(b'{"x":1,"x":2}',''),(b'{"x":NaN}',''),(b'{"x":1e999}',''),(gzip.compress(b'[]')+b'extra','gzip'),(b'[]','gzip')]:
            with self.assertRaises((ValueError,UnicodeError)):m.strict(raw,enc)
    def test_missing_hash_binding_order_and_provider_scope_fail(self):
        for edit in (lambda a,b,s:s.__setitem__(b[0]['original_ref']['key'],b'[]'),
                     lambda a,b,s:b[0].update(endpoint='https://example.invalid'),
                     lambda a,b,s:b[0].update(received_at='2099-01-01T00:00:00Z'),
                     lambda a,b,s:b.clear(),lambda a,b,s:b[0].update(http_status=True),
                     lambda a,b,s:a['breakout'].update(status='received')):
            a,b,s=captures();edit(a,b,s)
            with self.assertRaises(ValueError):m.build(a,b,s,AT)
    def test_explicit_empty_population_does_not_fabricate_measurements(self):
        a,b,s=captures(());p=m.build(a,b,s,AT)
        self.assertEqual(p['selection']['status'],'reported_empty_research_scope');self.assertEqual(p['volume_observations'],[])
        self.assertIsNone(p['n_fired']);self.assertEqual(p['coverage']['eligible_votes'],0)
    def test_failed_secondary_issuer_remains_unavailable_without_zero_metrics(self):
        a,b,s=captures(('SYNA','OTHER'));b[1].update(status='not_attempted_stop',network_attempted=False,http_status=None);b[1].pop('original_ref')
        p=m.build(a,b,s,AT);r=p['volume_observations'][1]
        self.assertIsNone(r['source_records']);self.assertTrue(all(x['value'] is None for x in r['measurements']))
        self.assertEqual(p['coverage']['planned_provider_requests'],2)
    def test_invalid_only_response_is_not_a_new_successful_publication(self):
        for raw in (b'{}',b'{"x":NaN}',b'[]'):
            a,b,s=captures();ref=store.identity(raw,m.PRIVATE,'sources');s[ref['key']]=raw;b[0]['original_ref']=ref
            with self.assertRaises(ValueError):m.build(a,b,s,AT)
    def test_original_provider_and_window_have_no_embedded_credential(self):
        url=m.endpoint('BRK.B',ASOF)
        self.assertEqual(url,'https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=BRK.B&from=2026-08-02&to=2026-09-28')
        with self.assertRaises(ValueError):m.endpoint('ABC&apikey=bad',ASOF)


if __name__=='__main__':unittest.main(verbosity=2)
