"""Desk joins retain every configured fund, original clock and separate history."""
from pathlib import Path
from unittest import mock
import copy,json,sys,unittest
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).parent)]
import etf_desk_model as model
import etf_desk_catalog as catalog
import provider_flow_catalog as flow_catalog
from test_provider_flow_research import fixtures as flow_rows, retain as flow_collection
from test_etf_profile_native import row as profile_row
from test_etf_holdings_native import row as holding_row
AT='2026-09-21T10:00:00+00:00'


def fixture(desk=('SPY','VOO','BND'),canonical=('SPY','VOO')):
    objects={}
    def emit(key,body):objects[key]=body
    def retained(prefix,doc):
        raw=model.encoded(doc);digest=model.sha(raw);key=prefix+digest+'.bin';objects[key]=raw
        return {'key':key,'sha256':digest,'bytes':len(raw)}
    def pair(ticker,kind):
        mod=model.profile if kind=='profile' else model.holdings
        out={}
        for role,cutoff,processed,effective in [('current','2026-09-21','2026-09-18','2026-09-18' if kind=='profile' else '2026-08-31'),('prior','2026-08-22','2026-08-21','2026-08-21' if kind=='profile' else '2026-07-31')]:
            rows=[profile_row(composite_ticker=ticker,processed_date=processed,effective_date=effective,num_holdings=515 if role=='current' else 517)] if kind=='profile' else [holding_row(composite_ticker=ticker,processed_date=processed,effective_date=effective),holding_row(composite_ticker=ticker,processed_date=processed,effective_date=effective,constituent_name='Cash',constituent_ticker=None,figi=None,weight=0,shares_held=0,constituent_rank=2)]
            def page(url,values):return {'url':url,'acquired_at':'2026-09-21T09:00:00+00:00','original':retained(mod.PRIVATE,{'status':'OK','results':values,'count':len(values)})}
            out[role]={'ticker':ticker,'cutoff':cutoff,'status':'complete_returned_profile_snapshot' if kind=='profile' else 'complete_returned_snapshot',
                'selection':page(mod.selection_url(ticker,cutoff),rows[:1]),'pages':[page(mod.snapshot_url(ticker,processed),rows)]}
        return out
    dates=sorted(r['effective_date'] for r in flow_rows());end=dates[-1]
    grid={'contract':'provider-reporting-reference.v1','ticker':'SPY','dates':dates,'exchange_calendar_verified':False}
    raw=model.encoded(grid);key=model.flow_model.PREFIX+'histories/'+model.sha(raw)+'.json';emit(key,raw)
    grid_ref={'key':key,'bytes':len(raw),'sha256':model.sha(raw),'end_date':end,'observations':len(dates)}
    flows={};holdings={};extra_flows={};extra_holdings={}
    for ticker in sorted(set(canonical)|set(desk)):
        f,raw=flow_collection(ticker,flow_rows(ticker));objects.update(raw);h=pair(ticker,'holdings')
        if ticker in canonical:
            flows[ticker]=model.extra_flow(ticker,f,objects.__getitem__,AT,dates,end,emit)
            holdings[ticker]=model.extra_holdings(h,objects.__getitem__,AT,emit)
        else:extra_flows[ticker]=f;extra_holdings[ticker]=h
    contexts={}
    for ticker in desk:
        key='data/etf-flow-hist/'+ticker+'.json'
        whole={'generated_at':'2026-09-20T22:20:00Z','d':['2017-04-03','2025-01-03','2026-09-18'],'f':[0,None,12],'unknown_future_field':{'kept':[True,None]}}
        contexts[key]={'source_key':key,**retained(model.profile.PRIVATE,whole)}
    cf={'contract':model.flow_model.CONTRACT,'generated_at':AT,'funds':flows,'reference':grid_ref,'replay':{'fixture':'canonical-flow'},**model.permissions()}
    ch={'contract':model.holdings_model.CONTRACT,'generated_at':AT,'funds':holdings,'replay':{'fixture':'canonical-holdings'},**model.permissions()}
    inputs={'contract':'etf-desk-inputs.v1','generated_at':AT,'query_date':'2026-09-21',
        'profiles':{t:pair(t,'profile') for t in desk},'extra_flows':extra_flows,'extra_holdings':extra_holdings,
        'contexts':contexts,'canonical_flows':{'fixture':'flow-ref'},'canonical_holdings':{'fixture':'holding-ref'},
        'provider_requests':len(desk)*4+len(extra_flows)*5,'original_provider_bytes':sum(map(len,objects.values())),'previous':None}
    return inputs,objects,cf,ch


class DeskJoin(unittest.TestCase):
    def run_model(self,inputs,objects,cf,ch,desk=('SPY','VOO','BND')):
        with mock.patch.object(catalog,'DESK',desk):
            return model.build(inputs,objects.__getitem__,lambda k,b:objects.__setitem__(k,b),cf,ch)
    def test_full_original_scope_including_sixteen_noncanonical_funds(self):
        desk=catalog.DESK;i,o,cf,ch=fixture(desk,tuple(flow_catalog.ETF_UNIVERSE))
        result=self.run_model(i,o,cf,ch,desk)
        self.assertEqual(len(result['funds']),116);self.assertEqual(result['quality']['canonical_overlap'],100)
        self.assertEqual(set(result['quality']['additional_funds']),set(desk)-set(flow_catalog.ETF_UNIVERSE))
        self.assertEqual(len(result['quality']['additional_funds']),16)
        self.assertEqual(result['funds']['SPY']['flows'],cf['funds']['SPY'])
        self.assertEqual(result['funds']['VOO']['holdings'],ch['funds']['VOO'])
        self.assertEqual(result['funds']['BND']['source_basis'],'additional_desk_originals')
        self.assertEqual(result['quality']['independent_investment_votes'],0)
        self.assertTrue(all(result[k] is False for k in model.PERMISSIONS))
    def test_profile_and_holdings_clocks_counts_remain_separate(self):
        i,o,cf,ch=fixture();p=self.run_model(i,o,cf,ch);fund=p['funds']['BND']
        self.assertEqual(fund['profiles']['current']['effective_date'],'2026-09-18')
        self.assertEqual(fund['holdings']['current']['effective_dates'],{'2026-08-31':2})
        diagnostic=fund['reported_count_comparison']
        self.assertEqual(diagnostic['profile_count_decimal'],'515');self.assertEqual(diagnostic['returned_constituent_rows'],2)
        self.assertFalse(diagnostic['same_effective_date']);self.assertFalse(diagnostic['equivalent_counting_scope_verified'])
    def test_long_history_retained_whole_and_never_merged(self):
        i,o,cf,ch=fixture();before=copy.deepcopy(o);p=self.run_model(i,o,cf,ch)
        old=p['funds']['BND']['legacy_history'];self.assertEqual(old['reported_first_date'],'2017-04-03')
        self.assertFalse(old['merged_into_verified_history'])
        ref=old['retained_original'];self.assertEqual(o[ref['key']],before[ref['key']])
        h=json.loads(o[p['funds']['BND']['flows']['history']['key']])
        self.assertEqual(len(h['history']),25);self.assertNotEqual(h['history'][0]['date'],'2017-04-03')
    def test_new_desk_clock_does_not_refresh_canonical_observations(self):
        i,o,cf,ch=fixture();i['generated_at']='2026-09-23T10:00:00+00:00'
        p=self.run_model(i,o,cf,ch)
        self.assertEqual(p['funds']['SPY']['flows'],cf['funds']['SPY'])
        self.assertFalse(p['funds']['SPY']['quality']['flow_current_eligible'])
        window=p['matched_desk_totals']['5']
        self.assertEqual(window['status'],'incomplete');self.assertIsNone(window['flow_usd_decimal'])
        self.assertIn('SPY',window['excluded']);self.assertEqual(p['canonical_sources']['flows']['generated_at'],AT)
    def test_scope_cutoff_and_source_authority_cannot_be_silently_changed(self):
        for mode in ('profile','extra_flow','extra_holdings','cutoff','authority','future'):
            i,o,cf,ch=fixture()
            if mode=='profile':del i['profiles']['BND']
            elif mode=='extra_flow':del i['extra_flows']['BND']
            elif mode=='extra_holdings':del i['extra_holdings']['BND']
            elif mode=='cutoff':i['profiles']['BND']['prior']['cutoff']='2026-08-23'
            elif mode=='authority':cf['sizing_eligible']=True
            elif mode=='future':cf['generated_at']='2026-09-22T10:00:00Z'
            with self.subTest(mode=mode),self.assertRaises(ValueError):self.run_model(i,o,cf,ch)
    def test_profile_changes_require_distinct_dates_and_keep_units(self):
        i,o,cf,ch=fixture();p=self.run_model(i,o,cf,ch)
        comparison=p['funds']['BND']['profiles']['comparison']
        count=next(x for x in comparison['fields'] if x['field']=='num_holdings')
        fee=next(x for x in comparison['fields'] if x['field']=='net_expenses')
        missing=next(x for x in comparison['fields'] if x['field']=='total_expenses')
        self.assertEqual(count['difference_raw_decimal'],'-2');self.assertTrue(count['unit_certified'])
        self.assertFalse(fee['unit_certified']);self.assertIsNone(missing['difference_raw_decimal'])
        a=p['funds']['BND']['profiles']['current']
        same=model.profile_changes(a,a)
        self.assertFalse(same['distinct_effective_dates']);self.assertTrue(all(v['difference_raw_decimal'] is None for v in same['fields']))
    def test_missing_ticker_cash_rows_remain_in_extra_holdings(self):
        i,o,cf,ch=fixture();p=self.run_model(i,o,cf,ch)
        ref=p['funds']['BND']['holdings']['current']['snapshot'];snap=model.checked(ref,o.__getitem__)
        rows=[r for part in snap['parts'] for r in model.checked(part,o.__getitem__)['rows']]
        self.assertEqual(len(rows),2);self.assertIsNone(rows[1]['constituent_ticker']);self.assertEqual(rows[1]['weight_raw_decimal'],'0')
    def test_same_input_reconstructs_every_published_artifact(self):
        i,o,cf,ch=fixture();p=self.run_model(i,o,cf,ch);before=dict(o)
        q=self.run_model(i,o,cf,ch);self.assertEqual(p,q);self.assertEqual(before,o)
        ref=p['reference'];o[ref['key']]+=b' '
        with self.assertRaisesRegex(ValueError,'source reference differs'):self.run_model(i,o,cf,ch)

if __name__=='__main__':unittest.main(verbosity=2)
