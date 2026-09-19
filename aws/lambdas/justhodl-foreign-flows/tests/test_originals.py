import copy,hashlib,io,json,sys,unittest,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-foreign-flows/source')]
import foreign_original as n
import foreign_research as m

AT='2026-09-19T16:40:00+00:00'
def retained(raw,url=n.CSLT_URL):
    source=n.evidence_store.public_source_url(url);sha=hashlib.sha256(raw).hexdigest();request=hashlib.sha256(source.encode()).hexdigest()
    key=f'data/evidence/tic/{request}/{sha}.bin.gz'
    ref={'url':url,'acquired_at':AT,'evidence':{'contract':'source-evidence.v1','captured':True,'provider':'tic','source_url':source,
         'first_received_at':AT,'sha256':sha,'bytes':len(raw),'key':key}}
    return ref,lambda wanted:raw if wanted==key else (_ for _ in ()).throw(KeyError(wanted))

def series(code='99996',name='Grand Total',family='lt_total',measure='net',state='A',values=None):
    return {'source_id':'for_'+family+'_'+measure+'_'+code,
        'metadata':{'title':n.MEASURES[measure]+n.FAMILIES[family]+': '+name+(' (DISCONTINUED)' if state=='D' else ''),
            'frequency':'M','season':'NSA','units':'Millions of Dollars','notes':'Synthetic source fixture.',
            'additional':{'status':state,'geography':{'name':name,'type':'country'}}},
        'observations':values if values is not None else [['2026-07-01','0.0'],['2026-06-01','100.0']]}

def archive(items):
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_DEFLATED) as z:z.writestr('cslt.json',json.dumps({'releaseID':'3','version':'2.0','series':items}))
    return retained(stream.getvalue())

def rows(values):return {day:{'date':day,'value_decimal':value,'row_index':i} for i,(day,value) in enumerate(values)}

class Tests(unittest.TestCase):
    def test_discontinued_combination_is_separate_from_current_countries(self):
        ref,read=archive([series(),series('10308','Belgium and Luxembourg','lt_treas','pos','D',[['2011-12-01','230960']]),
            series('10251','Belgium','lt_treas','pos'),series('11703','Luxembourg','lt_treas','pos')])
        groups,_=n.archive(ref,read,AT)
        self.assertEqual(groups['10308']['scope'],'historical_group');self.assertEqual(groups['10251']['name'],'Belgium')
        self.assertEqual(groups['10308']['series']['lt_treas:pos']['coverage']['last_observation'],'2011-12-01')
        self.assertEqual(groups['11703']['series']['lt_treas:pos']['rows'][-1]['value_decimal'],'0')
        self.assertEqual(m.COUNTRY_ALIASES['belgium'],'10251');self.assertEqual(m.COUNTRY_ALIASES['luxembourg'],'11703')

    def test_issuer_groups_are_not_countries(self):
        ref,read=archive([series(),series('82643','Issued by U.S. Depository Institutions','lt_corp','pos')])
        groups,_=n.archive(ref,read,AT);self.assertEqual(groups['82643']['scope'],'issuer_or_instrument_group')

    def test_source_identity_unit_precision_and_dates_fail_closed(self):
        for kind in ('title','unit','duplicate','fraction','future','day','boolean'):
            item=series();items=[item]
            if kind=='title':item['metadata']['title']='Unrelated metric'
            if kind=='unit':item['metadata']['units']='Billions of Dollars'
            if kind=='duplicate':items.append(copy.deepcopy(item))
            if kind=='fraction':item['observations'][0][1]='1.5'
            if kind=='future':item['observations'][0][0]='2027-01-01'
            if kind=='day':item['observations'][0][0]='2026-07-02'
            if kind=='boolean':item['observations'][0][1]=True
            ref,read=archive(items)
            with self.subTest(kind=kind),self.assertRaises(ValueError):n.archive(ref,read,AT)

    def test_hash_request_and_acquisition_clock_are_verified(self):
        ref,read=archive([series()]);bad=copy.deepcopy(ref);bad['url']='https://example.org/other.zip'
        with self.assertRaises(ValueError):n.archive(bad,read,AT)
        bad=copy.deepcopy(ref);bad['evidence']['bytes']+=1
        with self.assertRaises(ValueError):n.archive(bad,read,AT)
        with self.assertRaises(ValueError):n.archive(ref,lambda key:b'other bytes',AT)
        bad=copy.deepcopy(ref);bad['acquired_at']='2026-09-20T00:00:00Z'
        with self.assertRaises(ValueError):n.archive(bad,read,AT)

    def test_missing_native_month_is_retained_and_not_compressed(self):
        ref,read=archive([series(values=[['2026-07-01','.'],['2026-05-01','0']])]);groups,_=n.archive(ref,read,AT)
        native=groups['99996']['series']['lt_total:net']['rows'];mapping={r['date']:r for r in native}
        self.assertEqual(len(native),2);self.assertIsNone(native[-1]['value_decimal'])
        result=m.windows(mapping,'2026-07-01',3);self.assertIsNone(result['usd_million_decimal'])
        self.assertEqual(result['missing_months'],['2026-06-01','2026-07-01'])

    def test_period_age_uses_month_end_and_preserves_original_label(self):
        calendar={'nominal_expected_observation_month':'2026-07-01','calendar_expires_at':'2026-10-17T04:00:00Z','acquired_at':AT}
        q=n.quality('2026-07-01',AT,calendar,AT)
        self.assertEqual(q['observation_date'],'2026-07-01');self.assertEqual(q['observation_period_end'],'2026-07-31')
        self.assertEqual(q['observation_age_days'],50)
        self.assertEqual(n.quality('2026-06-01',AT,calendar,AT)['status'],'release_due_unverified')

    def test_stock_flow_residual_is_not_forced_to_zero(self):
        data={'lt_treas:pos':rows([('2026-06-01','100'),('2026-07-01','125')]),'lt_treas:net':rows([('2026-07-01','10')]),
              'lt_treas:valchg':rows([('2026-07-01','7')])}
        result=m.decompose(data,'lt_treas','2026-07-01',1)
        self.assertEqual(result['holdings_change_usd_million_decimal'],'25');self.assertEqual(result['other_change_residual_usd_million_decimal'],'8')
        self.assertEqual(result['status'],'complete');self.assertEqual(result['transaction_rows'],[0])
        data['lt_treas:valchg']=rows([('2026-06-01','7')]);result=m.decompose(data,'lt_treas','2026-07-01',1)
        self.assertIsNone(result['other_change_residual_usd_million_decimal']);self.assertEqual(result['missing_valuation_months'],['2026-07-01'])

    def test_stock_baseline_is_exact_calendar_month(self):
        data={'lt_treas:pos':rows([('2025-06-01','100'),('2026-07-01','125')])}
        result=m.decompose(data,'lt_treas','2026-07-01',12)
        self.assertIsNone(result['holdings_change_usd_million_decimal']);self.assertIsNone(result['prior_holdings_usd_million_decimal'])

    def test_table_rounding_bound_does_not_hide_disagreement(self):
        table={'rows':{'Belgium':{'values_usd_million_decimal':{'2026-07-01':'470700'},'source_row_index':6}}}
        areas={'10251':{'name':'Belgium','scope':'reported_country_or_territory'}}
        for raw,status in [('470750','within_reporting_rounding'),('470751','source_disagreement')]:
            checked=m.table_check(table,areas,{'10251':{'treas:pos':rows([('2026-07-01',raw)])}})
            self.assertEqual(checked[0]['status'],status)

    def test_table_other_is_a_residual_of_named_rows_not_legacy_other_country(self):
        areas={'99996':{'name':'Grand Total','scope':'reported_aggregate'},'10251':{'name':'Belgium','scope':'reported_country_or_territory'},
            '63908':{'name':'All Other','scope':'historical_group'}}
        table={'rows':{name:{'values_usd_million_decimal':{'2026-07-01':value},'source_row_index':i} for i,(name,value) in enumerate([('Belgium','300'),('Grand Total','1000'),('All Other','700')])}}
        maps={code:{'treas:pos':rows([('2026-07-01',value)])} for code,value in [('99996','1000'),('10251','300'),('63908','9999')]}
        result=m.table_check(table,areas,maps);other=next(v for v in result if v['name']=='All Other')
        self.assertEqual(other['native_value_usd_million_decimal'],'700');self.assertEqual(other['native_group_codes'],['99996','10251'])

    def test_zscore_excludes_current_and_requires_prior_calendar_coverage(self):
        at='2026-07-01';mapping=rows([(m.month_before(at,i),str(i)) for i in range(121)])
        first=m.zscore(mapping,at);mapping[at]['value_decimal']='1000';self.assertGreater(m.zscore(mapping,at),first)
        del mapping[m.month_before(at,60)];self.assertIsNone(m.zscore(mapping,at))
        self.assertIsNone(m.zscore(rows([(m.month_before(at,i),'0') for i in range(121)]),at))

    def test_composition_has_explicit_same_scope_and_never_fills_missing(self):
        maps={'99996':{'treas:net':rows([('2026-07-01','100')]),'lt_eqty:net':rows([('2026-07-01','40')])},
              '69995':{'treas:net':rows([('2026-07-01','999')])}}
        result=m.linear_series(maps,[('99996','treas:net',1),('99996','lt_eqty:net',-1)])
        self.assertEqual(result[0]['value_decimal'],'60');self.assertTrue(all(r['group']=='99996' for r in result[0]['source_rows']))
        del maps['99996']['lt_eqty:net']['2026-07-01'];result=m.linear_series(maps,[('99996','treas:net',1),('99996','lt_eqty:net',-1)])
        self.assertIsNone(result[0]['value_decimal']);self.assertEqual(result[0]['missing_components'],['99996:lt_eqty:net'])

    def test_holder_sum_requires_reconciliation_for_every_summed_month(self):
        at='2026-07-01';maps={code:{'lt_total:net':rows([(m.month_before(at,i),value) for i in range(12)])} for code,value in [('99996','100'),('99990','40'),('99991','60')]}
        result,_=m.holder_splits(maps,at);self.assertEqual(result['lt_total']['official']['sum_12m'],0.48)
        maps['99991']['lt_total:net'][m.month_before(at,6)]['value_decimal']='80';result,_=m.holder_splits(maps,at)
        self.assertEqual(result['lt_total']['official']['latest'],0.04);self.assertIsNone(result['lt_total']['official']['sum_12m'])
        self.assertFalse(result['lt_total']['rolling_twelve_months_reconciled'])


if __name__=='__main__':unittest.main(verbosity=2)
