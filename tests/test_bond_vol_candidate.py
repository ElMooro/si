"""Exact units, independent variance, dates, gaps, priors, quote identity and authority."""
from pathlib import Path
from copy import deepcopy
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_UP
import ast,hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/shared/tests','aws/ops/checks','scripts')]
import bond_vol_candidate as model
import verify_bond_vol_arithmetic as independent
from test_report_observations import inputs,NOW

def ref(raw=b'whole predecessor'):
    digest=hashlib.sha256(raw).hexdigest();return {'key':model.PRIVATE+digest+'.bin','sha256':digest,'bytes':len(raw)}
def context():return {'source_key':'data/funding-plumbing.json','status':'retained_unqualified_context','original':ref(b'whole funding packet'),'independent_votes':0}
def fixture(n=320,flat=False,alternating=False):
    originals={}
    for j,sid in enumerate(model.SERIES):
        rows=[]
        for i in range(n):
            value=Decimal('4')+(Decimal(i%2)/100 if alternating else Decimal(0) if flat else Decimal((i*i+3*j*i)%127)/1000)
            rows.append((str(date(2026,9,17)-timedelta(days=i)),str(value)))
        item=inputs(sid,'D',rows,'Percent');item['definition']['seriess'][0]['frequency']=model.SPECS[sid]['reviewed_definition'][2]
        originals[sid]=item
    return originals
def source(originals):
    available={s:o for s,o in originals.items() if o is not None}
    with localcontext() as precision:
        precision.prec=28;precision.rounding='ROUND_HALF_EVEN'
        out=model.observations.build({s:{} for s in available},available,NOW)
    out['replay']={'manifest_key':'data/report-research/runs/'+'a'*64+'.json','output_sha256':model.observations.digest(out)}
    return out
def quote(name='ICE BofA MOVE Index',values=(90.12,91.25)):
    stamps=[int(datetime(2026,9,16+i,13,30,tzinfo=timezone.utc).timestamp()) for i in range(len(values))]
    doc={'chart':{'error':None,'result':[{'meta':{'symbol':'^MOVE','instrumentType':'INDEX','exchangeTimezoneName':'America/New_York',
        'dataGranularity':'1d','longName':name,'shortName':name},'timestamp':stamps,'indicators':{'quote':[{'close':list(values)}]}}]}}
    return doc
def receipt(raw):return {'source_url':model.MOVE_URL,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'http_status':200,'acquired_at':NOW}
def build(originals,stamp=NOW,doc=None):
    raw=json.dumps(doc).encode() if doc is not None else None
    return model.build(source(originals),originals,stamp,context(),ref(),raw,receipt(raw) if raw else None)

class Tests(unittest.TestCase):
    def test_full_predecessor_scope(self):
        tree=ast.parse((ROOT/'aws/lambdas/justhodl-bond-vol/tests/legacy_lambda_function.py.txt').read_text(encoding='utf8'))
        channels=next(n.value for n in tree.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='CHANNELS')
        self.assertEqual(tuple(r['fred'] for r in ast.literal_eval(channels)),model.SERIES)

    def test_complete_originals_and_independent_all_window_replay(self):
        originals=fixture();before=deepcopy(originals);out=build(originals)
        proof=independent.verify(json.loads(model.observations.encoded(out)),source(originals),originals)
        self.assertEqual(proof['original_rows'],3200);self.assertEqual(proof['rolling_windows'],2900)
        self.assertEqual(proof['current_series'],10);self.assertEqual(originals,before)
        self.assertEqual(out['series']['DGS10']['historical_distribution']['prior_window_count'],252)

    def test_true_zero_and_nonzero_flat_baselines_have_no_invented_zscore(self):
        for options in ({'flat':True},{'alternating':True}):
            originals=fixture(**options);out=build(originals);independent.verify(out,source(originals),originals)
            for row in out['series'].values():
                self.assertEqual(row['historical_distribution']['status'],'flat_baseline')
                self.assertIsNone(row['historical_distribution']['z_score']['value'])
                self.assertEqual(row['historical_distribution']['midrank_percentile']['value'],50)

    def test_current_spike_is_excluded_from_baseline_and_not_clipped(self):
        originals=fixture();base=build(originals)['series']['DGS10']['historical_distribution']
        originals['DGS10']['observations']['observations'][0]['value']='40'
        out=build(originals);independent.verify(out,source(originals),originals)
        dist=out['series']['DGS10']['historical_distribution']
        self.assertEqual(base['mean_bp'],dist['mean_bp']);self.assertEqual(base['sample_sd_bp'],dist['sample_sd_bp'])
        self.assertGreater(dist['z_score']['value'],3);self.assertIsNone(out['composite_z_score'])

    def test_missing_latest_never_backfills_current_and_all_rows_remain(self):
        originals=fixture();originals['DGS2']['observations']['observations'][0]['value']='.'
        out=build(originals);independent.verify(out,source(originals),originals)
        row=out['series']['DGS2'];self.assertIsNone(row['current']);self.assertIsNotNone(row['last_calculated'])
        self.assertEqual(len(row['original_rows']),320);self.assertEqual(row['original_rows'][0]['value'],'.')

    def test_missing_interior_rows_and_unequal_intervals_are_disclosed(self):
        originals=fixture();originals['DGS10']['observations']['observations'][5]['value']='.'
        out=build(originals);independent.verify(out,source(originals),originals)
        last=out['series']['DGS10']['current'];self.assertEqual(last['missing_rows_inside_window'],1)
        self.assertEqual(last['elapsed_calendar_days'],31);self.assertEqual(last['max_interval_days'],2)

    def test_long_source_gap_withholds_current_without_deleting_history(self):
        originals=fixture()
        for row in originals['DGS10']['observations']['observations'][1:10]:row['value']='.'
        out=build(originals);independent.verify(out,source(originals),originals)
        row=out['series']['DGS10'];self.assertIsNone(row['current']);self.assertEqual(row['last_calculated']['max_interval_days'],10)
        self.assertEqual(row['historical_distribution']['status'],'noncomparable_intervals')

    def test_acquisition_cutoff_cannot_promote_future_rows_when_republished(self):
        originals=fixture();doc=originals['DGS10']['observations'];doc['observations'].insert(0,{'date':'2026-09-19','value':'99'});doc['count']+=1
        out=build(originals,'2026-09-20T00:00:00+00:00');independent.verify(out,source(originals),originals)
        row=out['series']['DGS10'];self.assertEqual(row['latest_observation']['date'],'2026-09-17');self.assertEqual(len(row['original_rows']),321)
        self.assertIsNone(row['current'])

    def test_unreviewed_units_missing_channel_and_short_history_stay_explicit(self):
        originals=fixture(n=60);originals['DGS2']=None;originals['DGS10']['definition']['seriess'][0]['units']='Index'
        out=build(originals);independent.verify(out,source(originals),originals)
        self.assertEqual(len(out['series']),10);self.assertEqual(out['series']['DGS10']['quality']['status'],'definition_mismatch')
        self.assertEqual(out['series']['DGS10']['rolling_history'],[])
        self.assertEqual(out['series']['DGS5']['historical_distribution']['status'],'insufficient_prior_windows')
        self.assertIsNotNone(out['series']['DGS5']['current'])

    def test_decimal_context_cannot_change_calculations(self):
        originals=fixture(n=65);expected=build(originals)
        with localcontext() as ctx:
            ctx.prec=9;ctx.rounding=ROUND_UP
            actual=build(originals)
        self.assertEqual(expected,actual)

    def test_duplicate_dates_invalid_values_and_false_replay_identity_rejected(self):
        originals=fixture(n=40)
        for value in (True,'NaN','Infinity',123):
            changed=deepcopy(originals);changed['DGS10']['observations']['observations'][0]['value']=value
            with self.assertRaises((ValueError,TypeError,ArithmeticError)):build(changed)
        changed=deepcopy(originals);changed['DGS10']['observations']['observations'][0]['date']=changed['DGS10']['observations']['observations'][1]['date']
        with self.assertRaises(ValueError):build(changed)
        packet=source(originals);packet['generated_at']='2026-09-18T19:00:00+00:00'
        with self.assertRaises(ValueError):model.build(packet,originals,NOW,context(),ref())

    def test_matching_symbol_with_wrong_name_is_quarantined_with_complete_quote(self):
        doc=quote('Northern Trust iBoxx 5-Year Tar');raw=json.dumps(doc).encode();originals=fixture(n=40);out=build(originals,doc=doc)
        proof=independent.verify(out,source(originals),originals,raw,receipt(raw))
        self.assertEqual(proof['quote_rows'],2);self.assertEqual(out['move']['status'],'identity_mismatch')
        self.assertEqual(out['move']['original'],doc);self.assertIsNone(out['move']['current']);self.assertFalse(out['move']['is_proxy'])

    def test_whole_real_quote_response_retains_all_501_rows_and_identity_conflict(self):
        raw=(ROOT/'tests/fixtures/bond-vol-yahoo-identity-conflict.json').read_bytes()
        receipt=json.loads((ROOT/'tests/fixtures/bond-vol-yahoo-identity-conflict-receipt.json').read_bytes())
        out=model.move_quote(raw,receipt,receipt['acquired_at'])
        self.assertEqual(hashlib.sha256(raw).hexdigest(),'59d9a27f7e57bc30530c37392c6bd1ddaa9aa137f190f454f94b46f52b16f79f')
        self.assertEqual(len(raw),51209);self.assertEqual(len(out['history']),501)
        self.assertEqual(out['status'],'identity_mismatch');self.assertIsNone(out['current'])
        independent.verify_quote(out,raw,receipt,receipt['acquired_at'])

    def test_previous_session_quote_and_same_day_provisional_status(self):
        originals=fixture(n=40);doc=quote();raw=json.dumps(doc).encode();out=build(originals,doc=doc)
        independent.verify(out,source(originals),originals,raw,receipt(raw));self.assertEqual(out['move']['current']['reported_close'],91.25)
        doc=quote(values=(90,91,92));out=build(originals,doc=doc)
        self.assertEqual(out['move']['status'],'provisional_or_future_session');self.assertIsNone(out['move']['current'])

    def test_quote_length_mismatch_null_tail_and_bad_payload_do_not_substitute_a_proxy(self):
        doc=quote();doc['chart']['result'][0]['indicators']['quote'][0]['close'].pop()
        raw=json.dumps(doc).encode();out=model.move_quote(raw,receipt(raw),NOW)
        self.assertEqual(out['status'],'schema_mismatch');self.assertEqual(out['original'],doc)
        raw=json.dumps(quote(values=(90,None))).encode();out=model.move_quote(raw,receipt(raw),NOW)
        self.assertEqual(out['status'],'missing_latest_close');self.assertIsNone(out['current'])
        for raw in (b'not JSON',b'null',b'[]',b'{"chart":[]}',b'{"chart":{"result":[null]}}'):
            out=model.move_quote(raw,receipt(raw),NOW);self.assertEqual(out['status'],'schema_mismatch');self.assertIsNone(out['current'])

    def test_quote_original_or_receipt_tampering_rejected(self):
        raw=json.dumps(quote()).encode();ref=receipt(raw);ref['sha256']='0'*64
        with self.assertRaises(ValueError):model.move_quote(raw,ref,NOW)

    def test_pinned_timezone_preserves_winter_and_summer_date_boundaries(self):
        zone=model.pinned_timezone.new_york()
        self.assertEqual(datetime(2026,1,15,4,30,tzinfo=timezone.utc).astimezone(zone).date().isoformat(),'2026-01-14')
        self.assertEqual(datetime(2026,7,15,4,30,tzinfo=timezone.utc).astimezone(zone).date().isoformat(),'2026-07-15')

    def test_verifier_does_not_accept_a_false_schema_rejection(self):
        raw=json.dumps(quote()).encode();out=model.move_quote(raw,receipt(raw),NOW)
        out.update(status='schema_mismatch',current=None,history=[])
        with self.assertRaises(AssertionError):independent.verify_quote(out,raw,receipt(raw),NOW)

    def test_independent_verifier_rejects_tampered_math_missing_rows_and_authority(self):
        originals=fixture(n=60);packet=source(originals);out=build(originals)
        for edit in (lambda o:o['series']['DGS10']['rolling_history'][0]['step_dispersion_bp'].update(value=999),
            lambda o:o['series']['DGS10']['original_rows'].pop(),lambda o:o.update(calls_eligible=True),
            lambda o:o['dependency_graph'].update(independent_votes=10)):
            changed=deepcopy(out);edit(changed)
            with self.assertRaises(AssertionError):independent.verify(changed,packet,originals)

if __name__=='__main__':unittest.main(verbosity=2)
