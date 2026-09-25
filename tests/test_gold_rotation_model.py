from pathlib import Path
from datetime import date,timedelta
from decimal import Decimal
import copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import gold_rotation_model as m
AT='2026-09-25T00:40:30+00:00'

def fixture():
    days=[];day=date(2026,9,24)
    while len(days)<270:
        if day.weekday()<5:days.append(str(day))
        day-=timedelta(days=1)
    blobs={};docs={};captures={};start='2024-04-08';end='2026-09-24'
    def put(raw):
        ref={'key':m.PRIVATE+m.sha(raw)+'.bin','sha256':m.sha(raw),'bytes':len(raw)};blobs[ref['key']]=raw;return ref
    for si,(symbol,(isin,exchange,label)) in enumerate(m.INSTRUMENTS.items()):
        for kind in m.KINDS:
            if kind=='profile':doc=[{'symbol':symbol,'isin':isin,'exchange':exchange,'currency':'USD','isEtf':True,'companyName':label}]
            else:
                doc=[]
                for i,day in enumerate(days):
                    price=Decimal(100+si*20)-Decimal(i)/10
                    if kind=='dividend-adjusted':price-=Decimal(i)/100
                    values={f:str(1000+i) if f=='volume' else str(price+1) if f in ('high','adjHigh') else str(price-1) if f in ('low','adjLow') else str(price) for f in m.FIELDS[kind]}
                    doc.append({'symbol':symbol,'date':day,**values})
            key=symbol+':'+kind;docs[key]=doc
            captures[key]={'symbol':symbol,'kind':kind,'source_url':m.source_url(symbol,kind,start,end),
                'received_at':'2026-09-25T00:39:00Z','http_status':200,'status':'response_retained','original':put(m.encoded(doc))}
    predecessor=put(m.encoded({'engine':'gold-equity-rotation','version':'1.0.0','as_of':'2026-09-24T22:45:00Z','trade_tickets':[{'side':'LONG','size_pct_portfolio':3}]}))
    return {'contract':'gold-rotation-inputs.v1','generated_at':AT,'range':{'from':start,'to':end},'predecessor':predecessor,'captures':captures},blobs,docs

def replace(inputs,blobs,docs,key):
    raw=m.encoded(docs[key]);ref={'key':m.PRIVATE+m.sha(raw)+'.bin','sha256':m.sha(raw),'bytes':len(raw)}
    blobs[ref['key']]=raw;inputs['captures'][key]['original']=ref

class Tests(unittest.TestCase):
    def test_originals_and_decimal_observations_are_preserved_without_forecast(self):
        inputs,blobs,_=fixture();out=m.compile_output(inputs,blobs.__getitem__)
        self.assertEqual(out['quality']['instruments_with_history'],8);self.assertEqual(len(out['instruments']['GLD']['history']),270)
        self.assertEqual(out['ratios']['full']['coverage']['matched_dates'],270)
        self.assertTrue(all(out[k] is False for k in m.FLAGS));self.assertEqual(out['trade_tickets'],[])
        self.assertTrue(all(v is None for v in out['current_metrics'].values()));self.assertEqual(out['independent_investment_votes'],0)
        self.assertEqual(out['instruments']['GLD']['first_publication_at'],None)

    def test_order_does_not_change_dated_arithmetic(self):
        inputs,blobs,docs=fixture();before=m.compile_output(inputs,blobs.__getitem__)
        docs['GLD:full'].reverse();replace(inputs,blobs,docs,'GLD:full');after=m.compile_output(inputs,blobs.__getitem__)
        self.assertEqual(before['ratios']['full']['returns'],after['ratios']['full']['returns'])
        self.assertEqual(before['ratios']['full']['moving_averages'],after['ratios']['full']['moving_averages'])
        self.assertNotEqual(before['ratios']['full']['latest']['gld_row_index'],after['ratios']['full']['latest']['gld_row_index'])

    def test_missing_date_never_shifts_a_positional_ratio_or_shortens_window(self):
        inputs,blobs,docs=fixture();missing=docs['GLD:full'][3]['date'];docs['GLD:full'].pop(3);replace(inputs,blobs,docs,'GLD:full')
        out=m.compile_output(inputs,blobs.__getitem__);ratio=out['ratios']['full']
        row=next(r for r in ratio['history'] if r['date']==missing)
        self.assertIsNone(row['value_decimal']);self.assertIsNotNone(row['numerator_decimal'])
        self.assertIsNone(ratio['returns']['20']['value_decimal']);self.assertIsNone(ratio['moving_averages']['50']['value_decimal'])
        self.assertIsNone(ratio['persistence']['direction'])

    def test_endpoint_adjustments_remain_separate(self):
        inputs,blobs,_=fixture();out=m.compile_output(inputs,blobs.__getitem__);asset=out['instruments']['GLD']
        self.assertNotEqual(asset['returns']['full']['20']['value_decimal'],asset['returns']['dividend-adjusted']['20']['value_decimal'])
        self.assertEqual(asset['legacy_light_reconciliation']['different_close_dates'],[])
        self.assertFalse(asset['legacy_light_reconciliation']['equality_establishes_adjustment_method'])

    def test_duplicate_dates_wrong_currency_and_invalid_ohlc_cannot_supply_measurements(self):
        for issue in ('duplicate','currency','ohlc'):
            inputs,blobs,docs=fixture();key='GLD:profile' if issue=='currency' else 'GLD:full'
            if issue=='duplicate':docs[key].append(docs[key][0])
            elif issue=='currency':docs[key][0]['currency']='EUR'
            else:docs[key][0]['high']='1'
            replace(inputs,blobs,docs,key);out=m.compile_output(inputs,blobs.__getitem__)
            self.assertIsNone(out['ratios']['full']['latest']['value_decimal'])
            self.assertIsNone(out['ratios']['full']['returns']['20']['value_decimal'])

    def test_no_variance_gives_null_zscore_instead_of_infinite_conviction(self):
        inputs,blobs,docs=fixture()
        for key in ('SPY:full','GLD:full'):
            for row in docs[key]:row.update(open='100',high='101',low='99',close='100',vwap='100')
            replace(inputs,blobs,docs,key)
        z=m.compile_output(inputs,blobs.__getitem__)['ratios']['full']['zscore']
        self.assertIsNone(z['value_decimal']);self.assertEqual(z['reason'],'zero_reference_variance')

    def test_changed_original_source_path_or_capture_time_fails_closed(self):
        for issue in ('bytes','url','time','private'):
            inputs,blobs,_=fixture();capture=inputs['captures']['GLD:full']
            if issue=='bytes':blobs[capture['original']['key']]+=b' '
            elif issue=='url':capture['source_url']='https://example.com/data'
            elif issue=='time':capture['received_at']='2026-09-26T00:00:00Z'
            else:capture['original']['key']='data/trade-tickets.json'
            with self.assertRaises(ValueError):m.compile_output(inputs,blobs.__getitem__)

    def test_provider_error_and_transport_failure_remain_missing(self):
        inputs,blobs,_=fixture()
        inputs['captures']['REM:full'].update(status='transport_unavailable',original=None)
        inputs['captures']['REM:dividend-adjusted'].update(status='provider_error_retained',http_status=403)
        out=m.compile_output(inputs,blobs.__getitem__)
        self.assertIsNone(out['instruments']['REM']['returns']['full']['20']['value_decimal'])
        self.assertIsNone(out['instruments']['REM']['returns']['dividend-adjusted']['20']['value_decimal'])
        self.assertIsNotNone(out['instruments']['REM']['returns']['light']['20']['value_decimal'])

    def test_source_numeric_tokens_are_not_rounded_through_binary_float(self):
        value=m.strict(b'{"close":100.123456789012345678901}',True)['close']
        self.assertEqual(m.ds(value),'100.123456789012345678901')
        for bad in (True,'NaN','Infinity',-1):
            with self.assertRaises(ValueError):m.number(bad,True)

if __name__=='__main__':unittest.main(verbosity=2)
