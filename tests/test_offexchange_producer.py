from pathlib import Path
from datetime import date,datetime,timezone
from io import BytesIO
from unittest.mock import patch
import json,sys,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import offexchange_producer as producer
import offexchange_research_model as model
import test_offexchange_measurements as fixture_module
from test_option_flow_store import S3

class FixedDateTime(datetime):
    @classmethod
    def now(cls,tz=None):return cls(2026,9,25,3,0,0,tzinfo=timezone.utc)

def discovery(dataset):
    return {'partitionFields':['weekStartDate','tierIdentifier'] if dataset=='weeklySummary' else ['monthStartDate','tierIdentifier'],
        'availablePartitions':[{'partitions':v} for v in ([['2026-08-31','T1'],['2026-08-17','T2'],['2026-08-17','OTCE']] if dataset=='weeklySummary' else [['2026-07-01','NMS'],['2026-06-01','NMS'],['2026-07-01','OTCE']])]}

class Transport:
    def __init__(self,fail=False):self.calls=[];self.fail=fail
    def __call__(self,request,timeout):
        url=request.full_url;self.calls.append(url);f=fixture_module.Tests()
        if '/partitions/' in url:raw=model.encoded(discovery(url.rsplit('/',1)[-1]));headers={}
        elif '/daily/' in url:
            if '20260925' in url:raise urllib.error.HTTPError(url,404,'Not yet published',{},BytesIO(b'not published'))
            raw=f.daily();headers={}
        else:
            if self.fail:raise urllib.error.HTTPError(url,503,'Unavailable',{},BytesIO(b'unavailable'))
            body=json.loads(request.data);filters={v['fieldName']:v['fieldValue'] for v in body['compareFilters']};code=filters['summaryTypeCode']
            if 'weekStartDate' in filters:
                row=f.row(code);row.update(weekStartDate=filters['weekStartDate'],summaryStartDate=filters['weekStartDate'],tierIdentifier=filters['tierIdentifier']);rows=[row]
            else:
                rows=[f.monthrow(),f.monthrow(0,'De Minimis Firms','40')]
                for row in rows:row.update(monthStartDate=filters['monthStartDate'],summaryStartDate=filters['monthStartDate'])
            raw=model.encoded(rows);headers={'Record-Total':str(len(rows)),'Record-Offset':str(body['offset']),'Record-Limit':'2000'}
        response=BytesIO(raw);response.status=200;response.headers=headers;return response

class RaceS3(S3):
    def put_object(self,**request):
        if request['Key']==model.CURRENT and request.get('IfMatch'):self.data[model.CURRENT]=b'{"concurrent_writer":true}'
        return super().put_object(**request)

class Tests(unittest.TestCase):
    def previous(self):return model.encoded({'version':'legacy','generated_at':'2026-09-24T20:00:00Z','board':[{'ticker':'AAPL','score':99}]})
    def test_plan_uses_advertised_periods_and_keeps_reporting_tiers_separate(self):
        plan=producer.plan(date(2026,9,25),discovery('weeklySummary'),discovery('monthlySummary'))
        self.assertEqual(len(plan['partitions']),6);self.assertEqual(plan['partitions'][0]['period'],'2026-08-31');self.assertEqual(plan['partitions'][2]['period'],'2026-08-17')
        self.assertEqual(plan['daily_candidate_dates'],['2026-09-25','2026-09-24','2026-09-23'])
        monday=producer.plan(date(2026,9,28),discovery('weeklySummary'),discovery('monthlySummary'));self.assertEqual(monday['daily_candidate_dates'],['2026-09-28','2026-09-25','2026-09-24'])
    def test_missing_tier_duplicate_and_future_advertised_partitions_fail(self):
        for kind in ('missing','duplicate','future'):
            weekly=discovery('weeklySummary')
            if kind=='missing':weekly['availablePartitions']=weekly['availablePartitions'][:1]
            if kind=='duplicate':weekly['availablePartitions'].append(weekly['availablePartitions'][0])
            if kind=='future':weekly['availablePartitions'][0]['partitions'][0]='2026-10-05'
            with self.assertRaises(ValueError):producer.plan(date(2026,9,25),weekly,discovery('monthlySummary'))
    def test_native_run_replays_before_conditional_publication_and_duplicate_id_does_not_recollect(self):
        client=S3({model.CURRENT:self.previous()});transport=Transport()
        with patch.object(producer,'datetime',FixedDateTime):
            result=producer.run(client,'bucket','normal','actual-execution',transport=transport);count=len(transport.calls)
            again=producer.run(client,'bucket','normal','other-execution',transport=transport)
        self.assertTrue(result['published']);self.assertEqual(result,again);self.assertEqual(len(transport.calls),count)
        packet=model.strict(client.data[model.CURRENT]);self.assertEqual(packet['contract'],model.CONTRACT);self.assertEqual(packet['daily']['date'],'2026-09-24')
        self.assertTrue(all(packet[key] is False for key in model.FLAGS));self.assertEqual(packet['board'],[])
        native=[v for v in client.writes if v['Key']==model.CURRENT];self.assertEqual(len(native),1);self.assertIn('IfMatch',native[0]);self.assertEqual(native[0]['CacheControl'],'no-store')
    def test_provider_failure_preserves_exact_current_and_cannot_be_retried_with_same_id(self):
        old=self.previous();client=S3({model.CURRENT:old});transport=Transport(fail=True)
        with patch.object(producer,'datetime',FixedDateTime):
            with self.assertRaises(ValueError):producer.run(client,'bucket','failure','execution',transport=transport)
            count=len(transport.calls)
            with self.assertRaisesRegex(ValueError,'already attempted'):producer.run(client,'bucket','failure','execution',transport=transport)
        self.assertEqual(client.data[model.CURRENT],old);self.assertEqual(len(transport.calls),count)
        journals=[model.strict(value) for key,value in client.data.items() if '/requests/' in key]
        self.assertTrue(any(v.get('status')=='complete' and 'capture' in v and v['capture'].get('http_status')==503 for v in journals))
    def test_concurrent_publication_is_not_overwritten(self):
        client=RaceS3({model.CURRENT:self.previous()})
        with patch.object(producer,'datetime',FixedDateTime):result=producer.run(client,'bucket','race','execution',transport=Transport())
        self.assertFalse(result['published']);self.assertEqual(result['reason'],'concurrent_publication');self.assertEqual(client.data[model.CURRENT],b'{"concurrent_writer":true}')
    def test_observation_rollback_is_rejected_even_when_compilation_is_newer(self):
        old={'contract':model.CONTRACT,'generated_at':'2026-09-25T01:00:00Z','daily':{'date':'2026-09-24'},'coverage':[{'dataset':'weeklySummary','category':'ATS_W_SMBL','tier':'T1','period_start':'2026-08-31'}]}
        new={**old,'generated_at':'2026-09-25T02:00:00Z','daily':{'date':'2026-09-23'}}
        self.assertFalse(producer.not_older(new,old));new['daily']=old['daily'];new['coverage']=[];self.assertFalse(producer.not_older(new,old))
    def test_unreviewed_endpoint_and_small_runtime_budget_do_not_make_requests(self):
        client=S3({});transport=Transport()
        with self.assertRaises(ValueError):producer.fetch(client,'bucket','x','bad',{'url':'https://example.org','body':None,'kind':'probe'},999999999,transport)
        with self.assertRaises(ValueError):producer.run(client,'bucket','x','execution',remaining_seconds=20,transport=transport)
        self.assertEqual(transport.calls,[]);self.assertEqual(client.writes,[])
if __name__=='__main__':unittest.main(verbosity=2)
