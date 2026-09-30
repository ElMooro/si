from pathlib import Path
from copy import deepcopy
from datetime import timedelta,timezone
import importlib.util,math,sys
R=Path(__file__).resolve().parents[2]
p=R/'aws/ops/staged/ops_6393_china_execution_metrics.py'
spec=importlib.util.spec_from_file_location('china_execution_metrics',p);op=importlib.util.module_from_spec(spec);spec.loader.exec_module(op)


def response(name,rows=None):
    unit,stats=op.METRICS[name]
    row={'Timestamp':op.START,'Unit':unit,**{k:1.0 for k in stats}}
    return {'Label':name,'Datapoints':[row] if rows is None else rows,'ResponseMetadata':{'HTTPStatusCode':200}}


def refused(fn):
    try:fn()
    except (ValueError,KeyError,TypeError):return
    raise AssertionError('Invalid complete metric evidence was accepted')


def test_fixed_scope_requests_all_four_complete_metrics_without_logs_or_native_invokes():
    class Client:
        def __init__(self):self.requests=[]
        def get_metric_statistics(self,**kw):
            self.requests.append(kw);return response(kw['MetricName'])
    c=Client();result=op.collect(c)
    assert set(result)==set(op.METRICS) and len(c.requests)==4
    for req in c.requests:
        assert req=={'Namespace':'AWS/Lambda','MetricName':req['MetricName'],'Dimensions':[{'Name':'FunctionName','Value':op.FN}],
          'StartTime':op.START,'EndTime':op.END,'Period':60,'Statistics':list(op.METRICS[req['MetricName']][1]),'Unit':op.METRICS[req['MetricName']][0]}
    x=op.summarize(result);assert x['execution_observed'] is True
    assert x['publication_verified'] is x['schedule_causation_verified'] is x['executing_code_identity_verified'] is False


def test_missing_and_explicit_zero_metrics_are_distinct_and_never_claim_publication():
    empty=op.normalize('Invocations',response('Invocations',[]));zero=response('Invocations');zero['Datapoints'][0]['Sum']=0.0
    zero=op.normalize('Invocations',zero)
    assert empty['sum_of_reported_points'] is None and zero['sum_of_reported_points']==0
    assert len(empty['unreported_minutes'])==15 and len(zero['unreported_minutes'])==14
    assert empty['unreported_minutes_are_zero'] is zero['unreported_minutes_are_zero'] is False
    for value in (empty,zero):
        result=op.summarize({'Invocations':value});assert result['execution_observed'] is None and result['publication_verified'] is False


def test_all_minutes_survive_out_of_order_responses_and_duplicates_or_partial_protocol_refuse():
    rows=[{'Timestamp':op.START+timedelta(minutes=i),'Unit':'Count','Sum':i} for i in range(15)]
    a=op.normalize('Invocations',response('Invocations',rows[::-1]));b=op.normalize('Invocations',response('Invocations',rows))
    assert a==b and len(a['datapoints'])==15 and a['sum_of_reported_points']==105 and a['unreported_minutes']==[]
    for bad in (rows+[rows[0]], [rows[0],rows[0]], [{**rows[0],'Timestamp':op.END}], [{**rows[0],'Timestamp':op.START+timedelta(seconds=1)}], [{**rows[0],'Timestamp':op.START.replace(tzinfo=None)}]):
        refused(lambda:op.normalize('Invocations',response('Invocations',bad)))
    bad=response('Invocations');bad['NextToken']='invented-incomplete';refused(lambda:op.normalize('Invocations',bad))


def test_wrong_units_types_nonfinite_negative_and_unreported_fields_refuse():
    changes=[('Sum',True),('Sum',float('nan')),('Sum',float('inf')),('Sum',-1),('Sum',1.5),('Sum',2**53),('Unit','None'),('extra','invented')]
    for key,value in changes:
        r=response('Errors');r['Datapoints'][0][key]=value;refused(lambda:op.normalize('Errors',r))
    for change in ({'Label':'Invocations'},{'Datapoints':None},{'ResponseMetadata':{'HTTPStatusCode':True}},{'ResponseMetadata':{'HTTPStatusCode':500}}):
        r=response('Errors');r.update(change);refused(lambda:op.normalize('Errors',r))


def test_duration_uses_weighted_samples_and_rejects_conflicting_statistics():
    rows=[{'Timestamp':op.START,'Unit':'Milliseconds','Minimum':2.0,'Maximum':6.0,'Sum':8.0,'SampleCount':2.0},
          {'Timestamp':op.START+timedelta(minutes=3),'Unit':'Milliseconds','Minimum':10.0,'Maximum':10.0,'Sum':10.0,'SampleCount':1.0}]
    result=op.normalize('Duration',response('Duration',rows));assert result['reported_samples']==3 and result['mean_of_reported_samples_ms']==6
    for change in ({'SampleCount':0},{'SampleCount':True},{'Minimum':7},{'Maximum':1},{'Sum':13},{'Sum':3},{'SampleCount':1.5}):
        bad=deepcopy(rows);bad[0].update(change);refused(lambda:op.normalize('Duration',response('Duration',bad)))
    empty=op.normalize('Duration',response('Duration',[]));assert empty['reported_samples'] is empty['mean_of_reported_samples_ms'] is None


def test_original_window_must_be_closed_and_within_minute_resolution_retention():
    op.check_window(op.END);op.check_window(op.START+timedelta(days=14))
    for at in (op.END-timedelta(seconds=1),op.START+timedelta(days=14,seconds=1),op.END.replace(tzinfo=None)):
        refused(lambda:op.check_window(at))
