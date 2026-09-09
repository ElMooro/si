import importlib.util
import io
import json
from pathlib import Path
import sys
import types
from unittest.mock import MagicMock, patch

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/shared'))
import private_artifact

def load(short):
    stub=types.ModuleType('managed_secret');stub.managed_secret=lambda *args:''
    boto=types.ModuleType('boto3');boto.client=MagicMock()
    router=types.ModuleType('llm_router');router.complete=MagicMock(return_value='')
    spec=importlib.util.spec_from_file_location('metric_'+short,ROOT/'aws/lambdas'/('justhodl-'+short+'-metrics')/'source/lambda_function.py')
    module=importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules,{'boto3':boto,'managed_secret':stub,'llm_router':router,'anthropic_shim':types.ModuleType('anthropic_shim'),'_fred_shim':types.ModuleType('_fred_shim')}):spec.loader.exec_module(module)
    return module

def test_public_metric_mutations_are_denied_before_any_config_or_provider_read():
    for short in ('ka','khalid'):
        module=load(short);module.load_config=MagicMock(side_effect=AssertionError('must not read'));module.refresh_data=MagicMock()
        with patch.object(private_artifact,'service_headers',return_value={'X-JH-Service-Token':'synthetic-service-identity'}):
            for method,path in [('POST','/'),('PUT','/'),('PATCH','/'),('DELETE','/'),('GET','/refresh'),('GET','/analyze')]:
                for headers in ({},{'Origin':'https://justhodl.ai'},{'x-jh-service-token':'wrong'}):
                    result=module.lambda_handler({'requestContext':{'http':{'method':method}},'rawPath':path,'headers':headers,'body':'{}'},None)
                    assert result['statusCode']==401
        module.load_config.assert_not_called();module.refresh_data.assert_not_called()

def test_valid_service_identity_can_refresh_and_normal_schedule_keeps_its_path():
    for short in ('ka','khalid'):
        module=load(short);module.load_config=lambda:{'metrics':[],'categories':[]}
        module.refresh_data=MagicMock(return_value={'count':0,'risk_index':None,'errors':[]})
        module.run_ai_analysis=lambda *args:{}
        with patch.object(private_artifact,'service_headers',return_value={'X-JH-Service-Token':'synthetic-service-identity'}):
            assert module.lambda_handler({'requestContext':{'http':{'method':'GET'}},'rawPath':'/refresh','headers':{'x-jh-service-token':'synthetic-service-identity'}},None)['statusCode']==200
        assert module.lambda_handler({},None)['statusCode']==200
        assert module.refresh_data.call_count==2

def test_absent_metrics_and_invalid_weights_cannot_be_neutral_risk():
    for short in ('ka','khalid'):
        module=load(short)
        assert module.calc_risk({}, {'metrics':[]}) is None
        assert module.calc_risk({'X':{'3m':float('nan')}},{'metrics':[{'id':'X','weight':5}]}) is None
        assert module.calc_risk({'X':{'3m':0}},{'metrics':[{'id':'X','weight':-2}]}) is None
        assert module.calc_risk({'X':{'3m':-10}},{'metrics':[{'id':'X','weight':5}]})==0
        module.load_config=lambda:{'metrics':[],'categories':[]};module.s3.get_object.side_effect=RuntimeError('synthetic-private-error')
        result=module.lambda_handler({'requestContext':{'http':{'method':'GET'}},'rawPath':'/data'},None)
        doc=json.loads(result['body']);assert doc['risk_index'] is None and doc['status']=='UNAVAILABLE'
        assert 'synthetic-private-error' not in result['body']
        assert not any(key.lower().startswith('access-control-') for key in result['headers'])


def test_failed_analysis_replaces_old_report_with_owned_unavailable_snapshot():
    for short in ('ka','khalid'):
        module=load(short)
        for data,reason in (({'risk_index':None},'metrics_unavailable'),({'risk_index':0},'analysis_unavailable')):
            result=module.run_ai_analysis({'metrics':[],'categories':[]},data)
            assert result['engine']=='justhodl-'+short+'-metrics'
            assert result['error']==reason and result['llm_status']=='unavailable' and result['execution_eligible'] is False
            stored=json.loads(module.s3.put_object.call_args.kwargs['Body'])
            assert stored==result and stored['generated'].endswith('+00:00')
            assert module.s3.put_object.call_args.kwargs['Key']=='data/'+short+'-analysis.json'
        with patch.object(module,'complete',side_effect=RuntimeError('synthetic-private-url-and-token')):
            result=module.run_ai_analysis({'metrics':[],'categories':[]},{'risk_index':0})
        assert result['error']=='analysis_unavailable' and 'synthetic-private' not in json.dumps(result)


def test_metric_analysis_uses_managed_budgeted_router_and_rejects_nonfinite_output():
    for short in ('ka','khalid'):
        module=load(short)
        module.complete.return_value='{"risk_analysis":{"risk_score":0}}'
        result=module.run_ai_analysis({'metrics':[],'categories':[]},{'risk_index':0,'generated':'2026-09-09T16:00:00Z'})
        assert result['llm_status']=='available' and result['risk_analysis']['risk_score']==0
        assert result['execution_eligible'] is False and result['input_generated']=='2026-09-09T16:00:00Z'
        assert module.complete.call_args.kwargs=={'tier':'reason','max_tokens':6000,'on_demand':False}
        module.complete.return_value='{"risk_analysis":{"risk_score":NaN}}'
        result=module.run_ai_analysis({'metrics':[],'categories':[]},{'risk_index':0})
        assert result['llm_status']=='unavailable' and result['error']=='analysis_unavailable'
