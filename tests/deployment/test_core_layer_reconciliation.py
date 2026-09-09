"""Current-state core-layer reconciliation: no AWS, no environment-value reports."""
import copy
import json
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
import core_layer_reconciliation as core

PREFIX='arn:aws:lambda:us-east-1:123456789012:layer:justhodl-core:'
OLD=PREFIX+'1';NEW=PREFIX+'2';OTHER='arn:aws:lambda:us-east-1:123456789012:layer:other:4'
NAME='justhodl-test'
FRED='managed-value-never-report'


def configuration(version='$LATEST'):
    return {'FunctionName':NAME,'Version':version,'RevisionId':'latest-r1' if version=='$LATEST' else 'version-r1',
            'State':'Active','LastUpdateStatus':'Successful','CodeSha256':'same-code-sha','Runtime':'python3.12',
            'Handler':'lambda_function.lambda_handler','Timeout':300,'MemorySize':1024,
            'Environment':{'Variables':{'AUTH_MODE':'owner','FRED_API_KEY':'previous-private-value','UNRELATED':'preserve'}},
            'Layers':[{'Arn':OLD},{'Arn':OTHER}],
            'VpcConfig':{'SubnetIds':['subnet-1'],'SecurityGroupIds':['sg-1'],'VpcId':'vpc-1'},
            'TracingConfig':{'Mode':'Active'}}


class LambdaFixture:
    class exceptions:
        class ResourceNotFoundException(Exception):pass
    def __init__(self):
        self.latest=configuration();self.versions={'7':configuration('7')}
        self.alias={'FunctionVersion':'7','RevisionId':'alias-r1','RoutingConfig':{}}
        self.calls=[];self.candidate_drift=None
    def get_paginator(self,name):
        assert name=='list_functions'
        return SimpleNamespace(paginate=lambda:iter([{'Functions':[copy.deepcopy(self.latest)]}]))
    def get_alias(self,FunctionName,Name):return copy.deepcopy(self.alias)
    def get_function_configuration(self,FunctionName,Qualifier=None):
        return copy.deepcopy(self.versions[Qualifier] if Qualifier else self.latest)
    def update_function_configuration(self,**kwargs):
        assert kwargs['RevisionId']==self.latest['RevisionId']
        self.calls.append(('update_configuration',copy.deepcopy(kwargs)))
        self.latest.update(Layers=[{'Arn':arn} for arn in kwargs['Layers']],Environment=copy.deepcopy(kwargs['Environment']),RevisionId='latest-r2')
    def get_waiter(self,name):
        assert name=='function_updated_v2'
        return SimpleNamespace(wait=lambda **kw:None)
    def publish_version(self,**kwargs):
        assert kwargs['RevisionId']==self.latest['RevisionId'] and kwargs['CodeSha256']==self.latest['CodeSha256']
        self.calls.append(('publish_version',copy.deepcopy(kwargs)))
        candidate=copy.deepcopy(self.latest);candidate.update(Version='8',RevisionId='version-r8')
        if self.candidate_drift:self.candidate_drift(candidate)
        self.versions['8']=candidate
        return {'Version':'8'}
    def update_alias(self,**kwargs):
        assert kwargs['RevisionId']==self.alias['RevisionId']
        self.calls.append(('update_alias',copy.deepcopy(kwargs)))
        self.alias.update(FunctionVersion=kwargs['FunctionVersion'],RevisionId='alias-r2')


def reconcile(client):
    return core.reconcile_consumer(client,NAME,copy.deepcopy(client.latest),copy.deepcopy(client.alias),
                                   copy.deepcopy(client.versions),PREFIX,NEW,FRED)


def test_same_code_with_non_layer_timeout_or_auth_drift_blocks_before_any_mutation():
    for field,mutate in (
        ('Timeout',lambda cfg:cfg.update(Timeout=900)),
        ('Environment',lambda cfg:cfg['Environment']['Variables'].update(AUTH_MODE='unapproved-auth-value')),
        ('MemorySize',lambda cfg:cfg.update(MemorySize=4096)),
        ('VpcConfig',lambda cfg:cfg['VpcConfig'].update(SubnetIds=['different-subnet'])),
        ('TracingConfig',lambda cfg:cfg['TracingConfig'].update(Mode='PassThrough')),
        ('Layers.non_core',lambda cfg:cfg['Layers'].__setitem__(1,{'Arn':'different-non-core-layer'})),
    ):
        client=LambdaFixture();mutate(client.latest)
        assert client.latest['CodeSha256']==client.versions['7']['CodeSha256']
        result=reconcile(client)
        assert result['status']=='BLOCKED' and result['reason']=='UNEXPLAINED_CONFIGURATION_DRIFT'
        assert field in result['differing_fields'] and client.calls==[]
        assert 'unapproved-auth-value' not in json.dumps(result) and FRED not in json.dumps(result)


def test_only_core_layer_and_fred_differences_are_permitted_by_drift_comparison():
    live=configuration('7');latest=configuration()
    latest['Layers'][0]['Arn']=NEW
    latest['Environment']['Variables'].update(FRED_API_KEY=FRED,FRED_KEY=FRED)
    assert core.drift_fields(latest,live,PREFIX)==[]
    assert live['Environment']['Variables']['FRED_API_KEY']=='previous-private-value'


def test_unique_sdk_request_metadata_does_not_create_configuration_drift():
    class RealisticResponses(LambdaFixture):
        def __init__(self):
            super().__init__()
            self.latest['ResponseMetadata']={'RequestId':'latest-initial','HTTPStatusCode':200}
            self.versions['7']['ResponseMetadata']={'RequestId':'qualified-initial','HTTPStatusCode':200}
            self.reads=0
        def get_function_configuration(self,**kwargs):
            response=super().get_function_configuration(**kwargs)
            self.reads+=1
            response['ResponseMetadata']={'RequestId':'read-'+str(self.reads),'HTTPStatusCode':200,
                                          'HTTPHeaders':{'x-amzn-requestid':'read-'+str(self.reads)},'RetryAttempts':0}
            return response
    client=RealisticResponses()
    result=reconcile(client)
    assert result['status']=='VERIFIED_CURRENT_STATE' and result['alias_promoted'] is True
    assert [name for name,_ in client.calls]==['update_configuration','publish_version','update_alias']
    client.calls.clear()
    assert reconcile(client)['status']=='VERIFIED_CURRENT_STATE' and client.calls==[]
    # Removing transport metadata must not hide actual configuration drift.
    client.latest['Timeout']=901
    blocked=reconcile(client)
    assert blocked['reason']=='UNEXPLAINED_CONFIGURATION_DRIFT'
    assert blocked['differing_fields']==['Timeout'] and client.calls==[]


def test_live_only_old_layer_consumer_is_discovered_and_blocked_not_silently_skipped():
    client=LambdaFixture();client.latest['Layers']=[{'Arn':OTHER}]
    found=list(core.discover(client,PREFIX))
    assert len(found)==1 and found[0][0]==NAME
    name,current,alias,versions=found[0]
    result=core.reconcile_consumer(client,name,current,alias,versions,PREFIX,NEW,FRED)
    assert result['status']=='BLOCKED' and result['reason']=='ALIAS_ONLY_LAYER_REQUIRES_EXPLICIT_RELEASE'
    assert result['live_layers_before']=={'7':[OLD]}
    assert client.calls==[]


def test_weighted_alias_old_layer_consumer_is_discovered_and_never_promoted():
    client=LambdaFixture();client.latest['Layers']=[{'Arn':OTHER}]
    client.versions['8']=copy.deepcopy(client.latest);client.alias.update(FunctionVersion='8',RoutingConfig={'AdditionalVersionWeights':{'7':.1}})
    found=list(core.discover(client,PREFIX))
    assert len(found)==1 and set(found[0][3])=={'7','8'}
    result=core.reconcile_consumer(client,*found[0],PREFIX,NEW,FRED)
    assert result['status']=='BLOCKED' and result['reason']=='WEIGHTED_LIVE_ALIAS_REQUIRES_REVIEW'
    assert client.calls==[]


def test_matching_configuration_moves_only_layer_fred_then_publishes_with_cas():
    client=LambdaFixture();result=reconcile(client)
    assert result['status']=='VERIFIED_CURRENT_STATE' and result['alias_promoted'] is True
    assert [name for name,_ in client.calls]==['update_configuration','publish_version','update_alias']
    config_call,publish_call,alias_call=[kwargs for _,kwargs in client.calls]
    assert config_call['RevisionId']=='latest-r1' and config_call['Layers']==[NEW,OTHER]
    assert config_call['Environment']['Variables']=={'AUTH_MODE':'owner','FRED_API_KEY':FRED,'UNRELATED':'preserve'}
    assert publish_call['RevisionId']=='latest-r2' and publish_call['CodeSha256']=='same-code-sha'
    assert alias_call['RevisionId']=='alias-r1' and alias_call['FunctionVersion']=='8'
    assert client.latest['Timeout']==300 and client.latest['VpcConfig']==client.versions['7']['VpcConfig']
    assert FRED not in json.dumps(result) and 'previous-private-value' not in json.dumps(result)


def test_numbered_candidate_configuration_is_checked_before_alias_promotion():
    client=LambdaFixture();client.candidate_drift=lambda cfg:cfg.update(Timeout=900)
    result=reconcile(client)
    assert result['status']=='BLOCKED' and result['reason']=='PUBLISHED_CONFIGURATION_PARITY_FAILED'
    assert [name for name,_ in client.calls]==['update_configuration','publish_version']
    assert client.alias['FunctionVersion']=='7'


def test_repeat_of_verified_current_configuration_is_idempotent():
    client=LambdaFixture();first=reconcile(client)
    assert first['status']=='VERIFIED_CURRENT_STATE'
    client.calls.clear();second=reconcile(client)
    assert second['status']=='VERIFIED_CURRENT_STATE' and second['alias_promoted'] is False
    assert client.calls==[]


def test_missing_protected_alias_configuration_blocks_before_any_mutation():
    client=LambdaFixture()
    result=core.reconcile_consumer(client,NAME,client.latest,client.alias,{},PREFIX,NEW,FRED)
    assert result['status']=='BLOCKED' and client.calls==[]


def test_unstable_numbered_candidate_never_receives_live_alias():
    client=LambdaFixture();client.candidate_drift=lambda cfg:cfg.update(State='Pending')
    result=reconcile(client)
    assert result['status']=='BLOCKED'
    assert [name for name,_ in client.calls]==['update_configuration','publish_version']
    assert client.alias['FunctionVersion']=='7'


def test_unavailable_exact_layer_package_blocks_before_secret_reads_or_mutation():
    from unittest.mock import patch
    class Inventory(LambdaFixture):
        def get_paginator(self,name):
            if name=='list_layers':return SimpleNamespace(paginate=lambda:iter([{'Layers':[{'LayerName':'justhodl-core','LayerArn':PREFIX[:-1],'LatestMatchingVersion':{'Version':2}}]}]))
            return super().get_paginator(name)
        def get_layer_version(self,**kwargs):return {'LayerVersionArn':NEW,'Content':{'CodeSha256':'wrong-package'}}
    client=Inventory()
    def denied(**kwargs):raise AssertionError('Secret must not be read before package verification')
    with patch.object(core,'layer_package',return_value=(b'reviewed','expected-package')):
        result=core.reconcile(client,SimpleNamespace(get_parameter=denied),ROOT)
    assert result['status']=='BLOCKED_REVIEWED_LAYER_PACKAGE_UNAVAILABLE' and result['ok'] is False
    assert client.calls==[]


def test_incomplete_qualified_discovery_cannot_report_zero_verified_consumers():
    from unittest.mock import patch
    class Inventory(LambdaFixture):
        def get_paginator(self,name):
            if name=='list_layers':return SimpleNamespace(paginate=lambda:iter([{'Layers':[{'LayerName':'justhodl-core','LayerArn':PREFIX[:-1],'LatestMatchingVersion':{'Version':2}}]}]))
            return super().get_paginator(name)
        def get_layer_version(self,**kwargs):return {'LayerVersionArn':NEW,'Content':{'CodeSha256':'expected-package'}}
        def get_alias(self,**kwargs):raise RuntimeError('Synthetic inaccessible alias')
    client=Inventory()
    with patch.object(core,'layer_package',return_value=(b'reviewed','expected-package')):
        result=core.reconcile(client,SimpleNamespace(get_parameter=lambda **kw:{'Parameter':{'Value':FRED}}),ROOT)
    assert result['status']=='BLOCKED_INCOMPLETE_DISCOVERY' and result['ok'] is False
    assert client.calls==[] and result['prior_5232_configuration_safety']=='UNVERIFIABLE_WITHOUT_PREMIGRATION_SNAPSHOT'
