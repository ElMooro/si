"""Offline source-owned namespace overlays; live registry documents stay untouched."""
import copy
from datetime import datetime, timezone
import importlib.util
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
SOURCE=ROOT/'aws/lambdas/justhodl-contract-gate/source'
sys.path[:0]=[str(SOURCE),str(ROOT/'aws/shared')]
import reviewed_contracts as reviewed
from public_brain_projection import source_map_public, SOURCE_MAP_PUBLICATION


def contract(key):return reviewed.load_overlay()['entries'][key]['contract']
def metrics(engine):
    return {'engine':engine,'schema_version':'macro-metrics.v1','metrics':{},'risk_index':50,
            'category_risks':{},'errors':[],'generated':datetime.now(timezone.utc).isoformat(),'count':0,'version':1}

def test_overlay_preserves_unrelated_contracts_and_original_document_without_relearning():
    original={'version':'legacy','contracts':{'data/unrelated.json':{'required_keys':['nested'],'rows_path':['records'],'min_rows':37,'max_age_hours':19,'custom_policy':{'keep':True}},
                'data/options-flow.json':{'min_rows':200},'data/ka-metrics.json':{'rows_path':['errors'],'min_rows':9}}}
    before=copy.deepcopy(original);updated=reviewed.apply_contracts(original)
    assert original==before
    assert updated['contracts']['data/unrelated.json']==before['contracts']['data/unrelated.json']
    assert 'data/options-flow.json' not in updated['contracts']
    assert updated['contracts']['data/ka-metrics.json']['min_rows']==0
    assert updated['contracts']['data/ka-metrics.json']['rows_path'] is None
    assert reviewed.apply_contracts(updated)==updated


def test_writer_overlay_changes_exact_reviewed_entries_only_and_preserves_shared_history():
    existing={'producers':{'data/unrelated.json':{'writers':['unrelated'],'readers':['reader'],'evidence':{'unchanged':True}},
               'data/options-flow.json':{'writers':['old-conflicting-a','old-conflicting-b']},
               'calibration/history/*.json':{'writers':['snapshotter','other-calibrator']},
               'data/calibration-snapshot.json':{'writers':['justhodl-calibration-snapshot']},
               'archive/*.json':{'writers':['many-writers']}}}
    before=copy.deepcopy(existing);updated=reviewed.apply_producers(existing);p=updated['producers']
    assert existing==before
    for key in ('data/unrelated.json','calibration/history/*.json','data/calibration-snapshot.json','archive/*.json'):assert p[key]==before['producers'][key]
    assert p['data/options-flow.json']['writers']==[] and p['data/options-flow.json'].get('retired')
    assert p['calibration/model-latest.json']['writers']==['justhodl-calibration-snapshotter']
    assert p['calibration/latest.json']['writers']==['justhodl-calibrator']
    assert p['data/khalid-analysis.json']['writers']==['justhodl-khalid-metrics']
    assert p['data/ka-analysis.json']['writers']==['justhodl-ka-metrics']
    assert reviewed.apply_producers(updated)==updated


def test_curated_theme_array_and_momentum_classifier_dictionary_are_distinct_real_shapes():
    curated={'engine':'themes','version':'1.0','generated_at':'2026-09-09T12:00:00Z','as_of_session':'2026-09-08','themes':[],'methodology':'Curated thematic watchlists'}
    classified={'schema_version':'1.0','producer':'justhodl-theme-classifier','generated_at':'2026-09-09T12:00:00Z','themes':{},'ticker_to_theme':{},'unclassified':[],'all_industries_seen':[],'n_active_themes':0}
    assert reviewed.validate_fields(curated,contract('data/themes.json'))==[]
    assert reviewed.validate_fields(classified,contract('data/momentum-themes.json'))==[]
    for doc,key,wrong in ((curated,'data/themes.json',{}),(classified,'data/momentum-themes.json',[])):
        broken=copy.deepcopy(doc);broken['themes']=wrong;assert reviewed.validate_fields(broken,contract(key))
    assert reviewed.validate_fields(classified,contract('data/themes.json'))
    assert reviewed.validate_fields(curated,contract('data/momentum-themes.json'))


def test_healthy_zero_error_metrics_are_valid_but_wrong_owner_schema_and_boolean_count_fail():
    for short in ('ka','khalid'):
        engine='justhodl-'+short+'-metrics';key='data/'+short+'-metrics.json';doc=metrics(engine)
        assert contract(key)['min_rows']==0 and contract(key)['rows_path'] is None
        assert reviewed.validate_fields(doc,contract(key))==[]
        for field,value in (('engine','PRIVATE_WRONG_OWNER_CANARY'),('schema_version','PRIVATE_WRONG_SCHEMA_CANARY'),('count',True)):
            bad=copy.deepcopy(doc);bad[field]=value;violations=reviewed.validate_fields(bad,contract(key));assert violations
            assert 'PRIVATE_WRONG' not in json.dumps(violations)
        bad=copy.deepcopy(doc);bad.pop('errors');assert reviewed.validate_fields(bad,contract(key))


def test_analysis_owner_and_input_artifact_cannot_be_replaced_by_other_engine():
    for short in ('ka','khalid'):
        key='data/'+short+'-analysis.json';doc={'engine':'justhodl-'+short+'-metrics','schema_version':'macro-analysis.v1','input_artifact':'data/'+short+'-metrics.json','input_generated':'2026-09-09T12:00:00Z','llm_status':'available','generated':'2026-09-09T12:01:00Z'}
        assert not reviewed.validate_fields(doc,contract(key))
        for name,value in (('engine','other'),('input_artifact','data/other-metrics.json'),('llm_status','unavailable')):
            changed=copy.deepcopy(doc);changed[name]=value;assert reviewed.validate_fields(changed,contract(key))


def test_actual_source_map_projection_passes_while_legacy_private_landing_does_not():
    raw={'sources':{'FRED:DGS10':{'source':'PRIVATE_CANARY'}},'last_harvest_diag':{'private':'PRIVATE_CANARY'}}
    assert reviewed.validate_fields(raw,contract('data/source-map.json'))
    projected=source_map_public({'schema_version':'public-source-map.v1','engine':'justhodl-source-map','publication':dict(SOURCE_MAP_PUBLICATION),
        'generated_at':'2026-09-09T12:00:00Z','input_artifact':'data/tv-sources.json','input_status':'AVAILABLE',
        'cleaned_sources':{'FRED:DGS10':{'source_family':'US-TREASURY','updated':'2026-09-09T11:00:00Z'}},'known_families':{'US-TREASURY':1},'errors':[]})
    assert reviewed.validate_fields(projected,contract('data/source-map.json'))==[]
    bad=copy.deepcopy(projected);bad['publication']['contains_private_data']=True
    violations=reviewed.validate_fields(bad,contract('data/source-map.json'));assert violations and 'PRIVATE_CANARY' not in json.dumps(violations)


def test_calibrator_report_and_available_model_snapshot_are_separate_contracts():
    model={'v':'2.0','audit_version':'2026-09-09.1','snapshot_id':'snapshot','as_of':'2026-09-09T12:00:00Z','calibrated_at':None,'available_at':'2026-09-09T12:00:00Z','training_end_at':None,'model_version':None,'code_version':'snapshotter-2.0','weights':{},'accuracy':{},'accuracy_meta':{},'outcome_counts_60d':{},'summary':{}}
    report={'generated_at':'2026-09-09T12:00:00Z','total_outcomes':0,'signal_types_tracked':0,'weights':{},'accuracy_by_type':{},'window_accuracy':{},'window_weights':{},'recommended_horizon':{},'khalid_component_weights':{},'top_performing_signals':[],'worst_performing_signals':[],'recommendations':[]}
    assert not reviewed.validate_fields(model,contract('calibration/model-latest.json'))
    assert not reviewed.validate_fields(report,contract('calibration/latest.json'))
    assert reviewed.validate_fields(model,contract('calibration/latest.json'))
    assert reviewed.validate_fields(report,contract('calibration/model-latest.json'))
    overlay=reviewed.load_overlay();assert 'data/calibration-snapshot.json' not in overlay['entries']
    assert 'calibration/history/*.json' not in overlay['entries'] and 'archive/*.json' not in overlay['entries']


def test_qualified_function_arns_preserve_identity_and_never_treat_alias_as_function():
    name='justhodl-ka-metrics';base='arn:aws:lambda:us-east-1:857687956942:function:'+name
    for value in (name,base,base+':live',base+':12',base+':$LATEST'):assert reviewed.artifact_function_name(value)==name
    for value in (None,'','arn:aws:sqs:us-east-1:857687956942:queue'):
        assert reviewed.artifact_function_name(value) in (None,'')


def test_actual_check_skips_private_bodies_preserves_all_findings_and_redacts_failure_text():
    reads=[];writes={};at=datetime.now(timezone.utc)
    class S3:
        def put_object(self, **kw):writes[kw['Key']]=json.loads(kw['Body'])
        def head_object(self, **kw):raise AssertionError('Unexpected head')
    client=S3()
    spec=importlib.util.spec_from_file_location('reviewed_gate_actual',SOURCE/'lambda_function.py')
    module=importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {'boto3':SimpleNamespace(client=lambda *a,**kw:client), 'botocore.config':SimpleNamespace(Config=lambda **kw:object())}):
        spec.loader.exec_module(module)
    contracts={f'data/fixture-{i}.json':{'required_keys':['missing']} for i in range(425)}
    contracts['portfolio/snapshot.json']={'required_keys':['positions']}
    contracts['data/broken-fixture.json']={'required_keys':[]}
    def get(key):
        reads.append(key)
        if key==module.CONTRACTS_KEY:return {'contracts':contracts}
        if key=='portfolio/snapshot.json':raise AssertionError('Private body read')
        if key=='data/broken-fixture.json':raise RuntimeError('SYNTHETIC_PRIVATE_DIAGNOSTIC')
        return {}
    module.get_json=get;module.get_json_raw=lambda key:b'{"exempt":{}}'
    module.apply_contracts=lambda doc:doc;module.load_overlay=lambda:{'entries':{}}
    module.list_artifacts=lambda:[{'key':key,'modified':at,'size':10} for key in contracts]+[{'key':f'data/extra-{i}.json','modified':at,'size':10} for i in range(120)]
    doc=module.check()
    assert 'portfolio/snapshot.json' not in reads
    assert doc['scope']=='PUBLIC_ENGINE_OUTPUTS' and doc['private_contracts_excluded']==1
    assert len(doc['violations'])==doc['n_violations']==426
    assert len(doc['uncontracted'])==doc['n_uncontracted']==120
    assert 'SYNTHETIC_PRIVATE_DIAGNOSTIC' not in json.dumps(writes)
    assert writes[module.VIOLATIONS_KEY]==doc
