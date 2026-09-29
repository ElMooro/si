"""Whole calendar originals, unavailable/empty distinction and hostile replay cases."""
from pathlib import Path
from datetime import datetime,timezone
from copy import deepcopy
from contextlib import redirect_stdout
import io,json,runpy,sys,tempfile
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import auction_calendar as model
import auction_calendar_store as store
support=runpy.run_path(str(ROOT/'tests/test_auction_originals.py'))
Memory, rejects=support['Memory'],support['rejects']
AT='2026-09-29T06:30:00+00:00'


def row(**change):
    return {'auctionDate':'2026-09-29','issueDate':'2026-09-30','securityType':'Note','securityTerm':'3-Year',
            'tips':'No','floatingRate':'No','offeringAmount':'0','cusip':'TEST00001',**change}


def frame(rows=None,at=AT):
    return {'request_url':model.URL,'response_url':model.URL,'http_status':200,'started_at':at,
            'acquired_at':at,'raw':model.encoded([row()] if rows is None else rows)}


def build(rows):return model.build(frame(rows),'2026-09-29',30,AT)


def retained(rows=None):
    client=Memory();ref,selected=store.retain(client,'fixture',frame(rows),generated_at=AT)
    return client,ref,selected


def test_every_original_occurrence_is_retained_and_selection_is_dated():
    rows=[row(),row(),row(auctionDate='2026-09-28'),row(auctionDate='bad'),None,
          row(auctionDate='2026-10-29'),row(auctionDate='2026-10-30')]
    before=deepcopy(rows);out=build(rows);c=out['coverage']
    assert rows==before and len(out['observations'])==7 and len(out['selected_auctions'])==3
    assert (c['outside_window'],c['invalid_records'],c['duplicate_occurrences'])==(2,2,1)
    assert out['status']=='partial' and not c['selection_complete']
    assert [v['source_row_index'] for v in out['selected_auctions']]==[0,1,5]
    assert all(out[key] is False for key in model.PERMISSIONS)


def test_complete_empty_response_is_distinct_from_failed_or_invalid_response():
    out=build([]);assert out['status']=='complete' and out['coverage']['selected']==0
    assert out['coverage']['selection_complete'] and not out['provider_universe_complete']
    for raw in (b'{}',b'null',b'{"a":1,"a":2}',b'[NaN]',b'[1e999]',b'[',b''):
        bad=frame();bad['raw']=raw;rejects(lambda:model.build(bad,'2026-09-29',30,AT))
    assert model.unavailable('acquisition_failed')['status']=='unavailable'


def test_zero_missing_invalid_amounts_flags_and_alias_conflicts_do_not_conflate():
    rows=[row(),row(offeringAmount=None),row(offeringAmount=True),row(offeringAmount='-1'),
          row(floatingRate='Yes'),row(tips='Yes'),row(offering_amount='99'),
          row(auction_date='2026-09-30'),row(floating_rate='Yes')]
    out=build(rows);values=out['selected_auctions']
    assert values[0]['offering_amount_billions']==0
    assert all(values[i]['offering_amount_billions'] is None for i in (1,2,3,6))
    assert values[4]['instrument_kind']=='FRN' and values[5]['instrument_kind']=='TIPS'
    assert out['observations'][7]['status']=='invalid_auction_date'
    assert values[7]['instrument_kind']=='UNKNOWN'
    assert out['observations'][6]['issues']==['conflicting_alias:offeringAmount','invalid_offering_amount']


def test_invalid_dates_and_issue_order_are_retained_with_explicit_gaps():
    for value in ('2026-02-30','2026-09-29garbage',True,42,None):
        out=build([row(auctionDate=value)]);assert out['coverage']['invalid_records']==1
    for value in ('2026-09-28','bad',None):
        out=build([row(issueDate=value)]);assert out['selected_auctions'][0]['issue_date'] is None
    out=build([row(auctionDate='2026-09-29T00:00:00')]);assert out['coverage']['selected']==1


def test_transport_identity_clocks_limits_and_partial_responses_fail_closed():
    for key,value in [('http_status',True),('http_status',206),('request_url','https://example.com'),
                      ('response_url',model.URL+'&other=1'),('started_at','2026-09-29T06:31:00Z'),
                      ('acquired_at','2026-09-29T06:31:00Z'),('started_at','2026-09-29T06:20:00Z'),
                      ('started_at','2026-09-29T06:30:00'),('raw',b' '*(model.MAX_BYTES+1)),
                      ('content_length',True),('content_length',-1),('content_length',99999)]:
        source=frame();source[key]=value;rejects(lambda:model.build(source,'2026-09-29',30,AT))
    for n in (-1,True,31):rejects(lambda:model.build(frame(),'2026-09-29',n,AT))
    rejects(lambda:model.build(frame(),'2026-09-28',30,AT))


def test_maximum_population_is_preserved_without_a_presentation_cutoff():
    out=build([row(cusip=str(n)) for n in range(model.MAX_ROWS)])
    assert len(out['observations'])==len(out['selected_auctions'])==5000
    assert out['selected_auctions'][-1]['cusip']=='4999'
    rejects(lambda:build([row() for _ in range(model.MAX_ROWS+1)]))


def test_entire_response_all_compilers_and_output_replay_before_pointer():
    client,ref,selected=retained([row(),None,row(auctionDate='2026-09-28')])
    replayed=store.replay(ref['manifest'],client.read)
    assert selected==replayed['selected_auctions'] and len(replayed['observations'])==3
    manifest=model.strict_json(client.read(ref['manifest']['key']))
    assert set(manifest['compilers'])==set(store.compiler_bytes()) and len(manifest['compilers'])==7
    assert ref['original_bytes_replayed'] and ref['selection_replayed'] and ref['status']=='partial'
    assert all(key.startswith((model.PREFIX,'data/evidence/treasury-upcoming/')) for key in client.reads+client.writes)


def test_original_compiler_manifest_or_measurement_corruption_rejects():
    for kind in ('original','compiler','manifest','output'):
        client,ref,_=retained();manifest=model.strict_json(client.read(ref['manifest']['key']))
        key={'original':manifest['source']['evidence']['key'], 'compiler':manifest['compilers']['auction_calendar']['key'],
             'manifest':ref['manifest']['key'],'output':manifest['output']['key']}[kind]
        client.objects[key]['raw']+=b'changed'
        try:store.replay(ref['manifest'],client.read)
        except (ValueError,RuntimeError,EOFError,OSError):pass
        else:raise AssertionError(kind)


def test_rehashed_hostile_manifest_cannot_read_other_namespaces_or_skip_a_compiler():
    for kind in ('source_key','output_key','compiler_key','missing_compiler','clock','window','url'):
        client,ref,_=retained();manifest=model.strict_json(client.read(ref['manifest']['key']))
        forbidden='private/account.json'
        if kind=='source_key':manifest['source']['evidence']['key']=forbidden
        elif kind=='output_key':manifest['output']['key']=forbidden
        elif kind=='compiler_key':manifest['compilers']['auction_calendar']['key']=forbidden
        elif kind=='missing_compiler':manifest['compilers'].pop('evidence_store')
        elif kind=='clock':manifest['source']['evidence']['first_received_at']='2026-09-30T00:00:00Z'
        elif kind=='window':manifest['request_window']['end']='2026-10-28'
        else:manifest['source']['response_url']='https://example.com'
        raw=model.encoded(manifest);changed=store.reference('runs',raw)
        client.objects[changed['key']]={'raw':raw,'metadata':{}}
        rejects(lambda:store.replay(changed,client.read))
        assert forbidden not in client.reads


def test_conditional_conflict_requires_exact_readback_and_real_errors_propagate():
    client,ref,_=retained();again,_=store.retain(client,'fixture',frame(),generated_at=AT)
    assert ref==again
    manifest=model.strict_json(client.read(ref['manifest']['key']))
    client.objects[manifest['output']['key']]['raw']=b'changed'
    rejects(lambda:store.retain(client,'fixture',frame(),generated_at=AT))
    client=Memory();client.put_object=lambda **kw:(_ for _ in ()).throw(RuntimeError('storage unavailable'))
    rejects(lambda:store.retain(client,'fixture',frame(),generated_at=AT))


def test_native_fetch_has_one_bounded_request_and_no_failure_journal_or_fallback():
    m=runpy.run_path(str(ROOT/'tests/test_auction_benchmarks.py'))['engine']()
    today=datetime.now(timezone.utc).date().isoformat();journal=[]
    response=io.BytesIO(model.encoded([row(auctionDate=today)]));response.status=200;response.geturl=lambda:model.URL
    with patch.object(m.urllib.request,'urlopen',return_value=response) as request:
        result=m.fetch_upcoming_auctions(source_out=journal)
    assert request.call_count==1 and len(result)==len(journal)==1 and isinstance(journal[0]['raw'],bytes)
    journal=[]
    with patch.object(m.urllib.request,'urlopen',side_effect=OSError('provider unavailable')):
        try:m.fetch_upcoming_auctions(source_out=journal)
        except OSError:pass
        else:raise AssertionError('Fetch failure hidden as empty')
    assert journal==[]
    for declared in ('99','-1','','NaN','true'):
        truncated=io.BytesIO(b'[]');truncated.status=200;truncated.geturl=lambda:model.URL
        truncated.headers={'Content-Length':declared}
        with patch.object(m.urllib.request,'urlopen',return_value=truncated):
            rejects(lambda:m.fetch_upcoming_auctions(source_out=journal))
        assert journal==[]


def native_calendar(failure=None):
    client,handler=support['native_setup']();env=handler.__globals__
    if failure=='acquisition':
        env['fetch_upcoming_auctions']=lambda *a,**kw:(_ for _ in ()).throw(OSError('synthetic'))
    elif failure=='storage':
        env['retain_calendar_originals']=lambda *a,**kw:(_ for _ in ()).throw(RuntimeError('synthetic'))
    elif failure=='heuristic':
        env['build_forward_calendar']=lambda *a,**kw:(_ for _ in ()).throw(RuntimeError('synthetic'))
    today=datetime.now(timezone.utc).date().isoformat()
    with patch('urllib.request.urlopen',return_value=support['response']([support['row'](auction_date=today)])):
        assert handler({},None)['statusCode']==200
    return client,model.strict_json(client.read('data/auction-crisis.json'))


def test_native_pointer_is_published_only_after_whole_calendar_replay():
    client,packet=native_calendar();ref=packet['calendar_source']
    assert ref['status']=='complete' and ref['coverage']['records_received']==0
    assert packet['forward_calendar_status']=={'status':'complete','reason':None}
    assert client.writes.index(ref['manifest']['key'])<client.writes.index('data/auction-crisis.json')
    assert store.replay(ref['manifest'],client.read)['selected_auctions']==[]


def test_native_failure_states_clear_rows_without_claiming_complete_empty():
    for failure,reason in [('acquisition','acquisition_failed'),('storage','retention_or_replay_failed'),('heuristic','heuristic_calculation_failed')]:
        _,packet=native_calendar(failure)
        assert packet['forward_calendar']==[] and packet['forward_calendar_status']=={'status':'unavailable','reason':reason}
        ref=packet['calendar_source']
        if failure=='heuristic':assert ref['status']=='complete' and ref['selection_replayed']
        else:assert ref['status']=='unavailable' and ref['original_bytes_replayed'] is False and 'manifest' not in ref


def test_actual_native_provider_adapter_preserves_all_calendar_records_and_gaps():
    client,handler=support['native_setup']();env=handler.__globals__
    m=runpy.run_path(str(ROOT/'tests/test_auction_benchmarks.py'))['engine']()
    env['fetch_upcoming_auctions']=m.fetch_upcoming_auctions
    today=datetime.now(timezone.utc).date().isoformat()
    rows=[row(auctionDate=today,cusip=str(i),issueDate=None) for i in range(52)]+[row(auctionDate='invalid')]
    calls=[]
    def provider(request,**kwargs):
        calls.append(request.full_url)
        if request.full_url==model.URL:
            response=io.BytesIO(model.encoded(rows));response.status=200;response.geturl=lambda:model.URL
            return response
        return support['response']([support['row'](auction_date=today)])
    with patch('urllib.request.urlopen',side_effect=provider):assert handler({},None)['statusCode']==200
    packet=model.strict_json(client.read('data/auction-crisis.json'))
    assert calls.count(model.URL)==1 and len(packet['forward_calendar'])==52
    assert packet['forward_calendar'][-1]['cusip']=='51' and packet['forward_calendar'][0]['offering_amount_billions']==0
    replayed=store.replay(packet['calendar_source']['manifest'],client.read)
    assert len(replayed['observations'])==53 and replayed['coverage']['invalid_records']==1
    assert packet['calendar_source']['status']=='partial'


def test_offline_cli_replays_without_network_or_executing_archived_code():
    client,ref,_=retained();cli=runpy.run_path(str(ROOT/'scripts/replay_auction_calendar.py'))
    with tempfile.TemporaryDirectory() as directory:
        root=Path(directory)
        for key,item in client.objects.items():
            path=root/key;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(item['raw'])
        pointer=root/'ref.json';pointer.write_bytes(model.encoded(ref['manifest']));out=io.StringIO()
        with redirect_stdout(out),patch('urllib.request.urlopen',side_effect=AssertionError('No network')):
            cli['main'](['--root',str(root),'--reference',str(pointer)])
        assert json.loads(out.getvalue())['coverage']['selected']==1


if __name__=='__main__':
    tests=[fn for name,fn in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction calendar original regressions passed:',len(tests))
