"""Synthetic-only source, identity, acquisition and publication checks."""
from pathlib import Path
from datetime import datetime,timezone
from io import BytesIO
from unittest.mock import patch
from copy import deepcopy
import ast,json,sys,time,unittest,urllib.request,urllib.error
ROOT=Path(__file__).resolve().parents[4]
SRC=ROOT/'aws/lambdas/justhodl-activist-filings-scanner/source'
sys.path[:0]=[str(SRC),str(ROOT/'aws/shared')]
import filing_observations as m
import sec_atom_model as atom
from momentum_research_boundary import DIRECT as MOMENTUM_SOURCE
AT='2026-09-28T01:00:00Z'
ACC='0000000123-26-000001'
URL='https://www.sec.gov/Archives/edgar/data/123/000000012326000001/'+ACC+'-index.htm'

def entry(role='Filer',name='Reporting party',cik='123',form='SCHEDULE 13D',updated='2026-09-25T17:00:00Z',url=URL,identifier=ACC):
    import html
    return '<a:entry><a:title>'+html.escape(form+' - '+name+' ('+cik+') ('+role+')')+'</a:title><a:id>'+identifier+'</a:id><a:link href="'+url+'"/><a:updated>'+updated+'</a:updated><a:category term="'+form+'"/><a:summary>'+('X'*700)+'</a:summary><a:unknown>retain</a:unknown></a:entry>'

def feed(entries=''):
    return ('<a:feed xmlns:a="http://www.w3.org/2005/Atom">'+entries+'</a:feed>').encode()

def captured(raw,url,kind,sources,status=200):
    ref=m.source_ref(raw,kind);sources[ref['key']]=raw
    return {'status':'received','endpoint':url,'requested_at':'2026-09-28T00:00:00Z',
        'received_at':'2026-09-28T00:00:01Z','original_ref':ref,'http_status':status}

def plan(records=None):
    sources={};rows=entry()+entry('Reporting','Co filer','456')+entry('Subject','Issuer','789') if records is None else records
    a={'mapping':captured(m.encode({'0':{'cik_str':789,'ticker':'TEST.A'},'1':{'cik_str':789,'ticker':'TEST.B'},'bad':None}),m.MAPPING,'json',sources),
       'universe':captured(m.encode({'stocks':[{'symbol':'TEST.A'},{'symbol':'TEST.A'},None]}),'data/universe.json','json',sources),
       'feeds':[captured(feed(rows if f=='SCHEDULE 13D' else ''),m.feed_url(f),'xml',sources) for f in m.FORMS]}
    return a,sources

class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}

class Memory:
    def __init__(self):
        self.previous=b'{"all_filings":[],"generated_at":"2026-09-27T11:00:00Z"}'
        self.data={m.HEAD:self.previous,'data/universe.json':b'{"stocks":[{"symbol":"TEST"}]}'}
        self.reads=[];self.writes=[];self.corrupt=False;self.race=False;self.denied=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=b'bad' if self.corrupt and ('/history/' in key or '/sources/' in key) else self.data[key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':m.sha(raw)}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if key==m.HEAD and self.race:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=m.sha(self.data.get(key,b'')):raise Error('PreconditionFailed')
        self.data[key]=kw['Body']

def native(memory=None):
    ns={'S3':memory or Memory(),'BUCKET':'fixture','S3_KEY':m.HEAD,'DAYS_BACK':30,'TIMEOUT_BUDGET_S':260,
        'SEC_USER_AGENT':'Synthetic test contact','time':time,'datetime':datetime,'timezone':timezone,
        'Path':Path,'__file__':str(SRC/'lambda_function.py'),'urllib':urllib,'json':json,'_atom_identity':atom}
    ns.update({name:getattr(m,name) for name in ('CONTRACT','HEAD','FORMS','MAPPING','FLAGS','feed_url','source_ref','validate_ref','content','build','clock','encode','sha','strict')})
    tree=ast.parse((SRC/'lambda_function.py').read_bytes())
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and (n.name.startswith('_activist_') or n.name in ('_ActivistSources','lambda_handler'))]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated active filing functions>','exec'),ns)
    return ns

def fake_fetch(url,sources,remaining,kind):
    raw=b'{}' if kind=='json' else feed(entry()+entry('Subject','Issuer','789')) if 'SCHEDULE%2013D&' in url else feed()
    out=sources.capture(raw,url,kind,datetime.now(timezone.utc).isoformat());out['http_status']=200;return out

def publication(memory=None):
    memory=memory or Memory();ns=native(memory);ns['_activist_fetch']=fake_fetch
    with patch.object(time,'sleep'):ns['lambda_handler']()
    return memory,m.strict(memory.data[m.HEAD])

class Tests(unittest.TestCase):
    def test_whole_predecessor_and_original_four_query_bytes(self):
        raw=(ROOT/'tests/fixtures/pre-activist-filings-research.py.txt').read_bytes()
        self.assertEqual(m.sha(raw),'5dd1456a4f9cc865d9ed9d810312ad729d8e185877a4762d0555523dd598dfe4')
        self.assertTrue((SRC/'lambda_function.py').read_bytes().startswith(raw.replace(b'def lambda_handler(',b'def _legacy_lambda_handler(',1)))
        self.assertEqual(m.feed_url('SC 13D/A'),'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=SC%2013D/A&output=atom&count=100')
        self.assertEqual(len(m.FORMS),8)
    def test_namespaced_complete_summaries_and_valid_accession(self):
        p=m.parse_atom(feed(entry()),'SCHEDULE 13D',AT,30);r=p['records'][0]
        self.assertEqual(r['accession'],ACC);self.assertFalse(r['issues']);self.assertIn('X'*700,r['summaries'][0]);self.assertIn('retain',r['raw_entry_xml'])
        self.assertIsNone(r['filing_accepted_at'])
    def test_all_cofilers_roles_share_classes_and_universe_duplicates_remain(self):
        a,s=plan();p=m.build(a,s,AT,30);g=p['filing_groups'][0]
        self.assertEqual(len(p['entry_occurrences']),3);self.assertEqual(len(g['parties']),3)
        self.assertEqual(g['current_ticker_candidates'],['TEST.A','TEST.B']);self.assertEqual(len(g['current_universe_occurrences']),2)
        self.assertEqual(len(p['mapping_population']['records']),3);self.assertEqual(g['identity_issues'],[])
        self.assertIsNone(g['reported_beneficial_ownership_percent']);self.assertFalse(g['calls_eligible'])
    def test_missing_subject_stays_missing_and_no_party_is_invented(self):
        a,s=plan(entry());g=m.build(a,s,AT,30)['filing_groups'][0]
        self.assertIsNone(g['subject_cik']);self.assertIn('explicit_subject_missing_or_conflicting',g['identity_issues'])
    def test_same_accession_repeats_are_not_independent_investors(self):
        a,s=plan(entry()+entry()+entry('Subject','Issuer','789'));g=m.build(a,s,AT,30)['filing_groups'][0]
        self.assertEqual(len(g['parties']),2);self.assertEqual(len(g['occurrence_indices']),3)
        self.assertFalse(g['independent_reporting_groups_verified'])
    def test_conflicting_subject_forms_and_names_never_resolve(self):
        a,s=plan(entry()+entry('Filer','Other name','123')+entry('Subject','Issuer','789')+entry('Subject','Other issuer','999'))
        g=m.build(a,s,AT,30)['filing_groups'][0]
        self.assertIn('party_name_conflict',g['identity_issues']);self.assertIsNone(g['subject_cik'])
        a,s=plan(entry()+entry(form='SCHEDULE 13D/A')+entry('Subject','Issuer','789'));g=m.build(a,s,AT,30)['filing_groups'][0]
        self.assertIn('accession_form_missing_or_conflicting',g['identity_issues'])
    def test_future_old_bad_clocks_and_conflicting_accessions_retained_excluded(self):
        rows=entry(updated='2030-01-01T00:00:00Z')+entry(updated='2025-01-01T00:00:00Z')+entry(updated='2026-09-25')+entry(identifier='0000000999-26-000001')
        a,s=plan(rows);p=m.build(a,s,AT,30)
        self.assertEqual(len(p['entry_occurrences']),4);self.assertTrue(all(not r['identity_eligible_for_grouping'] for r in p['entry_occurrences']))
    def test_unsafe_links_duplicate_identity_fields_dtd_and_wrong_namespace(self):
        for url in ('https://[malformed/'+ACC, 'https://evil.example/'+ACC+'-index.htm',URL+'?secret=1',URL.replace('www.sec.gov','www.sec.gov@evil.example'),URL.replace('000000012326000001','000000099926000001')):
            self.assertIsNone(m.index_identity(url))
        rows=m.parse_atom(feed(entry(url='https://[malformed/'+ACC)+entry()),'SCHEDULE 13D',AT,30)['records']
        self.assertEqual(len(rows),2);self.assertFalse(rows[0]['identity_eligible_for_grouping']);self.assertTrue(rows[1]['identity_eligible_for_grouping'])
        for raw in (b'<!DOCTYPE feed [<!ENTITY x "bad">]><feed/>',b'<feed/>'):
            with self.assertRaises(ValueError):m.parse_atom(raw,'SC 13D',AT,30)
        r=m.parse_atom(feed(entry().replace('</a:id>','</a:id><a:id>conflict</a:id>')),'SCHEDULE 13D',AT,30)['records'][0]
        self.assertIn('id_missing_or_repeated',r['issues'])
    def test_empty_success_differs_from_error_partial_and_unattempted(self):
        a,s=plan('');a['feeds'][0]['http_status']=403
        p=m.build(a,s,AT,30);self.assertEqual(p['feed_coverage'][0]['status'],'http_error_original_retained');self.assertEqual(p['entry_occurrences'],[])
        for x in a['feeds']:x['http_status']=403
        with self.assertRaises(ValueError):m.build(a,s,AT,30)
    def test_changed_truncated_extra_sources_and_invalid_clocks_rejected(self):
        a,s=plan();first=next(iter(s));bad=dict(s);bad[first]+=b' '
        with self.assertRaises(ValueError):m.build(a,bad,AT,30)
        with self.assertRaises(ValueError):m.build(a,{**s,'unknown':b'private'},AT,30)
        a['feeds'][0]['received_at']='2030-01-01T00:00:00Z'
        with self.assertRaises(ValueError):m.build(a,s,AT,30)
    def test_whole_publication_history_source_identity_and_no_private_state(self):
        mem,p=publication();self.assertEqual(p['measurement_contract'],m.CONTRACT)
        self.assertEqual(mem.data[p['previous_publication']['key']],mem.previous)
        self.assertEqual(mem.data['data/activist-filings/history/'+m.sha(mem.data[m.HEAD])+'.json'],mem.data[m.HEAD])
        self.assertEqual(len(p['source_files']),3);self.assertEqual(p['all_filings'],[])
        self.assertEqual(p['summary']['new_alerts_this_run'],[]);self.assertFalse(p['private_state_read_or_written'])
        self.assertTrue(all(k in (m.HEAD,'data/universe.json') or k.startswith('data/activist-filings/sources/') or k.startswith('data/activist-filings/history/') for k in mem.reads+mem.writes))
        sources={ref['original_ref']['key']:mem.data[ref['original_ref']['key']] for ref in [p['acquisitions']['mapping'],p['acquisitions']['universe'],*p['acquisitions']['feeds']]}
        for key,value in m.build(p['acquisitions'],sources,p['generated_at'],p['window_days']).items():self.assertEqual(p[key],value)
    def test_archive_corruption_race_and_denied_read_preserve_current(self):
        for mode in ('corrupt','race','denied'):
            mem=Memory();setattr(mem,mode,True)
            with self.assertRaises((ValueError,Error)):publication(mem)
            self.assertEqual(mem.data[m.HEAD],mem.previous)
    def test_all_feed_failures_preserve_current_and_do_not_touch_state(self):
        mem=Memory();ns=native(mem);ns['_activist_fetch']=lambda url,*a:{'endpoint':url,'status':'not_attempted_rate_or_runtime_limit'}
        with patch.object(time,'sleep'),self.assertRaises(ValueError):ns['lambda_handler']()
        self.assertEqual(mem.data[m.HEAD],mem.previous)
    def test_http_denial_is_retained_and_stops_further_requests_without_retry(self):
        mem=Memory();ns=native(mem);sources=ns['_ActivistSources']();calls=[]
        class Response(BytesIO):status=429
        class Client:
            def open(self,request,timeout):calls.append(request.full_url);return Response(b'denied')
        with patch.object(urllib.request,'build_opener',return_value=Client()):
            first=ns['_activist_fetch'](m.MAPPING,sources,lambda:100,'json')
            later=ns['_activist_fetch'](m.feed_url('SC 13D'),sources,lambda:100,'xml')
        self.assertEqual(len(calls),1);self.assertEqual(first['http_status'],429)
        self.assertEqual(sources.raw[first['original_ref']['key']],b'denied');self.assertEqual(later['status'],'not_attempted_rate_or_runtime_limit')
    def test_consumer_abstains_even_from_forged_legacy_score(self):
        source=ROOT/'aws/lambdas/justhodl-compound-aggregator/source/lambda_function.py'
        fn=next(n for n in ast.parse(source.read_bytes()).body if isinstance(n,ast.FunctionDef) and n.name=='load_feed')
        class Denied:
            def get_object(self,**kw):raise AssertionError('Excluded source must not be read or vote')
        ns={'S3':Denied(),'BUCKET':'fixture','json':json,'MOMENTUM_SOURCE':MOMENTUM_SOURCE}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'<isolated compound boundary>','exec'),ns)
        for key in (m.HEAD,MOMENTUM_SOURCE,'data/volatility-squeeze.json'):
            self.assertEqual(ns['load_feed'](key,'summary.top_25_overall','subject_ticker'),[])

if __name__=='__main__':unittest.main(verbosity=2)
