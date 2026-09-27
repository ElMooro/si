"""Actual active functions with isolated public donor fixtures; no AWS or network."""
from pathlib import Path
from datetime import datetime,timezone
from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace
import ast,json,sys,time,unittest
ROOT=Path(__file__).resolve().parents[4];SRC=ROOT/'aws/lambdas/justhodl-scarcity-radar/source';sys.path.insert(0,str(SRC))
from scarcity_observations import *
KEY='data/scarcity-radar.json'


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self,previous=b'{"version":"1.1.0","stealth_shortage_board":[]}'):
        self.data={} if previous is None else {KEY:previous};self.reads=[];self.writes=[];self.denied=set();self.corrupt=False;self.race=False;self.truncated=False
        for key,(_,fields) in SOURCES.items():
            self.data[key]=json.dumps({'generated_at':'2026-09-25T00:00:00Z',fields[0]:[{'ticker':'TEST','zero':0,'missing':None,'why':'unverified'}]}).encode()
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key in self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key]
        if self.corrupt and ('/history/' in key or key.startswith(PRIVATE)):raw=b'wrong'
        return {'Body':BytesIO(raw),'ContentLength':len(raw)+int(self.truncated),'ETag':sha(raw),'LastModified':datetime(2026,9,25,tzinfo=timezone.utc)}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if key!=KEY and kw.get('IfNoneMatch')!='*':raise AssertionError('Archives must be immutable')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if key==KEY and self.race:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=sha(self.data.get(key,b'')):raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None):
    ns={**globals(),'S3':memory or Memory(),'BUCKET':'fixture','OUT_KEY':KEY}
    tree=ast.Module(body=[n for n in ast.parse((SRC/'lambda_function.py').read_bytes()).body if isinstance(n,ast.FunctionDef) and (n.name.startswith('_scarcity_') or n.name=='lambda_handler')],type_ignores=[])
    exec(compile(tree,'<actual active scarcity functions>','exec'),ns);return ns


def compiled():
    memory=Memory();ns=native(memory);ns['lambda_handler']();return strict(memory.data[KEY]),memory


class Tests(unittest.TestCase):
    def test_preserved_original_body_and_active_path_excludes_legacy_learning_and_scores(self):
        old=ast.parse((ROOT/'tests/fixtures/pre-scarcity-radar-statement-observations.py.txt').read_bytes())
        new=ast.parse((SRC/'lambda_function.py').read_bytes())
        original=next(n for n in old.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        retained=next(n for n in new.body if isinstance(n,ast.FunctionDef) and n.name=='_legacy_lambda_handler');retained.name='lambda_handler'
        self.assertEqual(ast.dump(original),ast.dump(retained))
        p,memory=compiled();self.assertEqual(p['source_count'],7);self.assertEqual(p['signals_logged'],0)
        self.assertFalse(p['calls_eligible']);self.assertFalse(p['sizing_eligible']);self.assertIsNone(p['call']);self.assertIsNone(p['counts']['prime']);self.assertEqual(p['stealth_shortage_board'],[])
        self.assertTrue(all(k in SOURCES or k==KEY or k.startswith(PRIVATE) or k.startswith('data/scarcity-radar/history/') for k in memory.reads+memory.writes))
    def test_all_original_bytes_rows_unknown_fields_zeros_and_duplicate_occurrences_are_preserved(self):
        memory=Memory();key='data/chokepoint.json'
        packet={'unknown':{'keep':'everything'},'all_chokepoints':[{'ticker':'TEST','zero':0,'missing':None}]*40+[None,{'ticker':'TEST','extra':'x'*70000}]}
        raw=json.dumps(packet,indent=2).encode();memory.data[key]=raw;native(memory)['lambda_handler']();p=strict(memory.data[KEY])
        captured=next(r for r in p['source_inventory'] if r['key']==key)['capture'];self.assertEqual(memory.data[captured['original']['key']],raw)
        rows=[r for r in p['donor_occurrences'] if r['source_key']==key];self.assertEqual(len(rows),42);self.assertEqual([r['raw'] for r in rows],packet['all_chokepoints']);self.assertEqual(len({r['occurrence_id'] for r in rows}),42)
        self.assertNotIn('source_packets',p) # Avoid recursive Scarcity -> Inventory -> Scarcity embedding.
    def test_source_prose_is_not_upgraded_to_institutional_buying_or_shortage(self):
        memory=Memory();key='data/narrative-vs-tape.json';memory.data[key]=b'{"quiet_accumulation":[{"ticker":"TEST","edge":"No inference of institutional buying"}]}'
        native(memory)['lambda_handler']();p=strict(memory.data[KEY]);row=next(r for r in p['donor_occurrences'] if r['source_key']==key)
        self.assertEqual(row['raw']['edge'],'No inference of institutional buying');self.assertFalse(row['independence_verified']);self.assertNotIn('quiet',row);self.assertIsNone(row['call'])
    def test_source_pointer_escapes_and_every_by_theme_occurrence_survives(self):
        raw=b'{"by_theme":{"A/B~C":{"score":0},"X":null}}';ref=reference(raw)
        rows,pops=observations('data/supply-inflection.json',strict(raw),ref)
        self.assertEqual(rows[0]['source_pointer'],'/by_theme/A~1B~0C');self.assertEqual(rows[1]['raw'],None);self.assertEqual(len(rows),2)
    def test_unqualified_clocks_and_malformed_populations_are_explicit_not_fresh(self):
        memory=Memory();key='data/chokepoint.json';memory.data[key]=b'{"generated_at":"2099-01-01T00:00:00Z","all_chokepoints":"bad"}'
        native(memory)['lambda_handler']();p=strict(memory.data[KEY]);row=next(r for r in p['source_inventory'] if r['key']==key)
        self.assertEqual(row['producer_clock_status'],'future');self.assertEqual(row['populations'][-1]['status'],'invalid_population_type');self.assertFalse(row['may_vote']);self.assertEqual(p['quality']['status'],'partial')
    def test_malformed_originals_retained_without_parsing_or_claiming_valid_data(self):
        for raw in [b'{"x":1,"x":2}',b'{"x":NaN}',b'[1e999]',b'[1e-999]',b'\xff',b'[]',b'']:
            memory=Memory();key=next(iter(SOURCES));memory.data[key]=raw;native(memory)['lambda_handler']();p=strict(memory.data[KEY]);capture=p['source_inventory'][0]['capture']
            self.assertEqual(capture['status'],'invalid_original');self.assertEqual(memory.data[capture['original']['key']],raw)
    def test_missing_denied_size_and_time_states_remain_distinct(self):
        memory=Memory();keys=list(SOURCES);del memory.data[keys[0]];memory.denied.add(keys[1]);memory.data[keys[2]]=b' '* (MAX+1)
        native(memory)['lambda_handler']();p=strict(memory.data[KEY]);self.assertEqual([r['status'] for r in p['source_inventory'][:3]],['missing','unavailable','size_bound'])
        memory=Memory();values=iter([30000,10000]);native(memory)['lambda_handler'](context=SimpleNamespace(get_remaining_time_in_millis=lambda:next(values,10000)))
        p=strict(memory.data[KEY]);self.assertEqual(p['source_count'],1);self.assertEqual(sum(r['status']=='time_bound' for r in p['source_inventory']),6)
    def test_total_failure_or_runtime_exhaustion_preserves_previous_publication(self):
        for time_left in (0,180000):
            memory=Memory();before=memory.data[KEY];memory.denied.update(SOURCES)
            with self.assertRaises(ValueError):native(memory)['lambda_handler'](context=SimpleNamespace(get_remaining_time_in_millis=lambda:time_left))
            self.assertEqual(memory.data[KEY],before);self.assertNotIn(KEY,memory.writes)
    def test_archive_corruption_and_concurrent_publish_preserve_previous(self):
        for field in ('corrupt','race'):
            memory=Memory();setattr(memory,field,True);before=memory.data[KEY]
            with self.assertRaises((ValueError,OverflowError,Error)):native(memory)['lambda_handler']()
            self.assertEqual(memory.data[KEY],before)
    def test_previous_missing_allowed_but_access_denial_truncation_and_malformed_rejected(self):
        memory=Memory(None);native(memory)['lambda_handler']();self.assertIsNone(strict(memory.data[KEY])['previous_publication'])
        for memory in [Memory(b'{bad'),Memory(),Memory()]:
            if memory.data[KEY]!=b'{bad':
                if not hasattr(self,'denial_tested'):memory.denied.add(KEY);self.denial_tested=True
                else:memory.truncated=True
            with self.assertRaises((ValueError,Error)):native(memory)['lambda_handler']()
    def test_undeclared_private_or_consumer_keys_fail_before_read(self):
        memory=Memory();ns=native(memory)
        for key in ['data/pm-decision.json','private/accounts.json','learning/log.json']:
            with self.assertRaises(ValueError):ns['_scarcity_capture'](key,MAX)
        self.assertEqual(memory.reads,[])
    def test_whole_replay_checks_inventory_bytes_clocks_and_status(self):
        p,memory=compiled();captures={r['key']:r['capture'] for r in p['source_inventory']};raws={k:memory.data[c['original']['key']] for k,c in captures.items()}
        expected=compile_packet(captures,raws,p['generated_at']);self.assertTrue(all(p[k]==v for k,v in expected.items()))
        for mutate in [lambda c:c.pop(next(iter(c))),lambda c:c[next(iter(c))]['original'].update(bytes=1),lambda c:c[next(iter(c))].update(received_at='2099-01-01T00:00:00Z')]:
            changed=deepcopy(captures);mutate(changed)
            with self.assertRaises(ValueError):compile_packet(changed,raws,p['generated_at'])
    def test_previous_and_current_exact_public_packets_are_archived(self):
        memory=Memory();prior=memory.data[KEY];native(memory)['lambda_handler']();p=strict(memory.data[KEY]);self.assertEqual(memory.data[p['previous_publication']['key']],prior)
        self.assertEqual(memory.data['data/scarcity-radar/history/'+sha(memory.data[KEY])+'.json'],memory.data[KEY])


if __name__=='__main__':unittest.main(verbosity=2)
