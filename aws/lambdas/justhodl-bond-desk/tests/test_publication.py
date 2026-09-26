from pathlib import Path
import copy,json,sys,unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'source'))
import bond_publication as pub
from test_cohort_native import Memory,Error

AT='2026-09-26T21:00:00+00:00';OUT='2026-09-26T21:00:01+00:00'


def setup():
    m=Memory();m.data[pub.CURRENT]=b'{ "generated_at" : "2026-09-25T15:15:00+00:00", "legacy": true }'
    m.data[pub.HISTORY]=b'{"2026-09-25":{"anxiety":0,"appetite":0,"eqbond":null,"extra":"keep"}}'
    snap=pub.begin(m,'bucket',AT);history=copy.deepcopy(snap['history']['doc']);history['2026-09-26']={'anxiety':None,'appetite':None,'eqbond':None}
    doc={'generated_at':OUT,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'world_anxiety':None,'decision':{'verb':'WAIT','meaning':'abstain'},
        'anxiety_history':[{'date':k,'value':v['anxiety']} for k,v in sorted(history.items())]}
    return m,snap,doc,history


class Tests(unittest.TestCase):
    def test_whole_predecessors_and_exact_outputs_survive_publication(self):
        m,snap,doc,hist=setup();out=pub.publish(m,'bucket',snap,doc,hist)
        record=json.loads(m.data[out['publication_record']['manifest_key']])
        for name in ('head','history'):self.assertEqual(m.data[record['predecessors'][name]['key']],snap[name]['raw'])
        self.assertEqual(m.data[record['output']['key']],pub.encode(doc));self.assertEqual(m.data[record['history']['key']],pub.encode(hist))
        self.assertEqual(json.loads(m.data[pub.CURRENT]),out);self.assertEqual(json.loads(m.data[pub.HISTORY]),hist)
        self.assertIs(record['snapshot_atomic'],False);self.assertIs(record['original_source_replayed_here'],False)

    def test_access_denial_malformed_history_and_future_heads_never_become_empty(self):
        for raw in (b'',b'null',b'{"x":NaN}',b'{"x":1,"x":2}',b'{"2026-09-25":{}}'):
            m,snap,doc,hist=setup();m.data[pub.HISTORY]=raw
            with self.assertRaises((ValueError,KeyError)):pub.begin(m,'bucket',AT)
            self.assertEqual(m.writes,[])
        m,_,_,_=setup()
        with patch.object(m,'get_object',side_effect=Error('AccessDenied')),self.assertRaises(Error):pub.begin(m,'bucket',AT)
        self.assertEqual(m.writes,[])
        m.data[pub.CURRENT]=pub.encode({'generated_at':'2026-09-27T00:00:00Z'})
        with self.assertRaises(ValueError):pub.begin(m,'bucket',AT)

    def test_incomplete_history_tampered_permissions_and_nonfinite_values_write_nothing(self):
        for kind in ('missing','changed','authority','nan','time','type','view_type'):
            m,snap,doc,hist=setup()
            if kind=='missing':hist.pop('2026-09-25')
            if kind=='changed':hist['2026-09-25']['extra']='lost'
            if kind=='authority':doc['calls_eligible']=True
            if kind=='nan':doc['unqualified_legacy']=float('nan')
            if kind=='time':doc['generated_at']=AT.replace('21:','20:')
            if kind=='type':hist['2026-09-25']['anxiety']=False
            if kind=='view_type':doc['anxiety_history'][0]['value']=False
            with self.assertRaises(ValueError):pub.publish(m,'bucket',snap,doc,hist)
            self.assertEqual(m.writes,[])

    def test_concurrent_writer_before_or_during_commit_cannot_be_overwritten(self):
        for target in (pub.CURRENT,pub.HISTORY):
            m,snap,doc,hist=setup();new=b'{"generated_at":"2026-09-26T22:00:00Z"}'
            m.data[target]=new
            with self.assertRaises(ValueError):pub.publish(m,'bucket',snap,doc,hist)
            self.assertEqual(m.data[target],new);self.assertEqual(m.writes,[])
            m,snap,doc,hist=setup();put=m.put_object
            def race(**kw):
                if kw['Key']==target:m.data[target]=new
                return put(**kw)
            with patch.object(m,'put_object',side_effect=race),self.assertRaises(Error):pub.publish(m,'bucket',snap,doc,hist)
            self.assertEqual(m.data[target],new)
            self.assertTrue(any('/attempts/' in k for k in m.data))
            self.assertTrue(any(raw==snap['history']['raw'] for key,raw in m.data.items() if '/predecessors/' in key))

    def test_empty_installation_requires_conditional_creation_not_storage_denial(self):
        _,_,doc,_=setup();m=Memory();snap=pub.begin(m,'bucket',AT);hist={'2026-09-26':{'anxiety':None,'appetite':None,'eqbond':None}}
        doc['anxiety_history']=[{'date':'2026-09-26','value':None}]
        pub.publish(m,'bucket',snap,doc,hist)
        self.assertEqual(len(json.loads(m.data[pub.HISTORY])),1)


if __name__=='__main__':unittest.main()
