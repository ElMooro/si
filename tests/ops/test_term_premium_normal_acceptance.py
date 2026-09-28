from pathlib import Path
import importlib.util,json,sys,unittest
from datetime import datetime,timezone
from unittest.mock import Mock
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6292_term_premium_normal_acceptance as op
spec=importlib.util.spec_from_file_location('term_native_fixture',ROOT/'aws/lambdas/justhodl-term-premium/tests/test_term_native.py')
n=importlib.util.module_from_spec(spec);spec.loader.exec_module(n)


class Tests(unittest.TestCase):
    def fixture(self):
        case=n.Tests();case.setUp();self.addCleanup(case.doCleanups)
        return case,case.packet()

    def test_complete_native_workbook_tables_and_predecessors(self):
        case,packet=self.fixture();case.client.reads=[]
        result,protected=op.publication(op.model.encoded(packet),case.client)
        self.assertEqual(result['status'],'complete_native_workbook_replayed')
        self.assertTrue(result['original_workbook_replayed']);self.assertEqual(result['series'],60)
        self.assertEqual(sum(t['rows'] for t in result['tables'].values()),320)
        self.assertEqual(len(protected),2);self.assertFalse(result['investment_authority'])
        self.assertTrue(all(op.store.allowed(k) or k in protected for k in case.client.reads))

    def test_old_packet_remains_pending_without_extra_reads(self):
        db=n.Storage();result,protected=op.publication(db.objects[op.model.CURRENT],db)
        self.assertEqual(result['status'],'pending_original_schedule_publication')
        self.assertFalse(result['original_workbook_replayed']);self.assertEqual(db.reads,[]);self.assertEqual(protected,[])

    def test_typed_view_original_and_predecessor_tampering_fails(self):
        case,packet=self.fixture();raw=op.model.encoded(packet)
        bad=json.loads(raw);bad['calls_eligible']=0
        with self.assertRaises(ValueError):op.publication(op.model.encoded(bad),case.client)
        for key in (case.inputs['workbook']['key'],packet['predecessors']['packet']['key']):
            previous=case.client.objects[key];case.client.objects[key]=b'tampered'
            with self.assertRaises(ValueError):op.publication(raw,case.client)
            case.client.objects[key]=previous

    def test_arbitrary_or_ambiguous_predecessor_rejected_before_read(self):
        db=n.Storage()
        for ref in ({'key':'data/account.json','bytes':2,'sha256':'0'*64},
                    {'key':op.store.PRIVATE+'0'*64+'.bin','bytes':True,'sha256':'0'*64},
                    {'key':op.store.PRIVATE+'0'*64+'.bin','bytes':2.0,'sha256':'0'*64}):
            with self.assertRaises(ValueError):op.own_predecessor(ref,db)
        self.assertEqual(db.reads,[])

    def test_journal_reader_is_confined_to_own_prefix_and_reads_latest_only(self):
        db=n.Storage();prefix=op.store.PRIVATE+'requests/';keys=[prefix+c*64+'.json' for c in ('a','b')]
        db.objects.update({key:b'{"status":"failed","error_type":"ValueError","provider_request_attempts":1}' for key in keys})
        pages=[{'Contents':[{'Key':key,'LastModified':datetime(2026,9,27+i,tzinfo=timezone.utc)}]} for i,key in enumerate(keys)]
        paginator=Mock();paginator.paginate.return_value=pages;db.get_paginator=Mock(return_value=paginator)
        result,protected=op.latest_source_journal(db)
        self.assertEqual(protected,[keys[1]]);self.assertEqual(db.reads,[keys[1]]);self.assertEqual(result['error_type'],'ValueError')
        paginator.paginate.assert_called_once_with(Bucket=op.BUCKET,Prefix=prefix)
        pages[0]['Contents'][0]['Key']='data/account.json';db.reads=[]
        with self.assertRaises(ValueError):op.latest_source_journal(db)
        self.assertEqual(db.reads,[])


if __name__=='__main__':unittest.main(verbosity=2)
