from pathlib import Path
import sys,time,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import ops_6040_offexchange_complete_partitions as op
from test_option_flow_store import S3

class Tests(unittest.TestCase):
    def row(self,name):return {'issueSymbolIdentifier':name,'summaryTypeCode':'ATS_W_SMBL','weekStartDate':'2026-08-31','summaryStartDate':'2026-08-31','tierIdentifier':'T1','firmCRDNumber':None,'MPID':None}
    def run_capture(self,pages,totals=None,recheck=None):
        db=S3({});seen=[];total=sum(len(v) for v in pages)
        def fetch(s3,label,spec,deadline):
            offset=spec['body']['offset'];seen.append(offset)
            if 'recheck' in label:rows=recheck if recheck is not None else pages[0];count=total
            else:
                index=len(seen)-1;rows=pages[index];count=totals[index] if totals else total
            raw=op.prior.encoded(rows);ref=op.source.retain_response(s3,raw)
            return {'http_status':200,'status':'response_retained','original':ref,'headers':{'record-offset':str(offset),'record-total':str(count),'record-limit':'2000'},'journal_key':op.key_for(label)}
        return db,seen,lambda:op.collect(db,op.partitions()[0],time.monotonic()+15,fetch)
    def test_payload_limited_pages_use_actual_count_until_reported_end(self):
        db,seen,run=self.run_capture([[self.row('A')],[self.row('B'),self.row('C')]])
        result=run();self.assertEqual(seen,[0,1,0]);self.assertEqual(result['rows'],3);self.assertEqual(result['provider_requests'],3);self.assertFalse(result['snapshot_atomic'])
    def test_duplicate_across_pages_fails_and_preserves_a_failure_journal(self):
        db,seen,run=self.run_capture([[self.row('A')],[self.row('A')]])
        with self.assertRaisesRegex(ValueError,'Duplicate grain'):run()
        state=[op.prior.strict(v) for k,v in db.data.items() if k.endswith('.json')][0]
        self.assertEqual(state['status'],'failed');self.assertEqual(len(state['pages']),2)
    def test_count_drift_and_first_page_drift_fail(self):
        _,_,run=self.run_capture([[self.row('A')],[self.row('B')]],totals=[2,3])
        with self.assertRaisesRegex(ValueError,'Reported total changed'):run()
        _,_,run=self.run_capture([[self.row('A')]],recheck=[self.row('Z')])
        with self.assertRaisesRegex(ValueError,'First page changed'):run()
    def test_explicit_partitions_are_required_for_sorting(self):
        spec=op.specification(op.partitions()[0],0);filters={v['fieldName']:v['fieldValue'] for v in spec['body']['compareFilters']}
        self.assertEqual(filters,{'summaryTypeCode':'ATS_W_SMBL','weekStartDate':'2026-08-31','tierIdentifier':'T1'})
        self.assertEqual(spec['body']['sortFields'],['issueSymbolIdentifier'])
        with self.assertRaises(ValueError):op.specification({**op.partitions()[0],'tier':'ALL'},0)
    def test_monthly_grain_uses_exact_crd_including_de_minimis_bucket(self):
        part=op.partitions()[4];base={'summaryTypeCode':part['code'],'monthStartDate':part['period'],'summaryStartDate':part['period'],'tierIdentifier':'NMS','issueSymbolIdentifier':'A','firmCRDNumber':0}
        self.assertEqual(op.grain(base,part),('A','0'));self.assertNotEqual(op.grain(base,part),op.grain({**base,'firmCRDNumber':42},part))
        with self.assertRaises(ValueError):op.grain({**base,'monthStartDate':'2026-05-01'},part)
    def test_empty_204_requires_explicit_zero_records_and_never_returns_zero_shares(self):
        spec=op.specification(op.partitions()[0],0);empty={'http_status':204,'status':'empty_http_response','headers':{'record-total':'0'},'original':{'bytes':0,'sha256':op.prior.sha(b'')}}
        rows,count=op.rows_and_count(S3({}),empty,spec);self.assertEqual(rows,[]);self.assertTrue(count['reported_end_reached'])
        with self.assertRaises(ValueError):op.rows_and_count(S3({}),{**empty,'headers':{'record-total':'2'}},spec)
if __name__=='__main__':unittest.main(verbosity=2)
