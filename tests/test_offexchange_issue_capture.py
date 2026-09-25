from pathlib import Path
import sys,time,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import ops_6042_offexchange_reported_issue_capture as op
from test_option_flow_store import S3
class Tests(unittest.TestCase):
    def row(self,name='Issue A'):
        return {'summaryTypeCode':'OTC_M_SMBL_FIRM','monthStartDate':'2026-06-01','summaryStartDate':'2026-06-01','tierIdentifier':'NMS','issueSymbolIdentifier':'NVA','issueName':name,'firmCRDNumber':0}
    def test_issue_name_is_part_of_grain_and_sorting_without_asserting_identity(self):
        self.assertNotEqual(op.grain(self.row(),op.PART),op.grain(self.row('Issue B'),op.PART))
        self.assertEqual(op.spec(0)['body']['sortFields'],['issueSymbolIdentifier','issueName','firmCRDNumber'])
    def test_two_reported_issues_survive_pagination_and_exact_duplicate_fails(self):
        for second,good in [('Issue B',True),('Issue A',False)]:
            db=S3({});calls=[]
            def fetch(s3,label,spec,deadline):
                offset=spec['body']['offset'];calls.append(offset);raw=op.prior.encoded([self.row('Issue A' if offset==0 else second)])
                return {'http_status':200,'status':'response_retained','original':op.source.retain_response(s3,raw),
                    'headers':{'record-total':'2','record-offset':str(offset),'record-limit':'2000'},'journal_key':op.key_for(label)}
            if good:
                result=op.collect(db,time.monotonic()+10,fetch);self.assertEqual(result['rows'],2);self.assertEqual(calls,[0,1,0])
            else:
                with self.assertRaisesRegex(ValueError,'Duplicate reported issue'):op.collect(db,time.monotonic()+10,fetch)
if __name__=='__main__':unittest.main(verbosity=2)
