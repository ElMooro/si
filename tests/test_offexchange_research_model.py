from pathlib import Path
from copy import deepcopy
import sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import offexchange_research_model as model
import test_offexchange_measurements as fixture_module

def fixture():
    objects={}
    def protect(raw):
        ref={'key':model.PRIVATE+model.sha(raw)+'.bin','sha256':model.sha(raw),'bytes':len(raw)};objects[ref['key']]=raw;return ref
    def capture(raw,url,body,kind):
        return {'url':url,'body':body,'kind':kind,'requested_at':'2026-09-25T01:00:00Z','received_at':'2026-09-25T01:00:01Z',
            'http_status':200,'status':'response_retained','original':protect(raw),'headers':{}}
    parts={};f=fixture_module.Tests()
    for code in ('ATS_W_SMBL','OTC_W_SMBL','OTC_M_SMBL_FIRM'):
        weekly=code.endswith('W_SMBL');dataset='weeklySummary' if weekly else 'monthlySummary';period='2026-08-31' if weekly else '2026-06-01';tier='T1' if weekly else 'NMS';field='weekStartDate' if weekly else 'monthStartDate'
        rows=[f.row(code)] if weekly else [f.monthrow(),f.monthrow(0,'De Minimis Firms','40')]
        body={'offset':0,'limit':2000,'sortFields':['issueSymbolIdentifier'] if weekly else ['issueSymbolIdentifier','issueName','firmCRDNumber'],
            'compareFilters':[{'fieldName':k,'compareType':'EQUAL','fieldValue':v} for k,v in (('summaryTypeCode',code),(field,period),('tierIdentifier',tier))]}
        page=capture(model.encoded(rows),'https://api.finra.org/data/group/otcMarket/name/'+dataset,body,'probe')
        page['headers']={'record-total':str(len(rows)),'record-offset':'0','record-limit':'2000'}
        recheck=deepcopy(page);recheck.update(requested_at='2026-09-25T01:00:02Z',received_at='2026-09-25T01:00:03Z')
        parts[code]={'partition':{'dataset':dataset,'code':code,'period':period,'tier':tier},'pages':[page,recheck],'rows':len(rows),'reported_total':len(rows)}
    daily=capture(f.daily(),'https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260924.txt',None,'daily_file')
    return {'contract':'offexchange-original-inputs.v1','generated_at':'2026-09-25T02:00:00Z','partitions':parts,'daily':daily,'daily_date':'2026-09-24'},objects

class Tests(unittest.TestCase):
    def test_originals_reconstruct_dated_evidence_and_never_create_a_vote(self):
        inputs,objects=fixture();out=model.compile_output(inputs,objects.__getitem__);packet=out['packet']
        self.assertEqual(packet['counts']['weekly_source_rows'],2);self.assertEqual(packet['counts']['monthly_source_rows'],2)
        self.assertEqual(packet['decision']['verb'],'WAIT');self.assertEqual(packet['board'],[])
        self.assertTrue(all(packet[field] is False for field in model.FLAGS));self.assertIsNone(packet['dix']['own_dix_pct'])
        record=out['shards'][model.bucket('AAPL')]['records']['AAPL'];self.assertEqual(record['weekly'][0]['ats_pct_of_reported_offexchange'],'50.000000000000')
        self.assertEqual(record['monthly'][0]['reported_activity_hhi_upper_bound'],'5200.000000000000')
        self.assertFalse(record['security_master_identity_verified']);self.assertEqual(record['daily'][0]['source_line'],2)
        self.assertEqual(out,model.compile_output(inputs,objects.__getitem__))
    def test_every_public_shard_binds_exact_bytes_and_symbol_bucket(self):
        inputs,objects=fixture();out=model.compile_output(inputs,objects.__getitem__)
        for key,shard in out['shards'].items():
            self.assertEqual(out['packet']['record_shards'][key],model.record_identity(shard))
            self.assertTrue(all(model.bucket(symbol)==key for symbol in shard['records']))
    def test_tampered_whole_original_is_rejected(self):
        inputs,objects=fixture();objects[inputs['daily']['original']['key']]+=b' '
        with self.assertRaisesRegex(ValueError,'Original source identity'):model.compile_output(inputs,objects.__getitem__)
    def test_incomplete_page_count_and_duplicate_partition_are_rejected(self):
        inputs,objects=fixture();inputs['partitions']['ATS_W_SMBL']['reported_total']=2
        with self.assertRaises(ValueError):model.compile_output(inputs,objects.__getitem__)
        inputs,objects=fixture();inputs['partitions']['duplicate']=deepcopy(inputs['partitions']['ATS_W_SMBL'])
        with self.assertRaisesRegex(ValueError,'Duplicate partition'):model.compile_output(inputs,objects.__getitem__)
    def test_wrong_query_scope_or_endpoint_cannot_relabel_source_bytes(self):
        for mutation in ('endpoint','tier','filter','offset'):
            inputs,objects=fixture();page=inputs['partitions']['ATS_W_SMBL']['pages'][0]
            if mutation=='endpoint':page['url']='https://example.org/source'
            if mutation=='tier':page['body']['compareFilters'][-1]['fieldValue']='T2'
            if mutation=='filter':page['body']['compareFilters'].append(deepcopy(page['body']['compareFilters'][0]))
            if mutation=='offset':page['body']['offset']=5
            with self.assertRaises(ValueError):model.compile_output(inputs,objects.__getitem__)
    def test_acquisition_future_dates_error_response_and_length_mismatch_fail(self):
        for key,value in [('received_at','2026-09-26T00:00:00Z'),('requested_at','2026-09-25T01:01:00Z'),('http_status',503),('headers',{'content-length':'1'})]:
            inputs,objects=fixture();inputs['daily'][key]=value
            with self.assertRaises(ValueError):model.compile_output(inputs,objects.__getitem__)
    def test_current_or_account_paths_are_never_read_as_originals(self):
        inputs,objects=fixture();inputs['daily']['original']['key']='data/trade-tickets.json';reads=[]
        def read(key):reads.append(key);return objects[key]
        with self.assertRaises(ValueError):model.compile_output(inputs,read)
        self.assertNotIn('data/trade-tickets.json',reads)
    def test_first_page_recheck_must_bind_the_same_query_and_original(self):
        inputs,objects=fixture();inputs['partitions']['ATS_W_SMBL']['pages'][-1]['body']['limit']=1
        with self.assertRaisesRegex(ValueError,'First-page recheck'):model.compile_output(inputs,objects.__getitem__)
    def test_compiler_does_not_mutate_original_input_manifest(self):
        inputs,objects=fixture();before=deepcopy(inputs);model.compile_output(inputs,objects.__getitem__);self.assertEqual(inputs,before)
    def test_recheck_before_the_last_original_page_is_rejected(self):
        inputs,objects=fixture();inputs['partitions']['ATS_W_SMBL']['pages'][-1].update(requested_at='2026-09-25T00:59:00Z',received_at='2026-09-25T00:59:01Z')
        with self.assertRaisesRegex(ValueError,'predates completion'):model.compile_output(inputs,objects.__getitem__)
if __name__=='__main__':unittest.main(verbosity=2)
