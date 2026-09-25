from pathlib import Path
import copy,importlib.util,sys,unittest,json
from unittest.mock import patch,Mock
from contextlib import contextmanager
from io import BytesIO
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/checks','aws/ops/staged')]
spec=importlib.util.spec_from_file_location('stage_acceptance',ROOT/'aws/ops/staged/ops_6111_accounting_control_journal_acceptance.py')
acceptance=importlib.util.module_from_spec(spec);spec.loader.exec_module(acceptance)
from test_statement_producer import fixture,S3


def evidence():
    f,s3,reference=fixture();proof=f.verify(f.compile())
    previous={'status':'failed','error_type':'HTTPError','request_id':acceptance.prior.REQUEST,
        'diagnostic':acceptance.prior.DIAGNOSTIC,'replay':acceptance.prior.REFERENCE,
        'stages':{'source_replay_seconds':148.951,'independent_check_seconds':80.1}}
    # The failed journal really has no qualification field. 6107's code puts
    # it only into an intermediate journal and then overwrites that journal.
    ready={'replay':acceptance.prior.REFERENCE,'qualification':proof}
    return acceptance.source.strict(acceptance.source.encoded(previous)),ready,Path(acceptance.prior.__file__).read_bytes()


class Tests(unittest.TestCase):
    def test_complete_acceptance_from_strict_json_through_final_journal_without_provider_or_replay(self):
        previous,ready,script=evidence();s3=S3({});src=acceptance.source;capture=acceptance.capture
        def retain(value):return capture.retain(s3,src.encoded(value))
        def artifact(kind,n):
            digest=format(n,'064x');return {'key':'data/statement-research/'+kind+'/'+digest+'.json','sha256':digest,'bytes':1}
        reference=acceptance.prior.REFERENCE
        packet={'reported_names':500,'provider_responses':3000,'provider_rows':20000,'generated_at':'2026-09-25T16:26:15Z',
            'identity_index':{'original':artifact('inputs',4)},'issuers':[{'record':artifact('records',n)} for n in range(10,510)]}
        inputs={'source_manifest':retain({'source':'manifest'}),'identity_capture':retain({'source':'identity'})}
        run={'input':artifact('inputs',2),'output':artifact('outputs',3),
            'compilers':{'module'+str(n):artifact('compilers',n) for n in range(700,707)}}
        public={reference['manifest_key'],run['input']['key'],run['output']['key'],packet['identity_index']['original']['key'],
            *(r['record']['key'] for r in packet['issuers']),*(r['key'] for r in run['compilers'].values())}
        ready.update(fmp_requests=3000,sec_identity_requests=1,generated_at=packet['generated_at'],
            **dict.fromkeys(('producer_invocations','consumer_invocations','private_account_reads','signal_writes','notifications_sent','paid_ai_calls'),0))
        original={'status':'complete','request_id':'scheduled-accounting-source:36157251200','result':{'ready_advanced':True,'replay':reference},**inputs}
        diagnostic={'whole_ready_document':retain(ready),'whole_source_journal':retain(original),'replay':reference,
            'public_original_bytes_match':True,'mismatches':[],
            'results':[{'key':key,'matched':True,'status':200,'origin_bytes':1,'public_bytes':1,
                'origin_sha256':key.rsplit('/',1)[1].split('.')[0],'public_sha256':key.rsplit('/',1)[1].split('.')[0]} for key in public]}
        diagnostic_ref=retain(diagnostic);previous['diagnostic']=diagnostic_ref
        # Match the real on-disk journal wire format, then strict Decimal read.
        wire={**previous,'stages':{'source_replay_seconds':148.951,'independent_check_seconds':80.1}}
        s3.files[acceptance.prior.STATUS]=src.encoded(wire)
        s3.files[acceptance.producer.READY]=src.encoded(ready)
        s3.files['data/ops/releases/justhodl-forensic-screen.json']=src.encoded({'commit':acceptance.prior.NATIVE_COMMIT,'code_sha256':'native-code'})
        current=src.encoded({**packet,'replay':reference});s3.files[acceptance.producer.CURRENT]=current
        reply=BytesIO(current);reply.status=200;reply.headers={'Cache-Control':'no-store'}
        lam=Mock();lam.get_function_configuration.return_value={'CodeSha256':'native-code'}
        reporter=Mock()
        @contextmanager
        def report(_):yield reporter
        def checked(ref,kind,read):return packet if kind=='outputs' else inputs
        with patch.object(acceptance.prior,'DIAGNOSTIC',diagnostic_ref),patch.object(acceptance.subprocess,'run'),\
                patch.object(acceptance.boto3,'client',side_effect=lambda service,**kw:s3 if service=='s3' else lam),\
                patch.object(acceptance,'report',report),patch.object(acceptance.store,'verified_run',return_value=run),\
                patch.object(acceptance.store,'checked',side_effect=checked),patch.object(acceptance.producer,'current_matches',return_value=True),\
                patch.object(acceptance,'denied_with_retry',return_value=True),\
                patch.object(acceptance.urllib.request,'urlopen',return_value=reply) as http,\
                patch.object(acceptance.store,'replay',side_effect=AssertionError('Must adopt completed replay')):
            acceptance.main()
        complete=json.loads(s3.files[acceptance.STATUS]);self.assertEqual(complete['status'],'complete')
        result=complete['result'];self.assertEqual(result['stages_seconds']['source_replay_seconds'],'148.951')
        self.assertEqual(result['public_artifacts_checked'],511);self.assertEqual(result['provider_requests'],0)
        self.assertFalse(result['replay_repeated']);self.assertEqual(http.call_count,1)
        self.assertTrue(result['native_current_matches_ready']);self.assertTrue(result['current_ready_matches_snapshot'])
        self.assertTrue(all(w['Key'].startswith(capture.PRIVATE) for w in s3.writes))
        self.assertEqual(s3.files[acceptance.producer.CURRENT],current);lam.invoke.assert_not_called()

    def test_final_failure_journal_without_inline_proof_recovers_exact_verified_ready(self):
        previous,ready,script=evidence();before=copy.deepcopy(previous)
        self.assertNotIn('qualification',previous)
        self.assertEqual(acceptance.completed_proof(previous,ready,script),ready['qualification'])
        self.assertEqual(previous,before)
        self.assertEqual(ready['qualification']['metric_comparisons'],34)

    def test_missing_incomplete_or_wrong_failure_stages_cannot_be_adopted(self):
        changes=[lambda p:p['stages'].pop('independent_check_seconds'),
            lambda p:p.update(error_type='AssertionError'),lambda p:p.update(status='arithmetic_verified'),
            lambda p:p.update(request_id='another'),lambda p:p.update(replay={}),lambda p:p.update(diagnostic={})]
        changes += [lambda p,v=v:p['stages'].update(independent_check_seconds=v) for v in (0,-1,True,'80',None,float('nan'),float('inf'))]
        for change in changes:
            previous,ready,script=evidence();change(previous)
            with self.assertRaises(ValueError):acceptance.completed_proof(previous,ready,script)

    def test_different_code_snapshot_or_missing_proof_is_rejected(self):
        previous,ready,script=evidence()
        with self.assertRaises(ValueError):acceptance.completed_proof(previous,ready,script+b'\n')
        for update in ({'replay':{}},{'qualification':None},{'qualification':{}},{'qualification':[]}):
            with self.assertRaises(ValueError):acceptance.completed_proof(previous,{**ready,**update},script)

    def test_current_delivery_is_identified_get_without_invocation_or_private_target(self):
        request=acceptance.public_request('data/forensic-screen.json')
        self.assertEqual(request.get_method(),'GET');self.assertIsNone(request.data)
        self.assertEqual(request.get_header('User-agent'),'JustHodl-research-acceptance/1.0')
        self.assertFalse(request.has_header('Authorization'))
        for key in ('data/account.json','https://example.com','data/forensic-screen.json?kickoff=1'):
            with self.assertRaises(ValueError):acceptance.public_request(key)


if __name__=='__main__':unittest.main(verbosity=2)
