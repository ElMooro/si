"""Invented native clients; never access a Lambda, receipt or application packet."""
from pathlib import Path
import copy,runpy,sys,types,unittest
from unittest.mock import Mock,patch
R=Path(__file__).resolve().parents[3]
FNS=('justhodl-invented-a','justhodl-invented-b')


class Packages(unittest.TestCase):
    def load(self):
        helper=types.SimpleNamespace(runtime=Mock(),BUCKET='invented-bucket',schedule_evidence=Mock(),verified_alias=Mock())
        with patch.dict(sys.modules,{'market_runtime_evidence':helper}):
            module=runpy.run_path(str(R/'aws/ops/staged/ops_6462_signal_logger_packages.py'))
        g=module['inspect'].__globals__;g['SOURCES']={fn:{'invented.py':'invented-hash'} for fn in FNS}
        g['BASELINE']={fn:{'CodeSha256':'code-'+fn} for fn in FNS};g['ScheduleInventory']=lambda client:client
        helper.runtime.side_effect=lambda lam,s3,events,scheduler,fn:{'function_name':fn,'code_sha256':'code-'+fn,'source_files_checked':1,'receipt':{'status':'missing_predecessor_receipt'}}
        lam=Mock();lam.get_function_configuration.side_effect=lambda FunctionName:{'FunctionName':FunctionName,'State':'Active','LastUpdateStatus':'Successful','CodeSha256':'code-'+FunctionName}
        return module,helper,lam

    def test_all_named_sources_and_stable_native_identities_required(self):
        m,h,lam=self.load();out=m['inspect'](lam,Mock(),Mock(),Mock())
        self.assertTrue(out['all_matched']);self.assertEqual(set(out['functions']),set(FNS));self.assertEqual(out['source_members_planned'],2)
        self.assertEqual([c.args[-1] for c in h.runtime.call_args_list],list(FNS))
        self.assertEqual([c.kwargs['FunctionName'] for c in lam.get_function_configuration.call_args_list],list(FNS))
        for key in ('normal_publication_verified','source_qualified','investment_authority'):self.assertIs(out[key],False)
        for key in ('native_invocations','provider_requests','application_packet_reads','private_reads','account_reads','ledger_reads','native_writes','schedule_changes'):self.assertEqual(out[key],0)

    def test_wrong_identity_changed_code_missing_or_boolean_count_refused(self):
        for key,value in [('function_name','other'),('code_sha256','changed'),('source_files_checked',None),('source_files_checked',True),('source_files_checked',2)]:
            m,h,lam=self.load();original=h.runtime.side_effect
            h.runtime.side_effect=lambda *args,k=key,v=value:{**original(*args),k:v}
            with self.subTest(key=key,value=value):
                out=m['inspect'](lam,Mock(),Mock(),Mock());self.assertFalse(out['all_matched']);self.assertEqual(len(out['functions']),2)

    def test_after_read_identity_state_and_code_must_stay_stable(self):
        for key,value in [('FunctionName','other'),('State','Pending'),('LastUpdateStatus','InProgress'),('CodeSha256','changed')]:
            m,h,lam=self.load();original=lam.get_function_configuration.side_effect
            lam.get_function_configuration.side_effect=lambda **kwargs:{**original(**kwargs),key:value}
            self.assertFalse(m['inspect'](lam,Mock(),Mock(),Mock())['all_matched'])

    def test_signed_url_or_native_error_content_never_reaches_report(self):
        m,h,lam=self.load();h.runtime.side_effect=PermissionError('https://invented/zip?PRIVATE_CANARY=secret')
        out=m['inspect'](lam,Mock(),Mock(),Mock())
        self.assertFalse(out['all_matched']);self.assertNotIn('PRIVATE_CANARY',repr(out));self.assertNotIn('https:',repr(out))
        self.assertTrue(all(row['error_type']=='PermissionError' for row in out['functions'].values()))

    def test_one_bad_package_still_reports_all_named_functions(self):
        m,h,lam=self.load();original=h.runtime.side_effect
        def run(*args):
            if args[-1]==FNS[0]:raise ValueError('Invented mismatch')
            return original(*args)
        h.runtime.side_effect=run;out=m['inspect'](lam,Mock(),Mock(),Mock())
        self.assertFalse(out['all_matched']);self.assertFalse(out['functions'][FNS[0]]['matched']);self.assertTrue(out['functions'][FNS[1]]['matched'])

    def test_receipt_client_rejects_any_application_or_wrong_bucket_read(self):
        m,h,lam=self.load();client=Mock();guard=m['ReceiptOnly'](client)
        for request in ({'Bucket':'invented-bucket','Key':'data/feature-bus.json'}, {'Bucket':'other','Key':'data/ops/releases/'+FNS[0]+'.json'}, {'Bucket':'invented-bucket','Key':'data/ops/releases/other.json'}, {'Bucket':'invented-bucket','Key':'data/ops/releases/'+FNS[0]+'.json','VersionId':'extra'}):
            with self.assertRaises(ValueError):guard.get_object(**request)
        client.get_object.assert_not_called()
        request={'Bucket':'invented-bucket','Key':'data/ops/releases/'+FNS[0]+'.json'};guard.get_object(**request);client.get_object.assert_called_once_with(**request)

    def test_incomplete_scope_or_changed_source_fails_before_native_work(self):
        m,h,lam=self.load()
        with self.assertRaises(ValueError):m['validate_sources']()
        g=m['validate_sources'].__globals__;g['BASELINE']={str(i):{} for i in range(17)};g['SOURCES']={str(i):{'invented.py':'wrong'} for i in range(17)}
        with patch.object(Path,'read_bytes',return_value=b'invented source'),self.assertRaisesRegex(ValueError,'Reviewed predecessor source changed'):
            m['validate_sources']()
        h.runtime.assert_not_called()

    def test_main_persists_failure_before_raising(self):
        for matched in (False,True):
            m,h,lam=self.load();g=m['main'].__globals__;g['validate_sources']=Mock();g['inspect']=Mock(return_value={'all_matched':matched})
            report=Mock();report.__enter__=Mock(return_value=report);report.__exit__=Mock(return_value=False)
            external={'boto3':types.SimpleNamespace(client=Mock(return_value=object())), 'ops_report':types.SimpleNamespace(report=Mock(return_value=report))}
            with patch.dict(sys.modules,external):
                if matched:m['main']()
                else:
                    with self.assertRaises(ValueError):m['main']()
            report.kv.assert_called_once_with(evidence={'all_matched':matched})


if __name__=='__main__':unittest.main(verbosity=2)
