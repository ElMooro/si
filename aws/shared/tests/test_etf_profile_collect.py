"""Profile collection retains complete responses and fails closed on truncation."""
from pathlib import Path
from unittest import mock
import io,json,sys,time,unittest,urllib.error
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).parent)]
import etf_profile_collect as collect
import etf_profile_native as native
from test_etf_profile_native import row,AT

class ProfileCollection(unittest.TestCase):
    def arrange(self,key='fixture-only-secret'):
        saved={}
        def save(raw):
            digest=native.sha(raw);path=native.PRIVATE+digest+'.bin';saved[path]=raw
            return {'key':path,'sha256':digest,'bytes':len(raw)}
        return collect.Collector(key,save,time.monotonic()+100,'2026-09-21'),saved
    def test_exact_profile_selection_followup_and_header_auth(self):
        c,saved=self.arrange();raw=native.encoded({'status':'OK','results':[row()],'count':1,'extension':{'preserved':True}})
        opener=mock.Mock();opener.open.side_effect=[io.BytesIO(raw),io.BytesIO(raw)]
        with mock.patch.object(collect.urllib.request,'build_opener',return_value=opener),mock.patch.object(collect,'now',return_value=AT):
            result=c.snapshot('VOO','2026-09-21')
        self.assertEqual(result['status'],'complete_returned_profile_snapshot')
        self.assertEqual(c.requests,2);self.assertEqual(list(saved.values()),[raw])
        for call in opener.open.call_args_list:
            req=call.args[0];self.assertEqual(req.get_header('Authorization'),'Bearer fixture-only-secret')
            self.assertNotIn('fixture-only-secret',req.full_url)
        restored=native.reconstruct(result,saved.__getitem__,AT)
        self.assertTrue(restored['quality']['single_profile_unambiguous'])
    def test_missing_credential_or_expired_deadline_do_not_request(self):
        c,_=self.arrange('')
        with mock.patch.object(collect.urllib.request,'build_opener') as open_:
            pair=c.fund('VOO');self.assertEqual(pair['current']['status'],'credential_unavailable')
            self.assertEqual(pair['prior']['cutoff'],'2026-08-22');open_.assert_not_called()
        c,_=self.arrange();c.deadline=time.monotonic()-1
        with mock.patch.object(collect.urllib.request,'build_opener') as open_:
            self.assertEqual(c.snapshot('VOO','2026-09-21')['status'],'acquisition_deadline');open_.assert_not_called()
    def test_failed_pagination_keeps_prefix_without_claiming_complete(self):
        c,_=self.arrange();page={'url':native.snapshot_url('VOO','2026-09-18'),'original':{'fixture':True},'acquired_at':AT}
        values=[({'results':[row()]},{'selection':True}),({'results':[row()],'next_url':native.ENDPOINT+'?cursor=second'},page),(None,{'status':'provider_http_error','http_status':403})]
        with mock.patch.object(c,'page',side_effect=values):result=c.snapshot('VOO','2026-09-21')
        self.assertEqual(result['pages'],[page]);self.assertEqual(result['status'],'provider_http_error')
    def test_error_bodies_and_credential_reflection_never_persist(self):
        c,saved=self.arrange()
        class Body:
            def read(self,*a):raise AssertionError('Provider error bodies must not be read')
            def close(self):pass
        opener=mock.Mock();opener.open.side_effect=urllib.error.HTTPError(native.ENDPOINT,403,'private',{},Body())
        with mock.patch.object(collect.urllib.request,'build_opener',return_value=opener):result=c.snapshot('VOO','2026-09-21')
        self.assertEqual(result['status'],'provider_http_error');self.assertFalse(saved)
        opener.open.side_effect=None;opener.open.return_value=io.BytesIO(b'{"echo":"fixture-only-secret"}')
        with mock.patch.object(collect.urllib.request,'build_opener',return_value=opener):result=c.snapshot('VOO','2026-09-21')
        self.assertEqual(result['status'],'response_rejected');self.assertFalse(saved)
    def test_original_rejected_shape_stays_retained_for_diagnosis(self):
        c,saved=self.arrange();raw=b'{"status":"OK","results":[],"count":1}'
        opener=mock.Mock();opener.open.return_value=io.BytesIO(raw)
        with mock.patch.object(collect.urllib.request,'build_opener',return_value=opener):result=c.snapshot('VOO','2026-09-21')
        self.assertEqual(result['status'],'response_rejected')
        self.assertEqual(saved[result['rejected_original']['key']],raw)
    def test_storage_failure_is_not_treated_as_provider_unavailability(self):
        c,_=self.arrange();c.save=mock.Mock(side_effect=PermissionError('fixture denied'))
        opener=mock.Mock();opener.open.return_value=io.BytesIO(native.encoded({'status':'OK','results':[row()]}))
        with mock.patch.object(collect.urllib.request,'build_opener',return_value=opener),self.assertRaises(PermissionError):c.snapshot('VOO','2026-09-21')

if __name__=='__main__':unittest.main(verbosity=2)
