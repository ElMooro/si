from pathlib import Path
import json, sys, unittest
from unittest.mock import patch, Mock
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/checks')]
import share_structure_access_audit as audit


def outcome(key, denied=True):
    return {'key':key,'denied':denied,'method':'HEAD','origins':[
        {'url':url,'denied':denied,'attempts':[{'http_status':404 if denied else 503,'error_type':None}]}
        for url in audit.access.urls(key)]}


class Tests(unittest.TestCase):
    def test_entire_population_is_validated_before_requests_and_only_part6_is_allowed(self):
        with patch.object(audit.access,'check') as check:
            for part,keys in ((5,['audit-private/20260909-originals/x']), (6,[]), (6,['audit-private/20260909-originals/x','data/public.json'])):
                with self.assertRaises(ValueError):audit.verify(None,keys,part,Mock())
            check.assert_not_called()

    def test_all_outcomes_and_unknown_failure_are_retained_before_rejection(self):
        prefix='audit-private/20260909-originals/share-structure-research/'
        ref={'key':prefix+'a'*64+'.bin','sha256':'a'*64,'bytes':1}
        report=Mock(); captured=[]
        def retain(client,raw):captured.append(json.loads(raw));return ref
        with patch.object(audit.source,'retain',side_effect=retain),patch.object(audit.access,'check',side_effect=lambda key:outcome(key,key!=prefix+'source.bin')):
            with self.assertRaisesRegex(ValueError,'unknown responses'):audit.verify(None,[prefix+'source.bin'],6,report)
        self.assertEqual(captured[0]['outcomes'][0]['origins'][0]['attempts'][0]['http_status'],503)
        self.assertFalse(report.kv.call_args.kwargs['anonymous_access']['all_denied'])
        self.assertEqual(report.kv.call_args.kwargs['access_evidence'],ref)

    def test_success_checks_both_origins_and_retained_proof(self):
        prefix='audit-private/20260909-originals/share-structure-research/'
        ref={'key':prefix+'a'*64+'.bin','sha256':'a'*64,'bytes':1}
        with patch.object(audit.source,'retain',return_value=ref),patch.object(audit.access,'check',side_effect=outcome) as check:
            result=audit.verify(None,[prefix+'source.bin'],6,Mock())
        self.assertTrue(result['anonymous_access']['all_denied'])
        self.assertEqual(result['anonymous_access']['protected_paths_checked'],2)
        self.assertEqual(result['anonymous_access']['anonymous_origins_checked'],4)
        self.assertEqual(check.call_count,2)


if __name__=='__main__':unittest.main(verbosity=2)
