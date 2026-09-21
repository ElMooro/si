"""Exercise the actual canonical-source dispatch and definition boundary offline."""
from pathlib import Path
from unittest.mock import Mock,patch
from datetime import datetime,timezone,timedelta
import copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import ops_6021_dollar_canonical_sources as op
from test_futures_research_store import Memory


class Tests(unittest.TestCase):
    def packet(self,expanded=True):
        return {'generated_at':datetime.now(timezone.utc).isoformat(),'catalog':dict.fromkeys(op.EXPECTED if expanded else ('DTWEXBGS',),{}),
            'measurements':{sid:{'unit':unit,'frequency':freq,'current_decimal':'1.1'} for sid,(unit,freq) in op.EXPECTED.items()} if expanded else {},
            'replay':{'manifest_key':'recorded'},'calls_eligible':False,'sizing_eligible':False}
    def arrange(self,expanded=False,ambiguous=False):
        s=Memory();s.data[op.CURRENT]=op.base.encoded(self.packet(expanded));lam=Mock()
        def invoke(**kwargs):
            self.assertEqual(kwargs['FunctionName'],'justhodl-daily-report-v3')
            self.assertEqual(kwargs['InvocationType'],'Event')
            self.assertEqual(json.loads(kwargs['Payload']),{'action':'research_measurements'})
            if ambiguous:raise ConnectionError('Ambiguous acceptance')
            s.data[op.CURRENT]=op.base.encoded(self.packet());return {'StatusCode':202}
        lam.invoke.side_effect=invoke
        return s,lam
    def test_existing_qualified_publication_needs_no_invocation(self):
        s,lam=self.arrange(expanded=True)
        with patch.object(op.canonical_fred_replay,'pinned_report',side_effect=lambda p,r:p):
            (_,p),result=op.refresh(lam,s,'a'*40,lambda key:b'')
        lam.invoke.assert_not_called();self.assertFalse(result['invoke_sent']);self.assertEqual(set(p['catalog']),set(op.EXPECTED))
    def test_one_explicit_public_route_then_reuse_without_second_invocation(self):
        s,lam=self.arrange()
        # A real asynchronous collection completes after the dispatch claim;
        # make that separation explicit on low-resolution Windows clocks.
        claimed=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
        with patch.object(op.base,'now',return_value=claimed),patch.object(op.canonical_fred_replay,'pinned_report',side_effect=lambda p,r:p):
            (_,p),result=op.refresh(lam,s,'a'*40,lambda key:b'')
            (_,again),second=op.refresh(lam,s,'a'*40,lambda key:b'')
        self.assertEqual(lam.invoke.call_count,1);self.assertTrue(result['invoke_sent']);self.assertFalse(second['invoke_sent'])
        self.assertEqual(p,again);self.assertEqual(json.loads(s.data[result['status_key']])['status'],'publication_observed')
    def test_ambiguous_dispatch_preserves_claim_and_cannot_be_reissued(self):
        s,lam=self.arrange(ambiguous=True)
        with patch.object(op.canonical_fred_replay,'pinned_report',side_effect=lambda p,r:p):
            with self.assertRaises(ConnectionError):op.refresh(lam,s,'a'*40,lambda key:b'')
            ticks=iter((0,901))
            with self.assertRaisesRegex(AssertionError,'claimed request'):op.refresh(lam,s,'a'*40,lambda key:b'',monotonic=lambda:next(ticks),sleep=lambda n:None)
        self.assertEqual(lam.invoke.call_count,1)
        claims=[json.loads(v) for k,v in s.data.items() if '/requests/' in k]
        self.assertEqual(len(claims),1);self.assertEqual(claims[0]['status'],'claimed')
    def test_expanded_catalog_alone_is_not_source_qualification(self):
        p=self.packet();p['measurements'].pop('DEXTAUS')
        with patch.object(op.canonical_fred_replay,'pinned_report',side_effect=lambda p,r:p):
            with self.assertRaisesRegex(AssertionError,'DEXTAUS'):op.ready(p,lambda k:b'')
    def test_wrong_quote_direction_monthly_frequency_and_authority_refused(self):
        for sid,field,value in (('DEXUSAL','unit','Australian Dollars to One U.S. Dollar'),('IRLTLT01DEM156N','frequency','D')):
            p=self.packet();p['measurements'][sid][field]=value
            with patch.object(op.canonical_fred_replay,'pinned_report',side_effect=lambda p,r:p):
                with self.assertRaises(AssertionError):op.ready(p,lambda k:b'')
        p=self.packet();p['sizing_eligible']=True
        with patch.object(op.canonical_fred_replay,'pinned_report',side_effect=lambda p,r:p):
            with self.assertRaises(AssertionError):op.ready(p,lambda k:b'')

if __name__=='__main__':unittest.main(verbosity=2)
