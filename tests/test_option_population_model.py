from pathlib import Path
from unittest.mock import patch
import copy,sys,unittest
R=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(R/'aws/shared'),str(R/'tests')]
import option_population_model as model
from test_option_contract_research import row,compute
import test_option_flow_store as fixtures


class Tests(unittest.TestCase):
    def rows(self):
        a=row('call');a['open_interest']=3
        b=row('put');b['open_interest']=5
        return compute([a,b])['rows']
    def test_sign_is_contract_delta_not_invented_dealer_gamma(self):
        out=model.aggregate(self.rows())['sides']
        self.assertEqual(out['call']['gamma_oi_shares']['value'],'3.00')
        self.assertEqual(out['put']['gamma_oi_shares']['value'],'5.00')
        self.assertEqual(out['call']['delta_oi_shares']['value'],'150.0')
        self.assertEqual(out['put']['delta_oi_shares']['value'],'-250.0')
    def test_missing_gamma_not_zero_and_does_not_hide_valid_delta(self):
        rows=self.rows();rows[0]['metrics']['vendor_gamma'].update(value=None,state='missing')
        out=model.aggregate(rows)['sides']['call']
        self.assertIsNone(out['gamma_oi_shares']['value']);self.assertEqual(out['gamma_oi_shares']['excluded_rows'],1)
        self.assertEqual(out['delta_oi_shares']['value'],'150.0')
    def test_true_zero_gamma_does_not_trigger_repricing_fallback(self):
        rows=self.rows();rows[0]['metrics']['vendor_gamma'].update(value='0',state='reported_zero')
        out=model.aggregate(rows)['sides']['call']['gamma_oi_shares']
        self.assertEqual(out['value'],'0');self.assertTrue(out['complete_field_coverage'])
    def test_invalid_deliverable_or_identity_excluded(self):
        rows=self.rows();rows[0]['metrics']['shares_per_contract']['value']='10'
        with self.assertRaises(ValueError):model.aggregate(rows)
        rows=self.rows();rows[0]['identity_eligible']=False;rows[1]['identity_eligible']=False
        out=model.aggregate(rows)
        self.assertEqual(out['counts']['identity_excluded_rows'],2)
        self.assertIsNone(out['sides']['call']['gamma_oi_shares']['value']);self.assertIsNone(out['sides']['put']['gamma_oi_shares']['value'])
    def test_upstream_domain_or_authority_override_is_rejected(self):
        rows=self.rows();rows[0]['metrics']['vendor_gamma']['value']='-1'
        with self.assertRaises(ValueError):model.aggregate(rows)
        rows=self.rows();rows[0]['calls_eligible']=True
        with self.assertRaises(ValueError):model.aggregate(rows)
    def test_retained_record_graph_and_no_fixed_gamma_flip(self):
        t=fixtures.StoreTests();t.setUp()
        try:s3,inputs,out,ref=t.candidate()
        finally:t.tearDown()
        packet={**out,'replay':ref};raw=model.upstream.encoded(packet);read=model.evidence.reader(s3,'synthetic')
        verified=model.verified_packet(raw,read);result=model.chain(verified,'SPY',read)
        self.assertEqual(result['totals']['counts']['returned_rows'],2);self.assertIsNone(result['zero_gamma_flip'])
        self.assertEqual(result['source_run'],ref);self.assertFalse(result['calls_eligible'])
        packet['generated_at']='2099-01-01T00:00:00Z'
        with self.assertRaises(ValueError):model.verified_packet(model.upstream.encoded(packet),read)

    def test_units_and_zero_state_are_checked(self):
        rows=self.rows();rows[0]['metrics']['vendor_gamma']['unit']='percent'
        with self.assertRaises(ValueError):model.aggregate(rows)
        rows=self.rows();rows[0]['metrics']['vendor_gamma']['value']='0'
        with self.assertRaises(ValueError):model.aggregate(rows)
    def test_missing_oi_does_not_become_zero_gamma(self):
        rows=self.rows();rows[0]['metrics']['open_interest'].update(value=None,state='null')
        out=model.aggregate(rows)['sides']['call']
        for field in model.FIELDS:
            self.assertIsNone(out[field]['value']);self.assertEqual(out[field]['exclusion_reasons'],{'open_interest:null':1})
    def test_zero_oi_is_valid_but_unknown_gamma_is_not_imputed(self):
        rows=self.rows();rows[0]['metrics']['open_interest'].update(value='0',state='reported_zero')
        out=model.aggregate(rows)['sides']['call']
        self.assertEqual(out['gamma_oi_shares']['value'],'0');self.assertTrue(out['gamma_oi_shares']['complete_field_coverage'])
        rows[0]['metrics']['vendor_gamma'].update(value=None,state='missing')
        out=model.aggregate(rows)['sides']['call']
        self.assertEqual(out['reported_open_interest']['value'],'0');self.assertIsNone(out['gamma_oi_shares']['value'])
    def test_high_precision_products_are_exact_outside_caller_context(self):
        from decimal import Decimal,localcontext
        rows=self.rows();value='0.'+'1234567890'*10
        rows[0]['metrics']['vendor_gamma']['value']=value
        with localcontext() as ctx:
            ctx.prec=180;expected=Decimal(value)*300
        self.assertEqual(model.row_values(rows[0])['gamma_oi_shares'],expected)
        self.assertEqual(Decimal(model.aggregate(rows)['sides']['call']['gamma_oi_shares']['value']),expected)
    def test_negative_or_fractional_oi_never_qualifies(self):
        for bad in ('-2','1.5'):
            rows=self.rows();rows[0]['metrics']['open_interest']['value']=bad
            with self.assertRaises(ValueError):model.aggregate(rows)
    def test_source_bounds_cannot_be_bypassed(self):
        for bad in ('9'*200,'0.'+'0'*128+'1','1e2','nan'):
            rows=self.rows();rows[0]['metrics']['vendor_gamma']['value']=bad
            with self.assertRaises(ValueError):model.aggregate(rows)
    def test_empty_population_is_unknown(self):
        out=model.aggregate([])
        for kind in ('call','put'):
            for field in model.FIELDS:
                self.assertIsNone(out['sides'][kind][field]['value']);self.assertFalse(out['sides'][kind][field]['complete_field_coverage'])

    def output(self):
        t=fixtures.StoreTests();t.setUp()
        try:s3,inputs,out,ref=t.candidate()
        finally:t.tearDown()
        return model.chain({**out,'replay':ref},'SPY',model.evidence.reader(s3,'synthetic'))
    def test_delivery_is_bounded_and_exact(self):
        output=self.output();group=output['by_expiry_strike'][0]
        output['by_expiry_strike']=[{**copy.deepcopy(group),'strike_usd_per_share':str(n+1)} for n in range(201)]
        artifacts={};ref=model.deliver(output,artifacts.__setitem__)
        self.assertEqual(model.restore(ref,artifacts.__getitem__),output)
        summary=model.checked(ref,'chains',artifacts.__getitem__)
        self.assertEqual([r['groups'] for r in summary['group_blocks']],[100,100,1])
        self.assertEqual(len(artifacts),4)
    def test_delivery_tampering_cannot_hide_behind_recomputed_block_hash(self):
        output=self.output();artifacts={};ref=model.deliver(output,artifacts.__setitem__)
        summary=model.checked(ref,'chains',artifacts.__getitem__);item=summary['group_blocks'][0]
        block=model.checked(item['artifact'],'groups',artifacts.__getitem__)
        block['groups'][0]['sides']['call']['gamma_oi_shares']['value']='123456'
        raw=model.upstream.encoded(block);item['artifact']=model.reference(raw,'groups');artifacts[item['artifact']['key']]=raw
        raw=model.upstream.encoded(summary);ref=model.reference(raw,'chains');artifacts[ref['key']]=raw
        with self.assertRaisesRegex(ValueError,'Expanded'):model.restore(ref,artifacts.__getitem__)
    def test_unreviewed_artifact_paths_are_never_read(self):
        reads=[]
        with self.assertRaises(ValueError):model.checked({'key':'data/trade-tickets.json'},'groups',lambda k:reads.append(k))
        self.assertEqual(reads,[])
    def test_group_page_gaps_and_authority_overrides_rejected(self):
        output=self.output();artifacts={};ref=model.deliver(output,artifacts.__setitem__)
        summary=model.checked(ref,'chains',artifacts.__getitem__);summary['group_blocks'][0]['offset']=100
        raw=model.upstream.encoded(summary);ref=model.reference(raw,'chains');artifacts[ref['key']]=raw
        with self.assertRaisesRegex(ValueError,'inventory'):model.restore(ref,artifacts.__getitem__)
        output['calls_eligible']=True
        with self.assertRaises(ValueError):model.deliver(output,artifacts.__setitem__)


if __name__=='__main__':unittest.main(verbosity=2)
