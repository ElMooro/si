"""Domain/evidence regressions for descriptive Treasury research."""
from copy import deepcopy
from datetime import datetime,timezone
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import tenor_research_model as model

TODAY='2026-09-18'

def row(cusip='912345678',day='2026-09-17',term='2-Year',kind='Note',quote='4.0',reopening='No'):
    return {'cusip':cusip,'auction_date':day,'security_term':term,'original_security_term':term,
            'security_type':kind,'inflation_index_security':'No','floating_rate':'No','reopening':reopening,'cash_management_bill_cmb':'No',
            'total_accepted':'100','primary_dealer_accepted':'10','direct_bidder_accepted':'20',
            'indirect_bidder_accepted':'70','bid_to_cover_ratio':'3.0','high_yield':quote,
            'high_investment_rate':quote}


def fixture():
    rows=[row(),row('912345677','2026-08-17',quote='4.2'),
          row('912345676','2026-09-10','30-Year','Bond','4.5'),
          row('912345675','2026-06-10','30-Year','Bond','4.7')]
    rows.extend(row('91234566'+str(i),d,'4-Week','Bill') for i,d in enumerate(('2026-09-17','2026-09-10','2026-09-03','2026-08-27','2026-08-20')))
    return [{'data':rows}],{'observations':[{'date':'2026-09-17','value':'4.0'},{'date':'2026-09-10','value':'4.1'}]}


class DomainTests(unittest.TestCase):
    def test_exact_instrument_identity_and_no_size_guess(self):
        pages,ff=fixture();tips=row('912345673',quote='1.2');tips['inflation_index_security']='Yes'
        frn=row('912345672',quote='2.0');frn['floating_rate']='Yes'
        unknown=row('912345671');unknown.pop('floating_rate')
        pages[0]['data']+=[tips,frn,unknown]
        out=model.compile_research(pages,ff,TODAY)
        self.assertEqual(out['measurements']['nominal_2y']['latest_auction']['quote_value'],4.0)
        self.assertEqual(out['quality']['excluded_rows']['unverified_instrument_or_cohort'],1)
        self.assertEqual(out['measurements']['nominal_2y']['metrics']['yield_change_bp'],-20)

    def test_reopening_and_remaining_term_cannot_mix(self):
        pages,ff=fixture();pages[0]['data'][1]['reopening']='Yes'
        out=model.compile_research(pages,ff,TODAY)
        self.assertEqual(out['measurements']['nominal_2y']['state'],'UNAVAILABLE')
        pages[0]['data'][1]['reopening']='No';pages[0]['data'][1]['security_term']='1-Year 11-Month'
        self.assertEqual(model.compile_research(pages,ff,TODAY)['measurements']['nominal_2y']['state'],'UNAVAILABLE')

    def test_missing_participation_cannot_be_zero_or_a_partial_denominator(self):
        pages,ff=fixture();pages[0]['data'][4]['direct_bidder_accepted']='null'
        out=model.compile_research(pages,ff,TODAY)
        bill=out['measurements']['bill_participation']['metrics']['tenor_breakdown'][0]
        self.assertIsNone(bill['latest_auction']['indirect_pct']);self.assertIsNone(bill['indirect_drop_pts'])
        self.assertEqual(bill['btc_spike'],0)

    def test_missing_and_stale_rates_do_not_format_as_zero(self):
        pages,_=fixture();out=model.compile_research(pages,None,TODAY)
        self.assertIsNone(out['measurements']['nominal_2y']['metrics']['spread_to_ff_bp'])
        self.assertIsNone(out['fed_funds_rate']);self.assertEqual(out['quality']['status'],'degraded')

    def test_current_dff_cannot_be_compared_to_an_older_auction(self):
        pages,ff=fixture();ff['observations']=[{'date':TODAY,'value':'0'}]
        out=model.compile_research(pages,ff,TODAY)
        self.assertEqual(out['fed_funds_rate'],0)
        self.assertIsNone(out['measurements']['nominal_30y']['metrics']['spread_to_ff_bp'])

    def test_bill_terms_and_four_complete_prior_observations(self):
        pages,ff=fixture();pages[0]['data'][-1]['security_term']='8-Week'
        out=model.compile_research(pages,ff,TODAY)
        four=next(t for t in out['measurements']['bill_participation']['metrics']['tenor_breakdown'] if t['tenor']=='4-Week')
        self.assertEqual(four['state'],'UNAVAILABLE');self.assertIsNone(four['btc_spike'])

    def test_cash_management_bills_cannot_enter_regular_bill_comparisons(self):
        pages,ff=fixture();pages[0]['data'][4]['cash_management_bill_cmb']='Yes'
        out=model.compile_research(pages,ff,TODAY)
        self.assertEqual(out['quality']['excluded_rows']['cash_management_or_unverified_bill'],1)
        self.assertEqual(out['measurements']['bill_participation']['state'],'UNAVAILABLE')

    def test_52_week_auction_cadence_is_not_treated_as_weekly(self):
        pages,ff=fixture()
        pages[0]['data']=[row('91234566'+str(i),d,'52-Week','Bill') for i,d in enumerate(('2026-08-25','2026-07-28','2026-06-30','2026-06-02','2026-05-05'))]
        bill=model.compile_research(pages,ff,TODAY)['measurements']['bill_participation']['metrics']['tenor_breakdown'][0]
        self.assertEqual(bill['state'],'AVAILABLE');self.assertEqual(bill['observation_max_age_days'],45)

    def test_original_source_row_pointers_reproduce_quotes_and_zero_awards(self):
        pages,ff=fixture();pages[0]['data'][0]['indirect_bidder_accepted']='0'
        out=model.compile_research(pages,ff,TODAY);latest=out['measurements']['nominal_2y']['latest_auction']
        ref=latest['source_ref'];raw=pages[ref['page']]['data'][ref['row']]
        self.assertEqual(float(raw[latest['quote_field']]),latest['quote_value']);self.assertEqual(latest['indirect_pct'],0)

    def test_duplicate_source_identity_aborts_instead_of_increasing_sample(self):
        pages,ff=fixture();pages.append(deepcopy(pages[0]))
        with self.assertRaisesRegex(ValueError,'duplicate auction'):model.compile_research(pages,ff,TODAY)

    def test_old_observations_are_stale_and_nonfinite_missing(self):
        pages,ff=fixture();pages[0]['data'][0]['auction_date']='2026-06-10';pages[0]['data'][1]['auction_date']='2026-05-10'
        self.assertEqual(model.compile_research(pages,ff,TODAY)['measurements']['nominal_2y']['state'],'STALE')
        self.assertIsNone(model.number('nan'));self.assertIsNone(model.number(True))

    def test_legacy_aliases_never_gain_signal_or_position_authority(self):
        pages,ff=fixture();out=model.compile_research(pages,ff,TODAY)
        self.assertEqual(out['quality']['status'],'fresh')
        self.assertIsNone(out['any_firing']);self.assertIsNone(out['composite_score']);self.assertEqual(out['transitions'],[])
        for sig in out['signals'].values():
            self.assertFalse(sig['sizing_eligible']);self.assertFalse(sig['alert_eligible']);self.assertIsNone(sig['direction'])

    def test_summary_abstains_on_old_and_stale_schema(self):
        now=datetime(2026,9,18,tzinfo=timezone.utc)
        self.assertEqual(model.public_summary({'any_firing':True},now)['status'],'UNAVAILABLE')
        pages,ff=fixture();out=model.compile_research(pages,ff,TODAY);out['generated_at']='2026-01-01T00:00:00Z'
        self.assertEqual(model.public_summary(out,now)['status'],'STALE')
        out['generated_at']=now.isoformat();self.assertEqual(model.public_summary(out,now)['status'],'RESEARCH_ONLY')


if __name__=='__main__':unittest.main()
