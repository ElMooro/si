from pathlib import Path
from datetime import date,timedelta
from decimal import Decimal
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/ops/checks'))
import ici_research_candidate as model
import verify_ici_research as independent

ACQUIRED='2026-09-26T18:52:00Z';GENERATED='2026-09-26T18:52:10Z'
def release(kind):
    spec=model.SOURCES[kind];mmf=kind=='mmf';end=date(2026,9,23 if mmf else 16)
    dates=[(end-timedelta(days=7*i)).strftime('%m/%d/%Y') for i in range(3 if mmf else 5)]
    header=['',dates[0],dates[1],'$ Change*',dates[2]] if mmf else ['',*dates]
    values=[100,60,40,50,30,20,25,10,15,175,100,75] if mmf else [-30,-20,-10,5,12,8,4,3,-10]
    cells=[]
    for (label,_),v in zip(spec['labels'],values):
        vals=[Decimal(v),Decimal(v)*Decimal('.9'),Decimal(v)*Decimal('.1'),Decimal(v)*Decimal('.8')] if mmf else [Decimal(v)*i for i in range(1,6)]
        cells.append('<tr><td>'+label+'</td>'+''.join('<td>'+format(x,'.2f' if mmf else '.0f')+'</td>' for x in vals)+'</tr>')
    return ('<html><head><script>noise</script></head><body><p>Washington, DC; September '+('24' if mmf else '23')+', 2026</p>'
        '<h2>'+spec['unit_text']+'</h2><table><thead><tr>'+''.join('<th>'+x+'</th>' for x in header)+'</tr></thead><tbody>'+''.join(cells)+'</tbody></table></body></html>').encode()

def inputs():return {kind:{'raw':release(kind),'acquired_at':ACQUIRED} for kind in model.SOURCES}

class Tests(unittest.TestCase):
    def test_full_table_population_units_dates_hierarchy_and_arithmetic(self):
        out=model.compile_releases(inputs(),GENERATED)
        self.assertEqual(len(out['sources']['mmf']['observations']),36)
        self.assertEqual(len(out['sources']['mmf']['reported_changes']),12)
        self.assertEqual(len(out['sources']['combined_flows']['observations']),45)
        self.assertEqual(out['mmf']['total_b'],175);self.assertEqual(out['mmf']['wow_b'],17.5)
        self.assertEqual(out['mmf']['retail_b'],100);self.assertEqual(out['mmf']['govt_b'],100)
        self.assertEqual(out['long_term']['classes']['eq_dom']['sum_4w_m'],-200)
        self.assertEqual(out['long_term']['equity_sum_4w_m'],-300)
        self.assertEqual(out['long_term']['classes']['bond']['sum_4w_m'],120)
        self.assertEqual(out['long_term']['classes']['muni']['sum_4w_m'],40)
        self.assertEqual(len(out['reconciliation']['mmf']),33);self.assertEqual(len(out['reconciliation']['combined_flows']),15)
        self.assertTrue(all(r['status']=='within_reported_rounding' for checks in out['reconciliation'].values() for r in checks))
        self.assertEqual(out['sources']['mmf']['release_date'],'2026-09-24')
        self.assertEqual(out['sources']['mmf']['unit'],'usd_bn');self.assertEqual(out['sources']['combined_flows']['unit'],'usd_mn')
        for key in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'):self.assertIs(out[key],False)
        self.assertIsNone(out['mmf']['yoy_pct']);self.assertIsNone(out['mmf']['chg_13w_b']);self.assertIsNone(out['signal'])

    def test_lexemes_coordinates_zero_missing_and_negative_flows_survive(self):
        self.assertEqual(model.number('0'),0);self.assertEqual(model.number('(1,234.50)'),Decimal('-1234.50'))
        self.assertEqual(model.number('−12'),-12);self.assertIsNone(model.number('—'))
        parsed=model.parse_release(release('mmf').replace(b'<td>100.00</td>','<td>—</td>'.encode(),1),'mmf',ACQUIRED)
        row=parsed['observations'][0];self.assertIsNone(row['value']);self.assertEqual(row['source_cell']['row'],1)
        self.assertTrue(any(r['status']=='missing_input' for r in model.reconcile(parsed,'mmf')))
        for v in ('false','NaN','Infinity','1,23','1e6','12%'):
            with self.assertRaises(ValueError):model.number(v)

    def test_changed_units_layout_and_duplicate_source_tables_are_withheld(self):
        raw=release('mmf')
        for invalid in (raw.replace(b'Billions',b'Millions'),raw.replace(b'Government',b'Unknown'),raw.replace(b'<td>100.00</td>',b'',1),
                        raw.replace(b'09/16/2026',b'09/23/2026'),raw.replace(b'<td>',b'<td colspan="2">',1),raw+raw,
                        raw.replace(b'</table>',b'')):
            with self.assertRaises(ValueError):model.parse_release(invalid,'mmf',ACQUIRED)

    def test_current_vintage_has_no_fabricated_historical_comparisons(self):
        src=inputs();src['combined_flows']['raw']=src['combined_flows']['raw'].replace(b'08/26/2026',b'08/12/2026')
        out=model.compile_releases(src,GENERATED)
        self.assertIsNone(out['long_term']['equity_sum_4w_m'])
        self.assertIsNone(out['long_term']['classes']['bond']['sum_4w_m'])
        self.assertEqual(len(out['sources']['combined_flows']['observations']),45)
        self.assertIsNone(out['mmf']['z_13w']);self.assertIsNone(out['long_term']['equity_z_4w'])

    def test_timing_missing_sources_and_nonconsecutive_reported_change_are_rejected(self):
        for when in ('2026-09-26T18:51:59Z','2026-09-26T19:52:00Z','2026-09-26T18:52:10'):
            with self.assertRaises(ValueError):model.compile_releases(inputs(),when)
        with self.assertRaises(ValueError):model.compile_releases({'mmf':inputs()['mmf']},GENERATED)
        with self.assertRaises(ValueError):model.parse_release(release('mmf'),'mmf','2026-09-23T18:00:00Z')
        p=model.parse_release(release('mmf').replace(b'09/16/2026',b'09/15/2026'),'mmf',ACQUIRED)
        with self.assertRaises(ValueError):model.reconcile(p,'mmf')

    def test_rounding_is_bounded_and_errors_are_visible(self):
        src=inputs();src['mmf']['raw']=src['mmf']['raw'].replace(b'<td>100.00</td>',b'<td>100.01</td>',1)
        out=model.compile_releases(src,GENERATED);self.assertEqual(out['quality']['reconciliation_issues'],0)
        src['mmf']['raw']=src['mmf']['raw'].replace(b'<td>100.01</td>',b'<td>120.00</td>',1)
        out=model.compile_releases(src,GENERATED);self.assertGreater(out['quality']['reconciliation_issues'],0)
        self.assertEqual(out['quality']['status'],'measurement_reconciliation_failed');self.assertEqual(out['decision']['verb'],'WAIT')

    def test_independent_cell_and_rational_checker_rejects_changed_measurements(self):
        src=inputs();raw={k:v['raw'] for k,v in src.items()};out=model.compile_releases(src,GENERATED)
        proof=independent.verify(raw,out)
        self.assertEqual(proof['observation_checks'],81);self.assertEqual(proof['reported_change_checks'],12)
        self.assertEqual(proof['independent_rational_reconciliations'],48)
        for field,value in [('total_b',999),('total_b',True),('wow_b',0),('yoy_pct',1)]:
            bad=copy.deepcopy(out);bad['mmf'][field]=value
            with self.assertRaises(ValueError):independent.verify(raw,bad)
        bad=copy.deepcopy(out);bad['sources']['mmf']['observations'].pop()
        with self.assertRaises(ValueError):independent.verify(raw,bad)
        bad=copy.deepcopy(out);bad['sources']['mmf']['observations'][0]['date']='2026-01-01'
        with self.assertRaises(ValueError):independent.verify(raw,bad)
        bad=copy.deepcopy(out);bad['reconciliation']['mmf'][0]['residual_decimal']='1'
        with self.assertRaises(ValueError):independent.verify(raw,bad)
        for field in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'):
            bad=copy.deepcopy(out);bad[field]=True
            with self.assertRaises(ValueError):independent.verify(raw,bad)
        bad=copy.deepcopy(out);bad['sources']['mmf']['unit']='usd_mn'
        with self.assertRaises(ValueError):independent.verify(raw,bad)
        bad=copy.deepcopy(out);bad['mmf']['history'].pop()
        with self.assertRaises(ValueError):independent.verify(raw,bad)

if __name__=='__main__':unittest.main()
