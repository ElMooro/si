from pathlib import Path
from datetime import date
import sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import short_volume_source_index as model

def html(days=('20260924','20260923')):
    months=''.join('<option value="%02d">M</option>'%m for m in range(1,13))
    years='<option value="7">2026</option><option value="8">2025</option>'
    links=''.join('<a href="https://cdn.finra.org/equity/regsho/daily/CNMSshvol'+d+'.txt">Daily</a>\n' for d in days)
    return ('<form method="get" action="'+model.PATH+'"><select name="'+model.MONTH+'">'+months+'</select><select name="'+model.YEAR+'">'+years+'</select></form>'+links).encode()
class Tests(unittest.TestCase):
    def test_literal_links_and_actual_year_option_values_are_preserved(self):
        out=model.parse(html(),date(2026,9,25),'2026-09');plan=model.month_requests(date(2026,9,25),out)
        self.assertEqual(out['listed_files'][0]['observation_date'],'2026-09-23')
        self.assertIn('custom_year%5Byear%5D=7',plan[0]['url']);self.assertEqual(plan[-1]['period'],'2026-06')
        self.assertFalse(out['historical_availability_verified'])
    def test_missing_trading_dates_are_not_invented(self):
        out=model.parse(html(('20260924','20260921')),date(2026,9,25),'2026-09')
        selected=model.selected_files([out],date(2026,9,25),2)
        self.assertEqual([r['observation_date'] for r in selected],['2026-09-21','2026-09-24'])
        with self.assertRaises(ValueError):model.selected_files([out],date(2026,9,25),3)
    def test_duplicates_future_wrong_month_and_unreviewed_revisions_fail(self):
        for raw in (html(('20260924','20260924')),html(('20260926',)),html(('20260831',)),html().replace(b'20260924.txt',b'20260924-updated.txt')):
            with self.assertRaises(ValueError):model.parse(raw,date(2026,9,25),'2026-09')
    def test_cross_year_plan_uses_captured_labels(self):
        out=model.parse(html(('20260102',)),date(2026,1,3),'2026-01')
        plan=model.month_requests(date(2026,1,3),out)
        self.assertEqual(plan[1]['period'],'2025-12');self.assertIn('custom_year%5Byear%5D=8',plan[1]['url'])
    def test_duplicate_and_changed_filter_forms_fail(self):
        for raw in (html().replace(b'method="get"',b'method="post"'),html().replace(b'value="12"',b'value="11"')):
            with self.assertRaises(ValueError):model.parse(raw,date(2026,9,25),'2026-09')
    def test_overlapping_months_cannot_inflate_coverage(self):
        out=model.parse(html(),date(2026,9,25),'2026-09')
        with self.assertRaises(ValueError):model.selected_files([out,out],date(2026,9,25),2)
    def test_new_month_without_a_file_is_explicit_and_not_a_zero_measurement(self):
        out=model.parse(html(()),date(2026,10,1),'2026-10')
        self.assertEqual(out['period'],'2026-10');self.assertEqual(out['listing_status'],'no_files_listed')
        self.assertEqual(out['listed_files'],[])
        with self.assertRaises(ValueError):model.selected_files([out],date(2026,10,1),2)
if __name__=='__main__':unittest.main(verbosity=2)
