"""Whole source cells, exact arithmetic, missingness and strict authority."""
from pathlib import Path
from copy import deepcopy
from datetime import date,timedelta
from decimal import Decimal,localcontext,ROUND_UP
from unittest.mock import patch
import hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/checks','scripts','aws/lambdas/justhodl-term-premium/source')]
import term_premium_candidate as model
import verify_term_premium_arithmetic as independent
NOW='2026-09-26T04:00:00Z';RAW=b'complete synthetic workbook fixture'
SOURCE={'source_url':model.URL,'sha256':hashlib.sha256(RAW).hexdigest(),'bytes':len(RAW),'acquired_at':NOW}

class Cell:
    def __init__(self,value):self.value=value;self.ctype=0 if value is None else 1 if isinstance(value,str) else 2
class Sheet:
    def __init__(self,name,end,count):
        self.name=name;self.rows=[model.HEADERS.copy()]
        for i in range(count):
            day=end-timedelta(days=2*(count-i-1));tp=[(i-count)/64 for _ in range(10)];rn=[4+tenor/32 for tenor in range(1,11)]
            self.rows.append([day.strftime('%d-%b-%Y')]+[a+b for a,b in zip(tp,rn)]+tp+rn)
        self.nrows=len(self.rows);self.ncols=31
    def row_values(self,index):return deepcopy(self.rows[index])
    def cell(self,r,c):return Cell(self.rows[r][c])
class Book:
    datemode=0;nsheets=2
    def __init__(self):self.sheets={'ACM Daily':Sheet('ACM Daily',date(2026,9,24),300),'ACM Monthly':Sheet('ACM Monthly',date(2026,8,31),20)}
    def sheet_names(self):return list(self.sheets)
    def sheet_by_name(self,name):return self.sheets[name]

def build(book,stamp=NOW,source=SOURCE):
    with patch.object(model.xlrd,'open_workbook',return_value=book):
        output=model.build(RAW,source,stamp);proof=independent.verify(output,RAW)
    return output,proof

class Tests(unittest.TestCase):
    def test_every_returned_cell_and_both_frequencies_are_conserved_without_rounding(self):
        book=Book();before=deepcopy(book.sheets['ACM Daily'].rows);out,proof=build(book)
        self.assertEqual(proof['requested_series'],60);self.assertEqual(proof['current_series'],60)
        self.assertEqual(proof['original_rows'],320);self.assertEqual(proof['model_identity_checks'],3200)
        self.assertEqual(proof['original_cells_including_headers'],322*31);self.assertEqual(proof['observation_comparisons'],240)
        self.assertEqual(book.sheets['ACM Daily'].rows,before)
        self.assertEqual(out,json.loads(model.encoded(out)))
    def test_missing_latest_is_retained_and_cannot_backfill_current(self):
        book=Book();book.sheets['ACM Daily'].rows[-1][11]=None;out,proof=build(book)
        self.assertIsNone(out['series']['D:ACMTP01']['current']);self.assertIsNone(out['series']['D:ACMTP01']['current_comparisons'])
        self.assertIsNone(out['tables']['ACM Daily']['rows'][-1]['cells'][11]);self.assertEqual(proof['current_series'],59)
        self.assertEqual(proof['unavailable_model_identities'],1)
    def test_missing_dates_change_observation_comparisons_and_disclose_calendar_distance(self):
        book=Book();book.sheets['ACM Daily'].rows[-4][11]='.';out,_=build(book)
        comparison=out['series']['D:ACMTP01']['current_comparisons']['5']
        self.assertEqual(comparison['elapsed_calendar_days'],12);self.assertEqual(comparison['missing_observation_dates'],1)
    def test_zero_negative_and_small_ieee_values_remain_exact(self):
        book=Book();row=book.sheets['ACM Daily'].rows[-1];row[11]=-0.0;row[1]=row[21]
        row[12]=float.fromhex('0x0.0000000000001p-1022');row[2]=row[22]
        out,_=build(book);self.assertEqual(out['series']['D:ACMTP01']['current']['value'],0)
        self.assertEqual(out['series']['D:ACMTP02']['current']['value'],row[12]);self.assertEqual(Decimal(out['series']['D:ACMTP02']['current']['exact_decimal']),Decimal.from_float(row[12]))
    def test_old_acquisition_and_observation_withhold_current_but_keep_all_cells(self):
        book=Book();fresh,_=build(book);old,_=build(book,'2026-09-28T04:00:00Z')
        self.assertEqual(old['tables'],fresh['tables']);self.assertEqual(old['quality']['current_series'],0)
        self.assertEqual(old['series']['D:ACMY10']['quality']['status'],'stale_acquisition')
        older,_=build(book,'2026-12-28T04:00:00Z');self.assertEqual(older['series']['M:ACMY10']['quality']['status'],'stale_observation')
    def test_future_rows_remain_in_history_but_do_not_enter_current_values(self):
        book=Book();book.sheets['ACM Daily'].rows[-1][0]='30-Sep-2026';out,_=build(book)
        self.assertEqual(out['tables']['ACM Daily']['rows'][-1]['observation_date'],'2026-09-30')
        self.assertEqual(out['series']['D:ACMY10']['current']['observation_date'],'2026-09-22')
        book.sheets['ACM Daily'].rows[-1][0]='27-Sep-2026';later,_=build(book,'2026-09-27T04:00:00Z')
        self.assertEqual(later['series']['D:ACMY10']['current']['observation_date'],'2026-09-22')
    def test_definition_drift_duplicate_dates_bad_numbers_and_broken_identity_fail(self):
        for change in (lambda b:b.sheets['ACM Daily'].rows[0].__setitem__(1,'PAR_YIELD'),
                       lambda b:b.sheets['ACM Daily'].rows[-1].__setitem__(0,b.sheets['ACM Daily'].rows[-2][0]),
                       lambda b:b.sheets['ACM Daily'].rows[-1].__setitem__(1,float('nan')),
                       lambda b:b.sheets['ACM Daily'].rows[-1].__setitem__(1,999)):
            book=Book();change(book)
            with self.assertRaises(ValueError):build(book)
    def test_all_original_cells_and_coordinates_are_independently_checked(self):
        book=Book();out,_=build(book)
        for change in (lambda p:p['tables']['ACM Daily']['rows'][0]['cells'].__setitem__(12,999),
                       lambda p:p['series']['D:ACMY10']['current'].update(original_row=1),
                       lambda p:p.update(sizing_eligible=True)):
            bad=deepcopy(out);change(bad)
            with patch.object(model.xlrd,'open_workbook',return_value=book),self.assertRaises(AssertionError):independent.verify(bad,RAW)
    def test_fixed_precision_and_source_identity(self):
        book=Book();out,_=build(book)
        with localcontext() as ctx:
            ctx.prec=6;ctx.rounding=ROUND_UP
            other,_=build(book)
        self.assertEqual(out,other)
        for source in ({**SOURCE,'sha256':'0'*64},{**SOURCE,'acquired_at':'2026-09-27T00:00:00Z'},{**SOURCE,'source_url':'https://example.com/file.xls'}):
            with self.assertRaises(ValueError):build(book,source=source)

    def test_metadata_cannot_grant_independence_or_extend_freshness(self):
        book=Book();out,_=build(book)
        for change in (lambda p:p['source'].update(source_url='https://example.com/workbook'),
                       lambda p:p['source'].update(acquired_at='2026-09-26T04:00:00'),
                       lambda p:p['dependency_graph'].update(independent_votes=60),
                       lambda p:p['series']['D:ACMY10']['quality'].update(max_acquisition_age_seconds=999999),
                       lambda p:p['quality'].update(status='forecast_qualified')):
            bad=deepcopy(out);change(bad)
            with patch.object(model.xlrd,'open_workbook',return_value=book),self.assertRaises(AssertionError):independent.verify(bad,RAW)

if __name__=='__main__':unittest.main(verbosity=2)
