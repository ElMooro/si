import unittest
from datetime import datetime,timezone
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from donor_contract import inspect_donor,numeric,get_path

class ContractTests(unittest.TestCase):
    def test_zero_is_data_and_dates_are_independent(self):
        now=datetime(2026,9,9,tzinfo=timezone.utc)
        doc={'generated_at':'2026-09-08T12:00:00Z','as_of':'2026-09-04','score':0}
        good=inspect_donor(doc,'fixture',24,('as_of',),('score',),now=now,max_observation_age_hours=144)
        self.assertTrue(good['usable'])
        self.assertFalse(inspect_donor(doc,'fixture',24,('as_of',),('score',),now=now,max_observation_age_hours=24)['usable'])
        self.assertEqual(numeric(doc,'score'),0)
        self.assertIsNone(numeric(True))
    def test_invalid_future_and_absent_are_unusable(self):
        now=datetime(2026,9,9,tzinfo=timezone.utc)
        for doc in ({},{'generated_at':'garbage'},{'generated_at':'2099-01-01T00:00:00Z'}, {'generated_at':'2026-09-08T12:00:00Z','x':float('nan')}):
            self.assertFalse(inspect_donor(doc,'fixture',24,required_paths=('x',),now=now)['usable'])
    def test_paths_keep_rows_and_units(self):
        doc={'book':[{'symbol':'A','weight':0},{'symbol':'B','weight':2}]}
        self.assertEqual(get_path(doc,'book.*.symbol'),['A','B'])
        self.assertEqual(get_path(doc,'book.0.weight'),0)

if __name__=='__main__': unittest.main()
