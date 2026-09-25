from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6044_offexchange_available_partitions as op
class Tests(unittest.TestCase):
    def doc(self):return {'partitionFields':['weekStartDate','tierIdentifier'],'availablePartitions':[{'partitions':['2026-08-31','T1']},{'partitions':['2026-08-17','T2']}]}
    def test_source_advertised_tiers_have_separate_latest_dates(self):
        out=op.inspect(op.model.encoded(self.doc()),'weeklySummary');self.assertEqual(out['by_tier']['T1']['latest_ten'],['2026-08-31']);self.assertEqual(out['by_tier']['T2']['latest_ten'],['2026-08-17']);self.assertFalse(out['category_coverage_verified'])
    def test_malformed_dates_or_changed_dimensions_are_not_silently_interpreted(self):
        doc=self.doc();doc['availablePartitions'][0]['partitions'][0]='bad-date'
        with self.assertRaises(ValueError):op.inspect(op.model.encoded(doc),'weeklySummary')
        doc=self.doc();doc['partitionFields'].reverse()
        with self.assertRaises(ValueError):op.inspect(op.model.encoded(doc),'weeklySummary')
if __name__=='__main__':unittest.main(verbosity=2)
