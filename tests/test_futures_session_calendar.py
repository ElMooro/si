from pathlib import Path
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import futures_session_calendar as c
def rows():return [{'product_code':'ES','trading_venue':'XCME','session_end_date':'2026-09-21','event':event,'timestamp':stamp} for event,stamp in
    [('pre_open','2026-09-20T21:45:00Z'),('open','2026-09-20T22:00:00Z'),('close','2026-09-21T21:00:00Z')]]
def result(values=None,complete=True):return c.reconcile(rows() if values is None else values,'ES','XCME','2026-09-01','2026-09-21',complete)
class Tests(unittest.TestCase):
    def test_repeated_event_identities_keep_all_source_ordinals_without_extra_votes(self):
        out=result(rows()*13);self.assertEqual(out['returned_rows'],39);self.assertEqual(out['repeated_event_identity_rows'],36)
        self.assertEqual(out['qualified_session_closes'],1);session=out['sessions']['2026-09-21']
        self.assertEqual(session['scheduled_close_utc'],'2026-09-21T21:00:00+00:00')
        self.assertEqual(len(session['event_identities']),3);self.assertEqual(len(session['event_identities'][0]['source_row_ordinals']),13)
    def test_capture_before_and_after_scheduled_end_remains_distinct_from_finality(self):
        for clock,ended in [('2026-09-21T19:26:00Z',False),('2026-09-21T21:00:00Z',True)]:
            out=c.status(result(),'2026-09-21',clock);self.assertIs(out['scheduled_session_ended_by_capture'],ended)
            self.assertFalse(out['bar_finality_independently_verified'])
    def test_missing_or_conflicting_close_not_inferred(self):
        for values in (rows()[:-1],rows()+[{**rows()[-1],'timestamp':'2026-09-21T20:00:00Z'}]):
            out=result(values);self.assertEqual(out['qualified_session_closes'],0)
            self.assertIsNone(c.status(out,'2026-09-21','2026-09-22T00:00:00Z')['scheduled_session_ended_by_capture'])
    def test_incomplete_scope_foreign_identity_and_invalid_events_fail_closed(self):
        for change in ({'product_code':'NQ'},{'trading_venue':'XNYM'},{'session_end_date':'2026-09-22'},
            {'timestamp':'2026-09-21T21:00:00'},{'timestamp':'2025-09-21T21:00:00Z'},{'event':'settled'}):
            values=rows();values[-1].update(change);out=result(values)
            self.assertEqual(out['invalid_source_row_ordinals'],[2]);self.assertEqual(out['qualified_session_closes'],0)
        self.assertEqual(result(complete=False)['qualified_session_closes'],0)
    def test_open_after_close_is_ambiguous(self):
        values=rows();values[1]['timestamp']='2026-09-21T22:00:00Z'
        self.assertEqual(result(values)['qualified_session_closes'],0)
    def test_absent_session_is_unknown_not_closed(self):
        out=c.status(result(),'2026-09-20','2026-09-22T00:00:00Z')
        self.assertIsNone(out['scheduled_close_utc']);self.assertIsNone(out['scheduled_session_ended_by_capture'])
    def test_timezone_equivalent_events_are_same_identity(self):
        values=rows()+[{**rows()[-1],'timestamp':'2026-09-21T17:00:00-04:00'}]
        self.assertEqual(result(values)['repeated_event_identity_rows'],1);self.assertEqual(result(values)['qualified_session_closes'],1)
    def test_reconciliation_does_not_mutate_original_rows(self):
        values=rows();old=copy.deepcopy(values);result(values);self.assertEqual(values,old)
if __name__=='__main__':unittest.main(verbosity=2)
