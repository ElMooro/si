from pathlib import Path
from datetime import datetime,timedelta,timezone
from copy import deepcopy
import sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/staged')]
import ops_6195_business_cycle_acquisition_inventory as op
AT=datetime(2026,9,27,tzinfo=timezone.utc)


def row(date,bytes=100):
    return {'Key':op.PREFIX+date[:4]+'/'+date+'.json.gz','Size':bytes,'ETag':'whole-object','LastModified':AT}


class Client:
    def __init__(self,rows):self.rows=rows;self.requests=[]
    def get_paginator(self,name):
        assert name=='list_objects_v2';return self
    def paginate(self,**kwargs):
        assert kwargs['Bucket']==op.BUCKET;self.requests.append(kwargs['Prefix'])
        matches=[r for r in self.rows if r['Key'].startswith(kwargs['Prefix'])]
        # Exercise all pages, including an empty first page.
        yield {'Contents':[]}
        for r in matches:yield {'Contents':[r]}
    def get_object(self,**kwargs):raise AssertionError('Inventory must not read bodies')


class Tests(unittest.TestCase):
    def test_all_selected_keys_and_zero_byte_objects_preserved_with_native_cutoff(self):
        cutoff=(AT.date()-timedelta(days=365*5)).isoformat()
        rows=[row('2026-09-25'),row(cutoff,0),row('2021-01-01'),row('2026-12-31')]
        c=Client(rows);result=op.inventory(c,AT)
        self.assertEqual(len(result['selected']),3);self.assertEqual(len(result['excluded']),1)
        self.assertEqual(result['selected_stored_bytes'],200);self.assertEqual(result['largest_stored_object_bytes'],100)
        self.assertEqual(c.requests,[op.PREFIX+str(y)+'/' for y in range(2021,2027)])
        self.assertIn(row('2026-12-31')['Key'],[r['Key'] for r in result['selected']])
        self.assertFalse(result['original_provider_verified']);self.assertEqual(result['source_bodies_read'],0)

    def test_complete_metadata_identity_and_overlap_required(self):
        for field,value in (('Size',True),('Size',-1),('ETag',None),('LastModified',AT.replace(tzinfo=None))):
            r=row('2026-09-25');r[field]=value
            with self.assertRaises(ValueError):op.inventory(Client([r]),AT)
        with self.assertRaises(ValueError):op.inventory(Client([row('2026-09-25')]*2),AT)

    def test_failed_or_malformed_listing_never_becomes_empty_success(self):
        c=Client([])
        def denied(**kw):raise PermissionError('Denied')
        c.paginate=denied
        with self.assertRaises(PermissionError):op.inventory(c,AT)
        c.paginate=lambda **kw:iter([{'Contents':None}])
        with self.assertRaises(ValueError):op.inventory(c,AT)
        c.paginate=lambda **kw:iter([{'Contents':[row('2020-01-01')]}])
        with self.assertRaises(ValueError):op.inventory(c,AT)

    def test_selector_source_is_exact_and_audit_has_no_native_invocation(self):
        raw=(ROOT/'tests/fixtures/pre-acquisition-global-business-cycle.py.txt').read_bytes()
        self.assertEqual(op.store.sha(raw),op.SOURCE_SHA)
        self.assertIn(b'def __init__(self, years=5, workers=12):',raw)
        self.assertIn(b'BARS_ROOT = "data/warm/polygon-full/grouped/"',raw)
        self.assertIn(b'timedelta(days=365 * self.years)',raw)
        self.assertIn(b'datetime.now(timezone.utc).date()',raw)


if __name__=='__main__':unittest.main(verbosity=2)
