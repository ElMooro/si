"""Resume exact retained holdings dates; default provider order, no new selection."""
from pathlib import Path
import hashlib,io,json,unittest,urllib
from decimal import Decimal
from datetime import datetime,timezone
from collections import Counter
import re
import test_etf_constituent_preflight as base
base.PATH=base.ROOT/'aws/ops/staged/ops_5977_etf_constituent_snapshot_recovery.py'
HoldingsProbe=base.HoldingsProbe


class RetainedSelection(unittest.TestCase):
    def test_resume_retained_selection_without_fetching_date_again(self):
        row={'composite_ticker':'SPY','processed_date':'2026-09-18',
             'effective_date':'2026-09-17','weight':.2,'constituent_rank':1}
        raw=json.dumps({'status':'OK','results':[row]}).encode();digest=hashlib.sha256(raw).hexdigest()
        prefix='audit-private/20260909-originals/etf-constituent-research/'
        ref={'key':prefix+digest+'.bin','sha256':digest,'bytes':len(raw)}
        selection={'url':base.ENDPOINT+'?composite_ticker=SPY&processed_date.lte=2026-09-21&sort=processed_date.desc&limit=1',
                   'original':ref,'acquired_at':'2026-09-21T06:00:00Z'}
        calls=[]
        class Storage:
            def get_object(self,**kw):
                self.asserted=kw['Key'];return {'Body':io.BytesIO(raw)}
        def request(s,c,url,b):
            calls.append(url)
            return {'status':'OK','results':[row]},{'status':'retained','original':ref}
        ns=base.actual(['bounded','sha','checked_url','snapshot_probe'],{'urllib':urllib,'datetime':datetime,
            'timezone':timezone,'Counter':Counter,'Decimal':Decimal,'re':re,'ENDPOINT':base.ENDPOINT,
            'request_original':request,'json':json,'hashlib':hashlib,'PREFIX':prefix,'BUCKET':'fixture'})
        out=ns['snapshot_probe'](Storage(),'fixture','SPY','2026-09-21',{},selection)
        self.assertEqual(out['status'],'complete_returned_snapshot');self.assertEqual(len(calls),1)
        query=urllib.parse.parse_qs(urllib.parse.urlsplit(calls[0]).query)
        self.assertEqual(query,{'composite_ticker':['SPY'],'processed_date':['2026-09-18'],'limit':['5000']})
        self.assertEqual(out['selection'],selection)
        selection['original']['sha256']='0'*64
        with self.assertRaises(AssertionError):ns['snapshot_probe'](Storage(),'fixture','SPY','2026-09-21',{},selection)
        self.assertEqual(len(calls),1)


if __name__=='__main__':unittest.main(verbosity=2)
