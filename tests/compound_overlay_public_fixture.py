"""Invented S3 -> actual Compound handler -> public packet for browser checks."""
from pathlib import Path
from datetime import datetime, timezone, timedelta
import contextlib, io, json, socket, sys
from unittest.mock import patch
R=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(R/'aws/shared'),str(R/'aws/shared/tests')]
from ciss_vintage_test_support import load
from test_compound_numeric import Storage
from compound_test_support import empty_sources

def packet():
    with patch.object(socket.socket,'connect',side_effect=AssertionError('Invented storage only')):
        m=load('justhodl-compound-aggregator');now=datetime.now(timezone.utc);today=now.date()
        records=[{'ticker':'QA'+str(i),'score':i-2} for i in range(30)]
        objects={**empty_sources(m),
          'data/nobrainers.json':{'summary':{'top_25_overall':records}},
          'data/insider-clusters.json':{'clusters':[{'ticker':r['ticker'],'score':0} for r in records]},
          'data/compound-firstseen.json':{'QA29|nobrainers':(today+timedelta(days=1)).isoformat()},
          'data/risk-gate.json':{'posture':'NEUTRAL'},
          'data/trend-reversal.json':{'rows':[{'ticker':'QA2','direction':None,'reversal_score':0,'spk':[90,100,105,110]}],'breadth':{'bottom_pct':0,'top_pct':0}},
          'data/compound-history.json':{'days':[{'d':(today-timedelta(days=days)).isoformat(),
             'score_basis':m.BASIS,'numeric_contract':m.NUMERIC_CONTRACT,
             'activist_boundary':'ownership-feed-abstention.v1','volatility_boundary':'price-compression-abstention.v1',
             'momentum_boundary':m.MOMENTUM_BASIS,'scores':{'QA2':score}}
             for days,score in ((3,-10),(2,0),(1,15),(9000,999999))]}}
        db=Storage(objects)
        with patch.object(m,'S3',db),patch.object(m,'emit_alerts',side_effect=AssertionError('No sends')),contextlib.redirect_stdout(io.StringIO()):
            m.lambda_handler({'suppress_alerts':True})
        assert m.STATE_KEY not in db.writes
        return db.writes[m.S3_KEY]
if __name__=='__main__':
    print(json.dumps(packet(),allow_nan=False))
