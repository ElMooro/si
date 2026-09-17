"""Network-free production macro donor builders and capacity handler regressions."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/"shared/tests"))
from macro_donor_test_support import run

if __name__=="__main__":
    from ciss_vintage_test_support import load, TODAY
    from unittest.mock import patch
    engine=load('justhodl-euro-fragmentation')
    with patch.object(engine,'_get',side_effect=AssertionError('Current cache should avoid rate-limited API')):
        assert engine.fred('IRLTLT01DEM156N',cached=[{'date':TODAY,'value':0}]) == [(TODAY,0)]
    with patch.object(engine,'_get',return_value=b'{"observations": [{"date":"2026-08-01","value":"3.2"}]}') as live:
        assert engine.fred('IRLTLT01DEM156N',cached=[{'date':'2000-01-01','value':8}])[0][1] == 3.2
        assert live.call_count==1
    from ciss_vintage_test_support import run as run_ciss
    run_ciss()
    run()
