"""Build real current Katlin/Sizer output from offline in-memory test inputs."""
import contextlib
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import runpy
ROOT=Path(__file__).resolve().parents[2]
with contextlib.redirect_stdout(io.StringIO()):
    st=runpy.run_path(str(ROOT/'aws/lambdas/justhodl-risk-sizer/tests/run_tests.py'))
    _,sizer=st['_run'](st['_base_docs']())
    kt=runpy.run_path(str(ROOT/'aws/lambdas/justhodl-katlin/tests/run_tests.py'))
    katlin=kt['_load']()
    wr=katlin.war_room({'risk_gate':kt['_gate'](),'khalid_risk':kt['_auth']()})
    rows=[{'ticker':f'S{i}','tier':'READY','asset_class':'stock','learned_excess_126s_pct':100 if i==0 else 2,'composite':80,'vol_ann_pct':25} for i in range(3)]
    basket=katlin.build_basket(rows,wr)
print(json.dumps({'sizer':sizer,'katlin':{'engine':'justhodl-katlin','version':katlin.VERSION,'schema':'1.1','generated_at':datetime.now(timezone.utc).isoformat(),'research_generated_at':datetime.now(timezone.utc).isoformat(),'research_status':'FRESH','session':datetime.now(timezone.utc).date().isoformat(),'expires_at':wr['expires_at'],'war_room':wr,'basket':basket,'picks':rows}},allow_nan=False))
