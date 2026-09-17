"""No-network checks for separate FR2004 scopes and composite quality."""
import copy
import sys
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pd_fails_context import project, load, input_quality


def run():
    now = datetime(2026,9,17,tzinfo=timezone.utc)
    q = {'status':'fresh','observation_date':'2026-09-02'}
    doc = {'treasury':{'as_of':'2026-09-02','ftd_bn':92.61,'ftr_bn':97.63,'gross_bn':190.24,'quality':q},
           'headline':{'as_of':'2026-09-09','ftd_bn':113.83,'ftr_bn':119.22,'combined_bn':233.1,'quality':q}}
    out = project(doc,now)
    assert out['combined_bn']==190.24 and out['scope_id']=='treasury_incl_tips'
    assert out['ust_ex_tips']['combined_bn']==233.1 and out['ust_ex_tips']['as_of']=='2026-09-09'
    assert out['quality']['status']=='stale' and out['ust_ex_tips']['quality']['status']=='fresh'
    d=copy.deepcopy(doc);del d['treasury']['ftd_bn'];out=project(d,now)
    assert out['ftd_bn'] is None and out['ftr_bn']==97.63 and out['quality']['status']=='unavailable'
    d=copy.deepcopy(doc);d['treasury'].update(ftd_bn=0,ftr_bn=0,gross_bn=0,as_of='2026-09-09')
    assert project(d,now)['combined_bn']==0
    d['treasury']['scope_id']='ust_ex_tips'
    assert project(d,now)['quality']['status']=='unavailable'
    d['treasury']['ftd_bn']=float('nan')
    assert project(d,now)['ftd_bn'] is None
    assert project({},now)['combined_bn'] is None
    class Denied:
        def get_object(self,**kwargs):raise OSError('fixture unavailable')
    assert load(Denied(),'fixture',now)['quality']['reason']=='source_unavailable'
    feed={'generated_at':now.isoformat(),'quality':{'status':'fresh','observation_date':'2026-09-16'}}
    assert input_quality({'a':feed},now)['status']=='fresh'
    feed['quality']['observation_date']='2020-01-01'
    assert input_quality({'a':feed},now)['status']=='partial'
    feed['generated_at']='2020-01-01T00:00:00Z'
    assert input_quality({'a':feed},now)['status']=='partial'
    assert input_quality({'a':{'generated_at':now.isoformat()}},now)['status']=='partial'
    print('PD context checks passed: separate scopes, no field fallback, zero/null, date SLA, source failure, source quality')


if __name__=='__main__': run()
