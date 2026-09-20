"""Exercise one-source CISS context and missing-dimensional coverage."""
import io
import json
import sys
import types
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[3]/'shared/tests'))
from ciss_readthrough_test_support import packet, CLOCK, Frozen, load


def test_missing_dimensions_do_not_manufacture_normal_or_zero():
    m=load('justhodl-regime-composite')
    dims=m.compute_dimensions([])
    assert all(d['score'] is None and d['n']==0 for d in dims.values())
    assert m.compute_composite_score(dims) is None
    assert m.classify_meta_regime(dims,[])[0]=='UNAVAILABLE'
    dims['vol']={'score':1,'n':1}
    assert m.classify_meta_regime(dims,[])[0]=='UNAVAILABLE'


def test_ciss_contributions_and_headline_are_context_not_four_votes():
    m=load('justhodl-regime-composite');p=packet()
    ciss=[cfg for cfg in m.MODULES_CFG if cfg.get('derive') in m.CISS_SERIES]
    rows=[m.fetch_module(cfg,p,CLOCK) for cfg in ciss]
    assert len(rows)==4 and all(r['vote_eligible'] is False and r['polarity'] is None for r in rows)
    assert all(r['source_context']['status']=='fresh' for r in rows)
    assert len({r['source_context']['source_replay']['output_sha256'] for r in rows})==1
    assert {r['source_context']['unit'] for r in rows}=={'dimensionless_index','dimensionless_contribution'}
    assert all(d['n']==0 for d in m.compute_dimensions(rows).values())


def test_unmapped_labels_do_not_become_neutral_votes():
    m=load('justhodl-regime-composite');cfg=m.MODULES_CFG[0]
    client=types.SimpleNamespace(get_object=lambda **k: {'Body':io.BytesIO(b'{"market_composite":{"composite_regime":"NEW_UNKNOWN_LABEL"}}')})
    with patch.object(m,'S3',client):row=m.fetch_module(cfg)
    assert row['polarity'] is None and row['descriptive_eligible'] is False
    assert m.compute_dimensions([row])['vol']['n']==0


def test_actual_handler_fetches_ciss_once_and_emits_abstention_without_alert():
    m=load('justhodl-regime-composite');p=packet();reads=[];writes=[];notify=Mock()
    def get(**kw):
        reads.append(kw['Key'])
        if kw['Key']=='data/ciss-stress.json':return {'Body':io.BytesIO(json.dumps(p).encode())}
        raise KeyError(kw['Key'])
    with patch.object(m,'S3',types.SimpleNamespace(get_object=get,put_object=lambda **k:writes.append(k))), \
         patch.object(m,'datetime',Frozen),patch.object(m,'telegram_alert',notify), \
         patch.dict(sys.modules,{'wl_fusion':types.SimpleNamespace(block=lambda _: {'fixture':'SYNTHETIC'})}):
        result=m.lambda_handler({'suppress_alerts':True},None)
    out=json.loads(next(w['Body'] for w in writes if w['Key']==m.OUTPUT_KEY))
    assert result['statusCode']==200 and reads.count('data/ciss-stress.json')==1
    assert out['meta_regime']=='UNAVAILABLE' and out['composite_score'] is None
    assert out['decision']['meaning']=='abstain' and out['calls_eligible'] is False and out['sizing_eligible'] is False
    assert out['dependency_groups'][0]['independent_votes']==0 and out['dependency_groups'][0]['usable_source_families']==1
    assert len(out['composite_coverage']['missing_dimensions'])==7
    notify.assert_not_called()


def test_populated_heuristics_are_still_described_without_portfolio_commands():
    m=load('justhodl-regime-composite')
    for value in (-1,0,1):
        modules=[{'dimension':d,'label':d,'regime':'fixture','polarity':value,'missing':False} for d in m.DIMENSIONS]
        dims=m.compute_dimensions(modules)
        assert m.compute_composite_score(dims)==value*100
        label,narrative,_=m.classify_meta_regime(dims,modules)
        assert label!='UNAVAILABLE'
        for prohibited in ('Stay long equities','Reduce equity beta','ride momentum','tighten stops'):
            assert prohibited not in narrative


if __name__=='__main__':
    tests=[f for n,f in list(globals().items()) if n.startswith('test_')]
    for test in tests:test()
    import unittest
    suite=unittest.TestLoader().discover(str(Path(__file__).resolve().parents[3]/'shared/tests'),pattern='test_holdings_authority.py')
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():raise SystemExit(1)
    print(f'Meta-regime source and permission contracts passed: {len(tests)}')

if __name__=='__main__':
    from pathlib import Path
    import sys
    sys.path.insert(0,str(Path(__file__).resolve().parents[4]/'tests'))
    from retail_consumer_test_support import run as run_retail
    run_retail()
