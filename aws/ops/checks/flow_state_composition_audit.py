"""Independent reconciliation of the composition against exact parent publications.

This checks composition arithmetic and scope, not provider-original replay.
"""
from decimal import Decimal,localcontext

def independent(output,parents):
    etf=parents['data/etf-true-flows.json'];tic=parents['data/capital-inflows.json']
    categories=output['asset_class_rotation'];assert len(categories)==len(etf['category_rotation'])
    memberships={};covered=0;positive=0;negative=0;missing=0
    for index,(actual,reported) in enumerate(zip(categories,etf['category_rotation'])):
        assert actual['category']==reported['category'] and actual['period']==reported['period']
        assert actual['covered_members']==reported['covered_members'] and actual['unavailable_members']==reported['unavailable_members']
        assert actual['configured_members']==reported['configured_members']
        assert actual['source']=={'parent':'data/etf-true-flows.json','pointer':'/category_rotation/'+str(index)}
        expected=[]
        for ticker in reported['covered_members']:
            window=etf['by_etf'][ticker]['flow_windows']['5d']
            assert window['start_date']==reported['period']['start_date'] and window['end_date']==reported['period']['end_date']
            expected.append(Decimal(window['value_decimal']));memberships.setdefault(ticker,[]).append(reported['category'])
        with localcontext() as context:
            context.prec=100
            value=sum(expected,Decimal(0)) if expected else None
        assert actual['unit']=='usd' and actual['whole_market_total'] is False and actual['observed_cash_transfers'] is False
        if value is None:
            missing+=1;assert actual['value_decimal'] is None and actual['direction']=='unavailable'
        else:
            assert Decimal(actual['value_decimal'])==value==Decimal(reported['value_decimal'])
            assert actual['net_flow_5d_usd']==float(value)
            direction='net_issuance_estimate' if value>0 else 'net_redemption_estimate' if value<0 else 'unchanged_estimate'
            assert actual['direction']==direction;positive+=value>0;negative+=value<0
        covered+=len(expected)
    assert output['category_overlap']==[{'ticker':t,'categories':c,'additive_across_categories':False} for t,c in sorted(memberships.items()) if len(c)>1]
    rows=output['monthly_transactions'];assert len(rows)==2*len(tic['by_asset_class'])
    seen=set()
    for actual in rows:
        identity=(actual['asset_class'],actual['period']);assert identity not in seen;seen.add(identity)
        source=tic['by_asset_class'][identity[0]];window=source[identity[1]]
        assert actual['series_id']==source['series_id'] and actual['unit']=='usd_bn'
        assert actual['months']==window['months'] and actual['missing_months']==window['missing_months']
        assert actual['observation_date']==source['data_asof'] and actual['value_decimal']==window['usd_bn_decimal']
        if actual['value_decimal'] is not None:
            with localcontext() as context:
                context.prec=100
                assert Decimal(actual['value_decimal'])*1000==Decimal(window['usd_million_decimal'])
        assert actual['month_label_is_release_date'] is False and actual['price_or_valuation_change'] is False
    assert output['category_total'] is None and output['call'] is None and output['risk_regime_score'] is None
    assert all(output[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'))
    assert output['independent_investment_votes']==0 and output['decision']['status']=='abstain'
    assert all(v is None for v in output['hard_assets_and_dollar'].values())
    assert output['dark_pool']=={'accumulation_n':None,'distribution_n':None,'top_accumulation':[],'top_distribution':[]}
    return {'native_parents':len(output['parents']),'categories':len(categories),'covered_category_memberships':covered,
        'distinct_covered_funds':len(memberships),'overlapping_funds':len(output['category_overlap']),
        'positive_estimate_categories':positive,'negative_estimate_categories':negative,'unavailable_categories':missing,
        'monthly_transaction_windows':len(rows),'shared_roots':len(output['dependency_graph']['shared_roots']),
        'provider_original_replay_performed_by_this_audit':False}
