"""Original-bound dealer measurements, definition conflicts and exact reconciliation."""
from datetime import datetime,timedelta,timezone
from decimal import Decimal,localcontext
import hashlib
from zoneinfo import ZoneInfo
import dealer_original as native

CONTRACT='dealer-original-research.v1'
METHOD='exact-native.same-period-prior52.v1'
PREFIX='data/dealer-research/'
CURRENT='data/nyfed-primary-dealer.json'
AUTHORITY={'call':None,'portfolio_action':'WAIT','allocation_pct':None,'calls_eligible':False,
           'sizing_eligible':False,'execution_eligible':False,'forecast_eligible':False}
HISTORY_COLUMNS=['date','reported_value','original_row_index','source_status','source_period']
GROUP_COLUMNS=['date','usd_mn','source_period','complete']


def bn(value):return float(Decimal(value)/1000) if value is not None else None
def exact_bn(value):return format(Decimal(value)/1000,'f') if value is not None else None


def quality(observed,acquired,at,complete,reviewed=True):
    d=native.day(observed) if observed else None;clock=native.clock(at);received=native.clock(acquired)
    expected=(datetime.combine(d+timedelta(days=(3-d.weekday())%7+14),datetime.min.time(),ZoneInfo('America/New_York'))
              .replace(hour=16,minute=15).astimezone(timezone.utc)) if d else None
    missing=[]
    if not reviewed:missing.append('source_definition_unresolved')
    if not complete:missing.append('latest_report_suppressed_or_missing')
    if (clock-received).total_seconds()>8*86400:missing.append('original_acquisition_older_than_8_days')
    if expected is None or clock>expected+timedelta(hours=24):missing.append('normal_next_release_plus_24h_exceeded')
    if received>clock or d and d>clock.date():missing.append('future_source_clock')
    return {'status':'fresh' if not missing else 'definition_unverified' if not reviewed else 'incomplete' if not complete else 'stale',
            'observation_date':observed,'acquired_at':acquired,'publication_date':None,
            'next_expected_publication_date':expected.isoformat() if expected else None,
            'actual_publication_time_verified':False,'holiday_adjustment_verified':False,
            'max_acquisition_age_hours':192,'normal_release_grace_hours':24,'missing':missing}


def statistics(history):
    latest=history[-1] if history else None;prior=[]
    if latest and latest[1] is not None:
        last=native.day(latest[0])
        for row in reversed(history[:-1]):
            if row[1] is None or row[2]!=latest[2] or (last-native.day(row[0])).days!=7:break
            prior.append(row);last=native.day(row[0])
            if len(prior)==52:break
    out={'status':'insufficient_comparable_prior_reports','z':None,'percentile':None,'mean_usd_mn_decimal':None,
         'sample_sd_usd_mn_decimal':None,'current_excluded':True,'prior_n':len(prior),'minimum_prior_reports':26,
         'from_date':prior[-1][0] if prior else None,'to_date':prior[0][0] if prior else None,
         'source_period':latest[2] if latest else None,'probability':None,
         'method':'52 immediately preceding consecutive weekly reports in the same source period; sample SD; midrank ties'}
    if len(prior)<26:return out
    with localcontext() as ctx:
        ctx.prec=36;values=[Decimal(row[1]) for row in prior];value=Decimal(latest[1]);mean=sum(values)/len(values)
        sd=(sum((v-mean)**2 for v in values)/(len(values)-1)).sqrt()
        out.update(status='available' if sd else 'constant_prior_sample',z=float((value-mean)/sd) if sd else None,
                   percentile=float((sum(v<value for v in values)+Decimal('.5')*sum(v==value for v in values))*100/len(values)),
                   mean_usd_mn_decimal=format(mean,'f'),sample_sd_usd_mn_decimal=format(sd,'f'))
    return out


def comparisons(history):
    if not history:return {}
    latest=history[-1];by={r[0]:r for r in history};out={}
    for n,label in ((7,'1w'),(91,'13w'),(364,'52w')):
        target=(native.day(latest[0])-timedelta(days=n)).isoformat();base=by.get(target)
        usable=latest[1] is not None and base is not None and base[1] is not None and base[2]==latest[2]
        difference=latest[1]-base[1] if usable else None
        out[label]={'from':target,'to':latest[0],'elapsed_days':n,'difference_usd_mn':difference,
                    'difference_usd_bn_decimal':exact_bn(difference),'status':'available' if usable else 'exact_comparable_endpoint_unavailable'}
    return out


def groups():
    nominal=['PDSI'+str(n)+'NSP' for n in (2,3,5,7,10,20,30)]
    tips=['PDST'+str(n)+'NSP' for n in (5,10,30)];frn=['PDFRN2NSP']
    out={'specific_nominal':('On-the-run nominal Treasury settled positions',nominal),
         'specific_tips':('On-the-run TIPS settled positions',tips),
         'specific_frn':('On-the-run FRN settled position',frn),
         'specific_all':('All 11 reported on-the-run issue positions',nominal+tips+frn)}
    buckets={'u13m':('Due within 13 months','L13'),'m13m_5y':('More than 13 months through 5 years','G13'),
             'y5_10':('More than 5 through 10 years','G5L10'),'y10p':('More than 10 years','G10')}
    for name,(label,suffix) in buckets.items():out['corp_'+name]=(label,['PDPOSCSBND-'+suffix,'PDPOSCSBND-BEL'+suffix])
    under=out['corp_u13m'][1]+out['corp_m13m_5y'][1];over=out['corp_y5_10'][1]+out['corp_y10p'][1]
    out.update(corp_under5=('Corporate bonds due within 5 years',under),corp_over5=('Corporate bonds due after 5 years',over),
               corp_bonds=('Corporate bonds: eight IG/HY maturity buckets',under+over),
               corp_with_cp=('Corporate bonds plus commercial paper',under+over+['PDPOSCSCP']))
    for side,prefix in (('reverse_repo','PDSIRRA-'),('repo','PDSORA-'),('borrowed','PDSIOSB-'),('lent','PDSOOS-')):
        keys=[k for k in native.KEYS if k.startswith(prefix)]
        out['fin_'+side]=('All reported collateral: '+side.replace('_',' '),keys)
        out['treasury_'+side]=('Treasury including TIPS: '+side.replace('_',' '),[prefix+'UTSETTOT',prefix+'UTSTTOT'])
    out['treasury_two_sided']=('Treasury repo plus reverse repo (two-sided gross)',out['treasury_reverse_repo'][1]+out['treasury_repo'][1])
    return out


def group(identifier,label,keys,series,acquired,at):
    definitions=[native.SERIES[k] for k in keys];basis={v['valuation_basis'] for v in definitions}
    if len(basis)!=1:raise ValueError('group mixes valuation bases')
    reviewed=all(v['definition_status']=='reviewed' for v in definitions)
    dates=sorted(set().union(*(set(series[k]) for k in keys)));history=[]
    for d in dates:
        rows=[series[k].get(d) for k in keys]
        # The captured catalog defines current SBN2024 identities. Prior PDFs
        # establish accounting rules, not a historical key-by-key scope bridge.
        complete=reviewed and native.period(d)=='SBN2024' and all(row is not None and row['usd_mn'] is not None for row in rows)
        history.append([d,sum(row['usd_mn'] for row in rows) if complete else None,native.period(d),complete])
    latest=history[-1];components={k:series[k].get(latest[0]) for k in keys}
    return {'id':identifier,'label':label,'series_ids':keys,'as_of':latest[0],'usd_mn':latest[1],
            'usd_bn':bn(latest[1]),'exact_usd_bn':exact_bn(latest[1]),'valuation_basis':next(iter(basis)),
            'native_unit':'usd_mn','unit':'usd_bn','quality':quality(latest[0],acquired,at,latest[3],reviewed),
            'components':components,'history':history,'statistics':statistics(history),'comparisons':comparisons(history),
            'interpretation':'Sum of declared same-date identities; not a cash flow or directional signal.',**AUTHORITY}


def verified_context(ref,read):
    if ref is None:return {'status':'unavailable','reason':'Fails snapshot unavailable',**AUTHORITY}
    raw=read(ref['key'])
    if ref['key']!=PREFIX+'contexts/'+ref['sha256']+'.json' or len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:
        raise ValueError('frozen fails context differs')
    packet=native.strict_json(raw);replay=packet.get('replay') or {}
    if packet.get('contract')!='fr2004-fails-research.v1':return {'status':'unverified','reason':'Original fails contract unavailable',**AUTHORITY}
    key=replay.get('manifest_key','')
    import re
    if not re.fullmatch(r'data/fails-research/runs/[a-f0-9]{64}\.json',key):raise ValueError('fails manifest path differs')
    manifest=native.strict_json(read(key));ref=manifest['output'];body=read(ref['key'])
    if key!='data/fails-research/runs/'+native.digest(manifest)+'.json' or ref['key']!='data/fails-research/outputs/'+ref['sha256']+'.json':
        raise ValueError('fails manifest identity differs')
    if len(body)!=ref['bytes'] or hashlib.sha256(body).hexdigest()!=ref['sha256'] or native.strict_json(body)!={k:v for k,v in packet.items() if k!='replay'}:
        raise ValueError('fails immutable output differs')
    if replay.get('output_sha256')!=ref['sha256'] or manifest.get('output_sha256')!=ref['sha256']:raise ValueError('fails output identity differs')
    compact={}
    for name in ('treasury','headline'):
        row=packet[name]
        compact[name]={k:row[k] for k in ('scope_id','as_of','unit','ftd_usd_mn','ftr_usd_mn','gross_usd_mn','exact_usd_bn','quality')}
        if row['complete'] and row['ftd_usd_mn']+row['ftr_usd_mn']!=row['gross_usd_mn']:raise ValueError('fails gross differs')
    return {'status':'retained_output_verified','source':'data/settlement-fails.json','source_generated_at':packet['source_generated_at'],
            'source_replay':replay,'original_calculations_reexecuted_here':False,'scopes':compact,**AUTHORITY}


def compile_research(inputs,read):
    parsed,catalog=native.load(inputs,read);at=inputs['generated_at'];acquired=inputs['sources']['observations']['acquired_at'];series={}
    for key,rows in parsed.items():
        definition=native.SERIES[key];history=[[d,r['usd_mn'],r['row_index'],r['status'],r['seriesbreak']] for d,r in sorted(rows.items())]
        latest=history[-1];reviewed=definition['definition_status']=='reviewed'
        comparable=[[r[0],r[1] if reviewed and r[4]=='SBN2024' else None,r[4]] for r in history]
        series[key]={'definition':definition,'catalog_row_index':catalog[key]['row_index'],'history':history,'as_of':latest[0],
                     'reported_value':latest[1],'original_row_index':latest[2],'source_status':latest[3],
                     'reviewed_catalog_period':'SBN2024','history_scope_note':'Earlier rows are retained original numbers. Historical key-by-key scope mappings have not been verified; current labels must not be projected backwards.',
                     'quality':quality(latest[0],acquired,at,latest[1] is not None,reviewed),
                     'statistics':statistics(comparable),'comparisons':comparisons(comparable),**AUTHORITY}
    measured={key:group(key,label,ids,parsed,acquired,at) for key,(label,ids) in groups().items()}
    def value(key):
        row=measured[key];return row['usd_bn'] if row['quality']['status']=='fresh' else None
    def scalar(key):
        row=series[key];return bn(row['reported_value']) if row['quality']['status']=='fresh' else None
    corp=measured['corp_bonds'];expected=measured['corp_with_cp'];reported=series['PDPOSCS-TOT'];recon=None
    if expected['as_of']==reported['as_of'] and expected['usd_mn'] is not None and reported['reported_value'] is not None:
        recon=reported['reported_value']-expected['usd_mn']
    positions={label:{'latest_b':scalar(key),'as_of':series[key]['as_of'],'quality':series[key]['quality'],
                      'source_series':key,'wow_b':None,'z_52w':None,'definition_status':series[key]['definition']['definition_status']}
               for key,label in (('PDPOSABS-TOT','ABS'),('PDPOSFGS-TOT','AGENCY_DEBT'),('PDPOSSMGO-TOT','MUNIS'),
                                 ('PDPOSGST-TOT','TREASURY_EXTIPS'),('PDPOSMBS-TOT','AGENCY_MBS'))}
    corporate={'as_of':corp['as_of'],'net_bonds_b':value('corp_bonds'),'net_5yplus_b':value('corp_over5'),
               'net_under5y_b':value('corp_under5'),'cp_b':scalar('PDPOSCSCP'),'total_series_b':scalar('PDPOSCS-TOT'),
               'quality':corp['quality'],'components_aligned':corp['quality']['status']=='fresh',
               'regime':'UNQUALIFIED','squeeze_setup':None,'turnover_velocity':None,'z_52w':None,'wow_b':None,'d13w_b':None,
               'pctile_history':None,'all_time_min_b':None,'all_time_max_b':None,'prior_negative_date':None,'weekly_volume_b':None,
               'read':'Exact net fair-value positions. No inference about hedges, gross inventory, market-making capacity or a short squeeze.',
               'source_group':'corp_bonds','reconciliation':{'reported_total_usd_mn':reported['reported_value'],
                   'bond_plus_cp_usd_mn':expected['usd_mn'],'difference_usd_mn':recon,
                   'status':'matched' if recon==0 else 'differs' if recon is not None else 'unavailable'},**AUTHORITY}
    treasury=measured['treasury_two_sided']
    financing={'as_of':treasury['as_of'],'reverse_repo_in_b':value('fin_reverse_repo'),'repo_out_b':value('fin_repo'),
               'sec_borrowed_b':value('fin_borrowed'),'sec_lent_b':value('fin_lent'),'net_lend_b':None,
               'scope':'all_reported_collateral_classes','quality':measured['fin_reverse_repo']['quality'],
               'treasury':{'as_of':treasury['as_of'],'unit':'USD_bn','scope':'Treasury including TIPS','quality':treasury['quality'],
                           'reverse_repo_in_b':value('treasury_reverse_repo'),'repo_out_b':value('treasury_repo'),
                           'gross_two_sided_b':value('treasury_two_sided'),'source_group':'treasury_two_sided',
                           'note':'Gross repo plus reverse-repo balances, not unique collateral or measured reuse.'},**AUTHORITY}
    summary={status:sum(r['quality']['status']==status for r in series.values()) for status in sorted({r['quality']['status'] for r in series.values()})}
    out={'contract':CONTRACT,'engine':'justhodl-nyfed-pd','version':'4.0.0','methodology_version':METHOD,
         'generated_at':at,'source_generated_at':acquired,'as_of':corp['as_of'],
         'quality':{'status':'fresh' if summary.get('fresh')==len(series) else 'degraded','counts':summary,'total_series':len(series)},
         'native_series':series,'groups':measured,'history_columns':HISTORY_COLUMNS,'group_history_columns':GROUP_COLUMNS,
         'sources':inputs['sources'],'reporting_periods':native.DEFINITIONS,'section_pages':native.SECTION_PAGES,
         'reference_release':native.REGISTRY['reference_release'],'legacy_context':inputs['legacy_context'],
         'settlement_fails':verified_context(inputs.get('fails_context'),read),'corporate':corporate,
         'positions_ledger':positions,'financing':financing,'net_treasury_total_b':None,
         'specific_issue_net_settled_b':value('specific_all'),'net_positions_usd_b':{c:value(g) for c,g in (('TREASURY_COUPONS','specific_nominal'),('TIPS','specific_tips'),('TREASURY_FRN','specific_frn'))},
         'wow_usd_b':{},'z_52w':{},'by_tenor_usd_b':{c:{str(int(''.join(filter(str.isdigit,k)))):scalar(k) for k in measured[g]['series_ids']} for c,g in (('TREASURY_COUPONS','specific_nominal'),('TIPS','specific_tips'),('TREASURY_FRN','specific_frn'))},
         'transactions':{key:{'as_of':series[key]['as_of'],'daily_average_b':None,'daily_average_4w_b':None,'weekly_b':None,
                             'avg_4w_b':None,'unit':None,'quality':series[key]['quality'],'source_series':key,
                             'reason':native.SERIES[key]['reason']} for key in series if native.SERIES[key]['family']=='transactions'},
         'read':'Original-source dealer measurements, with unresolved catalog identities and suppressed values explicit. No directional recommendation.',
         'limitations':{'current_provider_vintage':True,'historical_publication_time_verified':False,'unique_collateral_measured':False,
                       'on_the_run_cusip_history_verified':False,'historical_series_scope_bridge_verified':False,
                       'holiday_adjusted_transactions_verified':False,'backtest_eligible':False},**AUTHORITY}
    changes=[];prior=inputs.get('previous_output')
    if prior:
        raw=read(prior['key'])
        if prior['key']!=PREFIX+'outputs/'+prior['sha256']+'.json' or len(raw)!=prior['bytes'] or hashlib.sha256(raw).hexdigest()!=prior['sha256']:raise ValueError('previous snapshot differs')
        old=native.strict_json(raw)
        if old.get('contract')!=CONTRACT or native.clock(old['generated_at'])>native.clock(at):raise ValueError('previous identity differs')
        for key,row in series.items():
            before={r[0]:r[1] for r in old['native_series'][key]['history']};after={r[0]:r[1] for r in row['history']}
            if set(before)-set(after):raise ValueError('provider dropped retained dealer dates')
            changes.extend({'series_id':key,'date':d,'previous':v,'current':after[d]} for d,v in before.items() if v!=after[d])
    out['revisions']={'previous_output':prior,'changes':changes,'known_changed_values':len(changes),'provider_revision_history_complete':False}
    out['decision_id']='dealer-'+native.digest({'method':METHOD,'at':at,'sources':inputs['sources']})
    return out
