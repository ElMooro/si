"""Typed, dated dealer measurement context. No forecast or sizing permission."""
from datetime import datetime,timezone,timedelta
from decimal import Decimal
import re

CONTRACT='dealer-original-research.v1'
AUTHORITY={'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'call':None}
CORP=tuple('PDPOSCSBND-'+grade+suffix for suffix in ('L13','G13','G5L10','G10') for grade in ('','BEL'))
FIN_IN=('PDSIRRA-UTSETTOT','PDSIRRA-UTSTTOT')
FIN_OUT=('PDSORA-UTSETTOT','PDSORA-UTSTTOT')


def clock(value):
    if not isinstance(value,str):raise ValueError('dated clock required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('timezone required')
    return result.astimezone(timezone.utc)


def fresh(q,at):
    try:
        age=(at-clock(q['acquired_at'])).total_seconds()
        return q['status']=='fresh' and 0<=age<=192*3600 and at<=clock(q['next_expected_publication_date'])+timedelta(hours=24) and clock(q['observation_date']+'T00:00:00Z')<=at
    except (KeyError,TypeError,ValueError):return False


def group(packet,name,ids,basis,at):
    row=(packet.get('groups') or {}).get(name) or {};parts=row.get('components') or {}
    if not fresh(row.get('quality') or {},at) or row.get('valuation_basis')!=basis or row.get('unit')!='usd_bn':return None
    if set(parts)!=set(ids) or set(row.get('series_ids') or [])!=set(ids):return None
    vals=[]
    for key,value in parts.items():
        if not isinstance(value,dict) or value.get('date')!=row.get('as_of') or value.get('status')!='observed':return None
        amount=value.get('usd_mn')
        if type(amount) is not int or abs(amount)>(2**53-1)//100:return None
        vals.append(amount)
    total=sum(vals)
    if type(row.get('usd_mn')) is not int or row['usd_mn']!=total:return None
    amount=float(Decimal(total)/1000)
    if row.get('usd_bn')!=amount or row['quality'].get('acquired_at')!=packet.get('source_generated_at'):return None
    return {'usd_bn':amount,'as_of':row['as_of'],'quality':row['quality'],'source_group':name,'source_series':list(ids),
            'unit':'usd_bn','valuation_basis':basis,**AUTHORITY}


def project(packet,at=None):
    at=at or datetime.now(timezone.utc)
    empty={'status':'unavailable','corporate':None,'treasury_financing':None,'specific_positions':None,
           'role':'dated_measurement_context','original_calculations_reexecuted_here':False,**AUTHORITY}
    if not isinstance(packet,dict) or packet.get('contract')!=CONTRACT or packet.get('call') is not None or any(packet.get(k) is not False for k in ('calls_eligible','sizing_eligible','execution_eligible')):return empty
    try:
        age=(at-clock(packet['generated_at'])).total_seconds();source=clock(packet['source_generated_at'])
        if not 0<=age<=192*3600 or source>clock(packet['generated_at']):return empty
        ref=packet['replay']
        if not re.fullmatch(r'data/dealer-research/runs/[a-f0-9]{64}\.json',ref['manifest_key']) or not re.fullmatch('[a-f0-9]{64}',ref['output_sha256']):return empty
    except (KeyError,TypeError,ValueError):return empty
    corp=group(packet,'corp_bonds',CORP,'fair_value',at)
    if corp:
        corp.update(net_bonds_b=corp['usd_bn'],regime=None,squeeze_setup=None,z_52w=None,turnover_velocity=None,
                    read='Dealer net fair-value bond positions; no measured gross inventory, hedges or market-making capacity.')
        for field,name,ids in (('net_under5y_b','corp_under5',CORP[:4]),('net_5yplus_b','corp_over5',CORP[4:])):
            value=group(packet,name,ids,'fair_value',at);corp[field]=value['usd_bn'] if value and value['as_of']==corp['as_of'] else None
    financing=group(packet,'treasury_two_sided',FIN_IN+FIN_OUT,'funds_paid_or_received; securities_only_at_fair_value',at)
    if financing:
        fi=group(packet,'treasury_reverse_repo',FIN_IN,financing['valuation_basis'],at)
        fo=group(packet,'treasury_repo',FIN_OUT,financing['valuation_basis'],at)
        if not fi or not fo or fi['as_of']!=financing['as_of'] or fo['as_of']!=financing['as_of']:financing=None
        else:financing.update(reverse_repo_in_b=fi['usd_bn'],repo_out_b=fo['usd_bn'],gross_two_sided_b=financing['usd_bn'],scope='Treasury including TIPS')
    specific={};mapping={'TREASURY_COUPONS':('specific_nominal',tuple('PDSI'+str(n)+'NSP' for n in (2,3,5,7,10,20,30))),
                         'TIPS':('specific_tips',tuple('PDST'+str(n)+'NSP' for n in (5,10,30))),
                         'TREASURY_FRN':('specific_frn',('PDFRN2NSP',))}
    for name,(key,ids) in mapping.items():
        value=group(packet,key,ids,'original_issuance_par',at)
        if value:
            value['by_original_tenor_usd_bn']={str(int(''.join(filter(str.isdigit,k)))):float(Decimal(packet['groups'][key]['components'][k]['usd_mn'])/1000) for k in ids}
            specific[name]=value
    return {**empty,'status':'context_only' if corp or financing or specific else 'unavailable','corporate':corp,
            'treasury_financing':financing,'specific_positions':specific or None,'source':'data/nyfed-primary-dealer.json',
            'source_generated_at':packet['source_generated_at'],'source_replay':ref,
            'note':'Original replay reference is supplied by the producer. Consumer checks typed amounts and clocks, not the archived provider bodies. No directional vote or sizing authority.'}
