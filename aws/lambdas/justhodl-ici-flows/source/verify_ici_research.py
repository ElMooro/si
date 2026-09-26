"""Separate table tokenizer and exact-rational checks for ICI candidate output."""
from datetime import datetime,timedelta
from fractions import Fraction
from html import unescape
import hashlib,re

def cells(raw):
    def text(value):return ' '.join(unescape(re.sub(r'<[^>]+>','',value)).split())
    return [[[text(c) for c in re.findall(r'<t[dh]\b[^>]*>(.*?)</t[dh]\s*>',row,re.I|re.S)]
             for row in re.findall(r'<tr\b[^>]*>(.*?)</tr\s*>',table,re.I|re.S)]
            for table in re.findall(r'<table\b[^>]*>(.*?)</table\s*>',raw.decode('utf-8-sig'),re.I|re.S)]

def rational(value):
    value=value.replace(',','').replace('−','-').strip()
    if value in ('','—','–','-','N/A','NA'):return None
    if value.startswith('(') and value.endswith(')'):value='-'+value[1:-1]
    return Fraction(value)

def same(value,expected):
    if expected is None:
        if value is not None:raise ValueError('Missing source cell was filled')
    elif type(value) not in (int,float) or abs(float(expected)-value)>max(1e-12,abs(float(expected))*1e-14):raise ValueError('Numeric output differs')

def verify(originals,packet):
    names={'mmf':['government','government_retail','government_institutional','prime','prime_retail','prime_institutional','tax_exempt','tax_exempt_retail','tax_exempt_institutional','total','retail','institutional'],
           'combined_flows':['equity','equity_domestic','equity_world','hybrid','bond','bond_taxable','bond_municipal','commodity','total']}
    groups={'mmf':[(1,2,3),(4,5,6),(7,8,9),(10,1,4,7),(10,11,12),(11,2,5,8),(12,3,6,9)],
            'combined_flows':[(1,2,3),(5,6,7),(9,1,4,5,8)]}
    all_data={};observation_checks=0;change_checks=0;reconciliation_checks=0;issues=0
    if set(packet['sources'])!=set(names) or set(originals)!=set(names):raise ValueError('Whole source population required')
    for kind,series_names in names.items():
        source=packet['sources'][kind];raw=originals[kind]
        unit='usd_bn' if kind=='mmf' else 'usd_mn'
        if source['source_id']!='ICI:'+kind or source['url']!='https://www.ici.org/research/stats/'+('mmf' if kind=='mmf' else 'combined_flows') or source['unit']!=unit or source['classification_rows']!=len(series_names):raise ValueError('Source identity or units differ')
        if source['sha256']!=hashlib.sha256(raw).hexdigest() or source['bytes']!=len(raw):raise ValueError('Original identity differs')
        tables=cells(raw);table=tables[source['table_index']];header=table[0]
        if len(table)!=len(series_names)+1 or any(len(row)!=len(header) for row in table):raise ValueError('Independent table shape differs')
        dates={i:datetime.strptime(value,'%m/%d/%Y').date().isoformat() for i,value in enumerate(header) if re.fullmatch(r'\d{1,2}/\d{1,2}/\d{4}',value)}
        expected={(r,c) for r in range(1,len(table)) for c in dates};seen=set();data={}
        if source['table_header']!=header or source['observation_dates']!=sorted(dates.values()):raise ValueError('Source header differs')
        for row in source['observations']:
            cell=row['source_cell'];r,c=cell['row'],cell['column'];coord=(r,c)
            if coord not in expected or coord in seen or cell['table']!=source['table_index']:raise ValueError('Missing/duplicate observation coordinate')
            seen.add(coord);value=rational(table[r][c]);series=series_names[r-1]
            if cell['text']!=table[r][c] or row['series']!=series or row['series_id']!='ici:'+kind+':'+series or row['date']!=dates[c]:raise ValueError('Source classification or date differs')
            if row['unit']!=('usd_bn' if kind=='mmf' else 'usd_mn'):raise ValueError('Units differ')
            if (row['decimal'] is None)!=(value is None) or value is not None and Fraction(row['decimal'])!=value:raise ValueError('Exact source decimal differs')
            same(row['value'],value);data[(r,dates[c])]=value;observation_checks+=1
        if seen!=expected:raise ValueError('Incomplete observation population')
        comparisons=[];quantum=Fraction(1,100) if kind=='mmf' else Fraction(1)
        for day in sorted(dates.values()):
            for group in groups[kind]:
                vals=[data[(r,day)] for r in group];residual=vals[0]-sum(vals[1:]) if all(v is not None for v in vals) else None
                comparisons.append((day,residual,quantum*len(vals)/2))
        if kind=='mmf':
            column=header.index('$ Change*') if '$ Change*' in header else header.index('$ Change')
            changes=source['reported_changes']
            if len(changes)!=len(series_names):raise ValueError('Whole reported change column required')
            printed=list(dates.values())
            for r,change in enumerate(changes,1):
                reported=rational(table[r][column]);cell=change['source_cell']
                if cell!={'table':source['table_index'],'row':r,'column':column,'text':table[r][column]} or change['series']!=series_names[r-1] or change['unit']!=unit:raise ValueError('Reported change coordinate differs')
                if (change['decimal'] is None)!=(reported is None) or reported is not None and Fraction(change['decimal'])!=reported:raise ValueError('Reported change differs')
                vals=[data[(r,printed[0])],data[(r,printed[1])],reported]
                residual=vals[0]-vals[1]-vals[2] if all(v is not None for v in vals) else None
                comparisons.append((printed[0],residual,quantum*3/2));change_checks+=1
        checks=packet['reconciliation'][kind]
        if len(checks)!=len(comparisons):raise ValueError('Whole reconciliation population required')
        for row,(day,residual,tolerance) in zip(checks,comparisons):
            expected_status='missing_input' if residual is None else 'within_reported_rounding' if abs(residual)<=tolerance else 'outside_reported_rounding'
            if (row['residual_decimal'] is None)!=(residual is None) or residual is not None and Fraction(row['residual_decimal'])!=residual:raise ValueError('Reconciliation residual differs')
            if row['date']!=day or Fraction(row['rounding_tolerance_decimal'])!=tolerance or row['status']!=expected_status:raise ValueError('Reconciliation verdict differs')
            reconciliation_checks+=1;issues+=expected_status!='within_reported_rounding'
        all_data[kind]=data
    md=max(packet['sources']['mmf']['observation_dates']);fd=max(packet['sources']['combined_flows']['observation_dates'])
    stocks=all_data['mmf'];flows=all_data['combined_flows'];mmf=packet['mmf']
    history=mmf['history'];days=packet['sources']['mmf']['observation_dates']
    if mmf['date']!=md or mmf['weeks_n']!=len(days) or [row['date'] for row in history]!=days:raise ValueError('Complete asset history required')
    for row in history:same(row['value'],stocks[(10,row['date'])])
    for key,r in [('total_b',10),('govt_b',1),('prime_b',4),('tax_exempt_b',7),('retail_b',11),('inst_b',12)]:same(mmf[key],stocks[(r,md)])
    before=(datetime.fromisoformat(md)-timedelta(days=7)).date().isoformat();latest=stocks[(10,md)];previous=stocks.get((10,before));gov=stocks[(1,md)]
    same(mmf['wow_b'],latest-previous if latest is not None and previous is not None else None)
    same(mmf['govt_share_pct'],100*gov/latest if gov is not None and latest is not None and latest>0 else None)
    for key,r in [('eq_dom',2),('eq_world',3),('hybrid',4),('bond',5),('muni',7),('commodity',8),('total',9),('equity',1)]:
        ds=sorted((datetime.fromisoformat(fd)-timedelta(days=7*i)).date().isoformat() for i in range(4))
        values=[flows.get((r,d)) for d in ds];total=sum(values) if all(v is not None for v in values) else None
        if key=='equity':same(packet['long_term']['equity_sum_4w_m'],total)
        else:
            row=packet['long_term']['classes'][key];same(row['latest_w_m'],flows[(r,fd)]);same(row['sum_4w_m'],total)
            if row['window_dates']!=ds or row['date']!=fd or row['z_4w'] is not None:raise ValueError('Flow window differs')
    if any(packet[k] is not False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')) or packet['decision']!={'verb':'WAIT','action':'abstain'}:raise ValueError('Unqualified authority required')
    if any(mmf[k] is not None for k in ('chg_13w_b','z_13w','yoy_pct')) or any(packet[k] is not None for k in ('signal','regime','call')) or packet['quality']['reconciliation_issues']!=issues:raise ValueError('Unavailable metric or quality differs')
    if packet['contract']!='ici-research.v1' or packet['quality']['status']!=('measurement_reconciliation_failed' if issues else 'dated_measurements'):raise ValueError('Research contract or quality differs')
    return {'whole_original_bytes':sum(len(raw) for raw in originals.values()),'observation_checks':observation_checks,
        'reported_change_checks':change_checks,'independent_rational_reconciliations':reconciliation_checks,
        'reconciliation_issues':issues,'original_release_vintages_verified':False,'forecast_qualified':False}
